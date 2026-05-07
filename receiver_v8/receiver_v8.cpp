#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <iostream>
#include <thread>
#include <atomic>
#include <mutex>
#include <chrono>
#include <optional>
#include <vector>

#include <opencv2/opencv.hpp>

extern "C"
{
#include "efos_sw_version.h"
#include "efos_v2x_api.h"
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <errno.h>

#include "picoquic.h"
#include "picosocks.h"
#include "picoquic_utils.h"
#include "picoquic_internal.h"
}

#include <gst/gst.h>
#include <gst/app/gstappsrc.h>
#include <gst/app/gstappsink.h>

// ===================== AWS SDK - IoT SiteWise =====================
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/iotsitewise/IoTSiteWiseClient.h>
#include <aws/iotsitewise/model/BatchPutAssetPropertyValueRequest.h>
#include <aws/iotsitewise/model/PutAssetPropertyValueEntry.h>
#include <aws/iotsitewise/model/AssetPropertyValue.h>
#include <aws/iotsitewise/model/Variant.h>
#include <aws/iotsitewise/model/TimeInNanos.h>
#include <queue>
#include <map>

// #####################GLOBAL Variable############################
std::atomic<bool> keepRunning(true);
static U16 g_channelID = 0;
std::mutex quic_mutex;
static GstAppSrc *g_appsrc = nullptr;
static GstAppSink *g_appsink = nullptr;

static std::vector<uint8_t> g_recvBuf;

static picoquic_cnx_t *g_server_cnx = nullptr;

// ===================== 패킷 타입 정의 =====================
#define PKT_TYPE_RTP 0x01
#define PKT_TYPE_YOLO 0x02

// ===================== YOLO 디텍션 결과 (수신용) =====================
struct Detection
{
    int classId;
    float confidence;
    cv::Rect bbox;
};

// 최신 YOLO 디텍션 결과 (메인 루프에서 읽기)
std::mutex yolo_mutex;
std::vector<Detection> g_latestDetections;

static const char *COCO_CLASSES[] = {
    "person", "bicycle", "car", "motorbike", "aeroplane", "bus", "train", "truck", "boat",
    "traffic light", "fire hydrant", "stop sign", "parking meter", "bench", "bird", "cat",
    "dog", "horse", "sheep", "cow", "elephant", "bear", "zebra", "giraffe", "backpack",
    "umbrella", "handbag", "tie", "suitcase", "frisbee", "skis", "snowboard", "sports ball",
    "kite", "baseball bat", "baseball glove", "skateboard", "surfboard", "tennis racket",
    "bottle", "wine glass", "cup", "fork", "knife", "spoon", "bowl", "banana", "apple",
    "sandwich", "orange", "broccoli", "carrot", "hot dog", "pizza", "donut", "cake", "chair",
    "sofa", "pottedplant", "bed", "diningtable", "toilet", "tvmonitor", "laptop", "mouse",
    "remote", "keyboard", "cell phone", "microwave", "oven", "toaster", "sink",
    "refrigerator", "book", "clock", "vase", "scissors", "teddy bear", "hair drier", "toothbrush"};
static const int NUM_CLASSES = 80;

// ===================== 네트워크 설정 =====================
// #define PC5_RECEIVER_IP     "192.168.70.101"
// #define PC5_SENDER_IP       "192.168.70.102"
#define PC5_SENDER_IP "192.168.1.11"
#define PC5_RECEIVER_IP "10.254.52.18"

// Uu (5G) 경로 - 실제 주소
#define UU_RECEIVER_IP "202.30.29.202" // Receiver 공인 IP (자기 자신)
#define UU_RECEIVER_PORT 4433
// Sender의 Uu 주소는 recvfrom에서 동적으로 알게 됨

// PC5 dummy 주소
struct sockaddr_storage pc5_remote_addr; // sender (PC5)
struct sockaddr_storage pc5_local_addr;  // receiver (PC5)

// Uu UDP 소켓
static int g_uu_sock = -1;

#define STREAM_NAME "test-stream"
#define AWS_REGION "ap-northeast-2"

// ===================== SiteWise 설정 =====================
// ★ AWS 콘솔에서 Asset/Property 만든 후 여기에 UUID 넣어야 함
#define SITEWISE_ASSET_ID "REPLACE-WITH-YOUR-ASSET-UUID"

// Property ID 매핑 (AWS 콘솔에서 각 Property UUID 확인 후 채우기)
// 추적할 관심 클래스만 골라서 property로 올림
struct SiteWiseProperty
{
    const char *cocoClass;  // COCO 클래스명 (매칭용)
    int cocoClassId;        // COCO class id
    const char *propertyId; // SiteWise Property UUID
};

static SiteWiseProperty g_sitewiseProps[] = {
    {"person", 0, "REPLACE-WITH-PERSON-PROPERTY-UUID"},
    {"bicycle", 1, "REPLACE-WITH-BICYCLE-PROPERTY-UUID"},
    {"car", 2, "REPLACE-WITH-CAR-PROPERTY-UUID"},
    {"motorbike", 3, "REPLACE-WITH-MOTORBIKE-PROPERTY-UUID"},
    {"bus", 5, "REPLACE-WITH-BUS-PROPERTY-UUID"},
    {"truck", 7, "REPLACE-WITH-TRUCK-PROPERTY-UUID"},
};
static const int NUM_SITEWISE_PROPS = sizeof(g_sitewiseProps) / sizeof(g_sitewiseProps[0]);

// total_count property (모든 객체 합계)
#define SITEWISE_TOTAL_PROPERTY_ID "REPLACE-WITH-TOTAL-PROPERTY-UUID"

// 한 프레임의 집계 결과 (전송 큐에 쌓임)
struct FrameCountSnapshot
{
    int64_t timestampNanos;         // 프레임 발생 시점 (나노초)
    int counts[NUM_SITEWISE_PROPS]; // 각 클래스별 카운트
    int totalCount;                 // 전체 객체 수
};

static std::mutex g_sitewiseQueueMutex;
static std::queue<FrameCountSnapshot> g_sitewiseQueue;
static Aws::IoTSiteWise::IoTSiteWiseClient *g_sitewiseClient = nullptr;

// ##########################FUNCTION###############################
static void APP_ControlRxCallback(V2xMsgType v2xMsgType, U8 *p_rxBuf) {}

// ===================== 간단한 JSON 파서 =====================
// {"f":123,"d":[{"c":0,"p":95,"b":[10,20,100,200]},...]}}
std::vector<Detection> parseYoloJson(const uint8_t *data, uint32_t len)
{
    std::vector<Detection> dets;
    std::string json((const char *)data, len);

    // "d":[ 찾기
    size_t dPos = json.find("\"d\":[");
    if (dPos == std::string::npos)
        return dets;

    size_t pos = dPos + 5; // '[' 다음부터

    while (pos < json.size())
    {
        // {"c": 찾기
        size_t cPos = json.find("\"c\":", pos);
        if (cPos == std::string::npos)
            break;

        Detection det;
        // classId
        det.classId = atoi(json.c_str() + cPos + 4);

        // confidence ("p":)
        size_t pPos = json.find("\"p\":", cPos);
        if (pPos == std::string::npos)
            break;
        det.confidence = atoi(json.c_str() + pPos + 4) / 100.0f;

        // bbox ("b":[x,y,w,h])
        size_t bPos = json.find("\"b\":[", cPos);
        if (bPos == std::string::npos)
            break;
        int x, y, w, h;
        if (sscanf(json.c_str() + bPos + 4, "[%d,%d,%d,%d]", &x, &y, &w, &h) == 4)
        {
            det.bbox = cv::Rect(x, y, w, h);
        }

        dets.push_back(det);

        // 다음 detection으로 이동
        size_t nextBrace = json.find('}', bPos);
        if (nextBrace == std::string::npos)
            break;
        pos = nextBrace + 1;
    }
    return dets;
}

// ===================== 디텍션 바운딩 박스 그리기 =====================
void drawDetections(cv::Mat &frame, const std::vector<Detection> &dets)
{
    for (const auto &d : dets)
    {
        cv::rectangle(frame, d.bbox, cv::Scalar(0, 255, 0), 2);
        char label[64];
        snprintf(label, sizeof(label), "%s %.0f%%",
                 (d.classId < NUM_CLASSES) ? COCO_CLASSES[d.classId] : "?",
                 d.confidence * 100);
        int baseline = 0;
        cv::Size textSize = cv::getTextSize(label, cv::FONT_HERSHEY_SIMPLEX, 0.5, 1, &baseline);
        cv::rectangle(frame,
                      cv::Point(d.bbox.x, d.bbox.y - textSize.height - 4),
                      cv::Point(d.bbox.x + textSize.width, d.bbox.y),
                      cv::Scalar(0, 255, 0), cv::FILLED);
        cv::putText(frame, label, cv::Point(d.bbox.x, d.bbox.y - 2),
                    cv::FONT_HERSHEY_SIMPLEX, 0.5, cv::Scalar(0, 0, 0), 1);
    }
}

int customQuicCallback(picoquic_cnx_t *cnx, uint64_t stream_id, uint8_t *bytes, size_t length,
                       picoquic_call_back_event_t event, void *callback_ctx, void *stream_ctx)
{
    g_server_cnx = cnx;

    if (event == picoquic_callback_stream_data && length > 0)
    {
        g_recvBuf.insert(g_recvBuf.end(), bytes, bytes + length);

        if (g_recvBuf.size() > 50000)
        {
            printf("[BUF] 버퍼 크기: %zu bytes\n", g_recvBuf.size());
        }

        // 파싱: [1바이트 타입][4바이트 길이][페이로드]
        while (g_recvBuf.size() >= 5) // 최소 1 + 4 = 5바이트
        {
            uint8_t type = g_recvBuf[0];
            uint32_t net_len;
            memcpy(&net_len, g_recvBuf.data() + 1, 4);
            uint32_t pktLen = ntohl(net_len);

            if (pktLen > 1000000) // 1MB 이상이면 비정상
            {
                printf("[Receiver] 비정상 패킷 길이: %u (type=%d), 버퍼 초기화\n", pktLen, type);
                g_recvBuf.clear();
                break;
            }

            if (g_recvBuf.size() < 5 + pktLen)
                break; // 아직 덜 왔음

            if (type == PKT_TYPE_RTP)
            {
                // RTP 패킷 → GStreamer로 push
                GstBuffer *gstbuf = gst_buffer_new_allocate(nullptr, pktLen, nullptr);
                GstMapInfo map;
                gst_buffer_map(gstbuf, &map, GST_MAP_WRITE);
                memcpy(map.data, g_recvBuf.data() + 5, pktLen);
                gst_buffer_unmap(gstbuf, &map);

                if (!g_appsrc || gst_app_src_push_buffer(g_appsrc, gstbuf) != GST_FLOW_OK)
                {
                    gst_buffer_unref(gstbuf);
                }
            }
            else if (type == PKT_TYPE_YOLO)
            {
                // YOLO JSON → 파싱해서 최신 디텍션 결과 갱신
                auto dets = parseYoloJson(g_recvBuf.data() + 5, pktLen);

                // ===== SiteWise용 집계 =====
                FrameCountSnapshot snap;
                memset(snap.counts, 0, sizeof(snap.counts));
                snap.totalCount = (int)dets.size();
                snap.timestampNanos = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                          std::chrono::system_clock::now().time_since_epoch())
                                          .count();

                for (const auto &d : dets)
                {
                    for (int i = 0; i < NUM_SITEWISE_PROPS; ++i)
                    {
                        if (d.classId == g_sitewiseProps[i].cocoClassId)
                        {
                            snap.counts[i]++;
                            break;
                        }
                    }
                }

                {
                    std::lock_guard<std::mutex> lock(g_sitewiseQueueMutex);
                    // 큐 무한 증가 방지 (최대 300 = 10초치)
                    if (g_sitewiseQueue.size() < 300)
                    {
                        g_sitewiseQueue.push(snap);
                    }
                }

                {
                    std::lock_guard<std::mutex> lock(yolo_mutex);
                    g_latestDetections = std::move(dets);
                }
            }
            else
            {
                printf("[Receiver] 알 수 없는 패킷 타입: 0x%02x\n", type);
            }

            // 처리한 만큼 버퍼에서 제거
            g_recvBuf.erase(g_recvBuf.begin(), g_recvBuf.begin() + 5 + pktLen);
        }
    }
    else if (event == picoquic_callback_ready)
    {
        printf("[QUIC Server] ★ 커넥션 READY! Multipath: %d, 경로 수: %d\n",
               cnx->is_multipath_enabled, cnx->nb_paths);
    }
    else if (event == picoquic_callback_next_path_allowed)
    {
        printf("[QUIC Server] 새 경로 추가 허용됨\n");
    }
    else if (event == picoquic_callback_close || event == picoquic_callback_application_close)
    {
        printf("[QUIC Server] 커넥션 종료\n");
    }
    return 0;
}

void dispatchQuicPacket(uint8_t *data, size_t len,
                        struct sockaddr_storage *peer_addr,
                        struct sockaddr_storage *local_addr)
{
    struct sockaddr_in *peer_sin = (struct sockaddr_in *)peer_addr;
    struct sockaddr_in *pc5_sin = (struct sockaddr_in *)&pc5_remote_addr;

    static std::atomic<int> pc5_count{0};
    static std::atomic<int> uu_count{0};
    static auto last_log = std::chrono::steady_clock::now();

    if (peer_sin->sin_addr.s_addr == pc5_sin->sin_addr.s_addr &&
        peer_sin->sin_port == pc5_sin->sin_port)
    {
        pc5_count++;
        std::vector<uint8_t> pkt(sizeof(V2xMsgReq) + len);
        V2xMsgReq *p_req = reinterpret_cast<V2xMsgReq *>(pkt.data());
        p_req->length = len;
        p_req->v2xChannelId = g_channelID;
        memcpy(p_req->data, data, len);
        EFOS_SendV2xMsg(p_req);
    }
    else
    {
        uu_count++;
        if (g_uu_sock >= 0)
        {
            sendto(g_uu_sock, data, len, 0,
                   (struct sockaddr *)peer_addr, sizeof(struct sockaddr_in));
        }
    }

    auto now = std::chrono::steady_clock::now();
    if (std::chrono::duration_cast<std::chrono::seconds>(now - last_log).count() >= 1)
    {
        struct sockaddr_in *p = (struct sockaddr_in *)peer_addr;
        printf("[Receiver DISPATCH] PC5=%d, Uu=%d (마지막 peer: %s:%d)\n",
               pc5_count.exchange(0), uu_count.exchange(0),
               inet_ntoa(p->sin_addr), ntohs(p->sin_port));
        last_log = now;
    }
}

// ===================== 초기화 함수들 =====================

std::optional<V2xMsgRegi> initSirius()
{
    if (EFOS_SetControlRxCallback((ControlRxCallbackFunc *)APP_ControlRxCallback) < EFOS_RESULT_SUCCESS)
    {
        printf("Failed to register control rx callback\n");
        return std::nullopt;
    }
    if (EFOS_InitializeV2x((char *)PC5_RECEIVER_IP) < EFOS_RESULT_SUCCESS)
    {
        printf("Failed to initialize\n");
        return std::nullopt;
    }
    printf("Connected to SIRIUS with IP (%s)\n", PC5_RECEIVER_IP);

    V2xMsgReq v2xmsgreq;
    V2xMsgRegi v2xmsgregi = {
        .v2xServiceId = EFOS_V2X_SERVICE_ID_BCAST_PQI_TP,
        .v2xMsgFamilyId = EFOS_V2X_MSG_FAMILY_IEEE1609,
        .commMode = EFOS_V2X_COMM_BROADCAST,
        .scheduleType = EFOS_SCHEDULE_TYPE_EVENT,
        .txInterval = 100.0,
        .mcsConf = {0xFF, 0xFF},
    };
    if (EFOS_EstablishV2xChannel(&v2xmsgreq.v2xChannelId, &v2xmsgregi) < EFOS_RESULT_SUCCESS)
    {
        printf("Failed EstablishV2XChannel\n");
        return std::nullopt;
    }
    g_channelID = v2xmsgreq.v2xChannelId;
    return v2xmsgregi;
}

void initGStreamerApp(GstElement *pipeline)
{
    g_appsrc = GST_APP_SRC(gst_bin_get_by_name(GST_BIN(pipeline), "src"));
    g_appsink = GST_APP_SINK(gst_bin_get_by_name(GST_BIN(pipeline), "sink"));
    gst_element_set_state(pipeline, GST_STATE_PLAYING);
}

GstElement *initGStreamerPipeline()
{
    std::string gst_pipeline =
        "appsrc name=src is-live=true format=time do-timestamp=true ! "
        "application/x-rtp,media=video,clock-rate=90000,encoding-name=H264,payload=96 ! "
        "rtpjitterbuffer latency=50 drop-on-latency=true ! " // 200→50ms
        "rtph264depay ! h264parse config-interval=-1 ! "
        "video/x-h264,stream-format=avc,alignment=au ! "
        "kvssink "
        "stream-name=\"" +
        std::string(STREAM_NAME) + "\" "
                                   "aws-region=\"" +
        std::string(AWS_REGION) + "\" "
                                  "storage-size=128 "
                                  "fragment-duration=1000 " // ★ 2000ms → 1000ms (1초)
                                  "framerate=30 "
                                  "key-frame-fragmentation=true ";

    GError *err = nullptr;
    GstElement *pipeline = gst_parse_launch(gst_pipeline.c_str(), &err);
    if (!pipeline || err)
    {
        printf("파이프라인 생성 실패: %s\n", err ? err->message : "unknown");
        return nullptr;
    }
    return pipeline;
}

int initUuSocket()
{
    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0)
    {
        printf("[Uu] 소켓 생성 실패: %s\n", strerror(errno));
        return -1;
    }

    // Non-blocking
    int flags = fcntl(sock, F_GETFL, 0);
    fcntl(sock, F_SETFL, flags | O_NONBLOCK);

    // SO_REUSEADDR
    int opt = 1;
    setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    // Receiver의 공인 IP:4433에 바인딩
    struct sockaddr_in bind_addr;
    memset(&bind_addr, 0, sizeof(bind_addr));
    bind_addr.sin_family = AF_INET;
    inet_pton(AF_INET, UU_RECEIVER_IP, &bind_addr.sin_addr);
    bind_addr.sin_port = htons(UU_RECEIVER_PORT);

    if (bind(sock, (struct sockaddr *)&bind_addr, sizeof(bind_addr)) < 0)
    {
        printf("[Uu] 바인딩 실패 (%s:%d): %s\n",
               UU_RECEIVER_IP, UU_RECEIVER_PORT, strerror(errno));
        // 공인 IP 바인딩 실패 시 INADDR_ANY로 재시도
        bind_addr.sin_addr.s_addr = INADDR_ANY;
        if (bind(sock, (struct sockaddr *)&bind_addr, sizeof(bind_addr)) < 0)
        {
            printf("[Uu] INADDR_ANY 바인딩도 실패: %s\n", strerror(errno));
            close(sock);
            return -1;
        }
        printf("[Uu] INADDR_ANY:%d로 바인딩 완료 (대체)\n", UU_RECEIVER_PORT);
    }
    else
    {
        printf("[Uu] 소켓 바인딩 완료: %s:%d\n", UU_RECEIVER_IP, UU_RECEIVER_PORT);
    }

    return sock;
}

void initAddresses()
{
    // PC5 dummy 주소 (receiver 입장에서 remote=sender, local=receiver)
    memset(&pc5_remote_addr, 0, sizeof(pc5_remote_addr));
    memset(&pc5_local_addr, 0, sizeof(pc5_local_addr));

    struct sockaddr_in *pc5_remote = (struct sockaddr_in *)&pc5_remote_addr;
    pc5_remote->sin_family = AF_INET;
    inet_pton(AF_INET, PC5_SENDER_IP, &pc5_remote->sin_addr); // sender (상대방)
    pc5_remote->sin_port = htons(12345);

    struct sockaddr_in *pc5_local = (struct sockaddr_in *)&pc5_local_addr;
    pc5_local->sin_family = AF_INET;
    inet_pton(AF_INET, PC5_RECEIVER_IP, &pc5_local->sin_addr); // receiver (나)
    pc5_local->sin_port = htons(9999);
}

picoquic_quic_t *initQuic()
{
    uint64_t current_time = picoquic_current_time();

    // QUIC context 생성 (서버 역할)
    picoquic_quic_t *quic = picoquic_create(
        8,
        "certs/cert.pem",
        "certs/key.pem",
        NULL,
        "v2x_test",
        customQuicCallback, // 서버는 여기서 기본 콜백 등록
        NULL,
        NULL, NULL, NULL,
        current_time, NULL, NULL, NULL, 0);

    if (!quic)
    {
        printf("[QUIC] picoquic_create 실패\n");
        return nullptr;
    }

    // ★ Multipath 활성화 (서버 측)
    quic->default_multipath_option = 1;
    quic->default_tp.initial_max_path_id = 4;

    printf("[QUIC] 서버 QUIC 엔진 초기화 완료 (Multipath 옵션: %d)\n",
           quic->default_multipath_option);

    return quic;
}

// ===================== SiteWise 초기화 / 전송 =====================

// AWS SDK 전역 옵션 (main에서 관리)
static Aws::SDKOptions g_awsOptions;

bool initSiteWise()
{
    Aws::InitAPI(g_awsOptions);

    Aws::Client::ClientConfiguration cfg;
    cfg.region = AWS_REGION;
    // 타임아웃 짧게 (블로킹 방지)
    cfg.connectTimeoutMs = 3000;
    cfg.requestTimeoutMs = 5000;

    // 인증은 기본 체인 사용: 환경변수 AWS_ACCESS_KEY_ID/SECRET, 또는 ~/.aws/credentials
    g_sitewiseClient = new Aws::IoTSiteWise::IoTSiteWiseClient(cfg);
    if (!g_sitewiseClient)
    {
        printf("[SiteWise] 클라이언트 생성 실패\n");
        return false;
    }
    printf("[SiteWise] 클라이언트 초기화 완료 (region=%s)\n", AWS_REGION);
    return true;
}

void shutdownSiteWise()
{
    if (g_sitewiseClient)
    {
        delete g_sitewiseClient;
        g_sitewiseClient = nullptr;
    }
    Aws::ShutdownAPI(g_awsOptions);
}

// SiteWise 전송 스레드: 1초마다 큐에 쌓인 프레임들을 배치로 업로드
void sitewiseSenderThread()
{
    printf("[SiteWise 전송] 스레드 시작 (1초 주기 배치 업로드)\n");

    using namespace Aws::IoTSiteWise::Model;

    while (keepRunning)
    {
        std::this_thread::sleep_for(std::chrono::seconds(1));

        // 큐에서 프레임 스냅샷들 꺼내기
        std::vector<FrameCountSnapshot> batch;
        {
            std::lock_guard<std::mutex> lock(g_sitewiseQueueMutex);
            while (!g_sitewiseQueue.empty())
            {
                batch.push_back(g_sitewiseQueue.front());
                g_sitewiseQueue.pop();
            }
        }

        if (batch.empty())
            continue;
        if (!g_sitewiseClient)
            continue;

        // SiteWise BatchPutAssetPropertyValue 제약:
        //   - 한 요청당 최대 10개 entry
        //   - 한 entry당 최대 10개 propertyValue
        // 우리는 Property당 1개 entry를 만들고, 그 안에 최대 10개 timestamp 값을 넣음
        // → 10프레임씩 쪼개서 여러 요청으로 보냄

        const int VALUES_PER_ENTRY = 10;
        int totalProps = NUM_SITEWISE_PROPS + 1; // +1 for total_count

        // 10프레임씩 끊어서 보냄
        for (size_t offset = 0; offset < batch.size(); offset += VALUES_PER_ENTRY)
        {
            size_t end = std::min(offset + VALUES_PER_ENTRY, batch.size());
            size_t chunkSize = end - offset;

            BatchPutAssetPropertyValueRequest req;
            Aws::Vector<PutAssetPropertyValueEntry> entries;

            // 각 Property별로 entry 생성
            auto buildEntry = [&](const char *propertyId,
                                  std::function<int(const FrameCountSnapshot &)> getter,
                                  const std::string &entryIdSuffix)
            {
                PutAssetPropertyValueEntry entry;
                entry.SetEntryId(Aws::String("e_") + entryIdSuffix +
                                 "_" + std::to_string(offset));
                entry.SetAssetId(SITEWISE_ASSET_ID);
                entry.SetPropertyId(propertyId);

                Aws::Vector<AssetPropertyValue> values;
                for (size_t i = offset; i < end; ++i)
                {
                    Variant v;
                    v.SetIntegerValue(getter(batch[i]));

                    TimeInNanos t;
                    int64_t ns = batch[i].timestampNanos;
                    t.SetTimeInSeconds(ns / 1000000000LL);
                    t.SetOffsetInNanos((int32_t)(ns % 1000000000LL));

                    AssetPropertyValue apv;
                    apv.SetValue(v);
                    apv.SetTimestamp(t);
                    apv.SetQuality(Quality::GOOD);
                    values.push_back(apv);
                }
                entry.SetPropertyValues(values);
                entries.push_back(entry);
            };

            // 클래스별 property
            for (int i = 0; i < NUM_SITEWISE_PROPS; ++i)
            {
                int idx = i; // 람다 캡처용
                buildEntry(g_sitewiseProps[i].propertyId, [idx](const FrameCountSnapshot &s)
                           { return s.counts[idx]; }, g_sitewiseProps[i].cocoClass);
            }
            // total_count property
            buildEntry(SITEWISE_TOTAL_PROPERTY_ID, [](const FrameCountSnapshot &s)
                       { return s.totalCount; }, "total");

            req.SetEntries(entries);

            auto outcome = g_sitewiseClient->BatchPutAssetPropertyValue(req);
            if (!outcome.IsSuccess())
            {
                printf("[SiteWise] 업로드 실패: %s\n",
                       outcome.GetError().GetMessage().c_str());
            }
            else
            {
                const auto &errs = outcome.GetResult().GetErrorEntries();
                if (!errs.empty())
                {
                    printf("[SiteWise] 부분 실패: %zu개 entry 에러\n", errs.size());
                    for (const auto &e : errs)
                    {
                        const auto &elist = e.GetErrors();
                        for (const auto &err : elist)
                        {
                            printf("  - entry=%s code=%d msg=%s\n",
                                   e.GetEntryId().c_str(),
                                   (int)err.GetErrorCode(),
                                   err.GetErrorMessage().c_str());
                        }
                    }
                }
            }
        }

        printf("[SiteWise 전송] %zu 프레임 업로드 완료\n", batch.size());
    }
    printf("[SiteWise 전송] 스레드 종료\n");
}

// ##########################THREAD###############################
void pathMonitorThread()
{
    while (keepRunning)
    {
        std::this_thread::sleep_for(std::chrono::seconds(3));
        std::lock_guard<std::mutex> lock(quic_mutex);
        if (g_server_cnx)
        {
            printf("[Server 상태] 경로 수=%d, Multipath=%s\n",
                   g_server_cnx->nb_paths,
                   g_server_cnx->is_multipath_enabled ? "ON" : "OFF");
        }
    }
}

void inputThread()
{
    char c;
    printf("종료하려면 q or Q를 터미널에 입력\n");
    while (keepRunning)
    {
        std::cin >> c;
        if (c == 'q' || c == 'Q')
        {
            keepRunning = false;
            break;
        }
    }
}

void pc5RecvThread(picoquic_quic_t *quic)
{
    uint8_t rxBuf[9100] = {0};
    printf("[PC5 수신] V2X -> QUIC 포워딩 스레드 시작...\n");

    static std::atomic<int> recv_count{0};
    static std::atomic<int> small_count{0};
    static auto last_log = std::chrono::steady_clock::now();

    while (keepRunning)
    {
        V2xMsgType v2xMsgType = EFOS_RecvV2xMsg(rxBuf);
        V2xMsg *p_v2xMsg = (V2xMsg *)rxBuf;

        if (p_v2xMsg->length == 0)
            continue;
        if (p_v2xMsg->length <= 34)
        {
            small_count++;
            continue;
        }

        recv_count++;
        // if (recv_count < 4)
        // {
        //     printf("[Receiver PC5 RECV] len=%d, first_byte=0x%02x\n",
        //            p_v2xMsg->length - 34, p_v2xMsg->data[34]);
        // }

        std::lock_guard<std::mutex> lock(quic_mutex);
        uint64_t current_time = picoquic_current_time();

        // picoquic_incoming_packet(quic,
        //                          p_v2xMsg->data + 34,
        //                          p_v2xMsg->length - 34,
        //                          (struct sockaddr*)&pc5_remote_addr,
        //                          (struct sockaddr*)&pc5_local_addr,
        //                          0, 0, current_time);
        int ret = picoquic_incoming_packet(quic,
                                           p_v2xMsg->data + 34, p_v2xMsg->length - 34,
                                           (struct sockaddr *)&pc5_remote_addr,
                                           (struct sockaddr *)&pc5_local_addr,
                                           0, 0, current_time);

        static int err_log_count = 0;
        if (ret != 0)
        {
            printf("[PC5 incoming] 에러 %d, len=%d, first_byte=0x%02x\n",
                   ret, p_v2xMsg->length - 34, p_v2xMsg->data[34]);
            err_log_count++;
        }
        else if (ret == 0 && err_log_count < 30)
        {
            // 성공도 한 번만 찍어보기
            static bool ok_logged = false;
            if (!ok_logged)
            {
                printf("[PC5 incoming] ★ 성공 발견! len=%d, first_byte=0x%02x\n",
                       p_v2xMsg->length - 34, p_v2xMsg->data[34]);
                ok_logged = true;
            }
        }

        auto now = std::chrono::steady_clock::now();
        if (std::chrono::duration_cast<std::chrono::seconds>(now - last_log).count() >= 1)
        {
            printf("[PC5 수신] 지난 1초: %d 패킷 수신 (너무 짧은 것 %d개 제외)\n",
                   recv_count.exchange(0), small_count.exchange(0));
            last_log = now;
        }
    }

    gst_app_src_end_of_stream(g_appsrc);
    printf("[PC5 수신] V2X -> QUIC 포워딩 스레드 종료\n");
}

// ★ Uu 수신 스레드 (실제 UDP 소켓에서 recvfrom)
void uuRecvThread(picoquic_quic_t *quic)
{
    uint8_t rxBuf[2048];
    printf("[Uu 수신] UDP 수신 스레드 시작 (port %d)...\n", UU_RECEIVER_PORT);

    while (keepRunning)
    {
        struct sockaddr_storage from_addr;
        socklen_t from_len = sizeof(from_addr);

        ssize_t recvLen = recvfrom(g_uu_sock, rxBuf, sizeof(rxBuf), 0,
                                   (struct sockaddr *)&from_addr, &from_len);

        if (recvLen > 0)
        {
            // Uu로 도착한 QUIC 패킷을 엔진에 전달
            // to 주소는 receiver 자신의 Uu 주소
            struct sockaddr_in uu_local;
            memset(&uu_local, 0, sizeof(uu_local));
            uu_local.sin_family = AF_INET;
            inet_pton(AF_INET, UU_RECEIVER_IP, &uu_local.sin_addr);
            uu_local.sin_port = htons(UU_RECEIVER_PORT);

            std::lock_guard<std::mutex> lock(quic_mutex);
            picoquic_incoming_packet(quic,
                                     rxBuf, recvLen,
                                     (struct sockaddr *)&from_addr, // from: sender (NAT된 공인IP)
                                     (struct sockaddr *)&uu_local,  // to: receiver (내 Uu 주소)
                                     0, 0, picoquic_current_time());
        }
        else if (recvLen < 0 && errno != EAGAIN && errno != EWOULDBLOCK)
        {
            printf("[Uu 수신] recvfrom 에러: %s\n", strerror(errno));
        }
        else
        {
            std::this_thread::sleep_for(std::chrono::microseconds(500));
        }
    }
    printf("[Uu 수신] UDP 수신 스레드 종료\n");
}

// ★ 통합 응답 송신 스레드 (기존 quicResponseThread의 multipath 버전)
void quicResponseThread(picoquic_quic_t *quic)
{
    uint8_t sendBuf[1536];
    printf("[응답 송신] 통합 QUIC 응답 스레드 시작...\n");

    while (keepRunning)
    {
        size_t sendLen = 0;
        struct sockaddr_storage peer_addr, local_addr;
        int if_index = 0;
        picoquic_connection_id_t log_cid;
        picoquic_cnx_t *last_cnx = nullptr;
        size_t send_msg_size = 0;

        {
            std::lock_guard<std::mutex> lock(quic_mutex);
            uint64_t current_time = picoquic_current_time();

            int ret = picoquic_prepare_next_packet_ex(
                quic, current_time,
                sendBuf, sizeof(sendBuf), &sendLen,
                &peer_addr, &local_addr,
                &if_index, &log_cid, &last_cnx, &send_msg_size);

            if (ret != 0)
            {
                printf("[응답 송신] prepare 에러: %d\n", ret);
                continue;
            }
        }

        if (sendLen > 0)
        {
            dispatchQuicPacket(sendBuf, sendLen, &peer_addr, &local_addr);
        }
        else
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }
    printf("[응답 송신] 통합 QUIC 응답 스레드 종료\n");
}

// ##########################MAIN###############################
int main(int argc, char **argv)
{
    gst_init(&argc, &argv);

    // ---------------------- INIT 주소 ----------------------
    initAddresses();

    // ---------------------- INIT V2X (PC5) ----------------------
    auto v2xmsgregi = initSirius();
    if (!v2xmsgregi)
    {
        printf("V2X initialize ERROR\n");
        return -1;
    }

    // ---------------------- INIT Uu 소켓 ----------------------
    g_uu_sock = initUuSocket();
    if (g_uu_sock < 0)
    {
        printf("Uu 소켓 초기화 실패\n");
        return -1;
    }

    // ---------------------- INIT GSTREAMER ----------------------
    GstElement *pipeline = initGStreamerPipeline();
    if (!pipeline)
    {
        printf("GStreamer 파이프라인 초기화 실패\n");
        return -1;
    }
    initGStreamerApp(pipeline);

    // ---------------------- INIT QUIC ----------------------
    picoquic_quic_t *quic = initQuic();
    if (!quic)
    {
        printf("QUIC 초기화 실패\n");
        return -1;
    }

    // ---------------------- INIT SiteWise ----------------------
    if (!initSiteWise())
    {
        printf("SiteWise 초기화 실패 (계속 진행은 가능)\n");
        // SiteWise 실패해도 영상 스트리밍은 계속 — return -1 안 함
    }

    // ---------------------- INIT THREAD ----------------------
    std::thread input_thread(inputThread);
    std::thread pc5_recv_thread(pc5RecvThread, quic);
    std::thread uu_recv_thread(uuRecvThread, quic);
    std::thread response_thread(quicResponseThread, quic);
    std::thread path_monitor_thread(pathMonitorThread);
    std::thread sitewise_thread(sitewiseSenderThread);

    // ---------------------- MAIN LOOP ----------------------
    while (keepRunning)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    // ---------------------- FINAL ----------------------
    pc5_recv_thread.join();
    uu_recv_thread.join();
    input_thread.join();
    response_thread.join();
    path_monitor_thread.join();
    sitewise_thread.join();

    shutdownSiteWise();

    gst_object_unref(g_appsrc);
    gst_object_unref(g_appsink);
    gst_element_set_state(pipeline, GST_STATE_NULL);
    gst_object_unref(pipeline);

    picoquic_free(quic);

    if (g_uu_sock >= 0)
        close(g_uu_sock);

    cv::destroyAllWindows();
    EFOS_TerminateV2x(v2xmsgregi->v2xServiceId, v2xmsgregi->commMode);
    return 0;
}