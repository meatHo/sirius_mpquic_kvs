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

// #####################GLOBAL Variable############################
std::atomic<bool> keepRunning(true);
static U16 g_channelID = 0;
std::mutex quic_mutex;
static GstAppSrc *g_appsrc = nullptr;
static GstAppSink *g_appsink = nullptr;

static std::vector<uint8_t> g_recvBuf;

static picoquic_cnx_t *g_server_cnx = nullptr;

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

// ##########################FUNCTION###############################
static void APP_ControlRxCallback(V2xMsgType v2xMsgType, U8 *p_rxBuf) {}

int customQuicCallback(picoquic_cnx_t *cnx, uint64_t stream_id, uint8_t *bytes, size_t length,
                       picoquic_call_back_event_t event, void *callback_ctx, void *stream_ctx)
{
    g_server_cnx = cnx;

    if (event == picoquic_callback_stream_data && length > 0)
    {
        g_recvBuf.insert(g_recvBuf.end(), bytes, bytes + length);

        if (g_recvBuf.size() > 10000)
        {
            printf("[BUF] 버퍼 크기: %zu bytes\n", g_recvBuf.size());
        }

        while (g_recvBuf.size() >= 4)
        {
            uint32_t net_len;
            memcpy(&net_len, g_recvBuf.data(), 4);
            uint32_t pktLen = ntohl(net_len);

            if (pktLen > 65535)
            {
                printf("[Receiver] 비정상 패킷 길이: %u, 버퍼 초기화\n", pktLen);
                g_recvBuf.clear();
                break;
            }

            if (g_recvBuf.size() < 4 + pktLen)
                break;

            GstBuffer *gstbuf = gst_buffer_new_allocate(nullptr, pktLen, nullptr);
            GstMapInfo map;
            gst_buffer_map(gstbuf, &map, GST_MAP_WRITE);
            memcpy(map.data, g_recvBuf.data() + 4, pktLen);
            gst_buffer_unmap(gstbuf, &map);

            if (!g_appsrc || gst_app_src_push_buffer(g_appsrc, gstbuf) != GST_FLOW_OK)
            {
                gst_buffer_unref(gstbuf);
            }

            g_recvBuf.erase(g_recvBuf.begin(), g_recvBuf.begin() + 4 + pktLen);
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
        "rtpjitterbuffer name=jbuf mode=0 latency=200 do-lost=true ! "
        "rtph264depay ! h264parse ! avdec_h264 max-threads=4 ! "
        "videoconvert ! video/x-raw,format=BGR ! "
        "appsink name=sink sync=false drop=true max-buffers=1";

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

    // ---------------------- INIT THREAD ----------------------
    std::thread input_thread(inputThread);
    std::thread pc5_recv_thread(pc5RecvThread, quic);
    std::thread uu_recv_thread(uuRecvThread, quic);
    std::thread response_thread(quicResponseThread, quic);
    std::thread path_monitor_thread(pathMonitorThread);

    // ---------------------- MAIN LOOP ----------------------
    while (keepRunning)
    {
        GstSample *sample = gst_app_sink_try_pull_sample(g_appsink, 30 * GST_MSECOND);
        if (!sample)
            continue;

        GstBuffer *buf = gst_sample_get_buffer(sample);
        GstCaps *caps = gst_sample_get_caps(sample);

        GstStructure *s = gst_caps_get_structure(caps, 0);
        int w = 0, h = 0;
        gst_structure_get_int(s, "width", &w);
        gst_structure_get_int(s, "height", &h);

        GstMapInfo map;
        if (w > 0 && h > 0 && gst_buffer_map(buf, &map, GST_MAP_READ))
        {
            cv::Mat frame(h, w, CV_8UC3, map.data);
            cv::imshow("V2X Video Stream Receiver (Multipath)", frame);
            gst_buffer_unmap(buf, &map);
        }

        gst_sample_unref(sample);

        if (cv::waitKey(1) == 'q')
        {
            keepRunning = false;
            break;
        }
    }

    // ---------------------- FINAL ----------------------
    pc5_recv_thread.join();
    uu_recv_thread.join();
    input_thread.join();
    response_thread.join();
    path_monitor_thread.join();

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
