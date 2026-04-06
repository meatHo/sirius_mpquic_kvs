#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <iostream>
#include <thread>
#include <atomic>
#include <mutex>
#include <chrono>
#include <arpa/inet.h>
#include <optional>

#include <opencv2/opencv.hpp>

extern "C"{
    #include "efos_sw_version.h"
    #include "efos_v2x_api.h"
    #include <unistd.h>

    #include "picoquic.h"
    #include "picosocks.h"
    #include "picoquic_utils.h"
}

#include <gst/gst.h>
#include <gst/app/gstappsrc.h>
#include <gst/app/gstappsink.h>

//#####################GLOBAL Variable############################
std::atomic<bool> keepRunning(true);
static U16 g_channelID = 0;
std::mutex quic_mutex;
static GstAppSrc* g_appsrc = nullptr;
static GstAppSink* g_appsink = nullptr;

static std::vector<uint8_t> g_recvBuf;

#define RECEIVER_SIRIUS_IP      "192.168.70.101"
#define SENDER_SIRIUS_IP        "192.168.70.102"
#define STREAM_NAME   "test-stream"
#define AWS_REGION    "ap-northeast-2"

// sender IP: 192.168.70.102
// receiver IP: 192.168.70.101
struct sockaddr_storage dummy_remote_addr; // sender (상대방)
struct sockaddr_storage dummy_local_addr;  // receiver (나)

//##########################FUNCTION###############################
static void APP_ControlRxCallback(V2xMsgType v2xMsgType, U8 *p_rxBuf){}

int customQuicCallback(picoquic_cnx_t *cnx, uint64_t stream_id, uint8_t *bytes, size_t length,
                         picoquic_call_back_event_t event, void *callback_ctx, void *stream_ctx) {
    if (event == picoquic_callback_stream_data && length > 0) {
        // 버퍼에 누적
        g_recvBuf.insert(g_recvBuf.end(), bytes, bytes + length);
        //test
        if (g_recvBuf.size() > 10000) {
            printf("[BUF] 버퍼 크기: %zu bytes\n", g_recvBuf.size());
        }
        //test

        // 완성된 RTP 패킷이 있으면 꺼내서 push
        while (g_recvBuf.size() >= 4) {
            uint32_t net_len;
            memcpy(&net_len, g_recvBuf.data(), 4);
            uint32_t pktLen = ntohl(net_len);

            if (pktLen > 65535) {  // 비정상 값 방어
                printf("[Receiver] 비정상 패킷 길이: %u, 버퍼 초기화\n", pktLen);
                g_recvBuf.clear();
                break;
            }

            if (g_recvBuf.size() < 4 + pktLen) break;  // 아직 덜 왔음

            // 완성된 RTP 패킷 → GStreamer로 push
            GstBuffer* gstbuf = gst_buffer_new_allocate(nullptr, pktLen, nullptr);
            GstMapInfo map;
            gst_buffer_map(gstbuf, &map, GST_MAP_WRITE);
            memcpy(map.data, g_recvBuf.data() + 4, pktLen);
            gst_buffer_unmap(gstbuf, &map);

            if (!g_appsrc || gst_app_src_push_buffer(g_appsrc, gstbuf) != GST_FLOW_OK) {
                gst_buffer_unref(gstbuf);
            }

            // 처리한 만큼 버퍼에서 제거
            g_recvBuf.erase(g_recvBuf.begin(), g_recvBuf.begin() + 4 + pktLen);
        }
    }
    return 0;
}

std::optional<V2xMsgRegi> initSirius(){
    if (EFOS_SetControlRxCallback((ControlRxCallbackFunc*)APP_ControlRxCallback) < EFOS_RESULT_SUCCESS) {
        printf("Failed to register control rx callback\n");
        return std::nullopt;
    }
    if (EFOS_InitializeV2x((char*)RECEIVER_SIRIUS_IP) < EFOS_RESULT_SUCCESS) {
        printf("Failed to initialize\n");
        return std::nullopt;
    }
    printf("Connected to SIRIUS with IP (%s)\n", RECEIVER_SIRIUS_IP);

    // ---------------------- V2X 채널 설정 ----------------------
    V2xMsgReq v2xmsgreq;
    V2xMsgRegi v2xmsgregi = {
        .v2xServiceId   = EFOS_V2X_SERVICE_ID_BCAST_PQI_TP,
        .v2xMsgFamilyId = EFOS_V2X_MSG_FAMILY_IEEE1609,
        .commMode       = EFOS_V2X_COMM_BROADCAST,
        .scheduleType   = EFOS_SCHEDULE_TYPE_EVENT,
        .txInterval     = 100.0,
        .mcsConf        = {0xFF, 0xFF},
    };
    if (EFOS_EstablishV2xChannel(&v2xmsgreq.v2xChannelId, &v2xmsgregi) < EFOS_RESULT_SUCCESS) {
        printf("Failed EstablishV2XChannel\n");
        return std::nullopt;
    }
    g_channelID = v2xmsgreq.v2xChannelId;
    return v2xmsgregi;
}

void initGStreamerApp(GstElement* pipeline){

    g_appsrc  = GST_APP_SRC(gst_bin_get_by_name(GST_BIN(pipeline), "src"));
    g_appsink = GST_APP_SINK(gst_bin_get_by_name(GST_BIN(pipeline), "sink"));

    gst_element_set_state(pipeline, GST_STATE_PLAYING);

}

GstElement* initGStreamerPipeline(){
std::string gst_pipeline =
    "appsrc name=src is-live=true format=time do-timestamp=true ! "
    "application/x-rtp,media=video,clock-rate=90000,encoding-name=H264,payload=96 ! "
    "rtpjitterbuffer latency=200 ! "
    "rtph264depay ! h264parse config-interval=-1 ! "
    "video/x-h264,stream-format=avc,alignment=au ! "
    "kvssink "
        "stream-name=\"" + std::string(STREAM_NAME) + "\" "
        "aws-region=\"" + std::string(AWS_REGION) + "\" "
        "storage-size=512 ";

        
    GError* err = nullptr;
    GstElement* pipeline = gst_parse_launch(gst_pipeline.c_str(), &err);
    if (!pipeline || err) {
        printf("파이프라인 생성 실패: %s\n", err ? err->message : "unknown");
        return nullptr;
    }
    return pipeline;
}

picoquic_quic_t* initQuic(){
    memset(&dummy_remote_addr, 0, sizeof(dummy_remote_addr));
    memset(&dummy_local_addr, 0, sizeof(dummy_local_addr));

    struct sockaddr_in* remote_sin = (struct sockaddr_in*)&dummy_remote_addr;
    remote_sin->sin_family = AF_INET;
    inet_pton(AF_INET, SENDER_SIRIUS_IP, &remote_sin->sin_addr); // sender IP
    remote_sin->sin_port = htons(12345);

    // local = receiver (나 자신)
    struct sockaddr_in* local_sin = (struct sockaddr_in*)&dummy_local_addr;
    local_sin->sin_family = AF_INET;
    inet_pton(AF_INET, RECEIVER_SIRIUS_IP, &local_sin->sin_addr); // receiver IP
    local_sin->sin_port = htons(4433);

    // 3. Picoquic 엔진 초기화 (서버 역할)
    uint64_t current_time = picoquic_current_time();
    picoquic_quic_t *quic = picoquic_create(8, "certs/cert.pem", "certs/key.pem", NULL, "v2x_test",
                                            customQuicCallback, NULL, // 서버는 여기서 콜백 등록
                                            NULL, NULL, NULL,
                                            current_time, NULL, NULL, NULL, 0);
    return quic;
}

//##########################THREAD###############################
void inputThread() {
    char c;
    printf("종료하려면 q or Q를 터미널에 입력\n");
    while (keepRunning) {
        std::cin >> c;
        if (c == 'q' || c == 'Q') {
            keepRunning = false;
            break;
        }
    }
}

void v2xToGstreamerThread(picoquic_quic_t* quic) {
    uint8_t rxBuf[9100] = { 0 };

    printf("V2X -> GStreamer 포워딩 스레드 시작...\n");

    while (keepRunning) {
        V2xMsgType v2xMsgType = EFOS_RecvV2xMsg(rxBuf);
        V2xMsg* p_v2xMsg = (V2xMsg*)rxBuf;
        // printf("[LOG] v2x 수신 %d\n",p_v2xMsg->length);

        if(p_v2xMsg->length==0){
            continue;
        }
        // printf("수진완료\n");

        if (p_v2xMsg->length <= 34) {
            printf("[Receiver] 무시됨: 패킷 길이가 너무 짧습니다. (길이: %d)\n", p_v2xMsg->length);
            continue;
        }

        std::lock_guard<std::mutex> lock(quic_mutex);
        uint64_t current_time = picoquic_current_time();

        // 서버 입장에서 패킷 수신
        picoquic_incoming_packet(quic,
                                 p_v2xMsg->data + 34, //34는 시리우스 기본으로 붙는 바이트
                                 p_v2xMsg->length - 34,
                                 (struct sockaddr*)&dummy_remote_addr, // from: sender
                                 (struct sockaddr*)&dummy_local_addr,  // to: receiver(나)
                                 0, // if_index
                                 0, // ecn_bits
                                 current_time);
       
        // printf("[LOG] V2X 패킷 수신 -> QUIC 엔진 전달: %d bytes\n", p_v2xMsg->length);

    }

    gst_app_src_end_of_stream(g_appsrc);
    printf("V2X -> GStreamer 포워딩 스레드 종료\n");
}

void quicResponseThread(picoquic_quic_t* quic) {
    uint8_t sendBuf[1536];
    
    printf("QUIC 응답 전송 스레드 시작...\n");

    while (keepRunning) {
        size_t sendLen = 0;
        struct sockaddr_storage peer_addr, local_addr;
        int if_index = 0;
        picoquic_connection_id_t log_cid;
        picoquic_cnx_t* last_cnx = nullptr;
        size_t send_msg_size = 0;

        {
            std::lock_guard<std::mutex> lock(quic_mutex);
            uint64_t current_time = picoquic_current_time();

            int ret = picoquic_prepare_next_packet_ex(
                quic, current_time,
                sendBuf, sizeof(sendBuf), &sendLen,
                &peer_addr, &local_addr,
                &if_index, &log_cid, &last_cnx, &send_msg_size
            );

            if (ret != 0) {
                printf("[QUIC Response] prepare 에러: %d\n", ret);
                continue;
            }
        }

        if (sendLen > 0) {
            std::vector<uint8_t> pkt(sizeof(V2xMsgReq) + sendLen);
            V2xMsgReq* p_req = reinterpret_cast<V2xMsgReq*>(pkt.data());
            p_req->length = sendLen;
            p_req->v2xChannelId = g_channelID;
            memcpy(p_req->data, sendBuf, sendLen);

            EFOS_SendV2xMsg(p_req);
            // printf("[LOG] QUIC 응답 전송: %zu bytes\n", sendLen);
            // free(p_req);
        } else {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

    printf("QUIC 응답 전송 스레드 종료\n");
}

//##########################MAIN###############################
int main(int argc, char**argv){
    gst_init(&argc, &argv);

    // ---------------------- INIT V2X ----------------------
    auto v2xmsgregi = initSirius();
    if (!v2xmsgregi){
        printf("initialize ERROR \n ");
        return -1;
    }
    // ---------------------- INIT UU ----------------------

    // ---------------------- INIT GSTREAMER ----------------------
    GstElement* pipeline = initGStreamerPipeline();
    if(!pipeline){
        printf("GStreamer 파이프라인 초기화 실패\n");
        return -1;
    }

    initGStreamerApp(pipeline);

    // ---------------------- INIT QUIC ----------------------
    picoquic_quic_t *quic = initQuic();

    // ---------------------- INIT THREAD ----------------------
    std::thread input_thread(inputThread);
    std::thread relay_thread(v2xToGstreamerThread, quic);
    std::thread response_thread(quicResponseThread, quic);

    // ---------------------- MAIN LOOP ----------------------
    while (keepRunning) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    // ---------------------- FINAL ----------------------
    relay_thread.join();
    input_thread.join();
    response_thread.join();

    gst_object_unref(g_appsrc);
    gst_object_unref(g_appsink);
    gst_element_set_state(pipeline, GST_STATE_NULL);
    gst_object_unref(pipeline);

    picoquic_free(quic);

    cv::destroyAllWindows();

    EFOS_TerminateV2x(v2xmsgregi->v2xServiceId, v2xmsgregi->commMode);
    return 0;

}