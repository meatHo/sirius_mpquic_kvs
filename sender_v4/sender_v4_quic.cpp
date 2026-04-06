#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <iostream>
#include <thread>
#include <atomic>
#include <optional>

#include <opencv2/opencv.hpp>
//g++ -o sender_v3 sender_v3.cpp $(pkg-config --cflags --libs opencv4 gstreamer-1.0 gstreamer-app-1.0) -lefosv2xsdk -lpthread -fPIC
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
static picoquic_cnx_t* g_cnx = nullptr;

static GstAppSrc* g_appsrc = nullptr;
static GstAppSink* g_appsink = nullptr;

#define RECEIVER_SIRIUS_IP      "192.168.70.101"
#define SENDER_SIRIUS_IP        "192.168.70.102"

// sender IP: 192.168.70.102
// receiver IP: 192.168.70.101
struct sockaddr_storage dummy_remote_addr; // receiver (나)
struct sockaddr_storage dummy_local_addr;  // sender (상대방)


#define CAMERA_WIDTH    640
#define CAMERA_HEIGHT   480
#define CAMERA_FRAME    30


//##########################FUNCTION###############################
static void APP_ControlRxCallback(V2xMsgType v2xMsgType, U8 *p_rxBuf){}

int i=0;

// appsink에서 RTP 패킷이 나올 때마다 호출되는 콜백 -> QUIC에 추가
GstFlowReturn onNewSample(GstAppSink* appsink, gpointer) {
    GstSample* sample = gst_app_sink_pull_sample(appsink);
    if (!sample) return GST_FLOW_ERROR;

    GstBuffer* buffer = gst_sample_get_buffer(sample);
    GstMapInfo map;
    if (gst_buffer_map(buffer, &map, GST_MAP_READ)) {
        uint32_t len = (uint32_t)map.size;
        uint32_t net_len = htonl(len);  // 네트워크 바이트 오더

        std::lock_guard<std::mutex> lock(quic_mutex);
        picoquic_add_to_stream(g_cnx, 4, (const uint8_t*)&net_len, 4, 0);  // 길이 먼저
        picoquic_add_to_stream(g_cnx, 4, map.data, map.size, 0);           // RTP 데이터
        gst_buffer_unmap(buffer, &map);
    }
    gst_sample_unref(sample);
    // for(;i<10;i++){
    //     printf("[LOG] gstreamer패킷 quic에 추가\n");
    // }
    return GST_FLOW_OK;
}


std::optional<V2xMsgRegi> initSirius(){
    if (EFOS_SetControlRxCallback((ControlRxCallbackFunc*)APP_ControlRxCallback) < EFOS_RESULT_SUCCESS) {
        printf("Failed to register control rx callback\n");
        return std::nullopt;
    }
    if (EFOS_InitializeV2x((char*)SENDER_SIRIUS_IP) < EFOS_RESULT_SUCCESS) {
        printf("Failed to initialize\n");
        return std::nullopt;
    }
    printf("Connected to SIRIUS with IP (%s)\n", SENDER_SIRIUS_IP);

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

    // appsink 콜백 등록
    GstAppSinkCallbacks callbacks = {};
    callbacks.new_sample = onNewSample;
    gst_app_sink_set_callbacks(g_appsink, &callbacks, nullptr, nullptr);

    // appsrc 캡 설정
    GstCaps* caps = gst_caps_new_simple("video/x-raw",
        "format", G_TYPE_STRING, "BGR",
        "width",  G_TYPE_INT,    CAMERA_WIDTH,
        "height", G_TYPE_INT,    CAMERA_HEIGHT,
        "framerate", GST_TYPE_FRACTION, CAMERA_FRAME, 1,
        nullptr);
    gst_app_src_set_caps(g_appsrc, caps);
    gst_caps_unref(caps);

    gst_element_set_state(pipeline, GST_STATE_PLAYING);

}


GstElement* initGStreamerPipeline(){
    std::string gst_pipeline =
        "appsrc name=src is-live=true format=time ! "
        "videoconvert ! "
        "x264enc tune=zerolatency bitrate=10000 speed-preset=ultrafast key-int-max=10 rc-lookahead=0 ! "
        "rtph264pay config-interval=1 mtu=1400 ! "
        "appsink name=sink sync=false drop=false";

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
    inet_pton(AF_INET, RECEIVER_SIRIUS_IP, &remote_sin->sin_addr); // receiver IP
    remote_sin->sin_port = htons(4433);

    // local = sender (나 자신)
    struct sockaddr_in* local_sin = (struct sockaddr_in*)&dummy_local_addr;
    local_sin->sin_family = AF_INET;
    inet_pton(AF_INET, SENDER_SIRIUS_IP, &local_sin->sin_addr); // sender IP
    local_sin->sin_port = htons(12345);

    // 3. Picoquic 엔진 초기화 (클라이언트 역할)
    uint64_t current_time = picoquic_current_time();
    picoquic_quic_t *quic = picoquic_create(8, "certs/cert.pem", "certs/key.pem", NULL, "v2x_test",
                                            NULL, NULL, NULL, NULL, NULL,
                                            current_time, NULL, NULL, NULL, 0);
    picoquic_set_default_congestion_algorithm_by_name(quic, "bbr");  // BBR로 변경
    picoquic_set_cwin_max(quic, 10000000);  // 10MB 상한
    // 클라이언트 커넥션 생성
    picoquic_cnx_t *cnx = picoquic_create_cnx(quic,
                                              picoquic_null_connection_id,
                                              picoquic_null_connection_id,
                                              (struct sockaddr*)&dummy_remote_addr,
                                              current_time, 0, "test.sni", "v2x_test", 1);

    picoquic_set_callback(cnx, [](picoquic_cnx_t*, uint64_t, uint8_t*, size_t,
        picoquic_call_back_event_t, void*, void*) -> int { return 0; }, NULL);

    picoquic_start_client_cnx(cnx);

    g_cnx = cnx;

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
        }
    }
}

void quicSendThread(picoquic_quic_t* quic) {
    uint8_t sendBuf[1536];
    printf("QUIC 송신 스레드 시작...\n");

    while (keepRunning) {
        size_t sendLen = 0;
        struct sockaddr_storage peer_addr, local_addr;
        int if_index = 0;
        picoquic_connection_id_t log_cid;
        picoquic_cnx_t* last_cnx = nullptr;
        size_t send_msg_size = 0;

        {
            std::lock_guard<std::mutex> lock(quic_mutex);

            // 커넥션 상태 확인
            // picoquic_state_enum state = picoquic_get_cnx_state(g_cnx);
            // if((int)state!=1){
            //     printf("[QUIC] 커넥션 상태: %d\n", (int)state);

            // }

            int ret = picoquic_prepare_next_packet_ex(
                quic, picoquic_current_time(),
                sendBuf, sizeof(sendBuf), &sendLen,
                &peer_addr, &local_addr,
                &if_index, &log_cid, &last_cnx, &send_msg_size
            );
            if (ret != 0) {
                printf("[QUIC] prepare 에러: %d\n", ret);
                continue;
            }
        }
        // static int sendCount = 0;
        if (sendLen > 0) {
            //test
            // sendCount++;
            // if (sendCount % 100 == 0) {
            //         uint64_t rtt = picoquic_get_rtt(g_cnx);
            //         uint64_t cwin = picoquic_get_cwin(g_cnx);
            //         printf("[QUIC] RTT=%llu us, CWIN=%llu bytes\n",
            //             (unsigned long long)rtt, (unsigned long long)cwin);
            // }
            //test
            std::vector<uint8_t> pkt(sizeof(V2xMsgReq) + sendLen);
            V2xMsgReq* p_req = reinterpret_cast<V2xMsgReq*>(pkt.data());
            p_req->v2xChannelId = g_channelID;
            p_req->length = (U16)sendLen;
            memcpy(p_req->data, sendBuf, sendLen);
            EFOS_SendV2xMsg(p_req);
            // printf("[LOG] v2x 전송 %d\n", (int)sendLen);
        } else {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }
    printf("QUIC 송신 스레드 종료\n");
}

void v2xRecvThread(picoquic_quic_t* quic) {
    uint8_t rxBuf[9100] = { 0 };
    printf("Sender V2X 수신 스레드 시작...\n");

    while (keepRunning) {
        V2xMsgType v2xMsgType = EFOS_RecvV2xMsg(rxBuf);
        V2xMsg* p_v2xMsg = (V2xMsg*)rxBuf;

        if (p_v2xMsg->length == 0) continue;


        if (p_v2xMsg->length <= 34) continue;

        int quicLen = p_v2xMsg->length - 34;

        // 자기가 보낸 패킷 무시 (sender의 QUIC 패킷 크기는 1252)
        // receiver 응답은 50바이트이므로 구분 가능
        // printf("[RECV] 수신 길이: %d QUIC 페이로드: %d bytes\n", p_v2xMsg->length, quicLen);

        std::lock_guard<std::mutex> lock(quic_mutex);
        picoquic_incoming_packet(quic,
            p_v2xMsg->data + 34,
            quicLen,
            (struct sockaddr*)&dummy_remote_addr,
            (struct sockaddr*)&dummy_local_addr,
            0, 0, picoquic_current_time());
    }
    printf("Sender V2X 수신 스레드 종료\n");
}

//##########################MAIN###############################
int main(int argc, char** argv) {
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

    // ---------------------- INIT OPENCV ----------------------
    cv::VideoCapture cap(0);
    cap.set(cv::CAP_PROP_FRAME_WIDTH,  CAMERA_WIDTH);
    cap.set(cv::CAP_PROP_FRAME_HEIGHT, CAMERA_HEIGHT);
    cap.set(cv::CAP_PROP_FPS,          CAMERA_FRAME);
    cap.set(cv::CAP_PROP_BUFFERSIZE,   1);
    if (!cap.isOpened()) {
        printf("카메라 열기 실패\n");
        return -1;
    }


    // ---------------------- INIT THREAD ----------------------
    std::thread input_thread(inputThread);
    std::thread send_thread(quicSendThread,quic);
    std::thread recv_thread(v2xRecvThread, quic);

    // ---------------------- MAIN LOOP ----------------------
    cv::Mat frame;
    GstClockTime timestamp = 0;
    const GstClockTime duration = GST_SECOND / CAMERA_FRAME;

    // int frameCount=0;
    while (keepRunning) {
        cap >> frame;
        if (frame.empty()) {
            continue;
        }

        //test 15프레임 설정
        // frameCount++;
        // if (frameCount % 2 != 0) continue;

        // //test 대략적 레이턴시 측정
        // auto now = std::chrono::system_clock::now();
        // auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        //     now.time_since_epoch()).count() % 100000;
        // char timeStr[32];
        // snprintf(timeStr, sizeof(timeStr), "%05lld", (long long)ms);
        // cv::putText(frame, timeStr, cv::Point(30, 60),
        //     cv::FONT_HERSHEY_SIMPLEX, 2.0, cv::Scalar(0, 255, 0), 3);
        // //test

        cv::imshow("V2X Video Stream Sender", frame);
        if (cv::waitKey(1) == 'q') { keepRunning = false; break; }

        // OpenCV Mat → GstBuffer → appsrc로 push
        gsize buf_size = frame.total() * frame.elemSize();
        GstBuffer* buffer = gst_buffer_new_allocate(nullptr, buf_size, nullptr);

        GstMapInfo map;
        gst_buffer_map(buffer, &map, GST_MAP_WRITE);
        memcpy(map.data, frame.data, buf_size);
        gst_buffer_unmap(buffer, &map);

        GST_BUFFER_PTS(buffer)      = timestamp;
        GST_BUFFER_DURATION(buffer) = duration;
        timestamp += duration;

        gst_app_src_push_buffer(g_appsrc, buffer); // buffer 소유권이 GStreamer로 넘어감
    }

    // ---------------------- 정리 ----------------------
    gst_app_src_end_of_stream(g_appsrc);
    gst_element_set_state(pipeline, GST_STATE_NULL);
    gst_object_unref(g_appsrc);
    gst_object_unref(g_appsink);
    gst_object_unref(pipeline);

    input_thread.join();
    send_thread.join();
    recv_thread.join();

    picoquic_free(quic);

    cap.release();
    cv::destroyAllWindows();
    EFOS_TerminateV2x(v2xmsgregi->v2xServiceId, v2xmsgregi->commMode);

    return 0;
}