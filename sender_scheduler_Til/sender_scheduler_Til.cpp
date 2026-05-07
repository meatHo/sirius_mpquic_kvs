#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <iostream>
#include <thread>
#include <atomic>
#include <optional>
#include <sstream>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <set>

#include <opencv2/opencv.hpp>
#include <opencv2/dnn.hpp>

extern "C"{
    #include "scheduler.h"
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

//#####################GLOBAL Variable############################
std::atomic<bool> keepRunning(true);
static U16 g_channelID = 0;

// ── GPS 스냅샷 (sendGpsData → sendFrameMeta 공유) ────────────
struct GpsSnapshot {
    double lat      = 0.0;
    double lon      = 0.0;
    float  speed_mps = 0.0f;
};
static GpsSnapshot g_gps_snap;
static std::mutex  g_gps_mtx;

// ── FrameMeta 패킷 구조체 (37 바이트, packed) ─────────────────
#pragma pack(push, 1)
struct FrameMetaPacket {
    uint64_t frame_id;      // 8  바이트
    uint64_t t_tx_us;       // 8  바이트  (gettimeofday, 마이크로초)
    uint8_t  scheduler_id;  // 1  바이트
    float    speed_mps;     // 4  바이트
    double   lat;           // 8  바이트
    double   lon;           // 8  바이트
    // 합계: 37 바이트
};
#pragma pack(pop)

std::mutex quic_mutex;
static picoquic_cnx_t* g_cnx = nullptr;

static int g_selected_scheduler = SCHEDULER_MIN_RTT;
static GstAppSrc* g_appsrc = nullptr;
static GstAppSink* g_appsink = nullptr;

// ── TiL sender metrics CSV ────────────────────────────────────
static FILE* g_sender_csv_fp = nullptr;
static std::mutex g_sender_csv_mtx;
static int s_adv_sent_expected = 0;
static int s_adv_recv_count   = 0;
static int s_steer_count      = 0;

#define ADVISORY_STREAM_ID  13
static float g_w_til = 0.5f;
static std::mutex g_wtil_mtx;

static std::vector<uint8_t> g_advisory_buf;

// ===================== 패킷 타입 정의 =====================
#define PKT_TYPE_RTP        0x01
#define PKT_TYPE_YOLO       0x02
#define PKT_TYPE_GPS        0x03
#define PKT_TYPE_FRAME_META 0x04
#define PKT_TYPE_ADV_ACK    0x06

// ===================== 네트워크 설정 =====================
#define PC5_SENDER_IP       "192.168.1.11"
#define PC5_RECEIVER_IP     "10.254.52.18"

#define UU_RECEIVER_IP      "202.30.29.202"
#define UU_RECEIVER_PORT    4433
#define UU_SENDER_IP        "192.168.10.102"
#define UU_SENDER_PORT      0
#define UU_INTERFACE        "enp3s0"

struct sockaddr_storage pc5_remote_addr;
struct sockaddr_storage pc5_local_addr;
struct sockaddr_storage uu_remote_addr;
struct sockaddr_storage uu_local_addr;
static int g_uu_sock = -1;

#define CAMERA_WIDTH    640
#define CAMERA_HEIGHT   480
#define CAMERA_FRAME    30

static std::atomic<bool> g_uu_handshake_done(false);
static std::atomic<bool> g_pc5_path_added(false);
// 콜백에서 세팅 → multipathProbeThread가 감지해서 probe
static std::atomic<bool> g_pc5_need_probe(false);

static std::atomic<uint64_t> g_tx_count_uu{0};
static std::atomic<uint64_t> g_tx_count_pc5{0};

// ── TwinSteer 상태 ────────────────────────────────────────────
static float    g_w_adv            = 0.5f;
static uint64_t g_t_adv_rx_us_last = 0;
static std::mutex g_wadv_mtx;

// TwinSteer 파라미터
static constexpr float TS_LAMBDA_L = 0.30f;
static constexpr float TS_EPSILON  = 0.30f;
static constexpr float TS_THETA_O  = 0.30f;
static constexpr float TS_H0       = 5.0f;

// ===================== YOLO 설정 =====================
#define YOLO_MODEL_ONNX "/home/icons-linux/Documents/yolo/yolov8n-oiv7.onnx"
#define YOLO_INPUT_SIZE 640
#define YOLO_CONF_THRESHOLD 0.25f
#define YOLO_NMS_THRESHOLD  0.45f

static const char* OIV7_CLASSES[] = {
    "Accordion","Adhesive tape","Aircraft","Airplane","Alarm clock","Alpaca","Ambulance","Animal","Ant","Antelope",
    "Apple","Armadillo","Artichoke","Auto part","Axe","Backpack","Bagel","Baked goods","Balance beam","Ball",
    "Balloon","Banana","Band-aid","Banjo","Barge","Barrel","Baseball bat","Baseball glove","Bat (Animal)","Bathroom accessory",
    "Bathroom cabinet","Bathtub","Beaker","Bear","Bed","Bee","Beehive","Beer","Beetle","Bell pepper",
    "Belt","Bench","Bicycle","Bicycle helmet","Bicycle wheel","Bidet","Billboard","Billiard table","Binoculars","Bird",
    "Blender","Blue jay","Boat","Bomb","Book","Bookcase","Boot","Bottle","Bottle opener","Bow and arrow",
    "Bowl","Bowling equipment","Box","Boy","Brassiere","Bread","Briefcase","Broccoli","Bronze sculpture","Brown bear",
    "Building","Bull","Burrito","Bus","Bust","Butterfly","Cabbage","Cabinetry","Cake","Cake stand",
    "Calculator","Camel","Camera","Can opener","Canary","Candle","Candy","Cannon","Canoe","Cantaloupe",
    "Car","Carnivore","Carrot","Cart","Cassette deck","Castle","Cat","Cat furniture","Caterpillar","Cattle",
    "Ceiling fan","Cello","Centipede","Chainsaw","Chair","Cheese","Cheetah","Chest of drawers","Chicken","Chime",
    "Chisel","Chopsticks","Christmas tree","Clock","Closet","Clothing","Coat","Cocktail","Cocktail shaker","Coconut",
    "Coffee","Coffee cup","Coffee table","Coffeemaker","Coin","Common fig","Common sunflower","Computer keyboard","Computer monitor","Computer mouse",
    "Container","Convenience store","Cookie","Cooking spray","Corded phone","Cosmetics","Couch","Countertop","Cowboy hat","Crab",
    "Cream","Cricket ball","Crocodile","Croissant","Crown","Crutch","Cucumber","Cupboard","Curtain","Cutting board",
    "Dagger","Dairy Product","Deer","Desk","Dessert","Diaper","Dice","Digital clock","Dinosaur","Dishwasher",
    "Dog","Dog bed","Doll","Dolphin","Door","Door handle","Doughnut","Dragonfly","Drawer","Dress",
    "Drill (Tool)","Drink","Drinking straw","Drum","Duck","Dumbbell","Eagle","Earrings","Egg (Food)","Elephant",
    "Envelope","Eraser","Face powder","Facial tissue holder","Falcon","Fashion accessory","Fast food","Fax","Fedora","Filing cabinet",
    "Fire hydrant","Fireplace","Fish","Flag","Flashlight","Flower","Flowerpot","Flute","Flying disc","Food",
    "Food processor","Football","Football helmet","Footwear","Fork","Fountain","Fox","French fries","French horn","Frog",
    "Fruit","Frying pan","Furniture","Garden Asparagus","Gas stove","Giraffe","Girl","Glasses","Glove","Goat",
    "Goggles","Goldfish","Golf ball","Golf cart","Gondola","Goose","Grape","Grapefruit","Grinder","Guacamole",
    "Guitar","Hair dryer","Hair spray","Hamburger","Hammer","Hamster","Hand dryer","Handbag","Handgun","Harbor seal",
    "Harmonica","Harp","Harpsichord","Hat","Headphones","Heater","Hedgehog","Helicopter","Helmet","High heels",
    "Hiking equipment","Hippopotamus","Home appliance","Honeycomb","Horizontal bar","Horse","Hot dog","House","Houseplant","Human arm",
    "Human beard","Human body","Human ear","Human eye","Human face","Human foot","Human hair","Human hand","Human head","Human leg",
    "Human mouth","Human nose","Humidifier","Ice cream","Indoor rower","Infant bed","Insect","Invertebrate","Ipod","Isopod",
    "Jacket","Jacuzzi","Jaguar (Animal)","Jeans","Jellyfish","Jet ski","Jug","Juice","Kangaroo","Kettle",
    "Kitchen & dining room table","Kitchen appliance","Kitchen knife","Kitchen utensil","Kitchenware","Kite","Knife","Koala","Ladder","Ladle",
    "Ladybug","Lamp","Land vehicle","Lantern","Laptop","Lavender (Plant)","Lemon","Leopard","Light bulb","Light switch",
    "Lighthouse","Lily","Limousine","Lion","Lipstick","Lizard","Lobster","Loveseat","Luggage and bags","Lynx",
    "Magpie","Mammal","Man","Mango","Maple","Maracas","Marine invertebrates","Marine mammal","Measuring cup","Mechanical fan",
    "Medical equipment","Microphone","Microwave oven","Milk","Miniskirt","Mirror","Missile","Mixer","Mixing bowl","Mobile phone",
    "Monkey","Moths and butterflies","Motorcycle","Mouse","Muffin","Mug","Mule","Mushroom","Musical instrument","Musical keyboard",
    "Nail (Construction)","Necklace","Nightstand","Oboe","Office building","Office supplies","Orange","Organ (Musical Instrument)","Ostrich","Otter",
    "Oven","Owl","Oyster","Paddle","Palm tree","Pancake","Panda","Paper cutter","Paper towel","Parachute",
    "Parking meter","Parrot","Pasta","Pastry","Peach","Pear","Pen","Pencil case","Pencil sharpener","Penguin",
    "Perfume","Person","Personal care","Personal flotation device","Piano","Picnic basket","Picture frame","Pig","Pillow","Pineapple",
    "Pitcher (Container)","Pizza","Pizza cutter","Plant","Plastic bag","Plate","Platter","Plumbing fixture","Polar bear","Pomegranate",
    "Popcorn","Porch","Porcupine","Poster","Potato","Power plugs and sockets","Pressure cooker","Pretzel","Printer","Pumpkin",
    "Punching bag","Rabbit","Raccoon","Racket","Radish","Ratchet (Device)","Raven","Rays and skates","Red panda","Refrigerator",
    "Remote control","Reptile","Rhinoceros","Rifle","Ring binder","Rocket","Roller skates","Rose","Rugby ball","Ruler",
    "Salad","Salt and pepper shakers","Sandal","Sandwich","Saucer","Saxophone","Scale","Scarf","Scissors","Scoreboard",
    "Scorpion","Screwdriver","Sculpture","Sea lion","Sea turtle","Seafood","Seahorse","Seat belt","Segway","Serving tray",
    "Sewing machine","Shark","Sheep","Shelf","Shellfish","Shirt","Shorts","Shotgun","Shower","Shrimp",
    "Sink","Skateboard","Ski","Skirt","Skull","Skunk","Skyscraper","Slow cooker","Snack","Snail",
    "Snake","Snowboard","Snowman","Snowmobile","Snowplow","Soap dispenser","Sock","Sofa bed","Sombrero","Sparrow",
    "Spatula","Spice rack","Spider","Spoon","Sports equipment","Sports uniform","Squash (Plant)","Squid","Squirrel","Stairs",
    "Stapler","Starfish","Stationary bicycle","Stethoscope","Stool","Stop sign","Strawberry","Street light","Stretcher","Studio couch",
    "Submarine","Submarine sandwich","Suit","Suitcase","Sun hat","Sunglasses","Surfboard","Sushi","Swan","Swim cap",
    "Swimming pool","Swimwear","Sword","Syringe","Table","Table tennis racket","Tablet computer","Tableware","Taco","Tank",
    "Tap","Tart","Taxi","Tea","Teapot","Teddy bear","Telephone","Television","Tennis ball","Tennis racket",
    "Tent","Tiara","Tick","Tie","Tiger","Tin can","Tire","Toaster","Toilet","Toilet paper",
    "Tomato","Tool","Toothbrush","Torch","Tortoise","Towel","Tower","Toy","Traffic light","Traffic sign",
    "Train","Training bench","Treadmill","Tree","Tree house","Tripod","Trombone","Trousers","Truck","Trumpet",
    "Turkey","Turtle","Umbrella","Unicycle","Van","Vase","Vegetable","Vehicle","Vehicle registration plate","Violin",
    "Volleyball (Ball)","Waffle","Waffle iron","Wall clock","Wardrobe","Washing machine","Waste container","Watch","Watercraft","Watermelon",
    "Weapon","Whale","Wheel","Wheelchair","Whisk","Whiteboard","Willow","Window","Window blind","Wine",
    "Wine glass","Wine rack","Winter melon","Wok","Woman","Wood-burning stove","Woodpecker","Worm","Wrench","Zebra","Zucchini"
};
static const int NUM_CLASSES = sizeof(OIV7_CLASSES) / sizeof(OIV7_CLASSES[0]);  // 601

static const std::set<int> INTERESTED_CLASSES = {
    70, 95, 257, 310, 354, 466, 546, 554,   // 건물/구조물
    73, 312, 519, 550, 558, 564, 6,          // 큰 차량
    90, 522,                                  // 일반 차량
    553,                                      // 나무
    46, 497, 205,                             // 기타 구조물
};

cv::dnn::Net g_yoloNet;

struct Detection {
    int classId;
    float confidence;
    cv::Rect bbox;
};

struct FrameJob {
    cv::Mat frame;
    uint64_t frameId;
};

static std::mutex g_yoloQueueMtx;
static std::condition_variable g_yoloQueueCv;
static std::queue<FrameJob> g_yoloQueue;
static const size_t YOLO_QUEUE_MAX = 1;

static std::mutex g_detMtx;
static std::vector<Detection> g_lastDetections;
static std::atomic<uint64_t> g_lastDetFrameId(0);


//##########################FUNCTION###############################
static void APP_ControlRxCallback(V2xMsgType v2xMsgType, U8 *p_rxBuf){}

std::vector<Detection> postprocessYolo(const std::vector<cv::Mat>& outputs,
                                        int imgW, int imgH) {
    std::vector<int> classIds;
    std::vector<float> confidences;
    std::vector<cv::Rect> boxes;

    cv::Mat output = outputs[0];

    static bool shape_logged = false;
    if (!shape_logged) {
        printf("[YOLO] 출력 dims=%d, shape=[", output.dims);
        for (int i = 0; i < output.dims; i++)
            printf("%d%s", output.size[i], (i < output.dims - 1) ? "," : "");
        printf("]\n");
        printf("[YOLO] NUM_CLASSES=%d\n", NUM_CLASSES);
        shape_logged = true;
    }

    if (output.dims != 3) return {};

    int cols = output.size[1];
    int rows = output.size[2];

    if (cols < 4 + NUM_CLASSES) {
        printf("[YOLO] 예상치 못한 출력 shape (cols=%d, expected >= %d)\n", cols, 4 + NUM_CLASSES);
        return {};
    }

    cv::Mat reshaped(cols, rows, CV_32F, output.ptr<float>());
    cv::Mat det = reshaped.t();

    float scaleX = (float)imgW / YOLO_INPUT_SIZE;
    float scaleY = (float)imgH / YOLO_INPUT_SIZE;

    for (int i = 0; i < rows; i++) {
        const float* row = det.ptr<float>(i);
        float cx = row[0], cy = row[1], w = row[2], h = row[3];

        float maxConf = 0; int maxIdx = 0;
        for (int c = 0; c < NUM_CLASSES; c++) {
            if (row[4 + c] > maxConf) { maxConf = row[4 + c]; maxIdx = c; }
        }
        if (maxConf < YOLO_CONF_THRESHOLD) continue;
        if (!INTERESTED_CLASSES.empty() &&
            INTERESTED_CLASSES.find(maxIdx) == INTERESTED_CLASSES.end()) continue;

        classIds.push_back(maxIdx);
        confidences.push_back(maxConf);
        boxes.push_back(cv::Rect(
            (int)((cx - w/2) * scaleX), (int)((cy - h/2) * scaleY),
            (int)(w * scaleX), (int)(h * scaleY)));
    }

    std::vector<int> indices;
    cv::dnn::NMSBoxes(boxes, confidences, YOLO_CONF_THRESHOLD, YOLO_NMS_THRESHOLD, indices);

    std::vector<Detection> results;
    for (int idx : indices) {
        Detection d;
        d.classId = classIds[idx]; d.confidence = confidences[idx]; d.bbox = boxes[idx];
        results.push_back(d);
    }
    return results;
}

std::string detectionsToJson(const std::vector<Detection>& dets, uint64_t frameId) {
    std::ostringstream oss;
    oss << "{\"f\":" << frameId << ",\"d\":[";
    for (size_t i = 0; i < dets.size(); i++) {
        if (i > 0) oss << ",";
        oss << "{\"c\":" << dets[i].classId
            << ",\"p\":" << (int)(dets[i].confidence * 100)
            << ",\"b\":[" << dets[i].bbox.x << "," << dets[i].bbox.y
            << "," << dets[i].bbox.width << "," << dets[i].bbox.height << "]}";
    }
    oss << "]}";
    return oss.str();
}

void sendTypedData(uint8_t type, const uint8_t* data, uint32_t len) {
    uint32_t net_len = htonl(len);
    std::lock_guard<std::mutex> lock(quic_mutex);
    if (!g_cnx) return;

    if (!scheduler_is_redundant()) {
        picoquic_add_to_stream(g_cnx, 4, &type, 1, 0);
        picoquic_add_to_stream(g_cnx, 4, (const uint8_t*)&net_len, 4, 0);
        picoquic_add_to_stream(g_cnx, 4, data, len, 0);
        return;
    }

    picoquic_add_to_stream(g_cnx, 4, &type, 1, 0);
    picoquic_add_to_stream(g_cnx, 4, (const uint8_t*)&net_len, 4, 0);
    picoquic_add_to_stream(g_cnx, 4, data, len, 0);

    picoquic_add_to_stream(g_cnx, 8, &type, 1, 0);
    picoquic_add_to_stream(g_cnx, 8, (const uint8_t*)&net_len, 4, 0);
    picoquic_add_to_stream(g_cnx, 8, data, len, 0);

    if (g_cnx->nb_paths >= 2) {
        int pc5_idx = scheduler_find_pc5_path_idx(g_cnx, PC5_RECEIVER_IP);
        if (pc5_idx >= 0) {
            scheduler_set_stream_path_affinity(g_cnx, REDUNDANT_SECONDARY_STREAM,
                                               g_cnx->path[pc5_idx]);
            static bool logged = false;
            if (!logged) {
                printf("[Redundant] stream4→Uu, stream8→PC5(path[%d]) affinity 설정 완료\n", pc5_idx);
                logged = true;
            }
        }
    }
}

void sendGpsData() {
    EfosGpsData gps;
    memset(&gps, 0, sizeof(gps));
    EFOS_GetGpsConciseInformation(&gps);
    printf("\n[GPS] time=%.6f lat=%.7f\n", gps.time, gps.latitude);

    EFOS_RESULT ret = EFOS_GetGpsConciseInformation(&gps);
    if (ret < EFOS_RESULT_SUCCESS) return;

    char json[256];
    int json_len = snprintf(json, sizeof(json),
        "{\"lat\":%.7f,\"lon\":%.7f,\"spd\":%.2f,\"hdg\":%.2f,\"alt\":%.2f}",
        gps.latitude, gps.longitude, gps.speed, gps.heading, gps.altitude);

    if (json_len > 0) {
        {
            std::lock_guard<std::mutex> lk(g_gps_mtx);
            g_gps_snap.lat       = gps.latitude;
            g_gps_snap.lon       = gps.longitude;
            g_gps_snap.speed_mps = gps.speed / 3.6f;
        }
        sendTypedData(PKT_TYPE_GPS, (const uint8_t*)json, (uint32_t)json_len);
        printf("\n[GPS] lat=%.7f lon=%.7f spd=%.1fkm/h hdg=%.1f°\n",
               gps.latitude, gps.longitude, gps.speed, gps.heading);
    }
}

void sendFrameMeta(uint64_t frameId) {
    FrameMetaPacket meta;
    memset(&meta, 0, sizeof(meta));
    meta.frame_id     = frameId;
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    meta.t_tx_us = (uint64_t)tv.tv_sec * 1000000ULL + tv.tv_usec;
    meta.scheduler_id = (uint8_t)g_selected_scheduler;
    {
        std::lock_guard<std::mutex> lk(g_gps_mtx);
        meta.speed_mps = g_gps_snap.speed_mps;
        meta.lat       = g_gps_snap.lat;
        meta.lon       = g_gps_snap.lon;
    }
    sendTypedData(PKT_TYPE_FRAME_META, (const uint8_t*)&meta, (uint32_t)sizeof(meta));
}

GstFlowReturn onNewSample(GstAppSink* appsink, gpointer) {
    GstSample* sample = gst_app_sink_pull_sample(appsink);
    if (!sample) return GST_FLOW_ERROR;
    GstBuffer* buffer = gst_sample_get_buffer(sample);
    GstMapInfo map;
    if (gst_buffer_map(buffer, &map, GST_MAP_READ)) {
        sendTypedData(PKT_TYPE_RTP, map.data, (uint32_t)map.size);
        gst_buffer_unmap(buffer, &map);
    }
    gst_sample_unref(sample);
    return GST_FLOW_OK;
}

void dispatchQuicPacket(uint8_t* data, size_t len,
                        struct sockaddr_storage* peer_addr,
                        struct sockaddr_storage* local_addr) {
    struct sockaddr_in* peer_sin = (struct sockaddr_in*)peer_addr;
    struct sockaddr_in* pc5_sin  = (struct sockaddr_in*)&pc5_remote_addr;

    bool is_pc5 = (peer_sin->sin_addr.s_addr == pc5_sin->sin_addr.s_addr &&
                   peer_sin->sin_port        == pc5_sin->sin_port);

    if (is_pc5) {
        g_tx_count_pc5++;
        std::vector<uint8_t> pkt(sizeof(V2xMsgReq) + len);
        V2xMsgReq* p_req = reinterpret_cast<V2xMsgReq*>(pkt.data());
        p_req->v2xChannelId = g_channelID;
        p_req->length = (U16)len;
        memcpy(p_req->data, data, len);
        EFOS_SendV2xMsg(p_req);
    } else {
        g_tx_count_uu++;
        if (g_uu_sock >= 0) {
            ssize_t ret = sendto(g_uu_sock, data, len, 0,
                (struct sockaddr*)peer_addr, sizeof(struct sockaddr_in));
            if (ret < 0) {
                printf("[Uu 송신] sendto 실패: %s\n", strerror(errno));
            }
        }
    }
}


// ===================== 초기화 함수들 =====================

std::optional<V2xMsgRegi> initSirius(){
    if (EFOS_SetControlRxCallback((ControlRxCallbackFunc*)APP_ControlRxCallback) < EFOS_RESULT_SUCCESS) {
        printf("Failed to register control rx callback\n");
        return std::nullopt;
    }
    if (EFOS_InitializeV2x((char*)PC5_SENDER_IP) < EFOS_RESULT_SUCCESS) {
        printf("Failed to initialize\n");
        return std::nullopt;
    }
    printf("Connected to SIRIUS with IP (%s)\n", PC5_SENDER_IP);

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

bool initYolo() {
    try {
        g_yoloNet = cv::dnn::readNetFromONNX(YOLO_MODEL_ONNX);
        g_yoloNet.setPreferableBackend(cv::dnn::DNN_BACKEND_OPENCV);
        g_yoloNet.setPreferableTarget(cv::dnn::DNN_TARGET_CPU);
        printf("[YOLO] Open Images V7 ONNX 모델 로드 완료: %s\n", YOLO_MODEL_ONNX);
        printf("[YOLO] 클래스 수: %d, 관심 클래스: %zu개\n", NUM_CLASSES, INTERESTED_CLASSES.size());
        return true;
    } catch (const cv::Exception& e) {
        printf("[YOLO] ONNX 모델 로드 실패: %s\n", e.what());
        return false;
    }
}

void initGStreamerApp(GstElement* pipeline){
    g_appsrc  = GST_APP_SRC(gst_bin_get_by_name(GST_BIN(pipeline), "src"));
    g_appsink = GST_APP_SINK(gst_bin_get_by_name(GST_BIN(pipeline), "sink"));

    GstAppSinkCallbacks callbacks = {};
    callbacks.new_sample = onNewSample;
    gst_app_sink_set_callbacks(g_appsink, &callbacks, nullptr, nullptr);

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
        "x264enc tune=zerolatency bitrate=2000 speed-preset=ultrafast "
        "key-int-max=10 rc-lookahead=0 bframes=0 pass=cbr ! "
        "video/x-h264,profile=baseline ! "
        "rtph264pay config-interval=-1 pt=96 mtu=1400 ! "
        "appsink name=sink sync=false drop=false";

    GError* err = nullptr;
    GstElement* pipeline = gst_parse_launch(gst_pipeline.c_str(), &err);
    if (!pipeline || err) {
        printf("파이프라인 생성 실패: %s\n", err ? err->message : "unknown");
        return nullptr;
    }
    return pipeline;
}

int initUuSocket() {
    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) { printf("[Uu] 소켓 생성 실패: %s\n", strerror(errno)); return -1; }

    int flags = fcntl(sock, F_GETFL, 0);
    fcntl(sock, F_SETFL, flags | O_NONBLOCK);

    if (setsockopt(sock, SOL_SOCKET, SO_BINDTODEVICE, UU_INTERFACE, strlen(UU_INTERFACE)) < 0)
        printf("[Uu] SO_BINDTODEVICE 실패 (%s): %s\n", UU_INTERFACE, strerror(errno));
    else
        printf("[Uu] NIC 바인딩: %s\n", UU_INTERFACE);

    struct sockaddr_in bind_addr;
    memset(&bind_addr, 0, sizeof(bind_addr));
    bind_addr.sin_family = AF_INET;
    inet_pton(AF_INET, UU_SENDER_IP, &bind_addr.sin_addr);
    bind_addr.sin_port = htons(UU_SENDER_PORT);

    if (bind(sock, (struct sockaddr*)&bind_addr, sizeof(bind_addr)) < 0) {
        printf("[Uu] 바인딩 실패 (%s): %s\n", UU_SENDER_IP, strerror(errno));
        close(sock); return -1;
    }

    socklen_t addr_len = sizeof(struct sockaddr_in);
    getsockname(sock, (struct sockaddr*)&bind_addr, &addr_len);
    printf("[Uu] 소켓 바인딩 완료: %s:%d\n",
           inet_ntoa(bind_addr.sin_addr), ntohs(bind_addr.sin_port));

    memset(&uu_local_addr, 0, sizeof(uu_local_addr));
    memcpy(&uu_local_addr, &bind_addr, sizeof(bind_addr));
    return sock;
}

void initAddresses() {
    memset(&pc5_remote_addr, 0, sizeof(pc5_remote_addr));
    memset(&pc5_local_addr,  0, sizeof(pc5_local_addr));

    struct sockaddr_in* pc5_remote = (struct sockaddr_in*)&pc5_remote_addr;
    pc5_remote->sin_family = AF_INET;
    inet_pton(AF_INET, PC5_RECEIVER_IP, &pc5_remote->sin_addr);
    pc5_remote->sin_port = htons(4433);

    struct sockaddr_in* pc5_local = (struct sockaddr_in*)&pc5_local_addr;
    pc5_local->sin_family = AF_INET;
    inet_pton(AF_INET, PC5_SENDER_IP, &pc5_local->sin_addr);
    pc5_local->sin_port = htons(12345);

    memset(&uu_remote_addr, 0, sizeof(uu_remote_addr));
    struct sockaddr_in* uu_remote = (struct sockaddr_in*)&uu_remote_addr;
    uu_remote->sin_family = AF_INET;
    inet_pton(AF_INET, UU_RECEIVER_IP, &uu_remote->sin_addr);
    uu_remote->sin_port = htons(UU_RECEIVER_PORT);
}

picoquic_quic_t* initQuic(){
    uint64_t current_time = picoquic_current_time();

    picoquic_quic_t *quic = picoquic_create(
        8, "certs/cert.pem", "certs/key.pem", NULL, "v2x_test",
        NULL, NULL, NULL, NULL, NULL,
        current_time, NULL, NULL, NULL, 0);

    if (!quic) { printf("[QUIC] picoquic_create 실패\n"); return nullptr; }

    quic->default_multipath_option = 1;
    quic->default_tp.initial_max_path_id = 4;
    picoquic_set_default_congestion_algorithm_by_name(quic, "bbr");
    picoquic_set_cwin_max(quic, 10000000);
    picoquic_set_default_idle_timeout(quic, 120000);

    picoquic_cnx_t *cnx = picoquic_create_cnx(quic,
        picoquic_null_connection_id, picoquic_null_connection_id,
        (struct sockaddr*)&uu_remote_addr,
        current_time, 0, "test.sni", "v2x_test", 1);

    if (!cnx) { printf("[QUIC] picoquic_create_cnx 실패\n"); return nullptr; }

    cnx->local_parameters.initial_max_path_id = 4;
    cnx->idle_timeout = 120000;

    picoquic_set_callback(cnx, [](picoquic_cnx_t* cnx, uint64_t stream_id, uint8_t* bytes, size_t length,
        picoquic_call_back_event_t event, void* callback_ctx, void* stream_ctx) -> int {

        if (event == picoquic_callback_stream_data && length > 0) {
            if (stream_id == ADVISORY_STREAM_ID) {
                g_advisory_buf.insert(g_advisory_buf.end(), bytes, bytes + length);
                while (g_advisory_buf.size() >= 5) {
                    uint32_t net_len;
                    memcpy(&net_len, g_advisory_buf.data() + 1, 4);
                    uint32_t pktLen = ntohl(net_len);
                    if (g_advisory_buf.size() < 5 + pktLen) break;

                    std::string payload((const char*)g_advisory_buf.data() + 5, pktLen);

                    struct timeval tv;
                    gettimeofday(&tv, nullptr);
                    uint64_t t_adv_rx_us = (uint64_t)tv.tv_sec * 1000000ULL + tv.tv_usec;

                    auto parse_float = [&](const char* key, float def = 0.f) -> float {
                        size_t p = payload.find(key);
                        if (p == std::string::npos) return def;
                        return std::stof(payload.substr(p + strlen(key)));
                    };
                    auto parse_u64 = [&](const char* key) -> uint64_t {
                        size_t p = payload.find(key);
                        if (p == std::string::npos) return 0ULL;
                        return strtoull(payload.c_str() + p + strlen(key), nullptr, 10);
                    };
                    auto parse_bool = [&](const char* key) -> bool {
                        size_t p = payload.find(key);
                        if (p == std::string::npos) return false;
                        size_t start = p + strlen(key);
                        while (start < payload.size() && payload[start] == ' ') start++;
                        return payload.substr(start, 4) == "true";
                    };

                    float    w_til      = parse_float("\"w_til\":");
                    float    o_t        = parse_float("\"o_t\":");
                    bool     steer      = parse_bool ("\"steer\":");
                    int      consec_frz = (int)parse_u64("\"consec_frz\":");
                    uint64_t epoch_ts   = parse_u64("\"epoch_ts\":");
                    uint64_t t_adv_sent = parse_u64("\"t_adv_sent_us\":");

                    // ── 임시 디버그 ──────────────────────────────────────
                    printf("[DEBUG] payload=%s\n", payload.c_str());
                    printf("[DEBUG] t_adv_sent=%llu t_adv_rx=%llu diff=%lld us\n",
                        (unsigned long long)t_adv_sent,
                        (unsigned long long)t_adv_rx_us,
                        (long long)t_adv_rx_us - (long long)t_adv_sent);
                    // ─────────────────────────────────────────────────────
                    float r_uu          = parse_float("\"r_uu\":");
                    float r_pc5         = parse_float("\"r_pc5\":");
                    float w_eff         = parse_float("\"w_eff\":");
                    float uu_dmf        = parse_float("\"uu_dmf\":");
                    float pc5_dmf       = parse_float("\"pc5_dmf\":");
                    bool  both_act      = parse_bool("\"both_active\":");
                    float h0            = parse_float("\"h0\":", 5.0f);

                    { std::lock_guard<std::mutex> lk(g_wtil_mtx); g_w_til = w_til; }

                    double return_ms = (t_adv_sent > 0 && t_adv_rx_us > t_adv_sent)
                        ? (t_adv_rx_us - t_adv_sent) / 1000.0 : -1.0;

                    uint64_t uu_cnt  = g_tx_count_uu.load();
                    uint64_t pc5_cnt = g_tx_count_pc5.load();
                    float w_eff_local = (uu_cnt + pc5_cnt > 0)
                        ? (float)uu_cnt / (uu_cnt + pc5_cnt) : 0.5f;

                    double elapsed_sec = (t_adv_rx_us > 0)
                        ? (t_adv_rx_us - t_adv_sent) / 1e6 : 0.0;
                    float g_t = (float)std::exp(-elapsed_sec / TS_H0);
                    g_t = std::max(0.0f, std::min(1.0f, g_t));

                    float w_adv = w_eff_local;
                    bool do_steer = both_act && (o_t >= TS_THETA_O);
                    float delta_t = 0.0f;
                    if (do_steer) {
                        float raw = TS_LAMBDA_L * (r_pc5 - r_uu);
                        delta_t = std::max(-TS_EPSILON, std::min(TS_EPSILON, raw));
                        w_adv = w_eff_local + g_t * delta_t;
                        w_adv = std::max(0.0f, std::min(1.0f, w_adv));
                    }

                    { std::lock_guard<std::mutex> lk(g_wadv_mtx);
                      g_w_adv = w_adv; g_t_adv_rx_us_last = t_adv_rx_us; }
                    scheduler_set_uu_weight(w_adv);
                    scheduler_set_adv_rx_time(t_adv_rx_us);

                    struct timeval tv_apply;
                    gettimeofday(&tv_apply, nullptr);
                    uint64_t t_applied_us = (uint64_t)tv_apply.tv_sec * 1000000ULL + tv_apply.tv_usec;
                    double apply_latency_ms = (t_applied_us - t_adv_rx_us) / 1000.0;

                    printf("\n[Advisory ↓] w_til=%.3f  o_t=%.3f  steer=%s  frz=%d  return=%.1fms\n"
                           "             r_uu=%.3f  r_pc5=%.3f  w_eff_srv=%.3f  w_eff_local=%.3f\n"
                           "             uu_dmf=%.3f  pc5_dmf=%.3f  both=%s  h0=%.1f\n"
                           "             tx_uu=%llu  tx_pc5=%llu\n"
                           "             [TwinSteer] do_steer=%s  g_t=%.3f  delta_t=%.3f  w_adv=%.3f\n",
                           w_til, o_t, steer ? "ON" : "OFF", consec_frz, return_ms,
                           r_uu, r_pc5, w_eff, w_eff_local,
                           uu_dmf, pc5_dmf, both_act ? "Y" : "N", h0,
                           (unsigned long long)uu_cnt, (unsigned long long)pc5_cnt,
                           do_steer ? "Y" : "N", g_t, delta_t, w_adv);

                    double adv_return_ms = return_ms;
                    s_adv_recv_count++;
                    s_adv_sent_expected = s_adv_recv_count;
                    if (do_steer) s_steer_count++;

                    double steer_rate_pct = (s_adv_recv_count > 0)
                        ? (double)s_steer_count / s_adv_recv_count * 100.0 : 0.0;

                    { std::lock_guard<std::mutex> lk(g_sender_csv_mtx);
                      if (g_sender_csv_fp) {
                          fprintf(g_sender_csv_fp, "%llu,%.3f,%d,%.1f,%s,%.3f\n",
                              (unsigned long long)epoch_ts, adv_return_ms,
                              s_adv_recv_count, steer_rate_pct,
                              do_steer ? "STEERED" : "PASS", apply_latency_ms);
                          fflush(g_sender_csv_fp);
                      }
                    }

                    printf("\n[TiL Metrics]\n"
                           "  [4] Adv return latency : %.3f ms\n"
                           "  [6] Adv recv count     : %d\n"
                           "  [7] Closed-loop rate   : %.1f%% (%d/%d steered)\n"
                           "  → CSV: til_sender_metrics.csv\n",
                           adv_return_ms, s_adv_recv_count,
                           steer_rate_pct, s_steer_count, s_adv_recv_count);

                    char ack_json[128];
                    
                    int ack_len = snprintf(ack_json, sizeof(ack_json),
                        "{\"epoch_ts\":%llu,\"t_adv_sent_us\":%llu,\"t_adv_rx_us\":%llu}",
                        (unsigned long long)epoch_ts,
                        (unsigned long long)t_adv_sent,
                        (unsigned long long)t_adv_rx_us);

                    uint8_t ack_type = PKT_TYPE_ADV_ACK;
                    uint32_t ack_net_len = htonl((uint32_t)ack_len);
                    picoquic_add_to_stream(cnx, 4, &ack_type, 1, 0);
                    picoquic_add_to_stream(cnx, 4, (const uint8_t*)&ack_net_len, 4, 0);
                    picoquic_add_to_stream(cnx, 4, (const uint8_t*)ack_json, ack_len, 0);

                    g_advisory_buf.erase(g_advisory_buf.begin(),
                                         g_advisory_buf.begin() + 5 + pktLen);
                }
                return 0;
            }
            return 0;
        }

        switch (event) {
        case picoquic_callback_ready:
            printf("[QUIC] ★ 커넥션 READY! Multipath: %d\n", cnx->is_multipath_enabled);
            g_uu_handshake_done = true;
            break;

        // ── PC5 경로 상태 콜백 ──────────────────────────────
        case picoquic_callback_path_available:
            printf("[Multipath] ★ PC5 경로 살아남! (path_available 콜백)\n");
            g_pc5_path_added = true;
            g_pc5_need_probe = false;
            break;

        case picoquic_callback_path_suspended:
            printf("[Multipath] PC5 경로 suspended (path_suspended 콜백)\n");
            g_pc5_path_added = false;
            g_pc5_need_probe = true;
            break;

        case picoquic_callback_path_deleted:
            printf("[Multipath] PC5 경로 삭제됨 (path_deleted 콜백) → 재probe 필요\n");
            g_pc5_path_added = false;
            g_pc5_need_probe = true;
            break;

        case picoquic_callback_next_path_allowed:
            printf("[QUIC] 새 경로 추가 허용됨\n");
            break;

        case picoquic_callback_close:
        case picoquic_callback_application_close:
            printf("[QUIC] 커넥션 종료\n");
            keepRunning = false;
            break;

        default:
            break;
        }
        return 0;
    }, NULL);

    picoquic_start_client_cnx(cnx);
    g_cnx = cnx;

    printf("[QUIC] 초기 핸드셰이크 시작 (Uu: %s:%d)\n", UU_RECEIVER_IP, UU_RECEIVER_PORT);
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
            g_yoloQueueCv.notify_all();
        }
    }
}

void quicSendThread(picoquic_quic_t* quic) {
    uint8_t sendBuf[1536];
    printf("[송신] QUIC 송신 스레드 시작...\n");
    while (keepRunning) {
        size_t sendLen = 0;
        struct sockaddr_storage peer_addr, local_addr;
        int if_index = 0;
        picoquic_connection_id_t log_cid;
        picoquic_cnx_t* last_cnx = nullptr;
        size_t send_msg_size = 0;
        {
            std::lock_guard<std::mutex> lock(quic_mutex);
            int ret = picoquic_prepare_next_packet_ex(
                quic, picoquic_current_time(),
                sendBuf, sizeof(sendBuf), &sendLen,
                &peer_addr, &local_addr,
                &if_index, &log_cid, &last_cnx, &send_msg_size);
            if (ret != 0) continue;
        }
        if (sendLen > 0)
            dispatchQuicPacket(sendBuf, sendLen, &peer_addr, &local_addr);
        else
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    printf("[송신] QUIC 송신 스레드 종료\n");
}

void pc5RecvThread(picoquic_quic_t* quic) {
    uint8_t rxBuf[9100] = { 0 };
    printf("[PC5 수신] 스레드 시작...\n");
    while (keepRunning) {
        V2xMsgType v2xMsgType = EFOS_RecvV2xMsg(rxBuf);
        V2xMsg* p_v2xMsg = (V2xMsg*)rxBuf;
        if (p_v2xMsg->length == 0) continue;
        if (p_v2xMsg->length <= 34) continue;
        
        std::lock_guard<std::mutex> lock(quic_mutex);
        picoquic_incoming_packet(quic,
            p_v2xMsg->data + 34, p_v2xMsg->length - 34,
            (struct sockaddr*)&pc5_remote_addr,
            (struct sockaddr*)&pc5_local_addr,
            0, 0, picoquic_current_time());
    }
    printf("[PC5 수신] 스레드 종료\n");
}

void uuRecvThread(picoquic_quic_t* quic) {
    uint8_t rxBuf[2048];
    printf("[Uu 수신] 스레드 시작...\n");
    while (keepRunning) {
        struct sockaddr_storage from_addr;
        socklen_t from_len = sizeof(from_addr);
        ssize_t recvLen = recvfrom(g_uu_sock, rxBuf, sizeof(rxBuf), 0,
                                   (struct sockaddr*)&from_addr, &from_len);
        if (recvLen > 0) {
            std::lock_guard<std::mutex> lock(quic_mutex);
            picoquic_incoming_packet(quic, rxBuf, recvLen,
                (struct sockaddr*)&from_addr, (struct sockaddr*)&uu_local_addr,
                0, 0, picoquic_current_time());
        } else if (recvLen < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
            // error
        } else {
            std::this_thread::sleep_for(std::chrono::microseconds(500));
        }
    }
    printf("[Uu 수신] 스레드 종료\n");
}

void multipathProbeThread(picoquic_quic_t* quic) {
    printf("[Multipath] PC5 경로 대기 중...\n");

    // ── 초기 Uu 핸드셰이크 대기 ──────────────────────────
    while (keepRunning && !g_uu_handshake_done)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    if (!keepRunning) return;

    printf("[Multipath] Uu 핸드셰이크 완료. PC5 초기 probe 시작...\n");
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // ── 초기 PC5 경로 추가 ────────────────────────────────
    const int MAX_RETRY = 20;
    for (int i = 0; i < MAX_RETRY && keepRunning && !g_pc5_path_added; i++) {
        {
            std::lock_guard<std::mutex> lock(quic_mutex);
            if (!g_cnx->is_multipath_enabled) {
                printf("[Multipath] Multipath 협상 실패. Uu 단일 경로로 진행.\n");
                break;
            }
            int ret = picoquic_probe_new_path_ex(g_cnx,
                (const struct sockaddr*)&pc5_remote_addr,
                (const struct sockaddr*)&pc5_local_addr,
                0, picoquic_current_time(), 0);
            if (ret == 0) {
                printf("[Multipath] ★ PC5 초기 경로 추가 성공! 경로 수: %d\n", g_cnx->nb_paths);
                // g_pc5_path_added는 path_available 콜백에서 true로 세팅됨
                break;
            } else {
                printf("[Multipath] PC5 probe 시도 %d/%d, ret=%d\n", i+1, MAX_RETRY, ret);
                int tmp = 0;
                picoquic_subscribe_new_path_allowed(g_cnx, &tmp);
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    // ── 복구 루프: 콜백이 need_probe 세팅 → 여기서 재probe ─
    auto last_log = std::chrono::steady_clock::now();

    while (keepRunning) {
        // alive면 5초, need_probe면 1초 간격
        int interval_ms = g_pc5_need_probe.load() ? 1000 : 5000;
        std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));

        if (!g_pc5_need_probe.load()) {
            // 5초마다 상태 요약만 출력
            auto now = std::chrono::steady_clock::now();
            if (std::chrono::duration_cast<std::chrono::seconds>(
                    now - last_log).count() >= 5) {
                std::lock_guard<std::mutex> lock(quic_mutex);
                if (g_cnx)
                    printf("[Multipath] 경로=%d | PC5=%s | MP=%s\n",
                           g_cnx->nb_paths,
                           g_pc5_path_added.load() ? "✅ alive" : "❌ dead",
                           g_cnx->is_multipath_enabled ? "ON" : "OFF");
                last_log = now;
            }
            continue;
        }

        // need_probe=true → 재probe 시도
        std::lock_guard<std::mutex> lock(quic_mutex);
        if (!g_cnx || !g_cnx->is_multipath_enabled) continue;

        // 이미 pending 경로가 있으면 picoquic이 재시도 중이므로 대기
        bool pc5_pending = false;
        struct sockaddr_in* pc5_sin = (struct sockaddr_in*)&pc5_remote_addr;
        for (int i = 0; i < g_cnx->nb_paths; i++) {
            struct sockaddr_in* peer =
                (struct sockaddr_in*)&g_cnx->path[i]->registered_peer_addr;
            if (peer->sin_addr.s_addr == pc5_sin->sin_addr.s_addr) {
                pc5_pending = true; break;
            }
        }

        if (!pc5_pending) {
            int ret = picoquic_probe_new_path_ex(g_cnx,
                (const struct sockaddr*)&pc5_remote_addr,
                (const struct sockaddr*)&pc5_local_addr,
                0, picoquic_current_time(), 0);
            printf("[Multipath] PC5 재probe (ret=%d, 경로=%d)\n", ret, g_cnx->nb_paths);
            if (ret == 0) g_pc5_need_probe = false;  // probe 전송 성공, 콜백 대기
        } else {
            printf("[Multipath] PC5 pending 경로 존재, 콜백 대기 중...\n");
        }

        auto now = std::chrono::steady_clock::now();
        if (std::chrono::duration_cast<std::chrono::seconds>(
                now - last_log).count() >= 5) {
            printf("[Multipath] 경로=%d | PC5=%s | need_probe=%s\n",
                   g_cnx->nb_paths,
                   g_pc5_path_added.load() ? "✅ alive" : "❌ dead",
                   g_pc5_need_probe.load() ? "Y" : "N");
            last_log = now;
        }
    }
}

void yoloWorkerThread() {
    printf("[YOLO 워커] 스레드 시작...\n");
    if (g_yoloNet.empty()) { printf("[YOLO 워커] 모델이 없어 워커 종료\n"); return; }

    while (keepRunning) {
        FrameJob job;
        {
            std::unique_lock<std::mutex> lk(g_yoloQueueMtx);
            g_yoloQueueCv.wait(lk, [] { return !g_yoloQueue.empty() || !keepRunning; });
            if (!keepRunning) break;
            job = std::move(g_yoloQueue.front());
            g_yoloQueue.pop();
        }

        try {
            cv::Mat blob;
            cv::dnn::blobFromImage(job.frame, blob, 1.0/255.0,
                                   cv::Size(YOLO_INPUT_SIZE, YOLO_INPUT_SIZE),
                                   cv::Scalar(), true, false);

            static bool blob_logged = false;
            if (!blob_logged) {
                printf("[YOLO] blob dims=%d, shape=[", blob.dims);
                for (int i = 0; i < blob.dims; i++)
                    printf("%d%s", blob.size[i], (i < blob.dims-1) ? "," : "");
                printf("]\n");
                blob_logged = true;
            }

            g_yoloNet.setInput(blob);
            std::vector<cv::Mat> outputs;
            g_yoloNet.forward(outputs, g_yoloNet.getUnconnectedOutLayersNames());

            static bool out_logged = false;
            if (!out_logged) {
                printf("[YOLO] outputs.size()=%zu\n", outputs.size());
                for (size_t i = 0; i < outputs.size(); i++) {
                    printf("  output[%zu] dims=%d, shape=[", i, outputs[i].dims);
                    for (int j = 0; j < outputs[i].dims; j++)
                        printf("%d%s", outputs[i].size[j], (j < outputs[i].dims-1) ? "," : "");
                    printf("]\n");
                }
                out_logged = true;
            }

            auto dets = postprocessYolo(outputs, job.frame.cols, job.frame.rows);
            { std::lock_guard<std::mutex> lk(g_detMtx); g_lastDetections = dets; g_lastDetFrameId = job.frameId; }

            std::string json = detectionsToJson(dets, job.frameId);
            sendTypedData(PKT_TYPE_YOLO, (const uint8_t*)json.c_str(), (uint32_t)json.size());

        } catch (const cv::Exception& e) {
            printf("[YOLO 워커] cv 에러: %s\n", e.what());
        } catch (const std::exception& e) {
            printf("[YOLO 워커] 일반 에러: %s\n", e.what());
        }
    }
    printf("[YOLO 워커] 스레드 종료\n");
}

void gpsWorkerThread() {
    printf("[GPS] 워커 스레드 시작...\n");
    while (keepRunning) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        sendGpsData();
    }
    printf("[GPS] 워커 스레드 종료\n");
}

//##########################MAIN###############################
int main(int argc, char** argv) {
    // ──────────────────────────────────────────────────
    gst_init(&argc, &argv);

    int selected_scheduler = SCHEDULER_MIN_RTT;
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--scheduler") == 0 && i + 1 < argc) {
            const char* name = argv[i + 1];
            if      (strcmp(name, "rr")         == 0) selected_scheduler = SCHEDULER_ROUND_ROBIN;
            else if (strcmp(name, "minrtt")      == 0) selected_scheduler = SCHEDULER_MIN_RTT;
            else if (strcmp(name, "redundant")   == 0) selected_scheduler = SCHEDULER_REDUNDANT;
            else if (strcmp(name, "mprdeadline") == 0) selected_scheduler = SCHEDULER_MPR_DEADLINE;
            else if (strcmp(name, "storm")       == 0) selected_scheduler = SCHEDULER_STORM;
            else if (strcmp(name, "peekaboo")    == 0) selected_scheduler = SCHEDULER_PEEKABOO;
            else {
                printf("[Scheduler] 알 수 없는 스케줄러: %s\n", name);
                printf("사용법: --scheduler [rr|minrtt|redundant|mprdeadline|storm|peekaboo]\n");
                return -1;
            }
            i++;
        }
    }
    picoquic_set_scheduler(selected_scheduler);
    g_selected_scheduler = selected_scheduler;
    initAddresses();

    auto v2xmsgregi = initSirius();
    if (!v2xmsgregi) { printf("V2X init ERROR\n"); return -1; }

    g_uu_sock = initUuSocket();
    if (g_uu_sock < 0) return -1;

    picoquic_quic_t *quic = initQuic();
    if (!quic) return -1;

    g_sender_csv_fp = fopen(
        "/home/icons-linux/Documents/sirius/sender_scheduler_Til/til_sender_metrics.csv", "w");
    if (g_sender_csv_fp) {
        fprintf(g_sender_csv_fp,
            "epoch_ts,adv_return_ms,adv_recv_count,steer_rate_pct,steer_result,apply_latency_ms\n");
    }

    std::thread send_thread(quicSendThread, quic);
    std::thread uu_recv_thread(uuRecvThread, quic);
    std::thread pc5_recv_thread(pc5RecvThread, quic);
    std::thread mp_probe_thread(multipathProbeThread, quic);

    bool yoloAvailable = initYolo();
    if (!yoloAvailable) printf("[YOLO] 모델 로드 실패. YOLO 없이 진행합니다.\n");

    GstElement* pipeline = initGStreamerPipeline();
    if (!pipeline) return -1;
    initGStreamerApp(pipeline);

    std::this_thread::sleep_for(std::chrono::seconds(2));

    cv::VideoCapture cap;
    cap.open(0, cv::CAP_V4L2);
    cap.set(cv::CAP_PROP_FOURCC, cv::VideoWriter::fourcc('M','J','P','G'));
    cap.set(cv::CAP_PROP_FRAME_WIDTH,  CAMERA_WIDTH);
    cap.set(cv::CAP_PROP_FRAME_HEIGHT, CAMERA_HEIGHT);
    cap.set(cv::CAP_PROP_FPS,          CAMERA_FRAME);
    cap.set(cv::CAP_PROP_BUFFERSIZE,   1);
    if (!cap.isOpened()) { printf("카메라 열기 실패\n"); return -1; }

    std::thread input_thread(inputThread);
    std::thread yolo_thread;
    if (yoloAvailable) yolo_thread = std::thread(yoloWorkerThread);
    std::thread gps_thread(gpsWorkerThread);

    cv::Mat frame;
    GstClockTime timestamp = 0;
    const GstClockTime duration = GST_SECOND / CAMERA_FRAME;
    uint64_t frameId = 0;

    while (keepRunning) {
        cap >> frame;
        if (frame.empty()) continue;
        cv::Mat original_frame = frame.clone();

        if (yoloAvailable) {
            FrameJob job;
            job.frame = frame.clone();
            job.frameId = frameId;
            {
                std::lock_guard<std::mutex> lk(g_yoloQueueMtx);
                while (g_yoloQueue.size() >= YOLO_QUEUE_MAX) g_yoloQueue.pop();
                g_yoloQueue.push(std::move(job));
            }
            g_yoloQueueCv.notify_one();
        }

        gsize buf_size = original_frame.total() * original_frame.elemSize();
        GstBuffer* buffer = gst_buffer_new_allocate(nullptr, buf_size, nullptr);
        GstMapInfo map;
        gst_buffer_map(buffer, &map, GST_MAP_WRITE);
        memcpy(map.data, original_frame.data, buf_size);
        gst_buffer_unmap(buffer, &map);
        GST_BUFFER_PTS(buffer)      = timestamp;
        GST_BUFFER_DURATION(buffer) = duration;
        timestamp += duration;
        sendFrameMeta(frameId);
        gst_app_src_push_buffer(g_appsrc, buffer);
        frameId++;
    }

    keepRunning = false;
    g_yoloQueueCv.notify_all();

    gst_app_src_end_of_stream(g_appsrc);
    gst_element_set_state(pipeline, GST_STATE_NULL);
    gst_object_unref(g_appsrc);
    gst_object_unref(g_appsink);
    gst_object_unref(pipeline);

    input_thread.join();
    send_thread.join();
    uu_recv_thread.join();
    pc5_recv_thread.join();
    mp_probe_thread.join();
    if (yolo_thread.joinable()) yolo_thread.join();
    gps_thread.join();

    picoquic_free(quic);
    if (g_uu_sock >= 0) close(g_uu_sock);
    if (g_sender_csv_fp) fclose(g_sender_csv_fp);

    cap.release();
    EFOS_TerminateV2x(v2xmsgregi->v2xServiceId, v2xmsgregi->commMode);
    return 0;
}