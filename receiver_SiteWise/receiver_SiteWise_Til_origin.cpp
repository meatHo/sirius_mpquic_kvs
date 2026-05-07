#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <iostream>
#include <sstream>
#include <thread>
#include <atomic>
#include <mutex>
#include <chrono>
#include <optional>
#include <vector>
#include <queue>
#include <algorithm>
#include <condition_variable>
#include <map>

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
#include <set> 
#include <gst/gst.h>
#include <gst/app/gstappsrc.h>
#include <gst/app/gstappsink.h>

// #####################GLOBAL Variable############################
std::atomic<bool> keepRunning(true);
static U16 g_channelID = 0;
std::mutex quic_mutex;
static GstAppSrc *g_appsrc = nullptr;
static GstAppSink *g_appsink = nullptr;

static std::vector<uint8_t> g_recvBuf_s4;  // ← 기존 g_recvBuf 대체
static std::vector<uint8_t> g_recvBuf_s8;  // ← 새로 추가

static std::set<uint64_t> g_seen_yolo_frames;
static std::mutex          g_seen_mtx;

// 전역변수
static uint64_t g_last_frame_rx_us = 0;
static std::mutex g_last_frame_rx_mtx;

#define SEEN_FRAMES_MAX 200


// ── 통합 CSV ─────────────────────────────────────────────────
static FILE* g_all_csv_fp = nullptr;

// epoch별 임시 저장 (ACK 도착 시 한 행으로 합침)
struct EpochMetrics {
    double uu_mean_ms   = 0, uu_p95_ms   = 0;
    double pc5_mean_ms  = 0, pc5_p95_ms  = 0;
    double twin_ms      = 0;
    double adv_gen_ms   = 0;
    double epoch_wait_ms = 0;
};
static EpochMetrics g_last_epoch_metrics;
static std::mutex   g_epoch_metrics_mtx;

// 4번, 5번 누적 버퍼
static std::vector<double> g_adv_return_buf;
static std::vector<double> g_e2e_buf;
static std::mutex g_metric_buf_mtx;
static picoquic_cnx_t *g_server_cnx = nullptr;

// ── 경로 식별 (recv 스레드 → callback 전달용) ─────────────────
// quic_mutex 보유 중에만 접근하므로 thread_local 불필요
static int g_incoming_path = -1;  // 0 = Uu, 1 = PC5

// ── FrameMeta 패킷 구조체 (sender 와 동일, packed 37 바이트) ──
#pragma pack(push, 1)
struct FrameMetaPacket {
    uint64_t frame_id;
    uint64_t t_tx_us;
    uint8_t  scheduler_id;
    float    speed_mps;
    double   lat;
    double   lon;
};
#pragma pack(pop)

// ── 프레임별 도착 추적 ────────────────────────────────────────
struct FrameArrival {
    uint64_t t_tx_us       = 0;
    uint64_t t_rx_uu_us    = 0;   // 0 = Uu 로 미도착
    uint64_t t_rx_pc5_us   = 0;   // 0 = PC5 로 미도착
    uint8_t  scheduler_id  = 0;
    float    speed_mps     = 0.0f;
    double   lat           = 0.0;
    double   lon           = 0.0;
};
static std::map<uint64_t, FrameArrival> g_frame_map;
static std::mutex                        g_frame_mtx;

// ── TiL 서버 연결 소켓 ───────────────────────────────────────
static int g_til_sock = -1;

// ── 다운링크 메트릭 추적 ─────────────────────────────────────
static FILE* g_cl_csv_fp = nullptr;   // closed-loop CSV

struct EpochAdvisoryRecord {
    uint64_t t_adv_sent_us  = 0;
    uint64_t epoch_first_tx = 0;   // epoch 내 첫 프레임 t_tx_us
};
static std::map<uint64_t, EpochAdvisoryRecord> g_adv_record;  // epoch_ts → record
static std::mutex g_adv_mtx;
static std::atomic<int> g_adv_sent_count{0};
static std::atomic<int> g_ack_recv_count{0};

// ── CSV 파일 ─────────────────────────────────────────────────
static FILE* g_csv_fp = nullptr;
static std::mutex g_csv_mtx;

// ── Epoch 설정 ────────────────────────────────────────────────
static const uint64_t EPOCH_US    = 5000000ULL;   // 5초 (마이크로초)
static const uint64_t DEADLINE_US = 200000ULL;    // 200ms deadline
static uint64_t g_epoch_last_tx = 0;

// ===================== 패킷 타입 정의 =====================
#define PKT_TYPE_RTP  0x01
#define PKT_TYPE_YOLO 0x02
#define PKT_TYPE_GPS   0x03
#define PKT_TYPE_FRAME_META 0x04  
#define PKT_TYPE_ADVISORY   0x05
#define ADVISORY_STREAM_ID  13 
#define PKT_TYPE_ADV_ACK    0x06   // ← 추가

// ===================== YOLO 디텍션 결과 (수신용) =====================
struct Detection
{
    int classId;
    float confidence;
    cv::Rect bbox;
};

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
#define PC5_SENDER_IP   "192.168.1.11"
#define PC5_RECEIVER_IP "10.254.52.18"

#define UU_RECEIVER_IP   "202.30.29.202"
#define UU_RECEIVER_PORT 4433

struct sockaddr_storage pc5_remote_addr;
struct sockaddr_storage pc5_local_addr;

static int g_uu_sock = -1;

#define STREAM_NAME "test-stream"
#define AWS_REGION  "ap-northeast-2"

// ===================== OIV7 클래스 이름 (601개) =====================
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
static const int OIV7_NUM_CLASSES = 601;

// ===================== SiteWise 설정 =====================
#define SITEWISE_ASSET_ID    "2e2097da-24d5-4c29-9b71-b527fcbfbce3"
#define SITEWISE_PROPERTY_ID "e409110c-bd5e-42c3-95f9-1588222eeea7"
#define SITEWISE_REGION      "ap-northeast-2"

// SiteWise 전송 큐
static std::mutex              g_swQueueMtx;
static std::condition_variable g_swQueueCv;
static std::queue<std::string> g_swQueue;
static const size_t            SW_QUEUE_MAX = 10;

// 마지막 SiteWise 전송 시각 (1초에 1번만 전송)
static std::atomic<int64_t> g_lastSwSendSec(0);

// ##########################FUNCTION###############################
static void APP_ControlRxCallback(V2xMsgType v2xMsgType, U8 *p_rxBuf) {}

// ===================== JSON 이스케이프 (따옴표, 백슬래시 처리) =====================
std::string escapeJsonForShell(const std::string& json) {
    std::string out;
    out.reserve(json.size() * 2);
    for (char c : json) {
        if (c == '"')       out += "\\\"";
        else if (c == '\\') out += "\\\\";
        else                out += c;
    }
    return out;
}

// ===================== 요약 JSON 생성 =====================
static const int SUMMARY_TOP_N = 5;

std::string makeSummaryJson(const std::string& jsonStr) {
    size_t fPos = jsonStr.find("\"f\":");
    uint64_t frameId = 0;
    if (fPos != std::string::npos) frameId = atoll(jsonStr.c_str() + fPos + 4);

    if (jsonStr.find("\"d\":[]") != std::string::npos) {
        char buf[64];
        snprintf(buf, sizeof(buf), "{\"f\":%lu,\"count\":0,\"top\":[]}", frameId);
        return std::string(buf);
    }

    struct ObjInfo { int classId; int conf; };
    std::vector<ObjInfo> objs;

    size_t dPos = jsonStr.find("\"d\":[");
    if (dPos == std::string::npos) return jsonStr;

    size_t pos = dPos + 5;
    while (pos < jsonStr.size()) {
        size_t cPos = jsonStr.find("\"c\":", pos);
        if (cPos == std::string::npos) break;
        int classId = atoi(jsonStr.c_str() + cPos + 4);

        size_t pPos = jsonStr.find("\"p\":", cPos);
        if (pPos == std::string::npos) break;
        int conf = atoi(jsonStr.c_str() + pPos + 4);

        objs.push_back({classId, conf});

        size_t nextBrace = jsonStr.find('}', pPos);
        if (nextBrace == std::string::npos) break;
        pos = nextBrace + 1;
    }

    std::ostringstream oss;
    oss << "{\"f\":" << frameId
        << ",\"count\":" << objs.size()
        << ",\"top\":[";

    int limit = std::min((int)objs.size(), SUMMARY_TOP_N);
    for (int i = 0; i < limit; i++) {
        if (i > 0) oss << ",";
        const char* name = (objs[i].classId >= 0 && objs[i].classId < OIV7_NUM_CLASSES)
                           ? OIV7_CLASSES[objs[i].classId] : "Unknown";
        oss << "{\"name\":\"" << name << "\",\"p\":" << objs[i].conf << "}";
    }
    oss << "]}";
    return oss.str();
}

// ===================== SiteWise CLI 전송 함수 =====================
void sendToSiteWise(const std::string& jsonStr) {
    int64_t nowSec = (int64_t)std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    int64_t lastSec = g_lastSwSendSec.load();
    if (nowSec - lastSec < 1) return;
    if (!g_lastSwSendSec.compare_exchange_strong(lastSec, nowSec)) return;

    // ★ 요약 JSON으로 변환 후 큐에 추가
    std::string summary = makeSummaryJson(jsonStr);

    {
        std::lock_guard<std::mutex> lk(g_swQueueMtx);
        if (g_swQueue.size() >= SW_QUEUE_MAX) {
            g_swQueue.pop();
        }
        g_swQueue.push(summary);
    }
    g_swQueueCv.notify_one();
}

// ===================== SiteWise 워커 스레드 =====================
void siteWiseWorkerThread() {
    printf("[SiteWise] 워커 스레드 시작...\n");

    while (keepRunning) {
        std::string jsonStr;
        {
            std::unique_lock<std::mutex> lk(g_swQueueMtx);
            g_swQueueCv.wait(lk, [] {
                return !g_swQueue.empty() || !keepRunning;
            });
            if (!keepRunning && g_swQueue.empty()) break;
            jsonStr = std::move(g_swQueue.front());
            g_swQueue.pop();
        }

        int64_t nowSec = (int64_t)std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();

        // ★ 감지 객체 터미널 출력 (요약 JSON 기준)
        {
            size_t fPos = jsonStr.find("\"f\":");
            uint64_t frameId = 0;
            if (fPos != std::string::npos) frameId = atoll(jsonStr.c_str() + fPos + 4);

            size_t cntPos = jsonStr.find("\"count\":");
            int totalCount = 0;
            if (cntPos != std::string::npos) totalCount = atoi(jsonStr.c_str() + cntPos + 8);

            if (totalCount == 0) {
                printf("[YOLO->SW] frame=%-6lu | 감지 없음\n", frameId);
            } else {
                printf("[YOLO->SW] frame=%-6lu | ", frameId);
                // "top":[{"name":"Car","p":87},...]  파싱
                size_t topPos = jsonStr.find("\"top\":[");
                if (topPos != std::string::npos) {
                    size_t pos = topPos + 7;
                    while (pos < jsonStr.size()) {
                        size_t nPos = jsonStr.find("\"name\":\"", pos);
                        if (nPos == std::string::npos) break;
                        nPos += 8;
                        size_t nEnd = jsonStr.find('\"', nPos);
                        if (nEnd == std::string::npos) break;
                        std::string name = jsonStr.substr(nPos, nEnd - nPos);

                        size_t pPos = jsonStr.find("\"p\":", nEnd);
                        if (pPos == std::string::npos) break;
                        int conf = atoi(jsonStr.c_str() + pPos + 5);

                        printf("%s(%d%%) ", name.c_str(), conf);

                        size_t nextBrace = jsonStr.find('}', pPos);
                        if (nextBrace == std::string::npos) break;
                        pos = nextBrace + 1;
                    }
                }
                printf("| 총 %d개\n", totalCount);
            }
        }

        // JSON 이스케이프
        std::string escaped = escapeJsonForShell(jsonStr);

        // AWS CLI 명령어 구성
        char cmd[4096];
        snprintf(cmd, sizeof(cmd),
            "aws iotsitewise batch-put-asset-property-value "
            "--region %s "
            "--entries '[{"
                "\"entryId\":\"yolo-%ld\","
                "\"assetId\":\"%s\","
                "\"propertyId\":\"%s\","
                "\"propertyValues\":[{"
                    "\"value\":{\"stringValue\":\"%s\"},"
                    "\"timestamp\":{\"timeInSeconds\":%ld}"
                "}]"
            "}]' 2>&1",
            SITEWISE_REGION,
            nowSec,
            SITEWISE_ASSET_ID,
            SITEWISE_PROPERTY_ID,
            escaped.c_str(),
            nowSec
        );

        // popen으로 실행
        FILE* fp = popen(cmd, "r");
        if (!fp) {
            printf("[SiteWise] popen 실패\n");
            continue;
        }

        char result[1024] = {0};
        fread(result, 1, sizeof(result) - 1, fp);
        pclose(fp);

        // "errorEntries":[] 이면 성공, 아니면 실패
        if (strstr(result, "\"errorEntries\": []") != nullptr ||
            strstr(result, "\"errorEntries\":[]") != nullptr) {
            printf("[SiteWise] 전송 성공\n\n");
        } else if (strlen(result) == 0) {
            printf("[SiteWise] 전송 성공\n\n");
        } else {
            // 에러 코드만 추출해서 출력
            const char* errCode = strstr(result, "\"errorCode\": \"");
            if (errCode) {
                char code[64] = {0};
                sscanf(errCode + 14, "%63[^\"]", code);
                printf("[SiteWise] 전송 실패: %s\n\n", code);
            } else {
                printf("[SiteWise] 전송 실패\n\n");
            }
        }
    }

    printf("[SiteWise] 워커 스레드 종료\n");
}

// ===================== 간단한 JSON 파서 =====================
std::vector<Detection> parseYoloJson(const uint8_t *data, uint32_t len)
{
    std::vector<Detection> dets;
    std::string json((const char *)data, len);

    size_t dPos = json.find("\"d\":[");
    if (dPos == std::string::npos) return dets;

    size_t pos = dPos + 5;

    while (pos < json.size())
    {
        size_t cPos = json.find("\"c\":", pos);
        if (cPos == std::string::npos) break;

        Detection det;
        det.classId = atoi(json.c_str() + cPos + 4);

        size_t pPos = json.find("\"p\":", cPos);
        if (pPos == std::string::npos) break;
        det.confidence = atoi(json.c_str() + pPos + 4) / 100.0f;

        size_t bPos = json.find("\"b\":[", cPos);
        if (bPos == std::string::npos) break;
        int x, y, w, h;
        if (sscanf(json.c_str() + bPos + 4, "[%d,%d,%d,%d]", &x, &y, &w, &h) == 4)
        {
            det.bbox = cv::Rect(x, y, w, h);
        }

        dets.push_back(det);

        size_t nextBrace = json.find('}', bPos);
        if (nextBrace == std::string::npos) break;
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
static uint64_t parse_yolo_frame_id(const uint8_t* data, uint32_t len) {
    std::string json((const char*)data, len);
    size_t fPos = json.find("\"f\":");
    if (fPos == std::string::npos) return UINT64_MAX;
    return (uint64_t)atoll(json.c_str() + fPos + 4);
}

static void processStreamBuf(std::vector<uint8_t>& buf, bool is_redundant_stream) {
    while (buf.size() >= 5) {
        uint8_t type = buf[0];
        uint32_t net_len;
        memcpy(&net_len, buf.data() + 1, 4);
        uint32_t pktLen = ntohl(net_len);
        if (pktLen > 1000000) { buf.clear(); break; }
        if (buf.size() < 5 + pktLen) break;

        if (type == PKT_TYPE_RTP) {
            if (!is_redundant_stream) {
                GstBuffer *gstbuf = gst_buffer_new_allocate(nullptr, pktLen, nullptr);
                GstMapInfo map;
                gst_buffer_map(gstbuf, &map, GST_MAP_WRITE);
                memcpy(map.data, buf.data() + 5, pktLen);
                gst_buffer_unmap(gstbuf, &map);
                if (!g_appsrc || gst_app_src_push_buffer(g_appsrc, gstbuf) != GST_FLOW_OK)
                    gst_buffer_unref(gstbuf);
            }
        } else if (type == PKT_TYPE_YOLO) {
            std::string jsonStr((const char*)buf.data() + 5, pktLen);
            uint64_t fid = parse_yolo_frame_id(buf.data() + 5, pktLen);
            bool already_seen = false;
            {
                std::lock_guard<std::mutex> lk(g_seen_mtx);
                if (g_seen_yolo_frames.count(fid)) {
                    already_seen = true;
                } else {
                    g_seen_yolo_frames.insert(fid);
                    if (g_seen_yolo_frames.size() > SEEN_FRAMES_MAX)
                        g_seen_yolo_frames.erase(g_seen_yolo_frames.begin());
                }
            }
            if (!already_seen) {
                auto dets = parseYoloJson(buf.data() + 5, pktLen);
                { std::lock_guard<std::mutex> lock(yolo_mutex); g_latestDetections = std::move(dets); }
                sendToSiteWise(jsonStr);
                if (is_redundant_stream)
                    printf("[Redundant] stream8 복구: frame=%lu\n", fid);
            }
        }
        else if (type == PKT_TYPE_GPS) {
            /* GPS JSON 수신 및 출력 */
            std::string gpsJson((const char*)buf.data() + 5, pktLen);
            printf("[GPS 수신] %s\n", gpsJson.c_str());
 
            /* SiteWise로 GPS 전송 (선택사항)
             * sendToSiteWise()는 1초에 1번만 전송하므로
             * GPS는 별도 property ID가 필요하면 따로 전송 함수 만들어야 함
             * 일단은 로그만 출력 */
 
        }
        else if (type == PKT_TYPE_FRAME_META) {
            // ── [TiL] 프레임 메타 수신 ──────────────────────
            if (pktLen == sizeof(FrameMetaPacket)) {
                FrameMetaPacket fmeta;
                memcpy(&fmeta, buf.data() + 5, sizeof(fmeta));

                struct timeval tv;
                gettimeofday(&tv, nullptr);
                uint64_t t_rx = (uint64_t)tv.tv_sec * 1000000ULL + tv.tv_usec;
                
                // ← 추가
                {
                    std::lock_guard<std::mutex> lk(g_last_frame_rx_mtx);
                    if (t_rx > g_last_frame_rx_us)
                        g_last_frame_rx_us = t_rx;
                }

                int path = g_incoming_path;   // 0=Uu, 1=PC5

                {
                    std::lock_guard<std::mutex> lk(g_frame_mtx);
                    auto& entry = g_frame_map[fmeta.frame_id];
                    entry.t_tx_us      = fmeta.t_tx_us;
                    entry.scheduler_id = fmeta.scheduler_id;
                    entry.speed_mps    = fmeta.speed_mps;
                    entry.lat          = fmeta.lat;
                    entry.lon          = fmeta.lon;
                    if (path == 0)
                        entry.t_rx_uu_us  = t_rx;
                    else
                        entry.t_rx_pc5_us = t_rx;

                    // CSV: 개별 프레임 uplink latency 기록
                    if (g_csv_fp && entry.t_tx_us > 0) {
                        uint64_t rx_time = (path == 0) ? entry.t_rx_uu_us : entry.t_rx_pc5_us;
                        if (rx_time > 0 && rx_time > entry.t_tx_us) {   // ← 0이거나 역전되면 스킵
                            uint64_t lat_us = rx_time - entry.t_tx_us;
                            std::lock_guard<std::mutex> cl(g_csv_mtx);
                            fprintf(g_csv_fp,
                                "%llu,%llu,%.3f,%d\n",
                                (unsigned long long)fmeta.frame_id,
                                (unsigned long long)t_rx,
                                lat_us / 1000.0,
                                path);
                            fflush(g_csv_fp);
                        }
                    }
                }
            }
            // ── [TiL 끝] ────────────────────────────────────
        }
        else if (type == PKT_TYPE_ADV_ACK) {
            std::string ackJson((const char*)buf.data() + 5, pktLen);

            // epoch_ts, t_adv_sent_us, t_adv_rx_us 파싱
            uint64_t epoch_ts = 0, t_adv_sent_us = 0, t_adv_rx_us = 0;
            auto parse_u64 = [&](const char* key) -> uint64_t {
                size_t p = ackJson.find(key);
                if (p == std::string::npos) return 0;
                return strtoull(ackJson.c_str() + p + strlen(key), nullptr, 10);
            };
            epoch_ts      = parse_u64("\"epoch_ts\":");
            t_adv_sent_us = parse_u64("\"t_adv_sent_us\":");
            t_adv_rx_us   = parse_u64("\"t_adv_rx_us\":");

            g_ack_recv_count++;

            // 메트릭 계산
            double adv_return_ms = (t_adv_sent_us > 0 && t_adv_rx_us > t_adv_sent_us)
                ? (t_adv_rx_us - t_adv_sent_us) / 1000.0 : -1.0;

            uint64_t epoch_first_tx = 0;
            {
                std::lock_guard<std::mutex> lk(g_adv_mtx);
                auto it = g_adv_record.find(epoch_ts);
                if (it != g_adv_record.end()) {
                    epoch_first_tx = it->second.epoch_first_tx;
                    g_adv_record.erase(it);
                }
            }

            double total_e2e_ms = (epoch_first_tx > 0 && t_adv_rx_us > epoch_first_tx)
                ? (t_adv_rx_us - epoch_first_tx) / 1000.0 : -1.0;

            // 4번, 5번 누적
            {
                std::lock_guard<std::mutex> lk(g_metric_buf_mtx);
                if (adv_return_ms > 0) g_adv_return_buf.push_back(adv_return_ms);
                if (total_e2e_ms  > 0) g_e2e_buf.push_back(total_e2e_ms);
            }    

            int sent  = g_adv_sent_count.load();
            int acked = g_ack_recv_count.load();
            double ack_return_rate = sent > 0 ? (double)acked / sent * 100.0 : 0.0;

            // mean/p95 계산
            auto calc = [](std::vector<double>& v) -> std::pair<double,double> {
                if (v.empty()) return {0.0, 0.0};
                double m = 0; for (auto x : v) m += x; m /= v.size();
                std::vector<double> s = v; std::sort(s.begin(), s.end());
                return {m, s[(size_t)(s.size() * 0.95)]};
            };

            double adv_ret_mean, adv_ret_p95, e2e_mean, e2e_p95;
            {
                std::lock_guard<std::mutex> lk(g_metric_buf_mtx);
                auto [arm, arp] = calc(g_adv_return_buf);
                auto [em,  ep]  = calc(g_e2e_buf);
                adv_ret_mean = arm; adv_ret_p95 = arp;
                e2e_mean = em;      e2e_p95 = ep;
            }

            // epoch metrics 가져오기
            EpochMetrics em;
            {
                std::lock_guard<std::mutex> lk(g_epoch_metrics_mtx);
                em = g_last_epoch_metrics;
            }

            printf("[Advisory ACK] return=%.1fms e2e=%.1fms ack_return=%.1f%%\n",
       adv_return_ms, total_e2e_ms, ack_return_rate);
            
            // ── 통합 CSV 기록 ──────────────────────────────────
            if (g_all_csv_fp) {
                fprintf(g_all_csv_fp,
                    "%llu,"
                    "%.3f,%.3f,%.3f,%.3f,"
                    "%.3f,%.3f,%.3f,"        // ← epoch_wait_ms 추가
                    "%.3f,%.3f,%.3f,"
                    "%.3f,%.3f,%.3f\n",
                    (unsigned long long)epoch_ts,
                    em.uu_mean_ms, em.uu_p95_ms,
                    em.pc5_mean_ms, em.pc5_p95_ms,
                    em.twin_ms, em.adv_gen_ms, em.epoch_wait_ms,  // ← 추가
                    adv_return_ms, adv_ret_mean, adv_ret_p95,
                    total_e2e_ms, e2e_mean, e2e_p95);
                fflush(g_all_csv_fp);
            }
                

            if (g_cl_csv_fp) {
                fprintf(g_cl_csv_fp,
                    "%llu,%.3f,%.3f\n",
                    (unsigned long long)epoch_ts,
                    adv_return_ms, total_e2e_ms);
                fflush(g_cl_csv_fp);
            }
        }
        buf.erase(buf.begin(), buf.begin() + 5 + pktLen);
    }
}

int customQuicCallback(picoquic_cnx_t *cnx, uint64_t stream_id, uint8_t *bytes, size_t length,
                       picoquic_call_back_event_t event, void *callback_ctx, void *stream_ctx)
{
    g_server_cnx = cnx;

    if (event == picoquic_callback_stream_data && length > 0)
    {
    if (stream_id == 8) {
        g_recvBuf_s8.insert(g_recvBuf_s8.end(), bytes, bytes + length);
        processStreamBuf(g_recvBuf_s8, true);
    } else {
        g_recvBuf_s4.insert(g_recvBuf_s4.end(), bytes, bytes + length);
        processStreamBuf(g_recvBuf_s4, false);
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
    struct sockaddr_in *pc5_sin  = (struct sockaddr_in *)&pc5_remote_addr;

    // static std::atomic<int> pc5_count{0};
    // static std::atomic<int> uu_count{0};
    // static auto last_log = std::chrono::steady_clock::now();

    if (peer_sin->sin_addr.s_addr == pc5_sin->sin_addr.s_addr &&
        peer_sin->sin_port == pc5_sin->sin_port)
    {
        //pc5_count++;
        std::vector<uint8_t> pkt(sizeof(V2xMsgReq) + len);
        V2xMsgReq *p_req = reinterpret_cast<V2xMsgReq *>(pkt.data());
        p_req->length = len;
        p_req->v2xChannelId = g_channelID;
        memcpy(p_req->data, data, len);
        EFOS_SendV2xMsg(p_req);
    }
    else
    {
        //uu_count++;
        if (g_uu_sock >= 0)
        {
            sendto(g_uu_sock, data, len, 0,
                   (struct sockaddr *)peer_addr, sizeof(struct sockaddr_in));
        }
    }

    // auto now = std::chrono::steady_clock::now();
    // if (std::chrono::duration_cast<std::chrono::seconds>(now - last_log).count() >= 1)
    // {
    //     struct sockaddr_in *p = (struct sockaddr_in *)peer_addr;
    //     printf("[Receiver DISPATCH] PC5=%d, Uu=%d (마지막 peer: %s:%d)\n",
    //            pc5_count.exchange(0), uu_count.exchange(0),
    //            inet_ntoa(p->sin_addr), ntohs(p->sin_port));
    //     last_log = now;
    // }
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
        .v2xServiceId   = EFOS_V2X_SERVICE_ID_BCAST_PQI_TP,
        .v2xMsgFamilyId = EFOS_V2X_MSG_FAMILY_IEEE1609,
        .commMode       = EFOS_V2X_COMM_BROADCAST,
        .scheduleType   = EFOS_SCHEDULE_TYPE_EVENT,
        .txInterval     = 100.0,
        .mcsConf        = {0xFF, 0xFF},
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
    g_appsrc  = GST_APP_SRC(gst_bin_get_by_name(GST_BIN(pipeline), "src"));
    g_appsink = GST_APP_SINK(gst_bin_get_by_name(GST_BIN(pipeline), "sink"));
    gst_element_set_state(pipeline, GST_STATE_PLAYING);
}

GstElement *initGStreamerPipeline()
{
    std::string gst_pipeline =
        "appsrc name=src is-live=true format=time do-timestamp=true ! "
        "application/x-rtp,media=video,clock-rate=90000,encoding-name=H264,payload=96 ! "
        "rtpjitterbuffer latency=200 drop-on-latency=true ! "
        "rtph264depay ! h264parse config-interval=-1 ! "
        "video/x-h264,stream-format=avc,alignment=au ! "
        "kvssink "
        "stream-name=\"" + std::string(STREAM_NAME) + "\" "
        "aws-region=\""  + std::string(AWS_REGION)  + "\" "
        "storage-size=128 "
        "fragment-duration=1000 "
        "framerate=30 ";

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

    int flags = fcntl(sock, F_GETFL, 0);
    fcntl(sock, F_SETFL, flags | O_NONBLOCK);

    int opt = 1;
    setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in bind_addr;
    memset(&bind_addr, 0, sizeof(bind_addr));
    bind_addr.sin_family = AF_INET;
    inet_pton(AF_INET, UU_RECEIVER_IP, &bind_addr.sin_addr);
    bind_addr.sin_port = htons(UU_RECEIVER_PORT);

    if (bind(sock, (struct sockaddr *)&bind_addr, sizeof(bind_addr)) < 0)
    {
        printf("[Uu] 바인딩 실패 (%s:%d): %s\n",
               UU_RECEIVER_IP, UU_RECEIVER_PORT, strerror(errno));
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
    memset(&pc5_remote_addr, 0, sizeof(pc5_remote_addr));
    memset(&pc5_local_addr,  0, sizeof(pc5_local_addr));

    struct sockaddr_in *pc5_remote = (struct sockaddr_in *)&pc5_remote_addr;
    pc5_remote->sin_family = AF_INET;
    inet_pton(AF_INET, PC5_SENDER_IP, &pc5_remote->sin_addr);
    pc5_remote->sin_port = htons(12345);

    struct sockaddr_in *pc5_local = (struct sockaddr_in *)&pc5_local_addr;
    pc5_local->sin_family = AF_INET;
    inet_pton(AF_INET, PC5_RECEIVER_IP, &pc5_local->sin_addr);
    pc5_local->sin_port = htons(9999);
}

picoquic_quic_t *initQuic()
{
    uint64_t current_time = picoquic_current_time();

    picoquic_quic_t *quic = picoquic_create(
        8,
        "certs/cert.pem",
        "certs/key.pem",
        NULL,
        "v2x_test",
        customQuicCallback,
        NULL,
        NULL, NULL, NULL,
        current_time, NULL, NULL, NULL, 0);

    if (!quic)
    {
        printf("[QUIC] picoquic_create 실패\n");
        return nullptr;
    }

    quic->default_multipath_option = 1;
    quic->default_tp.initial_max_path_id = 4;

    printf("[QUIC] 서버 QUIC 엔진 초기화 완료 (Multipath 옵션: %d)\n",
           quic->default_multipath_option);

    return quic;
}

// ##########################THREAD###############################
void pathMonitorThread()
{
    static uint64_t last_pc5 = 0;
    static uint64_t last_uu  = 0;

    while (keepRunning)
    {
        std::this_thread::sleep_for(std::chrono::seconds(5));
        std::lock_guard<std::mutex> lock(quic_mutex);
        if (g_server_cnx)
        {
            printf("\n[상태] 경로=%d | Multipath=%s | KVS 전송 중 ✅\n",
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
            g_swQueueCv.notify_all();
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

        if (p_v2xMsg->length == 0) continue;
        if (p_v2xMsg->length <= 34) { small_count++; continue; }

        recv_count++;

        std::lock_guard<std::mutex> lock(quic_mutex);
        g_incoming_path = 1;
        uint64_t current_time = picoquic_current_time();

        int ret = picoquic_incoming_packet(quic,
                                           p_v2xMsg->data + 34, p_v2xMsg->length - 34,
                                           (struct sockaddr *)&pc5_remote_addr,
                                           (struct sockaddr *)&pc5_local_addr,
                                           0, 0, current_time);

        static bool ok_logged = false;
        if (ret == 0 && !ok_logged)
        {
            printf("[PC5 incoming] ★ 성공! len=%d\n", p_v2xMsg->length - 34);
            ok_logged = true;
        }

        auto now = std::chrono::steady_clock::now();
        if (std::chrono::duration_cast<std::chrono::seconds>(now - last_log).count() >= 1)
        {
            printf("[PC5 수신] 지난 1초: %d 패킷 (너무 짧은 것 %d개 제외)\n",
                   recv_count.exchange(0), small_count.exchange(0));
            last_log = now;
        }
    }

    gst_app_src_end_of_stream(g_appsrc);
    printf("[PC5 수신] 스레드 종료\n");
}

void uuRecvThread(picoquic_quic_t *quic)
{
    uint8_t rxBuf[2048];
    printf("[Uu 수신] UDP 수신 스레드 시작 (port %d)...\n", UU_RECEIVER_PORT);

    static std::atomic<int> recv_count{0};      // ← 추가
    static auto last_log = std::chrono::steady_clock::now();  // ← 추가

    while (keepRunning)
    {
        struct sockaddr_storage from_addr;
        socklen_t from_len = sizeof(from_addr);

        ssize_t recvLen = recvfrom(g_uu_sock, rxBuf, sizeof(rxBuf), 0,
                                   (struct sockaddr *)&from_addr, &from_len);

        if (recvLen > 0)
        {
            recv_count++;    // ← 추가

            struct sockaddr_in uu_local;
            memset(&uu_local, 0, sizeof(uu_local));
            uu_local.sin_family = AF_INET;
            inet_pton(AF_INET, UU_RECEIVER_IP, &uu_local.sin_addr);
            uu_local.sin_port = htons(UU_RECEIVER_PORT);

            std::lock_guard<std::mutex> lock(quic_mutex);
            g_incoming_path = 0; 
            picoquic_incoming_packet(quic,
                                     rxBuf, recvLen,
                                     (struct sockaddr *)&from_addr,
                                     (struct sockaddr *)&uu_local,
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

        // ← 아래 블록 추가
        auto now = std::chrono::steady_clock::now();
        if (std::chrono::duration_cast<std::chrono::seconds>(now - last_log).count() >= 1)
        {
            printf("[Uu 수신] 지난 1초: %d 패킷\n", recv_count.exchange(0));
            last_log = now;
        }
    }
    printf("[Uu 수신] UDP 수신 스레드 종료\n");
}

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
            int ret = picoquic_prepare_next_packet_ex(
                quic, picoquic_current_time(),
                sendBuf, sizeof(sendBuf), &sendLen,
                &peer_addr, &local_addr,
                &if_index, &log_cid, &last_cnx, &send_msg_size);

            if (ret != 0) continue;
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
    printf("[응답 송신] 스레드 종료\n");
}

void epochWorkerThread() {
    printf("[Epoch] 워커 스레드 시작 (100ms 슬라이딩 윈도우, 5초 데이터)\n");

    uint64_t last_til_send_us = 0;

    // ── warmup: sender 연결 + 데이터 안정화 대기 ──────────
    printf("[Epoch] warmup 대기 중 (5초)...\n");
    std::this_thread::sleep_for(std::chrono::seconds(5));
    printf("[Epoch] warmup 완료. CSV 기록 시작.\n");
    // ─────────────────────────────────────────────────────

    while (keepRunning) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        std::vector<double> uu_lat_ms, pc5_lat_ms;

        uint64_t now = picoquic_current_time();

        // ── epoch wait time 측정 ──────────────────────────
        struct timeval tv_epoch;
        gettimeofday(&tv_epoch, nullptr);
        uint64_t t_epoch_run_us = (uint64_t)tv_epoch.tv_sec * 1000000ULL
                                  + tv_epoch.tv_usec;
        double epoch_wait_ms = 0.0;
        {
            std::lock_guard<std::mutex> lk(g_last_frame_rx_mtx);
            if (g_last_frame_rx_us > 0 && t_epoch_run_us > g_last_frame_rx_us)
                epoch_wait_ms = (t_epoch_run_us - g_last_frame_rx_us) / 1000.0;
        }

        // ── epoch 내 프레임 집계 ──────────────────────────────
        int uu_total = 0,  uu_met = 0;
        int pc5_total = 0, pc5_met = 0;
        float speed_mps = 0.0f;
        double lat = 37.2830, lon = 127.0466;
        int sched_id = 1;

        {
            std::lock_guard<std::mutex> lk(g_frame_mtx);

            struct timeval tv_now;
            gettimeofday(&tv_now, nullptr);
            uint64_t now_unix_us = (uint64_t)tv_now.tv_sec * 1000000ULL + tv_now.tv_usec;
            uint64_t cutoff_us = now_unix_us - 5000000ULL;

            uint64_t last_tx = 0;

            for (auto& [fid, e] : g_frame_map) {
                if (e.t_tx_us == 0) continue;
                if (e.t_tx_us < cutoff_us) continue;

                if (e.t_tx_us > last_tx)
                    last_tx = e.t_tx_us;

                if (e.t_rx_uu_us > 0 && e.t_rx_uu_us > e.t_tx_us) {
                    uu_total++;
                    uint64_t uu_lat_us = e.t_rx_uu_us - e.t_tx_us;
                    uu_lat_ms.push_back(uu_lat_us / 1000.0);
                    if (uu_lat_us <= DEADLINE_US) uu_met++;
                }
                if (e.t_rx_pc5_us > 0 && e.t_rx_pc5_us > e.t_tx_us) {
                    pc5_total++;
                    uint64_t pc5_lat_us = e.t_rx_pc5_us - e.t_tx_us;
                    pc5_lat_ms.push_back(pc5_lat_us / 1000.0);
                    if (pc5_lat_us <= DEADLINE_US) pc5_met++;
                }
                speed_mps = e.speed_mps;
                lat       = e.lat;
                lon       = e.lon;
                sched_id  = e.scheduler_id;
            }

            g_epoch_last_tx = last_tx;

            for (auto it = g_frame_map.begin(); it != g_frame_map.end(); ) {
                if (it->second.t_tx_us > 0 && it->second.t_tx_us < cutoff_us)
                    it = g_frame_map.erase(it);
                else
                    ++it;
            }
        }  // ← g_frame_mtx 닫기

        auto calc_stats = [](std::vector<double>& v) -> std::pair<double,double> {
            if (v.empty()) return {0.0, 0.0};
            double mean = 0;
            for (auto x : v) mean += x;
            mean /= v.size();
            std::sort(v.begin(), v.end());
            double p95 = v[(size_t)(v.size() * 0.95)];
            return {mean, p95};
        };

        auto [uu_mean, uu_p95]   = calc_stats(uu_lat_ms);
        auto [pc5_mean, pc5_p95] = calc_stats(pc5_lat_ms);

        float uu_dmf  = (uu_total  > 0) ? (float)uu_met  / uu_total  : 0.0f;
        float pc5_dmf = (pc5_total > 0) ? (float)pc5_met / pc5_total : 0.0f;

        // ── picoquic 경로 RTT / jitter 추출 ───────────────────
        float d_uu_ms = 0, d_pc5_ms = 0;
        float uu_jit_ms = 0, pc5_jit_ms = 0;

        {
            std::lock_guard<std::mutex> lk(quic_mutex);
            if (g_server_cnx) {
                struct sockaddr_in* pc5_sin = (struct sockaddr_in*)&pc5_remote_addr;
                for (int i = 0; i < g_server_cnx->nb_paths; i++) {
                    auto* p = g_server_cnx->path[i];
                    struct sockaddr_in* peer = (struct sockaddr_in*)&p->registered_peer_addr;
                    float rtt_ms = p->smoothed_rtt / 1000.0f;
                    float jit_ms = p->rtt_variant  / 1000.0f;
                    if (peer->sin_addr.s_addr == pc5_sin->sin_addr.s_addr) {
                        d_pc5_ms = rtt_ms; pc5_jit_ms = jit_ms;
                    } else {
                        d_uu_ms  = rtt_ms; uu_jit_ms  = jit_ms;
                    }
                }
            }
        }

        // ── CSV: epoch 요약 기록 ───────────────────────────────
        {
            std::lock_guard<std::mutex> cl(g_csv_mtx);
            if (g_csv_fp) {
                fprintf(g_csv_fp,
                    "# epoch,%llu,%.4f,%.4f,%d,%d,%d,%d,%.3f,%.3f,%.3f,%.3f,%.3f,%.7f,%.7f,%d\n",
                    (unsigned long long)now,
                    uu_dmf, pc5_dmf,
                    uu_total, uu_met, pc5_total, pc5_met,
                    d_uu_ms, d_pc5_ms,
                    uu_jit_ms, pc5_jit_ms,
                    speed_mps, lat, lon, sched_id);
                fflush(g_csv_fp);
            }
        }

        // ── til_server 1초 throttle ───────────────────────────
        if (t_epoch_run_us - last_til_send_us < 1000000ULL) {
            printf("[Epoch] Uu=%d/%d(%.2f) PC5=%d/%d(%.2f) "
                   "d_uu=%.1fms d_pc5=%.1fms\n",
                   uu_met, uu_total, uu_dmf,
                   pc5_met, pc5_total, pc5_dmf,
                   d_uu_ms, d_pc5_ms);
            continue;
        }
        last_til_send_us = t_epoch_run_us;

        // ── til_server.py 에 epoch JSON 전송 ─────────────────
        if (g_til_sock >= 0) {
            char json_buf[512];
            int n = snprintf(json_buf, sizeof(json_buf),
                "{\"uu_deadline_met_frac\":%.4f,"
                "\"pc5_deadline_met_frac\":%.4f,"
                "\"p_uu\":%.4f,\"p_pc5\":%.4f,"
                "\"uu_jitter_ms\":%.3f,\"pc5_jitter_ms\":%.3f,"
                "\"d_uu_ms\":%.3f,\"d_pc5_ms\":%.3f,"
                "\"uu_completed_frames\":%d,"
                "\"pc5_completed_frames\":%d,"
                "\"scheduler_id\":%d,"
                "\"speed_mps\":%.3f,"
                "\"lat\":%.7f,\"lon\":%.7f,"
                "\"uu_mean_latency_ms\":%.3f,"
                "\"uu_p95_latency_ms\":%.3f,"
                "\"pc5_mean_latency_ms\":%.3f,"
                "\"pc5_p95_latency_ms\":%.3f,"
                "\"epoch_ts\":%llu}\n",
                uu_dmf, pc5_dmf,
                1.0f - uu_dmf, 1.0f - pc5_dmf,
                uu_jit_ms, pc5_jit_ms,
                d_uu_ms, d_pc5_ms,
                uu_total, pc5_total,
                sched_id,
                speed_mps, lat, lon,
                uu_mean, uu_p95, pc5_mean, pc5_p95,
                (unsigned long long)now);

            if (send(g_til_sock, json_buf, n, MSG_NOSIGNAL) < 0) {
                printf("[Epoch] til_server 연결 끊김. 재연결 시도...\n");
                close(g_til_sock);
                g_til_sock = -1;
            }
            else {
                struct timeval tv;
                tv.tv_sec  = 0;
                tv.tv_usec = 800000;
                setsockopt(g_til_sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

                char resp_buf[512] = {0};
                ssize_t rlen = recv(g_til_sock, resp_buf, sizeof(resp_buf)-1, 0);
                if (rlen > 0) {
                    resp_buf[rlen] = '\0';
                    std::string resp(resp_buf, rlen);

                    auto parse_dbl = [&](const char* key) -> double {
                        size_t p = resp.find(key);
                        if (p == std::string::npos) return 0.0;
                        return atof(resp.c_str() + p + strlen(key));
                    };

                    {
                        std::lock_guard<std::mutex> lk(g_epoch_metrics_mtx);
                        g_last_epoch_metrics.uu_mean_ms    = uu_mean;
                        g_last_epoch_metrics.uu_p95_ms     = uu_p95;
                        g_last_epoch_metrics.pc5_mean_ms   = pc5_mean;
                        g_last_epoch_metrics.pc5_p95_ms    = pc5_p95;
                        g_last_epoch_metrics.twin_ms       = parse_dbl("\"twin_update_ms\":");
                        g_last_epoch_metrics.adv_gen_ms    = parse_dbl("\"adv_gen_ms\":");
                        g_last_epoch_metrics.epoch_wait_ms = epoch_wait_ms;
                    }

                    {
                        std::lock_guard<std::mutex> lk(g_adv_mtx);
                        EpochAdvisoryRecord rec;
                        rec.t_adv_sent_us  = 0;
                        rec.epoch_first_tx = g_epoch_last_tx;
                        g_adv_record[(uint64_t)now] = rec;
                    }
                    g_adv_sent_count++;

                    uint8_t type = PKT_TYPE_ADVISORY;
                    uint32_t net_len = htonl((uint32_t)rlen);
                    std::lock_guard<std::mutex> lk(quic_mutex);
                    if (g_server_cnx) {
                        picoquic_add_to_stream(g_server_cnx, ADVISORY_STREAM_ID,
                            &type, 1, 0);
                        picoquic_add_to_stream(g_server_cnx, ADVISORY_STREAM_ID,
                            (const uint8_t*)&net_len, 4, 0);
                        picoquic_add_to_stream(g_server_cnx, ADVISORY_STREAM_ID,
                            (const uint8_t*)resp_buf, (size_t)rlen, 0);
                    }
                }
            }
        }

        printf("[Epoch] Uu=%d/%d(%.2f) PC5=%d/%d(%.2f) "
               "d_uu=%.1fms d_pc5=%.1fms\n",
               uu_met, uu_total, uu_dmf,
               pc5_met, pc5_total, pc5_dmf,
               d_uu_ms, d_pc5_ms);
    }  // ← while 닫기

    printf("[Epoch] 워커 스레드 종료\n");
}  // ← 함수 닫기

void tilConnectThread() {
    printf("[TiL] til_server 연결 스레드 시작\n");
    while (keepRunning) {
        if (g_til_sock >= 0) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }

        int s = socket(AF_INET, SOCK_STREAM, 0);
        if (s < 0) { sleep(2); continue; }

        struct sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port   = htons(7777);
        inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

        if (connect(s, (struct sockaddr*)&addr, sizeof(addr)) == 0) {
            g_til_sock = s;
            printf("[TiL] til_server 연결 성공\n");
        } else {
            close(s);
            printf("[TiL] til_server 연결 실패. 3초 후 재시도...\n");
            std::this_thread::sleep_for(std::chrono::seconds(3));
        }
    }
}

// ##########################MAIN###############################
int main(int argc, char **argv)
{
    setenv("GST_DEBUG", "0", 1);         
    setenv("AWS_LOG_LEVEL", "WARN", 1);  
    gst_init(&argc, &argv);

    initAddresses();

    auto v2xmsgregi = initSirius();
    if (!v2xmsgregi) { printf("V2X initialize ERROR\n"); return -1; }

    g_uu_sock = initUuSocket();
    if (g_uu_sock < 0) { printf("Uu 소켓 초기화 실패\n"); return -1; }

    GstElement *pipeline = initGStreamerPipeline();
    if (!pipeline) { printf("GStreamer 파이프라인 초기화 실패\n"); return -1; }
    initGStreamerApp(pipeline);

    picoquic_quic_t *quic = initQuic();
    if (!quic) { printf("QUIC 초기화 실패\n"); return -1; }

    // ── [TiL] CSV 파일 열기 ──
        g_csv_fp = fopen("/home/loapp/sirius/receiver_SiteWise/til_latency.csv", "w");  // ← 추가
        if (g_csv_fp) {
            fprintf(g_csv_fp, "frame_id,t_rx_us,uplink_latency_ms,path\n");
        }

        g_cl_csv_fp = fopen("/home/loapp/sirius/receiver_SiteWise/til_closed_loop.csv", "w");
        if (g_cl_csv_fp) {
            fprintf(g_cl_csv_fp, "epoch_ts,adv_return_ms,total_e2e_ms,recv_rate_pct,closed_rate_pct\n");
        }
        g_all_csv_fp = fopen("/home/loapp/sirius/receiver_SiteWise/til_all_metrics.csv", "w");
        if (g_all_csv_fp) {
            fprintf(g_all_csv_fp,
                "epoch_ts,"
                "uu_uplink_mean_ms,uu_uplink_p95_ms,"
                "pc5_uplink_mean_ms,pc5_uplink_p95_ms,"
                "twin_update_ms,adv_gen_ms,epoch_wait_ms,"   // ← 추가
                "adv_return_ms,adv_return_mean_ms,adv_return_p95_ms,"
                "total_e2e_ms,e2e_mean_ms,e2e_p95_ms\n");
        }

    // 스레드 시작
    std::thread input_thread(inputThread);
    std::thread pc5_recv_thread(pc5RecvThread, quic);
    std::thread uu_recv_thread(uuRecvThread, quic);
    std::thread response_thread(quicResponseThread, quic);
    std::thread path_monitor_thread(pathMonitorThread);
    std::thread sitewise_thread(siteWiseWorkerThread);  // ★ SiteWise 워커
    std::thread epoch_thread(epochWorkerThread);     // ← [TiL 추가]
    std::thread til_conn_thread(tilConnectThread);   // ← [TiL 추가]

    printf("[Main] 모든 스레드 시작 완료. 수신 대기 중...\n");

    while (keepRunning)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    // 종료
    keepRunning = false;
    g_swQueueCv.notify_all();

    pc5_recv_thread.join();
    uu_recv_thread.join();
    input_thread.join();
    response_thread.join();
    path_monitor_thread.join();
    sitewise_thread.join();
    epoch_thread.join();      // ← [TiL 추가]
    til_conn_thread.join();   // ← [TiL 추가]

    if (g_til_sock >= 0) close(g_til_sock);   // ← [TiL 추가]
    if (g_csv_fp) fclose(g_csv_fp);           // ← [TiL 추가]
    if (g_cl_csv_fp) fclose(g_cl_csv_fp);
    if (g_all_csv_fp) fclose(g_all_csv_fp);

    gst_object_unref(g_appsrc);
    gst_object_unref(g_appsink);
    gst_element_set_state(pipeline, GST_STATE_NULL);
    gst_object_unref(pipeline);

    picoquic_free(quic);
    if (g_uu_sock >= 0) close(g_uu_sock);

    cv::destroyAllWindows();
    EFOS_TerminateV2x(v2xmsgregi->v2xServiceId, v2xmsgregi->commMode);
    return 0;
}