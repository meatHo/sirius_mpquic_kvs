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

std::mutex quic_mutex;
static picoquic_cnx_t* g_cnx = nullptr;

static GstAppSrc* g_appsrc = nullptr;
static GstAppSink* g_appsink = nullptr;

// ===================== 패킷 타입 정의 =====================
#define PKT_TYPE_RTP   0x01
#define PKT_TYPE_YOLO  0x02

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

// ===================== YOLO 설정 (Open Images V7 모델) =====================
#define YOLO_MODEL_ONNX "/home/icons-linux/Documents/yolo/yolov8n-oiv7.onnx"
#define YOLO_INPUT_SIZE 640
#define YOLO_CONF_THRESHOLD 0.25f
#define YOLO_NMS_THRESHOLD  0.45f

// ★ Open Images V7 클래스 (601개)
// 출처: ultralytics/cfg/datasets/open-images-v7.yaml
// 순서 변경 금지!
// NUM_CLASSES = 601
static const char* OIV7_CLASSES[] = {
    "Accordion",  // 0
    "Adhesive tape",  // 1
    "Aircraft",  // 2
    "Airplane",  // 3
    "Alarm clock",  // 4
    "Alpaca",  // 5
    "Ambulance",  // 6
    "Animal",  // 7
    "Ant",  // 8
    "Antelope",  // 9
    "Apple",  // 10
    "Armadillo",  // 11
    "Artichoke",  // 12
    "Auto part",  // 13
    "Axe",  // 14
    "Backpack",  // 15
    "Bagel",  // 16
    "Baked goods",  // 17
    "Balance beam",  // 18
    "Ball",  // 19
    "Balloon",  // 20
    "Banana",  // 21
    "Band-aid",  // 22
    "Banjo",  // 23
    "Barge",  // 24
    "Barrel",  // 25
    "Baseball bat",  // 26
    "Baseball glove",  // 27
    "Bat (Animal)",  // 28
    "Bathroom accessory",  // 29
    "Bathroom cabinet",  // 30
    "Bathtub",  // 31
    "Beaker",  // 32
    "Bear",  // 33
    "Bed",  // 34
    "Bee",  // 35
    "Beehive",  // 36
    "Beer",  // 37
    "Beetle",  // 38
    "Bell pepper",  // 39
    "Belt",  // 40
    "Bench",  // 41
    "Bicycle",  // 42
    "Bicycle helmet",  // 43
    "Bicycle wheel",  // 44
    "Bidet",  // 45
    "Billboard",  // 46
    "Billiard table",  // 47
    "Binoculars",  // 48
    "Bird",  // 49
    "Blender",  // 50
    "Blue jay",  // 51
    "Boat",  // 52
    "Bomb",  // 53
    "Book",  // 54
    "Bookcase",  // 55
    "Boot",  // 56
    "Bottle",  // 57
    "Bottle opener",  // 58
    "Bow and arrow",  // 59
    "Bowl",  // 60
    "Bowling equipment",  // 61
    "Box",  // 62
    "Boy",  // 63
    "Brassiere",  // 64
    "Bread",  // 65
    "Briefcase",  // 66
    "Broccoli",  // 67
    "Bronze sculpture",  // 68
    "Brown bear",  // 69
    "Building",  // 70
    "Bull",  // 71
    "Burrito",  // 72
    "Bus",  // 73
    "Bust",  // 74
    "Butterfly",  // 75
    "Cabbage",  // 76
    "Cabinetry",  // 77
    "Cake",  // 78
    "Cake stand",  // 79
    "Calculator",  // 80
    "Camel",  // 81
    "Camera",  // 82
    "Can opener",  // 83
    "Canary",  // 84
    "Candle",  // 85
    "Candy",  // 86
    "Cannon",  // 87
    "Canoe",  // 88
    "Cantaloupe",  // 89
    "Car",  // 90
    "Carnivore",  // 91
    "Carrot",  // 92
    "Cart",  // 93
    "Cassette deck",  // 94
    "Castle",  // 95
    "Cat",  // 96
    "Cat furniture",  // 97
    "Caterpillar",  // 98
    "Cattle",  // 99
    "Ceiling fan",  // 100
    "Cello",  // 101
    "Centipede",  // 102
    "Chainsaw",  // 103
    "Chair",  // 104
    "Cheese",  // 105
    "Cheetah",  // 106
    "Chest of drawers",  // 107
    "Chicken",  // 108
    "Chime",  // 109
    "Chisel",  // 110
    "Chopsticks",  // 111
    "Christmas tree",  // 112
    "Clock",  // 113
    "Closet",  // 114
    "Clothing",  // 115
    "Coat",  // 116
    "Cocktail",  // 117
    "Cocktail shaker",  // 118
    "Coconut",  // 119
    "Coffee",  // 120
    "Coffee cup",  // 121
    "Coffee table",  // 122
    "Coffeemaker",  // 123
    "Coin",  // 124
    "Common fig",  // 125
    "Common sunflower",  // 126
    "Computer keyboard",  // 127
    "Computer monitor",  // 128
    "Computer mouse",  // 129
    "Container",  // 130
    "Convenience store",  // 131
    "Cookie",  // 132
    "Cooking spray",  // 133
    "Corded phone",  // 134
    "Cosmetics",  // 135
    "Couch",  // 136
    "Countertop",  // 137
    "Cowboy hat",  // 138
    "Crab",  // 139
    "Cream",  // 140
    "Cricket ball",  // 141
    "Crocodile",  // 142
    "Croissant",  // 143
    "Crown",  // 144
    "Crutch",  // 145
    "Cucumber",  // 146
    "Cupboard",  // 147
    "Curtain",  // 148
    "Cutting board",  // 149
    "Dagger",  // 150
    "Dairy Product",  // 151
    "Deer",  // 152
    "Desk",  // 153
    "Dessert",  // 154
    "Diaper",  // 155
    "Dice",  // 156
    "Digital clock",  // 157
    "Dinosaur",  // 158
    "Dishwasher",  // 159
    "Dog",  // 160
    "Dog bed",  // 161
    "Doll",  // 162
    "Dolphin",  // 163
    "Door",  // 164
    "Door handle",  // 165
    "Doughnut",  // 166
    "Dragonfly",  // 167
    "Drawer",  // 168
    "Dress",  // 169
    "Drill (Tool)",  // 170
    "Drink",  // 171
    "Drinking straw",  // 172
    "Drum",  // 173
    "Duck",  // 174
    "Dumbbell",  // 175
    "Eagle",  // 176
    "Earrings",  // 177
    "Egg (Food)",  // 178
    "Elephant",  // 179
    "Envelope",  // 180
    "Eraser",  // 181
    "Face powder",  // 182
    "Facial tissue holder",  // 183
    "Falcon",  // 184
    "Fashion accessory",  // 185
    "Fast food",  // 186
    "Fax",  // 187
    "Fedora",  // 188
    "Filing cabinet",  // 189
    "Fire hydrant",  // 190
    "Fireplace",  // 191
    "Fish",  // 192
    "Flag",  // 193
    "Flashlight",  // 194
    "Flower",  // 195
    "Flowerpot",  // 196
    "Flute",  // 197
    "Flying disc",  // 198
    "Food",  // 199
    "Food processor",  // 200
    "Football",  // 201
    "Football helmet",  // 202
    "Footwear",  // 203
    "Fork",  // 204
    "Fountain",  // 205
    "Fox",  // 206
    "French fries",  // 207
    "French horn",  // 208
    "Frog",  // 209
    "Fruit",  // 210
    "Frying pan",  // 211
    "Furniture",  // 212
    "Garden Asparagus",  // 213
    "Gas stove",  // 214
    "Giraffe",  // 215
    "Girl",  // 216
    "Glasses",  // 217
    "Glove",  // 218
    "Goat",  // 219
    "Goggles",  // 220
    "Goldfish",  // 221
    "Golf ball",  // 222
    "Golf cart",  // 223
    "Gondola",  // 224
    "Goose",  // 225
    "Grape",  // 226
    "Grapefruit",  // 227
    "Grinder",  // 228
    "Guacamole",  // 229
    "Guitar",  // 230
    "Hair dryer",  // 231
    "Hair spray",  // 232
    "Hamburger",  // 233
    "Hammer",  // 234
    "Hamster",  // 235
    "Hand dryer",  // 236
    "Handbag",  // 237
    "Handgun",  // 238
    "Harbor seal",  // 239
    "Harmonica",  // 240
    "Harp",  // 241
    "Harpsichord",  // 242
    "Hat",  // 243
    "Headphones",  // 244
    "Heater",  // 245
    "Hedgehog",  // 246
    "Helicopter",  // 247
    "Helmet",  // 248
    "High heels",  // 249
    "Hiking equipment",  // 250
    "Hippopotamus",  // 251
    "Home appliance",  // 252
    "Honeycomb",  // 253
    "Horizontal bar",  // 254
    "Horse",  // 255
    "Hot dog",  // 256
    "House",  // 257
    "Houseplant",  // 258
    "Human arm",  // 259
    "Human beard",  // 260
    "Human body",  // 261
    "Human ear",  // 262
    "Human eye",  // 263
    "Human face",  // 264
    "Human foot",  // 265
    "Human hair",  // 266
    "Human hand",  // 267
    "Human head",  // 268
    "Human leg",  // 269
    "Human mouth",  // 270
    "Human nose",  // 271
    "Humidifier",  // 272
    "Ice cream",  // 273
    "Indoor rower",  // 274
    "Infant bed",  // 275
    "Insect",  // 276
    "Invertebrate",  // 277
    "Ipod",  // 278
    "Isopod",  // 279
    "Jacket",  // 280
    "Jacuzzi",  // 281
    "Jaguar (Animal)",  // 282
    "Jeans",  // 283
    "Jellyfish",  // 284
    "Jet ski",  // 285
    "Jug",  // 286
    "Juice",  // 287
    "Kangaroo",  // 288
    "Kettle",  // 289
    "Kitchen & dining room table",  // 290
    "Kitchen appliance",  // 291
    "Kitchen knife",  // 292
    "Kitchen utensil",  // 293
    "Kitchenware",  // 294
    "Kite",  // 295
    "Knife",  // 296
    "Koala",  // 297
    "Ladder",  // 298
    "Ladle",  // 299
    "Ladybug",  // 300
    "Lamp",  // 301
    "Land vehicle",  // 302
    "Lantern",  // 303
    "Laptop",  // 304
    "Lavender (Plant)",  // 305
    "Lemon",  // 306
    "Leopard",  // 307
    "Light bulb",  // 308
    "Light switch",  // 309
    "Lighthouse",  // 310
    "Lily",  // 311
    "Limousine",  // 312
    "Lion",  // 313
    "Lipstick",  // 314
    "Lizard",  // 315
    "Lobster",  // 316
    "Loveseat",  // 317
    "Luggage and bags",  // 318
    "Lynx",  // 319
    "Magpie",  // 320
    "Mammal",  // 321
    "Man",  // 322
    "Mango",  // 323
    "Maple",  // 324
    "Maracas",  // 325
    "Marine invertebrates",  // 326
    "Marine mammal",  // 327
    "Measuring cup",  // 328
    "Mechanical fan",  // 329
    "Medical equipment",  // 330
    "Microphone",  // 331
    "Microwave oven",  // 332
    "Milk",  // 333
    "Miniskirt",  // 334
    "Mirror",  // 335
    "Missile",  // 336
    "Mixer",  // 337
    "Mixing bowl",  // 338
    "Mobile phone",  // 339
    "Monkey",  // 340
    "Moths and butterflies",  // 341
    "Motorcycle",  // 342
    "Mouse",  // 343
    "Muffin",  // 344
    "Mug",  // 345
    "Mule",  // 346
    "Mushroom",  // 347
    "Musical instrument",  // 348
    "Musical keyboard",  // 349
    "Nail (Construction)",  // 350
    "Necklace",  // 351
    "Nightstand",  // 352
    "Oboe",  // 353
    "Office building",  // 354
    "Office supplies",  // 355
    "Orange",  // 356
    "Organ (Musical Instrument)",  // 357
    "Ostrich",  // 358
    "Otter",  // 359
    "Oven",  // 360
    "Owl",  // 361
    "Oyster",  // 362
    "Paddle",  // 363
    "Palm tree",  // 364
    "Pancake",  // 365
    "Panda",  // 366
    "Paper cutter",  // 367
    "Paper towel",  // 368
    "Parachute",  // 369
    "Parking meter",  // 370
    "Parrot",  // 371
    "Pasta",  // 372
    "Pastry",  // 373
    "Peach",  // 374
    "Pear",  // 375
    "Pen",  // 376
    "Pencil case",  // 377
    "Pencil sharpener",  // 378
    "Penguin",  // 379
    "Perfume",  // 380
    "Person",  // 381
    "Personal care",  // 382
    "Personal flotation device",  // 383
    "Piano",  // 384
    "Picnic basket",  // 385
    "Picture frame",  // 386
    "Pig",  // 387
    "Pillow",  // 388
    "Pineapple",  // 389
    "Pitcher (Container)",  // 390
    "Pizza",  // 391
    "Pizza cutter",  // 392
    "Plant",  // 393
    "Plastic bag",  // 394
    "Plate",  // 395
    "Platter",  // 396
    "Plumbing fixture",  // 397
    "Polar bear",  // 398
    "Pomegranate",  // 399
    "Popcorn",  // 400
    "Porch",  // 401
    "Porcupine",  // 402
    "Poster",  // 403
    "Potato",  // 404
    "Power plugs and sockets",  // 405
    "Pressure cooker",  // 406
    "Pretzel",  // 407
    "Printer",  // 408
    "Pumpkin",  // 409
    "Punching bag",  // 410
    "Rabbit",  // 411
    "Raccoon",  // 412
    "Racket",  // 413
    "Radish",  // 414
    "Ratchet (Device)",  // 415
    "Raven",  // 416
    "Rays and skates",  // 417
    "Red panda",  // 418
    "Refrigerator",  // 419
    "Remote control",  // 420
    "Reptile",  // 421
    "Rhinoceros",  // 422
    "Rifle",  // 423
    "Ring binder",  // 424
    "Rocket",  // 425
    "Roller skates",  // 426
    "Rose",  // 427
    "Rugby ball",  // 428
    "Ruler",  // 429
    "Salad",  // 430
    "Salt and pepper shakers",  // 431
    "Sandal",  // 432
    "Sandwich",  // 433
    "Saucer",  // 434
    "Saxophone",  // 435
    "Scale",  // 436
    "Scarf",  // 437
    "Scissors",  // 438
    "Scoreboard",  // 439
    "Scorpion",  // 440
    "Screwdriver",  // 441
    "Sculpture",  // 442
    "Sea lion",  // 443
    "Sea turtle",  // 444
    "Seafood",  // 445
    "Seahorse",  // 446
    "Seat belt",  // 447
    "Segway",  // 448
    "Serving tray",  // 449
    "Sewing machine",  // 450
    "Shark",  // 451
    "Sheep",  // 452
    "Shelf",  // 453
    "Shellfish",  // 454
    "Shirt",  // 455
    "Shorts",  // 456
    "Shotgun",  // 457
    "Shower",  // 458
    "Shrimp",  // 459
    "Sink",  // 460
    "Skateboard",  // 461
    "Ski",  // 462
    "Skirt",  // 463
    "Skull",  // 464
    "Skunk",  // 465
    "Skyscraper",  // 466
    "Slow cooker",  // 467
    "Snack",  // 468
    "Snail",  // 469
    "Snake",  // 470
    "Snowboard",  // 471
    "Snowman",  // 472
    "Snowmobile",  // 473
    "Snowplow",  // 474
    "Soap dispenser",  // 475
    "Sock",  // 476
    "Sofa bed",  // 477
    "Sombrero",  // 478
    "Sparrow",  // 479
    "Spatula",  // 480
    "Spice rack",  // 481
    "Spider",  // 482
    "Spoon",  // 483
    "Sports equipment",  // 484
    "Sports uniform",  // 485
    "Squash (Plant)",  // 486
    "Squid",  // 487
    "Squirrel",  // 488
    "Stairs",  // 489
    "Stapler",  // 490
    "Starfish",  // 491
    "Stationary bicycle",  // 492
    "Stethoscope",  // 493
    "Stool",  // 494
    "Stop sign",  // 495
    "Strawberry",  // 496
    "Street light",  // 497
    "Stretcher",  // 498
    "Studio couch",  // 499
    "Submarine",  // 500
    "Submarine sandwich",  // 501
    "Suit",  // 502
    "Suitcase",  // 503
    "Sun hat",  // 504
    "Sunglasses",  // 505
    "Surfboard",  // 506
    "Sushi",  // 507
    "Swan",  // 508
    "Swim cap",  // 509
    "Swimming pool",  // 510
    "Swimwear",  // 511
    "Sword",  // 512
    "Syringe",  // 513
    "Table",  // 514
    "Table tennis racket",  // 515
    "Tablet computer",  // 516
    "Tableware",  // 517
    "Taco",  // 518
    "Tank",  // 519
    "Tap",  // 520
    "Tart",  // 521
    "Taxi",  // 522
    "Tea",  // 523
    "Teapot",  // 524
    "Teddy bear",  // 525
    "Telephone",  // 526
    "Television",  // 527
    "Tennis ball",  // 528
    "Tennis racket",  // 529
    "Tent",  // 530
    "Tiara",  // 531
    "Tick",  // 532
    "Tie",  // 533
    "Tiger",  // 534
    "Tin can",  // 535
    "Tire",  // 536
    "Toaster",  // 537
    "Toilet",  // 538
    "Toilet paper",  // 539
    "Tomato",  // 540
    "Tool",  // 541
    "Toothbrush",  // 542
    "Torch",  // 543
    "Tortoise",  // 544
    "Towel",  // 545
    "Tower",  // 546
    "Toy",  // 547
    "Traffic light",  // 548
    "Traffic sign",  // 549
    "Train",  // 550
    "Training bench",  // 551
    "Treadmill",  // 552
    "Tree",  // 553
    "Tree house",  // 554
    "Tripod",  // 555
    "Trombone",  // 556
    "Trousers",  // 557
    "Truck",  // 558
    "Trumpet",  // 559
    "Turkey",  // 560
    "Turtle",  // 561
    "Umbrella",  // 562
    "Unicycle",  // 563
    "Van",  // 564
    "Vase",  // 565
    "Vegetable",  // 566
    "Vehicle",  // 567
    "Vehicle registration plate",  // 568
    "Violin",  // 569
    "Volleyball (Ball)",  // 570
    "Waffle",  // 571
    "Waffle iron",  // 572
    "Wall clock",  // 573
    "Wardrobe",  // 574
    "Washing machine",  // 575
    "Waste container",  // 576
    "Watch",  // 577
    "Watercraft",  // 578
    "Watermelon",  // 579
    "Weapon",  // 580
    "Whale",  // 581
    "Wheel",  // 582
    "Wheelchair",  // 583
    "Whisk",  // 584
    "Whiteboard",  // 585
    "Willow",  // 586
    "Window",  // 587
    "Window blind",  // 588
    "Wine",  // 589
    "Wine glass",  // 590
    "Wine rack",  // 591
    "Winter melon",  // 592
    "Wok",  // 593
    "Woman",  // 594
    "Wood-burning stove",  // 595
    "Woodpecker",  // 596
    "Worm",  // 597
    "Wrench",  // 598
    "Zebra",  // 599
    "Zucchini"  // 600
};

static const int NUM_CLASSES = sizeof(OIV7_CLASSES) / sizeof(OIV7_CLASSES[0]);  // 601

// ★ 차량 환경 인식용 관심 클래스 (Open Images V7 인덱스)
// 인덱스는 위 배열 순서대로 0부터 시작
// 필요하면 여기만 수정하면 됩니다.
// ★ 통신 환경 판단용 관심 클래스 (Open Images V7 인덱스)
// 전파 차단/반사/간섭 요인이 될 수 있는 객체들
static const std::set<int> INTERESTED_CLASSES = {
    // ===== 건물/구조물 (LOS 차단 주요인) =====
    70,   // Building
    95,   // Castle
    257,  // House
    310,  // Lighthouse
    354,  // Office building
    466,  // Skyscraper
    546,  // Tower
    554,  // Tree house

    // ===== 큰 차량 (동적 차폐, 멀티패스 유발) =====
    73,   // Bus
    312,  // Limousine
    519,  // Tank       (군용 - 드물지만 큰 금속체)
    550,  // Train
    558,  // Truck
    564,  // Van
    6,    // Ambulance  (큰 차량 + 이동 신호원)

    // ===== 일반 차량 (참고용, 필요없으면 빼세요) =====
    90,   // Car
    522,  // Taxi

    // ===== 대형 자연 장애물 =====
    553,  // Tree        (잎/가지가 고주파 감쇠 유발)

    // ===== 기타 큰 구조물/인프라 =====
    46,   // Billboard   (금속판 → 반사체)
    497,  // Street light(가로등 - 작지만 밀집시 영향)
    205,  // Fountain    (큰 구조물일 수 있음)
};

// ※ 위 인덱스는 배열을 직접 세어본 값이 아니라 추정치라, 오타가 있을 수 있습니다.
// ※ 처음 실행하면 로그에 각 인덱스와 이름이 찍히니 그때 확인/수정해주세요.

cv::dnn::Net g_yoloNet;

// ===================== YOLO 디텍션 결과 구조체 =====================
struct Detection {
    int classId;
    float confidence;
    cv::Rect bbox;
};

// ===================== YOLO 스레드 공유 데이터 =====================
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

// ===================== YOLO 후처리 =====================
std::vector<Detection> postprocessYolo(const std::vector<cv::Mat>& outputs,
                                        int imgW, int imgH) {
    std::vector<int> classIds;
    std::vector<float> confidences;
    std::vector<cv::Rect> boxes;

    cv::Mat output = outputs[0];

    static bool shape_logged = false;
    if (!shape_logged) {
        printf("[YOLO] 출력 dims=%d, shape=[", output.dims);
        for (int i = 0; i < output.dims; i++) {
            printf("%d%s", output.size[i], (i < output.dims - 1) ? "," : "");
        }
        printf("]\n");
        printf("[YOLO] NUM_CLASSES=%d\n", NUM_CLASSES);
        shape_logged = true;
    }

    if (output.dims != 3) {
        return {};
    }

    // Open Images V7: [1, 605, 8400]  (4 bbox + 601 classes)
    int cols = output.size[1];  // 605
    int rows = output.size[2];  // 8400

    if (cols < 4 + NUM_CLASSES) {
        printf("[YOLO] 예상치 못한 출력 shape (cols=%d, expected >= %d)\n",
               cols, 4 + NUM_CLASSES);
        return {};
    }

    cv::Mat reshaped(cols, rows, CV_32F, output.ptr<float>());
    cv::Mat det = reshaped.t();

    float scaleX = (float)imgW / YOLO_INPUT_SIZE;
    float scaleY = (float)imgH / YOLO_INPUT_SIZE;

    for (int i = 0; i < rows; i++) {
        const float* row = det.ptr<float>(i);
        float cx = row[0];
        float cy = row[1];
        float w  = row[2];
        float h  = row[3];

        float maxConf = 0;
        int maxIdx = 0;
        for (int c = 0; c < NUM_CLASSES; c++) {
            if (row[4 + c] > maxConf) {
                maxConf = row[4 + c];
                maxIdx = c;
            }
        }

        if (maxConf < YOLO_CONF_THRESHOLD) continue;

        // ★ 관심 클래스만 필터링 (비어있으면 전체 허용)
        if (!INTERESTED_CLASSES.empty() &&
            INTERESTED_CLASSES.find(maxIdx) == INTERESTED_CLASSES.end()) {
            continue;
        }

        int x = (int)((cx - w / 2) * scaleX);
        int y = (int)((cy - h / 2) * scaleY);
        int bw = (int)(w * scaleX);
        int bh = (int)(h * scaleY);

        classIds.push_back(maxIdx);
        confidences.push_back(maxConf);
        boxes.push_back(cv::Rect(x, y, bw, bh));
    }

    std::vector<int> indices;
    cv::dnn::NMSBoxes(boxes, confidences, YOLO_CONF_THRESHOLD, YOLO_NMS_THRESHOLD, indices);

    std::vector<Detection> results;
    for (int idx : indices) {
        Detection d;
        d.classId = classIds[idx];
        d.confidence = confidences[idx];
        d.bbox = boxes[idx];
        results.push_back(d);
    }
    return results;
}

// ===================== 디텍션 결과 → JSON 직렬화 =====================
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

// ===================== 디텍션 결과 그리기 =====================
void drawDetections(cv::Mat& frame, const std::vector<Detection>& dets) {
    for (const auto& d : dets) {
        cv::rectangle(frame, d.bbox, cv::Scalar(0, 255, 0), 2);
        char label[128];
        snprintf(label, sizeof(label), "%s %.0f%%",
                 (d.classId < NUM_CLASSES) ? OIV7_CLASSES[d.classId] : "?",
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


// ===================== QUIC 전송 =====================
void sendTypedData(uint8_t type, const uint8_t* data, uint32_t len) {
    uint32_t net_len = htonl(len);
    std::lock_guard<std::mutex> lock(quic_mutex);
    if (g_cnx) {
        picoquic_add_to_stream(g_cnx, 4, &type, 1, 0);
        picoquic_add_to_stream(g_cnx, 4, (const uint8_t*)&net_len, 4, 0);
        picoquic_add_to_stream(g_cnx, 4, data, len, 0);
    }
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

    if (peer_sin->sin_addr.s_addr == pc5_sin->sin_addr.s_addr &&
        peer_sin->sin_port == pc5_sin->sin_port) {
        std::vector<uint8_t> pkt(sizeof(V2xMsgReq) + len);
        V2xMsgReq* p_req = reinterpret_cast<V2xMsgReq*>(pkt.data());
        p_req->v2xChannelId = g_channelID;
        p_req->length = (U16)len;
        memcpy(p_req->data, data, len);
        EFOS_SendV2xMsg(p_req);
    } else {
        if (g_uu_sock >= 0) {
        ssize_t ret = sendto(g_uu_sock, data, len, 0,
               (struct sockaddr*)peer_addr, sizeof(struct sockaddr_in));
        if (ret < 0) {
            printf("[Uu 송신] sendto 실패: %s (peer: %s:%d)\n",
                strerror(errno),
                inet_ntoa(((struct sockaddr_in*)peer_addr)->sin_addr),
                ntohs(((struct sockaddr_in*)peer_addr)->sin_port));
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
        printf("[YOLO] 클래스 수: %d, 관심 클래스: %zu개\n",
               NUM_CLASSES, INTERESTED_CLASSES.size());

        // ★ 관심 클래스 목록 출력 (인덱스 검증용)
        /*printf("[YOLO] ---- 관심 클래스 목록 ----\n");
        for (int idx : INTERESTED_CLASSES) {
            if (idx >= 0 && idx < NUM_CLASSES) {
                printf("  [%d] %s\n", idx, OIV7_CLASSES[idx]);
            } else {
                printf("  [%d] ★ 잘못된 인덱스! (NUM_CLASSES=%d)\n", idx, NUM_CLASSES);
            }
        }
        printf("[YOLO] ---- 이름이 이상하면 INTERESTED_CLASSES 인덱스를 수정하세요 ----\n");
        */
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
    "x264enc tune=zerolatency bitrate=8000 speed-preset=ultrafast "
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
    if (sock < 0) {
        printf("[Uu] 소켓 생성 실패: %s\n", strerror(errno));
        return -1;
    }

    int flags = fcntl(sock, F_GETFL, 0);
    fcntl(sock, F_SETFL, flags | O_NONBLOCK);

    if (setsockopt(sock, SOL_SOCKET, SO_BINDTODEVICE,
                   UU_INTERFACE, strlen(UU_INTERFACE)) < 0) {
        printf("[Uu] SO_BINDTODEVICE 실패 (%s): %s\n", UU_INTERFACE, strerror(errno));
    } else {
        printf("[Uu] NIC 바인딩: %s\n", UU_INTERFACE);
    }

    struct sockaddr_in bind_addr;
    memset(&bind_addr, 0, sizeof(bind_addr));
    bind_addr.sin_family = AF_INET;
    inet_pton(AF_INET, UU_SENDER_IP, &bind_addr.sin_addr);
    bind_addr.sin_port = htons(UU_SENDER_PORT);

    if (bind(sock, (struct sockaddr*)&bind_addr, sizeof(bind_addr)) < 0) {
        printf("[Uu] 바인딩 실패 (%s): %s\n", UU_SENDER_IP, strerror(errno));
        close(sock);
        return -1;
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
    memset(&pc5_local_addr, 0, sizeof(pc5_local_addr));

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

    if (!quic) {
        printf("[QUIC] picoquic_create 실패\n");
        return nullptr;
    }

    quic->default_multipath_option = 1;
    quic->default_tp.initial_max_path_id = 4;

    picoquic_set_default_congestion_algorithm_by_name(quic, "bbr");
    picoquic_set_cwin_max(quic, 10000000);
    picoquic_set_default_idle_timeout(quic, 120000); // ← 추가 (120초)

    picoquic_cnx_t *cnx = picoquic_create_cnx(quic,
                                              picoquic_null_connection_id,
                                              picoquic_null_connection_id,
                                              (struct sockaddr*)&uu_remote_addr,
                                              current_time, 0, "test.sni", "v2x_test", 1);
    if (!cnx) {
        printf("[QUIC] picoquic_create_cnx 실패\n");
        return nullptr;
    }

    cnx->local_parameters.initial_max_path_id = 4;
    cnx->idle_timeout = 120000; 

    picoquic_set_callback(cnx, [](picoquic_cnx_t* cnx, uint64_t stream_id, uint8_t* bytes, size_t length,
        picoquic_call_back_event_t event, void* callback_ctx, void* stream_ctx) -> int {
        switch (event) {
        case picoquic_callback_ready:
            printf("[QUIC] ★ 커넥션 READY! Multipath: %d\n", cnx->is_multipath_enabled);
            g_uu_handshake_done = true;
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
        if (sendLen > 0) {
            dispatchQuicPacket(sendBuf, sendLen, &peer_addr, &local_addr);
        } else {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
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
        if (!g_pc5_path_added) continue;
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
            picoquic_incoming_packet(quic,
                rxBuf, recvLen,
                (struct sockaddr*)&from_addr,
                (struct sockaddr*)&uu_local_addr,
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
    while (keepRunning && !g_uu_handshake_done)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    if (!keepRunning) return;

    printf("[Multipath] Uu 핸드셰이크 완료. PC5 probe 시작...\n");
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    const int MAX_RETRY = 20;
    for (int i = 0; i < MAX_RETRY && keepRunning && !g_pc5_path_added; i++) {
        {
            std::lock_guard<std::mutex> lock(quic_mutex);
            if (!g_cnx->is_multipath_enabled) {
                printf("[Multipath] Multipath 협상 실패. Uu 단일 경로.\n");
                return;
            }
            int ret = picoquic_probe_new_path_ex(g_cnx,
                (const struct sockaddr*)&pc5_remote_addr,
                (const struct sockaddr*)&pc5_local_addr,
                0, picoquic_current_time(), 0);
            if (ret == 0) {
                printf("[Multipath] ★ PC5 경로 추가 성공! 경로 수: %d\n", g_cnx->nb_paths);
                g_pc5_path_added = true;
                break;
            } else {
                printf("[Multipath] PC5 probe 시도 %d/%d, 에러: %d\n", i+1, MAX_RETRY, ret);
                int tmp = 0;
                picoquic_subscribe_new_path_allowed(g_cnx, &tmp);
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    while (keepRunning) {
        std::this_thread::sleep_for(std::chrono::seconds(5));
        std::lock_guard<std::mutex> lock(quic_mutex);
        if (g_cnx) {
            printf("[Multipath] 경로 수: %d, Multipath: %s\n",
                   g_cnx->nb_paths, g_cnx->is_multipath_enabled ? "ON" : "OFF");
        }
    }
}

// ===================== YOLO 워커 스레드 =====================
void yoloWorkerThread() {
    printf("[YOLO 워커] 스레드 시작...\n");

    if (g_yoloNet.empty()) {
        printf("[YOLO 워커] 모델이 없어 워커 종료\n");
        return;
    }

    while (keepRunning) {
        FrameJob job;
        {
            std::unique_lock<std::mutex> lk(g_yoloQueueMtx);
            g_yoloQueueCv.wait(lk, [] {
                return !g_yoloQueue.empty() || !keepRunning;
            });
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
                for (int i = 0; i < blob.dims; i++) {
                    printf("%d%s", blob.size[i], (i < blob.dims - 1) ? "," : "");
                }
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
                    for (int j = 0; j < outputs[i].dims; j++) {
                        printf("%d%s", outputs[i].size[j],
                               (j < outputs[i].dims - 1) ? "," : "");
                    }
                    printf("]\n");
                }
                out_logged = true;
            }

            auto dets = postprocessYolo(outputs, job.frame.cols, job.frame.rows);

            {
                std::lock_guard<std::mutex> lk(g_detMtx);
                g_lastDetections = dets;
                g_lastDetFrameId = job.frameId;
            }

            std::string json = detectionsToJson(dets, job.frameId);
            sendTypedData(PKT_TYPE_YOLO,
                          (const uint8_t*)json.c_str(),
                          (uint32_t)json.size());

        } catch (const cv::Exception& e) {
            printf("[YOLO 워커] cv 에러: %s\n", e.what());
        } catch (const std::exception& e) {
            printf("[YOLO 워커] 일반 에러: %s\n", e.what());
        }
    }

    printf("[YOLO 워커] 스레드 종료\n");
}


//##########################MAIN###############################
int main(int argc, char** argv) {
    
    gst_init(&argc, &argv);
    initAddresses();

    // V2X
    auto v2xmsgregi = initSirius();
    if (!v2xmsgregi){ printf("V2X init ERROR\n"); return -1; }

    // Uu 소켓
    g_uu_sock = initUuSocket();
    if (g_uu_sock < 0) return -1;

    // QUIC
    picoquic_quic_t *quic = initQuic();
    if (!quic) return -1;


    // Threads
    std::thread send_thread(quicSendThread, quic);
    std::thread uu_recv_thread(uuRecvThread, quic);
    std::thread pc5_recv_thread(pc5RecvThread, quic);
    std::thread mp_probe_thread(multipathProbeThread, quic);

    // YOLO
    bool yoloAvailable = initYolo();
    if (!yoloAvailable) {
        printf("[YOLO] 모델 로드 실패. YOLO 없이 진행합니다.\n");
    }

    // GStreamer
    GstElement* pipeline = initGStreamerPipeline();
    if (!pipeline) return -1;
    initGStreamerApp(pipeline);

    // Camera
    std::this_thread::sleep_for(std::chrono::seconds(2));

    cv::VideoCapture cap;
cap.set(cv::CAP_PROP_FOURCC, cv::VideoWriter::fourcc('M','J','P','G'));
cap.open(2, cv::CAP_V4L2);
cap.set(cv::CAP_PROP_FRAME_WIDTH,  CAMERA_WIDTH);
cap.set(cv::CAP_PROP_FRAME_HEIGHT, CAMERA_HEIGHT);
cap.set(cv::CAP_PROP_FPS,          CAMERA_FRAME);
cap.set(cv::CAP_PROP_BUFFERSIZE,   1);
if (!cap.isOpened()) { printf("카메라 열기 실패\n"); return -1; }

    
    std::thread input_thread(inputThread);
    std::thread yolo_thread;
    if (yoloAvailable) {
        yolo_thread = std::thread(yoloWorkerThread);
    }

    // ---------------------- MAIN LOOP ----------------------
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
                while (g_yoloQueue.size() >= YOLO_QUEUE_MAX) {
                    g_yoloQueue.pop();
                }
                g_yoloQueue.push(std::move(job));
            }
            g_yoloQueueCv.notify_one();
        }

        std::vector<Detection> dets_to_draw;
        {
            std::lock_guard<std::mutex> lk(g_detMtx);
            dets_to_draw = g_lastDetections;
        }
        
       // drawDetections(frame, dets_to_draw);

       // cv::imshow("Sender - YOLO OIV7 Detection (Multipath)", frame);
       // if (cv::waitKey(1) == 'q') {
       //     keepRunning = false;
       //     g_yoloQueueCv.notify_all();
       //     break;
       // }
            

        gsize buf_size = original_frame.total() * original_frame.elemSize();
        GstBuffer* buffer = gst_buffer_new_allocate(nullptr, buf_size, nullptr);
        GstMapInfo map;
        gst_buffer_map(buffer, &map, GST_MAP_WRITE);
        memcpy(map.data, original_frame.data, buf_size);
        gst_buffer_unmap(buffer, &map);
        GST_BUFFER_PTS(buffer)      = timestamp;
        GST_BUFFER_DURATION(buffer) = duration;
        timestamp += duration;
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

    picoquic_free(quic);
    if (g_uu_sock >= 0) close(g_uu_sock);

    cap.release();
    // cv::destroyAllWindows();
    EFOS_TerminateV2x(v2xmsgregi->v2xServiceId, v2xmsgregi->commMode);
    return 0;
}
