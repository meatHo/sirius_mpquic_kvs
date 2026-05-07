CXX = g++
CXXFLAGS = -Wall -O2 -std=c++17 $(INCLUDES)

INCLUDES = -I. -I/usr/local/include -I./../../picoquic/picoquic \
           $(shell pkg-config --cflags opencv4 gstreamer-1.0 gstreamer-app-1.0)

LDFLAGS = -L/usr/local/lib

LIBS = -lefosv2xsdk \
       ./../../picoquic/libpicoquic-core.a \
       ./../../picoquic/libpicoquic-log.a \
       ./../../picotls/libpicotls-openssl.a \
       ./../../picotls/libpicotls-fusion.a \
       ./../../picotls/libpicotls-minicrypto.a \
       ./../../picotls/libpicotls-core.a \
       -lssl -lcrypto -pthread \
       $(shell pkg-config --libs opencv4 gstreamer-1.0 gstreamer-app-1.0)

TARGET = receiver_v5
SRCS = receiver_v5.cpp
OBJS = $(SRCS:.cpp=.o)

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $^ $(LIBS)

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -f $(OBJS) $(TARGET)