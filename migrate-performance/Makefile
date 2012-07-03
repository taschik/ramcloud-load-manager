# Defines
TARGET = migrate-performance

# Relative path to RAMCloud dir
RAMCLOUD_DIR = ../../ramcloud-AnalyticsDB

# Derived defines
INCLUDE = -I$(RAMCLOUD_DIR)/src -I$(RAMCLOUD_DIR)/obj.master -Iinclude
LIBS = $(RAMCLOUD_DIR)/obj.master/libramcloud.a -lprotobuf -lpcrecpp \
	-lboost_program_options -lprotobuf -lcryptopp -lrt -lboost_thread \
	-libverbs

CFLAGS   = -std=c++0x -pipe -Wall -g -O2
CXXFLAGS = $(CFLAGS)
CPPFLAGS = $(INCLUDE)

SRC_FILES = $(wildcard src/*.cc)
O_FILES   = $(SRC_FILES:%.cc=%.o)


.PHONY: all clean proper dependencies.mk $(TARGET)/run \
	ramcloud ramcloud/clean

all: $(TARGET)

clean:
	rm -rf migrate-performance ./src/*.o

$(TARGET): ramcloud $(O_FILES)
	$(CXX) -o $@ $(O_FILES) $(LIBS)

$(TARGET)/run: $(TARGET)
	./$(TARGET)

# Dependency tracking
#dependencies.mk:
#	$(CXX) -MM -MT _OBJ_ $(CPPFLAGS) $(SRC_FILES) $(TEST_FILES) | \
	  ruby -ne 'print$$_.gsub(/^_OBJ_:\s*(.+?)(\.cc?|\.cpp)/,"\\1.o:\\1\\2")'\
	  > dependencies.mk

#include dependencies.mk
