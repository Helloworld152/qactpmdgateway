# Makefile for QuantAxis Market Data Gateway
# =========================================

# 编译器设置
CXX = g++
CXXFLAGS = -std=c++17 -pthread -g -O2 -Wall -fPIC -Wno-unused-local-typedefs -Wno-reorder -Wno-class-memaccess -DBOOST_LOG_DYN_LINK

# 包含路径
INCLUDES = -I./libs \
          -I./include \
          -I/usr/include/rapidjson \
          -I/usr/local/include

# 库路径
LDFLAGS = -L./libs \
         -L/usr/local/lib

# 链接库
LIBS = ./libs/thostmduserapi_se.so ./libs/thosttraderapi_se.so \
       -lboost_log -lboost_log_setup -lboost_date_time \
	   -lboost_system -lboost_thread -lboost_chrono -lboost_filesystem -lboost_regex -lboost_atomic \
       -lssl -lcrypto -lcurl \
       -lpthread -lrt -lstdc++fs

# 源文件目录
SRCDIR = src
OBJDIR = obj
BINDIR = bin

# 源文件
SOURCES = $(wildcard $(SRCDIR)/*.cpp)
OBJECTS = $(SOURCES:$(SRCDIR)/%.cpp=$(OBJDIR)/%.o)

# 目标文件
TARGET = $(BINDIR)/open-ctp-mdgateway

# 默认目标
all: directories $(TARGET)

# 创建目录
directories:
	@mkdir -p $(OBJDIR)
	@mkdir -p $(BINDIR)
	@mkdir -p ctp_flow
	@mkdir -p config

# 编译目标
$(TARGET): $(OBJECTS)
	@echo "Linking $@..."
	$(CXX) $(OBJECTS) $(LDFLAGS) $(LIBS) -o $@
	@echo "Build completed: $@"

# 编译对象文件
$(OBJDIR)/%.o: $(SRCDIR)/%.cpp
	@echo "Compiling $<..."
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c $< -o $@

# 清理
clean:
	@echo "Cleaning..."
	@rm -rf $(OBJDIR) $(BINDIR)
	@rm -rf ctp_flow
	@echo "Clean completed"

# 调试目标
debug: CXXFLAGS += -DDEBUG -g3
debug: clean $(TARGET)

# 发布目标
release: CXXFLAGS += -O3 -DNDEBUG
release: clean $(TARGET)

.PHONY: all directories clean install test check-deps help debug release docs