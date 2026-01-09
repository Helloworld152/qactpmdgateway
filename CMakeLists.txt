cmake_minimum_required(VERSION 3.16)
project(qactpmdgateway)

# 设置 C++ 标准
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 生成 compile_commands.json
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# 编译选项
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -pthread -g -O2 -Wall -fPIC")

# 包含目录
include_directories(
    ./libs
    ../open-trade-common
    ../contrib/include
    /usr/local/include
)

# 库目录
link_directories(
    ./libs
    ../contrib/lib
    /usr/local/lib
)

# 源文件
file(GLOB SOURCES "src/*.cpp")

# 创建可执行文件
add_executable(open-ctp-mdgateway ${SOURCES})

# 链接库
target_link_libraries(open-ctp-mdgateway
    ./libs/thostmduserapi_se.so
    ./libs/thosttraderapi_se.so
    boost_system
    boost_thread
    boost_chrono
    boost_filesystem
    boost_regex
    boost_log_setup
    boost_log
    boost_date_time
    boost_atomic
    ssl
    crypto
    curl
    pthread
    rt
    stdc++fs
)

# 安装目标
install(TARGETS open-ctp-mdgateway
    RUNTIME DESTINATION bin
)
