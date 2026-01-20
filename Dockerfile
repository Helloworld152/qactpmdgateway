# 多阶段构建 - 减小镜像体积
# 阶段1: 构建环境
FROM ubuntu:20.04 as builder

ENV DEBIAN_FRONTEND=noninteractive

# 只安装构建依赖
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    g++ \
    make \
    libboost-all-dev \
    libssl-dev \
    libcurl4-openssl-dev \
    rapidjson-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# 复制项目文件
COPY . .

# 构建
RUN make clean && make -j$(nproc)

# 阶段2: 运行环境（最小化）
FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive

# 只安装运行时依赖
RUN apt-get update && apt-get install -y \
    libboost-system1.71.0 \
    libboost-thread1.71.0 \
    libboost-chrono1.71.0 \
    libboost-filesystem1.71.0 \
    libboost-regex1.71.0 \
    libboost-log1.71.0 \
    libboost-date-time1.71.0 \
    libboost-atomic1.71.0 \
    libssl1.1 \
    libcurl4 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

WORKDIR /app

# 只复制必要的运行时文件
COPY --from=builder /build/bin/open-ctp-mdgateway ./bin/
# 复制库文件
RUN mkdir -p libs
COPY --from=builder /build/libs/*.so ./libs/
COPY --from=builder /build/config/ ./config/
COPY docker-entrypoint.sh ./
RUN chmod +x ./docker-entrypoint.sh

# 创建日志目录
RUN mkdir -p logs

# 设置环境变量
ENV LD_LIBRARY_PATH=/app/libs:/usr/local/lib:$LD_LIBRARY_PATH

EXPOSE 7899

ENTRYPOINT ["./docker-entrypoint.sh"]
