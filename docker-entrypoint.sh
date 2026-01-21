#!/bin/bash
set -e

cd /app

# 检查配置文件是否存在
if [ ! -f "config/multi_ctp_config.json" ]; then
    echo "错误: 配置文件 config/multi_ctp_config.json 不存在"
    exit 1
fi

# 设置库路径
export LD_LIBRARY_PATH=/app/libs:/usr/local/lib:$LD_LIBRARY_PATH

# 启动应用
echo "启动 open-ctp-mdgateway..."
exec ./bin/open-ctp-mdgateway --config config/multi_ctp_config.json --port 7899
