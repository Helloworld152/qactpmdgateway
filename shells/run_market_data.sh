#!/bin/bash
cd "$(dirname "$0")"
LOG=./logs/script.log
LOGPATH=./logs
echo "======== $(date) ========" >> $LOG
echo "kill open-ctp-mdgateway" >> $LOG
pkill -f 'open-ctp-mdgateway'
sleep 5
echo "run open-ctp-mdgateway" >> $LOG

export LD_LIBRARY_PATH=./libs:./contrib/lib:/usr/local/lib:$LD_LIBRARY_PATH
nohup ./open-ctp-mdgateway --config config/multi_ctp_config.json --port 7899 >> $LOGPATH/marketdata_server_1.log 2>&1 &
PID=$!

sleep 5
if ps -p $PID > /dev/null 2>&1; then
    echo "open-ctp-mdgateway PID=$PID success" >> $LOG
else
    echo "open-ctp-mdgateway failed" >> $LOG
fi

