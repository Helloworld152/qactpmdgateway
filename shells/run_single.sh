pkill -f 'open-ctp-mdgateway'
sleep 5
echo "run open-ctp-mdgateway" >> $LOG

export LD_LIBRARY_PATH=./libs:./contrib/lib:/usr/local/lib:$LD_LIBRARY_PATH
nohup ./open-ctp-mdgateway --port 7899 >> $LOGPATH/marketdata_server_single.log 2>&1 &
PID=$!

sleep 5
if ps -p $PID > /dev/null 2>&1; then
    echo "open-ctp-mdgateway PID=$PID success" >> $LOG
else
    echo "open-ctp-mdgateway failed" >> $LOG
fi