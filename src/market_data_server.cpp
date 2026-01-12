#include "market_data_server.h"
#include <boost/log/trivial.hpp>
#include <boost/filesystem.hpp>
#include <sstream>
#include <cstring>
#include <chrono>
#include <random>
#include <algorithm>
#include <cmath>

// 预分配的JSON key字符串（避免运行时字符串拼接）
namespace {
    const char* const ASK_PRICE_KEYS[] = {
        "ask_price1", "ask_price2", "ask_price3", "ask_price4", "ask_price5",
        "ask_price6", "ask_price7", "ask_price8", "ask_price9", "ask_price10"
    };
    const char* const ASK_VOLUME_KEYS[] = {
        "ask_volume1", "ask_volume2", "ask_volume3", "ask_volume4", "ask_volume5",
        "ask_volume6", "ask_volume7", "ask_volume8", "ask_volume9", "ask_volume10"
    };
    const char* const BID_PRICE_KEYS[] = {
        "bid_price1", "bid_price2", "bid_price3", "bid_price4", "bid_price5",
        "bid_price6", "bid_price7", "bid_price8", "bid_price9", "bid_price10"
    };
    const char* const BID_VOLUME_KEYS[] = {
        "bid_volume1", "bid_volume2", "bid_volume3", "bid_volume4", "bid_volume5",
        "bid_volume6", "bid_volume7", "bid_volume8", "bid_volume9", "bid_volume10"
    };
}

namespace beast = boost::beast;
namespace http = beast::http;
namespace websocket = beast::websocket;
namespace net = boost::asio;
using tcp = net::ip::tcp;

// WebSocketSession实现
WebSocketSession::WebSocketSession(tcp::socket&& socket, MarketDataServer* server)
    : ws_(std::move(socket))
    , server_(server)
    , is_writing_(false)
{
    // 生成唯一的session ID
    session_id_ = server_->create_session_id();
}

WebSocketSession::~WebSocketSession()
{
    if (server_) {
        server_->remove_session(session_id_);
    }
}

void WebSocketSession::run()
{
    // 设置WebSocket选项
    ws_.set_option(websocket::stream_base::timeout::suggested(beast::role_type::server));
    ws_.set_option(websocket::stream_base::decorator(
        [](websocket::response_type& res)
        {
            res.set(http::field::server, "QuantAxis-MarketData-Server");
        }));

    // 接受WebSocket握手
    ws_.async_accept(
        beast::bind_front_handler(&WebSocketSession::on_accept, shared_from_this()));
}

void WebSocketSession::on_accept(beast::error_code ec)
{
    if (ec) {
        server_->log_error("WebSocket accept error: " + ec.message());
        return;
    }

    server_->log_info("WebSocket session connected: " + session_id_);
    
    // 发送欢迎消息
    rapidjson::Document welcome;
    welcome.SetObject();
    rapidjson::Document::AllocatorType& allocator = welcome.GetAllocator();
    
    welcome.AddMember("type", "welcome", allocator);
    welcome.AddMember("message", "Connected to QuantAxis MarketData Server", allocator);
    welcome.AddMember("session_id", rapidjson::StringRef(session_id_.c_str()), allocator);
    welcome.AddMember("ctp_connected", server_->is_ctp_connected(), allocator);
    welcome.AddMember("timestamp", std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count(), allocator);
    
    send_response("welcome", welcome);
    
    // 开始读取消息
    do_read();
}

void WebSocketSession::do_read()
{
    ws_.async_read(
        buffer_,
        beast::bind_front_handler(&WebSocketSession::on_read, shared_from_this()));
}

void WebSocketSession::on_read(beast::error_code ec, std::size_t bytes_transferred)
{
    boost::ignore_unused(bytes_transferred);

    if (ec == websocket::error::closed) {
        server_->log_info("WebSocket session closed: " + session_id_);
        server_->remove_session(session_id_);
        return;
    }

    if (ec) {
        server_->log_error("WebSocket read error: " + ec.message());
        return;
    }

    // 处理接收到的消息
    std::string message = beast::buffers_to_string(buffer_.data());
    buffer_.clear();
    
    handle_message(message);
    do_read();
}

void WebSocketSession::handle_message(const std::string& message)
{
    server_->log_info("Received message from session " + session_id_ + ": " + message);
    
    try {
        rapidjson::Document doc;
        if (doc.Parse(message.c_str()).HasParseError()) {
            send_error("Invalid JSON format");
            return;
        }
        
        // 兼容mdservice协议
        if (doc.HasMember("aid") && doc["aid"].IsString()) {
            std::string aid = doc["aid"].GetString();
            
            if (aid == "subscribe_quote") {
                // 处理mdservice订阅请求
                if (!doc.HasMember("ins_list") || !doc["ins_list"].IsString()) {
                    send_error("Missing or invalid 'ins_list' field");
                    return;
                }
                
                std::string ins_list = doc["ins_list"].GetString();
                std::vector<std::string> instruments;
                
                // 解析逗号分隔的合约列表
                std::istringstream iss(ins_list);
                std::string instrument;
                while (std::getline(iss, instrument, ',')) {
                    if (!instrument.empty()) {
                        // 去掉交易所前缀 (如 GFEX. -> 空)
                        std::string nohead_instrument = instrument;
                        size_t dot_pos = instrument.find('.');
                        if (dot_pos != std::string::npos) {
                            nohead_instrument = instrument.substr(dot_pos + 1);
                        }
                        
                        instruments.push_back(nohead_instrument);
                        subscriptions_.insert(nohead_instrument);
                        
                        // 更新映射表和订阅者
                        server_->noheadtohead_instruments_map_[nohead_instrument] = instrument;
                        server_->subscribe_instrument(session_id_, nohead_instrument);  // 使用CTP格式订阅
                    }
                }
                
                // 发送mdservice格式响应
                rapidjson::Document response;
                response.SetObject();
                auto& allocator = response.GetAllocator();
                response.AddMember("aid", "subscribe_quote", allocator);
                response.AddMember("status", "ok", allocator);
                
                send_response("subscribe_quote_response", response);
                return;
            }
            if (aid == "peek_message") {
                // 处理peek_message，发送缓存的行情数据
                server_->handle_peek_message(session_id_);
                return;
            }
        }
    } catch (const std::exception& e) {
        send_error("Error processing message: " + std::string(e.what()));
    }
}

void WebSocketSession::send_error(const std::string& error_msg)
{
    rapidjson::Document error;
    error.SetObject();
    auto& allocator = error.GetAllocator();
    error.AddMember("type", "error", allocator);
    error.AddMember("message", rapidjson::StringRef(error_msg.c_str()), allocator);
    error.AddMember("timestamp", std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count(), allocator);
    
    send_response("error", error);
}

void WebSocketSession::send_response(const std::string& type, const rapidjson::Document& data)
{
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    data.Accept(writer);
    
    send_message(buffer.GetString());
}

void WebSocketSession::send_message(const std::string& message)
{
    std::lock_guard<std::mutex> lock(write_mutex_);
    
    message_queue_.push(message);
    
    if (!is_writing_) {
        is_writing_ = true;
        start_write();
    }
}

void WebSocketSession::start_write()
{
    if (message_queue_.empty()) {
        is_writing_ = false;
        return;
    }

    current_write_message_ = std::move(message_queue_.front());
    message_queue_.pop();

    ws_.async_write(
        net::buffer(current_write_message_),
        beast::bind_front_handler(&WebSocketSession::on_write, shared_from_this()));
}

void WebSocketSession::on_write(beast::error_code ec, std::size_t bytes_transferred)
{
    boost::ignore_unused(bytes_transferred);

    if (ec) {
        server_->log_error("WebSocket write error: " + ec.message());
        std::lock_guard<std::mutex> lock(write_mutex_);
        is_writing_ = false;
        return;
    }

    std::lock_guard<std::mutex> lock(write_mutex_);
    
    // 继续写入队列中的下一条消息
    start_write();
}

void WebSocketSession::close()
{
    beast::error_code ec;
    ws_.close(websocket::close_code::normal, ec);
    if (ec) {
        server_->log_error("Error closing WebSocket: " + ec.message());
    }
}

// MarketDataSpi实现
MarketDataSpi::MarketDataSpi(MarketDataServer* server) : server_(server)
{
}

MarketDataSpi::~MarketDataSpi()
{
}

void MarketDataSpi::OnFrontConnected()
{
    server_->log_info("CTP front connected");
    server_->ctp_login();
}

void MarketDataSpi::OnFrontDisconnected(int nReason)
{
    server_->log_warning("CTP front disconnected, reason: " + std::to_string(nReason));
}

void MarketDataSpi::OnRspUserLogin(CThostFtdcRspUserLoginField *pRspUserLogin,
                                  CThostFtdcRspInfoField *pRspInfo,
                                  int nRequestID, bool bIsLast)
{
    if (pRspInfo && pRspInfo->ErrorID != 0) {
        server_->log_error("CTP login failed: " + std::string(pRspInfo->ErrorMsg));
        return;
    }
    
    server_->log_info("CTP login successful");
}

void MarketDataSpi::OnRspSubMarketData(CThostFtdcSpecificInstrumentField *pSpecificInstrument,
                                      CThostFtdcRspInfoField *pRspInfo,
                                      int nRequestID, bool bIsLast)
{
    if (pRspInfo && pRspInfo->ErrorID != 0) {
        server_->log_error("Subscribe market data failed: " + std::string(pRspInfo->ErrorMsg));
        return;
    }
    
    if (pSpecificInstrument) {
        server_->log_info("Subscribed to instrument: " + std::string(pSpecificInstrument->InstrumentID));
    }
}

// 从CTP数据构建MarketDataStruct
MarketDataStruct MarketDataServer::build_market_data_struct(CThostFtdcDepthMarketDataField *pDepthMarketData,
                                                             const std::string& display_instrument,
                                                             const uint64_t cur_time)
{
    if (!pDepthMarketData) {
        MarketDataStruct empty;
        memset(&empty, 0, sizeof(empty));
        return empty;
    }
    MarketDataStruct data;
    memset(&data, 0, sizeof(data));
    
    // 复制instrument_id
    strncpy(data.instrument_id, display_instrument.c_str(), sizeof(data.instrument_id) - 1);
    data.instrument_id[sizeof(data.instrument_id) - 1] = '\0';
    
    // 手动高效构建 datetime 字符串 (YYYY-MM-DD HH:MM:SS.mmm)
    char* dt = data.datetime;
    const char* td = pDepthMarketData->TradingDay;
    const char* ut = pDepthMarketData->UpdateTime;
    int ms = pDepthMarketData->UpdateMillisec;

    // YYYY-MM-DD
    if (td[0] >= '0' && td[0] <= '9') {
        dt[0] = td[0]; dt[1] = td[1]; dt[2] = td[2]; dt[3] = td[3];
        dt[4] = '-';
        dt[5] = td[4]; dt[6] = td[5];
        dt[7] = '-';
        dt[8] = td[6]; dt[9] = td[7];
        dt[10] = ' ';
        dt += 11;
    }

    // HH:MM:SS.mmm
    if (ut[0] != '\0') {
        memcpy(dt, ut, 8);
        dt[8] = '.';
        dt[9] = '0' + (ms / 100 % 10);
        dt[10] = '0' + (ms / 10 % 10);
        dt[11] = '0' + (ms % 10);
        dt[12] = '\0';
    } else {
        *dt = '\0';
    }
    
    data.timestamp = cur_time;
    
    // 初始化数组为0
    for (int i = 0; i < 10; ++i) {
        data.ask_price[i] = 0.0;
        data.ask_volume[i] = 0;
        data.bid_price[i] = 0.0;
        data.bid_volume[i] = 0;
    }
    
    // Ask 5-1
    double ask5 = pDepthMarketData->AskPrice5;
    if (ask5 > 1e-6 && ask5 < 1e300) {
        data.ask_price[4] = round(ask5 * 100.0) / 100.0;
        data.ask_volume[4] = pDepthMarketData->AskVolume5;
    }
    
    double ask4 = pDepthMarketData->AskPrice4;
    if (ask4 > 1e-6 && ask4 < 1e300) {
        data.ask_price[3] = round(ask4 * 100.0) / 100.0;
        data.ask_volume[3] = pDepthMarketData->AskVolume4;
    }
    
    double ask3 = pDepthMarketData->AskPrice3;
    if (ask3 > 1e-6 && ask3 < 1e300) {
        data.ask_price[2] = round(ask3 * 100.0) / 100.0;
        data.ask_volume[2] = pDepthMarketData->AskVolume3;
    }
    
    double ask2 = pDepthMarketData->AskPrice2;
    if (ask2 > 1e-6 && ask2 < 1e300) {
        data.ask_price[1] = round(ask2 * 100.0) / 100.0;
        data.ask_volume[1] = pDepthMarketData->AskVolume2;
    }
    
    double ask1 = pDepthMarketData->AskPrice1;
    if (ask1 > 1e-6 && ask1 < 1e300) {
        data.ask_price[0] = round(ask1 * 100.0) / 100.0;
        data.ask_volume[0] = pDepthMarketData->AskVolume1;
    }
    
    // Bid 1-5
    double bid1 = pDepthMarketData->BidPrice1;
    if (bid1 > 1e-6 && bid1 < 1e300) {
        data.bid_price[0] = round(bid1 * 100.0) / 100.0;
        data.bid_volume[0] = pDepthMarketData->BidVolume1;
    }
    
    double bid2 = pDepthMarketData->BidPrice2;
    if (bid2 > 1e-6 && bid2 < 1e300) {
        data.bid_price[1] = round(bid2 * 100.0) / 100.0;
        data.bid_volume[1] = pDepthMarketData->BidVolume2;
    }
    
    double bid3 = pDepthMarketData->BidPrice3;
    if (bid3 > 1e-6 && bid3 < 1e300) {
        data.bid_price[2] = round(bid3 * 100.0) / 100.0;
        data.bid_volume[2] = pDepthMarketData->BidVolume3;
    }
    
    double bid4 = pDepthMarketData->BidPrice4;
    if (bid4 > 1e-6 && bid4 < 1e300) {
        data.bid_price[3] = round(bid4 * 100.0) / 100.0;
        data.bid_volume[3] = pDepthMarketData->BidVolume4;
    }
    
    double bid5 = pDepthMarketData->BidPrice5;
    if (bid5 > 1e-6 && bid5 < 1e300) {
        data.bid_price[4] = round(bid5 * 100.0) / 100.0;
        data.bid_volume[4] = pDepthMarketData->BidVolume5;
    }
    
    // 其他价格字段
    double last_price = pDepthMarketData->LastPrice;
    if (last_price > 1e-6 && last_price < 1e300) {
        data.last_price = round(last_price * 100.0) / 100.0;
    }
    
    double highest = pDepthMarketData->HighestPrice;
    if (highest > 1e-6 && highest < 1e300) {
        data.highest = round(highest * 100.0) / 100.0;
    }
    
    double lowest = pDepthMarketData->LowestPrice;
    if (lowest > 1e-6 && lowest < 1e300) {
        data.lowest = round(lowest * 100.0) / 100.0;
    }
    
    double open = pDepthMarketData->OpenPrice;
    if (open > 1e-6 && open < 1e300) {
        data.open = round(open * 100.0) / 100.0;
    }
    
    double close = pDepthMarketData->ClosePrice;
    if (close > 1e-6 && close < 1e300) {
        data.close = round(close * 100.0) / 100.0;
    }
    
    data.volume = pDepthMarketData->Volume;
    data.amount = pDepthMarketData->Turnover;
    data.open_interest = static_cast<int64_t>(pDepthMarketData->OpenInterest);
    
    double settlement = pDepthMarketData->SettlementPrice;
    if (settlement > 1e-6 && settlement < 1e300) {
        data.settlement = round(settlement * 100.0) / 100.0;
    }
    
    double upper_limit = pDepthMarketData->UpperLimitPrice;
    if (upper_limit > 1e-6 && upper_limit < 1e300) {
        data.upper_limit = round(upper_limit * 100.0) / 100.0;
    }
    
    double lower_limit = pDepthMarketData->LowerLimitPrice;
    if (lower_limit > 1e-6 && lower_limit < 1e300) {
        data.lower_limit = round(lower_limit * 100.0) / 100.0;
    }
    
    data.pre_open_interest = static_cast<int64_t>(pDepthMarketData->PreOpenInterest);
    
    double pre_settlement = pDepthMarketData->PreSettlementPrice;
    if (pre_settlement > 1e-6 && pre_settlement < 1e300) {
        data.pre_settlement = round(pre_settlement * 100.0) / 100.0;
    }
    
    double pre_close = pDepthMarketData->PreClosePrice;
    if (pre_close > 1e-6 && pre_close < 1e300) {
        data.pre_close = round(pre_close * 100.0) / 100.0;
    }
    
    return data;
}

// 从MarketDataStruct构建JSON（用于发送）
rapidjson::Value MarketDataServer::struct_to_json(const MarketDataStruct& data,
                                                  rapidjson::Document::AllocatorType& allocator)
{
    rapidjson::Value inst_data(rapidjson::kObjectType);
    inst_data.AddMember("instrument_id", rapidjson::Value(data.instrument_id, allocator), allocator);
    inst_data.AddMember("datetime", rapidjson::Value(data.datetime, allocator), allocator);
    inst_data.AddMember("timestamp", data.timestamp, allocator);
    
    // Ask 10-1 (10-6为null)
    inst_data.AddMember("ask_price10", rapidjson::Value().SetNull(), allocator);
    inst_data.AddMember("ask_volume10", rapidjson::Value().SetNull(), allocator);
    inst_data.AddMember("ask_price9", rapidjson::Value().SetNull(), allocator);
    inst_data.AddMember("ask_volume9", rapidjson::Value().SetNull(), allocator);
    inst_data.AddMember("ask_price8", rapidjson::Value().SetNull(), allocator);
    inst_data.AddMember("ask_volume8", rapidjson::Value().SetNull(), allocator);
    inst_data.AddMember("ask_price7", rapidjson::Value().SetNull(), allocator);
    inst_data.AddMember("ask_volume7", rapidjson::Value().SetNull(), allocator);
    inst_data.AddMember("ask_price6", rapidjson::Value().SetNull(), allocator);
    inst_data.AddMember("ask_volume6", rapidjson::Value().SetNull(), allocator);
    
    // Ask 5-1 (使用预分配的key，使用StringRef避免拷贝)
    for (int i = 4; i >= 0; --i) {
        inst_data.AddMember(rapidjson::StringRef(ASK_PRICE_KEYS[i]), data.ask_price[i], allocator);
        inst_data.AddMember(rapidjson::StringRef(ASK_VOLUME_KEYS[i]), data.ask_volume[i], allocator);
    }
    
    // Bid 1-5 (使用预分配的key，使用StringRef避免拷贝)
    for (int i = 0; i < 5; ++i) {
        inst_data.AddMember(rapidjson::StringRef(BID_PRICE_KEYS[i]), data.bid_price[i], allocator);
        inst_data.AddMember(rapidjson::StringRef(BID_VOLUME_KEYS[i]), data.bid_volume[i], allocator);
    }
    
    // Bid 6-10为null
    inst_data.AddMember("bid_price6", rapidjson::Value().SetNull(), allocator);
    inst_data.AddMember("bid_volume6", rapidjson::Value().SetNull(), allocator);
    inst_data.AddMember("bid_price7", rapidjson::Value().SetNull(), allocator);
    inst_data.AddMember("bid_volume7", rapidjson::Value().SetNull(), allocator);
    inst_data.AddMember("bid_price8", rapidjson::Value().SetNull(), allocator);
    inst_data.AddMember("bid_volume8", rapidjson::Value().SetNull(), allocator);
    inst_data.AddMember("bid_price9", rapidjson::Value().SetNull(), allocator);
    inst_data.AddMember("bid_volume9", rapidjson::Value().SetNull(), allocator);
    inst_data.AddMember("bid_price10", rapidjson::Value().SetNull(), allocator);
    inst_data.AddMember("bid_volume10", rapidjson::Value().SetNull(), allocator);
    
    // 其他字段
    inst_data.AddMember("last_price", data.last_price, allocator);
    inst_data.AddMember("highest", data.highest, allocator);
    inst_data.AddMember("lowest", data.lowest, allocator);
    inst_data.AddMember("open", data.open, allocator);
    inst_data.AddMember("close", data.close, allocator);
    
    inst_data.AddMember("average", rapidjson::Value().SetNull(), allocator);
    inst_data.AddMember("volume", data.volume, allocator);
    inst_data.AddMember("amount", data.amount, allocator);
    inst_data.AddMember("open_interest", data.open_interest, allocator);
    
    inst_data.AddMember("settlement", data.settlement, allocator);
    inst_data.AddMember("upper_limit", data.upper_limit, allocator);
    inst_data.AddMember("lower_limit", data.lower_limit, allocator);
    
    inst_data.AddMember("pre_open_interest", data.pre_open_interest, allocator);
    
    inst_data.AddMember("pre_settlement", data.pre_settlement, allocator);
    inst_data.AddMember("pre_close", data.pre_close, allocator);
    
    return inst_data;
}

// 结构体字段级比较，返回差异字段的JSON
bool MarketDataServer::has_struct_changes(const MarketDataStruct& old_data, const MarketDataStruct& new_data)
{
    // 快速比较：先比较基本字段
    if (strcmp(old_data.instrument_id, new_data.instrument_id) != 0 ||
        strcmp(old_data.datetime, new_data.datetime) != 0 ||
        old_data.timestamp != new_data.timestamp) {
        return true;
    }
    
    // 比较数组字段
    if (memcmp(old_data.ask_price, new_data.ask_price, sizeof(old_data.ask_price)) != 0 ||
        memcmp(old_data.ask_volume, new_data.ask_volume, sizeof(old_data.ask_volume)) != 0 ||
        memcmp(old_data.bid_price, new_data.bid_price, sizeof(old_data.bid_price)) != 0 ||
        memcmp(old_data.bid_volume, new_data.bid_volume, sizeof(old_data.bid_volume)) != 0) {
        return true;
    }
    
    // 比较其他价格字段
    if (old_data.last_price != new_data.last_price ||
        old_data.highest != new_data.highest ||
        old_data.lowest != new_data.lowest ||
        old_data.open != new_data.open ||
        old_data.close != new_data.close ||
        old_data.settlement != new_data.settlement ||
        old_data.upper_limit != new_data.upper_limit ||
        old_data.lower_limit != new_data.lower_limit ||
        old_data.pre_settlement != new_data.pre_settlement ||
        old_data.pre_close != new_data.pre_close) {
        return true;
    }
    
    // 比较整数字段
    if (old_data.volume != new_data.volume ||
        old_data.amount != new_data.amount ||
        old_data.open_interest != new_data.open_interest ||
        old_data.pre_open_interest != new_data.pre_open_interest) {
        return true;
    }
    
    return false;
}

void MarketDataServer::compute_struct_diff(const MarketDataStruct& old_data,
                                          const MarketDataStruct& new_data,
                                          rapidjson::Writer<rapidjson::StringBuffer>& writer)
{
    writer.StartObject();
    
    // 比较基本字段
    if (strcmp(old_data.instrument_id, new_data.instrument_id) != 0) {
        writer.Key("instrument_id");
        writer.String(new_data.instrument_id);
    }
    
    if (strcmp(old_data.datetime, new_data.datetime) != 0) {
        writer.Key("datetime");
        writer.String(new_data.datetime);
    }
    
    if (old_data.timestamp != new_data.timestamp) {
        writer.Key("timestamp");
        writer.Uint64(new_data.timestamp);
    }
    
    // 比较Ask价格和量
    for (int i = 0; i < 10; ++i) {
        if (old_data.ask_price[i] != new_data.ask_price[i]) {
            writer.Key(ASK_PRICE_KEYS[i]);
            writer.Double(new_data.ask_price[i]);
        }
        if (old_data.ask_volume[i] != new_data.ask_volume[i]) {
            writer.Key(ASK_VOLUME_KEYS[i]);
            writer.Int(new_data.ask_volume[i]);
        }
    }
    
    // 比较Bid价格和量
    for (int i = 0; i < 10; ++i) {
        if (old_data.bid_price[i] != new_data.bid_price[i]) {
            writer.Key(BID_PRICE_KEYS[i]);
            writer.Double(new_data.bid_price[i]);
        }
        if (old_data.bid_volume[i] != new_data.bid_volume[i]) {
            writer.Key(BID_VOLUME_KEYS[i]);
            writer.Int(new_data.bid_volume[i]);
        }
    }
    
    // 比较其他价格字段
    auto add_price_diff = [&](const char* key, double old_val, double new_val) {
        if (old_val != new_val) {
            writer.Key(key);
            writer.Double(new_val);
        }
    };
    
    add_price_diff("last_price", old_data.last_price, new_data.last_price);
    add_price_diff("highest", old_data.highest, new_data.highest);
    add_price_diff("lowest", old_data.lowest, new_data.lowest);
    add_price_diff("open", old_data.open, new_data.open);
    add_price_diff("close", old_data.close, new_data.close);
    add_price_diff("upper_limit", old_data.upper_limit, new_data.upper_limit);
    add_price_diff("lower_limit", old_data.lower_limit, new_data.lower_limit);
    add_price_diff("pre_settlement", old_data.pre_settlement, new_data.pre_settlement);
    add_price_diff("pre_close", old_data.pre_close, new_data.pre_close);
    add_price_diff("settlement", old_data.settlement, new_data.settlement);
    
    // 比较整数字段
    if (old_data.volume != new_data.volume) {
        writer.Key("volume");
        writer.Int(new_data.volume);
    }
    
    if (old_data.amount != new_data.amount) {
        writer.Key("amount");
        writer.Double(new_data.amount);
    }
    
    if (old_data.open_interest != new_data.open_interest) {
        writer.Key("open_interest");
        writer.Int64(new_data.open_interest);
    }
    
    if (old_data.pre_open_interest != new_data.pre_open_interest) {
        writer.Key("pre_open_interest");
        writer.Int64(new_data.pre_open_interest);
    }
    
    writer.EndObject();
}

void MarketDataSpi::OnRtnDepthMarketData(CThostFtdcDepthMarketDataField *pDepthMarketData)
{
    if (!pDepthMarketData) return;

    auto cur_time = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

    std::string instrument_id = pDepthMarketData->InstrumentID;
    
    // 通过映射表查找带前缀的格式
    auto map_it = server_->noheadtohead_instruments_map_.find(instrument_id);
    std::string display_instrument = (map_it != server_->noheadtohead_instruments_map_.end()) 
        ? map_it->second : instrument_id;
    
    // 直接构建结构体（避免JSON构建，提高回调线程性能）
    MarketDataStruct market_data = MarketDataServer::build_market_data_struct(pDepthMarketData, display_instrument, cur_time);
    
    // 缓存行情数据（使用结构体存储）
    server_->cache_market_data(instrument_id, market_data, display_instrument);
    
    // 通知线性合约管理器组件行情更新（使用结构体传递）
    server_->on_component_update(instrument_id, market_data);
}

void MarketDataSpi::OnRspError(CThostFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast)
{
    if (pRspInfo && pRspInfo->ErrorID != 0) {
        server_->log_error("CTP error: " + std::string(pRspInfo->ErrorMsg));
    }
}

// MarketDataServer实现
MarketDataServer::MarketDataServer(const std::string& ctp_front_addr,
                                 const std::string& broker_id,
                                 int websocket_port)
    : ctp_front_addr_(ctp_front_addr)
    , broker_id_(broker_id)
    , ioc_()
    , websocket_port_(websocket_port)
    , ctp_api_(nullptr)
    , ctp_connected_(false)
    , ctp_logged_in_(false)
    , acceptor_(ioc_)
    , segment_(nullptr)
    , alloc_inst_(nullptr)
    , ins_map_(nullptr)
    , use_multi_ctp_mode_(false)
    , is_running_(false)
    , request_id_(0)
{
    // 预分配缓存，避免运行时的内存重分配
    market_data_cache_.resize(50000);
    display_instruments_cache_.resize(50000);
}

MarketDataServer::MarketDataServer(const MultiCTPConfig& config)
    : broker_id_(config.connections.empty() ? "9999" : config.connections[0].broker_id)
    , ioc_()
    , websocket_port_(config.websocket_port)
    , ctp_api_(nullptr)
    , ctp_connected_(false)
    , ctp_logged_in_(false)
    , acceptor_(ioc_)
    , segment_(nullptr)
    , alloc_inst_(nullptr)
    , ins_map_(nullptr)
    , multi_ctp_config_(config)
    , use_multi_ctp_mode_(true)
    , is_running_(false)
    , request_id_(0)
{
    // 预分配缓存，避免运行时的内存重分配
    market_data_cache_.resize(50000);
    display_instruments_cache_.resize(50000);
}

MarketDataServer::~MarketDataServer()
{
    stop();
}

bool MarketDataServer::start()
{
    if (is_running_) {
        return true;
    }
    
    std::string mode = use_multi_ctp_mode_ ? "multi-CTP" : "single-CTP";
    log_info("Starting MarketData Server in " + mode + " mode...");
    
    try {
        // 初始化共享内存
        init_shared_memory();
        
        // 启动WebSocket服务器
        start_websocket_server();
        
        if (use_multi_ctp_mode_) {
            // 多CTP连接模式
            if (!init_multi_ctp_system()) {
                log_error("Failed to initialize multi-CTP system");
                return false;
            }
        } else {
            // 单CTP连接模式（兼容性）
            std::string flow_path = "./ctpflow/single/";
            
            // 确保flow目录存在
            try {
                boost::filesystem::create_directories(flow_path);
            } catch (const std::exception& e) {
                log_warning("Failed to create flow directory: " + flow_path + ", error: " + e.what());
            }
            
            ctp_api_ = CThostFtdcMdApi::CreateFtdcMdApi(flow_path.c_str());
            if (!ctp_api_) {
                log_error("Failed to create CTP API");
                return false;
            }
            
            md_spi_ = std::make_unique<MarketDataSpi>(this);
            ctp_api_->RegisterSpi(md_spi_.get());
            ctp_api_->RegisterFront(const_cast<char*>(ctp_front_addr_.c_str()));
            ctp_api_->Init(); 
        }
        
        is_running_ = true;
        
        // 启动服务器线程
        server_thread_ = boost::thread([this]() {
            ioc_.run();
        });
        
        log_info("MarketData Server started on port " + std::to_string(websocket_port_));
        return true;
        
    } catch (const std::exception& e) {
        log_error("Failed to start server: " + std::string(e.what()));
        return false;
    }
}

void MarketDataServer::stop()
{
    if (!is_running_) {
        return;
    }
    
    log_info("Stopping MarketData Server...");
    is_running_ = false;
    
    // 关闭acceptor
    if (acceptor_.is_open()) {
        boost::system::error_code ec;
        acceptor_.cancel(ec); 
        acceptor_.close(ec);
        if (ec) {
            log_error("Failed to close acceptor: " + ec.message());
        } else {
            log_info("Acceptor closed successfully");
        }
    }
    
    // 关闭所有WebSocket连接
    {
        std::map<std::string, std::shared_ptr<WebSocketSession>> sessions_snapshot;
        {
            std::lock_guard<std::mutex> lock(sessions_mutex_);
            sessions_snapshot = sessions_;
            sessions_.clear();
        }
        
        for (auto& pair : sessions_snapshot) {
            pair.second->close();
        }
        sessions_snapshot.clear();
    }
    
    // 停止IO上下文
    ioc_.stop();
    
    // 等待服务器线程结束
    if (server_thread_.joinable()) {
        server_thread_.join();
    }
    
    // 清理CTP资源
    if (ctp_api_) {
        ctp_api_->Release();
        ctp_api_ = nullptr;
    }

    cleanup_shared_memory();
    if (use_multi_ctp_mode_) {
        cleanup_multi_ctp_system();
    }
    
    log_info("MarketData Server stopped");
}

void MarketDataServer::init_shared_memory()
{
    try {
        // 尝试连接到现有的共享内存段
        segment_ = new boost::interprocess::managed_shared_memory(
            boost::interprocess::open_only, "qamddata");
        
        alloc_inst_ = new ShmemAllocator(segment_->get_segment_manager());
        ins_map_ = segment_->find<InsMapType>("InsMap").first;
        
        if (ins_map_) {
            log_info("Connected to existing shared memory segment with " + 
                    std::to_string(ins_map_->size()) + " instruments");
        } else {
            log_warning("Shared memory segment found but InsMap not found");
        }
        
    } catch (const boost::interprocess::interprocess_exception& e) {
        log_warning("Failed to connect to existing shared memory: " + std::string(e.what()));
        log_info("Creating new shared memory segment");
        
        try {
            boost::interprocess::shared_memory_object::remove("qamddata");
            
            segment_ = new boost::interprocess::managed_shared_memory(
                boost::interprocess::create_only,
                "qamddata",
                32 * 1024 * 1024);  // 32MB
            
            alloc_inst_ = new ShmemAllocator(segment_->get_segment_manager());
            ins_map_ = segment_->construct<InsMapType>("InsMap")(
                CharArrayComparer(), *alloc_inst_);
            
            log_info("Created new shared memory segment");
            
        } catch (const std::exception& e) {
            log_error("Failed to create shared memory: " + std::string(e.what()));
            throw;
        }
    }
}

void MarketDataServer::cleanup_shared_memory()
{
    if (alloc_inst_) {
        delete alloc_inst_;
        alloc_inst_ = nullptr;
    }
    
    if (segment_) {
        delete segment_;
        segment_ = nullptr;
    }
    
    ins_map_ = nullptr;
}

void MarketDataServer::start_websocket_server()
{
    auto const address = net::ip::make_address("0.0.0.0");
    auto const port = static_cast<unsigned short>(websocket_port_);
    
    tcp::endpoint endpoint{address, port};
    acceptor_.open(endpoint.protocol());
    acceptor_.set_option(net::socket_base::reuse_address(true));
    acceptor_.bind(endpoint);
    acceptor_.listen(net::socket_base::max_listen_connections);
    
    // 开始接受连接
    acceptor_.async_accept(
        net::make_strand(ioc_),
        beast::bind_front_handler(&MarketDataServer::handle_accept, this));
}

void MarketDataServer::handle_accept(beast::error_code ec, tcp::socket socket)
{
    if(!acceptor_.is_open()) {
        return;
    }
    if (ec) {
        log_error("Accept error: " + ec.message());
    } else {
        // 创建新的会话
        auto session = std::make_shared<WebSocketSession>(std::move(socket), this);
        add_session(session);
        session->run();
    }
    
    // 继续接受连接
    acceptor_.async_accept(
        net::make_strand(ioc_),
        beast::bind_front_handler(&MarketDataServer::handle_accept, this));
}

void MarketDataServer::ctp_login()
{
    CThostFtdcReqUserLoginField req;
    memset(&req, 0, sizeof(req));
    
    // 行情API登录只需要BrokerID，不需要用户名和密码
    strcpy(req.BrokerID, broker_id_.c_str());
    // 行情登录可以使用空的用户ID和密码
    strcpy(req.UserID, "");
    strcpy(req.Password, "");
    
    int ret = ctp_api_->ReqUserLogin(&req, ++request_id_);
    if (ret != 0) {
        log_error("Failed to send market data login request, return code: " + std::to_string(ret));
    } else {
        ctp_connected_ = true;
        ctp_logged_in_ = true;
        log_info("Market data login request sent");
    }
}

std::string MarketDataServer::create_session_id()
{
    static std::random_device rd;
    static std::mt19937 gen(rd());
    static std::uniform_int_distribution<> dis(100000, 999999);
    
    auto now = std::chrono::system_clock::now();
    auto time_t = std::chrono::system_clock::to_time_t(now);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;
    
    std::ostringstream oss;
    oss << "session_" << time_t << "_" << ms.count() << "_" << dis(gen);
    return oss.str();
}

void MarketDataServer::add_session(std::shared_ptr<WebSocketSession> session)
{
    std::lock_guard<std::mutex> lock(sessions_mutex_);
    sessions_[session->get_session_id()] = session;
}

void MarketDataServer::remove_session(const std::string& session_id)
{
    if (use_multi_ctp_mode_) {
        // 多CTP连接模式：使用订阅分发器
        if (subscription_dispatcher_) {
            subscription_dispatcher_->remove_all_subscriptions_for_session(session_id);
        }
    } else {
        // 单CTP连接模式（兼容性）
        std::lock_guard<std::mutex> lock2(subscribers_mutex_);
        
        auto it = sessions_.find(session_id);
        if (it != sessions_.end()) {
            // 移除该会话的所有订阅
            const auto& subscriptions = it->second->get_subscriptions();
            for (const auto& instrument_id : subscriptions) {
                auto sub_it = instrument_subscribers_.find(instrument_id);
                if (sub_it != instrument_subscribers_.end()) {
                    sub_it->second.erase(session_id);
                    // 如果没有会话订阅该合约了，从CTP取消订阅
                    if (sub_it->second.empty()) {
                        instrument_subscribers_.erase(sub_it);
                        
                        if (ctp_api_ && ctp_logged_in_) {
                            char* instruments[] = {const_cast<char*>(instrument_id.c_str())};
                            int ret = ctp_api_->UnSubscribeMarketData(instruments, 1);
                            if (ret == 0) {
                                log_info("Auto-unsubscribed from CTP market data: " + instrument_id + 
                                       " (session disconnected)");
                            } else {
                                log_error("Failed to auto-unsubscribe from CTP market data: " + instrument_id +
                                         ", return code: " + std::to_string(ret));
                            }
                        }
                    }
                }
            }
        }
    }
    
    // 通用的session移除逻辑
    std::lock_guard<std::mutex> lock1(sessions_mutex_);
    auto it = sessions_.find(session_id);
    if (it != sessions_.end()) {
        sessions_.erase(it);
        log_info("Session removed: " + session_id);
    }
    
    // 清理上次发送的快照和版本号
    {
        std::lock_guard<std::mutex> lock_json(session_last_sent_mutex_);
        session_last_sent_structs_.erase(session_id);
        session_last_versions_.erase(session_id);
    }
    
    // 清理挂起队列
    {
        std::lock_guard<std::mutex> lock_pending(pending_peek_mutex_);
        pending_peek_sessions_.erase(session_id);
    }
}

void MarketDataServer::subscribe_instrument(const std::string& session_id, const std::string& instrument_id)
{
    if (use_multi_ctp_mode_) {
        // 多CTP连接模式：使用订阅分发器
        if (subscription_dispatcher_) {
            subscription_dispatcher_->add_subscription(session_id, instrument_id);
        }
        
        // 同时维护instrument_subscribers_映射以支持broadcast_market_data
        {
            std::lock_guard<std::mutex> lock(subscribers_mutex_);
            instrument_subscribers_[instrument_id].insert(session_id);
        }
    } else {
        // 单CTP连接模式（兼容性）
        std::lock_guard<std::mutex> lock(subscribers_mutex_);
        
        instrument_subscribers_[instrument_id].insert(session_id);
        
        // 如果这是第一个订阅该合约的会话，向CTP订阅行情
        if (instrument_subscribers_[instrument_id].size() == 1 && ctp_api_ && ctp_logged_in_) {
            char* instruments[] = {const_cast<char*>(instrument_id.c_str())};
            int ret = ctp_api_->SubscribeMarketData(instruments, 1);
            if (ret == 0) {
                log_info("Subscribed to CTP market data: " + instrument_id);
            } else {
                log_error("Failed to subscribe to CTP market data: " + instrument_id + 
                         ", return code: " + std::to_string(ret));
            }
        }
    }
}

void MarketDataServer::unsubscribe_instrument(const std::string& session_id, const std::string& instrument_id)
{
    if (use_multi_ctp_mode_) {

        // 多CTP连接模式：使用订阅分发器
        if (subscription_dispatcher_) {
            subscription_dispatcher_->remove_subscription(session_id, instrument_id);
        }
        
        // 同时维护instrument_subscribers_映射
        {
            std::lock_guard<std::mutex> lock(subscribers_mutex_);
            auto it = instrument_subscribers_.find(instrument_id);
            if (it != instrument_subscribers_.end()) {
                it->second.erase(session_id);
                if (it->second.empty()) {
                    instrument_subscribers_.erase(it);
                }
            }
        }
    } else {
        // 单CTP连接模式（兼容性）
        std::lock_guard<std::mutex> lock(subscribers_mutex_);
        
        auto it = instrument_subscribers_.find(instrument_id);
        if (it != instrument_subscribers_.end()) {
            it->second.erase(session_id);
            
            // 如果没有会话订阅该合约了，从CTP取消订阅
            if (it->second.empty()) {
                instrument_subscribers_.erase(it);
                
                if (ctp_api_ && ctp_logged_in_) {
                    char* instruments[] = {const_cast<char*>(instrument_id.c_str())};
                    int ret = ctp_api_->UnSubscribeMarketData(instruments, 1);
                    if (ret == 0) {
                        log_info("Unsubscribed from CTP market data: " + instrument_id);
                    } else {
                        log_error("Failed to unsubscribe from CTP market data: " + instrument_id +
                                 ", return code: " + std::to_string(ret));
                    }
                }
            }
        }
    }
}

int MarketDataServer::get_or_create_index(const std::string& instrument_id, const std::string& display_instrument)
{
    // 快速路径：使用读锁查找
    {
        std::shared_lock<std::shared_mutex> lock(index_map_mutex_);
        auto it = instrument_index_map_.find(instrument_id);
        if (it != instrument_index_map_.end()) {
            return it->second;
        }
    }
    
    // 慢速路径：使用写锁插入
    std::unique_lock<std::shared_mutex> lock(index_map_mutex_);
    
    // 再次检查以防其他线程已经插入
    auto it = instrument_index_map_.find(instrument_id);
    if (it != instrument_index_map_.end()) {
        return it->second;
    }
    
    int index = static_cast<int>(instrument_index_map_.size());
    
    // Check capacity
    if (index >= static_cast<int>(market_data_cache_.size())) {
        log_error("Market data cache capacity exceeded (" + std::to_string(market_data_cache_.size()) + ")");
        return -1;
    }
    
    instrument_index_map_[instrument_id] = index;
    if (!display_instrument.empty()) {
        display_instruments_cache_[index] = display_instrument;
    } else {
        // 回退或查找已有映射
        auto map_it = noheadtohead_instruments_map_.find(instrument_id);
        display_instruments_cache_[index] = (map_it != noheadtohead_instruments_map_.end()) 
            ? map_it->second : instrument_id;
    }
    
    return index;
}

int MarketDataServer::get_index(const std::string& instrument_id) const
{
    std::shared_lock<std::shared_mutex> lock(index_map_mutex_);
    auto it = instrument_index_map_.find(instrument_id);
    if (it != instrument_index_map_.end()) {
        return it->second;
    }
    return -1;
}

void MarketDataServer::cache_market_data(const std::string& instrument_id, const MarketDataStruct& data, const std::string& display_instrument)
{
    int index = get_or_create_index(instrument_id, display_instrument);
    if (index < 0) return;
    
    // SeqLock 写入过程:
    // 1. 加载当前序列号
    // 2. 序列号 + 1 (变奇数) -> 内存屏障 release
    // 3. 写入数据
    // 4. 序列号 + 1 (变偶数) -> 内存屏障 release
    
    AtomicMarketDataEntry& entry = market_data_cache_[index];
    
    uint64_t seq = entry.sequence.load(std::memory_order_relaxed);
    entry.sequence.store(seq + 1, std::memory_order_release);
    
    entry.data = data;
    entry.has_data = true;
    
    entry.sequence.store(seq + 2, std::memory_order_release);
    
    // 异步到WebSocket io_context线程，避免阻塞CTP回调线程
    ioc_.post([this, instrument_id]() {
        notify_pending_sessions(instrument_id);
    });
}

void MarketDataServer::on_component_update(const std::string& component_id, const MarketDataStruct& market_data)
{
}

void MarketDataServer::handle_peek_message(const std::string& session_id)
{
    using clock = std::chrono::steady_clock;
    const auto start_time = clock::now();
    
    // 1. 获取 Session 和订阅信息
    std::shared_ptr<WebSocketSession> session_ptr;
    const std::set<std::string>* subscriptions_ptr = nullptr;
    
    {
        std::lock_guard<std::mutex> lock(sessions_mutex_);
        auto session_it = sessions_.find(session_id);
        if (session_it == sessions_.end()) {
            return;
        }
        session_ptr = session_it->second;
        // 使用引用避免拷贝，但需要确保在锁保护下使用
        subscriptions_ptr = &session_it->second->get_subscriptions();
    }
    
    // 快速检查是否为空（避免后续处理）
    if (!subscriptions_ptr || subscriptions_ptr->empty()) {
        return;
    }
    
    // 创建局部拷贝用于后续处理（此时已释放锁）
    std::set<std::string> subscriptions = *subscriptions_ptr;

    // 2. 获取该 Session 上次的状态（版本号和快照）
    // 使用指针引用避免不必要的拷贝，只在需要时拷贝
    const std::unordered_map<std::string, uint64_t>* last_versions_ptr = nullptr;
    const std::unordered_map<std::string, MarketDataStruct>* last_sent_structs_ptr = nullptr;
    bool has_last_snapshot = false;
    
    {
        std::lock_guard<std::mutex> lock(session_last_sent_mutex_);
        auto ver_it = session_last_versions_.find(session_id);
        if (ver_it != session_last_versions_.end() && !ver_it->second.empty()) {
            last_versions_ptr = &ver_it->second;
        }
        
        auto struct_it = session_last_sent_structs_.find(session_id);
        has_last_snapshot = (struct_it != session_last_sent_structs_.end() && !struct_it->second.empty());
        if (has_last_snapshot) {
            last_sent_structs_ptr = &struct_it->second;
        }
    }
    
    // 创建局部拷贝用于后续处理（此时已释放锁）
    std::unordered_map<std::string, uint64_t> last_versions_copy;
    std::unordered_map<std::string, MarketDataStruct> last_sent_structs_copy;
    if (last_versions_ptr) {
        last_versions_copy = *last_versions_ptr;
    }
    if (last_sent_structs_ptr) {
        last_sent_structs_copy = *last_sent_structs_ptr;
    }

    // 3. 收集有更新的合约数据
    auto updated_instruments = collect_market_data_updates(subscriptions, last_versions_copy);
    
    // 4. 如果没有更新，加入挂起队列
    if (updated_instruments.empty()) {
        if (has_last_snapshot) { // 只有在已经推送过数据的情况下才挂起，否则应该推送空全量(虽然这很少见)
            std::lock_guard<std::mutex> lock(pending_peek_mutex_);
            pending_peek_sessions_.insert(session_id);
        }
        return;
    }
 
    // 5. 发送数据（全量或增量）
    if (!has_last_snapshot) {
        send_full_snapshot(session_ptr, updated_instruments);
    } else {
        size_t diff_count = send_diff_snapshot(session_ptr, updated_instruments, last_sent_structs_copy);
        const auto end_time = clock::now();
        const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
        BOOST_LOG_TRIVIAL(info) << "peek_message processing time: " << elapsed_ms << " ms, diff instrument count: " << diff_count;
    }

    // 6. 更新 Session 状态
    update_session_state(session_id, updated_instruments);
}

std::vector<std::pair<std::string, MarketDataServer::SnapshotData>> MarketDataServer::collect_market_data_updates(
    const std::set<std::string>& subscriptions, 
    const std::unordered_map<std::string, uint64_t>& last_versions)
{
    std::vector<std::pair<std::string, SnapshotData>> updated_instruments;
    updated_instruments.reserve(subscriptions.size());
    
    for (const auto& instrument_id : subscriptions) {
        int index = get_index(instrument_id);
        if (index == -1) continue;

        AtomicMarketDataEntry& entry = market_data_cache_[index];
        if (!entry.has_data) continue;

        MarketDataStruct data_snapshot;
        uint64_t seq_start = 0, seq_end = 0;
        int retries = 0;

        // SeqLock 读取循环
        do {
            seq_start = entry.sequence.load(std::memory_order_acquire);
            if (seq_start % 2 != 0) {
                std::this_thread::yield();
                if (++retries > 100) break;
                continue;
            }
            
            data_snapshot = entry.data;
            
            seq_end = entry.sequence.load(std::memory_order_acquire);
            if (++retries > 100) break;
            
        } while (seq_start != seq_end);

        if (seq_start != seq_end || seq_start % 2 != 0) {
            continue;
        }

        uint64_t current_version = seq_end / 2;

        // 检查版本号
        if (last_versions.empty() || 
            last_versions.find(instrument_id) == last_versions.end() ||
            current_version > last_versions.at(instrument_id)) {
            
            SnapshotData snapshot;
            snapshot.data = data_snapshot;
            {
                std::shared_lock<std::shared_mutex> lock(index_map_mutex_);
                if (index < static_cast<int>(display_instruments_cache_.size())) {
                    snapshot.display_instrument = &display_instruments_cache_[index];
                }
            }
            snapshot.has_data = true;
            snapshot.version = current_version;
            updated_instruments.emplace_back(instrument_id, std::move(snapshot));
        }
    }
    return updated_instruments;
}

void MarketDataServer::send_full_snapshot(
    std::shared_ptr<WebSocketSession> session, 
    const std::vector<std::pair<std::string, SnapshotData>>& updates)
{
    rapidjson::Document full_response;
    full_response.SetObject();
    auto& allocator = full_response.GetAllocator();
    
    rapidjson::Value data_array(rapidjson::kArrayType);
    rapidjson::Value data_obj(rapidjson::kObjectType);
    rapidjson::Value quotes_obj(rapidjson::kObjectType);
    
    for (const auto& entry : updates) {
        const auto& instrument_id = entry.first;
        const auto& cached_data = entry.second;
        
        // display_instrument 已在 collect_market_data_updates 中设置，无需重复查找
        const std::string& display_instrument = (cached_data.display_instrument == nullptr || cached_data.display_instrument->empty()) 
            ? instrument_id : *cached_data.display_instrument;
        
        rapidjson::Value inst_data = struct_to_json(cached_data.data, allocator);
        quotes_obj.AddMember(rapidjson::Value(display_instrument.c_str(), allocator), inst_data, allocator);
    }
    
    data_obj.AddMember("quotes", quotes_obj, allocator);
    data_array.PushBack(data_obj, allocator);
    
    rapidjson::Value meta_obj(rapidjson::kObjectType);
    meta_obj.AddMember("account_id", rapidjson::Value("", allocator), allocator);
    meta_obj.AddMember("ins_list", rapidjson::Value("", allocator), allocator);
    meta_obj.AddMember("mdhis_more_data", false, allocator);
    data_array.PushBack(meta_obj, allocator);
    
    full_response.AddMember("aid", "rtn_data", allocator);
    full_response.AddMember("data", data_array, allocator);
    
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    full_response.Accept(writer);
    session->send_message(buffer.GetString());
}

size_t MarketDataServer::send_diff_snapshot(
    std::shared_ptr<WebSocketSession> session, 
    const std::vector<std::pair<std::string, SnapshotData>>& updates,
    const std::unordered_map<std::string, MarketDataStruct>& last_snapshots)
{
    // 计算 Diff
    std::vector<std::pair<std::string, SnapshotData>> diff_instruments;
    
    for (const auto& entry : updates) {
        const auto& instrument_id = entry.first;
        const auto& cached_data = entry.second;
        
        auto old_it = last_snapshots.find(instrument_id);
        if (old_it != last_snapshots.end()) {
            // 预先检查是否有差异，避免不必要的 JSON 构建
            if (has_struct_changes(old_it->second, cached_data.data)) {
                diff_instruments.push_back(entry);
            }
        } else {
            // 新增合约
            diff_instruments.push_back(entry);
        }
    }
    
    if (diff_instruments.empty()) return 0;

    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    
    // 开始构建JSON: { "aid": "rtn_data", "data": [ ... ] }
    writer.StartObject();
    
    writer.Key("aid");
    writer.String("rtn_data");
    
    writer.Key("data");
    writer.StartArray();
    
    // Data Object 1: Quotes
    writer.StartObject();
    writer.Key("quotes");
    writer.StartObject();
    
    bool has_content = false;

    // 预备分配器用于全量数据构建(struct_to_json仍需要allocator，暂时保留DOM模式混合使用或者重构)
    // 为了最大化性能，全量数据应该也SAX化，但此处为了最小改动先只处理Diff逻辑
    rapidjson::Document temp_doc; 
    auto& allocator = temp_doc.GetAllocator();

    for (const auto& entry : diff_instruments) {
        const auto& instrument_id = entry.first;
        const auto& cached_data = entry.second;
        
        // display_instrument 已在 collect_market_data_updates 中设置，无需重复查找
        const std::string& display_instrument = (cached_data.display_instrument == nullptr || cached_data.display_instrument->empty()) 
            ? instrument_id : *cached_data.display_instrument;
        
        auto old_it = last_snapshots.find(instrument_id);
        if (old_it != last_snapshots.end()) {
            // 计算 Diff 并直接写入 Writer
            writer.Key(display_instrument.c_str());
            compute_struct_diff(old_it->second, cached_data.data, writer);
            has_content = true;
        } else {
            // 全量 - 这里暂时混合使用 struct_to_json (DOM) 然后 Accept 到 Writer
            // 理想情况是 struct_to_json 也重构为 Writer 模式
            writer.Key(display_instrument.c_str());
            rapidjson::Value full_val = struct_to_json(cached_data.data, allocator);
            full_val.Accept(writer);
            has_content = true;
        }
    }
    
    writer.EndObject(); // End quotes
    writer.EndObject(); // End Data Object 1

    // Data Object 2: Meta
    writer.StartObject();
    writer.Key("account_id"); writer.String("");
    writer.Key("ins_list"); writer.String("");
    writer.Key("mdhis_more_data"); writer.Bool(false);
    writer.EndObject();
    
    writer.EndArray(); // End data array
    writer.EndObject(); // End root object
    
    // 如果没有实际内容（理论上 diff_instruments 不空就不会发生，除非 compute_struct_diff 全空），
    // 但前面已有 has_struct_changes 检查，所以这里应该总是有内容。
    if (has_content) {
        session->send_message(buffer.GetString());
    } else {
        std::lock_guard<std::mutex> lock(pending_peek_mutex_);
        pending_peek_sessions_.insert(session->get_session_id());
    }
    
    return diff_instruments.size();
}

void MarketDataServer::update_session_state(
    const std::string& session_id, 
    const std::vector<std::pair<std::string, SnapshotData>>& updates)
{
    std::lock_guard<std::mutex> lock(session_last_sent_mutex_);
    
    // 再次检查 session 是否存在，可能在处理过程中被删除
    auto struct_it = session_last_sent_structs_.find(session_id);
    if (struct_it == session_last_sent_structs_.end()) {
        session_last_sent_structs_[session_id] = std::unordered_map<std::string, MarketDataStruct>();
        struct_it = session_last_sent_structs_.find(session_id);
    }
    
    auto& snapshot_structs = struct_it->second;
    auto& version_map = session_last_versions_[session_id];
    
    for (const auto& entry : updates) {
        const auto& instrument_id = entry.first;
        const auto& cached_data = entry.second;
        snapshot_structs[instrument_id] = cached_data.data;
        version_map[instrument_id] = cached_data.version;
    }
}

void MarketDataServer::notify_pending_sessions(const std::string& instrument_id)
{
    std::set<std::string> sessions_to_notify;
    
    // 获取订阅了该合约且处于挂起状态的session
    {
        std::lock_guard<std::mutex> lock1(subscribers_mutex_);
        std::lock_guard<std::mutex> lock2(pending_peek_mutex_);
        
        auto sub_it = instrument_subscribers_.find(instrument_id);
        if (sub_it == instrument_subscribers_.end()) {
            return;
        }
        
        // 找出既订阅了该合约，又在挂起队列中的session
        for (const auto& session_id : sub_it->second) {
            if (pending_peek_sessions_.find(session_id) != pending_peek_sessions_.end()) {
                sessions_to_notify.insert(session_id);
                pending_peek_sessions_.erase(session_id);  // 从挂起队列移除
            }
        }
    }
    
    // 唤醒这些session，触发数据推送
    for (const auto& session_id : sessions_to_notify) {
        log_info("Waking up pending session: " + session_id + " due to market data update: " + instrument_id);
        handle_peek_message(session_id);  // 重新处理peek_message
    }
}

void MarketDataServer::send_to_session(const std::string& session_id, const std::string& message)
{
    std::lock_guard<std::mutex> lock(sessions_mutex_);
    
    auto it = sessions_.find(session_id);
    if (it != sessions_.end()) {
        it->second->send_message(message);
    }
}

std::vector<std::string> MarketDataServer::get_all_instruments()
{
    std::vector<std::string> instruments;
    
    if (ins_map_) {
        for (auto it = ins_map_->begin(); it != ins_map_->end(); ++it) {
            std::string key(it->first.data());
            key.erase(std::find(key.begin(), key.end(), '\0'), key.end());
            if (!key.empty()) {
                instruments.push_back(key);
            }
        }
    }
    
    return instruments;
}

std::vector<std::string> MarketDataServer::search_instruments(const std::string& pattern)
{
    std::vector<std::string> matching_instruments;
    
    if (ins_map_) {
        std::string lower_pattern = pattern;
        std::transform(lower_pattern.begin(), lower_pattern.end(), lower_pattern.begin(), ::tolower);
        
        for (auto it = ins_map_->begin(); it != ins_map_->end(); ++it) {
            std::string key(it->first.data());
            key.erase(std::find(key.begin(), key.end(), '\0'), key.end());
            
            if (!key.empty()) {
                std::string lower_key = key;
                std::transform(lower_key.begin(), lower_key.end(), lower_key.begin(), ::tolower);
                
                if (lower_key.find(lower_pattern) != std::string::npos) {
                    matching_instruments.push_back(key);
                }
            }
        }
    }
    
    return matching_instruments;
}

void MarketDataServer::log_info(const std::string& message)
{
    BOOST_LOG_TRIVIAL(info) << message;
}

void MarketDataServer::log_error(const std::string& message)
{
    BOOST_LOG_TRIVIAL(error) << message;
}

void MarketDataServer::log_warning(const std::string& message)
{
    BOOST_LOG_TRIVIAL(warning) << message;
}

// 多CTP系统实现
bool MarketDataServer::init_multi_ctp_system()
{
    log_info("Initializing multi-CTP system...");
    
    try {
        // 创建订阅分发器
        subscription_dispatcher_ = std::make_unique<SubscriptionDispatcher>(this);
        
        // 创建连接管理器
        connection_manager_ = std::make_unique<CTPConnectionManager>(this, subscription_dispatcher_.get());
        
        // 初始化订阅分发器
        if (!subscription_dispatcher_->initialize(connection_manager_.get(), multi_ctp_config_)) {
            log_error("Failed to initialize subscription dispatcher");
            return false;
        }
        
        // 添加所有连接配置
        for (const auto& conn_config : multi_ctp_config_.connections) {
            if (conn_config.enabled) {
                if (!connection_manager_->add_connection(conn_config)) {
                    log_error("Failed to add connection: " + conn_config.connection_id);
                    return false;
                }
                log_info("Added CTP connection: " + conn_config.connection_id + 
                        " -> " + conn_config.front_addr);
            } else {
                log_info("Skipped disabled connection: " + conn_config.connection_id);
            }
        }
        
        // 启动所有连接
        if (!connection_manager_->start_all_connections()) {
            log_warning("Some CTP connections failed to start");
        }
        
        log_info("Multi-CTP system initialized successfully with " + 
                std::to_string(connection_manager_->get_total_connections()) + " connections");
        return true;
        
    } catch (const std::exception& e) {
        log_error("Exception initializing multi-CTP system: " + std::string(e.what()));
        return false;
    }
}

void MarketDataServer::cleanup_multi_ctp_system()
{
    if (connection_manager_) {
        connection_manager_->stop_all_connections();
        connection_manager_.reset();
    }
    
    if (subscription_dispatcher_) {
        subscription_dispatcher_->shutdown();
        subscription_dispatcher_.reset();
    }
    
    log_info("Multi-CTP system cleaned up");
}

// 多连接版本的状态查询
bool MarketDataServer::is_ctp_connected() const
{
    if (use_multi_ctp_mode_) {
        return connection_manager_ && connection_manager_->get_active_connections() > 0;
    } else {
        return ctp_connected_;
    }
}

bool MarketDataServer::is_ctp_logged_in() const
{
    if (use_multi_ctp_mode_) {
        return connection_manager_ && connection_manager_->get_active_connections() > 0;
    } else {
        return ctp_logged_in_;
    }
}

size_t MarketDataServer::get_active_connections_count() const
{
    if (use_multi_ctp_mode_ && connection_manager_) {
        return connection_manager_->get_active_connections();
    }
    return ctp_logged_in_ ? 1 : 0;
}

std::vector<std::string> MarketDataServer::get_connection_status() const
{
    std::vector<std::string> status_list;
    
    if (use_multi_ctp_mode_ && connection_manager_) {
        auto connections = connection_manager_->get_all_connections();
        for (const auto& conn : connections) {
            std::string status = conn->get_connection_id() + ": ";
            switch (conn->get_status()) {
                case CTPConnectionStatus::DISCONNECTED:
                    status += "DISCONNECTED";
                    break;
                case CTPConnectionStatus::CONNECTING:
                    status += "CONNECTING";
                    break;
                case CTPConnectionStatus::CONNECTED:
                    status += "CONNECTED";
                    break;
                case CTPConnectionStatus::LOGGED_IN:
                    status += "LOGGED_IN (" + std::to_string(conn->get_subscription_count()) + " subs)";
                    break;
                case CTPConnectionStatus::ERROR:
                    status += "ERROR";
                    break;
            }
            status_list.push_back(status);
        }
    } else {
        std::string status = "single_ctp: ";
        if (ctp_logged_in_) {
            status += "LOGGED_IN";
        } else if (ctp_connected_) {
            status += "CONNECTED";
        } else {
            status += "DISCONNECTED";
        }
        status_list.push_back(status);
    }
    
    return status_list;
}