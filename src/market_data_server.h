#pragma once

#include "../libs/ThostFtdcMdApi.h"
#include <boost/asio.hpp>
#include <boost/beast.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/thread.hpp>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/containers/map.hpp>
#include <boost/interprocess/allocators/allocator.hpp>
#include <memory>
#include <set>
#include <map>
#include <string>
#include <atomic>
#include <mutex>
#include <queue>
#include <array>
#include <unordered_map>
#include "../include/open-trade-common/types.h"
#include "ctp_connection_manager.h"
#include "subscription_dispatcher.h"
#include "multi_ctp_config.h"

namespace beast = boost::beast;
namespace http = beast::http;
namespace websocket = beast::websocket;
namespace net = boost::asio;
using tcp = net::ip::tcp;

struct MarketDataStruct {
    char instrument_id[32];
    char datetime[32];
    uint64_t timestamp;
    
    double ask_price[10];
    int ask_volume[10];
    double bid_price[10];
    int bid_volume[10];
    
    double last_price;
    double highest;
    double lowest;
    double open;
    double close;
    double average;
    int volume;
    double amount;
    int64_t open_interest;
    double settlement;
    double upper_limit;
    double lower_limit;
    int64_t pre_open_interest;
    double pre_settlement;
    double pre_close;

    // Flags to indicate validity of fields (since we need to handle nulls/-)
    // Using a simple convention: 
    // Prices: > 1e-6 && < 1e300 is valid.
    // Close/Settlement: checks against 1e300.
};

class MarketDataServer;

// WebSocket连接会话
class WebSocketSession : public std::enable_shared_from_this<WebSocketSession>
{
public:
    explicit WebSocketSession(tcp::socket&& socket, MarketDataServer* server);
    ~WebSocketSession();
    
    void run();
    void send_message(const std::string& message);
    void close();
    
    std::string get_session_id() const { return session_id_; }
    const std::set<std::string>& get_subscriptions() const { return subscriptions_; }
    
private:
    void on_accept(beast::error_code ec);
    void do_read();
    void on_read(beast::error_code ec, std::size_t bytes_transferred);
    void start_write();
    void on_write(beast::error_code ec, std::size_t bytes_transferred);
    
    void handle_message(const std::string& message);
    void send_error(const std::string& error_msg);
    void send_response(const std::string& type, const rapidjson::Document& data);
    
    websocket::stream<beast::tcp_stream> ws_;
    beast::flat_buffer buffer_;
    std::queue<std::string> message_queue_;
    std::string current_write_message_;
    std::string session_id_;
    std::set<std::string> subscriptions_;
    MarketDataServer* server_;
    std::mutex write_mutex_;
    bool is_writing_;
};

// CTP行情SPI回调实现
class MarketDataSpi : public CThostFtdcMdSpi
{
public:
    explicit MarketDataSpi(MarketDataServer* server);
    virtual ~MarketDataSpi();
    
    // CTP回调函数
    virtual void OnFrontConnected() override;
    virtual void OnFrontDisconnected(int nReason) override;
    virtual void OnRspUserLogin(CThostFtdcRspUserLoginField *pRspUserLogin, 
                               CThostFtdcRspInfoField *pRspInfo, 
                               int nRequestID, bool bIsLast) override;
    virtual void OnRspSubMarketData(CThostFtdcSpecificInstrumentField *pSpecificInstrument, 
                                   CThostFtdcRspInfoField *pRspInfo, 
                                   int nRequestID, bool bIsLast) override;
    virtual void OnRtnDepthMarketData(CThostFtdcDepthMarketDataField *pDepthMarketData) override;
    virtual void OnRspError(CThostFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast) override;
    
private:
    MarketDataServer* server_;
};

// 主服务器类
class MarketDataServer
{
public:
    // 原有构造函数（兼容性）
    explicit MarketDataServer(const std::string& ctp_front_addr,
                             const std::string& broker_id,
                             int websocket_port = 7799);
    
    // 新的多连接构造函数
    explicit MarketDataServer(const MultiCTPConfig& config);
    
    ~MarketDataServer();
    
    bool start();
    void stop();
    bool is_running() const { return is_running_; }
    
    // WebSocket会话管理
    void add_session(std::shared_ptr<WebSocketSession> session);
    void remove_session(const std::string& session_id);
    void subscribe_instrument(const std::string& session_id, const std::string& instrument_id);
    void unsubscribe_instrument(const std::string& session_id, const std::string& instrument_id);
    
    // 行情数据推送
    void send_to_session(const std::string& session_id, const std::string& message);
    void handle_peek_message(const std::string& session_id);
    void cache_market_data(const std::string& instrument_id, const MarketDataStruct& data, const std::string& display_instrument);
    void on_component_update(const std::string& component_id, const MarketDataStruct& market_data);
    
    
    // 从CTP数据构建MarketDataStruct
    static MarketDataStruct build_market_data_struct(CThostFtdcDepthMarketDataField *pDepthMarketData,
                                                     const std::string& display_instrument,
                                                     const uint64_t cur_time);
    
    // 从MarketDataStruct构建JSON（用于发送）
    static rapidjson::Value struct_to_json(const MarketDataStruct& data,
                                          rapidjson::Document::AllocatorType& allocator);
    
    // 结构体字段级比较，返回差异字段的JSON
    static void compute_struct_diff(const MarketDataStruct& old_data,
                                   const MarketDataStruct& new_data,
                                   rapidjson::Value& diff_val,
                                   rapidjson::Document::AllocatorType& allocator);

    // 合约管理
    std::vector<std::string> get_all_instruments();
    std::vector<std::string> search_instruments(const std::string& pattern);
    
    // CTP连接状态（多连接版本）
    bool is_ctp_connected() const;
    bool is_ctp_logged_in() const;
    size_t get_active_connections_count() const;
    std::vector<std::string> get_connection_status() const;
    
    // 多连接管理接口
    CTPConnectionManager* get_connection_manager() { return connection_manager_.get(); }
    SubscriptionDispatcher* get_subscription_dispatcher() { return subscription_dispatcher_.get(); }
    
    // 获取服务器状态信息
    rapidjson::Document get_server_status_json();
    void notify_pending_sessions(const std::string& instrument_id);
    
    net::io_context& io_context() { return ioc_; }

    // 日志函数
    void log_info(const std::string& message);
    void log_error(const std::string& message);
    void log_warning(const std::string& message);
    
private:
    struct CachedMarketData {
        MarketDataStruct data;
        std::string display_instrument;
        uint64_t version = 0;
        bool has_data = false;
    };
    
    struct alignas(64) MarketDataCacheShard {
        std::mutex mutex;
        std::unordered_map<std::string, CachedMarketData> cache;
    };
    void init_shared_memory();
    void cleanup_shared_memory();
    void start_websocket_server();
    void handle_accept(beast::error_code ec, tcp::socket socket);
    MarketDataCacheShard& get_market_data_cache_shard(const std::string& instrument_id);
public:
    void ctp_login();
    std::string create_session_id();
    std::map<std::string, std::string> noheadtohead_instruments_map_; // ctp_instrument -> display_instrument
private:
    bool init_multi_ctp_system();
    void cleanup_multi_ctp_system();
    
    // 兼容性：单连接模式
    std::string ctp_front_addr_;
    std::string broker_id_;
    CThostFtdcMdApi* ctp_api_;
    std::unique_ptr<MarketDataSpi> md_spi_;
    std::atomic<bool> ctp_connected_;
    std::atomic<bool> ctp_logged_in_;
    
    // 多连接系统
    MultiCTPConfig multi_ctp_config_;
    std::unique_ptr<CTPConnectionManager> connection_manager_;
    std::unique_ptr<SubscriptionDispatcher> subscription_dispatcher_;
    bool use_multi_ctp_mode_;
    
    // WebSocket服务器
    net::io_context ioc_;
    int websocket_port_;
    tcp::acceptor acceptor_;
    std::map<std::string, std::shared_ptr<WebSocketSession>> sessions_;
    std::map<std::string, std::set<std::string>> instrument_subscribers_; // instrument_id -> session_ids
    std::array<MarketDataCacheShard, 1024> market_data_cache_shards_;
    
    // 客户端上次发送的行情数据快照（使用结构体存储）: session_id -> instrument_id -> MarketDataStruct
    std::map<std::string, std::unordered_map<std::string, MarketDataStruct>> session_last_sent_structs_;
    // session上次发送的合约版本号: session_id -> instrument_id -> version
    std::unordered_map<std::string, std::unordered_map<std::string, uint64_t>> session_last_versions_;
    std::mutex session_last_sent_mutex_;
    
    // 等待行情更新的session集合（挂起的peek_message）
    std::set<std::string> pending_peek_sessions_;
    std::mutex pending_peek_mutex_;
    
    // 共享内存相关
    boost::interprocess::managed_shared_memory* segment_;
    ShmemAllocator* alloc_inst_;
    InsMapType* ins_map_;
    
    // 线程同步
    std::mutex sessions_mutex_;
    std::mutex subscribers_mutex_;
    std::atomic<bool> is_running_;
    boost::thread server_thread_;
    
    // 请求ID管理
    std::atomic<int> request_id_;
    
};