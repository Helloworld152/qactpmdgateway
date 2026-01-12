#include "ctp_connection_manager.h"
#include "subscription_dispatcher.h"
#include "market_data_server.h"
#include <boost/filesystem.hpp>
#include <algorithm>
#include <cstring>
#include <thread>
#include <cstdlib>
#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>

// CTPConnection 实现
CTPConnection::CTPConnection(const CTPConnectionConfig& config, 
                            MarketDataServer* server,
                            SubscriptionDispatcher* dispatcher)
    : config_(config)
    , server_(server)
    , dispatcher_(dispatcher)
    , ctp_api_(nullptr)
    , status_(CTPConnectionStatus::DISCONNECTED)
    , error_count_(0)
    , request_id_(0)
{
}

CTPConnection::~CTPConnection()
{
    stop();
}

bool CTPConnection::start()
{
    std::lock_guard<std::mutex> lock(api_mutex_);
    
    if (status_ != CTPConnectionStatus::DISCONNECTED) {
        return false;
    }
    
    try {
        status_ = CTPConnectionStatus::CONNECTING;
        
        // 创建CTP API实例
        std::string flow_path = "./ctpflow/" + config_.connection_id + "/";
        
        // 确保flow目录存在
        try {
            boost::filesystem::create_directories(flow_path);
        } catch (const std::exception& e) {
            server_->log_warning("Failed to create flow directory: " + flow_path + ", error: " + e.what());
        }
        
        ctp_api_ = CThostFtdcMdApi::CreateFtdcMdApi(flow_path.c_str());
        
        if (!ctp_api_) {
            server_->log_error("Failed to create CTP API for connection: " + config_.connection_id);
            status_ = CTPConnectionStatus::ERROR;
            return false;
        }
        
        ctp_api_->RegisterSpi(this);
        ctp_api_->RegisterFront(const_cast<char*>(config_.front_addr.c_str()));
        ctp_api_->Init();
        
        server_->log_info("CTP connection " + config_.connection_id + " starting...");
        return true;
        
    } catch (const std::exception& e) {
        server_->log_error("Exception starting CTP connection " + config_.connection_id + ": " + e.what());
        status_ = CTPConnectionStatus::ERROR;
        return false;
    }
}

void CTPConnection::stop()
{
    std::lock_guard<std::mutex> lock(api_mutex_);
    
    status_ = CTPConnectionStatus::DISCONNECTED;
    
    if (ctp_api_) {
        ctp_api_->Release();
        ctp_api_ = nullptr;
    }
    
    {
        std::lock_guard<std::mutex> sub_lock(subscriptions_mutex_);
        subscribed_instruments_.clear();
    }
    
    server_->log_info("CTP connection " + config_.connection_id + " stopped");
}

bool CTPConnection::restart()
{
    server_->log_info("Restarting CTP connection: " + config_.connection_id);
    stop();
    std::this_thread::sleep_for(std::chrono::seconds(2));

    if (!server_->is_running()) {
        server_->log_info("Server is stopping, cancelling restart of " + config_.connection_id);
        return false;
    }

    return start();
}

bool CTPConnection::subscribe_instrument(const std::string& instrument_id)
{
    std::lock_guard<std::mutex> api_lock(api_mutex_);
    std::lock_guard<std::mutex> sub_lock(subscriptions_mutex_);
    
    if (status_ != CTPConnectionStatus::LOGGED_IN) {
        server_->log_warning("CTP connection " + config_.connection_id + " not ready for subscription");
        return false;
    }
    
    if (subscribed_instruments_.find(instrument_id) != subscribed_instruments_.end()) {
        server_->log_warning("Instrument " + instrument_id + " already subscribed on connection " + config_.connection_id);
        return true;
    }
    
    if (subscribed_instruments_.size() >= static_cast<size_t>(config_.max_subscriptions)) {
        server_->log_warning("Connection " + config_.connection_id + " has reached max subscriptions limit");
        return false;
    }
    
    char* instruments[] = {const_cast<char*>(instrument_id.c_str())};
    int ret = ctp_api_->SubscribeMarketData(instruments, 1);
    
    if (ret == 0) {
        subscribed_instruments_.insert(instrument_id);
        server_->log_info("Subscribed to " + instrument_id + " on connection " + config_.connection_id);
        return true;
    } else {
        server_->log_error("Failed to subscribe to " + instrument_id + " on connection " + 
                          config_.connection_id + ", return code: " + std::to_string(ret));
        error_count_++;
        return false;
    }
}

bool CTPConnection::unsubscribe_instrument(const std::string& instrument_id)
{
    std::lock_guard<std::mutex> api_lock(api_mutex_);
    std::lock_guard<std::mutex> sub_lock(subscriptions_mutex_);
    
    if (status_ != CTPConnectionStatus::LOGGED_IN) {
        return false;
    }
    
    auto it = subscribed_instruments_.find(instrument_id);
    if (it == subscribed_instruments_.end()) {
        return true; // 已经没有订阅了
    }
    
    char* instruments[] = {const_cast<char*>(instrument_id.c_str())};
    int ret = ctp_api_->UnSubscribeMarketData(instruments, 1);
    
    if (ret == 0) {
        subscribed_instruments_.erase(it);
        server_->log_info("Unsubscribed from " + instrument_id + " on connection " + config_.connection_id);
        return true;
    } else {
        server_->log_error("Failed to unsubscribe from " + instrument_id + " on connection " + 
                          config_.connection_id + ", return code: " + std::to_string(ret));
        error_count_++;
        return false;
    }
}

size_t CTPConnection::get_subscription_count() const
{
    std::lock_guard<std::mutex> lock(subscriptions_mutex_);
    return subscribed_instruments_.size();
}

bool CTPConnection::can_accept_more_subscriptions() const
{
    if (status_ != CTPConnectionStatus::LOGGED_IN) {
        return false;
    }
    
    std::lock_guard<std::mutex> lock(subscriptions_mutex_);
    return subscribed_instruments_.size() < static_cast<size_t>(config_.max_subscriptions);
}

void CTPConnection::OnFrontConnected()
{
    server_->log_info("CTP connection " + config_.connection_id + " front connected");
    status_ = CTPConnectionStatus::CONNECTED;
    login();
}

void CTPConnection::OnFrontDisconnected(int nReason)
{
    server_->log_warning("CTP connection " + config_.connection_id + " front disconnected, reason: " + std::to_string(nReason));
    status_ = CTPConnectionStatus::DISCONNECTED;
    error_count_++;
    
    // 通知订阅分发器连接断开
    if (dispatcher_) {
        dispatcher_->handle_connection_failure(config_.connection_id);
    }
}

void CTPConnection::OnRspUserLogin(CThostFtdcRspUserLoginField *pRspUserLogin,
                                  CThostFtdcRspInfoField *pRspInfo,
                                  int nRequestID, bool bIsLast)
{
    if (pRspInfo && pRspInfo->ErrorID != 0) {
        server_->log_error("CTP login failed on connection " + config_.connection_id + ": " + std::string(pRspInfo->ErrorMsg));
        status_ = CTPConnectionStatus::ERROR;
        error_count_++;
        return;
    }
    
    server_->log_info("CTP login successful on connection " + config_.connection_id);
    status_ = CTPConnectionStatus::LOGGED_IN;
    
    // 通知订阅分发器连接恢复
    if (dispatcher_) {
        dispatcher_->handle_connection_recovery(config_.connection_id);
    }
}

void CTPConnection::OnRspSubMarketData(CThostFtdcSpecificInstrumentField *pSpecificInstrument,
                                      CThostFtdcRspInfoField *pRspInfo,
                                      int nRequestID, bool bIsLast)
{
    if (pRspInfo && pRspInfo->ErrorID != 0) {
        std::string error_msg = pRspInfo->ErrorMsg ? std::string(pRspInfo->ErrorMsg) : "Unknown error";
        server_->log_error("Subscribe market data failed on connection " + config_.connection_id + ": " + error_msg);
        
        if (pSpecificInstrument && dispatcher_) {
            dispatcher_->on_subscription_failed(config_.connection_id, pSpecificInstrument->InstrumentID);
        }
        error_count_++;
        return;
    }
    
    if (pSpecificInstrument && dispatcher_) {
        std::string instrument_id = pSpecificInstrument->InstrumentID;
        server_->log_info("Successfully subscribed to " + instrument_id + " on connection " + config_.connection_id);
        dispatcher_->on_subscription_success(config_.connection_id, instrument_id);
    }
}

void CTPConnection::OnRspUnSubMarketData(CThostFtdcSpecificInstrumentField *pSpecificInstrument,
                                        CThostFtdcRspInfoField *pRspInfo,
                                        int nRequestID, bool bIsLast)
{
    if (pRspInfo && pRspInfo->ErrorID != 0) {
        std::string error_msg = pRspInfo->ErrorMsg ? std::string(pRspInfo->ErrorMsg) : "Unknown error";
        server_->log_error("Unsubscribe market data failed on connection " + config_.connection_id + ": " + error_msg);
        error_count_++;
        return;
    }
    
    if (pSpecificInstrument && dispatcher_) {
        std::string instrument_id = pSpecificInstrument->InstrumentID;
        server_->log_info("Successfully unsubscribed from " + instrument_id + " on connection " + config_.connection_id);
        dispatcher_->on_unsubscription_success(config_.connection_id, instrument_id);
    }
}

void CTPConnection::OnRtnDepthMarketData(CThostFtdcDepthMarketDataField *pDepthMarketData)
{
    using clock = std::chrono::steady_clock;
    const auto start = clock::now();
    const auto cur_time = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    
    if (!pDepthMarketData || !dispatcher_) {
        if (server_) {
            server_->log_error("OnRtnDepthMarketData called with null data or dispatcher on connection " + config_.connection_id);
        }
        return;
    }
    
    std::string instrument_id = pDepthMarketData->InstrumentID;
    
    // 通过映射表查找带前缀的格式
    auto map_it = server_->noheadtohead_instruments_map_.find(instrument_id);
    std::string display_instrument = (map_it != server_->noheadtohead_instruments_map_.end()) 
        ? map_it->second : instrument_id;
    
    MarketDataStruct market_data = MarketDataServer::build_market_data_struct(pDepthMarketData, display_instrument, cur_time);
    server_->cache_market_data(instrument_id, market_data, display_instrument);
    dispatcher_->on_market_data(config_.connection_id, instrument_id, market_data, display_instrument);


    const auto end = clock::now();
    const auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();

    static std::atomic<uint64_t> total_ns{0};
    static std::atomic<uint64_t> call_count{0};

    total_ns.fetch_add(elapsed_ns, std::memory_order_relaxed);
    const auto count = call_count.fetch_add(1, std::memory_order_relaxed) + 1;

    if ((count % 50000) == 0) {                 // 任意输出周期
        const auto avg_ns = total_ns.load(std::memory_order_relaxed) / count;
        server_->log_info("OnRtnDepthMarketData avg cost: " + std::to_string(avg_ns) + " ns (" + std::to_string(count) + " calls)");
    }
}

void CTPConnection::OnRspError(CThostFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast)
{
    if (pRspInfo && pRspInfo->ErrorID != 0) {
        std::string error_msg = pRspInfo->ErrorMsg ? std::string(pRspInfo->ErrorMsg) : "Unknown error";
        server_->log_error("CTP error on connection " + config_.connection_id + ": " + error_msg);
        error_count_++;
        handle_connection_error();
    }
}

void CTPConnection::login()
{
    CThostFtdcReqUserLoginField req;
    memset(&req, 0, sizeof(req));
    
    strcpy(req.BrokerID, config_.broker_id.c_str());
    strcpy(req.UserID, "");      // 行情登录不需要用户名
    strcpy(req.Password, "");    // 行情登录不需要密码
    
    int ret = ctp_api_->ReqUserLogin(&req, ++request_id_);
    if (ret != 0) {
        server_->log_error("Failed to send login request on connection " + config_.connection_id + 
                          ", return code: " + std::to_string(ret));
        status_ = CTPConnectionStatus::ERROR;
        error_count_++;
    } else {
        server_->log_info("Login request sent on connection " + config_.connection_id);
    }
}

void CTPConnection::handle_connection_error()
{
    if (error_count_ > 10) {
        server_->log_error("Too many errors on connection " + config_.connection_id + ", marking as failed");
        status_ = CTPConnectionStatus::ERROR;
    }
}

// CTPConnectionManager 实现
CTPConnectionManager::CTPConnectionManager(MarketDataServer* server, 
                                         SubscriptionDispatcher* dispatcher)
    : server_(server)
    , dispatcher_(dispatcher)
    , health_check_running_(false)
    , health_check_interval_(30) // 30秒健康检查间隔
{
}

CTPConnectionManager::~CTPConnectionManager()
{
    stop_health_monitor();
    stop_all_connections();
}

bool CTPConnectionManager::add_connection(const CTPConnectionConfig& config)
{
    std::lock_guard<std::mutex> lock(connections_mutex_);
    
    if (connections_.find(config.connection_id) != connections_.end()) {
        server_->log_error("Connection " + config.connection_id + " already exists");
        return false;
    }
    
    auto connection = std::make_shared<CTPConnection>(config, server_, dispatcher_);
    connections_[config.connection_id] = connection;
    
    server_->log_info("Added CTP connection: " + config.connection_id + " -> " + config.front_addr);
    return true;
}

bool CTPConnectionManager::remove_connection(const std::string& connection_id)
{
    std::lock_guard<std::mutex> lock(connections_mutex_);
    
    auto it = connections_.find(connection_id);
    if (it == connections_.end()) {
        return false;
    }
    
    it->second->stop();
    connections_.erase(it);
    
    server_->log_info("Removed CTP connection: " + connection_id);
    return true;
}

bool CTPConnectionManager::start_all_connections()
{
    std::lock_guard<std::mutex> lock(connections_mutex_);
    
    bool all_started = true;
    for (auto& pair : connections_) {
        if (pair.second->get_status() == CTPConnectionStatus::DISCONNECTED) {
            if (!pair.second->start()) {
                server_->log_error("Failed to start connection: " + pair.first);
                all_started = false;
            }
        }
    }
    
    // 启动健康检查
    start_health_monitor();
    
    server_->log_info("Started " + std::to_string(connections_.size()) + " CTP connections");
    return all_started;
}

void CTPConnectionManager::stop_all_connections()
{
    stop_health_monitor();
    
    std::lock_guard<std::mutex> lock(connections_mutex_);
    
    for (auto& pair : connections_) {
        pair.second->stop();
    }
    
    server_->log_info("Stopped all CTP connections");
}

std::shared_ptr<CTPConnection> CTPConnectionManager::get_connection(const std::string& connection_id)
{
    std::lock_guard<std::mutex> lock(connections_mutex_);
    
    auto it = connections_.find(connection_id);
    return (it != connections_.end()) ? it->second : nullptr;
}

std::vector<std::shared_ptr<CTPConnection>> CTPConnectionManager::get_all_connections()
{
    std::lock_guard<std::mutex> lock(connections_mutex_);
    
    std::vector<std::shared_ptr<CTPConnection>> result;
    for (const auto& pair : connections_) {
        result.push_back(pair.second);
    }
    return result;
}

std::vector<std::shared_ptr<CTPConnection>> CTPConnectionManager::get_available_connections()
{
    std::lock_guard<std::mutex> lock(connections_mutex_);
    
    std::vector<std::shared_ptr<CTPConnection>> result;
    for (const auto& pair : connections_) {
        if (pair.second->get_status() == CTPConnectionStatus::LOGGED_IN && 
            pair.second->can_accept_more_subscriptions()) {
            result.push_back(pair.second);
        }
    }
    return result;
}

size_t CTPConnectionManager::get_total_connections() const
{
    std::lock_guard<std::mutex> lock(connections_mutex_);
    return connections_.size();
}

size_t CTPConnectionManager::get_active_connections() const
{
    std::lock_guard<std::mutex> lock(connections_mutex_);
    
    size_t active_count = 0;
    for (const auto& pair : connections_) {
        if (pair.second->get_status() == CTPConnectionStatus::LOGGED_IN) {
            active_count++;
        }
    }
    return active_count;
}

size_t CTPConnectionManager::get_total_subscriptions() const
{
    std::lock_guard<std::mutex> lock(connections_mutex_);
    
    size_t total = 0;
    for (const auto& pair : connections_) {
        total += pair.second->get_subscription_count();
    }
    return total;
}

void CTPConnectionManager::start_health_monitor()
{
    if (health_check_running_) {
        return;
    }
    
    health_check_running_ = true;
    health_check_thread_ = std::make_unique<std::thread>(&CTPConnectionManager::health_check_loop, this);
    
    server_->log_info("Started CTP connection health monitor");
}

void CTPConnectionManager::stop_health_monitor()
{
    health_check_running_ = false;
    
    if (health_check_thread_ && health_check_thread_->joinable()) {
        health_check_thread_->join();
    }
    
    health_check_thread_.reset();
    server_->log_info("Stopped CTP connection health monitor");
}

void CTPConnectionManager::health_check_loop()
{
    while (health_check_running_ && server_->is_running()) {
        try {
            std::vector<std::shared_ptr<CTPConnection>> connections_to_check;
            
            {
                std::lock_guard<std::mutex> lock(connections_mutex_);
                for (const auto& pair : connections_) {
                    connections_to_check.push_back(pair.second);
                }
            }
            
            for (const auto& conn : connections_to_check) {
                CTPConnectionStatus status = conn->get_status();
                
                // 检查连接状态
                if (status == CTPConnectionStatus::ERROR || 
                    (status == CTPConnectionStatus::DISCONNECTED && conn->get_error_count() > 5)) {
                    const std::string conn_id = conn->get_connection_id();
                    bool should_restart = false;
                    
                    // 检查是否允许重启（去重+退避）
                    {
                        std::lock_guard<std::mutex> lock(restart_mutex_);
                        const auto now = std::chrono::steady_clock::now();
                        const auto it = next_restart_allowed_.find(conn_id);
                        
                        if (it == next_restart_allowed_.end() || now >= it->second) {
                            // 允许重启：首次或已过退避期
                            next_restart_allowed_[conn_id] = now + std::chrono::seconds(10);
                            should_restart = true;
                        }
                    }
                    
                    if (should_restart) {
                        server_->log_warning("Connection " + conn_id + " is unhealthy, attempting restart");
                        // 直接在健康检查线程内重启，不创建新线程
                        conn->restart();
                    }
                }
            }
            
        } catch (const std::exception& e) {
            server_->log_error("Health check error: " + std::string(e.what()));
        }
        
        // 等待下次检查
        for (int i = 0; i < health_check_interval_.count() && health_check_running_; ++i) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }
}
