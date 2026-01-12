#include "subscription_dispatcher.h"
#include "ctp_connection_manager.h"
#include "market_data_server.h"
#include <algorithm>

SubscriptionDispatcher::SubscriptionDispatcher(MarketDataServer* server)
    : server_(server)
    , connection_manager_(nullptr)
    , round_robin_counter_(0)
    , maintenance_running_(false)
    , maintenance_interval_(60) // 60秒维护间隔
    , max_retry_count_(3)
{
}

SubscriptionDispatcher::~SubscriptionDispatcher()
{
    shutdown();
}

bool SubscriptionDispatcher::initialize(CTPConnectionManager* connection_manager, const MultiCTPConfig& config)
{
    if (!connection_manager) {
        server_->log_error("CTPConnectionManager is null");
        return false;
    }
    
    connection_manager_ = connection_manager;
    
    // 从配置中读取参数
    maintenance_interval_ = std::chrono::seconds(config.maintenance_interval);
    max_retry_count_ = config.max_retry_count;
    
    // 启动维护定时器
    start_maintenance_timer();
    
    server_->log_info("SubscriptionDispatcher initialized successfully with config:");
    server_->log_info("  - Maintenance interval: " + std::to_string(config.maintenance_interval) + " seconds");
    server_->log_info("  - Max retry count: " + std::to_string(max_retry_count_));
    server_->log_info("  - Auto failover: " + std::string(config.auto_failover ? "enabled" : "disabled"));
    
    return true;
}

void SubscriptionDispatcher::shutdown()
{
    stop_maintenance_timer();
    
    std::lock_guard<std::mutex> sub_lock(subscriptions_mutex_);
    std::lock_guard<std::mutex> sess_lock(sessions_mutex_);
    std::lock_guard<std::mutex> conn_lock(connections_mutex_);
    
    global_subscriptions_.clear();
    session_subscriptions_.clear();
    connection_subscriptions_.clear();
    
    server_->log_info("SubscriptionDispatcher shutdown completed");
}

bool SubscriptionDispatcher::add_subscription(const std::string& session_id, const std::string& instrument_id)
{
    std::lock_guard<std::mutex> sub_lock(subscriptions_mutex_);
    std::lock_guard<std::mutex> sess_lock(sessions_mutex_);
    
    // 检查是否已经存在全局订阅
    auto global_it = global_subscriptions_.find(instrument_id);
    if (global_it != global_subscriptions_.end()) {
        // 添加session到现有订阅
        global_it->second->requesting_sessions.insert(session_id);
        session_subscriptions_[session_id].insert(instrument_id);
        
        server_->log_info("Added session " + session_id + " to existing subscription: " + instrument_id);
        return true;
    }
    
    // 创建新的订阅
    auto subscription_info = std::make_shared<SubscriptionInfo>(instrument_id);
    subscription_info->requesting_sessions.insert(session_id);
    global_subscriptions_[instrument_id] = subscription_info;
    session_subscriptions_[session_id].insert(instrument_id);
    
    // 使用轮询策略选择连接
    std::shared_ptr<CTPConnection> best_connection = select_connection_round_robin();
    if (!best_connection) {
        server_->log_error("No available connection for subscription: " + instrument_id);
        subscription_info->status = SubscriptionStatus::FAILED;
        // 加入重试队列
        if (subscription_info->retry_count < max_retry_count_) {
            std::lock_guard<std::mutex> retry_lock(retry_set_mutex_);
            retry_set_.insert(instrument_id);
        }
        return false;
    }
    
    subscription_info->assigned_connection_id = best_connection->get_connection_id();
    subscription_info->status = SubscriptionStatus::SUBSCRIBING;
    
    // 执行订阅
    bool result = execute_subscription(instrument_id, best_connection->get_connection_id());
    if (!result) {
        subscription_info->status = SubscriptionStatus::FAILED;
    }
    
    server_->log_info("Added new subscription: " + instrument_id + " on connection " + 
                     best_connection->get_connection_id());
    return result;
}

bool SubscriptionDispatcher::remove_subscription(const std::string& session_id, const std::string& instrument_id)
{
    std::lock_guard<std::mutex> sub_lock(subscriptions_mutex_);
    std::lock_guard<std::mutex> sess_lock(sessions_mutex_);
    
    // 从session订阅中移除
    auto sess_it = session_subscriptions_.find(session_id);
    if (sess_it != session_subscriptions_.end()) {
        sess_it->second.erase(instrument_id);
        if (sess_it->second.empty()) {
            session_subscriptions_.erase(sess_it);
        }
    }
    
    // 检查全局订阅
    auto global_it = global_subscriptions_.find(instrument_id);
    if (global_it == global_subscriptions_.end()) {
        return true; // 订阅不存在
    }
    
    // 从请求session列表中移除
    global_it->second->requesting_sessions.erase(session_id);
    
    // 如果没有session再需要此订阅，则取消CTP订阅
    if (global_it->second->requesting_sessions.empty()) {
        std::string connection_id = global_it->second->assigned_connection_id;
        
        if (execute_unsubscription(instrument_id, connection_id)) {
            server_->log_info("Removed subscription: " + instrument_id + " from connection " + connection_id);
        }
        
        global_subscriptions_.erase(global_it);
    } else {
        server_->log_info("Kept subscription " + instrument_id + " (still needed by " + 
                         std::to_string(global_it->second->requesting_sessions.size()) + " sessions)");
    }
    
    return true;
}

void SubscriptionDispatcher::remove_all_subscriptions_for_session(const std::string& session_id)
{
    std::vector<std::string> instruments_to_remove;
    
    {
        std::lock_guard<std::mutex> sess_lock(sessions_mutex_);
        auto sess_it = session_subscriptions_.find(session_id);
        if (sess_it != session_subscriptions_.end()) {
            instruments_to_remove.assign(sess_it->second.begin(), sess_it->second.end());
        }
    }
    
    for (const auto& instrument_id : instruments_to_remove) {
        remove_subscription(session_id, instrument_id);
    }
    
    server_->log_info("Removed all subscriptions for session: " + session_id);
}

std::vector<std::string> SubscriptionDispatcher::get_subscriptions_for_session(const std::string& session_id)
{
    std::lock_guard<std::mutex> lock(sessions_mutex_);
    
    auto it = session_subscriptions_.find(session_id);
    if (it != session_subscriptions_.end()) {
        return std::vector<std::string>(it->second.begin(), it->second.end());
    }
    
    return {};
}

std::vector<std::string> SubscriptionDispatcher::get_sessions_for_instrument(const std::string& instrument_id)
{
    std::lock_guard<std::mutex> lock(subscriptions_mutex_);
    
    auto it = global_subscriptions_.find(instrument_id);
    if (it != global_subscriptions_.end()) {
        return std::vector<std::string>(it->second->requesting_sessions.begin(), 
                                       it->second->requesting_sessions.end());
    }
    
    return {};
}

SubscriptionStatus SubscriptionDispatcher::get_subscription_status(const std::string& instrument_id)
{
    std::lock_guard<std::mutex> lock(subscriptions_mutex_);
    
    auto it = global_subscriptions_.find(instrument_id);
    if (it != global_subscriptions_.end()) {
        return it->second->status;
    }
    
    return SubscriptionStatus::CANCELLED;
}

size_t SubscriptionDispatcher::get_total_subscriptions() const
{
    std::lock_guard<std::mutex> lock(subscriptions_mutex_);
    return global_subscriptions_.size();
}

std::shared_ptr<CTPConnection> SubscriptionDispatcher::select_connection_round_robin()
{
    auto available_connections = connection_manager_->get_available_connections();
    if (available_connections.empty()) {
        return nullptr;
    }
    
    size_t index = round_robin_counter_++ % available_connections.size();
    return available_connections[index];
}

void SubscriptionDispatcher::handle_connection_failure(const std::string& connection_id)
{
    std::lock_guard<std::mutex> sub_lock(subscriptions_mutex_);
    std::lock_guard<std::mutex> conn_lock(connections_mutex_);
    
    server_->log_warning("Handling connection failure: " + connection_id);
    
    // 找出所有使用失败连接的订阅
    std::vector<std::string> affected_instruments;
    
    for (const auto& pair : global_subscriptions_) {
        if (pair.second->assigned_connection_id == connection_id && 
            pair.second->status == SubscriptionStatus::ACTIVE) {
            affected_instruments.push_back(pair.first);
            pair.second->status = SubscriptionStatus::FAILED;
        }
    }
    
    // 将受影响的订阅迁移到其他连接
    for (const auto& instrument_id : affected_instruments) {
        std::shared_ptr<CTPConnection> new_connection = select_connection_round_robin();
        if (new_connection && new_connection->get_connection_id() != connection_id) {
            migrate_subscription(instrument_id, connection_id, new_connection->get_connection_id());
        } else {
            server_->log_error("No available connection to migrate subscription: " + instrument_id);
            // 找不到可用连接，加入重试队列
            auto it = global_subscriptions_.find(instrument_id);
            if (it != global_subscriptions_.end() && it->second->retry_count < max_retry_count_) {
                std::lock_guard<std::mutex> retry_lock(retry_set_mutex_);
                retry_set_.insert(instrument_id);
            }
        }
    }
    
    // 清理连接相关的订阅记录
    connection_subscriptions_.erase(connection_id);
    
    server_->log_info("Connection failure handling completed for: " + connection_id);
}

void SubscriptionDispatcher::handle_connection_recovery(const std::string& connection_id)
{
    server_->log_info("Connection recovered: " + connection_id);
    
    // 处理待重试的订阅
    process_pending_subscriptions();
}

void SubscriptionDispatcher::migrate_subscription(const std::string& instrument_id, 
                                                const std::string& from_connection_id, 
                                                const std::string& to_connection_id)
{
    server_->log_info("Migrating subscription " + instrument_id + " from " + 
                     from_connection_id + " to " + to_connection_id);
    
    auto it = global_subscriptions_.find(instrument_id);
    if (it == global_subscriptions_.end()) {
        return;
    }
    
    // 更新订阅信息
    it->second->assigned_connection_id = to_connection_id;
    it->second->status = SubscriptionStatus::SUBSCRIBING;
    it->second->retry_count = 0;
    
    // 执行新订阅
    if (execute_subscription(instrument_id, to_connection_id)) {
        server_->log_info("Successfully migrated subscription: " + instrument_id);
    } else {
        server_->log_error("Failed to migrate subscription: " + instrument_id);
        it->second->status = SubscriptionStatus::FAILED;
        
        // 转移失败，加入重试队列
        if (it->second->retry_count < max_retry_count_) {
            std::lock_guard<std::mutex> retry_lock(retry_set_mutex_);
            retry_set_.insert(instrument_id);
        }
    }
}

void SubscriptionDispatcher::on_subscription_success(const std::string& connection_id, const std::string& instrument_id)
{
    std::lock_guard<std::mutex> sub_lock(subscriptions_mutex_);
    std::lock_guard<std::mutex> conn_lock(connections_mutex_);
    
    auto it = global_subscriptions_.find(instrument_id);
    if (it != global_subscriptions_.end()) {
        it->second->status = SubscriptionStatus::ACTIVE;
        it->second->last_update_time = std::chrono::system_clock::now();
        
        connection_subscriptions_[connection_id].insert(instrument_id);
        
        server_->log_info("Subscription successful: " + instrument_id + " on " + connection_id);
    }
}

void SubscriptionDispatcher::on_subscription_failed(const std::string& connection_id, const std::string& instrument_id)
{
    std::lock_guard<std::mutex> sub_lock(subscriptions_mutex_);
    
    auto it = global_subscriptions_.find(instrument_id);
    if (it != global_subscriptions_.end()) {
        it->second->status = SubscriptionStatus::FAILED;
        it->second->retry_count++;
        it->second->last_update_time = std::chrono::system_clock::now();
        
        
        // 如果重试次数未超限，加入重试队列
        if (it->second->retry_count < max_retry_count_) {
            std::lock_guard<std::mutex> retry_lock(retry_set_mutex_);
            retry_set_.insert(instrument_id);
        }
        
        server_->log_error("Subscription failed: " + instrument_id + " on " + connection_id + 
                          " (retry: " + std::to_string(it->second->retry_count) + ")");
    }
}

void SubscriptionDispatcher::on_unsubscription_success(const std::string& connection_id, const std::string& instrument_id)
{
    std::lock_guard<std::mutex> conn_lock(connections_mutex_);
    
    auto it = connection_subscriptions_.find(connection_id);
    if (it != connection_subscriptions_.end()) {
        it->second.erase(instrument_id);
        if (it->second.empty()) {
            connection_subscriptions_.erase(it);
        }
    }
    
    server_->log_info("Unsubscription successful: " + instrument_id + " on " + connection_id);
}

void SubscriptionDispatcher::on_market_data(const std::string& connection_id, 
                                          const std::string& instrument_id, 
                                          const MarketDataStruct& market_data,
                                          const std::string& display_instrument)
{
    // 行情数据已经在ctp_connection_manager中使用结构体缓存
    // 通知线性合约管理器组件行情更新（使用结构体传递）
    if (server_) {
        server_->on_component_update(instrument_id, market_data);
    }
}

bool SubscriptionDispatcher::execute_subscription(const std::string& instrument_id, const std::string& connection_id)
{
    if (!connection_manager_) {
        return false;
    }
    
    auto connection = connection_manager_->get_connection(connection_id);
    if (!connection) {
        server_->log_error("Connection not found: " + connection_id);
        return false;
    }
    
    return connection->subscribe_instrument(instrument_id);
}

bool SubscriptionDispatcher::execute_unsubscription(const std::string& instrument_id, const std::string& connection_id)
{
    if (!connection_manager_) {
        return false;
    }
    
    auto connection = connection_manager_->get_connection(connection_id);
    if (!connection) {
        return true; // 连接不存在，认为取消订阅成功
    }
    
    return connection->unsubscribe_instrument(instrument_id);
}

void SubscriptionDispatcher::process_pending_subscriptions()
{
    std::set<std::string> current_retry_set;
    
    {
        std::lock_guard<std::mutex> retry_lock(retry_set_mutex_);
        current_retry_set = retry_set_;
        retry_set_.clear(); // 清空重试队列
    }
    
    std::set<std::string> failed_again;
    for (const auto& instrument_id : current_retry_set) {
        std::lock_guard<std::mutex> sub_lock(subscriptions_mutex_);
        auto it = global_subscriptions_.find(instrument_id);
        if (it != global_subscriptions_.end() && it->second->status == SubscriptionStatus::FAILED) {
            // 使用轮询策略选择新连接重试
            std::shared_ptr<CTPConnection> new_connection = select_connection_round_robin();
            if (new_connection) {
                it->second->assigned_connection_id = new_connection->get_connection_id();
                it->second->status = SubscriptionStatus::SUBSCRIBING;
                
                if (!execute_subscription(instrument_id, new_connection->get_connection_id())) {
                    it->second->status = SubscriptionStatus::FAILED;
                    if (it->second->retry_count < max_retry_count_) {
                        failed_again.insert(instrument_id);
                    }
                } else {
                    // 重试成功，移除重试计数
                    it->second->retry_count = 0;
                }
            } else {
                // 找不到可用连接，加入重试队列
                if (it->second->retry_count < max_retry_count_) {
                    failed_again.insert(instrument_id);
                }
            }
        }
    }

    if (!failed_again.empty()) {
        std::lock_guard<std::mutex> retry_lock(retry_set_mutex_);
        retry_set_.insert(failed_again.begin(), failed_again.end());
    }
}

void SubscriptionDispatcher::start_maintenance_timer()
{
    if (maintenance_running_) {
        return;
    }
    
    maintenance_running_ = true;
    maintenance_thread_ = std::make_unique<std::thread>(&SubscriptionDispatcher::maintenance_task, this);
    
    server_->log_info("Started subscription maintenance timer");
}

void SubscriptionDispatcher::stop_maintenance_timer()
{
    maintenance_running_ = false;
    
    if (maintenance_thread_ && maintenance_thread_->joinable()) {
        maintenance_thread_->join();
    }
    
    maintenance_thread_.reset();
    server_->log_info("Stopped subscription maintenance timer");
}

void SubscriptionDispatcher::maintenance_task()
{
    while (maintenance_running_) {
        try {
            // 处理待重试的订阅
            process_pending_subscriptions();
            
            // 清理过期订阅
            cleanup_expired_subscriptions();
            
        } catch (const std::exception& e) {
            server_->log_error("Maintenance task error: " + std::string(e.what()));
        }
        
        // 等待下次维护
        for (int i = 0; i < maintenance_interval_.count() && maintenance_running_; ++i) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }
}

void SubscriptionDispatcher::cleanup_expired_subscriptions()
{
    std::lock_guard<std::mutex> sub_lock(subscriptions_mutex_);
    
    auto now = std::chrono::system_clock::now();
    std::vector<std::string> to_remove;
    
    for (const auto& pair : global_subscriptions_) {
        // 清理长时间失败的订阅
        if (pair.second->status == SubscriptionStatus::FAILED) {
            auto time_since_failure = now - pair.second->last_update_time;
            if (time_since_failure > std::chrono::minutes(10)) { // 10分钟
                to_remove.push_back(pair.first);
            }
        }
    }
    
    for (const auto& instrument_id : to_remove) {
        global_subscriptions_.erase(instrument_id);
        server_->log_info("Cleaned up expired subscription: " + instrument_id);
    }
}