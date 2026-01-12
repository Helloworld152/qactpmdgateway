#pragma once

#include <vector>
#include <string>
#include <map>

// CTP连接配置
struct CTPConnectionConfig {
    std::string front_addr;
    std::string broker_id;
    std::string connection_id;
    int max_subscriptions = 500;  // 每个连接最大订阅数
    int priority = 1;             // 连接优先级（1-10，数字越小优先级越高）
    bool enabled = true;          // 是否启用此连接
};

// 多CTP连接配置
struct MultiCTPConfig {
    // 全局配置
    int websocket_port = 7799;
    
    // 连接配置列表
    std::vector<CTPConnectionConfig> connections;
    
    // 高级配置
    int health_check_interval = 30;     // 健康检查间隔(秒)
    int maintenance_interval = 60;      // 维护间隔(秒)  
    int max_retry_count = 3;           // 最大重试次数
    bool auto_failover = true;         // 是否开启自动故障转移
};

// 配置加载器
class ConfigLoader {
public:
    static bool load_from_file(const std::string& config_file, MultiCTPConfig& config);
    static bool load_from_json(const std::string& json_content, MultiCTPConfig& config);
    static MultiCTPConfig create_default_config();
    static bool validate_config(const MultiCTPConfig& config);
    
private:
    static void setup_default_connections(MultiCTPConfig& config);
};

// 默认配置
inline MultiCTPConfig create_simnow_config() {
    MultiCTPConfig config;
    config.websocket_port = 7799;
    
    // SimNow环境的多个前置机
    CTPConnectionConfig conn1;
    conn1.connection_id = "simnow_telecom";
    conn1.front_addr = "tcp://180.168.146.187:10210";
    conn1.broker_id = "9999";
    conn1.max_subscriptions = 500;
    conn1.priority = 1;
    conn1.enabled = true;
    
    CTPConnectionConfig conn2;
    conn2.connection_id = "simnow_unicom";
    conn2.front_addr = "tcp://180.168.146.187:10211";
    conn2.broker_id = "9999";
    conn2.max_subscriptions = 500;
    conn2.priority = 2;
    conn2.enabled = true;
    
    CTPConnectionConfig conn3;
    conn3.connection_id = "simnow_mobile";
    conn3.front_addr = "tcp://218.202.237.33:10212";
    conn3.broker_id = "9999";
    conn3.max_subscriptions = 500;
    conn3.priority = 3;
    conn3.enabled = true;
    
    config.connections = {conn1, conn2, conn3};
    config.auto_failover = true;
    
    return config;
}