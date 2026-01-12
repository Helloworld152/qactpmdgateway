# CTP 市场数据网关

基于 CTP（中金所期货行情接口）的高性能市场数据网关服务，提供实时行情数据转发和分发功能。

## 核心功能

### 1. 多CTP连接管理
- **多前置机支持**：支持同时连接多个CTP前置机，提升系统容量和可用性
- **负载均衡**：使用轮询（Round Robin）策略分配订阅请求
- **自动故障转移**：连接故障时自动迁移订阅到其他可用连接

### 2. 高性能行情分发
- **WebSocket协议**：基于Boost.Asio和Beast实现异步WebSocket服务器
- **增量数据推送**：使用结构体缓存和版本控制，仅推送变化的字段，降低网络开销
- **多客户端支持**：支持多会话并发，每个会话独立订阅管理
- **共享内存**：使用Boost.Interprocess实现进程间数据共享，支持多进程架构

### 3. 订阅管理
- **订阅分发**：使用轮询策略分配订阅请求到各个CTP连接
- **订阅去重**：同一合约的多客户端订阅自动合并，减少CTP连接压力
- **自动重试**：订阅失败自动重试，支持可配置重试次数
- **订阅状态跟踪**：完整记录订阅状态生命周期（待订阅/订阅中/已订阅/失败/已取消）

### 4. 行情数据协议
- **兼容mdservice协议**：支持`subscribe_quote`和`peek_message`标准接口
- **JSON格式数据**：行情数据以JSON格式推送，包含完整买卖盘、成交、持仓等信息
- **合约映射**：支持CTP格式合约ID到显示格式的映射（如`rb2501` ↔ `SHFE.rb2501`）

## 技术亮点

### 性能优化
- **结构体缓存**：使用`MarketDataStruct`替代JSON构建，降低CTP回调线程开销
- **分片缓存**：1024个分片缓存，减少锁竞争，提升并发性能
- **异步IO**：基于Boost.Asio的异步非阻塞IO模型，支持高并发连接
- **内存对齐**：使用`alignas(64)`优化缓存行对齐，提升CPU缓存效率

### 可靠性保障
- **健康检查**：定期检查连接健康状态，及时发现并处理异常
- **优雅关闭**：支持信号处理和优雅关闭，确保资源正确释放
- **错误处理**：完善的错误处理和日志记录机制
- **连接恢复**：支持连接断线自动重连和订阅恢复

### 可配置性
- **JSON配置**：支持通过JSON配置文件灵活配置多个CTP连接
- **命令行参数**：支持单CTP和多CTP两种运行模式

## 使用方式

### 单CTP模式（兼容性）
```bash
./open-ctp-mdgateway --front-addr tcp://182.254.243.31:30011 --broker-id 9999 --port 7799
```

### 多CTP模式（推荐）
```bash
# 使用默认SimNow配置
./open-ctp-mdgateway --multi-ctp --port 7799

# 使用自定义配置文件
./open-ctp-mdgateway --config config/multi_ctp_config.json

# 查看连接状态
./open-ctp-mdgateway --config config/multi_ctp_config.json --status
```

## 配置文件示例

```json
{
  "websocket_port": 7799,
  "auto_failover": true,
  "health_check_interval": 30,
  "connections": [
    {
      "connection_id": "simnow_main",
      "front_addr": "tcp://182.254.243.31:30011",
      "broker_id": "9999",
      "max_subscriptions": 500,
      "priority": 1,
      "enabled": true
    }
  ]
}
```

## 客户端协议

### 订阅行情
```json
{
  "aid": "subscribe_quote",
  "ins_list": "rb2501,cu2501"
}
```

### 获取行情快照
```json
{
  "aid": "peek_message"
}
```

### 行情推送格式
```json
{
  "aid": "rtn_data",
  "data": [
    {
      "quotes": {
        "SHFE.rb2501": {
          "instrument_id": "SHFE.rb2501",
          "datetime": "2024-01-15 14:30:00.500",
          "bid_price1": 3850.0,
          "bid_volume1": 100,
          "ask_price1": 3851.0,
          "ask_volume1": 50,
          "last_price": 3850.5,
          "volume": 10000,
          "open_interest": 50000
        }
      }
    },
    {
      "account_id": "",
      "ins_list": "",
      "mdhis_more_data": false
    }
  ]
}
```

## 架构特点

- **模块化设计**：连接管理、订阅分发、数据缓存等功能模块解耦
- **线程安全**：使用mutex和atomic保证多线程环境下的数据安全
- **资源管理**：RAII原则管理资源，确保异常安全
- **扩展性强**：易于添加新的功能特性

## 技术栈

- **C++17**：现代C++特性
- **Boost.Asio**：异步网络IO
- **Boost.Beast**：WebSocket实现
- **Boost.Interprocess**：进程间通信
- **RapidJSON**：JSON解析和生成
- **CTP API**：中金所期货行情接口

## 性能指标

- **支持连接数**：单个网关可管理数十个CTP前置机连接
- **订阅容量**：单个连接支持数百个合约订阅，总体支持数千个合约
- **推送延迟**：端到端延迟通常在毫秒级
- **并发客户端**：支持数百个WebSocket客户端同时连接

## 适用场景

- 量化交易系统的行情数据源
- 多账户、多策略的行情分发中心
- 需要高可用性的生产环境
- 需要横向扩展的分布式系统
