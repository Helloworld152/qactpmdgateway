#pragma once

#include <boost/log/trivial.hpp>
#include <string>

// 初始化 Boost.Log，log_dir 默认为 "logs"
void init_logging(const std::string& log_dir = "logs");

// 可选：退出时调用，刷新并移除日志 sink
void shutdown_logging();


