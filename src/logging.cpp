#include "logging.h"

#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/sinks/text_file_backend.hpp>
#include <boost/log/sinks/async_frontend.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/support/date_time.hpp>
#include <boost/log/attributes/scoped_attribute.hpp>
#include <boost/log/attributes/current_process_id.hpp>
#include <boost/log/attributes/current_thread_id.hpp>
#include <boost/filesystem.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/make_shared.hpp>
#include <mutex>

namespace logging   = boost::log;
namespace sinks     = boost::log::sinks;
namespace expr      = boost::log::expressions;
namespace keywords  = boost::log::keywords;

namespace {

using file_backend_t = sinks::text_file_backend;
using file_sink_t    = sinks::asynchronous_sink<file_backend_t>;

boost::shared_ptr<file_sink_t> g_file_sink;
std::once_flag g_init_flag;

void do_init_logging(const std::string& log_dir) {
    if (!boost::filesystem::exists(log_dir)) {
        boost::filesystem::create_directories(log_dir);
    }

    auto backend = boost::make_shared<file_backend_t>(
        keywords::file_name = log_dir + "/market_data_%Y%m%d.log",
        keywords::open_mode = std::ios_base::app,
        keywords::time_based_rotation = sinks::file::rotation_at_time_point(0, 0, 0),
        keywords::auto_flush = true);

    g_file_sink = boost::make_shared<file_sink_t>(backend);
    g_file_sink->set_formatter(
        expr::stream
            << expr::format_date_time<boost::posix_time::ptime>("TimeStamp", "%Y-%m-%d %H:%M:%S.%f")
            << " [" << logging::trivial::severity << "] "
            << expr::smessage); 

    logging::core::get()->add_sink(g_file_sink);
    logging::add_common_attributes();
}

} // namespace

void init_logging(const std::string& log_dir) {
    std::call_once(g_init_flag, [log_dir]() {
        do_init_logging(log_dir);
    });
}

void shutdown_logging() {
    if (g_file_sink) {
        logging::core::get()->remove_sink(g_file_sink);
        g_file_sink->stop();
        g_file_sink->flush();
        g_file_sink.reset();
    }
}


