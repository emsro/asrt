
/// Permission to use, copy, modify, and/or distribute this software for any
/// purpose with or without fee is hereby granted.
///
/// THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
/// REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY
/// AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT,
/// INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM
/// LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR
/// OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR
/// PERFORMANCE OF THIS SOFTWARE.
#include "../asrtl/log.h"
#include "./final_receiver.hpp"
#include "./log_sink.hpp"
#include "./real_fs.hpp"
#include "./run_session.hpp"

#include <CLI/CLI.hpp>
#include <chrono>
#include <cstdarg>
#include <cstdio>
#include <memory>
#include <sstream>

using namespace std::literals::chrono_literals;

namespace asrtio
{
using asrt::opt;
namespace
{
pbar::terminal_progress  g_bar;
pbar::terminal_progress* g_active_bar = nullptr;
asrt_log_level           g_log_level  = ASRT_LOG_ERROR;
std::ostream*            g_log_file   = nullptr;
}  // namespace

extern "C" {
void asrt_log( enum asrt_log_level level, char const* module, char const* fmt, ... )
{
        va_list args;
        va_start( args, fmt );
        log_sink_write( level, g_log_level, module, fmt, args, g_log_file, g_active_bar );
        va_end( args );
}
}

namespace
{

struct tcp_opts
{
        std::string host;
        uint16_t    port = 0;
};

}  // namespace
}  // namespace asrtio

int main( int argc, char* argv[] )
{
        using namespace asrtio;
        uv_loop_t* loop = uv_default_loop();
        std::optional< asrtio::complete_arena_connect_result< task< void >, final_receiver > > t;
        asrt::malloc_free_memory_resource mem_res;
        real_fs                           rfs;
        null_fs                           nfs;
        task_ctx                          ctx{ mem_res };
        arena                             ar{ ctx, mem_res };
        steady_clock                      clk;
        uv_idle_t                         idle;
        CLI::App                          app{ "App description" };
        argv = app.ensure_utf8( argv );

        auto opt       = std::make_shared< tcp_opts >();
        int  verbosity = 0;
        app.add_flag( "-v,--verbose", verbosity, "Verbosity: -v = info, -vv = debug" );
        app.require_subcommand( 1, 1 );

        auto* sub = app.add_subcommand( "tcp", "Connect to TCP-based system" );
        sub->fallthrough();

        sub->add_option( "-p,--port", opt->port, "Port to connect to" )->required();
        sub->add_option( "--host", opt->host, "Host to connect to" )->required();

        uint32_t    timeout_ms = 5000U;
        std::string params_file;
        std::string output_dir;
        app.add_option( "--timeout", timeout_ms, "Timeout in milliseconds" );
        app.add_option( "--params", params_file, "Path to JSON param config file" );
        app.add_option( "--output", output_dir, "Output directory for test data files" );

        auto launch = [&]( auto make_task ) {
                auto timeout = std::chrono::milliseconds{ timeout_ms };
                if ( output_dir.empty() )
                        ASRT_INF_LOG(
                            "asrtio", "No --output specified; data files will not be written" );
                auto params = std::make_unique< param_config >();
                if ( !params_file.empty() ) {
                        params = param_config_from_file( params_file );
                        if ( !params ) {
                                std::fprintf( stderr, "Failed to load param config\n" );
                                std::exit( 1 );
                        }
                }
                // Construct in-place to avoid move-constructing op<R>, which
                // derives from ecor::schedulable->ll_base<schedulable>. The
                // ll_base move ctor calls derived() (CRTP downcast) on the
                // partially-constructed target before its vptr is set, causing
                // a UBSan -fsanitize=vptr crash.
                t.emplace(
                    ar,
                    make_task( timeout, std::move( params ) ),
                    final_receiver{ &idle, &g_active_bar } );
                g_active_bar = &g_bar;
        };

        sub->callback( [&, opt] {
                launch( [&, opt]( auto timeout, auto params ) {
                        return run_tcp(
                            ctx,
                            ar,
                            clk,
                            loop,
                            opt->host.data(),
                            opt->port,
                            timeout,
                            std::move( params ),
                            output_dir.empty() ? static_cast< output_fs& >( nfs ) : rfs,
                            output_dir,
                            g_bar );
                } );
        } );

        auto* rsim      = app.add_subcommand( "rsim", "Run the test RSIM system" );
        auto  rsim_seed = std::make_shared< uint32_t >( 42U );
        rsim->add_option( "--seed", *rsim_seed, "Seed for pseudo-random test simulation" );
        rsim->fallthrough();
        rsim->callback( [&, rsim_seed] {
                launch( [&, rsim_seed]( auto timeout, auto params ) {
                        if ( params_file.empty() ) {
                                std::istringstream in( R"({
                                        "*": {"default": 1},
                                        "demo_param_value": [{"val": 10}, {"val": 20}],
                                        "demo_param_count": {"a": 1, "b": 2, "c": 3}
                                })" );
                                params = param_config_from_stream( in );
                        }
                        return run_rsim(
                            ctx,
                            ar,
                            clk,
                            loop,
                            *rsim_seed,
                            timeout,
                            std::move( params ),
                            output_dir.empty() ? static_cast< output_fs& >( nfs ) : rfs,
                            output_dir,
                            g_bar );
                } );
        } );

        auto  ser_cfg = std::make_shared< serial_config >();
        auto* ser_sub = app.add_subcommand( "serial", "Connect to serial port device" );
        ser_sub->fallthrough();
        ser_sub->add_option( "--port,-d", ser_cfg->path, "Serial device path (e.g. /dev/ttyUSB0)" )
            ->required();
        ser_sub->add_option( "--baud,-b", ser_cfg->baud, "Baud rate (default: 115200)" );
        ser_sub->add_option( "--parity", ser_cfg->parity, "Parity" )
            ->transform( CLI::CheckedTransformer(
                std::map< std::string, serial_config::parity_t >{
                    { "none", serial_config::parity_t::none },
                    { "odd", serial_config::parity_t::odd },
                    { "even", serial_config::parity_t::even } },
                CLI::ignore_case ) );
        ser_sub->add_option( "--stop-bits", ser_cfg->stop, "Stop bits" )
            ->transform( CLI::CheckedTransformer(
                std::map< std::string, serial_config::stop_bits_t >{
                    { "1", serial_config::stop_bits_t::one },
                    { "2", serial_config::stop_bits_t::two } },
                CLI::ignore_case ) );
        ser_sub->add_option( "--flow", ser_cfg->flow, "Flow control" )
            ->transform( CLI::CheckedTransformer(
                std::map< std::string, serial_config::flow_t >{
                    { "none", serial_config::flow_t::none },
                    { "rtscts", serial_config::flow_t::rtscts },
                    { "xonxoff", serial_config::flow_t::xonxoff } },
                CLI::ignore_case ) );
        ser_sub->callback( [&, ser_cfg] {
                launch( [&, ser_cfg]( auto timeout, auto params ) {
                        return run_serial(
                            ctx,
                            ar,
                            clk,
                            loop,
                            *ser_cfg,
                            timeout,
                            std::move( params ),
                            output_dir.empty() ? static_cast< output_fs& >( nfs ) : rfs,
                            output_dir,
                            g_bar );
                } );
        } );

        CLI11_PARSE( app, argc, argv );

        if ( verbosity >= 2 )
                g_log_level = ASRT_LOG_DEBUG;
        else if ( verbosity == 1 )
                g_log_level = ASRT_LOG_INFO;
        else
                g_log_level = ASRT_LOG_ERROR;

        std::optional< file_writer > log_writer;
        if ( !output_dir.empty() ) {
                rfs.create_directories( output_dir );
                log_writer.emplace(
                    rfs.open_write( std::filesystem::path{ output_dir } / "asrtio.log" ) );
                g_log_file = &log_writer->stream();
        }

        idle.data = &ctx;
        uv_idle_init( loop, &idle );
        uv_idle_start( &idle, []( uv_idle_t* handle ) {
                auto& ctx = *static_cast< task_ctx* >( handle->data );
                ctx.tick();
        } );

        if ( t )
                t->start();

        uv_run( loop, UV_RUN_DEFAULT );
        uv_loop_close( loop );

        g_log_file = nullptr;
        log_writer.reset();

        return 0;
}
