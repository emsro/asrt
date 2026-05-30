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
#include "./run_session.hpp"

#include "../asrtl/log.h"
#include "./cntr_stream_sys.hpp"
#include "./euv.hpp"
#include "./rsim.hpp"

#include <memory>

namespace asrtio
{

using cntr_tcp_sys    = cntr_stream_sys< tcp_transport >;
using cntr_serial_sys = cntr_stream_sys< serial_transport >;

task< void > run_tcp(
    task_ctx&                       ctx,
    arena&                          arena,
    steady_clock&                   clk,
    uv_loop_t*                      loop,
    char const*                     host,
    uint16_t                        port,
    std::chrono::milliseconds       timeout,
    std::unique_ptr< param_config > params,
    output_fs&                      fs,
    std::filesystem::path           output_dir,
    suite_reporter&                 reporter )
{
        auto client = std::make_shared< uv_tcp_t >();
        if ( auto r = uv_tcp_init( loop, client.get() ); r != 0 ) {
                ASRT_ERR_LOG( "asrtio", "uv_tcp_init failed: %s", uv_strerror( r ) );
                co_await ecor::just_error( ASRT_INIT_ERR );
        }
        co_await tcp_connect{ { client.get(), host, port } };
        auto sys = arena.make< cntr_tcp_sys >( tcp_transport{ client }, clk );
        sys->start();

        co_await run_test_suite( ctx, *sys, reporter, timeout, *params, fs, output_dir );
}

task< void > run_rsim(
    task_ctx&                       ctx,
    arena&                          arena,
    steady_clock&                   clk,
    uv_loop_t*                      loop,
    uint32_t                        seed,
    std::chrono::milliseconds       timeout,
    std::unique_ptr< param_config > params,
    output_fs&                      fs,
    std::filesystem::path           output_dir,
    suite_reporter&                 reporter )
{
        auto rs = arena.make< rsim_ctx >( loop, seed );
        rs->start();

        auto client = std::make_shared< uv_tcp_t >();
        if ( auto r = uv_tcp_init( loop, client.get() ); r != 0 ) {
                ASRT_ERR_LOG( "asrtio", "uv_tcp_init failed: %s", uv_strerror( r ) );
                co_await ecor::just_error( ASRT_INIT_ERR );
        }
        co_await tcp_connect{ { client.get(), "0.0.0.0", rs->port() } };
        auto sys = arena.make< cntr_tcp_sys >( tcp_transport{ client }, clk );
        sys->start();

        co_await run_test_suite( ctx, *sys, reporter, timeout, *params, fs, output_dir );
        ASRT_INF_LOG( "asrtio", "run test suite finished" );
}

task< void > run_serial(
    task_ctx&                       ctx,
    arena&                          arena,
    steady_clock&                   clk,
    uv_loop_t*                      loop,
    serial_config                   cfg,
    std::chrono::milliseconds       timeout,
    std::unique_ptr< param_config > params,
    output_fs&                      fs,
    std::filesystem::path           output_dir,
    suite_reporter&                 reporter )
{
        std::string errmsg;
        auto        transport = serial_transport::open( loop, cfg, errmsg );
        if ( !transport ) {
                ASRT_ERR_LOG( "asrtio", "Failed to open serial port: %s", errmsg.c_str() );
                co_await ecor::just_error( ASRT_INIT_ERR );
                co_return;
        }
        auto sys = arena.make< cntr_serial_sys >( std::move( *transport ), clk );
        sys->start();

        co_await run_test_suite( ctx, *sys, reporter, timeout, *params, fs, output_dir );
}

}  // namespace asrtio
