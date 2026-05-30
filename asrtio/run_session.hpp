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
#pragma once

#include "./cntr_stream_sys.hpp"
#include "./output_fs.hpp"
#include "./param_config.hpp"
#include "./task.hpp"
#include "./transport.hpp"
#include "./util.hpp"

#include <chrono>
#include <filesystem>
#include <memory>
#include <uv.h>

namespace asrtio
{

/// Each run_* coroutine accepts an externally constructed reporter.
/// The caller is responsible for constructing the reporter and managing its lifetime.

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
    suite_reporter&                 reporter );

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
    suite_reporter&                 reporter );

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
    suite_reporter&                 reporter );

}  // namespace asrtio
