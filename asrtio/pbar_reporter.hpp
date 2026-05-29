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
#include "./pbar.hpp"
#include "./task.hpp"

#include <string>

namespace asrtio
{

/// Progress-bar reporter. Holds a reference to an externally-owned
/// terminal_progress. Construct with the active task_ctx and the bar.
struct pbar_reporter : reporter_base
{
        pbar::terminal_progress& bar;
        int                      done   = 0;
        int                      failed = 0;

        explicit pbar_reporter( task_ctx& ctx, pbar::terminal_progress& bar )
          : reporter_base( ctx )
          , bar( bar )
        {
        }

        task< void > on_count( uint32_t total ) override
        {
                bar.set_total( (int) total );
                co_return;
        }

        task< void > on_test_start( std::string_view name, uint32_t run_idx, uint32_t run_total )
            override
        {
                auto label = std::string{ name };
                if ( run_total > 1 )
                        label +=
                            " " + std::to_string( run_idx ) + "/" + std::to_string( run_total );
                bar.set_status( label );
                co_return;
        }

        task< void > on_test_done(
            std::string_view name,
            bool             passed,
            double           duration_ms,
            uint32_t         run_idx,
            uint32_t         run_total ) override
        {
                if ( !passed )
                        ++failed;
                auto label = std::string{ name };
                if ( run_total > 1 )
                        label +=
                            " " + std::to_string( run_idx ) + "/" + std::to_string( run_total );
                bar.log_result( label, passed, duration_ms );
                bar.set_progress( ++done, failed );
                co_return;
        }

        task< void > on_diagnostic( std::string_view file, uint32_t line, std::string_view extra )
            override
        {
                auto loc = std::string{ file } + ":" + std::to_string( line );
                if ( !extra.empty() )
                        loc += " " + std::string{ extra };
                bar.log( pbar::colored_wall_time() + "    " + pbar::fg( loc, pbar::colors::red ) );
                co_return;
        }

        task< void > on_collect_data( std::string_view, asrt_flat_tree const* ) override
        {
                co_return;
        }

        task< void > on_stream_data( std::string_view, asrt::stream_schemas const& ) override
        {
                co_return;
        }
};

}  // namespace asrtio
