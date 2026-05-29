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

#include "../asrtl/log.h"
#include "../asrtl/status_to_str.h"
#include "./pbar.hpp"
#include "./task.hpp"

#include <uv.h>

namespace asrtio
{

/// ecor receiver that drives a uv_idle_t event loop and clears the active
/// progress-bar pointer on task completion.
struct final_receiver
{
        using receiver_concept = ecor::receiver_t;

        uv_idle_t*                idle       = nullptr;
        pbar::terminal_progress** active_bar = nullptr;

        void set_value()
        {
                ASRT_INF_LOG( "asrtio_main", "Task completed successfully" );
                clear_bar();
                stop_idle();
        }

        void set_error( ecor::task_error )
        {
                ASRT_ERR_LOG( "asrtio_main", "Task error" );
                clear_bar();
                stop_idle();
        }

        void set_error( asrt::status s )
        {
                ASRT_ERR_LOG( "asrtio_main", "Task error: %s", asrt_status_to_str( s ) );
                clear_bar();
                stop_idle();
        }

        void set_stopped()
        {
                ASRT_INF_LOG( "asrtio_main", "Task stopped" );
                clear_bar();
                stop_idle();
        }

private:
        void clear_bar()
        {
                if ( active_bar )
                        *active_bar = nullptr;
        }

        void stop_idle()
        {
                if ( idle ) {
                        uv_idle_stop( idle );
                        uv_close( (uv_handle_t*) idle, nullptr );
                        idle = nullptr;
                }
        }
};

}  // namespace asrtio
