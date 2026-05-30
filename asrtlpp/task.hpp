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

#include "../asrtl/status.h"

#include <cstdlib>
#include <ecor/ecor.hpp>

namespace asrt
{

struct malloc_free_memory_resource
{
        void* allocate( std::size_t n, std::size_t ) { return malloc( n ); }

        void deallocate( void* p, std::size_t, std::size_t ) noexcept { std::free( p ); }
};

/// Event-loop context that owns a coroutine task scheduler.
/// Call tick() once per main-loop iteration to drive all pending tasks.
struct task_ctx
{
        task_ctx( auto& mem_res )
          : _memory_resource( mem_res )
        {
        }

        void tick() { _core.run_once(); }

        auto& query( ecor::get_memory_resource_t ) { return _memory_resource; }

        auto& query( ecor::get_task_core_t ) { return _core; }

        void reschedule( ecor::schedulable& s ) { _core.reschedule( s ); }

private:
        ecor::task_memory_resource _memory_resource;
        ecor::task_core            _core;
};

using status = asrt_status;
using ecor::suspend;

/// Converts an ecor framework error to the nearest asrt status code.
/// Exhaustive switch — compiler will error if task_error gains a new enumerator.
constexpr status to_status( ecor::task_error e ) noexcept
{
        switch ( e ) {
        case ecor::task_error::none:
                return ASRT_SUCCESS;
        case ecor::task_error::task_allocation_failure:
                return ASRT_ALLOC_ERR;
        case ecor::task_error::task_unhandled_exception:
                return ASRT_UNHANDLED_EXCEPTION;
        case ecor::task_error::task_unfinished:
        case ecor::task_error::task_already_started:
        case ecor::task_error::task_missing:
                return ASRT_INTERNAL_ERR;
        }
}

/// Error signatures and trace policy used by all asrt tasks.
struct task_cfg
{
        using error_signatures = ecor::completion_signatures< ecor::set_error_t( status ) >;
        using trace_type       = ecor::task_default_trace;

        static status convert_error( ecor::task_error e ) noexcept { return to_status( e ); }
};

/// Coroutine task type used throughout asrt's C++ layer.
/// Supports asrt::status error channel (use ASRT_FAILURE for test assertion failures).
template < typename T >
using task = ecor::task< T, asrt::task_cfg >;

/// Sender that completes with success if the contained status is ASRT_SUCCESS, and with
/// ASRT_FAILURE otherwise.
struct status_sender
{
        using sender_concept = ecor::sender_t;
        using completion_signatures =
            ecor::completion_signatures< ecor::set_value_t(), ecor::set_error_t( status ) >;

        asrt_status status;

        status_sender( asrt_status s )
          : status( s )
        {
        }

        template < ecor::receiver R >
        struct op
        {
                asrt_status s;
                R           recv;

                void start()
                {
                        if ( s == ASRT_SUCCESS )
                                recv.set_value();
                        else
                                recv.set_error( s );
                }
        };

        template < ecor::receiver R >
        auto connect( R&& r ) && noexcept
        {
                return op< R >{ status, (R&&) r };
        }

        constexpr      operator asrt_status() const { return status; }
        constexpr bool operator==( asrt_status st ) const { return status == st; }
};

}  // namespace asrt
