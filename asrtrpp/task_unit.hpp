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

#include "../asrtlpp/task.hpp"
#include "./reactor.hpp"

#include <ecor/ecor.hpp>

namespace asrt
{


/// Coroutine context object passed to task tests.
/// Provides query() to drive pending coroutine operations from inside exec().
struct task_test
{
        task_test( task_ctx& ctx )
          : _ctx( ctx )
        {
        }

        template < typename T >
        auto& query( T&& q )
        {
                return _ctx.query( (T&&) q );
        }

private:
        task_ctx& _ctx;
};

using ecor::suspend;
using ecor::with_error;

/// Coroutine test adaptor that wraps a definition type T into an asrt_test
/// driven by an ecor coroutine.  T must provide:
///   char const* name  — test name
///   task<void> exec() — coroutine body; co_await any asrt sender inside it
/// The coroutine runs incrementally: the reactor calls cb() on every tick
/// until exec() completes.
template < typename T >
struct task_unit : asrt_test
{

        struct recv
        {
                using receiver_concept = ecor::receiver_t;

                record* rec;

                void set_value() { rec->state = ASRT_TEST_PASS; }
                void set_error( ecor::task_error ) { rec->state = ASRT_TEST_FAIL; }
                void set_error( test_fail_t ) { rec->state = ASRT_TEST_FAIL; }
                void set_error( asrt::status ) { rec->state = ASRT_TEST_ERROR; }
                void set_stopped() { rec->state = ASRT_TEST_FAIL; }
        };

        task_unit( T def )
          : _def( std::move( def ) )
        {
                asrt_test_init( this, _def.name, static_cast< task_unit* >( this ), task_unit::cb );
        }

        static asrt_status cb( record* rec )
        {
                auto& self = *static_cast< task_unit* >( rec->inpt->test_ptr );

                if ( rec->state == ASRT_TEST_INIT ) {
                        rec->state = ASRT_TEST_RUNNING;
                        self._op   = self._def.exec().connect( recv{ rec } );
                        self._op.start();
                }

                return ASRT_SUCCESS;
        }

private:
        using task_type = decltype( std::declval< T >().exec() );

        T _def;
        // XXX: note that this being here is waste of memory - it is needed only per active test
        ecor::connect_type< task_type, recv > _op;
};

}  // namespace asrt
