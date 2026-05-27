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
#include <memory>

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

template < typename Op >
struct task_op_deleter
{
        ecor::task_memory_resource* mem;
        void                        operator()( Op* p ) const
        {
                p->~Op();
                mem->deallocate( p, sizeof( Op ), alignof( Op ) );
        }
};

struct task_unit_recv
{
        using receiver_concept = ecor::receiver_t;

        asrt_test_state* done;

        void set_value() { *done = ASRT_TEST_PASS; }
        void set_error( ecor::task_error ) { *done = ASRT_TEST_FAIL; }
        void set_error( test_fail_t ) { *done = ASRT_TEST_FAIL; }
        void set_error( asrt::status ) { *done = ASRT_TEST_ERROR; }
        void set_stopped() { *done = ASRT_TEST_FAIL; }
};

/// Coroutine test adaptor that wraps a definition type T into an asrt_test
/// driven by an ecor coroutine.  T must provide:
///   char const* name  — test name
///   task<void> exec() — coroutine body; co_await any asrt sender inside it
/// The coroutine runs incrementally: the reactor calls cb() on every tick
/// until exec() completes.
template < typename T >
struct task_unit : asrt_test
{


        task_unit( T def )
          : _def( std::move( def ) )
        {
                asrt_test_init( this, _def.name, static_cast< task_unit* >( this ), task_unit::cb );
        }

        task_unit( task_unit const& )            = delete;
        task_unit( task_unit&& )                 = delete;
        task_unit& operator=( task_unit const& ) = delete;
        task_unit& operator=( task_unit&& )      = delete;

        static asrt_status cb( record* rec )
        {
                auto& self = *static_cast< task_unit* >( rec->inpt->test_ptr );

                if ( rec->state == ASRT_TEST_INIT ) {
                        rec->state = ASRT_TEST_RUNNING;
                        auto& mem  = ecor::get_memory_resource( self._def );
                        void* p    = mem.allocate( sizeof( op_type ), alignof( op_type ) );
                        self._op.reset( new ( p ) op_type(
                            self._def.exec().connect( task_unit_recv{ &self._done_state } ) ) );
                        self._op->start();
                }

                if ( self._op && self._done_state != ASRT_TEST_RUNNING ) {
                        rec->state       = self._done_state;
                        self._done_state = ASRT_TEST_RUNNING;
                        self._op.reset();
                }

                return ASRT_SUCCESS;
        }

private:
        using task_type  = decltype( std::declval< T >().exec() );
        using op_type    = ecor::connect_type< task_type, task_unit_recv >;
        using op_deleter = task_op_deleter< op_type >;
        using op_ptr     = std::unique_ptr< op_type, op_deleter >;

        T               _def;
        asrt_test_state _done_state = ASRT_TEST_RUNNING;
        op_ptr          _op{ nullptr, op_deleter{ &ecor::get_memory_resource( _def ) } };
};

}  // namespace asrt
