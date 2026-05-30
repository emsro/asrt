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

        void set_value() const { *done = ASRT_TEST_PASS; }
        void set_error( asrt::status s ) const
        {
                *done = ( s == ASRT_FAILURE ) ? ASRT_TEST_FAIL : ASRT_TEST_ERROR;
        }
        void set_stopped() const { *done = ASRT_TEST_FAIL; }
};

template < typename T >
struct task_unit_base : asrt_test
{
        using asrt_test::asrt_test;
        using task_type  = T;
        using op_type    = ecor::connect_type< task_type, task_unit_recv >;
        using op_deleter = task_op_deleter< op_type >;
        using op_ptr     = std::unique_ptr< op_type, op_deleter >;

        task_unit_base( ecor::task_memory_resource& mem )
          : _op{ nullptr, op_deleter{ &mem } }
        {
        }

        asrt_status base_cb(
            record*                     rec,
            ecor::task_memory_resource& mem,
            T ( *cb )( task_unit_base* ) )
        {
                if ( rec->state == ASRT_TEST_INIT ) {
                        rec->state = ASRT_TEST_RUNNING;
                        void* p    = mem.allocate( sizeof( op_type ), alignof( op_type ) );
                        _op.reset( new ( p ) op_type(
                            cb( this ).connect( task_unit_recv{ &_done_state } ) ) );
                        _op->start();
                }

                if ( _op && _done_state != ASRT_TEST_RUNNING ) {
                        rec->state  = _done_state;
                        _done_state = ASRT_TEST_RUNNING;
                        _op.reset();
                }

                return ASRT_SUCCESS;
        }

protected:
        op_ptr          _op;
        asrt_test_state _done_state = ASRT_TEST_RUNNING;
};

template < typename T >
using task_unit_task_type = decltype( std::declval< T >().exec() );

/// Coroutine test adaptor that wraps a definition type T into an asrt_test
/// driven by an ecor coroutine.  T must provide:
///   char const* name  — test name
///   task<void> exec() — coroutine body; co_await any asrt sender inside it
/// The coroutine runs incrementally: the reactor calls cb() on every tick
/// until exec() completes.
template < typename T >
struct task_unit : task_unit_base< task_unit_task_type< T > >
{
        using base = task_unit_base< task_unit_task_type< T > >;

        task_unit( T def )
          : base( ecor::get_memory_resource( def ) )
          , _def( std::move( def ) )
        {
                asrt_test_init( this, _def.name, static_cast< task_unit* >( this ), task_unit::cb );
        }

        task_unit( task_unit const& )            = delete;
        task_unit( task_unit&& )                 = delete;
        task_unit& operator=( task_unit const& ) = delete;
        task_unit& operator=( task_unit&& )      = delete;

        static asrt_status cb( record* rec )
        {
                auto* self = static_cast< task_unit* >( rec->inpt->test_ptr );

                return self->base_cb( rec, ecor::get_memory_resource( self->_def ), []( base* b ) {
                        auto* self = static_cast< task_unit* >( b );
                        return self->_def.exec();
                } );
        }

private:
        T _def;
};

}  // namespace asrt
