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

#include "../asrtc/cntr_assm.h"
#include "../asrtc/controller.h"
#include "../asrtcpp/collect.hpp"
#include "../asrtcpp/controller.hpp"
#include "../asrtcpp/diag.hpp"
#include "../asrtcpp/param.hpp"
#include "../asrtcpp/stream.hpp"
#include "../asrtl/log.h"
#include "../asrtl/status_to_str.h"
#include "../asrtlpp/fmt.hpp"
#include "../asrtlpp/util.hpp"
#include "./euv.hpp"
#include "./output_fs.hpp"
#include "./param_config.hpp"
#include "./task.hpp"
#include "./transport.hpp"
#include "./util.hpp"

#include <chrono>
#include <cstring>
#include <optional>
#include <set>
#include <uv.h>

namespace asrtio
{

struct cntr_sys
{
        virtual asrt_diag_record*     take_diag_record() = 0;
        virtual clock const&          clk() const        = 0;
        virtual asrt_controller&      cntr()             = 0;
        virtual asrt_cntr_assm&       assembly()         = 0;
        virtual asrt_flat_tree const* collect_tree()     = 0;
        virtual asrt::stream_schemas  stream_take()      = 0;
        virtual ~cntr_sys()                              = default;
};

template < typename Transport >
struct cntr_stream_sys : cntr_sys
{
        cntr_stream_sys( Transport transport, clock const& clk )
          : _transport( std::move( transport ) )
          , _clk( clk )
        {
                auto st = asrt_cntr_assm_init( &_asm, asrt_default_allocator() );
                ASRT_ASSERT( st == ASRT_SUCCESS );
                if ( st != ASRT_SUCCESS )
                        ASRT_ERR_LOG(
                            "cntr_stream_sys",
                            "Failed to initialize controller assembly: %s",
                            asrt_status_to_str( st ) );
        }

        asrt_diag_record* take_diag_record() override
        {
                return asrt_diag_server_take_record( &_asm.diag );
        }

        clock const& clk() const override { return _clk; }

        void tick()
        {
                auto now = static_cast< uint32_t >( _clk.now().count() );
                asrt_cntr_assm_tick( &_asm, now );

                while ( auto* req = asrt_send_req_list_next( &_asm.send_queue ) ) {
                        if ( _disconnected ) {
                                asrt_send_req_list_done( &_asm.send_queue, ASRT_SEND_ERR );
                                continue;
                        }
                        auto st = _rx.write( _transport.stream(), req->chid, req->buff );
                        asrt_send_req_list_done( &_asm.send_queue, st );
                }
        }

        void start()
        {
                auto* loop = _transport.stream()->loop;
                uv_idle_init( loop, &_idle_handle );
                _idle_handle.data = this;
                uv_idle_start( &_idle_handle, []( uv_idle_t* h ) {
                        static_cast< cntr_stream_sys* >( h->data )->tick();
                } );
                _rx.start(
                    _transport.stream(), &_asm.cntr.node, "asrtio_cntr", [this]( ssize_t nread ) {
                            if ( nread == UV_EOF )
                                    ASRT_DBG_LOG( "asrtio_main", "Connection closed by remote" );
                            else
                                    ASRT_ERR_LOG(
                                        "asrtio_main",
                                        "Read error: %s",
                                        uv_strerror( static_cast< int >( nread ) ) );
                            ASRT_INF_LOG(
                                "asrtio_main", "Stopping cntr_stream_sys and closing connection" );
                            disconnect();
                    } );
        }

        void disconnect()
        {
                if ( _disconnected )
                        return;
                _disconnected = true;
                _transport.close();
        }

        asrt_controller& cntr() override { return _asm.cntr; }

        asrt::stream_schemas stream_take() override
        {
                return asrt::stream_schemas{ asrt_stream_server_take( &_asm.stream ) };
        }

        asrt_flat_tree const* collect_tree() override
        {
                return asrt_collect_server_tree( &_asm.collect );
        }

        asrt_cntr_assm& assembly() override { return _asm; }

        template < typename T >
        friend task< void > async_destroy( task_ctx&, cntr_stream_sys< T >& );

private:
        bool         _disconnected = false;
        uv_idle_t    _idle_handle;
        Transport    _transport;
        clock const& _clk;
        asrt_cntr_assm _asm;
        cobs_node _rx;
};

template < typename T >
inline task< void > async_destroy( task_ctx&, cntr_stream_sys< T >& sys )
{
        uv_idle_stop( &sys._idle_handle );
        asrt_cntr_assm_deinit( &sys._asm );
        co_await uv_close_handle{ (uv_handle_t*) &sys._idle_handle };
        if ( !sys._disconnected )
                co_await uv_close_handle{ (uv_handle_t*) sys._transport.stream() };
}

struct suite_reporter
{
        virtual task< void > on_count( uint32_t total ) = 0;
        virtual task< void > on_test_start(
            std::string_view name,
            uint32_t         run_idx,
            uint32_t         run_total ) = 0;
        virtual task< void > on_test_done(
            std::string_view name,
            bool             passed,
            double           duration_ms,
            uint32_t         run_idx,
            uint32_t         run_total ) = 0;
        virtual task< void > on_diagnostic(
            std::string_view file,
            uint32_t         line,
            std::string_view extra ) = 0;
        virtual task< void > on_collect_data(
            std::string_view      name,
            asrt_flat_tree const* tree ) = 0;
        virtual task< void > on_stream_data(
            std::string_view            name,
            asrt::stream_schemas const& schemas ) = 0;
        virtual ~suite_reporter()                 = default;
};

struct reporter_base : suite_reporter
{
        explicit reporter_base( task_ctx& ctx )
          : _ctx( ctx )
        {
        }

        auto& query( ecor::get_memory_resource_t tag ) { return _ctx.query( tag ); }
        auto& query( ecor::get_task_core_t tag ) { return _ctx.query( tag ); }

protected:
        task_ctx& _ctx;
};

struct _cntr_assembly_exec_test
{
        using completion_signatures = ecor::completion_signatures<
            ecor::set_value_t( asrt::result ),
            ecor::set_error_t( asrt::status ) >;

        asrt_cntr_assm&           asm_ref;
        uint16_t                  tid;
        asrt_flat_tree const*     tree;
        asrt_flat_id              root_id;
        std::chrono::milliseconds timeout;

        template < typename OP >
        void start( OP& op )
        {
                auto s = asrt_cntr_assm_exec_test(
                    &asm_ref,
                    tree,
                    root_id,
                    tid,
                    static_cast< uint32_t >( timeout.count() ),
                    +[]( void* p, asrt_status s, asrt_result* res ) -> asrt_status {
                            auto* op = static_cast< OP* >( p );
                            if ( s != ASRT_SUCCESS ) {
                                    ASRT_ERR_LOG(
                                        "asrtio_main",
                                        "Assembly exec_test failed: %s",
                                        asrt_status_to_str( s ) );
                                    op->receiver.set_error( s );
                                    return ASRT_SUCCESS;
                            }
                            op->receiver.set_value( *res );
                            return ASRT_SUCCESS;
                    },
                    &op );
                if ( s != ASRT_SUCCESS ) {
                        ASRT_ERR_LOG(
                            "asrtio_main",
                            "Assembly exec_test failed: %s",
                            asrt_status_to_str( s ) );
                        op.receiver.set_error( s );
                }
        }
};
using cntr_assembly_exec_test = ecor::sender_from< _cntr_assembly_exec_test >;

void write_strm_field( std::ostream& os, enum asrt_strm_field_type_e ft, uint8_t*& p );
void write_strm_field( std::ostream& os, struct asrt_stream_field_desc const* desc, uint8_t*& p );

void write_stream_csv(
    output_fs&                   fs,
    std::filesystem::path const& path,
    asrt_stream_schema const&    sc );

task< void > handle_stream(
    task_ctx&                    ctx,
    asrt::stream_schemas         schemas,
    suite_reporter&              reporter,
    std::string_view             name,
    output_fs&                   fs,
    std::filesystem::path const& run_dir,
    bool                         do_output );

task< void > handle_collect(
    task_ctx&                    ctx,
    asrt_flat_tree const*        tree,
    suite_reporter&              reporter,
    std::string_view             name,
    output_fs&                   fs,
    std::filesystem::path const& path,
    bool                         do_output );

task< void > handle_diag(
    task_ctx&                    ctx,
    cntr_sys&                    sys,
    suite_reporter&              reporter,
    output_fs&                   fs,
    std::filesystem::path const& path,
    bool                         do_output );

task< void > run_test_suite(
    task_ctx&                 ctx,
    cntr_sys&                 sys,
    suite_reporter&           reporter,
    std::chrono::milliseconds timeout,
    param_config const&       params,
    output_fs&                fs,
    std::filesystem::path     output_dir );

}  // namespace asrtio
