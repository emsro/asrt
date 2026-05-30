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
#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN

#include "../asrtc/cntr_assm.h"
#include "../asrtc/result.h"
#include "../asrtcpp/cntr_assm.hpp"
#include "../asrtcpp/collect.hpp"
#include "../asrtcpp/controller.hpp"
#include "../asrtcpp/diag.hpp"
#include "../asrtcpp/param.hpp"
#include "../asrtl/collect_proto.h"
#include "../asrtl/log.h"
#include "../asrtlpp/callback.hpp"
#include "../asrtlpp/fmt.hpp"
#include "../asrtlpp/task.hpp"
#include "../asrtlpp/util.hpp"
#include "../asrtrpp/diag.hpp"
#include "../asrtrpp/reac_assm.hpp"
#include "../asrtrpp/reactor.hpp"
#include "./collector.hpp"
#include "./util.hpp"

#include <doctest/doctest.h>
#include <format>
#include <string>
#include <vector>

static ASRT_DEFINE_GPOS_LOG()

    namespace
{
}

// ---------------------------------------------------------------------------
// helpers

/// Drain all requests from sq, find the matching node in peer_head's chain by chid,
/// and deliver each message directly via asrt_chann_recv.
static void deliver( asrt_send_req_list* sq, asrt_node* peer_head )
{
        while ( sq->head ) {
                asrt_send_req* req = sq->head;
                sq->head           = req->next;
                if ( !sq->head )
                        sq->tail = nullptr;
                req->next = nullptr;

                uint8_t  flat[512];
                uint8_t* p = flat;
                memcpy( p, req->buff.b, (size_t) ( req->buff.e - req->buff.b ) );
                p += req->buff.e - req->buff.b;
                for ( asrt_rec_span const* s = req->buff.next; s != nullptr; s = s->next ) {
                        size_t sz = (size_t) ( s->e - s->b );
                        memcpy( p, s->b, sz );
                        p += sz;
                }

                asrt_node*       target = asrt_chann_find( peer_head, req->chid );
                enum asrt_status st =
                    target ? asrt_chann_recv( target, { flat, p } ) : ASRT_CHANN_NOT_FOUND;
                if ( req->done_cb )
                        req->done_cb( req->done_ptr, st );
        }
}

// ---------------------------------------------------------------------------
// test callables for the reactor side

namespace
{

// Receiver that maps coroutine completions to asrt_test_state.
struct task_result_receiver
{
        using receiver_concept = ecor::receiver_t;
        asrt_test_state* out;

        void set_value() const { *out = ASRT_TEST_PASS; }
        void set_error( asrt::status s ) const
        {
                *out = ( s == ASRT_FAILURE ) ? ASRT_TEST_FAIL : ASRT_TEST_ERROR;
        }
        void set_stopped() const { *out = ASRT_TEST_FAIL; }
        auto get_env() const noexcept { return ecor::empty_env{}; }
};

// Connect and drive a coroutine task to completion.
// make_task(task_ctx&) returns the task; pump() is called once per iteration
// before tctx.tick().
template < typename MakeTask, typename Pump >
asrt_test_state run_until_done(
    asrt::task_ctx& tctx,
    MakeTask&&      make_task,
    Pump&&          pump,
    int             max_iters = 200 )
{
        asrt_test_state result = ASRT_TEST_INIT;
        auto            op     = make_task( tctx ).connect( task_result_receiver{ &result } );
        op.start();
        for ( int i = 0; i < max_iters && result == ASRT_TEST_INIT; i++ ) {
                tctx.tick();
                pump();
        }
        return result;
}

// Mixin that adds coroutine infrastructure (memory resource + task context).
struct coroutine_support
{
        asrt::malloc_free_memory_resource mem;
        asrt::task_ctx                    tctx{ mem };
};

struct passing_test
{
        char const* name() const { return "passing_test"; }
        asrt_status operator()( asrt::record& rec )
        {
                rec.state = ASRT_TEST_PASS;
                return ASRT_SUCCESS;
        }
};

struct failing_test
{
        char const* name() const { return "failing_test"; }
        asrt_status operator()( asrt::record& rec )
        {
                rec.state = ASRT_TEST_FAIL;
                return ASRT_SUCCESS;
        }
};

// ---------------------------------------------------------------------------
// paired fixture: controller <-> reactor wired in-process

struct paired_ctx : coroutine_support
{
        int          init_cb_count = 0;
        asrt::status init_status   = ASRT_SUCCESS;
        uint32_t     t             = 1;

        asrt::unit< passing_test > t0;
        asrt::unit< failing_test > t1;

        // Outgoing send queues — must be declared before r/c so they are valid when init runs.
        asrt_send_req_list r_send_queue = {};
        asrt_send_req_list c_send_queue = {};

        asrt_reactor    r;
        asrt_controller c;

        paired_ctx()
        {
                if ( asrt::init( r, r_send_queue, "paired_reactor" ) != ASRT_SUCCESS )
                        throw std::runtime_error( "reactor init failed" );

                REQUIRE( asrt::add_test( r, t0 ) == ASRT_SUCCESS );
                REQUIRE( asrt::add_test( r, t1 ) == ASRT_SUCCESS );

                if ( asrt::init( c, &c_send_queue, asrt_default_allocator() ) != ASRT_SUCCESS )
                        throw std::runtime_error( "controller init failed" );

                // start init handshake
                REQUIRE(
                    asrt::start(
                        c,
                        { []( void* self, asrt::status s ) -> asrt::status {
                                 auto* p = static_cast< paired_ctx* >( self );
                                 p->init_cb_count++;
                                 p->init_status = s;
                                 return ASRT_SUCCESS;
                         },
                          this },
                        1000 ) == ASRT_SUCCESS );

                // complete PROTO_VERSION handshake
                for ( int i = 0; i < 100 && !asrt::is_idle( c ); i++ ) {
                        (void) asrt::tick( asrt::node( c ), t++ );  // tick status checked
                                                                    // implicitly via is_idle
                        deliver( &c_send_queue, &r.node );
                        (void) asrt::tick( asrt::node( r ), t++ );
                        deliver( &r_send_queue, &c.node );
                }
                CHECK( asrt::is_idle( c ) );
        }

        ~paired_ctx()
        {
                asrt::deinit( c );
                asrt::deinit( r );
        }

        // tick both sides until controller is idle again
        void spin()
        {
                for ( int i = 0; i < 100 && !asrt::is_idle( c ); i++ ) {
                        (void) asrt::tick( asrt::node( c ), t++ );  // tick status checked
                                                                    // implicitly via is_idle
                        deliver( &c_send_queue, &r.node );
                        (void) asrt::tick( asrt::node( r ), t++ );
                        deliver( &r_send_queue, &c.node );
                }
                CHECK( asrt::is_idle( c ) );
        }

        template < typename F >
        asrt_test_state run_task( F&& make_task )
        {
                return run_until_done( tctx, std::forward< F >( make_task ), [&] {
                        (void) asrt::tick( asrt::node( c ), t++ );
                        deliver( &c_send_queue, &r.node );
                        (void) asrt::tick( asrt::node( r ), t++ );
                        deliver( &r_send_queue, &c.node );
                } );
        }
};

}  // namespace

// ---------------------------------------------------------------------------
// fmt

TEST_CASE( "fmt_success" )
{
        std::string s = std::format( "{}", ASRT_SUCCESS );
        CHECK_EQ( s, asrt_status_to_str( ASRT_SUCCESS ) );
}

TEST_CASE( "fmt_error" )
{
        std::string s = std::format( "{}", ASRT_INIT_ERR );
        CHECK_EQ( s, asrt_status_to_str( ASRT_INIT_ERR ) );
}

// ---------------------------------------------------------------------------
// controller init

TEST_CASE_FIXTURE( paired_ctx, "controller_init" )
{
        CHECK( asrt::is_idle( c ) );
        CHECK_EQ( ASRT_CORE, asrt::node( c ).chid );
        CHECK_EQ( ASRT_SUCCESS, init_status );
}

TEST_CASE_FIXTURE( paired_ctx, "init_cb_fires_once" )
{
        // cimpl_do clears init_cb after first call; verify it was called exactly once
        CHECK_EQ( 1, init_cb_count );
}

// ---------------------------------------------------------------------------
// query_desc

TEST_CASE_FIXTURE( paired_ctx, "query_desc" )
{
        std::string  received;
        asrt::status st = asrt::query_desc(
            c,
            { []( void* self, asrt::status s, char const* desc ) -> asrt::status {
                     auto* p = static_cast< std::string* >( self );
                     CHECK_EQ( ASRT_SUCCESS, s );
                     *p = std::string{ desc };
                     return ASRT_SUCCESS;
             },
              &received },
            1000 );
        CHECK_EQ( ASRT_SUCCESS, st );
        spin();
        CHECK_EQ( "paired_reactor", received );
}

// ---------------------------------------------------------------------------
// query_test_count

TEST_CASE_FIXTURE( paired_ctx, "query_test_count" )
{
        // C callback uses uint16_t; store widened value in uint32_t for assertions.
        uint32_t     count = 0;
        asrt::status st    = asrt::query_test_count(
            c,
            { []( void* p, asrt::status s, uint16_t n ) -> asrt::status {
                     CHECK_EQ( ASRT_SUCCESS, s );
                     auto* count_ptr = static_cast< uint32_t* >( p );
                     *count_ptr      = n;
                     return ASRT_SUCCESS;
             },
                 &count },
            1000 );
        CHECK_EQ( ASRT_SUCCESS, st );
        spin();
        CHECK_EQ( 2U, count );
}

// ---------------------------------------------------------------------------
// query_test_info

TEST_CASE_FIXTURE( paired_ctx, "query_test_info" )
{
        uint16_t    tid = 0xFFFF;
        std::string name;
        struct ctx
        {
                uint16_t*    tid;
                std::string* name;
        } cb_ctx = { &tid, &name };

        asrt::status st = asrt::query_test_info(
            c,
            0,
            { []( void* p, asrt::status s, uint16_t t, char const* desc ) -> asrt::status {
                     auto* ctx = static_cast< struct ctx* >( p );
                     CHECK_EQ( ASRT_SUCCESS, s );
                     *ctx->tid  = t;
                     *ctx->name = std::string{ desc };
                     return ASRT_SUCCESS;
             },
              &cb_ctx },
            1000 );
        CHECK_EQ( ASRT_SUCCESS, st );
        spin();
        CHECK_EQ( 0U, tid );
        CHECK_EQ( "passing_test", name );
}

// ---------------------------------------------------------------------------
// exec_test

TEST_CASE_FIXTURE( paired_ctx, "exec_test_pass" )
{
        asrt_test_result res = ASRT_TEST_RESULT_ERROR;
        asrt::status     st  = asrt::exec_test(
            c,
            0,
            { []( void* p, asrt::status s, asrt_result* r ) -> asrt::status {
                     auto* out = static_cast< asrt_test_result* >( p );
                     CHECK_EQ( ASRT_SUCCESS, s );
                     *out = r->res;
                     return ASRT_SUCCESS;
             },
                   &res },
            1000 );
        CHECK_EQ( ASRT_SUCCESS, st );
        spin();
        CHECK_EQ( ASRT_TEST_RESULT_SUCCESS, res );
}

TEST_CASE_FIXTURE( paired_ctx, "exec_test_fail" )
{
        asrt_test_result res = ASRT_TEST_RESULT_ERROR;
        asrt::status     st  = asrt::exec_test(
            c,
            1,
            { []( void* p, asrt::status s, asrt_result* r ) -> asrt::status {
                     auto* out = static_cast< asrt_test_result* >( p );
                     CHECK_EQ( ASRT_SUCCESS, s );
                     *out = r->res;
                     return ASRT_SUCCESS;
             },
                   &res },
            1000 );
        CHECK_EQ( ASRT_SUCCESS, st );
        spin();
        CHECK_EQ( ASRT_TEST_RESULT_FAILURE, res );
}

// ---------------------------------------------------------------------------
// busy_error: second query while controller is busy must fail and NOT store its callback

TEST_CASE_FIXTURE( paired_ctx, "busy_error" )
{
        bool first_called  = false;
        bool second_called = false;

        // start the first query — succeeds, callback stored
        CHECK_EQ(
            ASRT_SUCCESS,
            asrt::query_desc(
                c,
                { []( void* p, asrt::status, char const* ) -> asrt::status {
                         auto* called = static_cast< bool* >( p );
                         *called      = true;
                         return ASRT_SUCCESS;
                 },
                  &first_called },
                1000 ) );

        // controller is now busy — second query must be rejected; its callback must NOT be stored
        CHECK_EQ(
            ASRT_BUSY_ERR,
            asrt::query_test_count(
                c,
                { []( void* p, asrt::status, uint16_t ) -> asrt::status {
                         auto* called = static_cast< bool* >( p );
                         *called      = true;
                         return ASRT_SUCCESS;
                 },
                  &second_called },
                1000 ) );

        // complete the first query
        spin();

        CHECK( first_called );
        CHECK_FALSE( second_called );
}

// ---------------------------------------------------------------------------
// controller + diagnostics
//
// Extends paired_ctx with an diag on the reactor side and an
// diag on the controller side, wired bidirectionally.
//
// The in-process paired_ctx routes messages directly via recv_cb, bypassing
// channel dispatch.  Each diag sender delivers straight to the peer's
// recv_cb.  Construction order: send lambdas first (capture this, only called
// at runtime), then the diag objects that consume them.

namespace
{

struct diag_paired_ctx : paired_ctx
{
        asrt_diag_server c_diag;
        asrt_diag_client r_diag;

        diag_paired_ctx()
        {
                if ( asrt::init( c_diag, asrt::node( this->c ), asrt_default_allocator() ) !=
                     ASRT_SUCCESS )
                        throw std::runtime_error( "c_diag init failed" );
                if ( asrt::init( r_diag, asrt::node( this->r ) ) != ASRT_SUCCESS )
                        throw std::runtime_error( "r_diag init failed" );
        }

        ~diag_paired_ctx()
        {
                asrt::deinit( r_diag );
                asrt::deinit( c_diag );
        }
};

}  // namespace

TEST_CASE_FIXTURE( diag_paired_ctx, "diag_record_received_by_controller" )
{
        auto noop_done = []( asrt_status ) {};
        asrt::rec_diag( r_diag, "test_file.c", 42, nullptr, noop_done );
        deliver( &r_send_queue, &c.node );

        auto rec = asrt::take_record( c_diag );
        REQUIRE( rec != nullptr );
        CHECK_EQ( 42U, rec->line );
        CHECK( std::string_view{ rec->file }.ends_with( "test_file.c" ) );

        // no more records
        CHECK( asrt::take_record( c_diag ) == nullptr );
}

TEST_CASE_FIXTURE( diag_paired_ctx, "diag_multiple_records_queued_in_order" )
{
        auto noop_done = []( asrt_status ) {};
        asrt::rec_diag( r_diag, "a.c", 1, nullptr, noop_done );
        asrt::rec_diag( r_diag, "b.c", 2, nullptr, noop_done );
        asrt::rec_diag( r_diag, "c.c", 3, nullptr, noop_done );
        deliver( &r_send_queue, &c.node );

        auto r1 = asrt::take_record( c_diag );
        auto r2 = asrt::take_record( c_diag );
        REQUIRE( r1 );
        REQUIRE( !r2 );

        CHECK_EQ( 1U, r1->line );

        CHECK( asrt::take_record( c_diag ) == nullptr );
}

TEST_CASE_FIXTURE( diag_paired_ctx, "diag_independent_of_controller_queries" )
{
        // both a controller query and a diag record in flight at the same time
        std::string desc;
        auto        on_desc = [&]( asrt::status s, char const* sv ) -> asrt::status {
                CHECK_EQ( ASRT_SUCCESS, s );
                desc = std::string{ sv };
                return ASRT_SUCCESS;
        };
        CHECK_EQ( ASRT_SUCCESS, asrt::query_desc( c, on_desc, 1000 ) );

        auto noop_done = []( asrt_status ) {};
        asrt::rec_diag( r_diag, "diag_test.c", 99, nullptr, noop_done );

        spin();

        CHECK_EQ( "paired_reactor", desc );

        auto rec = asrt::take_record( c_diag );
        REQUIRE( rec != nullptr );
        CHECK_EQ( 99U, rec->line );
}

// ---------------------------------------------------------------------------
// param_server wrapper

namespace
{

struct param_server_ctx : paired_ctx
{
        collector         param_coll;
        asrt_param_server srv;

        param_server_ctx()
        {
                if ( asrt::init( srv, asrt::node( c ), asrt_default_allocator() ) != ASRT_SUCCESS )
                        throw std::runtime_error( "param_server init failed" );
        }

        ~param_server_ctx() { asrt::deinit( srv ); }

        void drain_param() { drain_send_queue( &c_send_queue, &param_coll ); }
};

}  // namespace

TEST_CASE_FIXTURE( param_server_ctx, "param_server_node_chained" )
{
        CHECK_EQ( ASRT_PARA, asrt::node( srv ).chid );
}

TEST_CASE_FIXTURE( param_server_ctx, "param_server_set_tree_and_send_ready" )
{
        struct asrt_flat_tree tree;
        asrt_flat_tree_init( &tree, asrt_default_allocator(), 4, 16 );
        asrt_flat_tree_append_cont( &tree, 0, 1, nullptr, ASRT_FLAT_CTYPE_OBJECT );
        asrt_flat_tree_append_scalar( &tree, 1, 2, "k", ASRT_FLAT_STYPE_U32, { .u32_val = 42 } );

        asrt::set_tree( srv, tree );
        auto noop = []( asrt_status ) {};
        CHECK_EQ( ASRT_SUCCESS, asrt::send_ready( srv, 1U, noop, 1000 ) );
        drain_param();

        REQUIRE_EQ( 1U, param_coll.data.size() );
        CHECK_EQ( ASRT_PARA, param_coll.data.front().id );
        CHECK_EQ( ASRT_PARAM_MSG_READY, param_coll.data.front().data[0] );
        param_coll.data.pop_front();

        asrt_flat_tree_deinit( &tree );
}

TEST_CASE_FIXTURE( param_server_ctx, "param_server_ready_ack_cb_fires" )
{
        struct asrt_flat_tree tree;
        asrt_flat_tree_init( &tree, asrt_default_allocator(), 4, 16 );
        asrt_flat_tree_append_cont( &tree, 0, 1, nullptr, ASRT_FLAT_CTYPE_OBJECT );

        asrt::set_tree( srv, tree );

        int  ack_count = 0;
        auto on_ack    = [&]( asrt_status ) {
                ++ack_count;
        };
        CHECK_EQ( ASRT_SUCCESS, asrt::send_ready( srv, 1U, on_ack, 1000 ) );
        drain_param();
        param_coll.data.clear();

        // Build a READY_ACK message: [ASRT_PARAM_MSG_READY_ACK, max_msg_size(256) LE]
        uint8_t   ack_msg[5] = { ASRT_PARAM_MSG_READY_ACK, 0, 1, 0, 0 };  // 256 LE
        asrt_span sp         = { .b = ack_msg, .e = ack_msg + sizeof ack_msg };

        auto& n = asrt::node( srv );
        CHECK_EQ( ASRT_SUCCESS, asrt::recv( n, sp ) );

        CHECK_EQ( 0, ack_count );  // not yet — pending, needs tick
        CHECK_EQ( ASRT_SUCCESS, asrt::tick( asrt::node( srv ), t++ ) );
        CHECK_EQ( 1, ack_count );  // fired once

        // Second tick should not fire again
        CHECK_EQ( ASRT_SUCCESS, asrt::tick( asrt::node( srv ), t++ ) );
        CHECK_EQ( 1, ack_count );

        asrt_flat_tree_deinit( &tree );
}

// ---------------------------------------------------------------------------
// collect_server wrapper
// ---------------------------------------------------------------------------

static inline asrt_status inject_csrv_msg( asrt_node* n, uint8_t* b, uint8_t* e )
{
        return asrt_chann_recv( n, ( asrt_span ){ .b = b, .e = e } );
}

static uint8_t* build_csrv_ready_ack( uint8_t* buf )
{
        buf[0] = ASRT_COLLECT_MSG_READY_ACK;
        return buf + 1;
}

static uint8_t* build_csrv_append(
    uint8_t*                      buf,
    asrt::flat_id                 parent_id,
    asrt::flat_id                 node_id,
    char const*                   key,
    struct asrt_flat_value const* value )
{
        struct asrt_collect_append_msg amsg = {};
        struct asrt_send_req*          req =
            asrt_msg_rtoc_collect_append( &amsg, parent_id, node_id, key, value );
        uint8_t* p = buf;
        memcpy( p, req->buff.b, (size_t) ( req->buff.e - req->buff.b ) );
        p += req->buff.e - req->buff.b;
        for ( asrt_rec_span const* s = req->buff.next; s != nullptr; s = s->next ) {
                size_t sz = (size_t) ( s->e - s->b );
                memcpy( p, s->b, sz );
                p += sz;
        }
        return p;
}

namespace
{

struct collect_server_ctx
{
        collector           coll;
        asrt_send_req_list  sq = {};
        asrt_node           head{};
        asrt_collect_server srv;
        uint32_t            t = 1;

        collect_server_ctx()
        {
                ASRT_INF_LOG( "asrtcpp_test", "collect_server_ctx init" );
                head.send_queue = &sq;
                if ( asrt::init( srv, head, asrt_default_allocator(), 4, 16 ) != ASRT_SUCCESS )
                        throw std::runtime_error( "collect_server init failed" );
        }

        ~collect_server_ctx()
        {
                ASRT_INF_LOG( "asrtcpp_test", "collect_server_ctx deinit" );
                asrt::deinit( srv );
        }

        void drain() { drain_send_queue( &sq, &coll ); }

        void make_active( asrt::flat_id root_id = 0 )
        {
                auto noop = []( asrt_status ) {};
                CHECK_EQ( ASRT_SUCCESS, asrt::send_ready( srv, root_id, noop, 1000 ) );
                drain();
                coll.data.clear();
                uint8_t buf[16];
                CHECK_EQ(
                    ASRT_SUCCESS,
                    inject_csrv_msg( &asrt::node( srv ), buf, build_csrv_ready_ack( buf ) ) );
                CHECK_EQ( ASRT_SUCCESS, asrt::tick( asrt::node( srv ), t++ ) );
                coll.data.clear();
        }
};

}  // namespace

TEST_CASE_FIXTURE( collect_server_ctx, "collect_server_node_chained" )
{
        CHECK_EQ( ASRT_COLL, asrt::node( srv ).chid );
}

TEST_CASE_FIXTURE( collect_server_ctx, "collect_server_next_node_id_starts_at_1" )
{
        CHECK_EQ( 1U, asrt::next_node_id( srv ) );
}

TEST_CASE_FIXTURE( collect_server_ctx, "collect_server_send_ready_encodes" )
{
        auto noop = []( asrt_status ) {};
        CHECK_EQ( ASRT_SUCCESS, asrt::send_ready( srv, 5U, noop, 1000 ) );
        drain();

        REQUIRE_EQ( 1U, coll.data.size() );
        auto& msg = coll.data.front();
        CHECK_EQ( ASRT_COLL, msg.id );
        CHECK_EQ( ASRT_COLLECT_MSG_READY, msg.data[0] );
        // root_id = 5 at offset 1..4 (LE)
        assert_u32( 5U, msg.data.data() + 1 );
        // next_node_id = 1 at offset 5..8 (LE)
        assert_u32( 1U, msg.data.data() + 5 );
        CHECK_EQ( 9U, msg.data.size() );
}

TEST_CASE_FIXTURE( collect_server_ctx, "collect_server_ready_ack_fires_cb" )
{
        int  ack_count = 0;
        auto on_ack    = [&]( asrt_status ) {
                ++ack_count;
        };
        CHECK_EQ( ASRT_SUCCESS, asrt::send_ready( srv, 1U, on_ack, 1000 ) );
        drain();
        coll.data.clear();

        uint8_t buf[16];
        CHECK_EQ(
            ASRT_SUCCESS, inject_csrv_msg( &asrt::node( srv ), buf, build_csrv_ready_ack( buf ) ) );

        CHECK_EQ( 0, ack_count );
        CHECK_EQ( ASRT_SUCCESS, asrt::tick( asrt::node( srv ), t++ ) );
        CHECK_EQ( 1, ack_count );

        // second tick must not re-fire
        CHECK_EQ( ASRT_SUCCESS, asrt::tick( asrt::node( srv ), t++ ) );
        CHECK_EQ( 1, ack_count );
}

TEST_CASE_FIXTURE( collect_server_ctx, "collect_server_append_builds_tree" )
{
        make_active();

        // Append: parent=0, node_id=1, key="root", type=OBJECT
        asrt_flat_value obj_val = { .type = ASRT_FLAT_CTYPE_OBJECT };
        uint8_t         buf[256];
        CHECK_EQ(
            ASRT_SUCCESS,
            inject_csrv_msg(
                &asrt::node( srv ), buf, build_csrv_append( buf, 0, 1, "root", &obj_val ) ) );
        CHECK_EQ( ASRT_SUCCESS, asrt::tick( asrt::node( srv ), t++ ) );

        // Append: parent=1, node_id=2, key="val", type=U32, value=42
        asrt_flat_value u32_val = { .type = ASRT_FLAT_STYPE_U32 };
        u32_val.data.s.u32_val  = 42;
        CHECK_EQ(
            ASRT_SUCCESS,
            inject_csrv_msg(
                &asrt::node( srv ), buf, build_csrv_append( buf, 1, 2, "val", &u32_val ) ) );
        CHECK_EQ( ASRT_SUCCESS, asrt::tick( asrt::node( srv ), t++ ) );

        // Verify tree
        auto const& tree = asrt::tree( srv );

        asrt_flat_query_result res = {};
        CHECK_EQ( ASRT_SUCCESS, asrt_flat_tree_query( &tree, 1, &res ) );
        CHECK_EQ( ASRT_FLAT_CTYPE_OBJECT, res.value.type );
        REQUIRE_NE( nullptr, res.key );
        CHECK_EQ( std::string_view{ "root" }, std::string_view{ res.key } );

        CHECK_EQ( ASRT_SUCCESS, asrt_flat_tree_query( &tree, 2, &res ) );
        CHECK_EQ( ASRT_FLAT_STYPE_U32, res.value.type );
        CHECK_EQ( 42U, res.value.data.s.u32_val );
        REQUIRE_NE( nullptr, res.key );
        CHECK_EQ( std::string_view{ "val" }, std::string_view{ res.key } );
}

TEST_CASE_FIXTURE( collect_server_ctx, "collect_server_append_string_value" )
{
        make_active();

        asrt_flat_value str_val = { .type = ASRT_FLAT_STYPE_STR };
        str_val.data.s.str_val  = "hello";
        uint8_t buf[256];
        CHECK_EQ(
            ASRT_SUCCESS,
            inject_csrv_msg(
                &asrt::node( srv ), buf, build_csrv_append( buf, 0, 1, "greeting", &str_val ) ) );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &asrt::node( srv ), t++ ) );

        auto const& tree = asrt::tree( srv );

        asrt_flat_query_result res = {};
        CHECK_EQ( ASRT_SUCCESS, asrt_flat_tree_query( &tree, 1, &res ) );
        CHECK_EQ( ASRT_FLAT_STYPE_STR, res.value.type );
        CHECK_EQ( std::string_view{ "hello" }, std::string_view{ res.value.data.s.str_val } );
}

TEST_CASE_FIXTURE( collect_server_ctx, "collect_server_duplicate_append_sends_error" )
{
        make_active();

        asrt_flat_value u32_val = { .type = ASRT_FLAT_STYPE_U32 };
        u32_val.data.s.u32_val  = 1;
        uint8_t buf[256];
        CHECK_EQ(
            ASRT_SUCCESS,
            inject_csrv_msg(
                &asrt::node( srv ), buf, build_csrv_append( buf, 0, 1, "a", &u32_val ) ) );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &asrt::node( srv ), t++ ) );
        drain();
        coll.data.clear();

        // duplicate node_id=1
        CHECK_EQ(
            ASRT_SUCCESS,
            inject_csrv_msg(
                &asrt::node( srv ), buf, build_csrv_append( buf, 0, 1, "b", &u32_val ) ) );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &asrt::node( srv ), t++ ) );
        drain();

        // server should send an ERROR back
        REQUIRE_GE( coll.data.size(), 1U );
        CHECK_EQ( ASRT_COLL, coll.data.back().id );
        CHECK_EQ( ASRT_COLLECT_MSG_ERROR, coll.data.back().data[0] );
}

// ---------------------------------------------------------------------------
// Sender-based controller API
//
// Each test co_awaits a sender variant and verifies the completion value.
// Free functions are used because ecor coroutines require task_ctx& as their
// first argument (lambdas that are coroutines are not supported by ecor).

static asrt::task< void > cntr_sender_start( asrt::task_ctx&, asrt_controller& c, uint32_t timeout )
{
        co_await asrt::start( c, timeout );
}

static asrt::task< void > cntr_sender_query_desc(
    asrt::task_ctx&,
    asrt_controller& c,
    std::string&     out,
    uint32_t         timeout )
{
        out = co_await asrt::query_desc( c, timeout );
}

static asrt::task< void > cntr_sender_query_test_count(
    asrt::task_ctx&,
    asrt_controller& c,
    uint16_t&        out,
    uint32_t         timeout )
{
        out = co_await asrt::query_test_count( c, timeout );
}

static asrt::task< void > cntr_sender_query_test_info(
    asrt::task_ctx&,
    asrt_controller& c,
    uint16_t         id,
    uint16_t&        out_tid,
    std::string&     out_name,
    uint32_t         timeout )
{
        auto [tid, name] = co_await asrt::query_test_info( c, id, timeout );
        out_tid          = tid;
        out_name         = name;
}

static asrt::task< void > cntr_sender_exec_test(
    asrt::task_ctx&,
    asrt_controller&  c,
    uint16_t          id,
    asrt_test_result& out,
    uint32_t          timeout )
{
        asrt_result res = co_await asrt::exec_test( c, id, timeout );
        out             = res.res;
}

// cntr_start_sender: the constructor already calls start() via callback variant;
// call it again on a fresh controller to exercise the sender path.
TEST_CASE( "cntr_start_sender" )
{
        coroutine_support cs;

        asrt::unit< passing_test > t0;
        asrt_send_req_list         r_sq = {};
        asrt_send_req_list         c_sq = {};
        asrt_reactor               r;
        asrt_controller            c;
        uint32_t                   t = 1;

        REQUIRE( asrt::init( r, r_sq, "sender_reactor" ) == ASRT_SUCCESS );
        REQUIRE( asrt::add_test( r, t0 ) == ASRT_SUCCESS );
        REQUIRE( asrt::init( c, &c_sq, asrt_default_allocator() ) == ASRT_SUCCESS );

        auto state = run_until_done(
            cs.tctx,
            [&]( asrt::task_ctx& ctx ) {
                    return cntr_sender_start( ctx, c, 1000 );
            },
            [&] {
                    (void) asrt::tick( asrt::node( c ), t++ );
                    deliver( &c_sq, &r.node );
                    (void) asrt::tick( asrt::node( r ), t++ );
                    deliver( &r_sq, &c.node );
            } );

        CHECK_EQ( ASRT_TEST_PASS, state );

        asrt::deinit( c );
        asrt::deinit( r );
}

TEST_CASE_FIXTURE( paired_ctx, "cntr_query_desc_sender" )
{
        std::string desc;
        auto        state = run_task( [&]( asrt::task_ctx& ctx ) {
                return cntr_sender_query_desc( ctx, c, desc, 1000 );
        } );
        CHECK_EQ( ASRT_TEST_PASS, state );
        CHECK_EQ( "paired_reactor", desc );
}

TEST_CASE_FIXTURE( paired_ctx, "cntr_query_test_count_sender" )
{
        uint16_t count = 0;
        auto     state = run_task( [&]( asrt::task_ctx& ctx ) {
                return cntr_sender_query_test_count( ctx, c, count, 1000 );
        } );
        CHECK_EQ( ASRT_TEST_PASS, state );
        CHECK_EQ( 2U, count );
}

TEST_CASE_FIXTURE( paired_ctx, "cntr_query_test_info_sender_id0" )
{
        uint16_t    tid = 0xffff;
        std::string name;
        auto        state = run_task( [&]( asrt::task_ctx& ctx ) {
                return cntr_sender_query_test_info( ctx, c, 0, tid, name, 1000 );
        } );
        CHECK_EQ( ASRT_TEST_PASS, state );
        CHECK_EQ( 0U, tid );
        CHECK_EQ( "passing_test", name );
}

TEST_CASE_FIXTURE( paired_ctx, "cntr_query_test_info_sender_id1" )
{
        uint16_t    tid = 0xffff;
        std::string name;
        auto        state = run_task( [&]( asrt::task_ctx& ctx ) {
                return cntr_sender_query_test_info( ctx, c, 1, tid, name, 1000 );
        } );
        CHECK_EQ( ASRT_TEST_PASS, state );
        CHECK_EQ( 1U, tid );
        CHECK_EQ( "failing_test", name );
}

TEST_CASE_FIXTURE( paired_ctx, "cntr_exec_test_sender_pass" )
{
        asrt_test_result res   = ASRT_TEST_RESULT_ERROR;
        auto             state = run_task( [&]( asrt::task_ctx& ctx ) {
                return cntr_sender_exec_test( ctx, c, 0, res, 1000 );
        } );
        CHECK_EQ( ASRT_TEST_PASS, state );
        CHECK_EQ( ASRT_TEST_RESULT_SUCCESS, res );
}

TEST_CASE_FIXTURE( paired_ctx, "cntr_exec_test_sender_fail" )
{
        asrt_test_result res   = ASRT_TEST_RESULT_ERROR;
        auto             state = run_task( [&]( asrt::task_ctx& ctx ) {
                return cntr_sender_exec_test( ctx, c, 1, res, 1000 );
        } );
        CHECK_EQ( ASRT_TEST_PASS, state );
        CHECK_EQ( ASRT_TEST_RESULT_FAILURE, res );
}

// ---------------------------------------------------------------------------
// collect_send_ready_sender and param_send_ready_sender

static asrt::task< void > do_collect_send_ready(
    asrt::task_ctx&,
    asrt_collect_server& s,
    uint32_t             timeout )
{
        ASRT_INF_LOG( "asrtcpp_test", "Applying send ready" );
        co_await asrt::send_ready( s, 1U, timeout );
}

static asrt::task< void > do_param_send_ready(
    asrt::task_ctx&,
    asrt_param_server& s,
    uint32_t           timeout )
{
        co_await asrt::send_ready( s, 1U, timeout );
}

TEST_CASE( "collect_send_ready_sender" )
{
        collect_server_ctx ctx;
        coroutine_support  cs;
        auto               state = run_until_done(
            cs.tctx,
            [&]( asrt::task_ctx& tc ) {
                    return do_collect_send_ready( tc, ctx.srv, 1000 );
            },
            [&] {
                    ctx.drain();
                    bool const had_ready = !ctx.coll.data.empty();
                    ctx.coll.data.clear();
                    if ( had_ready ) {
                            uint8_t buf[16];
                            CHECK_EQ(
                                ASRT_SUCCESS,
                                inject_csrv_msg(
                                    &asrt::node( ctx.srv ), buf, build_csrv_ready_ack( buf ) ) );
                    }
                    CHECK_EQ( ASRT_SUCCESS, asrt::tick( asrt::node( ctx.srv ), ctx.t++ ) );
            },
            50 );
        CHECK_EQ( ASRT_TEST_PASS, state );
}

TEST_CASE( "param_send_ready_sender" )
{
        param_server_ctx      ctx;
        struct asrt_flat_tree tree;
        asrt_flat_tree_init( &tree, asrt_default_allocator(), 4, 16 );
        asrt_flat_tree_append_cont( &tree, 0, 1, nullptr, ASRT_FLAT_CTYPE_OBJECT );
        asrt::set_tree( ctx.srv, tree );

        auto state = run_until_done(
            ctx.tctx,
            [&]( asrt::task_ctx& tc ) {
                    return do_param_send_ready( tc, ctx.srv, 1000 );
            },
            [&] {
                    ctx.drain_param();
                    bool const had_ready = !ctx.param_coll.data.empty();
                    ctx.param_coll.data.clear();
                    if ( had_ready ) {
                            uint8_t   ack_msg[5] = { ASRT_PARAM_MSG_READY_ACK, 0, 1, 0, 0 };
                            asrt_span sp         = { .b = ack_msg, .e = ack_msg + sizeof ack_msg };
                            CHECK_EQ( ASRT_SUCCESS, asrt::recv( asrt::node( ctx.srv ), sp ) );
                    }
                    CHECK_EQ( ASRT_SUCCESS, asrt::tick( asrt::node( ctx.srv ), ctx.t++ ) );
            },
            50 );
        CHECK_EQ( ASRT_TEST_PASS, state );

        asrt_flat_tree_deinit( &tree );
}

// ---------------------------------------------------------------------------
// cntr_assm / reac_assm wrappers — assembly-paired fixture

namespace
{

/// Paired fixture that uses the C++ assembly wrappers on both sides.
struct assembly_ctx : coroutine_support
{
        uint32_t t = 1;

        asrt::unit< passing_test > t0;
        asrt::unit< failing_test > t1;

        asrt_reac_assm r_assm = {};
        asrt_cntr_assm c_assm = {};

        int          init_cb_count = 0;
        asrt::status init_status   = ASRT_SUCCESS;

        assembly_ctx()
        {
                REQUIRE_EQ( ASRT_SUCCESS, asrt::init( r_assm, "assm_reactor", 500 ) );
                REQUIRE_EQ( ASRT_SUCCESS, asrt::add_test( r_assm, t0 ) );
                REQUIRE_EQ( ASRT_SUCCESS, asrt::add_test( r_assm, t1 ) );

                REQUIRE_EQ( ASRT_SUCCESS, asrt::init( c_assm, asrt_default_allocator() ) );

                REQUIRE_EQ(
                    ASRT_SUCCESS,
                    asrt::start(
                        c_assm.cntr,
                        { []( void* self, asrt::status s ) -> asrt::status {
                                 auto* p = static_cast< assembly_ctx* >( self );
                                 p->init_cb_count++;
                                 p->init_status = s;
                                 return ASRT_SUCCESS;
                         },
                          this },
                        1000 ) );

                for ( int i = 0; i < 100 && !asrt::is_idle( c_assm.cntr ); i++ )
                        pump();
                CHECK( asrt::is_idle( c_assm.cntr ) );
        }

        ~assembly_ctx()
        {
                asrt::deinit( c_assm );
                asrt::deinit( r_assm );
        }

        void pump()
        {
                asrt::tick( r_assm, t );
                deliver( &r_assm.send_queue, &c_assm.cntr.node );
                asrt::tick( c_assm, t );
                deliver( &c_assm.send_queue, &r_assm.reactor.node );
                tctx.tick();
                t++;
        }

        void spin()
        {
                for ( int i = 0; i < 100; i++ )
                        pump();
                CHECK( asrt::is_idle( c_assm.cntr ) );
        }

        template < typename F >
        asrt_test_state run_task( F&& make_task )
        {
                return run_until_done( tctx, std::forward< F >( make_task ), [&] {
                        pump();
                } );
        }
};

}  // namespace

// ---------------------------------------------------------------------------
// cntr_assm_init_deinit

TEST_CASE( "cntr_assm_init_deinit" )
{
        asrt_cntr_assm assm = {};
        CHECK_EQ( ASRT_SUCCESS, asrt::init( assm, asrt_default_allocator() ) );
        asrt::deinit( assm );
}

TEST_CASE( "cntr_assm_tick_does_not_crash" )
{
        asrt_cntr_assm assm = {};
        REQUIRE_EQ( ASRT_SUCCESS, asrt::init( assm, asrt_default_allocator() ) );
        asrt::tick( assm, 1 );
        asrt::tick( assm, 2 );
        asrt::deinit( assm );
}

// ---------------------------------------------------------------------------
// assembly_ctx handshake

TEST_CASE_FIXTURE( assembly_ctx, "assm_init_handshake" )
{
        CHECK( asrt::is_idle( c_assm.cntr ) );
        CHECK_EQ( 1, init_cb_count );
        CHECK_EQ( ASRT_SUCCESS, init_status );
}

// ---------------------------------------------------------------------------
// exec_test callback variant

TEST_CASE_FIXTURE( assembly_ctx, "assm_exec_test_pass_callback" )
{
        asrt_test_result res = ASRT_TEST_RESULT_ERROR;
        CHECK_EQ(
            ASRT_SUCCESS,
            asrt::exec_test(
                c_assm,
                nullptr,
                0,
                0,
                1000,
                { []( void* p, asrt::status s, asrt_result* r ) -> asrt_status {
                         ASRT_INF_LOG( "asrtcpp_test", "Test callback called with status %d", s );
                         CHECK_EQ( ASRT_SUCCESS, s );
                         *static_cast< asrt_test_result* >( p ) = r->res;
                         return ASRT_SUCCESS;
                 },
                  &res } ) );
        spin();
        CHECK_EQ( ASRT_TEST_RESULT_SUCCESS, res );
}

TEST_CASE_FIXTURE( assembly_ctx, "assm_exec_test_fail_callback" )
{
        asrt_test_result res = ASRT_TEST_RESULT_ERROR;
        CHECK_EQ(
            ASRT_SUCCESS,
            asrt::exec_test(
                c_assm,
                nullptr,
                0,
                1,
                1000,
                { []( void* p, asrt::status s, asrt_result* r ) -> asrt_status {
                         CHECK_EQ( ASRT_SUCCESS, s );
                         *static_cast< asrt_test_result* >( p ) = r->res;
                         return ASRT_SUCCESS;
                 },
                  &res } ) );
        spin();
        CHECK_EQ( ASRT_TEST_RESULT_FAILURE, res );
}

// ---------------------------------------------------------------------------
// exec_test sender (co_await) variant

static asrt::task< void > do_assm_exec_test(
    asrt::task_ctx&,
    asrt_cntr_assm&   assm,
    uint16_t          tid,
    asrt_test_result& out )
{
        asrt_result r = co_await asrt::exec_test( assm, nullptr, 0, tid, 1000 );
        out           = r.res;
}

TEST_CASE_FIXTURE( assembly_ctx, "assm_exec_test_pass_sender" )
{
        asrt_test_result res   = ASRT_TEST_RESULT_ERROR;
        asrt_test_state  state = run_until_done(
            tctx,
            [&]( asrt::task_ctx& tc ) {
                    return do_assm_exec_test( tc, c_assm, 0, res );
            },
            [&] {
                    pump();
            } );
        CHECK_EQ( ASRT_TEST_PASS, state );
        CHECK_EQ( ASRT_TEST_RESULT_SUCCESS, res );
}

TEST_CASE_FIXTURE( assembly_ctx, "assm_exec_test_fail_sender" )
{
        asrt_test_result res   = ASRT_TEST_RESULT_ERROR;
        asrt_test_state  state = run_until_done(
            tctx,
            [&]( asrt::task_ctx& tc ) {
                    return do_assm_exec_test( tc, c_assm, 1, res );
            },
            [&] {
                    pump();
            } );
        CHECK_EQ( ASRT_TEST_PASS, state );
        CHECK_EQ( ASRT_TEST_RESULT_FAILURE, res );
}

// ---------------------------------------------------------------------------
// asrt::next() / send_req_ref

namespace
{
asrt_node make_node( asrt_send_req_list& sq )
{
        asrt_node n{};
        n.send_queue = &sq;
        n.chid       = ASRT_CORE;
        return n;
}
}  // namespace

TEST_CASE( "next_empty_queue_is_falsy" )
{
        asrt_send_req_list sq = {};
        CHECK_FALSE( asrt::next( sq ) );
}

TEST_CASE( "next_pending_request_is_truthy" )
{
        asrt_send_req_list sq   = {};
        asrt_node          node = make_node( sq );
        asrt_u8d2msg       msg{};
        msg.buff[0] = 0xAB;
        msg.buff[1] = 0xCD;
        asrt_send_enque( &node, &msg.req, nullptr, nullptr );

        auto req = asrt::next( sq );
        REQUIRE( req );
        CHECK_EQ( ASRT_CORE, req->chid );
}

TEST_CASE( "next_finish_pops_request" )
{
        asrt_send_req_list sq   = {};
        asrt_node          node = make_node( sq );
        asrt_u8d2msg       msg{};
        asrt_send_enque( &node, &msg.req, nullptr, nullptr );

        REQUIRE( asrt::next( sq ) );
        asrt::next( sq ).finish( ASRT_SUCCESS );
        CHECK_FALSE( asrt::next( sq ) );
}

TEST_CASE( "next_drains_multiple_requests" )
{
        asrt_send_req_list sq   = {};
        asrt_node          node = make_node( sq );
        asrt_u8d2msg       m0{}, m1{}, m2{};
        asrt_send_enque( &node, &m0.req, nullptr, nullptr );
        asrt_send_enque( &node, &m1.req, nullptr, nullptr );
        asrt_send_enque( &node, &m2.req, nullptr, nullptr );

        int count = 0;
        while ( auto req = asrt::next( sq ) ) {
                count++;
                req.finish( ASRT_SUCCESS );
        }
        CHECK_EQ( 3, count );
        CHECK_FALSE( asrt::next( sq ) );
}

TEST_CASE( "next_finish_invokes_done_cb" )
{
        asrt_send_req_list sq   = {};
        asrt_node          node = make_node( sq );
        asrt_u8d2msg       msg{};

        asrt_status reported = ASRT_INIT_ERR;
        asrt_send_enque(
            &node,
            &msg.req,
            +[]( void* p, asrt_status s ) {
                    *static_cast< asrt_status* >( p ) = s;
            },
            &reported );

        asrt::next( sq ).finish( ASRT_SUCCESS );
        CHECK_EQ( ASRT_SUCCESS, reported );
}
