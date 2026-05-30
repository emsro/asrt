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
#include "../asrtcpp/param.hpp"
#include "../asrtcpp/stream.hpp"
#include "../asrtl/log.h"
#include "../asrtlpp/fmt.hpp"
#include "../asrtlpp/util.hpp"
#include "../asrtr/reactor.h"
#include "../asrtr/record.h"
#include "../asrtrpp/collect.hpp"
#include "../asrtrpp/diag.hpp"
#include "../asrtrpp/param.hpp"
#include "../asrtrpp/reac_assm.hpp"
#include "../asrtrpp/reactor.hpp"
#include "../asrtrpp/stream.hpp"
#include "../asrtrpp/task_unit.hpp"
#include "./collector.hpp"
#include "./util.hpp"

#include <algorithm>
#include <cstring>
#include <doctest/doctest.h>
#include <format>
#include <functional>
#include <span>
#include <vector>

static ASRT_DEFINE_GPOS_LOG()

    namespace
{
}

// ---------------------------------------------------------------------------
// helpers

namespace
{
/// Drain all requests from sq into coll.
void drain( asrt_send_req_list* sq, collector* coll )
{
        drain_send_queue( sq, coll );
}

void assert_diag_record( collected_data& cd, uint32_t line )
{
        assert_collected_diag_hdr( cd, ASRT_DIAG_MSG_RECORD );
        assert_u32( line, cd.data.data() + 1 );
        CHECK( cd.data.size() > 5 );
        auto const* fn_b = cd.data.data() + 5;
        auto const* fn_e = cd.data.data() + cd.data.size();
        CHECK( std::none_of( fn_b, fn_e, []( uint8_t b ) {
                return b == '\0';
        } ) );
}

void ready_ack_noop( void*, asrt_status ) {}

// ---------------------------------------------------------------------------
// test callables for unit<T>

struct pass_test
{
        char const* name() const { return "pass_test"; }
        asrt_status operator()( asrt::record& rec )
        {
                rec.state = ASRT_TEST_PASS;
                return ASRT_SUCCESS;
        }
};

// Returns a transport error — tests the trampoline's error→FAIL mapping.
struct err_cb_test
{
        char const* name() const { return "err_cb_test"; }
        asrt_status operator()( asrt::record& ) { return ASRT_INTERNAL_ERR; }
};

// Properly-failing test: sets state=FAIL and returns SUCCESS,.
struct fail_test
{
        char const* name() const { return "fail_test"; }
        asrt_status operator()( asrt::record& rec )
        {
                rec.state = ASRT_TEST_FAIL;
                return ASRT_SUCCESS;
        }
};

// ---------------------------------------------------------------------------
// fixtures

struct reactor_ctx
{
        asrt_send_req_list sq = {};
        collector          coll;
        asrt_reactor       r;

        reactor_ctx()
        {
                if ( asrt::init( r, sq, "test_reactor" ) != ASRT_SUCCESS )
                        throw std::runtime_error( "reactor init failed" );
        }
        ~reactor_ctx()
        {
                CHECK( coll.data.empty() );
                asrt::deinit( r );
        }
};

struct diag_ctx
{
        asrt_send_req_list sq_r = {};
        asrt_send_req_list sq_d = {};
        collector          coll_r;
        collector          coll_d;
        asrt_reactor       r;
        asrt_diag_client   d;

        diag_ctx()
        {
                if ( asrt::init( r, sq_r, "test_reactor" ) != ASRT_SUCCESS )
                        throw std::runtime_error( "reactor init failed" );
                if ( asrt::init( d, asrt::node( r ) ) != ASRT_SUCCESS )
                        throw std::runtime_error( "diag init failed" );
                asrt::node( d ).send_queue = &sq_d;
        }
        ~diag_ctx()
        {
                CHECK( coll_r.data.empty() );
                CHECK( coll_d.data.empty() );
                asrt::deinit( d );
                asrt::deinit( r );
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
// reactor

TEST_CASE_FIXTURE( reactor_ctx, "reactor_init" )
{
        CHECK_EQ( ASRT_CORE, asrt::node( r ).chid );
}

TEST_CASE_FIXTURE( reactor_ctx, "reactor_tick" )
{
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &asrt::node( r ), 0 ) );
}

// ---------------------------------------------------------------------------
// unit<T>

TEST_CASE( "unit_cb_pass" )
{
        asrt::unit< pass_test > u;

        asrt_test_input input{};
        input.test_ptr = &u;

        asrt_record rec{
            .state = ASRT_TEST_RUNNING,
            .inpt  = &input,
        };

        asrt_status st = asrt::unit< pass_test >::cb( &rec );
        CHECK_EQ( ASRT_SUCCESS, st );
        CHECK_NE( ASRT_TEST_FAIL, rec.state );
}

TEST_CASE( "unit_cb_fail" )
{
        // When T returns a transport error, cb forces rec.state to FAIL.
        asrt::unit< err_cb_test > u;

        asrt_test_input input{};
        input.test_ptr = &u;

        asrt_record rec{
            .state = ASRT_TEST_RUNNING,
            .inpt  = &input,
        };

        asrt_status st = asrt::unit< err_cb_test >::cb( &rec );
        CHECK_NE( ASRT_SUCCESS, st );
        CHECK_EQ( ASRT_TEST_FAIL, rec.state );
}

// ---------------------------------------------------------------------------
// diag

TEST_CASE_FIXTURE( diag_ctx, "diag_init" )
{
        CHECK_EQ( ASRT_DIAG, asrt::node( d ).chid );
        CHECK_EQ( &asrt::node( d ), asrt::node( r ).next );
}

TEST_CASE_FIXTURE( diag_ctx, "diag_record" )
{
        auto noop_done = []( asrt_status ) {};
        asrt::rec_diag( d, "diag_test.cpp", 42, nullptr, noop_done );
        drain( &sq_d, &coll_d );
        REQUIRE_EQ( 1U, coll_d.data.size() );
        assert_diag_record( coll_d.data.front(), 42 );
        coll_d.data.pop_front();
}

// ---------------------------------------------------------------------------
// reactor end-to-end
namespace
{
void assert_test_start_msg( collected_data& cd, uint16_t test_id, uint32_t run_id )
{
        assert_collected_core_hdr( cd, 0x08, ASRT_MSG_TEST_START );
        assert_u16( test_id, cd.data.data() + 2 );
        assert_u32( run_id, cd.data.data() + 4 );
}

void assert_test_result_msg( collected_data& cd, uint32_t run_id, enum asrt_test_result_e result )
{
        assert_collected_core_hdr( cd, 0x08, ASRT_MSG_TEST_RESULT );
        assert_u32( run_id, cd.data.data() + 2 );
        assert_u16( result, cd.data.data() + 6 );
}

struct e2e_ctx
{
        asrt_send_req_list      sq = {};
        collector               coll;
        asrt_reactor            r;
        asrt::unit< pass_test > t0;
        asrt::unit< fail_test > t1;
        asrt::unit< pass_test > t2;

        e2e_ctx()
        {
                ASRT_INF_LOG( "asrtrpp_test", "e2e_ctx test start" );
                if ( asrt::init( r, sq, "e2e_reactor" ) != ASRT_SUCCESS )
                        throw std::runtime_error( "reactor init failed" );
                if ( asrt::add_test( r, t0 ) != ASRT_SUCCESS )
                        throw std::runtime_error( "add_test failed" );
                if ( asrt::add_test( r, t1 ) != ASRT_SUCCESS )
                        throw std::runtime_error( "add_test failed" );
                if ( asrt::add_test( r, t2 ) != ASRT_SUCCESS )
                        throw std::runtime_error( "add_test failed" );
        }

        ~e2e_ctx()
        {
                ASRT_INF_LOG( "asrtrpp_test", "e2e_ctx test end" );
                CHECK( coll.data.empty() );
                asrt::deinit( r );
        }

        void run( uint16_t test_id, uint32_t run_id )
        {
                struct asrt_u8d8msg   msg = {};
                struct asrt_send_req* req = asrt_msg_ctor_test_start( &msg, test_id, run_id );
                uint8_t               flat[8];
                memcpy( flat, req->buff.b, (size_t) ( req->buff.e - req->buff.b ) );
                CHECK_EQ(
                    ASRT_SUCCESS,
                    asrt::recv(
                        asrt::node( r ),
                        asrt_span{ .b = flat, .e = flat + ( req->buff.e - req->buff.b ) } ) );
                for ( int i = 0; i < 10; i++ ) {
                        CHECK_EQ( ASRT_SUCCESS, asrt::tick( asrt::node( r ), 0 ) );
                        drain( &sq, &coll );
                }
        }
};
}  // namespace
TEST_CASE_FIXTURE( e2e_ctx, "reactor_e2e" )
{
        // test 0: pass_test
        run( 0, 10 );
        REQUIRE_EQ( 2U, coll.data.size() );
        assert_test_result_msg( coll.data.back(), 10, ASRT_TEST_RESULT_SUCCESS );
        coll.data.pop_back();
        assert_test_start_msg( coll.data.back(), 0, 10 );
        coll.data.pop_back();

        // test 1: fail_test
        run( 1, 20 );
        REQUIRE_EQ( 2U, coll.data.size() );
        assert_test_result_msg( coll.data.back(), 20, ASRT_TEST_RESULT_FAILURE );
        coll.data.pop_back();
        assert_test_start_msg( coll.data.back(), 1, 20 );
        coll.data.pop_back();

        // test 2: pass_test (same callable as t0, different slot)
        run( 2, 30 );
        REQUIRE_EQ( 2U, coll.data.size() );
        assert_test_result_msg( coll.data.back(), 30, ASRT_TEST_RESULT_SUCCESS );
        coll.data.pop_back();
        assert_test_start_msg( coll.data.back(), 2, 30 );
        coll.data.pop_back();
}

// ---------------------------------------------------------------------------
// param_client wrapper — loopback with C++ param_server

namespace
{

/// Flatten a send_req and dispatch to the matching channel node under peer_head.
void deliver_req( asrt_send_req_list* sq, asrt_node* peer_head )
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
                enum asrt_status st     = ASRT_SUCCESS;
                asrt_node*       target = asrt_chann_find( peer_head, req->chid );
                if ( target )
                        st = asrt_chann_recv( target, ( asrt_span ){ flat, p } );
                if ( req->done_cb )
                        req->done_cb( req->done_ptr, st );
        }
}

struct param_loopback_cpp_ctx
{
        asrt_send_req_list srv_sq   = {};
        asrt_send_req_list cli_sq   = {};
        asrt_node          srv_head = {};
        asrt_node          cli_head = {};

        asrt_param_server srv;

        static constexpr uint32_t buff_size          = 256;
        uint8_t                   cli_buf[buff_size] = {};
        asrt_param_client         cli;

        // response state
        struct received_node
        {
                asrt::flat_id   id;
                std::string     key;
                asrt_flat_value value;
                asrt::flat_id   next_sibling;
        };
        std::vector< received_node > received;
        int                          error_called = 0;
        uint32_t                     t            = 1;
        asrt_param_query             query        = {};

        static void query_cb( asrt_param_client*, asrt_param_query* q, asrt_flat_value val )
        {
                auto* ctx = (param_loopback_cpp_ctx*) q->cb_ptr;
                if ( q->error_code != 0 ) {
                        ctx->error_called++;
                } else {
                        ctx->received.push_back(
                            { q->node_id, q->key ? q->key : "", val, q->next_sibling } );
                }
        }

        void spin( int max_iter = 100 )
        {
                for ( int i = 0; i < max_iter; i++ ) {
                        CHECK_EQ( ASRT_SUCCESS, asrt::tick( asrt::node( srv ), t++ ) );
                        deliver_req( &srv_sq, &cli_head );
                        CHECK_EQ( ASRT_SUCCESS, asrt::tick( asrt::node( cli ), t++ ) );
                        deliver_req( &cli_sq, &srv_head );
                        if ( asrt::ready( cli ) )
                                break;
                }
        }

        void spin_query( int max_iter = 100 )
        {
                for ( int i = 0; i < max_iter; i++ ) {
                        CHECK_EQ( ASRT_SUCCESS, asrt::tick( asrt::node( srv ), t++ ) );
                        deliver_req( &srv_sq, &cli_head );
                        CHECK_EQ( ASRT_SUCCESS, asrt::tick( asrt::node( cli ), t++ ) );
                        deliver_req( &cli_sq, &srv_head );
                }
        }

        param_loopback_cpp_ctx()
        {
                srv_head.send_queue = &srv_sq;
                cli_head.send_queue = &cli_sq;
                srv_head.chid       = ASRT_CORE;
                cli_head.chid       = ASRT_CORE;

                if ( asrt::init( srv, srv_head, asrt_default_allocator() ) != ASRT_SUCCESS )
                        throw std::runtime_error( "server init failed" );
                if ( asrt::init(
                         cli,
                         cli_head,
                         asrt_span{ .b = cli_buf, .e = cli_buf + buff_size },
                         100 ) != ASRT_SUCCESS )
                        throw std::runtime_error( "client init failed" );
        }

        ~param_loopback_cpp_ctx()
        {
                received.clear();
                asrt::deinit( cli );
                asrt::deinit( srv );
        }
};

}  // namespace

TEST_CASE_FIXTURE( param_loopback_cpp_ctx, "param_cpp_loopback_handshake" )
{
        struct asrt_flat_tree tree;
        asrt_flat_tree_init( &tree, asrt_default_allocator(), 4, 16 );
        asrt_flat_tree_append_cont( &tree, 0, 1, nullptr, ASRT_FLAT_CTYPE_OBJECT );
        asrt::set_tree( srv, tree );

        CHECK_FALSE( asrt::ready( cli ) );
        CHECK_EQ( ASRT_SUCCESS, asrt::send_ready( srv, 1U, { ready_ack_noop, nullptr }, 1000 ) );
        spin();
        CHECK( asrt::ready( cli ) );
        CHECK_EQ( 1U, asrt::root_id( cli ) );

        asrt_flat_tree_deinit( &tree );
}

TEST_CASE_FIXTURE( param_loopback_cpp_ctx, "param_cpp_loopback_traversal" )
{
        struct asrt_flat_tree tree;
        asrt_flat_tree_init( &tree, asrt_default_allocator(), 4, 16 );
        asrt_flat_tree_append_cont( &tree, 0, 1, nullptr, ASRT_FLAT_CTYPE_OBJECT );
        asrt_flat_tree_append_scalar( &tree, 1, 2, "a", ASRT_FLAT_STYPE_U32, { .u32_val = 10 } );
        asrt_flat_tree_append_scalar( &tree, 1, 3, "b", ASRT_FLAT_STYPE_STR, { .str_val = "hi" } );
        asrt::set_tree( srv, tree );

        CHECK_EQ( ASRT_SUCCESS, asrt::send_ready( srv, 1U, { ready_ack_noop, nullptr }, 1000 ) );
        spin();
        REQUIRE( asrt::ready( cli ) );

        // Query root
        CHECK_EQ( ASRT_SUCCESS, asrt::fetch( cli, &query, 1U, query_cb, this ) );
        spin_query();
        REQUIRE_EQ( 1U, received.size() );
        CHECK_EQ( 1U, received[0].id );
        CHECK_EQ( (uint8_t) ASRT_FLAT_CTYPE_OBJECT, (uint8_t) received[0].value.type );
        asrt::flat_id first_child = received[0].value.data.cont.first_child;

        // Query first child "a"
        CHECK_EQ( ASRT_SUCCESS, asrt::fetch( cli, &query, first_child, query_cb, this ) );
        spin_query();
        REQUIRE_EQ( 2U, received.size() );
        CHECK_EQ( "a", received[1].key );
        CHECK_EQ( 10U, received[1].value.data.s.u32_val );
        asrt::flat_id next_sib = received[1].next_sibling;

        // Query next sibling "b"
        CHECK_EQ( ASRT_SUCCESS, asrt::fetch( cli, &query, next_sib, query_cb, this ) );
        spin_query();
        REQUIRE_EQ( 3U, received.size() );
        CHECK_EQ( "b", received[2].key );
        CHECK_EQ( (uint8_t) ASRT_FLAT_STYPE_STR, (uint8_t) received[2].value.type );
        CHECK_EQ( 0U, received[2].next_sibling );

        CHECK_EQ( 0, error_called );
        asrt_flat_tree_deinit( &tree );
}

TEST_CASE_FIXTURE( param_loopback_cpp_ctx, "param_cpp_error_reaches_callback" )
{
        struct asrt_flat_tree tree;
        asrt_flat_tree_init( &tree, asrt_default_allocator(), 4, 16 );
        asrt_flat_tree_append_cont( &tree, 0, 1, nullptr, ASRT_FLAT_CTYPE_OBJECT );
        asrt::set_tree( srv, tree );

        CHECK_EQ( ASRT_SUCCESS, asrt::send_ready( srv, 1U, { ready_ack_noop, nullptr }, 1000 ) );
        spin();
        REQUIRE( asrt::ready( cli ) );

        // Query non-existent node — server sends ERROR
        CHECK_EQ( ASRT_SUCCESS, asrt::fetch( cli, &query, 999U, query_cb, this ) );
        spin_query();

        // The node doesn't exist, so server sends a response with NONE type
        // (not an error — the server encodes whatever the tree returns).
        // The callback fires with the queried id.
        REQUIRE_EQ( 1U, received.size() );
        CHECK_EQ( 999U, received[0].id );

        asrt_flat_tree_deinit( &tree );
}

// ---------------------------------------------------------------------------
// typed query<T> tests
namespace
{

struct typed_loopback_ctx
{
        typed_loopback_ctx( typed_loopback_ctx const& )            = delete;
        typed_loopback_ctx& operator=( typed_loopback_ctx const& ) = delete;
        typed_loopback_ctx( typed_loopback_ctx&& )                 = delete;
        typed_loopback_ctx& operator=( typed_loopback_ctx&& )      = delete;

        asrt_send_req_list srv_sq   = {};
        asrt_send_req_list cli_sq   = {};
        asrt_node          srv_head = {};
        asrt_node          cli_head = {};

        asrt_param_server srv;

        static constexpr uint32_t buff_size          = 256;
        uint8_t                   cli_buf[buff_size] = {};
        asrt_param_client         cli;

        uint32_t         t     = 1;
        asrt_param_query query = {};

        // results
        uint32_t             u32_val = 0;
        int32_t              i32_val = 0;
        float                flt_val = 0.0F;
        std::string          str_val;
        asrt_flat_child_list obj_val  = {};
        asrt_flat_child_list arr_val  = {};
        uint32_t             bool_val = 0;
        int                  cb_count = 0;
        bool                 got_null = false;

        typed_loopback_ctx()
        {
                srv_head.send_queue = &srv_sq;
                cli_head.send_queue = &cli_sq;
                srv_head.chid       = ASRT_CORE;
                cli_head.chid       = ASRT_CORE;

                if ( asrt::init( srv, srv_head, asrt_default_allocator() ) != ASRT_SUCCESS )
                        throw std::runtime_error( "server init failed" );
                if ( asrt::init(
                         cli,
                         cli_head,
                         asrt_span{ .b = cli_buf, .e = cli_buf + buff_size },
                         100 ) != ASRT_SUCCESS )
                        throw std::runtime_error( "client init failed" );
        }

        ~typed_loopback_ctx()
        {
                asrt::deinit( cli );
                asrt::deinit( srv );
        }

        void setup_tree_and_handshake( asrt_flat_tree* tree )
        {
                asrt::set_tree( srv, *tree );
                CHECK_EQ(
                    ASRT_SUCCESS, asrt::send_ready( srv, 1U, { ready_ack_noop, nullptr }, 1000 ) );
                for ( int i = 0; i < 100; i++ ) {
                        CHECK_EQ( ASRT_SUCCESS, asrt::tick( asrt::node( srv ), t++ ) );
                        deliver_req( &srv_sq, &cli_head );
                        CHECK_EQ( ASRT_SUCCESS, asrt::tick( asrt::node( cli ), t++ ) );
                        deliver_req( &cli_sq, &srv_head );
                        if ( asrt::ready( cli ) )
                                break;
                }
                REQUIRE( asrt::ready( cli ) );
        }

        void spin_query( int max_iter = 100 )
        {
                for ( int i = 0; i < max_iter; i++ ) {
                        CHECK_EQ( ASRT_SUCCESS, asrt::tick( asrt::node( srv ), t++ ) );
                        deliver_req( &srv_sq, &cli_head );
                        CHECK_EQ( ASRT_SUCCESS, asrt::tick( asrt::node( cli ), t++ ) );
                        deliver_req( &cli_sq, &srv_head );
                }
        }
};
}  // namespace
TEST_CASE_FIXTURE( typed_loopback_ctx, "typed_query_u32_happy" )
{
        asrt_flat_tree tree;
        asrt_flat_tree_init( &tree, asrt_default_allocator(), 4, 16 );
        asrt_flat_tree_append_cont( &tree, 0, 1, nullptr, ASRT_FLAT_CTYPE_OBJECT );
        asrt_flat_tree_append_scalar( &tree, 1, 2, "val", ASRT_FLAT_STYPE_U32, { .u32_val = 42 } );
        setup_tree_and_handshake( &tree );

        auto cb = [this]( asrt_param_client*, asrt_param_query*, uint32_t v ) {
                cb_count++;
                u32_val = v;
        };
        CHECK_EQ( ASRT_SUCCESS, asrt::fetch< uint32_t >( cli, &query, 2U, cb ) );
        spin_query();
        CHECK_EQ( 1, cb_count );
        CHECK_EQ( 42U, u32_val );
        asrt_flat_tree_deinit( &tree );
}

TEST_CASE_FIXTURE( typed_loopback_ctx, "typed_query_u32_mismatch" )
{
        asrt_flat_tree tree;
        asrt_flat_tree_init( &tree, asrt_default_allocator(), 4, 16 );
        asrt_flat_tree_append_cont( &tree, 0, 1, nullptr, ASRT_FLAT_CTYPE_OBJECT );
        asrt_flat_tree_append_scalar(
            &tree, 1, 2, "val", ASRT_FLAT_STYPE_STR, { .str_val = "nope" } );
        setup_tree_and_handshake( &tree );

        auto cb = [this]( asrt_param_client*, asrt_param_query* q, uint32_t ) {
                cb_count++;
                got_null = ( q->error_code != 0 );
        };
        CHECK_EQ( ASRT_SUCCESS, asrt::fetch< uint32_t >( cli, &query, 2U, cb ) );
        spin_query();
        CHECK_EQ( 1, cb_count );
        CHECK( got_null );
        asrt_flat_tree_deinit( &tree );
}

TEST_CASE_FIXTURE( typed_loopback_ctx, "typed_query_i32_happy" )
{
        asrt_flat_tree tree;
        asrt_flat_tree_init( &tree, asrt_default_allocator(), 4, 16 );
        asrt_flat_tree_append_cont( &tree, 0, 1, nullptr, ASRT_FLAT_CTYPE_OBJECT );
        asrt_flat_tree_append_scalar( &tree, 1, 2, "val", ASRT_FLAT_STYPE_I32, { .i32_val = -7 } );
        setup_tree_and_handshake( &tree );

        auto cb = [this]( asrt_param_client*, asrt_param_query*, int32_t v ) {
                cb_count++;
                i32_val = v;
        };
        CHECK_EQ( ASRT_SUCCESS, asrt::fetch< int32_t >( cli, &query, 2U, cb ) );
        spin_query();
        CHECK_EQ( 1, cb_count );
        CHECK_EQ( -7, i32_val );
        asrt_flat_tree_deinit( &tree );
}

TEST_CASE_FIXTURE( typed_loopback_ctx, "typed_query_str_happy" )
{
        asrt_flat_tree tree;
        asrt_flat_tree_init( &tree, asrt_default_allocator(), 4, 16 );
        asrt_flat_tree_append_cont( &tree, 0, 1, nullptr, ASRT_FLAT_CTYPE_OBJECT );
        asrt_flat_tree_append_scalar(
            &tree, 1, 2, "val", ASRT_FLAT_STYPE_STR, { .str_val = "hello" } );
        setup_tree_and_handshake( &tree );

        auto cb = [this]( asrt_param_client*, asrt_param_query*, char const* v ) {
                cb_count++;
                if ( v )
                        str_val = v;
        };
        CHECK_EQ( ASRT_SUCCESS, asrt::fetch< char const* >( cli, &query, 2U, cb ) );
        spin_query();
        CHECK_EQ( 1, cb_count );
        CHECK_EQ( "hello", str_val );
        asrt_flat_tree_deinit( &tree );
}

TEST_CASE_FIXTURE( typed_loopback_ctx, "typed_query_float_happy" )
{
        asrt_flat_tree tree;
        asrt_flat_tree_init( &tree, asrt_default_allocator(), 4, 16 );
        asrt_flat_tree_append_cont( &tree, 0, 1, nullptr, ASRT_FLAT_CTYPE_OBJECT );
        asrt_flat_tree_append_scalar(
            &tree, 1, 2, "val", ASRT_FLAT_STYPE_FLOAT, { .float_val = 3.14F } );
        setup_tree_and_handshake( &tree );

        auto cb = [this]( asrt_param_client*, asrt_param_query*, float v ) {
                cb_count++;
                flt_val = v;
        };
        CHECK_EQ( ASRT_SUCCESS, asrt::fetch< float >( cli, &query, 2U, cb ) );
        spin_query();
        CHECK_EQ( 1, cb_count );
        CHECK_EQ( doctest::Approx( 3.14F ), flt_val );
        asrt_flat_tree_deinit( &tree );
}

TEST_CASE_FIXTURE( typed_loopback_ctx, "typed_query_any_happy" )
{
        asrt_flat_tree tree;
        asrt_flat_tree_init( &tree, asrt_default_allocator(), 4, 16 );
        asrt_flat_tree_append_cont( &tree, 0, 1, nullptr, ASRT_FLAT_CTYPE_OBJECT );
        asrt_flat_tree_append_scalar( &tree, 1, 2, "val", ASRT_FLAT_STYPE_U32, { .u32_val = 99 } );
        setup_tree_and_handshake( &tree );

        auto cb = [this]( asrt_param_client*, asrt_param_query*, asrt_flat_value v ) {
                cb_count++;
                u32_val = v.data.s.u32_val;
        };
        // untyped query — explicit asrt_flat_value
        CHECK_EQ( ASRT_SUCCESS, asrt::fetch< asrt_flat_value >( cli, &query, 2U, cb ) );
        spin_query();
        CHECK_EQ( 1, cb_count );
        CHECK_EQ( 99U, u32_val );
        asrt_flat_tree_deinit( &tree );
}

TEST_CASE_FIXTURE( typed_loopback_ctx, "query_pending_cpp" )
{
        asrt_flat_tree tree;
        asrt_flat_tree_init( &tree, asrt_default_allocator(), 4, 16 );
        asrt_flat_tree_append_cont( &tree, 0, 1, nullptr, ASRT_FLAT_CTYPE_OBJECT );
        asrt_flat_tree_append_scalar( &tree, 1, 2, "val", ASRT_FLAT_STYPE_U32, { .u32_val = 7 } );
        setup_tree_and_handshake( &tree );

        CHECK_FALSE( asrt::query_pending( cli ) );

        auto cb = [this]( asrt_param_client*, asrt_param_query*, uint32_t v ) {
                cb_count++;
                u32_val = v;
        };
        CHECK_EQ( ASRT_SUCCESS, asrt::fetch< uint32_t >( cli, &query, 2U, cb ) );
        CHECK( asrt::query_pending( cli ) );

        spin_query();
        CHECK_FALSE( asrt::query_pending( cli ) );
        CHECK_EQ( 1, cb_count );
        CHECK_EQ( 7U, u32_val );
        asrt_flat_tree_deinit( &tree );
}

// ============================================================================
// C++ query timeout tests
// ============================================================================

static asrt_status send_ready_to_client( asrt_param_client& cli, asrt::flat_id root_id )
{
        uint8_t  buf[8];
        uint8_t* p = buf;
        *p++       = ASRT_PARAM_MSG_READY;
        asrt_add_u32( &p, root_id );
        auto* n = &asrt::node( cli );
        return asrt_chann_recv( n, asrt_span{ .b = buf, .e = p } );
}

TEST_CASE( "param_client_cpp_timeout" )
{
        asrt_send_req_list sq   = {};
        asrt_node          head = {};
        head.chid               = ASRT_CORE;
        head.send_queue         = &sq;
        collector coll;

        static constexpr uint32_t buff_size      = 256;
        static constexpr uint32_t timeout        = 10;
        uint8_t                   buf[buff_size] = {};
        asrt_param_client         cli;
        REQUIRE_EQ(
            ASRT_SUCCESS,
            asrt::init( cli, head, asrt_span{ .b = buf, .e = buf + buff_size }, timeout ) );

        // make ready
        REQUIRE_EQ( ASRT_SUCCESS, send_ready_to_client( cli, 1U ) );
        REQUIRE_EQ( ASRT_SUCCESS, asrt::tick( asrt::node( cli ), 0 ) );
        drain_send_queue( &sq, &coll );
        coll.data.clear();
        REQUIRE( asrt::ready( cli ) );

        int     called = 0;
        uint8_t err    = 0;
        auto    cb     = [&]( asrt_param_client*, asrt_param_query* q, uint32_t ) {
                called++;
                err = q->error_code;
        };
        asrt_param_query query = {};
        CHECK_EQ( ASRT_SUCCESS, asrt::fetch< uint32_t >( cli, &query, 10U, cb ) );

        // DELIVER tick → wire
        CHECK_EQ( ASRT_SUCCESS, asrt::tick( asrt::node( cli ), 100 ) );
        CHECK_EQ( 0, called );

        // start timing
        CHECK_EQ( ASRT_SUCCESS, asrt::tick( asrt::node( cli ), 100 ) );
        CHECK_EQ( 0, called );

        // just before timeout
        CHECK_EQ( ASRT_SUCCESS, asrt::tick( asrt::node( cli ), 109 ) );
        CHECK_EQ( 0, called );

        // at timeout
        CHECK_EQ( ASRT_SUCCESS, asrt::tick( asrt::node( cli ), 110 ) );
        CHECK_EQ( 1, called );
        CHECK_EQ( ASRT_PARAM_ERR_TIMEOUT, err );
        CHECK_FALSE( asrt::query_pending( cli ) );

        asrt::deinit( cli );
}

// ============================================================================
// C++ find-by-key tests
// ============================================================================

TEST_CASE_FIXTURE( param_loopback_cpp_ctx, "param_cpp_find_by_key_raw" )
{
        struct asrt_flat_tree tree;
        asrt_flat_tree_init( &tree, asrt_default_allocator(), 4, 16 );
        asrt_flat_tree_append_cont( &tree, 0, 1, nullptr, ASRT_FLAT_CTYPE_OBJECT );
        asrt_flat_tree_append_scalar( &tree, 1, 2, "a", ASRT_FLAT_STYPE_U32, { .u32_val = 10 } );
        asrt_flat_tree_append_scalar( &tree, 1, 3, "b", ASRT_FLAT_STYPE_STR, { .str_val = "hi" } );
        asrt::set_tree( srv, tree );

        CHECK_EQ( ASRT_SUCCESS, asrt::send_ready( srv, 1U, { ready_ack_noop, nullptr }, 1000 ) );
        spin();
        REQUIRE( asrt::ready( cli ) );

        // Find "b" by key using raw callback
        CHECK_EQ( ASRT_SUCCESS, asrt::find( cli, &query, 1U, "b", query_cb, this ) );
        spin_query();
        REQUIRE_EQ( 1U, received.size() );
        CHECK_EQ( 3U, received[0].id );
        CHECK_EQ( "b", received[0].key );
        CHECK_EQ( (uint8_t) ASRT_FLAT_STYPE_STR, (uint8_t) received[0].value.type );
        CHECK_EQ( 0, error_called );

        asrt_flat_tree_deinit( &tree );
}

TEST_CASE_FIXTURE( typed_loopback_ctx, "typed_find_u32_happy" )
{
        asrt_flat_tree tree;
        asrt_flat_tree_init( &tree, asrt_default_allocator(), 4, 16 );
        asrt_flat_tree_append_cont( &tree, 0, 1, nullptr, ASRT_FLAT_CTYPE_OBJECT );
        asrt_flat_tree_append_scalar( &tree, 1, 2, "val", ASRT_FLAT_STYPE_U32, { .u32_val = 42 } );
        asrt_flat_tree_append_scalar(
            &tree, 1, 3, "other", ASRT_FLAT_STYPE_STR, { .str_val = "x" } );
        setup_tree_and_handshake( &tree );

        auto cb = [this]( asrt_param_client*, asrt_param_query*, uint32_t v ) {
                cb_count++;
                u32_val = v;
        };
        CHECK_EQ( ASRT_SUCCESS, asrt::find< uint32_t >( cli, &query, 1U, "val", cb ) );
        spin_query();
        CHECK_EQ( 1, cb_count );
        CHECK_EQ( 42U, u32_val );
        asrt_flat_tree_deinit( &tree );
}

TEST_CASE_FIXTURE( typed_loopback_ctx, "typed_find_str_happy" )
{
        asrt_flat_tree tree;
        asrt_flat_tree_init( &tree, asrt_default_allocator(), 4, 16 );
        asrt_flat_tree_append_cont( &tree, 0, 1, nullptr, ASRT_FLAT_CTYPE_OBJECT );
        asrt_flat_tree_append_scalar(
            &tree, 1, 2, "val", ASRT_FLAT_STYPE_STR, { .str_val = "world" } );
        setup_tree_and_handshake( &tree );

        auto cb = [this]( asrt_param_client*, asrt_param_query*, char const* v ) {
                cb_count++;
                if ( v )
                        str_val = v;
        };
        CHECK_EQ( ASRT_SUCCESS, asrt::find< char const* >( cli, &query, 1U, "val", cb ) );
        spin_query();
        CHECK_EQ( 1, cb_count );
        CHECK_EQ( "world", str_val );
        asrt_flat_tree_deinit( &tree );
}

TEST_CASE_FIXTURE( typed_loopback_ctx, "typed_find_not_found" )
{
        asrt_flat_tree tree;
        asrt_flat_tree_init( &tree, asrt_default_allocator(), 4, 16 );
        asrt_flat_tree_append_cont( &tree, 0, 1, nullptr, ASRT_FLAT_CTYPE_OBJECT );
        asrt_flat_tree_append_scalar( &tree, 1, 2, "val", ASRT_FLAT_STYPE_U32, { .u32_val = 7 } );
        setup_tree_and_handshake( &tree );

        auto cb = [this]( asrt_param_client*, asrt_param_query* q, uint32_t ) {
                cb_count++;
                got_null = ( q->error_code != 0 );
        };
        CHECK_EQ( ASRT_SUCCESS, asrt::find< uint32_t >( cli, &query, 1U, "missing", cb ) );
        spin_query();
        CHECK_EQ( 1, cb_count );
        CHECK( got_null );
        asrt_flat_tree_deinit( &tree );
}

TEST_CASE_FIXTURE( typed_loopback_ctx, "typed_find_c_callback" )
{
        asrt_flat_tree tree;
        asrt_flat_tree_init( &tree, asrt_default_allocator(), 4, 16 );
        asrt_flat_tree_append_cont( &tree, 0, 1, nullptr, ASRT_FLAT_CTYPE_OBJECT );
        asrt_flat_tree_append_scalar( &tree, 1, 2, "val", ASRT_FLAT_STYPE_U32, { .u32_val = 55 } );
        setup_tree_and_handshake( &tree );

        auto c_cb = []( asrt_param_client*, asrt_param_query* q, uint32_t v ) {
                auto* ctx = (typed_loopback_ctx*) q->cb_ptr;
                ctx->cb_count++;
                ctx->u32_val = v;
        };
        CHECK_EQ( ASRT_SUCCESS, asrt::find< uint32_t >( cli, &query, 1U, "val", c_cb, this ) );
        spin_query();
        CHECK_EQ( 1, cb_count );
        CHECK_EQ( 55U, u32_val );
        asrt_flat_tree_deinit( &tree );
}

namespace
{
// ---------------------------------------------------------------------------
// task_unit<T> — coroutine-based test harness

// --- test task definitions ---

struct tu_pass : asrt::task_test
{
        char const*        name = "tu_pass";
        asrt::task< void > exec() { co_return; }
};

struct tu_fail : asrt::task_test
{
        char const*        name = "tu_fail";
        asrt::task< void > exec() { co_yield asrt::with_error{ ASRT_FAILURE }; }
};

struct tu_error : asrt::task_test
{
        char const*        name = "tu_error";
        asrt::task< void > exec() { co_yield asrt::with_error{ ASRT_INIT_ERR }; }
};

struct tu_multi_pass : asrt::task_test
{
        char const*        name  = "tu_multi_pass";
        int                ticks = 0;
        asrt::task< void > exec()
        {
                for ( int i = 0; i < 4; ++i ) {
                        ++ticks;
                        co_await asrt::suspend;
                }
        }
};

struct tu_multi_fail : asrt::task_test
{
        char const*        name = "tu_multi_fail";
        asrt::task< void > exec()
        {
                co_await asrt::suspend;
                co_await asrt::suspend;
                co_yield asrt::with_error{ ASRT_FAILURE };
        }
};

struct tu_multi_error : asrt::task_test
{
        char const*        name = "tu_multi_error";
        asrt::task< void > exec()
        {
                co_await asrt::suspend;
                co_yield asrt::with_error{ ASRT_INIT_ERR };
        }
};

template < typename T >
asrt_test_state run_task_unit_once(
    asrt::task_ctx&       ctx,
    asrt::task_unit< T >& u,
    int                   max_ticks = 100 )
{
        asrt_test_input input{};
        input.test_ptr   = &u;
        input.continue_f = asrt::task_unit< T >::cb;

        asrt_record rec{
            .state = ASRT_TEST_INIT,
            .inpt  = &input,
        };

        for ( int i = 0; i < max_ticks; ++i ) {
                asrt::task_unit< T >::cb( &rec );
                ctx.tick();
                if ( rec.state != ASRT_TEST_RUNNING && rec.state != ASRT_TEST_INIT )
                        break;
        }

        return rec.state;
}
}  // namespace
// --- task_unit: construction ---

TEST_CASE( "task_unit_name" )
{
        asrt::malloc_free_memory_resource mem;
        asrt::task_ctx                    ctx{ mem };
        asrt::task_unit< tu_pass >        u{ tu_pass{ ctx } };

        CHECK_EQ( std::string( "tu_pass" ), u.desc );
}

// --- task_unit: direct cb tests ---
namespace
{
struct tu_cb_ctx
{
        asrt::malloc_free_memory_resource mem;
        asrt::task_ctx                    ctx{ mem };

        template < typename T >
        void run_to_completion( asrt::task_unit< T >&, asrt_record& rec, int max_ticks = 100 )
        {
                for ( int i = 0; i < max_ticks; ++i ) {
                        asrt::task_unit< T >::cb( &rec );
                        ctx.tick();
                        if ( rec.state != ASRT_TEST_RUNNING && rec.state != ASRT_TEST_INIT )
                                return;
                }
        }
};
}  // namespace
TEST_CASE_FIXTURE( tu_cb_ctx, "task_unit_cb_pass" )
{
        asrt::task_unit< tu_pass > u{ tu_pass{ ctx } };

        asrt_test_input input{};
        input.test_ptr   = &u;
        input.continue_f = asrt::task_unit< tu_pass >::cb;

        asrt_record rec{
            .state = ASRT_TEST_INIT,
            .inpt  = &input,
        };

        run_to_completion( u, rec );
        CHECK_EQ( ASRT_TEST_PASS, rec.state );
}

TEST_CASE_FIXTURE( tu_cb_ctx, "task_unit_cb_fail" )
{
        asrt::task_unit< tu_fail > u{ tu_fail{ ctx } };

        asrt_test_input input{};
        input.test_ptr   = &u;
        input.continue_f = asrt::task_unit< tu_fail >::cb;

        asrt_record rec{
            .state = ASRT_TEST_INIT,
            .inpt  = &input,
        };

        run_to_completion( u, rec );
        CHECK_EQ( ASRT_TEST_FAIL, rec.state );
}

TEST_CASE_FIXTURE( tu_cb_ctx, "task_unit_cb_error" )
{
        asrt::task_unit< tu_error > u{ tu_error{ ctx } };

        asrt_test_input input{};
        input.test_ptr   = &u;
        input.continue_f = asrt::task_unit< tu_error >::cb;

        asrt_record rec{
            .state = ASRT_TEST_INIT,
            .inpt  = &input,
        };

        run_to_completion( u, rec );
        CHECK_EQ( ASRT_TEST_ERROR, rec.state );
}

TEST_CASE_FIXTURE( tu_cb_ctx, "task_unit_cb_multi_step_pass" )
{
        asrt::task_unit< tu_multi_pass > u{ tu_multi_pass{ ctx } };

        asrt_test_input input{};
        input.test_ptr   = &u;
        input.continue_f = asrt::task_unit< tu_multi_pass >::cb;

        asrt_record rec{
            .state = ASRT_TEST_INIT,
            .inpt  = &input,
        };

        // First cb call: sets RUNNING, starts coroutine
        asrt::task_unit< tu_multi_pass >::cb( &rec );
        CHECK_EQ( ASRT_TEST_RUNNING, rec.state );

        // Subsequent cb calls are no-ops (state already RUNNING)
        asrt::task_unit< tu_multi_pass >::cb( &rec );
        CHECK_EQ( ASRT_TEST_RUNNING, rec.state );

        // Drive with cb→tick pairs until the coroutine completes
        for ( int i = 0; i < 20; ++i ) {
                asrt::task_unit< tu_multi_pass >::cb( &rec );
                ctx.tick();
                if ( rec.state != ASRT_TEST_RUNNING )
                        break;
        }
        CHECK_EQ( ASRT_TEST_PASS, rec.state );
}

TEST_CASE_FIXTURE( tu_cb_ctx, "task_unit_cb_multi_step_fail" )
{
        asrt::task_unit< tu_multi_fail > u{ tu_multi_fail{ ctx } };

        asrt_test_input input{};
        input.test_ptr   = &u;
        input.continue_f = asrt::task_unit< tu_multi_fail >::cb;

        asrt_record rec{
            .state = ASRT_TEST_INIT,
            .inpt  = &input,
        };

        run_to_completion( u, rec );
        CHECK_EQ( ASRT_TEST_FAIL, rec.state );
}

TEST_CASE_FIXTURE( tu_cb_ctx, "task_unit_cb_multi_step_error" )
{
        asrt::task_unit< tu_multi_error > u{ tu_multi_error{ ctx } };

        asrt_test_input input{};
        input.test_ptr   = &u;
        input.continue_f = asrt::task_unit< tu_multi_error >::cb;

        asrt_record rec{
            .state = ASRT_TEST_INIT,
            .inpt  = &input,
        };

        run_to_completion( u, rec );
        CHECK_EQ( ASRT_TEST_ERROR, rec.state );
}

// cb is no-op when state is not INIT (coroutine already started)
TEST_CASE_FIXTURE( tu_cb_ctx, "task_unit_cb_noop_after_start" )
{
        asrt_record rec{
            .state = ASRT_TEST_INIT,
            .inpt  = nullptr,
        };

        asrt::task_unit< tu_multi_pass > u{ tu_multi_pass{ ctx } };

        asrt_test_input input{};
        input.test_ptr   = &u;
        input.continue_f = asrt::task_unit< tu_multi_pass >::cb;

        rec.inpt = &input;

        // Start the coroutine
        auto st = asrt::task_unit< tu_multi_pass >::cb( &rec );
        CHECK_EQ( ASRT_SUCCESS, st );
        CHECK_EQ( ASRT_TEST_RUNNING, rec.state );

        // Calling cb again should not restart — still RUNNING
        st = asrt::task_unit< tu_multi_pass >::cb( &rec );
        CHECK_EQ( ASRT_SUCCESS, st );
        CHECK_EQ( ASRT_TEST_RUNNING, rec.state );
}

TEST_CASE( "task_unit_releases_op_memory_immediate_completion" )
{
        uint8_t                                                      storage[2048]{};
        ecor::circular_buffer_memory< uint16_t >                     mem{ storage };
        asrt::task_ctx                                               ctx{ mem };
        std::vector< std::shared_ptr< asrt::task_unit< tu_pass > > > units;
        units.reserve( 32 );

        auto const baseline = mem.used_bytes();

        for ( int i = 0; i < 32; ++i ) {
                auto u = std::make_shared< asrt::task_unit< tu_pass > >( tu_pass{ ctx } );
                CHECK_EQ( ASRT_TEST_PASS, run_task_unit_once( ctx, *u ) );
                CHECK_EQ( baseline, mem.used_bytes() );
                units.push_back( std::move( u ) );
                CHECK_EQ( baseline, mem.used_bytes() );
        }
}

TEST_CASE( "task_unit_releases_op_memory_async_completion" )
{
        uint8_t                                                            storage[2048]{};
        ecor::circular_buffer_memory< uint16_t >                           mem{ storage };
        asrt::task_ctx                                                     ctx{ mem };
        std::vector< std::shared_ptr< asrt::task_unit< tu_multi_pass > > > units;
        units.reserve( 32 );

        auto const baseline = mem.used_bytes();

        for ( int i = 0; i < 32; ++i ) {
                auto u =
                    std::make_shared< asrt::task_unit< tu_multi_pass > >( tu_multi_pass{ ctx } );
                CHECK_EQ( ASRT_TEST_PASS, run_task_unit_once( ctx, *u ) );
                CHECK_EQ( baseline, mem.used_bytes() );
                units.push_back( std::move( u ) );
                CHECK_EQ( baseline, mem.used_bytes() );
        }
}

namespace
{
// --- task_unit: reactor integration ---

struct tu_e2e_ctx
{
        asrt::malloc_free_memory_resource mem;
        asrt::task_ctx                    ctx{ mem };
        collector                         coll;
        asrt_send_req_list                sq = {};
        asrt_reactor                      r;

        std::shared_ptr< asrt::task_unit< tu_pass > >       t0;
        std::shared_ptr< asrt::task_unit< tu_fail > >       t1;
        std::shared_ptr< asrt::task_unit< tu_error > >      t2;
        std::shared_ptr< asrt::task_unit< tu_multi_pass > > t3;

        tu_e2e_ctx()
          : t0( std::make_shared< asrt::task_unit< tu_pass > >( tu_pass{ ctx } ) )
          , t1( std::make_shared< asrt::task_unit< tu_fail > >( tu_fail{ ctx } ) )
          , t2( std::make_shared< asrt::task_unit< tu_error > >( tu_error{ ctx } ) )
          , t3( std::make_shared< asrt::task_unit< tu_multi_pass > >( tu_multi_pass{ ctx } ) )
        {
                if ( asrt::init( r, sq, "task_reactor" ) != ASRT_SUCCESS )
                        throw std::runtime_error( "reactor init failed" );
                if ( asrt::add_test( r, *t0 ) != ASRT_SUCCESS )
                        throw std::runtime_error( "add_test failed" );
                if ( asrt::add_test( r, *t1 ) != ASRT_SUCCESS )
                        throw std::runtime_error( "add_test failed" );
                if ( asrt::add_test( r, *t2 ) != ASRT_SUCCESS )
                        throw std::runtime_error( "add_test failed" );
                if ( asrt::add_test( r, *t3 ) != ASRT_SUCCESS )
                        throw std::runtime_error( "add_test failed" );
        }

        ~tu_e2e_ctx()
        {
                CHECK( coll.data.empty() );
                asrt::deinit( r );
        }

        void run( uint16_t test_id, uint32_t run_id )
        {
                struct asrt_u8d8msg   msg = {};
                struct asrt_send_req* req = asrt_msg_ctor_test_start( &msg, test_id, run_id );
                uint8_t               flat[8];
                memcpy( flat, req->buff.b, (size_t) ( req->buff.e - req->buff.b ) );
                CHECK_EQ(
                    ASRT_SUCCESS,
                    asrt::recv(
                        asrt::node( r ),
                        asrt_span{ .b = flat, .e = flat + ( req->buff.e - req->buff.b ) } ) );
                for ( int i = 0; i < 50; i++ ) {
                        ctx.tick();
                        CHECK_EQ( ASRT_SUCCESS, asrt::tick( asrt::node( r ), 0 ) );
                        drain( &sq, &coll );
                }
        }
};
}  // namespace

TEST_CASE_FIXTURE( tu_e2e_ctx, "task_unit_e2e_pass" )
{
        run( 0, 100 );
        REQUIRE_EQ( 2U, coll.data.size() );
        assert_test_result_msg( coll.data.back(), 100, ASRT_TEST_RESULT_SUCCESS );
        coll.data.pop_back();
        assert_test_start_msg( coll.data.back(), 0, 100 );
        coll.data.pop_back();
}

TEST_CASE_FIXTURE( tu_e2e_ctx, "task_unit_e2e_fail" )
{
        run( 1, 200 );
        REQUIRE_EQ( 2U, coll.data.size() );
        assert_test_result_msg( coll.data.back(), 200, ASRT_TEST_RESULT_FAILURE );
        coll.data.pop_back();
        assert_test_start_msg( coll.data.back(), 1, 200 );
        coll.data.pop_back();
}

TEST_CASE_FIXTURE( tu_e2e_ctx, "task_unit_e2e_error" )
{
        run( 2, 300 );
        REQUIRE_EQ( 2U, coll.data.size() );
        assert_test_result_msg( coll.data.back(), 300, ASRT_TEST_RESULT_ERROR );
        coll.data.pop_back();
        assert_test_start_msg( coll.data.back(), 2, 300 );
        coll.data.pop_back();
}

TEST_CASE_FIXTURE( tu_e2e_ctx, "task_unit_e2e_multi_step" )
{
        run( 3, 400 );
        REQUIRE_EQ( 2U, coll.data.size() );
        assert_test_result_msg( coll.data.back(), 400, ASRT_TEST_RESULT_SUCCESS );
        coll.data.pop_back();
        assert_test_start_msg( coll.data.back(), 3, 400 );
        coll.data.pop_back();
}

// ---------------------------------------------------------------------------
// param_query_sender — param() and find() free functions
namespace
{

// Test fixture: cross-wired param loopback with task_ctx for coroutine tests.
// The sender under test (param_query_sender) is async — the callback fires
// during cli.tick() when the server response arrives.  For coroutine tests
// we also need to tick the ecor task_core so that co_await resumes.
struct param_sender_ctx
{
        asrt_send_req_list srv_sq   = {};
        asrt_send_req_list cli_sq   = {};
        asrt_node          srv_head = {};
        asrt_node          cli_head = {};

        asrt_param_server srv;

        static constexpr uint32_t buff_size          = 256;
        uint8_t                   cli_buf[buff_size] = {};
        asrt_param_client         cli;

        asrt::malloc_free_memory_resource mem;
        asrt::task_ctx                    tctx{ mem };
        uint32_t                          t = 1;

        param_sender_ctx()
        {
                srv_head.chid       = ASRT_CORE;
                cli_head.chid       = ASRT_CORE;
                srv_head.send_queue = &srv_sq;
                cli_head.send_queue = &cli_sq;

                if ( asrt::init( srv, srv_head, asrt_default_allocator() ) != ASRT_SUCCESS )
                        throw std::runtime_error( "server init failed" );
                if ( asrt::init(
                         cli,
                         cli_head,
                         asrt_span{ .b = cli_buf, .e = cli_buf + buff_size },
                         100 ) != ASRT_SUCCESS )
                        throw std::runtime_error( "client init failed" );
        }

        ~param_sender_ctx()
        {
                asrt::deinit( cli );
                asrt::deinit( srv );
        }

        void setup_tree_and_handshake( asrt_flat_tree* tree )
        {
                asrt::set_tree( srv, *tree );
                CHECK_EQ(
                    ASRT_SUCCESS, asrt::send_ready( srv, 1U, { ready_ack_noop, nullptr }, 1000 ) );
                for ( int i = 0; i < 100; i++ ) {
                        CHECK_EQ( ASRT_SUCCESS, asrt::tick( asrt::node( srv ), t++ ) );
                        deliver_req( &srv_sq, &cli_head );
                        CHECK_EQ( ASRT_SUCCESS, asrt::tick( asrt::node( cli ), t++ ) );
                        deliver_req( &cli_sq, &srv_head );
                        if ( asrt::ready( cli ) )
                                break;
                }
                REQUIRE( asrt::ready( cli ) );
        }

        void spin( int max_iter = 200 )
        {
                for ( int i = 0; i < max_iter; i++ ) {
                        CHECK_EQ( ASRT_SUCCESS, asrt::tick( asrt::node( srv ), t++ ) );
                        deliver_req( &srv_sq, &cli_head );
                        CHECK_EQ( ASRT_SUCCESS, asrt::tick( asrt::node( cli ), t++ ) );
                        deliver_req( &cli_sq, &srv_head );
                        tctx.tick();
                }
        }

        // Run a task<void> to completion, return the final test state.
        // F should be a callable that takes (task_ctx&) and returns task<void>.
        template < typename F >
        asrt_test_state run_task( F&& make_task )
        {
                struct test_recv
                {
                        using receiver_concept = ecor::receiver_t;
                        asrt_test_state* out;

                        void set_value() { *out = ASRT_TEST_PASS; }
                        void set_error( asrt::status s )
                        {
                                *out = ( s == ASRT_FAILURE ) ? ASRT_TEST_FAIL : ASRT_TEST_ERROR;
                        }
                        void set_stopped() { *out = ASRT_TEST_FAIL; }
                };
                asrt_test_state result = ASRT_TEST_INIT;
                auto            op     = make_task( tctx ).connect( test_recv{ &result } );
                op.start();
                for ( int i = 0; i < 200 && result == ASRT_TEST_INIT; i++ ) {
                        CHECK_EQ( ASRT_SUCCESS, asrt::tick( asrt::node( srv ), t++ ) );
                        deliver_req( &srv_sq, &cli_head );
                        CHECK_EQ( ASRT_SUCCESS, asrt::tick( asrt::node( cli ), t++ ) );
                        deliver_req( &cli_sq, &srv_head );
                        tctx.tick();
                }
                return result;
        }
};

// Free-function coroutines (ecor requires task_ctx& as first arg, no lambda coroutines).

template < typename T >
asrt::task< void > ps_do_fetch( asrt::task_ctx&, asrt_param_client& c, uint16_t id )
{
        asrt::param_result res = co_await asrt::fetch< T >( c, id );
        (void) res;  // exercises the fetch protocol path; result not verified in this helper
}

template < typename T >
asrt::task< void > ps_do_find(
    asrt::task_ctx&,
    asrt_param_client& c,
    uint16_t           parent,
    char const*        key )
{
        co_await asrt::find< T >( c, parent, key );
}

asrt::task< void > ps_capture_find_u32(
    asrt::task_ctx&,
    asrt_param_client& c,
    uint16_t           parent,
    char const*        key,
    uint32_t*          out )
{
        *out = co_await asrt::find< uint32_t >( c, parent, key );
}

asrt::task< void > ps_capture_find_str(
    asrt::task_ctx&,
    asrt_param_client& c,
    uint16_t           parent,
    char const*        key,
    std::string*       out )
{
        auto v = co_await asrt::find< char const* >( c, parent, key );
        *out   = v;
}

asrt::task< void > ps_find_u32_then_check(
    asrt::task_ctx&,
    asrt_param_client& c,
    uint16_t           parent,
    char const*        key,
    bool*              reached )
{
        auto v = co_await asrt::find< uint32_t >( c, parent, key );
        (void) v;
        *reached = true;
}

asrt::task< void > ps_sequential_finds(
    asrt::task_ctx&,
    asrt_param_client& c,
    uint16_t           parent,
    char const*        key_a,
    char const*        key_b,
    uint32_t*          sum )
{
        auto a = co_await asrt::find< uint32_t >( c, parent, key_a );
        auto b = co_await asrt::find< uint32_t >( c, parent, key_b );
        *sum   = a.value + b.value;
}

asrt::task< void > ps_second_fails(
    asrt::task_ctx&,
    asrt_param_client& c,
    uint16_t           parent,
    char const*        key_a,
    char const*        key_b,
    uint32_t*          first_val,
    bool*              reached )
{
        *first_val = co_await asrt::find< uint32_t >( c, parent, key_a );
        auto b     = co_await asrt::find< uint32_t >( c, parent, key_b );
        (void) b;
        *reached = true;
}

asrt::task< void > ps_fetch_void_capture_type(
    asrt::task_ctx&,
    asrt_param_client*    c,
    uint16_t              id,
    asrt_flat_value_type* out_type )
{
        asrt::param_result< void > res = co_await asrt::fetch( *c, id );
        *out_type                      = res.value.type;
}

asrt::task< void > ps_find_void_capture_type(
    asrt::task_ctx&,
    asrt_param_client*    c,
    uint16_t              parent,
    char const*           key,
    asrt_flat_value_type* out_type )
{
        asrt::param_result< void > res = co_await asrt::find( *c, parent, key );
        *out_type                      = res.value.type;
}
}  // namespace
// --- param<T>: happy paths ---

TEST_CASE_FIXTURE( param_sender_ctx, "ps_param_u32_happy" )
{
        asrt_flat_tree tree;
        asrt_flat_tree_init( &tree, asrt_default_allocator(), 4, 16 );
        asrt_flat_tree_append_scalar(
            &tree, 0, 1, nullptr, ASRT_FLAT_STYPE_U32, { .u32_val = 42 } );
        setup_tree_and_handshake( &tree );

        auto state = run_task( [&]( asrt::task_ctx& ctx ) {
                return ps_do_fetch< uint32_t >( ctx, cli, 1 );
        } );
        CHECK_EQ( ASRT_TEST_PASS, state );
        asrt_flat_tree_deinit( &tree );
}

TEST_CASE_FIXTURE( param_sender_ctx, "ps_param_i32_happy" )
{
        asrt_flat_tree tree;
        asrt_flat_tree_init( &tree, asrt_default_allocator(), 4, 16 );
        asrt_flat_tree_append_scalar(
            &tree, 0, 1, nullptr, ASRT_FLAT_STYPE_I32, { .i32_val = -5 } );
        setup_tree_and_handshake( &tree );

        auto state = run_task( [&]( asrt::task_ctx& ctx ) {
                return ps_do_fetch< int32_t >( ctx, cli, 1 );
        } );
        CHECK_EQ( ASRT_TEST_PASS, state );
        asrt_flat_tree_deinit( &tree );
}

// --- find<T>: happy paths ---

TEST_CASE_FIXTURE( param_sender_ctx, "ps_find_u32_happy" )
{
        asrt_flat_tree tree;
        asrt_flat_tree_init( &tree, asrt_default_allocator(), 4, 16 );
        asrt_flat_tree_append_cont( &tree, 0, 1, nullptr, ASRT_FLAT_CTYPE_OBJECT );
        asrt_flat_tree_append_scalar( &tree, 1, 2, "count", ASRT_FLAT_STYPE_U32, { .u32_val = 7 } );
        setup_tree_and_handshake( &tree );

        auto state = run_task( [&]( asrt::task_ctx& ctx ) {
                return ps_do_find< uint32_t >( ctx, cli, 1, "count" );
        } );
        CHECK_EQ( ASRT_TEST_PASS, state );
        asrt_flat_tree_deinit( &tree );
}

TEST_CASE_FIXTURE( param_sender_ctx, "ps_find_str_happy" )
{
        asrt_flat_tree tree;
        asrt_flat_tree_init( &tree, asrt_default_allocator(), 4, 16 );
        asrt_flat_tree_append_cont( &tree, 0, 1, nullptr, ASRT_FLAT_CTYPE_OBJECT );
        asrt_flat_tree_append_scalar(
            &tree, 1, 2, "msg", ASRT_FLAT_STYPE_STR, { .str_val = "hello" } );
        setup_tree_and_handshake( &tree );

        auto state = run_task( [&]( asrt::task_ctx& ctx ) {
                return ps_do_find< char const* >( ctx, cli, 1, "msg" );
        } );
        CHECK_EQ( ASRT_TEST_PASS, state );
        asrt_flat_tree_deinit( &tree );
}

TEST_CASE_FIXTURE( param_sender_ctx, "ps_find_float_happy" )
{
        asrt_flat_tree tree;
        asrt_flat_tree_init( &tree, asrt_default_allocator(), 4, 16 );
        asrt_flat_tree_append_cont( &tree, 0, 1, nullptr, ASRT_FLAT_CTYPE_OBJECT );
        asrt_flat_tree_append_scalar(
            &tree, 1, 2, "pi", ASRT_FLAT_STYPE_FLOAT, { .float_val = 3.14F } );
        setup_tree_and_handshake( &tree );

        auto state = run_task( [&]( asrt::task_ctx& ctx ) {
                return ps_do_find< float >( ctx, cli, 1, "pi" );
        } );
        CHECK_EQ( ASRT_TEST_PASS, state );
        asrt_flat_tree_deinit( &tree );
}

TEST_CASE_FIXTURE( param_sender_ctx, "ps_find_obj_happy" )
{
        asrt_flat_tree tree;
        asrt_flat_tree_init( &tree, asrt_default_allocator(), 4, 16 );
        asrt_flat_tree_append_cont( &tree, 0, 1, nullptr, ASRT_FLAT_CTYPE_OBJECT );
        asrt_flat_tree_append_cont( &tree, 1, 2, "sub", ASRT_FLAT_CTYPE_OBJECT );
        setup_tree_and_handshake( &tree );

        auto state = run_task( [&]( asrt::task_ctx& ctx ) {
                return ps_do_find< asrt::obj >( ctx, cli, 1, "sub" );
        } );
        CHECK_EQ( ASRT_TEST_PASS, state );
        asrt_flat_tree_deinit( &tree );
}

// --- error: type mismatch (callback fires with error_code) ---

TEST_CASE_FIXTURE( param_sender_ctx, "ps_param_type_mismatch" )
{
        asrt_flat_tree tree;
        asrt_flat_tree_init( &tree, asrt_default_allocator(), 4, 16 );
        asrt_flat_tree_append_scalar(
            &tree, 0, 1, nullptr, ASRT_FLAT_STYPE_STR, { .str_val = "nope" } );
        setup_tree_and_handshake( &tree );

        // Request u32 but node is a string → TYPE_MISMATCH → set_error
        auto state = run_task( [&]( asrt::task_ctx& ctx ) {
                return ps_do_fetch< uint32_t >( ctx, cli, 1 );
        } );
        CHECK_EQ( ASRT_TEST_ERROR, state );
        asrt_flat_tree_deinit( &tree );
}

TEST_CASE_FIXTURE( param_sender_ctx, "ps_find_type_mismatch" )
{
        asrt_flat_tree tree;
        asrt_flat_tree_init( &tree, asrt_default_allocator(), 4, 16 );
        asrt_flat_tree_append_cont( &tree, 0, 1, nullptr, ASRT_FLAT_CTYPE_OBJECT );
        asrt_flat_tree_append_scalar(
            &tree, 1, 2, "val", ASRT_FLAT_STYPE_STR, { .str_val = "nope" } );
        setup_tree_and_handshake( &tree );

        // Request u32 but "val" is a string → TYPE_MISMATCH
        auto state = run_task( [&]( asrt::task_ctx& ctx ) {
                return ps_do_find< uint32_t >( ctx, cli, 1, "val" );
        } );
        CHECK_EQ( ASRT_TEST_ERROR, state );
        asrt_flat_tree_deinit( &tree );
}

// --- error: key not found ---

TEST_CASE_FIXTURE( param_sender_ctx, "ps_find_key_not_found" )
{
        asrt_flat_tree tree;
        asrt_flat_tree_init( &tree, asrt_default_allocator(), 4, 16 );
        asrt_flat_tree_append_cont( &tree, 0, 1, nullptr, ASRT_FLAT_CTYPE_OBJECT );
        asrt_flat_tree_append_scalar( &tree, 1, 2, "val", ASRT_FLAT_STYPE_U32, { .u32_val = 5 } );
        setup_tree_and_handshake( &tree );

        auto state = run_task( [&]( asrt::task_ctx& ctx ) {
                return ps_do_find< uint32_t >( ctx, cli, 1, "missing" );
        } );
        CHECK_EQ( ASRT_TEST_ERROR, state );
        asrt_flat_tree_deinit( &tree );
}

// --- error: client not ready (find returns ASRT_ARG_ERR → immediate set_error) ---

TEST_CASE_FIXTURE( param_sender_ctx, "ps_param_not_ready" )
{
        // Don't handshake — client is not ready
        auto state = run_task( [&]( asrt::task_ctx& ctx ) {
                return ps_do_fetch< uint32_t >( ctx, cli, 1 );
        } );
        CHECK_EQ( ASRT_TEST_ERROR, state );
}

TEST_CASE_FIXTURE( param_sender_ctx, "ps_find_not_ready" )
{
        auto state = run_task( [&]( asrt::task_ctx& ctx ) {
                return ps_do_find< uint32_t >( ctx, cli, 1, "x" );
        } );
        CHECK_EQ( ASRT_TEST_ERROR, state );
}

// --- error: query already pending (second sender fails immediately) ---

TEST_CASE_FIXTURE( param_sender_ctx, "ps_param_query_pending" )
{
        asrt_flat_tree tree;
        asrt_flat_tree_init( &tree, asrt_default_allocator(), 4, 16 );
        asrt_flat_tree_append_cont( &tree, 0, 1, nullptr, ASRT_FLAT_CTYPE_OBJECT );
        asrt_flat_tree_append_scalar( &tree, 1, 2, "a", ASRT_FLAT_STYPE_U32, { .u32_val = 1 } );
        asrt_flat_tree_append_scalar( &tree, 1, 3, "b", ASRT_FLAT_STYPE_U32, { .u32_val = 2 } );
        setup_tree_and_handshake( &tree );

        // Start a raw query to occupy the pending slot
        asrt_param_query q  = {};
        auto             cb = []( asrt_param_client*, asrt_param_query*, uint32_t ) {};
        CHECK_EQ( ASRT_SUCCESS, asrt::fetch< uint32_t >( cli, &q, 2U, cb, nullptr ) );
        CHECK( asrt::query_pending( cli ) );

        // Now a param sender should fail because a query is already pending
        auto state = run_task( [&]( asrt::task_ctx& ctx ) {
                return ps_do_fetch< uint32_t >( ctx, cli, 3 );
        } );
        CHECK_EQ( ASRT_TEST_ERROR, state );

        // Drain the pending query
        spin();
        asrt_flat_tree_deinit( &tree );
}

// --- coroutine integration: value returned via co_await ---

TEST_CASE_FIXTURE( param_sender_ctx, "ps_param_value_in_coroutine" )
{
        asrt_flat_tree tree;
        asrt_flat_tree_init( &tree, asrt_default_allocator(), 4, 16 );
        asrt_flat_tree_append_cont( &tree, 0, 1, nullptr, ASRT_FLAT_CTYPE_OBJECT );
        asrt_flat_tree_append_scalar(
            &tree, 1, 2, "count", ASRT_FLAT_STYPE_U32, { .u32_val = 42 } );
        setup_tree_and_handshake( &tree );

        uint32_t captured = 0;
        auto     state    = run_task( [&]( asrt::task_ctx& ctx ) {
                return ps_capture_find_u32( ctx, cli, 1, "count", &captured );
        } );
        CHECK_EQ( ASRT_TEST_PASS, state );
        CHECK_EQ( 42U, captured );
        asrt_flat_tree_deinit( &tree );
}

TEST_CASE_FIXTURE( param_sender_ctx, "ps_find_value_in_coroutine" )
{
        asrt_flat_tree tree;
        asrt_flat_tree_init( &tree, asrt_default_allocator(), 4, 16 );
        asrt_flat_tree_append_cont( &tree, 0, 1, nullptr, ASRT_FLAT_CTYPE_OBJECT );
        asrt_flat_tree_append_scalar(
            &tree, 1, 2, "msg", ASRT_FLAT_STYPE_STR, { .str_val = "hi" } );
        setup_tree_and_handshake( &tree );

        std::string captured;
        auto        state = run_task( [&]( asrt::task_ctx& ctx ) {
                return ps_capture_find_str( ctx, cli, 1, "msg", &captured );
        } );
        CHECK_EQ( ASRT_TEST_PASS, state );
        CHECK_EQ( "hi", captured );
        asrt_flat_tree_deinit( &tree );
}

// --- coroutine integration: error propagates and aborts the task ---

TEST_CASE_FIXTURE( param_sender_ctx, "ps_error_propagates_in_coroutine" )
{
        asrt_flat_tree tree;
        asrt_flat_tree_init( &tree, asrt_default_allocator(), 4, 16 );
        asrt_flat_tree_append_cont( &tree, 0, 1, nullptr, ASRT_FLAT_CTYPE_OBJECT );
        asrt_flat_tree_append_scalar(
            &tree, 1, 2, "val", ASRT_FLAT_STYPE_STR, { .str_val = "nope" } );
        setup_tree_and_handshake( &tree );

        bool reached = false;
        auto state   = run_task( [&]( asrt::task_ctx& ctx ) {
                return ps_find_u32_then_check( ctx, cli, 1, "val", &reached );
        } );
        CHECK_EQ( ASRT_TEST_ERROR, state );
        CHECK_FALSE( reached );
        asrt_flat_tree_deinit( &tree );
}

// --- coroutine integration: sequential co_awaits ---

TEST_CASE_FIXTURE( param_sender_ctx, "ps_sequential_queries_in_coroutine" )
{
        asrt_flat_tree tree;
        asrt_flat_tree_init( &tree, asrt_default_allocator(), 4, 16 );
        asrt_flat_tree_append_cont( &tree, 0, 1, nullptr, ASRT_FLAT_CTYPE_OBJECT );
        asrt_flat_tree_append_scalar( &tree, 1, 2, "a", ASRT_FLAT_STYPE_U32, { .u32_val = 10 } );
        asrt_flat_tree_append_scalar( &tree, 1, 3, "b", ASRT_FLAT_STYPE_U32, { .u32_val = 20 } );
        setup_tree_and_handshake( &tree );

        uint32_t sum   = 0;
        auto     state = run_task( [&]( asrt::task_ctx& ctx ) {
                return ps_sequential_finds( ctx, cli, 1, "a", "b", &sum );
        } );
        CHECK_EQ( ASRT_TEST_PASS, state );
        CHECK_EQ( 30U, sum );
        asrt_flat_tree_deinit( &tree );
}

// --- coroutine integration: second query fails, first succeeded ---

TEST_CASE_FIXTURE( param_sender_ctx, "ps_second_query_fails_in_coroutine" )
{
        asrt_flat_tree tree;
        asrt_flat_tree_init( &tree, asrt_default_allocator(), 4, 16 );
        asrt_flat_tree_append_cont( &tree, 0, 1, nullptr, ASRT_FLAT_CTYPE_OBJECT );
        asrt_flat_tree_append_scalar( &tree, 1, 2, "a", ASRT_FLAT_STYPE_U32, { .u32_val = 10 } );
        setup_tree_and_handshake( &tree );

        uint32_t first_val = 0;
        bool     reached   = false;
        auto     state     = run_task( [&]( asrt::task_ctx& ctx ) {
                return ps_second_fails( ctx, cli, 1, "a", "missing", &first_val, &reached );
        } );
        CHECK_EQ( ASRT_TEST_ERROR, state );
        CHECK_EQ( 10U, first_val );
        CHECK_FALSE( reached );
        asrt_flat_tree_deinit( &tree );
}

// --- typeless fetch / find (no explicit T, returns param_result<void>) ---

TEST_CASE_FIXTURE( param_sender_ctx, "ps_fetch_void_u32_happy" )
{
        asrt_flat_tree tree;
        asrt_flat_tree_init( &tree, asrt_default_allocator(), 4, 16 );
        asrt_flat_tree_append_scalar(
            &tree, 0, 1, nullptr, ASRT_FLAT_STYPE_U32, { .u32_val = 99 } );
        setup_tree_and_handshake( &tree );

        asrt_flat_value_type out_type = 0;
        auto                 state    = run_task( [&]( asrt::task_ctx& ctx ) {
                return ps_fetch_void_capture_type( ctx, &cli, 1, &out_type );
        } );
        CHECK_EQ( ASRT_TEST_PASS, state );
        CHECK_EQ( static_cast< asrt_flat_value_type >( ASRT_FLAT_STYPE_U32 ), out_type );
        asrt_flat_tree_deinit( &tree );
}

TEST_CASE_FIXTURE( param_sender_ctx, "ps_find_void_str_happy" )
{
        asrt_flat_tree tree;
        asrt_flat_tree_init( &tree, asrt_default_allocator(), 4, 16 );
        asrt_flat_tree_append_cont( &tree, 0, 1, nullptr, ASRT_FLAT_CTYPE_OBJECT );
        asrt_flat_tree_append_scalar(
            &tree, 1, 2, "tag", ASRT_FLAT_STYPE_STR, { .str_val = "hello" } );
        setup_tree_and_handshake( &tree );

        asrt_flat_value_type out_type = 0;
        auto                 state    = run_task( [&]( asrt::task_ctx& ctx ) {
                return ps_find_void_capture_type( ctx, &cli, 1, "tag", &out_type );
        } );
        CHECK_EQ( ASRT_TEST_PASS, state );
        CHECK_EQ( static_cast< asrt_flat_value_type >( ASRT_FLAT_STYPE_STR ), out_type );
        asrt_flat_tree_deinit( &tree );
}

// ---------------------------------------------------------------------------
// collect_client (C++ wrapper)
// ---------------------------------------------------------------------------

namespace
{

inline asrt_status inject_collect_msg( asrt_node* n, uint8_t* b, uint8_t* e )
{
        return asrt_chann_recv( n, ( asrt_span ){ .b = b, .e = e } );
}

inline uint8_t* make_coll_ready(
    uint8_t*      buf,
    asrt::flat_id root_id,
    asrt::flat_id next_node_id = 1 )
{
        uint8_t* p = buf;
        *p++       = ASRT_COLLECT_MSG_READY;
        asrt_add_u32( &p, root_id );
        asrt_add_u32( &p, next_node_id );
        return p;
}

struct collect_cpp_ctx
{
        collector           coll;
        asrt_send_req_list  sq = {};
        asrt_node           head{};
        asrt_collect_client cc;

        collect_cpp_ctx()
        {
                head.send_queue = &sq;
                if ( asrt::init( cc, head ) != ASRT_SUCCESS )
                        throw std::runtime_error( "collect_client init failed" );
        }

        ~collect_cpp_ctx()
        {
                coll.data.clear();
                asrt::deinit( cc );
        }

        void make_active( asrt::flat_id root_id = 1U )
        {
                uint8_t buf[16];
                REQUIRE_EQ(
                    ASRT_SUCCESS,
                    inject_collect_msg( &asrt::node( cc ), buf, make_coll_ready( buf, root_id ) ) );
                REQUIRE_EQ( ASRT_SUCCESS, asrt::tick( asrt::node( cc ), 0 ) );
                drain_send_queue( &sq, &coll );
                coll.data.clear();
        }
};
}  // namespace

TEST_CASE_FIXTURE( collect_cpp_ctx, "collect_cpp_handshake" )
{
        uint8_t buf[16];
        CHECK_EQ(
            ASRT_SUCCESS,
            inject_collect_msg( &asrt::node( cc ), buf, make_coll_ready( buf, 42U ) ) );

        CHECK_EQ( ASRT_SUCCESS, asrt::tick( asrt::node( cc ), 0 ) );
        drain_send_queue( &sq, &coll );
        CHECK_EQ( 42U, asrt::root_id( cc ) );

        REQUIRE_EQ( 1U, coll.data.size() );
        CHECK_EQ( ASRT_COLL, coll.data.front().id );
        CHECK_EQ( ASRT_COLLECT_MSG_READY_ACK, coll.data.front().data[0] );
}

TEST_CASE_FIXTURE( collect_cpp_ctx, "collect_cpp_set_and_append_all_types" )
{
        make_active();

        asrt::flat_id obj = 0;
        CHECK_EQ( ASRT_SUCCESS, asrt::set< asrt::obj >( cc, 0, "root", obj, {} ) );
        drain_send_queue( &sq, &coll );
        CHECK_NE( 0U, obj );

        asrt::flat_id arr = 0;
        CHECK_EQ( ASRT_SUCCESS, asrt::set< asrt::arr >( cc, obj, "items", arr, {} ) );
        drain_send_queue( &sq, &coll );
        CHECK_NE( 0U, arr );

        CHECK_EQ( ASRT_SUCCESS, asrt::set< uint32_t >( cc, obj, "count", 42, {} ) );
        drain_send_queue( &sq, &coll );
        CHECK_EQ( ASRT_SUCCESS, asrt::set< int32_t >( cc, obj, "offset", -7, {} ) );
        drain_send_queue( &sq, &coll );
        CHECK_EQ( ASRT_SUCCESS, asrt::set< char const* >( cc, obj, "name", "hello", {} ) );
        drain_send_queue( &sq, &coll );
        CHECK_EQ( ASRT_SUCCESS, asrt::set< bool >( cc, obj, "flag", true, {} ) );
        drain_send_queue( &sq, &coll );
        CHECK_EQ( ASRT_SUCCESS, asrt::set< float >( cc, obj, "ratio", 3.14F, {} ) );
        drain_send_queue( &sq, &coll );

        // 1 object + 1 array + 5 scalars = 7 messages
        CHECK_EQ( 7U, coll.data.size() );
}

TEST_CASE_FIXTURE( collect_cpp_ctx, "collect_cpp_set_u32d2" )
{
        make_active();

        asrt::flat_id obj_id = 0;
        CHECK_EQ( ASRT_SUCCESS, asrt::set< asrt::obj >( cc, 0, "root", obj_id, {} ) );
        drain_send_queue( &sq, &coll );

        asrt::u32d2 ts{ .hi = 0xDEADBEEFU, .lo = 0xCAFEBABEU };
        CHECK_EQ( ASRT_SUCCESS, asrt::set< asrt::u32d2 >( cc, obj_id, "ts", ts, {} ) );
        drain_send_queue( &sq, &coll );

        // 1 object + 1 u32d2 scalar = 2 messages
        CHECK_EQ( 2U, coll.data.size() );
}

// ---------------------------------------------------------------------------
// collect_sender (coroutine support)
// ---------------------------------------------------------------------------
namespace
{
asrt::task< void > cs_do_set_scalar(
    asrt::task_ctx&,
    asrt_collect_client& cc,
    asrt::flat_id        parent )
{
        co_await asrt::set( cc, parent, "count", 42 );
        co_await asrt::set( cc, parent, "offset", -7 );
        co_await asrt::set( cc, parent, "name", "hello" );
        co_await asrt::set( cc, parent, "flag", true );
        co_await asrt::set( cc, parent, "ratio", 3.14F );
}

asrt::task< void > cs_do_append_tree( asrt::task_ctx&, asrt_collect_client& cc )
{
        auto root = co_await asrt::set< asrt::obj >( cc, 0, "root" );
        auto arr  = co_await asrt::set< asrt::arr >( cc, root, "items" );
        co_await asrt::append( cc, arr, uint32_t( 10 ) );
        co_await asrt::append( cc, arr, uint32_t( 20 ) );
        co_await asrt::set( cc, root, "label", "test" );
}

struct collect_sender_ctx : collect_cpp_ctx
{
        asrt::malloc_free_memory_resource mem;
        asrt::task_ctx                    tctx{ mem };

        template < typename F >
        asrt_test_state run_task( F&& make_task )
        {
                struct test_recv
                {
                        using receiver_concept = ecor::receiver_t;
                        asrt_test_state* out;

                        void set_value() { *out = ASRT_TEST_PASS; }
                        void set_error( asrt::status ) { *out = ASRT_TEST_FAIL; }
                        void set_stopped() { *out = ASRT_TEST_FAIL; }
                };
                asrt_test_state result = ASRT_TEST_INIT;
                auto            op     = make_task( tctx ).connect( test_recv{ &result } );
                op.start();
                for ( int i = 0; i < 100 && result == ASRT_TEST_INIT; i++ ) {
                        tctx.tick();
                        drain_send_queue( &sq, &coll );
                }
                return result;
        }
};
}  // namespace
TEST_CASE_FIXTURE( collect_sender_ctx, "cs_set_scalars" )
{
        make_active();
        auto state = run_task( [&]( asrt::task_ctx& ctx ) {
                return cs_do_set_scalar( ctx, cc, 0 );
        } );
        CHECK_EQ( ASRT_TEST_PASS, state );
        CHECK_EQ( 5U, coll.data.size() );
}

TEST_CASE_FIXTURE( collect_sender_ctx, "cs_append_tree" )
{
        make_active();
        auto state = run_task( [&]( asrt::task_ctx& ctx ) {
                return cs_do_append_tree( ctx, cc );
        } );
        CHECK_EQ( ASRT_TEST_PASS, state );
        // 1 obj + 1 arr + 2 u32 + 1 str = 5 messages
        CHECK_EQ( 5U, coll.data.size() );
}

// =====================================================================
// stream C++ wrapper tests
// =====================================================================

// ---------------------------------------------------------------------------
// helpers
namespace
{

struct strm_cpp_ctx
{
        asrt_send_req_list sq_r = {};
        asrt_send_req_list sq_c = {};

        asrt_node root_r = { .chid = ASRT_CORE };
        asrt_node root_c = { .chid = ASRT_CORE };

        asrt_stream_client client;
        asrt_stream_server server;

        strm_cpp_ctx()
        {
                root_r.send_queue = &sq_r;
                root_c.send_queue = &sq_c;

                if ( asrt::init( client, root_r ) != ASRT_SUCCESS )
                        throw std::runtime_error( "client init failed" );
                if ( asrt::init( server, root_c, asrt_default_allocator() ) != ASRT_SUCCESS )
                        throw std::runtime_error( "server init failed" );
        }

        ~strm_cpp_ctx()
        {
                asrt::deinit( client );
                asrt::deinit( server );
        }

        /// Tick the stream client once, then deliver client→server and server→client.
        /// This is needed because in the new send-queue model the application is responsible
        /// for draining the queue; a single tick_client() call replaces the old synchronous
        /// loopback sender that used to deliver inline.
        void tick_client( uint32_t t = 0 )
        {
                CHECK_EQ( ASRT_SUCCESS, asrt::tick( asrt::node( client ), t ) );
                deliver_req( &sq_r, &root_c );
                CHECK_EQ( ASRT_SUCCESS, asrt::tick( asrt::node( client ), t ) );
                deliver_req( &sq_c, &root_r );
        }
};

}  // namespace
// --- basic C++ wrapper ---

TEST_CASE_FIXTURE( strm_cpp_ctx, "strm_cpp: define + tick cycle" )
{
        uint8_t     fields[] = { ASRT_STRM_FIELD_U8 };
        asrt_status done_st;
        auto        done_cb = []( void* p, enum asrt_status s ) {
                *static_cast< asrt_status* >( p ) = s;
        };
        CHECK_EQ(
            ASRT_SUCCESS,
            asrt::define( client, 0, fields, sizeof( fields ), 1, { done_cb, &done_st } ) );
        tick_client();
        CHECK_EQ( ASRT_SUCCESS, done_st );
}

TEST_CASE_FIXTURE( strm_cpp_ctx, "strm_cpp: record via wrapper" )
{
        uint8_t fields[] = { ASRT_STRM_FIELD_U8 };
        CHECK_EQ( ASRT_SUCCESS, asrt::define( client, 0, fields, sizeof( fields ), 1, {} ) );
        tick_client();

        uint8_t data[] = { 99 };
        CHECK_EQ( ASRT_SUCCESS, asrt::emit( client, 0, data, 1, {} ) );
        tick_client();

        auto result = asrt::take( server );
        REQUIRE_EQ( 1U, result->schema_count );
        CHECK_EQ( 1U, result->schemas[0].count );
        CHECK_EQ( 99, result->schemas[0].first->data[0] );
}

TEST_CASE_FIXTURE( strm_cpp_ctx, "strm_cpp: reset via wrapper" )
{
        CHECK_EQ( ASRT_SUCCESS, asrt::reset( client ) );
}

// --- stream_schema typed wrapper ---

TEST_CASE_FIXTURE( strm_cpp_ctx, "strm_schema: u8 define + emit" )
{
        asrt_status done_st;
        auto        done_cb = []( void* p, enum asrt_status s ) {
                *static_cast< asrt_status* >( p ) = s;
        };
        asrt::stream_schema< uint8_t > schema( client, 0, { done_cb, &done_st } );
        tick_client();
        CHECK_EQ( ASRT_SUCCESS, done_st );

        CHECK_EQ( ASRT_SUCCESS, schema.emit( 42, {} ) );
        tick_client();

        auto result = asrt::take( server );
        REQUIRE_EQ( 1U, result->schema_count );
        CHECK_EQ( 1U, result->schemas[0].count );
        CHECK_EQ( 42, result->schemas[0].first->data[0] );
}

TEST_CASE_FIXTURE( strm_cpp_ctx, "strm_schema: u16 encoding" )
{
        asrt::stream_schema< uint16_t > schema( client, 0, {} );
        tick_client();
        CHECK_EQ( 2U, decltype( schema )::emit_size );

        CHECK_EQ( ASRT_SUCCESS, schema.emit( 0x1234, {} ) );
        tick_client();

        auto result = asrt::take( server );
        REQUIRE_EQ( 1U, result->schema_count );
        auto* rec = result->schemas[0].first;
        REQUIRE_NE( nullptr, rec );
        uint16_t val;
        asrt_u8d2_to_u16( rec->data, &val );
        CHECK_EQ( 0x1234, val );
}

TEST_CASE_FIXTURE( strm_cpp_ctx, "strm_schema: u32 encoding" )
{
        asrt::stream_schema< uint32_t > schema( client, 0, {} );
        tick_client();
        CHECK_EQ( 4U, decltype( schema )::emit_size );

        CHECK_EQ( ASRT_SUCCESS, schema.emit( 0xDEADBEEF, {} ) );
        tick_client();

        auto result = asrt::take( server );
        REQUIRE_EQ( 1U, result->schema_count );
        uint32_t val;
        asrt_u8d4_to_u32( result->schemas[0].first->data, &val );
        CHECK_EQ( 0xDEADBEEF, val );
}

TEST_CASE_FIXTURE( strm_cpp_ctx, "strm_schema: i8 encoding" )
{
        asrt::stream_schema< int8_t > schema( client, 0, {} );
        tick_client();

        CHECK_EQ( ASRT_SUCCESS, schema.emit( -42, {} ) );
        tick_client();

        auto result = asrt::take( server );
        REQUIRE_EQ( 1U, result->schema_count );
        CHECK_EQ( static_cast< uint8_t >( -42 ), result->schemas[0].first->data[0] );
}

TEST_CASE_FIXTURE( strm_cpp_ctx, "strm_schema: i16 encoding" )
{
        asrt::stream_schema< int16_t > schema( client, 0, {} );
        tick_client();

        CHECK_EQ( ASRT_SUCCESS, schema.emit( -1000, {} ) );
        tick_client();

        auto result = asrt::take( server );
        REQUIRE_EQ( 1U, result->schema_count );
        uint16_t raw;
        asrt_u8d2_to_u16( result->schemas[0].first->data, &raw );
        CHECK_EQ( static_cast< uint16_t >( static_cast< int16_t >( -1000 ) ), raw );
}

TEST_CASE_FIXTURE( strm_cpp_ctx, "strm_schema: i32 encoding" )
{
        asrt::stream_schema< int32_t > schema( client, 0, {} );
        tick_client();

        CHECK_EQ( ASRT_SUCCESS, schema.emit( -100000, {} ) );
        tick_client();

        auto result = asrt::take( server );
        REQUIRE_EQ( 1U, result->schema_count );
        // raw bytes match the two's complement representation
        uint32_t raw;
        asrt_u8d4_to_u32( result->schemas[0].first->data, &raw );
        CHECK_EQ( static_cast< uint32_t >( -100000 ), raw );
}

TEST_CASE_FIXTURE( strm_cpp_ctx, "strm_schema: float encoding" )
{
        asrt::stream_schema< float > schema( client, 0, {} );
        tick_client();
        CHECK_EQ( 4U, decltype( schema )::emit_size );

        CHECK_EQ( ASRT_SUCCESS, schema.emit( 3.14F, {} ) );
        tick_client();

        auto result = asrt::take( server );
        REQUIRE_EQ( 1U, result->schema_count );
        uint32_t raw;
        asrt_u8d4_to_u32( result->schemas[0].first->data, &raw );
        float restored;
        memcpy( &restored, &raw, 4 );
        CHECK_EQ( doctest::Approx( 3.14F ), restored );
}

TEST_CASE_FIXTURE( strm_cpp_ctx, "strm_schema: bool encoding" )
{
        asrt::stream_schema< bool > schema( client, 0, {} );
        tick_client();
        CHECK_EQ( 1U, decltype( schema )::emit_size );

        CHECK_EQ( ASRT_SUCCESS, schema.emit( true, {} ) );
        tick_client();
        CHECK_EQ( ASRT_SUCCESS, schema.emit( false, {} ) );
        tick_client();

        auto result = asrt::take( server );
        REQUIRE_EQ( 1U, result->schema_count );
        CHECK_EQ( 2U, result->schemas[0].count );
        CHECK_EQ( 1, result->schemas[0].first->data[0] );
        CHECK_EQ( 0, result->schemas[0].first->next->data[0] );
}

TEST_CASE_FIXTURE( strm_cpp_ctx, "strm_schema: multi-field emit" )
{
        asrt::stream_schema< uint8_t, uint32_t, bool > schema( client, 5, {} );
        tick_client();
        CHECK_EQ( 6U, decltype( schema )::emit_size );  // 1+4+1

        CHECK_EQ( ASRT_SUCCESS, schema.emit( 0xAB, 0x12345678, true, {} ) );
        tick_client();

        auto result = asrt::take( server );
        REQUIRE_EQ( 1U, result->schema_count );
        auto* rec = result->schemas[0].first;
        REQUIRE_NE( nullptr, rec );
        CHECK_EQ( 0xAB, rec->data[0] );
        uint32_t u32_val;
        asrt_u8d4_to_u32( rec->data + 1, &u32_val );
        CHECK_EQ( 0x12345678, u32_val );
        CHECK_EQ( 1, rec->data[5] );
}

TEST_CASE_FIXTURE( strm_cpp_ctx, "strm_schema: multiple emits streamed" )
{
        asrt::stream_schema< uint8_t > schema( client, 0, {} );
        tick_client();

        for ( uint8_t i = 0; i < 10; i++ ) {
                CHECK_EQ( ASRT_SUCCESS, schema.emit( i, {} ) );
                tick_client();
        }

        auto result = asrt::take( server );
        REQUIRE_EQ( 1U, result->schema_count );
        CHECK_EQ( 10U, result->schemas[0].count );

        auto* rec = result->schemas[0].first;
        for ( uint8_t i = 0; i < 10; i++ ) {
                REQUIRE_NE( nullptr, rec );
                CHECK_EQ( i, rec->data[0] );
                rec = rec->next;
        }
}

TEST_CASE_FIXTURE( strm_cpp_ctx, "strm_schema: multiple schemas on same client" )
{
        asrt::stream_schema< uint8_t > s0( client, 0, {} );
        tick_client();

        asrt::stream_schema< uint16_t > s1( client, 1, {} );
        tick_client();

        CHECK_EQ( ASRT_SUCCESS, s0.emit( 0xAA, {} ) );
        tick_client();
        CHECK_EQ( ASRT_SUCCESS, s1.emit( 0xBBCC, {} ) );
        tick_client();

        auto result = asrt::take( server );
        REQUIRE_EQ( 2U, result->schema_count );
        CHECK_EQ( 0, result->schemas[0].schema_id );
        CHECK_EQ( 1, result->schemas[1].schema_id );
        CHECK_EQ( 0xAA, result->schemas[0].first->data[0] );
        uint16_t v;
        asrt_u8d2_to_u16( result->schemas[1].first->data, &v );
        CHECK_EQ( 0xBBCC, v );
}

// --- server clear via C++ wrapper ---

TEST_CASE_FIXTURE( strm_cpp_ctx, "strm_cpp_server: clear" )
{
        uint8_t fields[] = { ASRT_STRM_FIELD_U8 };
        CHECK_EQ( ASRT_SUCCESS, asrt::define( client, 0, fields, sizeof( fields ), 1, {} ) );
        tick_client();
        uint8_t data[] = { 1 };
        CHECK_EQ( ASRT_SUCCESS, asrt::emit( client, 0, data, 1, {} ) );
        tick_client();

        asrt::clear( server );

        auto result = asrt::take( server );
        CHECK_EQ( 0U, result->schema_count );
}

// --- record done_cb tests ---

TEST_CASE_FIXTURE( strm_cpp_ctx, "strm_cpp: emit done_cb fires via tick" )
{
        uint8_t fields[] = { ASRT_STRM_FIELD_U8 };
        CHECK_EQ( ASRT_SUCCESS, asrt::define( client, 0, fields, sizeof( fields ), 1, {} ) );
        tick_client();

        asrt_status cb_status;
        auto        done_cb = []( void* p, enum asrt_status s ) {
                *static_cast< asrt_status* >( p ) = s;
        };
        uint8_t data[] = { 42 };
        CHECK_EQ( ASRT_SUCCESS, asrt::emit( client, 0, data, 1, { done_cb, &cb_status } ) );
        tick_client();
        CHECK_EQ( ASRT_SUCCESS, cb_status );

        auto result = asrt::take( server );
        REQUIRE_EQ( 1U, result->schema_count );
        CHECK_EQ( 42, result->schemas[0].first->data[0] );
}

TEST_CASE_FIXTURE( strm_cpp_ctx, "strm_cpp: emit with null done_cb succeeds" )
{
        uint8_t fields[] = { ASRT_STRM_FIELD_U8 };
        CHECK_EQ( ASRT_SUCCESS, asrt::define( client, 0, fields, sizeof( fields ), 1, {} ) );
        tick_client();

        uint8_t data[] = { 7 };
        CHECK_EQ( ASRT_SUCCESS, asrt::emit( client, 0, data, 1, {} ) );
        tick_client();

        auto result = asrt::take( server );
        REQUIRE_EQ( 1U, result->schema_count );
        CHECK_EQ( 7, result->schemas[0].first->data[0] );
}

TEST_CASE_FIXTURE( strm_cpp_ctx, "strm_schema: emit done_cb fires via tick" )
{
        asrt_status done_st;
        auto        done_cb = []( void* p, enum asrt_status s ) {
                *static_cast< asrt_status* >( p ) = s;
        };
        asrt::stream_schema< uint8_t > schema( client, 0, {} );
        tick_client();

        asrt_status rec_st;
        auto        rec_cb = []( void* p, enum asrt_status s ) {
                *static_cast< asrt_status* >( p ) = s;
        };
        CHECK_EQ( ASRT_SUCCESS, schema.emit( uint8_t( 55 ), { rec_cb, &rec_st } ) );
        tick_client();
        CHECK_EQ( ASRT_SUCCESS, rec_st );

        auto result = asrt::take( server );
        REQUIRE_EQ( 1U, result->schema_count );
        CHECK_EQ( 55, result->schemas[0].first->data[0] );
}

TEST_CASE_FIXTURE( strm_cpp_ctx, "strm_schema: multi-field emit done_cb" )
{
        asrt::stream_schema< uint8_t, uint16_t > schema( client, 0, {} );
        tick_client();

        asrt_status rec_st;
        auto        rec_cb = []( void* p, enum asrt_status s ) {
                *static_cast< asrt_status* >( p ) = s;
        };
        CHECK_EQ(
            ASRT_SUCCESS, schema.emit( uint8_t( 0xAB ), uint16_t( 0x1234 ), { rec_cb, &rec_st } ) );
        tick_client();
        CHECK_EQ( ASRT_SUCCESS, rec_st );

        auto result = asrt::take( server );
        REQUIRE_EQ( 1U, result->schema_count );
        auto* rec = result->schemas[0].first;
        CHECK_EQ( 0xAB, rec->data[0] );
        uint16_t val;
        asrt_u8d2_to_u16( rec->data + 1, &val );
        CHECK_EQ( 0x1234, val );
}

// --- T[N] typed wrapper ---

TEST_CASE( "strm_field_traits: uint32_t[5] size and field_def_size" )
{
        using af5 = uint32_t[5];
        CHECK_EQ( 20U, asrt::strm_field_traits< af5 >::size );
        CHECK_EQ( 4U, asrt::strm_field_traits< af5 >::field_def_size );
}

TEST_CASE( "strm_field_traits: nested uint8_t[4][3]" )
{
        using inner = uint8_t[4];
        using outer = inner[3];
        CHECK_EQ( 12U, asrt::strm_field_traits< outer >::size );
        CHECK_EQ( 7U, asrt::strm_field_traits< outer >::field_def_size );  // ARRAY+2+ARRAY+2+U8
}

TEST_CASE( "strm_schema: uint32_t[5] emit_size and field_def_bytes" )
{
        using schema_t = asrt::stream_schema< uint32_t[5] >;
        CHECK_EQ( 20U, schema_t::emit_size );
        REQUIRE_EQ( 4U, schema_t::field_def_bytes.size() );
        CHECK_EQ( (uint8_t) ASRT_STRM_FIELD_ARRAY, schema_t::field_def_bytes[0] );
        CHECK_EQ( 0x00, schema_t::field_def_bytes[1] );
        CHECK_EQ( 0x05, schema_t::field_def_bytes[2] );
        CHECK_EQ( (uint8_t) ASRT_STRM_FIELD_U32, schema_t::field_def_bytes[3] );
}

TEST_CASE_FIXTURE( strm_cpp_ctx, "strm_schema: uint32_t[3] define + emit" )
{
        using schema_t = asrt::stream_schema< uint32_t[3] >;
        schema_t schema( client, 0, {} );
        tick_client();

        CHECK_EQ( 12U, schema_t::emit_size );

        uint32_t vals[3] = { 10U, 20U, 30U };
        CHECK_EQ( ASRT_SUCCESS, schema.emit( vals, {} ) );
        tick_client();

        auto result = asrt::take( server );
        REQUIRE_EQ( 1U, result->schema_count );
        auto* rec = result->schemas[0].first;
        REQUIRE_NE( nullptr, rec );

        uint32_t v0, v1, v2;
        asrt_u8d4_to_u32( rec->data + 0, &v0 );
        asrt_u8d4_to_u32( rec->data + 4, &v1 );
        asrt_u8d4_to_u32( rec->data + 8, &v2 );
        CHECK_EQ( 10U, v0 );
        CHECK_EQ( 20U, v1 );
        CHECK_EQ( 30U, v2 );
}

TEST_CASE_FIXTURE( strm_cpp_ctx, "strm_schema: uint8_t + uint16_t[3] + bool" )
{
        using schema_t = asrt::stream_schema< uint8_t, uint16_t[3], bool >;
        schema_t schema( client, 0, {} );
        tick_client();

        CHECK_EQ( 8U, schema_t::emit_size );  // 1 + 6 + 1

        uint16_t uvs[3] = { 100U, 200U, 300U };
        CHECK_EQ( ASRT_SUCCESS, schema.emit( uint8_t( 0x42 ), uvs, bool( true ), {} ) );
        tick_client();

        auto result = asrt::take( server );
        REQUIRE_EQ( 1U, result->schema_count );
        auto* rec = result->schemas[0].first;
        REQUIRE_NE( nullptr, rec );

        CHECK_EQ( 0x42, rec->data[0] );
        uint16_t a, b, c;
        asrt_u8d2_to_u16( rec->data + 1, &a );
        asrt_u8d2_to_u16( rec->data + 3, &b );
        asrt_u8d2_to_u16( rec->data + 5, &c );
        CHECK_EQ( 100U, a );
        CHECK_EQ( 200U, b );
        CHECK_EQ( 300U, c );
        CHECK_EQ( 1, rec->data[7] );
}

TEST_CASE_FIXTURE( strm_cpp_ctx, "strm_schema: nested uint8_t[4][3]" )
{
        using inner_t  = uint8_t[4];
        using schema_t = asrt::stream_schema< inner_t[3] >;
        schema_t schema( client, 0, {} );
        tick_client();

        CHECK_EQ( 12U, schema_t::emit_size );

        using val_t = typename asrt::strm_field_traits< inner_t[3] >::value_type;
        val_t arr   = { { 1, 2, 3, 4 }, { 5, 6, 7, 8 }, { 9, 10, 11, 12 } };
        CHECK_EQ( ASRT_SUCCESS, schema.emit( arr, {} ) );
        tick_client();

        auto result = asrt::take( server );
        REQUIRE_EQ( 1U, result->schema_count );
        auto* rec = result->schemas[0].first;
        REQUIRE_NE( nullptr, rec );

        for ( uint8_t i = 0; i < 12; i++ )
                CHECK_EQ( (int) ( i + 1 ), (int) rec->data[i] );
}

// ---------------------------------------------------------------------------
// diag_rec_sender

namespace
{

struct diag_sender_ctx : diag_ctx
{
        asrt::malloc_free_memory_resource mem;
        asrt::task_ctx                    tctx{ mem };

        template < typename F >
        asrt_test_state run_task( F&& make_task )
        {
                struct test_recv
                {
                        using receiver_concept = ecor::receiver_t;
                        asrt_test_state* out;

                        void set_value() { *out = ASRT_TEST_PASS; }
                        void set_error( asrt::status s )
                        {
                                *out = ( s == ASRT_FAILURE ) ? ASRT_TEST_FAIL : ASRT_TEST_ERROR;
                        }
                        void set_stopped() { *out = ASRT_TEST_FAIL; }
                };
                asrt_test_state result = ASRT_TEST_INIT;
                auto            op     = make_task( tctx ).connect( test_recv{ &result } );
                op.start();
                for ( int i = 0; i < 50 && result == ASRT_TEST_INIT; i++ ) {
                        drain( &sq_d, &coll_d );
                        tctx.tick();
                }
                return result;
        }
};

}  // namespace

static asrt::task< void > do_diag_rec(
    asrt::task_ctx&,
    asrt_diag_client& d,
    char const*       file,
    uint32_t          line )
{
        co_await asrt::rec_diag( d, file, line, nullptr );
}

TEST_CASE_FIXTURE( diag_sender_ctx, "diag_rec_sender" )
{
        auto state = run_task( [&]( asrt::task_ctx& ctx ) {
                return do_diag_rec( ctx, d, "diag_sender_test.c", 77 );
        } );
        CHECK_EQ( ASRT_TEST_PASS, state );
        REQUIRE_EQ( 1U, coll_d.data.size() );
        assert_diag_record( coll_d.data.front(), 77 );
        coll_d.data.pop_front();
}

// ---------------------------------------------------------------------------
// reac_assm wrappers

TEST_CASE( "reac_assm_init_deinit" )
{
        asrt_reac_assm assm;
        CHECK_EQ( ASRT_SUCCESS, asrt::init( assm, "test_reactor", 500 ) );
        asrt::deinit( assm );
}

TEST_CASE( "reac_assm_add_test" )
{
        asrt_reac_assm          assm;
        asrt::unit< pass_test > t0;
        REQUIRE_EQ( ASRT_SUCCESS, asrt::init( assm, "test_reactor", 500 ) );
        CHECK_EQ( ASRT_SUCCESS, asrt::add_test( assm, t0 ) );
        asrt::deinit( assm );
}

TEST_CASE( "reac_assm_tick_does_not_crash" )
{
        asrt_reac_assm assm;
        REQUIRE_EQ( ASRT_SUCCESS, asrt::init( assm, "test_reactor", 500 ) );
        asrt::tick( assm, 1 );
        asrt::tick( assm, 2 );
        asrt::deinit( assm );
}
