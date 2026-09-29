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
#include "../asrtl/core_proto.h"
#include "../asrtl/log.h"
#include "../asrtl/proto_version.h"
#include "../asrtlpp/flat_type_traits.hpp"
#include "../asrtr/collect.h"
#include "../asrtr/diag.h"
#include "../asrtr/param.h"
#include "../asrtr/reactor.h"
#include "./asrtr_tests.h"
#include "./collector.hpp"
#include "./util.hpp"

#include <algorithm>
#include <doctest/doctest.h>

static ASRT_DEFINE_GPOS_LOG()

    namespace
{

        //---------------------------------------------------------------------
        // lib

        void setup_test(
            struct asrt_reactor * r,
            struct asrt_test * t,
            char const*        name,
            void*              data,
            asrt_test_callback start_f )
        {
                assert( r );
                assert( t );
                enum asrt_status st = asrt_test_init( t, name, data, start_f );
                CHECK_EQ( ASRT_SUCCESS, st );
                st = asrt_reactor_add_test( r, t );
                CHECK_EQ( ASRT_SUCCESS, st );
        }

        void check_reactor_init(
            struct asrt_reactor * reac, struct asrt_send_req_list * send_queue, char const* desc )
        {
                enum asrt_status st = asrt_reactor_init( reac, send_queue, desc );
                CHECK_EQ( ASRT_SUCCESS, st );
        }

        void check_diag_init( struct asrt_diag_client * diag, struct asrt_node * prev )
        {
                enum asrt_status st = asrt_diag_client_init( diag, prev );
                CHECK_EQ( ASRT_SUCCESS, st );
        }

        void check_reactor_recv( struct asrt_reactor * reac, struct asrt_span msg )
        {
                enum asrt_status st = asrt_chann_recv( &reac->node, msg );
                CHECK_EQ( ASRT_SUCCESS, st );
        }

        void check_reactor_recv_flags(
            struct asrt_reactor * reac, struct asrt_span msg, uint32_t flags )
        {
                check_reactor_recv( reac, msg );
                CHECK_EQ( flags, reac->flags & ~ASRT_PASSIVE_FLAGS );
        }

        void check_reactor_tick( struct asrt_reactor * reac, collector * coll )
        {
                enum asrt_status st = asrt_chann_tick( &reac->node, 0 );
                CHECK_EQ( ASRT_SUCCESS, st );
                CHECK_EQ( 0x00, reac->flags & ~ASRT_PASSIVE_FLAGS );
                drain_send_queue( reac->node.send_queue, coll );
        }

        void check_recv_and_spin(
            struct asrt_reactor * reac,
            collector * coll,
            uint8_t * beg,
            uint8_t * end,
            enum asrt_reactor_flags fls )
        {
                check_reactor_recv_flags( reac, ( struct asrt_span ){ beg, end }, fls );
                int       i = 0;
                int const n = 1000;
                for ( ; i < n; i++ ) {
                        check_reactor_tick( reac, coll );
                        if ( reac->state == ASRT_REAC_IDLE )
                                break;
                }
                CHECK_NE( i, n );
        }

        void check_run_test(
            struct asrt_reactor * reac, collector * coll, uint32_t test_id, uint32_t run_id )
        {
                struct asrt_u8d8msg   msg = {};
                struct asrt_send_req* req = asrt_msg_ctor_test_start( &msg, test_id, run_id );
                check_recv_and_spin( reac, coll, req->buff.b, req->buff.e, ASRT_FLAG_TSTART );
        }

        void assert_diag_record_any_line( struct collected_data & collected )
        {
                assert_collected_diag_hdr( collected, ASRT_DIAG_MSG_RECORD );
                uint32_t line = 0;
                asrt_u8d4_to_u32( collected.data.data() + 1, &line );
                CHECK( line >= 1 );
                CHECK( collected.data.size() > 5 );
                auto* fn_begin = collected.data.data() + 5;
                auto* fn_end   = collected.data.data() + collected.data.size();
                CHECK( std::none_of( fn_begin, fn_end, []( uint8_t b ) {
                        return b == '\0';
                } ) );
        }

        void assert_diag_record( struct collected_data & collected, uint32_t line )
        {
                assert_collected_diag_hdr( collected, ASRT_DIAG_MSG_RECORD );
                assert_u32( line, collected.data.data() + 1 );
                CHECK( collected.data.size() > 5 );
                auto* fn_begin = collected.data.data() + 5;
                auto* fn_end   = collected.data.data() + collected.data.size();
                CHECK( std::none_of( fn_begin, fn_end, []( uint8_t b ) {
                        return b == '\0';
                } ) );
        }

        void assert_test_result(
            struct collected_data & collected, uint32_t id, enum asrt_test_result_e result )
        {
                assert_collected_core_hdr( collected, 0x08, ASRT_MSG_TEST_RESULT );
                assert_u32( id, collected.data.data() + 2 );
                assert_u16( result, collected.data.data() + 6 );
        }

        void assert_test_start(
            struct collected_data & collected, uint16_t test_id, uint32_t run_id )
        {
                assert_collected_core_hdr( collected, 0x08, ASRT_MSG_TEST_START );
                assert_u16( test_id, collected.data.data() + 2 );
                assert_u32( run_id, collected.data.data() + 4 );
        }


        struct reactor_ctx
        {
                struct asrt_reactor reac;
                collector           coll;
                asrt_send_req_list  reac_sq    = {};
                asrt_send_req_list  diag_sq    = {};
                uint8_t             buffer[64] = {};
                struct asrt_span    sp         = {};

                reactor_ctx() { sp = { buffer, buffer + sizeof buffer }; }
                ~reactor_ctx() { CHECK_EQ( coll.data.empty(), true ); }
        };

        //---------------------------------------------------------------------
        // tests

        enum asrt_status dataless_test_fun( struct asrt_record * x )
        {
                (void) x;
                return ASRT_SUCCESS;
        }
}

TEST_CASE_FIXTURE( reactor_ctx, "reactor_init" )
{
        enum asrt_status st;

        st = asrt_reactor_init( NULL, &reac_sq, "rec1" );
        CHECK_EQ( ASRT_INIT_ERR, st );

        st = asrt_reactor_init( &reac, &reac_sq, NULL );
        CHECK_EQ( ASRT_INIT_ERR, st );

        st = asrt_reactor_init( &reac, &reac_sq, "rec1" );
        CHECK_EQ( ASRT_SUCCESS, st );
        CHECK_EQ( reac.first_test, nullptr );
        CHECK_EQ( reac.node.chid, ASRT_CORE );
        CHECK_EQ( reac.state, ASRT_REAC_IDLE );

        struct asrt_test t1, t2;
        st = asrt_test_init( &t1, NULL, NULL, &dataless_test_fun );
        CHECK_EQ( st, ASRT_INIT_ERR );
        st = asrt_test_init( &t1, "test1", NULL, NULL );
        CHECK_EQ( st, ASRT_INIT_ERR );

        setup_test( &reac, &t1, "test1", NULL, &dataless_test_fun );
        setup_test( &reac, &t2, "test2", NULL, &dataless_test_fun );

        CHECK_EQ( t2.next, nullptr );
        CHECK_EQ( t1.next, &t2 );
        CHECK_EQ( reac.first_test, &t1 );
}

TEST_CASE_FIXTURE( reactor_ctx, "reactor_version" )
{
        check_reactor_init( &reac, &reac_sq, "rec1" );

        struct asrt_u8d2msg pv_cmd = {};
        auto*               pv_req = asrt_msg_ctor_proto_version( &pv_cmd );
        check_recv_and_spin( &reac, &coll, pv_req->buff.b, pv_req->buff.e, ASRT_FLAG_PROTO_VER );

        auto& collected = coll.data.back();
        assert_collected_core_hdr( collected, 0x08, ASRT_MSG_PROTO_VERSION );
        assert_u16( ASRT_PROTO_MAJOR, collected.data.data() + 2 );
        assert_u16( ASRT_PROTO_MINOR, collected.data.data() + 4 );
        assert_u16( ASRT_PROTO_PATCH, collected.data.data() + 6 );

        coll.data.pop_back();
}

TEST_CASE_FIXTURE( reactor_ctx, "reactor_desc" )
{
        check_reactor_init( &reac, &reac_sq, "rec1" );

        struct asrt_u8d2msg dc_cmd = {};
        auto*               dc_req = asrt_msg_ctor_desc( &dc_cmd );
        check_recv_and_spin( &reac, &coll, dc_req->buff.b, dc_req->buff.e, ASRT_FLAG_DESC );

        auto& collected = coll.data.back();
        assert_collected_core_hdr( collected, 0x06, ASRT_MSG_DESC );
        assert_data_ll_contain_str( "rec1", collected, 2 );

        coll.data.pop_back();
}

TEST_CASE_FIXTURE( reactor_ctx, "reactor_test_count" )
{
        check_reactor_init( &reac, &reac_sq, "rec1" );

        struct asrt_u8d2msg tc_cmd = {};
        auto*               tc_req = asrt_msg_ctor_test_count( &tc_cmd );
        check_recv_and_spin( &reac, &coll, tc_req->buff.b, tc_req->buff.e, ASRT_FLAG_TC );

        {
                auto& collected = coll.data.back();
                assert_collected_core_hdr( collected, 0x04, ASRT_MSG_TEST_COUNT );
                assert_u16( 0x00, collected.data.data() + 2 );
                coll.data.pop_back();
        }

        // re-init to add a test before any recv
        check_reactor_init( &reac, &reac_sq, "rec1" );
        struct asrt_test t1;
        setup_test( &reac, &t1, "test1", NULL, &dataless_test_fun );

        check_recv_and_spin( &reac, &coll, tc_req->buff.b, tc_req->buff.e, ASRT_FLAG_TC );

        {
                auto& collected = coll.data.back();
                assert_collected_core_hdr( collected, 0x04, ASRT_MSG_TEST_COUNT );
                assert_u16( 0x01, collected.data.data() + 2 );
                coll.data.pop_back();
        }
}

TEST_CASE_FIXTURE( reactor_ctx, "reactor_test_info" )
{
        check_reactor_init( &reac, &reac_sq, "rec1" );

        struct asrt_u8d4msg ti_cmd = {};
        auto*               ti_req = asrt_msg_ctor_test_info( &ti_cmd, 0 );
        check_recv_and_spin( &reac, &coll, ti_req->buff.b, ti_req->buff.e, ASRT_FLAG_TI );

        {
                auto& collected = coll.data.back();
                assert_collected_core_hdr( collected, 0x05, ASRT_MSG_TEST_INFO );
                assert_u16( 0x00, collected.data.data() + 2 );
                CHECK_EQ( ASRT_TEST_INFO_MISSING_TEST_ERR, collected.data[4] );
                CHECK_EQ( 5U, collected.data.size() );
                coll.data.pop_back();
        }

        // re-init to add a test before any recv
        check_reactor_init( &reac, &reac_sq, "rec1" );
        struct asrt_test t1;
        setup_test( &reac, &t1, "test1", NULL, &dataless_test_fun );

        check_recv_and_spin( &reac, &coll, ti_req->buff.b, ti_req->buff.e, ASRT_FLAG_TI );

        {
                auto& collected = coll.data.back();
                assert_collected_core_hdr( collected, 0x0A, ASRT_MSG_TEST_INFO );
                assert_u16( 0x00, collected.data.data() + 2 );
                CHECK_EQ( ASRT_TEST_INFO_SUCCESS, collected.data[4] );
                assert_data_ll_contain_str( "test1", collected, 5 );
                coll.data.pop_back();
        }
}

TEST_CASE_FIXTURE( reactor_ctx, "reactor_start" )
{
        check_reactor_init( &reac, &reac_sq, "rec1" );

        struct asrt_test       t1;
        struct insta_test_data data = { .state = ASRT_TEST_PASS, .counter = 0 };
        setup_test( &reac, &t1, "test1", &data, &insta_test_fun );

        // just run one test
        check_run_test( &reac, &coll, 0, 0 );
        CHECK_EQ( 1, data.counter );

        {
                auto& collected = coll.data.back();
                assert_test_result( collected, 0, ASRT_TEST_RESULT_SUCCESS );
                coll.data.pop_back();
        }

        {
                auto& collected = coll.data.back();
                assert_test_start( collected, 0, 0 );
                coll.data.pop_back();
        }

        {
                struct asrt_u8d8msg cmd42 = {};
                auto*               req42 = asrt_msg_ctor_test_start( &cmd42, 42, 0 );
                check_recv_and_spin( &reac, &coll, req42->buff.b, req42->buff.e, ASRT_FLAG_TSTART );
        }

        CHECK_EQ( 1, data.counter );
        {
                auto& collected = coll.data.back();
                assert_test_result( collected, 0, ASRT_TEST_RESULT_ERROR );
                coll.data.pop_back();
        }

        {
                auto& collected = coll.data.back();
                assert_test_start( collected, 42, 0 );
                coll.data.pop_back();
        }
}

TEST_CASE_FIXTURE( reactor_ctx, "reactor_start_busy" )
{
        check_reactor_init( &reac, &reac_sq, "rec1" );

        struct asrt_test t1;
        uint64_t         counter = 8;
        setup_test( &reac, &t1, "test1", &counter, &countdown_test );

        struct asrt_u8d8msg cmd0 = {};
        auto*               req0 = asrt_msg_ctor_test_start( &cmd0, 0, 0 );
        check_reactor_recv_flags(
            &reac, ( struct asrt_span ){ req0->buff.b, req0->buff.e }, ASRT_FLAG_TSTART );

        check_reactor_tick( &reac, &coll );
        CHECK_EQ( 8, counter );
        check_reactor_tick( &reac, &coll );
        CHECK_EQ( 7, counter );

        {
                auto& collected = coll.data.back();
                assert_test_start( collected, 0, 0 );
                coll.data.pop_back();
        }

        check_reactor_recv_flags(
            &reac, ( struct asrt_span ){ req0->buff.b, req0->buff.e }, ASRT_FLAG_TSTART );

        check_reactor_tick( &reac, &coll );
        CHECK_EQ( 7, counter );

        {
                auto& collected = coll.data.back();
                assert_test_result( collected, 0, ASRT_TEST_RESULT_ERROR );
                coll.data.pop_back();
        }

        {
                auto& collected = coll.data.back();
                assert_test_start( collected, 0, 0 );
                coll.data.pop_back();
        }
}

TEST_CASE_FIXTURE( reactor_ctx, "check_macro" )
{
        check_reactor_init( &reac, &reac_sq, "rec1" );
        reac.node.send_queue = &diag_sq;
        struct asrt_test        t1;
        struct asrt_diag_client diag;
        check_diag_init( &diag, &reac.node );
        struct astrt_check_ctx check_ctx = {
            .diag    = &diag,
            .counter = 0,
        };
        setup_test( &reac, &t1, "test1", &check_ctx, &check_macro_test );

        check_run_test( &reac, &coll, 0, 0 );

        CHECK_EQ( 2, check_ctx.counter );

        {
                auto& collected = coll.data.back();
                assert_test_result( collected, 0, ASRT_TEST_RESULT_FAILURE );
                coll.data.pop_back();
        }

        {
                auto& collected = coll.data.back();
                assert_diag_record( collected, 38 );
                coll.data.pop_back();
        }

        {
                auto& collected = coll.data.back();
                assert_test_start( collected, 0, 0 );
                coll.data.pop_back();
        }
}

TEST_CASE_FIXTURE( reactor_ctx, "require_macro" )
{
        check_reactor_init( &reac, &reac_sq, "rec1" );
        reac.node.send_queue = &diag_sq;
        struct asrt_test        t1;
        struct asrt_diag_client diag;
        check_diag_init( &diag, &reac.node );
        struct astrt_check_ctx check_ctx = {
            .diag    = &diag,
            .counter = 0,
        };
        setup_test( &reac, &t1, "test1", &check_ctx, &require_macro_test );

        check_run_test( &reac, &coll, 0, 0 );

        CHECK_EQ( 1, check_ctx.counter );
        {
                auto& collected = coll.data.back();
                assert_test_result( collected, 0, ASRT_TEST_RESULT_FAILURE );
                coll.data.pop_back();
        }

        {
                auto& collected = coll.data.back();
                assert_diag_record( collected, 28 );
                coll.data.pop_back();
        }

        {
                auto& collected = coll.data.back();
                assert_test_start( collected, 0, 0 );
                coll.data.pop_back();
        }
}

TEST_CASE_FIXTURE( reactor_ctx, "reactor_start_done_error_result_busy" )
{
        check_reactor_init( &reac, &reac_sq, "rec1" );
        struct asrt_test t1;
        setup_test( &reac, &t1, "test1", NULL, &dataless_test_fun );

        // Trigger a test start − reactor will enter WAIT_SEND and enqueue test_start_msg
        struct asrt_u8d8msg cmd0 = {};
        auto*               req0 = asrt_msg_ctor_test_start( &cmd0, 0, 42 );
        check_reactor_recv_flags(
            &reac, ( struct asrt_span ){ req0->buff.b, req0->buff.e }, ASRT_FLAG_TSTART );

        enum asrt_status st = asrt_chann_tick( &reac.node, 0 );
        CHECK_EQ( ASRT_SUCCESS, st );
        CHECK_EQ( ASRT_REAC_WAIT_SEND, reac.state );

        // Simulate err_result_msg slot already in-flight from a previous failure
        asrt_send_req sentinel                 = {};
        reac.wait_send.err_result_msg.req.next = &sentinel;

        // Drain test_start_msg with an error − done_cb fires, sees err slot busy, goes IDLE
        drain_send_queue_ex( &reac_sq, &coll, ASRT_SEND_ERR );
        CHECK_EQ( ASRT_REAC_IDLE, reac.state );
        CHECK( reac_sq.head == nullptr );  // no extra error result enqueued

        reac.wait_send.err_result_msg.req.next = nullptr;  // restore

        {
                auto& collected = coll.data.back();
                assert_test_start( collected, 0, 42 );
                coll.data.pop_back();
        }
}

TEST_CASE_FIXTURE( reactor_ctx, "test_counter" )
{
        check_reactor_init( &reac, &reac_sq, "rec1" );

        struct asrt_test t1;
        uint64_t         counter = 0;
        setup_test( &reac, &t1, "test1", &counter, &countdown_test );

        for ( uint32_t x = 0; x < 42; x++ ) {
                counter = 1;
                check_run_test( &reac, &coll, 0, x );
                {
                        auto& collected = coll.data.back();
                        assert_test_result( collected, x, ASRT_TEST_RESULT_SUCCESS );
                        coll.data.pop_back();
                }

                {
                        auto& collected = coll.data.back();
                        assert_test_start( collected, 0, x );
                        coll.data.pop_back();
                }
        }
}

TEST_CASE_FIXTURE( reactor_ctx, "reactor_unknown_flag" )
{
        CHECK_EQ( ASRT_SUCCESS, asrt_reactor_init( &reac, &reac_sq, "desc" ) );
        // set only unknown flag bits (known flags are 0x01..0x20); the else branch must signal an
        // error
        reac.flags          = 0x40;
        enum asrt_status st = asrt_chann_tick( &reac.node, 0 );
        CHECK_NE( ASRT_SUCCESS, st );
}

// R03: duplicate TEST_INFO before tick must be rejected
TEST_CASE_FIXTURE( reactor_ctx, "reactor_test_info_repeat" )
{
        check_reactor_init( &reac, &reac_sq, "rec1" );

        struct asrt_test t1;
        setup_test( &reac, &t1, "test1", NULL, &dataless_test_fun );

        struct asrt_u8d4msg ti_rpt_cmd = {};
        auto*               ti_rpt_req = asrt_msg_ctor_test_info( &ti_rpt_cmd, 0 );

        // first recv — flag must be set
        check_reactor_recv_flags(
            &reac, ( struct asrt_span ){ ti_rpt_req->buff.b, ti_rpt_req->buff.e }, ASRT_FLAG_TI );

        // second recv before tick — must be rejected
        enum asrt_status st = asrt_chann_recv(
            &reac.node, ( struct asrt_span ){ ti_rpt_req->buff.b, ti_rpt_req->buff.e } );
        CHECK_EQ( ASRT_RECV_ERR, st );

        // flag must still be set
        CHECK_EQ( ASRT_FLAG_TI, reac.flags & ~ASRT_PASSIVE_FLAGS );
}

// R03: duplicate TEST_START before tick must be rejected
TEST_CASE_FIXTURE( reactor_ctx, "reactor_test_start_repeat" )
{
        check_reactor_init( &reac, &reac_sq, "rec1" );

        struct asrt_test t1;
        setup_test( &reac, &t1, "test1", NULL, &dataless_test_fun );

        struct asrt_u8d8msg ts_rpt_cmd = {};
        auto*               ts_rpt_req = asrt_msg_ctor_test_start( &ts_rpt_cmd, 0, 42 );

        // first recv — flag must be set
        check_reactor_recv_flags(
            &reac,
            ( struct asrt_span ){ ts_rpt_req->buff.b, ts_rpt_req->buff.e },
            ASRT_FLAG_TSTART );

        // second recv before tick — must be rejected
        enum asrt_status st = asrt_chann_recv(
            &reac.node, ( struct asrt_span ){ ts_rpt_req->buff.b, ts_rpt_req->buff.e } );
        CHECK_EQ( ASRT_RECV_ERR, st );

        // flag must still be set
        CHECK_EQ( ASRT_FLAG_TSTART, reac.flags & ~ASRT_PASSIVE_FLAGS );
}

// R04: add_test must be rejected after the first recv call
TEST_CASE_FIXTURE( reactor_ctx, "reactor_add_test_after_recv" )
{
        check_reactor_init( &reac, &reac_sq, "rec1" );

        struct asrt_test t1, t2;
        setup_test( &reac, &t1, "test1", NULL, &dataless_test_fun );

        // any valid recv locks registration
        {
                struct asrt_u8d2msg pv_lock_cmd = {};
                auto*               pv_lock_req = asrt_msg_ctor_proto_version( &pv_lock_cmd );
                check_reactor_recv(
                    &reac, ( struct asrt_span ){ pv_lock_req->buff.b, pv_lock_req->buff.e } );
        }

        // adding a test after recv must be rejected
        enum asrt_status st = asrt_test_init( &t2, "test2", NULL, &dataless_test_fun );
        CHECK_EQ( ASRT_SUCCESS, st );
        st = asrt_reactor_add_test( &reac, &t2 );
        CHECK_EQ( ASRT_BUSY_ERR, st );

        // test list must not have grown
        CHECK_EQ( t1.next, nullptr );
}

// continue_f returning an error sets ASRT_TEST_ERROR → sends ASRT_TEST_ERROR result
TEST_CASE_FIXTURE( reactor_ctx, "reactor_test_error" )
{
        check_reactor_init( &reac, &reac_sq, "rec1" );

        struct asrt_test t1;
        setup_test( &reac, &t1, "test1", NULL, &error_continue_fun );

        check_run_test( &reac, &coll, 0, 0 );

        {
                auto& collected = coll.data.back();
                assert_test_result( collected, 0, ASRT_TEST_RESULT_ERROR );
                coll.data.pop_back();
        }

        {
                auto& collected = coll.data.back();
                assert_test_start( collected, 0, 0 );
                coll.data.pop_back();
        }
}

// two requests received before any tick — both flags set simultaneously
TEST_CASE_FIXTURE( reactor_ctx, "reactor_multi_flag" )
{
        check_reactor_init( &reac, &reac_sq, "rec1" );

        // First request: test count
        struct asrt_u8d2msg mf_tc_cmd = {};
        auto*               mf_tc_req = asrt_msg_ctor_test_count( &mf_tc_cmd );
        check_reactor_recv( &reac, ( struct asrt_span ){ mf_tc_req->buff.b, mf_tc_req->buff.e } );

        // Second request: description (no tick between)
        struct asrt_u8d2msg mf_dc_cmd = {};
        auto*               mf_dc_req = asrt_msg_ctor_desc( &mf_dc_cmd );
        check_reactor_recv( &reac, ( struct asrt_span ){ mf_dc_req->buff.b, mf_dc_req->buff.e } );

        // Both flags must be set at the same time
        CHECK( ( reac.flags & ASRT_FLAG_TC ) );
        CHECK( ( reac.flags & ASRT_FLAG_DESC ) );

        // First tick: DESC handled (highest priority in if-else chain)
        enum asrt_status st = asrt_chann_tick( &reac.node, 0 );
        CHECK_EQ( ASRT_SUCCESS, st );
        drain_send_queue( reac.node.send_queue, &coll );
        {
                auto& collected = coll.data.back();
                assert_collected_core_hdr( collected, 0x06, ASRT_MSG_DESC );
                assert_data_ll_contain_str( "rec1", collected, 2 );
                coll.data.pop_back();
        }
        CHECK( ( reac.flags & ASRT_FLAG_TC ) );
        CHECK( !( reac.flags & ASRT_FLAG_DESC ) );

        // Second tick: TC handled
        st = asrt_chann_tick( &reac.node, 0 );
        CHECK_EQ( ASRT_SUCCESS, st );
        drain_send_queue( reac.node.send_queue, &coll );
        {
                auto& collected = coll.data.back();
                assert_collected_core_hdr( collected, 0x04, ASRT_MSG_TEST_COUNT );
                assert_u16( 0x00, collected.data.data() + 2 );
                coll.data.pop_back();
        }
        CHECK( !( reac.flags & ASRT_FLAG_TC ) );
}

// =============================================================================
// diag_record accepted/busy — standalone fixture, no reactor dependency
// =============================================================================

namespace
{

struct diag_record_ctx
{
        asrt_send_req_list send_queue = {};
        asrt_node          root_node  = {
                      .chid       = ASRT_CORE,
                      .e_cb_ptr   = nullptr,
                      .e_cb       = nullptr,
                      .next       = nullptr,
                      .prev       = nullptr,
                      .send_queue = &send_queue,
        };
        asrt_diag_client diag = {};
        collector        coll;

        diag_record_ctx() { CHECK_EQ( ASRT_SUCCESS, asrt_diag_client_init( &diag, &root_node ) ); }
        ~diag_record_ctx() { coll.data.clear(); }
};

}  // namespace

// fire-and-forget (NULL callback): slot free → ACCEPTED, message queued
TEST_CASE_FIXTURE( diag_record_ctx, "diag_record_no_cb_accepted" )
{
        auto result = asrt_diag_client_record( &diag, "f.c", 10, nullptr, nullptr, nullptr );
        CHECK_EQ( ASRT_DIAG_RECORD_ACCEPTED, result );
        CHECK( send_queue.head != nullptr );
        drain_send_queue( &send_queue, &coll );
        REQUIRE_EQ( 1U, coll.data.size() );
        assert_diag_record( coll.data.back(), 10 );
        coll.data.clear();
}

// fire-and-forget (NULL callback): slot busy → BUSY, nothing enqueued
TEST_CASE_FIXTURE( diag_record_ctx, "diag_record_no_cb_busy" )
{
        // Simulate slot in-flight: set next to a non-NULL sentinel so
        // asrt_send_is_req_used() returns true.
        asrt_send_req sentinel = {};
        diag.msg.req.next      = &sentinel;

        auto result = asrt_diag_client_record( &diag, "f.c", 10, nullptr, nullptr, nullptr );
        CHECK_EQ( ASRT_DIAG_RECORD_BUSY, result );
        CHECK( send_queue.head == nullptr );  // nothing new was enqueued

        diag.msg.req.next = nullptr;  // restore
}

// with callback: slot free → ACCEPTED, callback is invoked by drain
TEST_CASE_FIXTURE( diag_record_ctx, "diag_record_with_cb_accepted" )
{
        bool             cb_called = false;
        enum asrt_status cb_status = ASRT_SIZE_ERR;

        asrt_diag_record_done_cb cb = []( void* p, enum asrt_status s ) {
                auto* pair    = static_cast< std::pair< bool*, enum asrt_status* >* >( p );
                *pair->first  = true;
                *pair->second = s;
        };
        auto pair = std::make_pair( &cb_called, &cb_status );

        auto result = asrt_diag_client_record( &diag, "f.c", 7, nullptr, cb, &pair );
        CHECK_EQ( ASRT_DIAG_RECORD_ACCEPTED, result );
        CHECK_FALSE( cb_called );  // not yet fired before drain

        drain_send_queue( &send_queue, &coll );
        CHECK( cb_called );
        CHECK_EQ( ASRT_SUCCESS, cb_status );
        coll.data.clear();
}

// with callback: slot busy → BUSY, callback is NOT invoked
TEST_CASE_FIXTURE( diag_record_ctx, "diag_record_with_cb_busy" )
{
        bool                     cb_called = false;
        asrt_diag_record_done_cb cb        = []( void* p, enum asrt_status ) {
                *static_cast< bool* >( p ) = true;
        };

        asrt_send_req sentinel = {};
        diag.msg.req.next      = &sentinel;

        auto result = asrt_diag_client_record( &diag, "f.c", 9, nullptr, cb, &cb_called );
        CHECK_EQ( ASRT_DIAG_RECORD_BUSY, result );
        CHECK_FALSE( cb_called );  // must NOT be called when busy

        diag.msg.req.next = nullptr;  // restore
}

// =============================================================================

TEST_CASE_FIXTURE( reactor_ctx, "diag_init" )
{
        struct asrt_diag_client diag = {};

        // NULL diag
        CHECK_EQ( ASRT_INIT_ERR, asrt_diag_client_init( NULL, &reac.node ) );

        // NULL prev
        CHECK_EQ( ASRT_INIT_ERR, asrt_diag_client_init( &diag, NULL ) );

        // valid init
        check_reactor_init( &reac, &reac_sq, "rec1" );
        CHECK_EQ( ASRT_SUCCESS, asrt_diag_client_init( &diag, &reac.node ) );
        CHECK_EQ( ASRT_DIAG, diag.node.chid );
        CHECK_EQ( &diag.node, reac.node.next );  // node appended after prev
        CHECK_EQ( nullptr, diag.node.next );     // chain-terminal
}

TEST_CASE_FIXTURE( reactor_ctx, "diag_record" )
{
        check_reactor_init( &reac, &reac_sq, "rec1" );
        reac.node.send_queue         = &diag_sq;
        struct asrt_diag_client diag = {};
        check_diag_init( &diag, &reac.node );

        // normal call — verify full byte content
        asrt_diag_client_record( &diag, "test.c", 42, nullptr, nullptr, nullptr );
        drain_send_queue( &diag_sq, &coll );
        {
                auto& collected = coll.data.back();
                assert_diag_record( collected, 42 );
                CHECK_EQ( 1U + 4U + 1U + 6U, collected.data.size() );
                assert_data_ll_contain_str( "test.c", collected, 6 );
                coll.data.pop_back();
        }

        // line = 0
        asrt_diag_client_record( &diag, "f.c", 0, nullptr, nullptr, nullptr );
        drain_send_queue( &diag_sq, &coll );
        {
                auto& collected = coll.data.back();
                assert_diag_record( collected, 0 );
                coll.data.pop_back();
        }

        // line = UINT32_MAX
        asrt_diag_client_record( &diag, "f.c", UINT32_MAX, nullptr, nullptr, nullptr );
        drain_send_queue( &diag_sq, &coll );
        {
                auto& collected = coll.data.back();
                assert_diag_record( collected, UINT32_MAX );
                coll.data.pop_back();
        }
}

// two consecutive CHECK failures → two diag messages, counter incremented twice
TEST_CASE_FIXTURE( reactor_ctx, "check_macro_two_fails" )
{
        check_reactor_init( &reac, &reac_sq, "rec1" );
        reac.node.send_queue = &diag_sq;
        struct asrt_test        t1;
        struct asrt_diag_client diag;
        check_diag_init( &diag, &reac.node );
        struct astrt_check_ctx check_ctx = { .diag = &diag, .counter = 0 };
        setup_test( &reac, &t1, "test1", &check_ctx, &check_macro_two_fails );

        check_run_test( &reac, &coll, 0, 0 );

        CHECK_EQ( 2, check_ctx.counter );
        {
                auto& collected = coll.data.back();
                assert_test_result( collected, 0, ASRT_TEST_RESULT_FAILURE );
                coll.data.pop_back();
        }
        {
                auto& collected = coll.data.back();
                // second CHECK also fails but the single diag slot is still busy, so
                // that record is dropped (ASRT_DIAG_RECORD_BUSY); only one diag message.
                assert_diag_record_any_line( collected );  // first CHECK failure
                coll.data.pop_back();
        }
        {
                auto& collected = coll.data.back();
                assert_test_start( collected, 0, 0 );
                coll.data.pop_back();
        }
}

// one CHECK failure then one pass → one diag message, counter = 2
TEST_CASE_FIXTURE( reactor_ctx, "check_macro_fail_pass" )
{
        check_reactor_init( &reac, &reac_sq, "rec1" );
        reac.node.send_queue = &diag_sq;
        struct asrt_test        t1;
        struct asrt_diag_client diag;
        check_diag_init( &diag, &reac.node );
        struct astrt_check_ctx check_ctx = { .diag = &diag, .counter = 0 };
        setup_test( &reac, &t1, "test1", &check_ctx, &check_macro_fail_pass );

        check_run_test( &reac, &coll, 0, 0 );

        CHECK_EQ( 2, check_ctx.counter );
        {
                auto& collected = coll.data.back();
                assert_test_result( collected, 0, ASRT_TEST_RESULT_FAILURE );
                coll.data.pop_back();
        }
        {
                auto& collected = coll.data.back();
                assert_diag_record_any_line( collected );
                coll.data.pop_back();
        }
        {
                auto& collected = coll.data.back();
                assert_test_start( collected, 0, 0 );
                coll.data.pop_back();
        }
}

// failing REQUIRE → code after it unreachable
TEST_CASE_FIXTURE( reactor_ctx, "require_fail_then_check" )
{
        check_reactor_init( &reac, &reac_sq, "rec1" );
        reac.node.send_queue = &diag_sq;
        struct asrt_test        t1;
        struct asrt_diag_client diag;
        check_diag_init( &diag, &reac.node );
        struct astrt_check_ctx check_ctx = { .diag = &diag, .counter = 0 };
        setup_test( &reac, &t1, "test1", &check_ctx, &require_then_check );

        check_run_test( &reac, &coll, 0, 0 );

        CHECK_EQ( 0, check_ctx.counter );  // counter never incremented
        {
                auto& collected = coll.data.back();
                assert_test_result( collected, 0, ASRT_TEST_RESULT_FAILURE );
                coll.data.pop_back();
        }
        {
                auto& collected = coll.data.back();
                assert_diag_record_any_line( collected );  // only one record (REQUIRE)
                coll.data.pop_back();
        }
        {
                auto& collected = coll.data.back();
                assert_test_start( collected, 0, 0 );
                coll.data.pop_back();
        }
}

// CHECK fails, REQUIRE passes, CHECK fails → one diag message (second CHECK dropped: slot busy),
// counter = 3
TEST_CASE_FIXTURE( reactor_ctx, "mix_check_require_check" )
{
        check_reactor_init( &reac, &reac_sq, "rec1" );
        reac.node.send_queue = &diag_sq;
        struct asrt_test        t1;
        struct asrt_diag_client diag;
        check_diag_init( &diag, &reac.node );
        struct astrt_check_ctx check_ctx = { .diag = &diag, .counter = 0 };
        setup_test( &reac, &t1, "test1", &check_ctx, &mix_check_require_check );

        check_run_test( &reac, &coll, 0, 0 );

        CHECK_EQ( 3, check_ctx.counter );
        {
                auto& collected = coll.data.back();
                assert_test_result( collected, 0, ASRT_TEST_RESULT_FAILURE );
                coll.data.pop_back();
        }
        {
                auto& collected = coll.data.back();
                // second CHECK also fails but the diag slot is still busy (not drained
                // within a single tick), so only first CHECK failure is recorded.
                assert_diag_record_any_line( collected );  // first CHECK failure
                coll.data.pop_back();
        }
        {
                auto& collected = coll.data.back();
                assert_test_start( collected, 0, 0 );
                coll.data.pop_back();
        }
}

// CHECK fails, REQUIRE fails → one diag message (REQUIRE dropped: slot busy), counter = 1
TEST_CASE_FIXTURE( reactor_ctx, "mix_check_require_fail" )
{
        check_reactor_init( &reac, &reac_sq, "rec1" );
        reac.node.send_queue = &diag_sq;
        struct asrt_test        t1;
        struct asrt_diag_client diag;
        check_diag_init( &diag, &reac.node );
        struct astrt_check_ctx check_ctx = { .diag = &diag, .counter = 0 };
        setup_test( &reac, &t1, "test1", &check_ctx, &mix_check_require_fail );

        check_run_test( &reac, &coll, 0, 0 );

        CHECK_EQ( 1, check_ctx.counter );  // second increment unreachable
        {
                auto& collected = coll.data.back();
                assert_test_result( collected, 0, ASRT_TEST_RESULT_FAILURE );
                coll.data.pop_back();
        }
        {
                auto& collected = coll.data.back();
                // REQUIRE also fails but the diag slot is still busy (not drained
                // within a single tick), so only the CHECK failure is recorded.
                assert_diag_record_any_line( collected );  // CHECK diag
                coll.data.pop_back();
        }
        {
                auto& collected = coll.data.back();
                assert_test_start( collected, 0, 0 );
                coll.data.pop_back();
        }
}

// truncated and trailing-byte recv errors in asrt_reactor_recv
TEST_CASE_FIXTURE( reactor_ctx, "reactor_recv_truncated" )
{
        check_reactor_init( &reac, &reac_sq, "rec1" );

        uint8_t          buf[16];
        struct asrt_span sp;
        enum asrt_status rst;

        // Truncated TEST_INFO: only message ID, no u16 tid
        sp = ( struct asrt_span ){ .b = buf, .e = buf + sizeof buf };
        asrt_add_u16( &sp.b, ASRT_MSG_TEST_INFO );
        rst = asrt_chann_recv( &reac.node, ( struct asrt_span ){ .b = buf, .e = sp.b } );
        CHECK_EQ( ASRT_RECV_ERR, rst );

        // Truncated TEST_START: only ID + partial tid(2), missing run_id(4)
        sp = ( struct asrt_span ){ .b = buf, .e = buf + sizeof buf };
        asrt_add_u16( &sp.b, ASRT_MSG_TEST_START );
        asrt_add_u16( &sp.b, 0 );  // tid only, no run_id
        rst = asrt_chann_recv( &reac.node, ( struct asrt_span ){ .b = buf, .e = sp.b } );
        CHECK_EQ( ASRT_RECV_ERR, rst );

        // Trailing bytes: PROTO_VERSION request + extra bytes
        sp = ( struct asrt_span ){ .b = buf, .e = buf + sizeof buf };
        asrt_add_u16( &sp.b, ASRT_MSG_PROTO_VERSION );
        asrt_add_u16( &sp.b, 0xFFFF );  // extra bytes after a no-payload message
        rst = asrt_chann_recv( &reac.node, ( struct asrt_span ){ .b = buf, .e = sp.b } );
        CHECK_EQ( ASRT_RECV_ERR, rst );
}

// ============================================================================
// asrt_param_client — reactor PARAM channel (Phase 3)
// ============================================================================

static inline enum asrt_status call_rtr_param_client_recv(
    struct asrt_param_client* p,
    uint8_t*                  b,
    uint8_t*                  e )
{
        return asrt_chann_recv( &p->node, ( struct asrt_span ){ .b = b, .e = e } );
}

static uint8_t* build_param_ready( uint8_t* buf, asrt::flat_id root_id )
{
        uint8_t* p = buf;
        *p++       = ASRT_PARAM_MSG_READY;
        asrt_add_u32( &p, root_id );
        return p;
}

static uint8_t* build_param_error( uint8_t* buf, uint8_t error_code, asrt::flat_id node_id )
{
        uint8_t* p = buf;
        *p++       = ASRT_PARAM_MSG_ERROR;
        *p++       = error_code;
        asrt_add_u32( &p, node_id );
        return p;
}

// Build a RESPONSE payload: msg_id + nodes + trailing next_sibling_id
// Each node: u32 id | key\0 | u8 type | value bytes
// Common header: RESPONSE msg_id + node_id + key\0
// Returns pointer past the key terminator, ready for type byte + value.
static uint8_t* build_param_response_head( uint8_t* buf, asrt::flat_id node_id, char const* key )
{
        uint8_t* p = buf;
        *p++       = ASRT_PARAM_MSG_RESPONSE;
        asrt_add_u32( &p, node_id );
        size_t klen = strlen( key );
        memcpy( p, key, klen );
        p += klen;
        *p++ = '\0';
        return p;
}

static uint8_t* build_param_response_u32(
    uint8_t*      buf,
    asrt::flat_id node_id,
    char const*   key,
    uint32_t      value,
    asrt::flat_id next_sibling_id )
{
        uint8_t* p = build_param_response_head( buf, node_id, key );
        *p++       = ASRT_FLAT_STYPE_U32;
        asrt_add_u32( &p, value );
        asrt_add_u32( &p, next_sibling_id );
        return p;
}

static uint8_t* build_param_response_i32(
    uint8_t*      buf,
    asrt::flat_id node_id,
    char const*   key,
    int32_t       value,
    asrt::flat_id next_sibling_id )
{
        uint8_t* p = build_param_response_head( buf, node_id, key );
        *p++       = ASRT_FLAT_STYPE_I32;
        asrt_add_i32( &p, value );
        asrt_add_u32( &p, next_sibling_id );
        return p;
}

static uint8_t* build_param_response_str(
    uint8_t*      buf,
    asrt::flat_id node_id,
    char const*   key,
    char const*   value,
    asrt::flat_id next_sibling_id )
{
        uint8_t* p  = build_param_response_head( buf, node_id, key );
        *p++        = ASRT_FLAT_STYPE_STR;
        size_t vlen = strlen( value );
        memcpy( p, value, vlen );
        p += vlen;
        *p++ = '\0';
        asrt_add_u32( &p, next_sibling_id );
        return p;
}

static uint8_t* build_param_response_float(
    uint8_t*      buf,
    asrt::flat_id node_id,
    char const*   key,
    float         value,
    asrt::flat_id next_sibling_id )
{
        uint8_t* p = build_param_response_head( buf, node_id, key );
        *p++       = ASRT_FLAT_STYPE_FLOAT;
        uint32_t bits;
        memcpy( &bits, &value, sizeof bits );
        asrt_add_u32( &p, bits );
        asrt_add_u32( &p, next_sibling_id );
        return p;
}

static uint8_t* build_param_response_bool(
    uint8_t*      buf,
    asrt::flat_id node_id,
    char const*   key,
    uint32_t      value,
    asrt::flat_id next_sibling_id )
{
        uint8_t* p = build_param_response_head( buf, node_id, key );
        *p++       = ASRT_FLAT_STYPE_BOOL;
        asrt_add_u32( &p, value );
        asrt_add_u32( &p, next_sibling_id );
        return p;
}

static uint8_t* build_param_response_obj(
    uint8_t*      buf,
    asrt::flat_id node_id,
    char const*   key,
    asrt::flat_id first_child,
    asrt::flat_id last_child,
    asrt::flat_id next_sibling_id )
{
        uint8_t* p = build_param_response_head( buf, node_id, key );
        *p++       = ASRT_FLAT_CTYPE_OBJECT;
        asrt_add_u32( &p, first_child );
        asrt_add_u32( &p, last_child );
        asrt_add_u32( &p, next_sibling_id );
        return p;
}

static uint8_t* build_param_response_arr(
    uint8_t*      buf,
    asrt::flat_id node_id,
    char const*   key,
    asrt::flat_id first_child,
    asrt::flat_id last_child,
    asrt::flat_id next_sibling_id )
{
        uint8_t* p = build_param_response_head( buf, node_id, key );
        *p++       = ASRT_FLAT_CTYPE_ARRAY;
        asrt_add_u32( &p, first_child );
        asrt_add_u32( &p, last_child );
        asrt_add_u32( &p, next_sibling_id );
        return p;
}

namespace
{

struct param_client_ctx
{
        static constexpr uint32_t buff_size = 256;

        asrt_send_req_list       send_queue         = {};
        struct asrt_node         head               = {};
        struct asrt_param_client client             = {};
        uint8_t                  msg_buf[buff_size] = {};
        collector                coll;
        struct asrt_param_query  query = {};

        // callback state
        int           resp_called = 0;
        asrt::flat_id resp_id     = 0;
        std::string   resp_key;
        uint32_t      resp_u32_val  = 0;
        uint8_t       resp_type     = 0;
        asrt::flat_id resp_next_sib = 0;
        uint8_t       error_code    = 0;
        asrt::flat_id error_node_id = 0;
        int           error_called  = 0;
        uint32_t      t             = 100;

        static void query_cb(
            struct asrt_param_client*,
            struct asrt_param_query* q,
            struct asrt_flat_value   val )
        {
                auto* ctx = (param_client_ctx*) q->cb_ptr;
                if ( q->error_code != 0 ) {
                        ctx->error_code    = q->error_code;
                        ctx->error_node_id = q->node_id;
                        ctx->error_called++;
                } else {
                        ctx->resp_called++;
                        ctx->resp_id       = q->node_id;
                        ctx->resp_key      = q->key ? q->key : "";
                        ctx->resp_type     = (uint8_t) val.type;
                        ctx->resp_u32_val  = val.data.s.u32_val;
                        ctx->resp_next_sib = q->next_sibling;
                }
        }

        // helpers
        void make_ready( asrt::flat_id root_id = 1U )
        {
                uint8_t buf[8];
                REQUIRE_EQ(
                    ASRT_SUCCESS,
                    call_rtr_param_client_recv( &client, buf, build_param_ready( buf, root_id ) ) );
                REQUIRE_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, t++ ) );
                drain_send_queue( &send_queue, &coll );
                coll.data.clear();
        }

        param_client_ctx()
        {
                head.chid           = ASRT_CORE;
                head.send_queue     = &send_queue;
                struct asrt_span mb = { .b = msg_buf, .e = msg_buf + buff_size };
                REQUIRE_EQ( ASRT_SUCCESS, asrt_param_client_init( &client, &head, mb, 100 ) );
        }
        ~param_client_ctx() { asrt_param_client_deinit( &client ); }
};

}  // namespace

TEST_CASE( "asrt_param_client_init" )
{
        asrt_send_req_list send_queue    = {};
        struct asrt_node   head          = {};
        head.chid                        = ASRT_CORE;
        head.send_queue                  = &send_queue;
        struct asrt_param_client client  = {};
        uint8_t                  buf[64] = {};
        struct asrt_span         mb      = { .b = buf, .e = buf + sizeof buf };
        struct asrt_span         bad     = { .b = nullptr, .e = nullptr };

        CHECK_EQ( ASRT_INIT_ERR, asrt_param_client_init( NULL, &head, mb, 0 ) );
        CHECK_EQ( ASRT_INIT_ERR, asrt_param_client_init( &client, NULL, mb, 0 ) );
        CHECK_EQ( ASRT_INIT_ERR, asrt_param_client_init( &client, &head, bad, 0 ) );
        CHECK_EQ( ASRT_INIT_ERR, asrt_param_client_init( &client, &head, mb, 0 ) );

        CHECK_EQ( ASRT_SUCCESS, asrt_param_client_init( &client, &head, mb, 100 ) );
        CHECK_EQ( ASRT_PARA, client.node.chid );
        CHECK_NE( nullptr, (void*) (uintptr_t) client.node.e_cb_ptr );
        CHECK_EQ( &client.node, head.next );
        CHECK_EQ( 0, client.ready );
        asrt_param_client_deinit( &client );
}

TEST_CASE_FIXTURE( param_client_ctx, "asrt_param_client_ready_sends_ack_and_stores_root" )
{
        uint8_t buf[8];
        CHECK_EQ(
            ASRT_SUCCESS,
            call_rtr_param_client_recv( &client, buf, build_param_ready( buf, 3U ) ) );
        CHECK_EQ( 0, client.ready );

        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, t++ ) );
        CHECK_EQ( ASRT_PARAM_CLIENT_READY_SENT, client.state );
        CHECK_EQ( 0, client.ready );

        // READY_ACK sent with capacity as big-endian u32
        drain_send_queue( &send_queue, &coll );
        REQUIRE_EQ( 1U, coll.data.size() );
        auto& msg = coll.data.front();
        CHECK_EQ( ASRT_PARA, msg.id );
        REQUIRE_EQ( 5U, msg.data.size() );
        CHECK_EQ( ASRT_PARAM_MSG_READY_ACK, msg.data[0] );
        CHECK_EQ( 0U, msg.data[1] );
        CHECK_EQ( 0U, msg.data[2] );
        CHECK_EQ( 1U, msg.data[3] );  // 256 = 0x00000100
        CHECK_EQ( 0U, msg.data[4] );

        CHECK_EQ( ASRT_PARAM_CLIENT_IDLE, client.state );
        CHECK_EQ( 1, client.ready );
        CHECK_EQ( 3U, asrt_param_client_root_id( &client ) );
}

TEST_CASE_FIXTURE( param_client_ctx, "asrt_param_client_query_before_ready_returns_error" )
{
        CHECK_EQ(
            ASRT_ARG_ERR, asrt_param_client_fetch_any( &query, &client, 2U, nullptr, nullptr ) );
        CHECK( coll.data.empty() );
}

TEST_CASE_FIXTURE( param_client_ctx, "asrt_param_client_query_cache_miss_sends_wire" )
{
        make_ready( 1U );

        CHECK_EQ(
            ASRT_SUCCESS, asrt_param_client_fetch_any( &query, &client, 2U, query_cb, this ) );
        // query itself does NOT send — tick does the cache lookup + sends on miss
        CHECK( coll.data.empty() );

        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, t++ ) );
        drain_send_queue( &send_queue, &coll );
        REQUIRE_EQ( 1U, coll.data.size() );
        auto& msg = coll.data.front();
        CHECK_EQ( ASRT_PARA, msg.id );
        REQUIRE_EQ( 5U, msg.data.size() );
        CHECK_EQ( ASRT_PARAM_MSG_QUERY, msg.data[0] );
        // node_id = 2 big-endian
        CHECK_EQ( 0U, msg.data[1] );
        CHECK_EQ( 0U, msg.data[2] );
        CHECK_EQ( 0U, msg.data[3] );
        CHECK_EQ( 2U, msg.data[4] );
}

TEST_CASE_FIXTURE( param_client_ctx, "asrt_param_client_response_delivers_one_node" )
{
        make_ready( 1U );

        CHECK_EQ(
            ASRT_SUCCESS, asrt_param_client_fetch_any( &query, &client, 10U, query_cb, this ) );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, t++ ) );  // cache miss → wire
        drain_send_queue( &send_queue, &coll );
        coll.data.clear();

        // Inject a RESPONSE with one node: id=10, key="abc", type=U32, value=42, next_sib=0
        uint8_t  rbuf[64];
        uint8_t* re = build_param_response_u32( rbuf, 10U, "abc", 42U, 0U );
        CHECK_EQ( ASRT_SUCCESS, call_rtr_param_client_recv( &client, rbuf, re ) );

        CHECK_EQ( 0, resp_called );  // pending
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, t++ ) );
        CHECK_EQ( 1, resp_called );
        CHECK_EQ( 10U, resp_id );
        CHECK_EQ( "abc", resp_key );
        CHECK_EQ( ASRT_FLAT_STYPE_U32, resp_type );
        CHECK_EQ( 42U, resp_u32_val );
        CHECK_EQ( 0U, resp_next_sib );

        // query cleared after delivery
        CHECK_EQ( nullptr, client.pending_query );
}

TEST_CASE_FIXTURE( param_client_ctx, "asrt_param_client_cache_hit_delivers_without_wire" )
{
        make_ready( 1U );

        // First query — cache miss, sends wire, gets response
        CHECK_EQ(
            ASRT_SUCCESS, asrt_param_client_fetch_any( &query, &client, 10U, query_cb, this ) );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, t++ ) );  // wire
        coll.data.clear();

        uint8_t  rbuf[64];
        uint8_t* re = build_param_response_u32( rbuf, 10U, "abc", 42U, 0U );
        CHECK_EQ( ASRT_SUCCESS, call_rtr_param_client_recv( &client, rbuf, re ) );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, t++ ) );  // delivers
        resp_called = 0;

        // Second query for same node — should be a cache hit, no wire sent
        CHECK_EQ(
            ASRT_SUCCESS, asrt_param_client_fetch_any( &query, &client, 10U, query_cb, this ) );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, t++ ) );

        CHECK_EQ( 1, resp_called );
        CHECK_EQ( 10U, resp_id );
        CHECK_EQ( "abc", resp_key );
        CHECK_EQ( 42U, resp_u32_val );
        // no wire message sent
        CHECK( coll.data.empty() );
}

TEST_CASE_FIXTURE( param_client_ctx, "asrt_param_client_error_dispatches_and_clears" )
{
        make_ready( 1U );

        CHECK_EQ(
            ASRT_SUCCESS, asrt_param_client_fetch_any( &query, &client, 5U, query_cb, this ) );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, t++ ) );  // cache miss → wire
        coll.data.clear();

        // Inject PARAM_ERROR
        uint8_t buf[8];
        CHECK_EQ(
            ASRT_SUCCESS,
            call_rtr_param_client_recv(
                &client, buf, build_param_error( buf, ASRT_PARAM_ERR_RESPONSE_TOO_LARGE, 5U ) ) );
        CHECK_EQ( 0, error_called );

        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, t++ ) );
        CHECK_EQ( 1, error_called );
        CHECK_EQ( ASRT_PARAM_ERR_RESPONSE_TOO_LARGE, error_code );
        CHECK_EQ( 5U, error_node_id );

        // query cleared after error dispatch
        CHECK_EQ( nullptr, client.pending_query );
}

TEST_CASE_FIXTURE( param_client_ctx, "asrt_param_client_cache_next_sibling_stored" )
{
        make_ready( 1U );

        CHECK_EQ(
            ASRT_SUCCESS, asrt_param_client_fetch_any( &query, &client, 10U, query_cb, this ) );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, t++ ) );  // wire
        coll.data.clear();

        // RESPONSE with next_sibling_id = 99
        uint8_t  rbuf[64];
        uint8_t* re = build_param_response_u32( rbuf, 10U, "x", 7U, 99U );
        CHECK_EQ( ASRT_SUCCESS, call_rtr_param_client_recv( &client, rbuf, re ) );
        CHECK_EQ( 99U, client.cache_next_sibling );

        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, t++ ) );
        CHECK_EQ( 1, resp_called );
        CHECK_EQ( 99U, resp_next_sib );
}

// ============================================================================
// Typed query API tests
// ============================================================================

// --- query_u32 ---

TEST_CASE_FIXTURE( param_client_ctx, "query_u32_happy" )
{
        make_ready( 1U );
        int      called = 0;
        uint32_t got    = 0;
        struct
        {
                int*      called;
                uint32_t* got;
        } ctx   = { &called, &got };
        auto cb = []( asrt_param_client*, asrt_param_query* q, uint32_t val ) {
                auto* c = (decltype( ctx )*) q->cb_ptr;
                ( *c->called )++;
                *c->got = val;
        };
        CHECK_EQ( ASRT_SUCCESS, asrt_param_client_fetch_u32( &query, &client, 10U, cb, &ctx ) );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, t++ ) );  // wire
        uint8_t  rbuf[64];
        uint8_t* re = build_param_response_u32( rbuf, 10U, "k", 42U, 0U );
        CHECK_EQ( ASRT_SUCCESS, call_rtr_param_client_recv( &client, rbuf, re ) );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, t++ ) );
        CHECK_EQ( 1, called );
        CHECK_EQ( 42U, got );
        CHECK_EQ( 0U, query.error_code );
}

TEST_CASE_FIXTURE( param_client_ctx, "query_u32_type_mismatch" )
{
        make_ready( 1U );
        int called = 0;
        struct
        {
                int* called;
        } ctx   = { &called };
        auto cb = []( asrt_param_client*, asrt_param_query* q, uint32_t ) {
                auto* c = (decltype( ctx )*) q->cb_ptr;
                ( *c->called )++;
        };
        CHECK_EQ( ASRT_SUCCESS, asrt_param_client_fetch_u32( &query, &client, 10U, cb, &ctx ) );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, t++ ) );
        uint8_t  rbuf[64];
        uint8_t* re = build_param_response_str( rbuf, 10U, "k", "hello", 0U );
        CHECK_EQ( ASRT_SUCCESS, call_rtr_param_client_recv( &client, rbuf, re ) );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, t++ ) );
        CHECK_EQ( 1, called );
        CHECK_EQ( ASRT_PARAM_ERR_TYPE_MISMATCH, query.error_code );
}

// --- query_i32 ---

TEST_CASE_FIXTURE( param_client_ctx, "query_i32_happy" )
{
        make_ready( 1U );
        int     called = 0;
        int32_t got    = 0;
        struct
        {
                int*     called;
                int32_t* got;
        } ctx   = { &called, &got };
        auto cb = []( asrt_param_client*, asrt_param_query* q, int32_t val ) {
                auto* c = (decltype( ctx )*) q->cb_ptr;
                ( *c->called )++;
                *c->got = val;
        };
        CHECK_EQ( ASRT_SUCCESS, asrt_param_client_fetch_i32( &query, &client, 10U, cb, &ctx ) );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, t++ ) );
        uint8_t  rbuf[64];
        uint8_t* re = build_param_response_i32( rbuf, 10U, "k", -7, 0U );
        CHECK_EQ( ASRT_SUCCESS, call_rtr_param_client_recv( &client, rbuf, re ) );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, t++ ) );
        CHECK_EQ( 1, called );
        CHECK_EQ( -7, got );
        CHECK_EQ( 0U, query.error_code );
}

TEST_CASE_FIXTURE( param_client_ctx, "query_i32_type_mismatch" )
{
        make_ready( 1U );
        int called = 0;
        struct
        {
                int* called;
        } ctx   = { &called };
        auto cb = []( asrt_param_client*, asrt_param_query* q, int32_t ) {
                auto* c = (decltype( ctx )*) q->cb_ptr;
                ( *c->called )++;
        };
        CHECK_EQ( ASRT_SUCCESS, asrt_param_client_fetch_i32( &query, &client, 10U, cb, &ctx ) );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, t++ ) );
        uint8_t  rbuf[64];
        uint8_t* re = build_param_response_u32( rbuf, 10U, "k", 99U, 0U );
        CHECK_EQ( ASRT_SUCCESS, call_rtr_param_client_recv( &client, rbuf, re ) );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, t++ ) );
        CHECK_EQ( 1, called );
        CHECK_EQ( ASRT_PARAM_ERR_TYPE_MISMATCH, query.error_code );
}

// --- query_str ---

TEST_CASE_FIXTURE( param_client_ctx, "query_str_happy" )
{
        make_ready( 1U );
        int         called = 0;
        std::string got;
        struct
        {
                int*         called;
                std::string* got;
        } ctx   = { &called, &got };
        auto cb = []( asrt_param_client*, asrt_param_query* q, char const* val ) {
                auto* c = (decltype( ctx )*) q->cb_ptr;
                ( *c->called )++;
                if ( val )
                        *c->got = val;
        };
        CHECK_EQ( ASRT_SUCCESS, asrt_param_client_fetch_str( &query, &client, 10U, cb, &ctx ) );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, t++ ) );
        uint8_t  rbuf[64];
        uint8_t* re = build_param_response_str( rbuf, 10U, "k", "hello", 0U );
        CHECK_EQ( ASRT_SUCCESS, call_rtr_param_client_recv( &client, rbuf, re ) );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, t++ ) );
        CHECK_EQ( 1, called );
        CHECK_EQ( "hello", got );
        CHECK_EQ( 0U, query.error_code );
}

TEST_CASE_FIXTURE( param_client_ctx, "query_str_type_mismatch" )
{
        make_ready( 1U );
        int called = 0;
        struct
        {
                int* called;
        } ctx   = { &called };
        auto cb = []( asrt_param_client*, asrt_param_query* q, char const* ) {
                auto* c = (decltype( ctx )*) q->cb_ptr;
                ( *c->called )++;
        };
        CHECK_EQ( ASRT_SUCCESS, asrt_param_client_fetch_str( &query, &client, 10U, cb, &ctx ) );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, t++ ) );
        uint8_t  rbuf[64];
        uint8_t* re = build_param_response_u32( rbuf, 10U, "k", 1U, 0U );
        CHECK_EQ( ASRT_SUCCESS, call_rtr_param_client_recv( &client, rbuf, re ) );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, t++ ) );
        CHECK_EQ( 1, called );
        CHECK_EQ( ASRT_PARAM_ERR_TYPE_MISMATCH, query.error_code );
}

// --- query_float ---

TEST_CASE_FIXTURE( param_client_ctx, "query_float_happy" )
{
        make_ready( 1U );
        int   called = 0;
        float got    = 0.0F;
        struct
        {
                int*   called;
                float* got;
        } ctx   = { &called, &got };
        auto cb = []( asrt_param_client*, asrt_param_query* q, float val ) {
                auto* c = (decltype( ctx )*) q->cb_ptr;
                ( *c->called )++;
                *c->got = val;
        };
        CHECK_EQ( ASRT_SUCCESS, asrt_param_client_fetch_float( &query, &client, 10U, cb, &ctx ) );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, t++ ) );
        uint8_t  rbuf[64];
        uint8_t* re = build_param_response_float( rbuf, 10U, "k", 3.14F, 0U );
        CHECK_EQ( ASRT_SUCCESS, call_rtr_param_client_recv( &client, rbuf, re ) );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, t++ ) );
        CHECK_EQ( 1, called );
        CHECK_EQ( 3.14F, got );
        CHECK_EQ( 0U, query.error_code );
}

TEST_CASE_FIXTURE( param_client_ctx, "query_float_type_mismatch" )
{
        make_ready( 1U );
        int called = 0;
        struct
        {
                int* called;
        } ctx   = { &called };
        auto cb = []( asrt_param_client*, asrt_param_query* q, float ) {
                auto* c = (decltype( ctx )*) q->cb_ptr;
                ( *c->called )++;
        };
        CHECK_EQ( ASRT_SUCCESS, asrt_param_client_fetch_float( &query, &client, 10U, cb, &ctx ) );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, t++ ) );
        uint8_t  rbuf[64];
        uint8_t* re = build_param_response_u32( rbuf, 10U, "k", 1U, 0U );
        CHECK_EQ( ASRT_SUCCESS, call_rtr_param_client_recv( &client, rbuf, re ) );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, t++ ) );
        CHECK_EQ( 1, called );
        CHECK_EQ( ASRT_PARAM_ERR_TYPE_MISMATCH, query.error_code );
}

// --- query_bool ---

TEST_CASE_FIXTURE( param_client_ctx, "query_bool_happy" )
{
        make_ready( 1U );
        int      called = 0;
        uint32_t got    = 0;
        struct
        {
                int*      called;
                uint32_t* got;
        } ctx   = { &called, &got };
        auto cb = []( asrt_param_client*, asrt_param_query* q, uint32_t val ) {
                auto* c = (decltype( ctx )*) q->cb_ptr;
                ( *c->called )++;
                *c->got = val;
        };
        CHECK_EQ( ASRT_SUCCESS, asrt_param_client_fetch_bool( &query, &client, 10U, cb, &ctx ) );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, t++ ) );
        uint8_t  rbuf[64];
        uint8_t* re = build_param_response_bool( rbuf, 10U, "k", 1U, 0U );
        CHECK_EQ( ASRT_SUCCESS, call_rtr_param_client_recv( &client, rbuf, re ) );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, t++ ) );
        CHECK_EQ( 1, called );
        CHECK_EQ( 1U, got );
        CHECK_EQ( 0U, query.error_code );
}

TEST_CASE_FIXTURE( param_client_ctx, "query_bool_type_mismatch" )
{
        make_ready( 1U );
        int called = 0;
        struct
        {
                int* called;
        } ctx   = { &called };
        auto cb = []( asrt_param_client*, asrt_param_query* q, uint32_t ) {
                auto* c = (decltype( ctx )*) q->cb_ptr;
                ( *c->called )++;
        };
        CHECK_EQ( ASRT_SUCCESS, asrt_param_client_fetch_bool( &query, &client, 10U, cb, &ctx ) );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, t++ ) );
        uint8_t  rbuf[64];
        uint8_t* re = build_param_response_str( rbuf, 10U, "k", "nope", 0U );
        CHECK_EQ( ASRT_SUCCESS, call_rtr_param_client_recv( &client, rbuf, re ) );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, t++ ) );
        CHECK_EQ( 1, called );
        CHECK_EQ( ASRT_PARAM_ERR_TYPE_MISMATCH, query.error_code );
}

// --- query_obj ---

TEST_CASE_FIXTURE( param_client_ctx, "query_obj_happy" )
{
        make_ready( 1U );
        int                  called = 0;
        asrt_flat_child_list got    = {};
        struct
        {
                int*                  called;
                asrt_flat_child_list* got;
        } ctx   = { &called, &got };
        auto cb = []( asrt_param_client*, asrt_param_query* q, asrt_flat_child_list val ) {
                auto* c = (decltype( ctx )*) q->cb_ptr;
                ( *c->called )++;
                *c->got = val;
        };
        CHECK_EQ( ASRT_SUCCESS, asrt_param_client_fetch_obj( &query, &client, 10U, cb, &ctx ) );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, t++ ) );
        uint8_t  rbuf[64];
        uint8_t* re = build_param_response_obj( rbuf, 10U, "k", 2U, 5U, 0U );
        CHECK_EQ( ASRT_SUCCESS, call_rtr_param_client_recv( &client, rbuf, re ) );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, t++ ) );
        CHECK_EQ( 1, called );
        CHECK_EQ( 2U, got.first_child );
        CHECK_EQ( 5U, got.last_child );
        CHECK_EQ( 0U, query.error_code );
}

TEST_CASE_FIXTURE( param_client_ctx, "query_obj_type_mismatch" )
{
        make_ready( 1U );
        int called = 0;
        struct
        {
                int* called;
        } ctx   = { &called };
        auto cb = []( asrt_param_client*, asrt_param_query* q, asrt_flat_child_list ) {
                auto* c = (decltype( ctx )*) q->cb_ptr;
                ( *c->called )++;
        };
        CHECK_EQ( ASRT_SUCCESS, asrt_param_client_fetch_obj( &query, &client, 10U, cb, &ctx ) );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, t++ ) );
        uint8_t  rbuf[64];
        uint8_t* re = build_param_response_u32( rbuf, 10U, "k", 1U, 0U );
        CHECK_EQ( ASRT_SUCCESS, call_rtr_param_client_recv( &client, rbuf, re ) );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, t++ ) );
        CHECK_EQ( 1, called );
        CHECK_EQ( ASRT_PARAM_ERR_TYPE_MISMATCH, query.error_code );
}

// --- query_arr ---

TEST_CASE_FIXTURE( param_client_ctx, "query_arr_happy" )
{
        make_ready( 1U );
        int                  called = 0;
        asrt_flat_child_list got    = {};
        struct
        {
                int*                  called;
                asrt_flat_child_list* got;
        } ctx   = { &called, &got };
        auto cb = []( asrt_param_client*, asrt_param_query* q, asrt_flat_child_list val ) {
                auto* c = (decltype( ctx )*) q->cb_ptr;
                ( *c->called )++;
                *c->got = val;
        };
        CHECK_EQ( ASRT_SUCCESS, asrt_param_client_fetch_arr( &query, &client, 10U, cb, &ctx ) );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, t++ ) );
        uint8_t  rbuf[64];
        uint8_t* re = build_param_response_arr( rbuf, 10U, "k", 3U, 6U, 0U );
        CHECK_EQ( ASRT_SUCCESS, call_rtr_param_client_recv( &client, rbuf, re ) );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, t++ ) );
        CHECK_EQ( 1, called );
        CHECK_EQ( 3U, got.first_child );
        CHECK_EQ( 6U, got.last_child );
        CHECK_EQ( 0U, query.error_code );
}

TEST_CASE_FIXTURE( param_client_ctx, "query_arr_type_mismatch" )
{
        make_ready( 1U );
        int called = 0;
        struct
        {
                int* called;
        } ctx   = { &called };
        auto cb = []( asrt_param_client*, asrt_param_query* q, asrt_flat_child_list ) {
                auto* c = (decltype( ctx )*) q->cb_ptr;
                ( *c->called )++;
        };
        CHECK_EQ( ASRT_SUCCESS, asrt_param_client_fetch_arr( &query, &client, 10U, cb, &ctx ) );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, t++ ) );
        uint8_t  rbuf[64];
        uint8_t* re = build_param_response_u32( rbuf, 10U, "k", 1U, 0U );
        CHECK_EQ( ASRT_SUCCESS, call_rtr_param_client_recv( &client, rbuf, re ) );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, t++ ) );
        CHECK_EQ( 1, called );
        CHECK_EQ( ASRT_PARAM_ERR_TYPE_MISMATCH, query.error_code );
}

// --- typed query: server error delivers null val ---

TEST_CASE_FIXTURE( param_client_ctx, "query_u32_server_error_delivers_null" )
{
        make_ready( 1U );
        int called = 0;
        struct
        {
                int* called;
        } ctx   = { &called };
        auto cb = []( asrt_param_client*, asrt_param_query* q, uint32_t ) {
                auto* c = (decltype( ctx )*) q->cb_ptr;
                ( *c->called )++;
        };
        CHECK_EQ( ASRT_SUCCESS, asrt_param_client_fetch_u32( &query, &client, 5U, cb, &ctx ) );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, t++ ) );
        uint8_t buf[8];
        CHECK_EQ(
            ASRT_SUCCESS,
            call_rtr_param_client_recv(
                &client, buf, build_param_error( buf, ASRT_PARAM_ERR_RESPONSE_TOO_LARGE, 5U ) ) );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, t++ ) );
        CHECK_EQ( 1, called );
        CHECK_EQ( ASRT_PARAM_ERR_RESPONSE_TOO_LARGE, query.error_code );
}

// --- pending query guard ---

TEST_CASE_FIXTURE( param_client_ctx, "query_rejects_when_pending" )
{
        make_ready( 1U );
        CHECK_EQ(
            ASRT_SUCCESS, asrt_param_client_fetch_any( &query, &client, 10U, query_cb, this ) );
        // second query while first is pending
        asrt_param_query query2 = {};
        CHECK_EQ(
            ASRT_ARG_ERR, asrt_param_client_fetch_any( &query2, &client, 11U, query_cb, this ) );
}

// --- query pending flag ---

TEST_CASE_FIXTURE( param_client_ctx, "query_pending_flag" )
{
        make_ready( 1U );
        CHECK_FALSE( asrt_param_query_pending( &client ) );

        CHECK_EQ(
            ASRT_SUCCESS, asrt_param_client_fetch_any( &query, &client, 10U, query_cb, this ) );
        CHECK( asrt_param_query_pending( &client ) );

        // deliver response
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, t++ ) );
        coll.data.clear();
        uint8_t  rbuf[64];
        uint8_t* re = build_param_response_u32( rbuf, 10U, "k", 42U, 0U );
        CHECK_EQ( ASRT_SUCCESS, call_rtr_param_client_recv( &client, rbuf, re ) );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, t++ ) );

        CHECK_FALSE( asrt_param_query_pending( &client ) );
        CHECK_EQ( 1, resp_called );
}

// ============================================================================
// Query timeout tests
// ============================================================================

namespace
{

struct param_timeout_ctx
{
        static constexpr uint32_t bugg_size = 256;
        static constexpr uint32_t timeout   = 10;

        asrt_send_req_list       send_queue         = {};
        struct asrt_node         head               = {};
        struct asrt_param_client client             = {};
        uint8_t                  msg_buf[bugg_size] = {};
        collector                coll;
        struct asrt_param_query  query = {};

        int           cb_called  = 0;
        uint8_t       error_code = 0;
        asrt::flat_id resp_id    = 0;
        uint32_t      resp_u32   = 0;
        uint32_t      t          = 1;

        static void query_cb(
            struct asrt_param_client*,
            struct asrt_param_query* q,
            struct asrt_flat_value   val )
        {
                auto* ctx = (param_timeout_ctx*) q->cb_ptr;
                ctx->cb_called++;
                ctx->error_code = q->error_code;
                ctx->resp_id    = q->node_id;
                ctx->resp_u32   = val.data.s.u32_val;
        }

        void make_ready( asrt::flat_id root_id = 1U )
        {
                uint8_t buf[8];
                REQUIRE_EQ(
                    ASRT_SUCCESS,
                    call_rtr_param_client_recv( &client, buf, build_param_ready( buf, root_id ) ) );
                REQUIRE_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, t++ ) );
                drain_send_queue( &send_queue, &coll );
                coll.data.clear();
        }

        param_timeout_ctx()
        {
                head.chid           = ASRT_CORE;
                head.send_queue     = &send_queue;
                struct asrt_span mb = { .b = msg_buf, .e = msg_buf + bugg_size };
                REQUIRE_EQ( ASRT_SUCCESS, asrt_param_client_init( &client, &head, mb, timeout ) );
        }
        ~param_timeout_ctx() { asrt_param_client_deinit( &client ); }
};

}  // namespace

TEST_CASE_FIXTURE( param_timeout_ctx, "query_timeout_fires_after_deadline" )
{
        make_ready( 1U );

        CHECK_EQ(
            ASRT_SUCCESS, asrt_param_client_fetch_any( &query, &client, 10U, query_cb, this ) );
        // first tick: DELIVER → cache miss → sends wire query
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, 100 ) );
        CHECK_EQ( 0, cb_called );

        // second tick: NONE with pending_query → starts timing (query_start = 105)
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, 105 ) );
        CHECK_EQ( 0, cb_called );

        // tick just before timeout (105 + 10 - 1 = 114): no timeout
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, 114 ) );
        CHECK_EQ( 0, cb_called );

        // tick at timeout (115 - 105 = 10 >= TIMEOUT): fires
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, 115 ) );
        CHECK_EQ( 1, cb_called );
        CHECK_EQ( ASRT_PARAM_ERR_TIMEOUT, error_code );
        CHECK_FALSE( asrt_param_query_pending( &client ) );
}

TEST_CASE_FIXTURE( param_timeout_ctx, "query_no_timeout_when_response_arrives" )
{
        make_ready( 1U );

        CHECK_EQ(
            ASRT_SUCCESS, asrt_param_client_fetch_any( &query, &client, 10U, query_cb, this ) );
        // DELIVER tick → cache miss → wire
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, 100 ) );

        // start timing
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, 102 ) );

        // response arrives before timeout
        uint8_t  rbuf[64];
        uint8_t* re = build_param_response_u32( rbuf, 10U, "k", 42U, 0U );
        CHECK_EQ( ASRT_SUCCESS, call_rtr_param_client_recv( &client, rbuf, re ) );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, 105 ) );

        CHECK_EQ( 1, cb_called );
        CHECK_EQ( 0U, error_code );
        CHECK_EQ( 42U, resp_u32 );
}

TEST_CASE_FIXTURE( param_timeout_ctx, "query_timeout_resets_for_new_query" )
{
        make_ready( 1U );

        // First query — let it timeout
        CHECK_EQ(
            ASRT_SUCCESS, asrt_param_client_fetch_any( &query, &client, 10U, query_cb, this ) );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, 100 ) );  // DELIVER → wire
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, 100 ) );  // start timing
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, 200 ) );  // timeout
        CHECK_EQ( 1, cb_called );
        CHECK_EQ( ASRT_PARAM_ERR_TIMEOUT, error_code );

        // Reset state
        cb_called  = 0;
        error_code = 0;

        // Second query — should start fresh timing
        CHECK_EQ(
            ASRT_SUCCESS, asrt_param_client_fetch_any( &query, &client, 20U, query_cb, this ) );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, 300 ) );  // DELIVER → wire
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, 300 ) );  // start timing
        // not timed out yet
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, 305 ) );
        CHECK_EQ( 0, cb_called );

        // now timeout
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, 310 ) );
        CHECK_EQ( 1, cb_called );
        CHECK_EQ( ASRT_PARAM_ERR_TIMEOUT, error_code );
}

TEST_CASE_FIXTURE( param_timeout_ctx, "query_timeout_with_typed_callback" )
{
        make_ready( 1U );

        int      called = 0;
        uint32_t got    = 99;
        struct
        {
                int*      called;
                uint32_t* got;
                uint8_t   err;
        } ctx   = { &called, &got, 0 };
        auto cb = []( asrt_param_client*, asrt_param_query* q, uint32_t val ) {
                auto* c = (decltype( ctx )*) q->cb_ptr;
                ( *c->called )++;
                *c->got = val;
                c->err  = q->error_code;
        };

        CHECK_EQ( ASRT_SUCCESS, asrt_param_client_fetch_u32( &query, &client, 10U, cb, &ctx ) );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, 50 ) );  // wire
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, 50 ) );  // start timing
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, 60 ) );  // timeout
        CHECK_EQ( 1, called );
        CHECK_EQ( ASRT_PARAM_ERR_TIMEOUT, ctx.err );
        CHECK_EQ( 0U, got );  // zero value on timeout
}

// ============================================================================
// asrt_collect_client — reactor COLL channel
// ============================================================================

static inline enum asrt_status call_collect_client_recv(
    struct asrt_collect_client* c,
    uint8_t*                    b,
    uint8_t*                    e )
{
        return asrt_chann_recv( &c->node, ( struct asrt_span ){ .b = b, .e = e } );
}

static uint8_t* build_coll_ready(
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

static uint8_t* build_coll_error( uint8_t* buf, uint8_t error_code )
{
        uint8_t* p = buf;
        *p++       = ASRT_COLLECT_MSG_ERROR;
        *p++       = error_code;
        return p;
}

namespace
{

struct collect_client_ctx
{
        asrt_send_req_list         send_queue = {};
        struct asrt_node           head       = {};
        struct asrt_collect_client client     = {};
        collector                  coll;

        void make_active( asrt::flat_id root_id = 1U )
        {
                uint8_t buf[16];
                REQUIRE_EQ(
                    ASRT_SUCCESS,
                    call_collect_client_recv( &client, buf, build_coll_ready( buf, root_id ) ) );
                REQUIRE_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, 0 ) );
                drain_send_queue( &send_queue, &coll );
                coll.data.clear();
        }

        collect_client_ctx()
        {
                head.chid       = ASRT_CORE;
                head.send_queue = &send_queue;
                REQUIRE_EQ( ASRT_SUCCESS, asrt_collect_client_init( &client, &head ) );
        }
        ~collect_client_ctx() = default;
};

}  // namespace

TEST_CASE( "asrt_collect_client_init" )
{
        asrt_send_req_list send_queue = {};
        struct asrt_node   head       = {};
        head.chid                     = ASRT_CORE;
        head.send_queue               = &send_queue;
        struct asrt_collect_client c  = {};

        CHECK_EQ( ASRT_INIT_ERR, asrt_collect_client_init( NULL, &head ) );
        CHECK_EQ( ASRT_INIT_ERR, asrt_collect_client_init( &c, NULL ) );

        CHECK_EQ( ASRT_SUCCESS, asrt_collect_client_init( &c, &head ) );
        CHECK_EQ( ASRT_COLL, c.node.chid );
        CHECK_NE( nullptr, (void*) (uintptr_t) c.node.e_cb_ptr );
        CHECK_EQ( &c.node, head.next );
        CHECK_EQ( ASRT_COLLECT_CLIENT_IDLE, c.state );
}

TEST_CASE_FIXTURE( collect_client_ctx, "asrt_collect_client_ready_handshake" )
{
        uint8_t buf[16];
        CHECK_EQ(
            ASRT_SUCCESS, call_collect_client_recv( &client, buf, build_coll_ready( buf, 42U ) ) );
        CHECK_EQ( ASRT_COLLECT_CLIENT_READY_RECV, client.state );
        CHECK_EQ( 42U, client.root_id );

        // tick enqueues READY_ACK; state is now READY_SENT
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, 0 ) );
        CHECK_EQ( ASRT_COLLECT_CLIENT_READY_SENT, client.state );

        // drain fires done_cb → ACTIVE
        drain_send_queue( &send_queue, &coll );
        CHECK_EQ( ASRT_COLLECT_CLIENT_ACTIVE, client.state );
        CHECK_EQ( 42U, asrt_collect_client_root_id( &client ) );

        // Verify READY_ACK was sent
        REQUIRE_EQ( 1U, coll.data.size() );
        auto& msg = coll.data.front();
        CHECK_EQ( ASRT_COLL, msg.id );
        REQUIRE_EQ( 1U, msg.data.size() );
        CHECK_EQ( ASRT_COLLECT_MSG_READY_ACK, msg.data[0] );
}

TEST_CASE_FIXTURE( collect_client_ctx, "asrt_collect_client_insert_sends_wire" )
{
        make_active( 1U );

        // insert transitions to INSERT_SENT while the send is in flight
        CHECK_EQ(
            ASRT_SUCCESS, asrt_collect_client_insert_u32( &client, 0, "alpha", 99, NULL, NULL ) );
        CHECK_EQ( ASRT_COLLECT_CLIENT_INSERT_SENT, client.state );
        drain_send_queue( &send_queue, &coll );
        CHECK_EQ( ASRT_COLLECT_CLIENT_ACTIVE, client.state );

        REQUIRE_EQ( 1U, coll.data.size() );
        auto& msg = coll.data.front();
        CHECK_EQ( ASRT_COLL, msg.id );
        // msg_id(1) + parent_id(4) + node_id(4) + "alpha\0"(6) + type(1) + u32(4) = 20
        REQUIRE_EQ( 20U, msg.data.size() );
        CHECK_EQ( ASRT_COLLECT_MSG_APPEND, msg.data[0] );
        assert_u32( 0U, msg.data.data() + 1 );  // parent_id
        assert_u32( 1U, msg.data.data() + 5 );  // node_id (auto-assigned)
        CHECK_EQ( std::string( "alpha" ), std::string( (char*) &msg.data[9] ) );
}

TEST_CASE_FIXTURE( collect_client_ctx, "asrt_collect_client_insert_before_active_returns_error" )
{
        CHECK_EQ( ASRT_ARG_ERR, asrt_collect_client_insert_u32( &client, 0, NULL, 1, NULL, NULL ) );
}

TEST_CASE_FIXTURE( collect_client_ctx, "asrt_collect_client_error_sets_fatal" )
{
        make_active( 1U );

        uint8_t buf[4];
        CHECK_EQ(
            ASRT_SUCCESS, call_collect_client_recv( &client, buf, build_coll_error( buf, 0x01 ) ) );
        CHECK_EQ( ASRT_COLLECT_CLIENT_ERROR, client.state );
}

TEST_CASE_FIXTURE( collect_client_ctx, "asrt_collect_client_insert_after_error_returns_error" )
{
        make_active( 1U );

        // Inject error
        uint8_t buf[4];
        call_collect_client_recv( &client, buf, build_coll_error( buf, 0x01 ) );

        CHECK_EQ( ASRT_ARG_ERR, asrt_collect_client_insert_u32( &client, 0, NULL, 1, NULL, NULL ) );
}

TEST_CASE_FIXTURE( collect_client_ctx, "asrt_collect_client_ready_from_error_re_handshakes" )
{
        make_active( 1U );

        // Inject error
        uint8_t buf[16];
        call_collect_client_recv( &client, buf, build_coll_error( buf, 0x01 ) );
        CHECK_EQ( ASRT_COLLECT_CLIENT_ERROR, client.state );

        // READY accepted even from ERROR state
        CHECK_EQ(
            ASRT_SUCCESS, call_collect_client_recv( &client, buf, build_coll_ready( buf, 99U ) ) );
        CHECK_EQ( ASRT_COLLECT_CLIENT_READY_RECV, client.state );

        // Complete handshake
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, 0 ) );
        drain_send_queue( &send_queue, &coll );
        CHECK_EQ( ASRT_COLLECT_CLIENT_ACTIVE, client.state );
        CHECK_EQ( 99U, asrt_collect_client_root_id( &client ) );

        // Insert works again, node_id counter starts from next_node_id
        coll.data.clear();
        CHECK_EQ( ASRT_SUCCESS, asrt_collect_client_insert_u32( &client, 0, "x", 7, NULL, NULL ) );
        drain_send_queue( &send_queue, &coll );
        REQUIRE_EQ( 1U, coll.data.size() );
        assert_u32( 1U, coll.data.front().data.data() + 5 );  // node_id starts from 1
}

TEST_CASE_FIXTURE( collect_client_ctx, "asrt_collect_client_ready_re_handshake" )
{
        make_active( 1U );

        // Second READY resets to handshake
        uint8_t buf[16];
        CHECK_EQ(
            ASRT_SUCCESS, call_collect_client_recv( &client, buf, build_coll_ready( buf, 7U ) ) );
        CHECK_EQ( ASRT_COLLECT_CLIENT_READY_RECV, client.state );

        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, 0 ) );
        CHECK_EQ( ASRT_COLLECT_CLIENT_READY_SENT, client.state );
        drain_send_queue( &send_queue, &coll );
        CHECK_EQ( ASRT_COLLECT_CLIENT_ACTIVE, client.state );
        CHECK_EQ( 7U, asrt_collect_client_root_id( &client ) );
}

TEST_CASE_FIXTURE( collect_client_ctx, "asrt_collect_client_tick_idle_is_noop" )
{
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, 0 ) );
        CHECK_EQ( ASRT_COLLECT_CLIENT_IDLE, client.state );
        CHECK( coll.data.empty() );
}

TEST_CASE_FIXTURE( collect_client_ctx, "asrt_collect_client_insert_done_cb_called_on_success" )
{
        make_active( 1U );

        int              called = 0;
        enum asrt_status cb_st  = ASRT_SUCCESS;
        // Use a stateful lambda via a lambda-to-fptr adapter
        struct ctx_s
        {
                int*              called;
                enum asrt_status* st;
        } ctx{ &called, &cb_st };
        auto fptr = []( void* p, enum asrt_status st ) {
                auto* c = static_cast< ctx_s* >( p );
                *c->called += 1;
                *c->st = st;
        };

        asrt_flat_value val_x = { .type = ASRT_FLAT_STYPE_U32, .data = {} };
        CHECK_EQ(
            ASRT_SUCCESS,
            asrt_collect_client_insert( &client, 0, "x", &val_x, nullptr, fptr, &ctx ) );
        CHECK_EQ( ASRT_COLLECT_CLIENT_INSERT_SENT, client.state );
        CHECK_EQ( 0, called );  // not yet

        drain_send_queue( &send_queue, &coll );
        CHECK_EQ( 1, called );
        CHECK_EQ( ASRT_SUCCESS, cb_st );
        CHECK_EQ( ASRT_COLLECT_CLIENT_ACTIVE, client.state );
}

TEST_CASE_FIXTURE( collect_client_ctx, "asrt_collect_client_insert_done_cb_called_on_send_failure" )
{
        make_active( 1U );

        int              called = 0;
        enum asrt_status cb_st  = ASRT_SUCCESS;
        struct ctx_s
        {
                int*              called;
                enum asrt_status* st;
        } ctx{ &called, &cb_st };
        auto fptr = []( void* p, enum asrt_status st ) {
                auto* c = static_cast< ctx_s* >( p );
                *c->called += 1;
                *c->st = st;
        };

        asrt_flat_value val_y = { .type = ASRT_FLAT_STYPE_U32, .data = {} };
        CHECK_EQ(
            ASRT_SUCCESS,
            asrt_collect_client_insert( &client, 0, "y", &val_y, nullptr, fptr, &ctx ) );

        drain_send_queue_ex( &send_queue, &coll, ASRT_SEND_ERR );
        CHECK_EQ( 1, called );
        CHECK_EQ( ASRT_SEND_ERR, cb_st );
        // send failure → ERROR state (mirrors reactor wait_send_done pattern)
        CHECK_EQ( ASRT_COLLECT_CLIENT_ERROR, client.state );
}

TEST_CASE_FIXTURE( collect_client_ctx, "asrt_collect_client_insert_busy_while_in_flight" )
{
        make_active( 1U );

        CHECK_EQ( ASRT_SUCCESS, asrt_collect_client_insert_u32( &client, 0, "a", 1, NULL, NULL ) );
        CHECK_EQ( ASRT_COLLECT_CLIENT_INSERT_SENT, client.state );

        // second insert while first is still in flight → BUSY
        CHECK_EQ( ASRT_BUSY_ERR, asrt_collect_client_insert_u32( &client, 0, "b", 2, NULL, NULL ) );

        // after drain the slot is free → can insert again
        drain_send_queue( &send_queue, &coll );
        CHECK_EQ( ASRT_COLLECT_CLIENT_ACTIVE, client.state );
        coll.data.clear();
        CHECK_EQ( ASRT_SUCCESS, asrt_collect_client_insert_u32( &client, 0, "b", 2, NULL, NULL ) );
        drain_send_queue( &send_queue, &coll );
        REQUIRE_EQ( 1U, coll.data.size() );
}

// =====================================================================
// stream client tests
// =====================================================================

#include "../asrtr/stream.h"

namespace
{

struct strm_client_ctx
{
        collector                 coll;
        struct asrt_send_req_list send_queue = {};
        asrt_node                 root       = {};
        asrt_stream_client        client     = {};

        strm_client_ctx()
        {
                root.send_queue = &send_queue;
                CHECK_EQ( ASRT_SUCCESS, asrt_stream_client_init( &client, &root ) );
        }
        ~strm_client_ctx() { coll.data.clear(); }
        void drain( enum asrt_status st = ASRT_SUCCESS )
        {
                drain_send_queue_ex( &send_queue, &coll, st );
        }
};

}  // namespace

// --- init ---

TEST_CASE( "strm_client_init: null client" )
{
        asrt_node root = {};
        CHECK_EQ( ASRT_INIT_ERR, asrt_stream_client_init( nullptr, &root ) );
}

TEST_CASE( "strm_client_init: null prev" )
{
        asrt_stream_client client = {};
        CHECK_EQ( ASRT_INIT_ERR, asrt_stream_client_init( &client, nullptr ) );
}

TEST_CASE_FIXTURE( strm_client_ctx, "strm_client_init: valid" )
{
        CHECK_EQ( ASRT_STRM_IDLE, client.state );
        CHECK_EQ( ASRT_STRM, client.node.chid );
        CHECK_EQ( &client.node, root.next );
}

// --- define ---

TEST_CASE_FIXTURE( strm_client_ctx, "strm_client_define: null client" )
{
        uint8_t fields[] = { ASRT_STRM_FIELD_U8 };
        CHECK_EQ(
            ASRT_ARG_ERR,
            asrt_stream_client_define(
                nullptr, 1, fields, sizeof( fields ), 1, nullptr, nullptr ) );
}

TEST_CASE_FIXTURE( strm_client_ctx, "strm_client_define: null fields" )
{
        CHECK_EQ(
            ASRT_ARG_ERR,
            asrt_stream_client_define( &client, 1, nullptr, 0, 1, nullptr, nullptr ) );
}

TEST_CASE_FIXTURE( strm_client_ctx, "strm_client_define: zero field_count" )
{
        uint8_t fields[] = { ASRT_STRM_FIELD_U8 };
        CHECK_EQ(
            ASRT_ARG_ERR,
            asrt_stream_client_define(
                &client, 1, fields, sizeof( fields ), 0, nullptr, nullptr ) );
}

TEST_CASE_FIXTURE( strm_client_ctx, "strm_client_define: valid sets DEFINE_SEND" )
{
        uint8_t fields[] = { ASRT_STRM_FIELD_U8, ASRT_STRM_FIELD_U16 };
        CHECK_EQ(
            ASRT_SUCCESS,
            asrt_stream_client_define(
                &client, 5, fields, sizeof( fields ), 2, nullptr, nullptr ) );
        CHECK_EQ( ASRT_STRM_DEFINE_SEND, client.state );
        CHECK_EQ( 5, client.op.define.schema_id );
        CHECK_EQ( 2, client.op.define.field_count );
        CHECK_EQ( fields, client.op.define.field_bytes );
}

TEST_CASE_FIXTURE( strm_client_ctx, "strm_client_define: stores done_cb" )
{
        bool                called = false;
        asrt_stream_done_cb cb     = []( void* p, enum asrt_status ) {
                *static_cast< bool* >( p ) = true;
        };
        uint8_t fields[] = { ASRT_STRM_FIELD_U8 };
        CHECK_EQ(
            ASRT_SUCCESS,
            asrt_stream_client_define( &client, 0, fields, sizeof( fields ), 1, cb, &called ) );
        CHECK_EQ( cb, client.done_cb );
        CHECK_EQ( &called, client.done_cb_ptr );
}

TEST_CASE_FIXTURE( strm_client_ctx, "strm_client_define: not idle returns BUSY_ERR" )
{
        uint8_t fields[] = { ASRT_STRM_FIELD_U8 };
        CHECK_EQ(
            ASRT_SUCCESS,
            asrt_stream_client_define(
                &client, 0, fields, sizeof( fields ), 1, nullptr, nullptr ) );
        // Now in DEFINE_SEND, second define should fail
        CHECK_EQ(
            ASRT_BUSY_ERR,
            asrt_stream_client_define(
                &client, 1, fields, sizeof( fields ), 1, nullptr, nullptr ) );
}

TEST_CASE_FIXTURE( strm_client_ctx, "strm_client_define: BUSY for each non-idle state" )
{
        uint8_t fields[] = { ASRT_STRM_FIELD_U8 };

        SUBCASE( "DEFINE_SEND" )
        {
                client.state = ASRT_STRM_DEFINE_SEND;
        }
        SUBCASE( "WAIT" )
        {
                client.state = ASRT_STRM_WAIT;
        }
        SUBCASE( "DONE" )
        {
                client.state = ASRT_STRM_DONE;
        }
        SUBCASE( "ERROR" )
        {
                client.state = ASRT_STRM_ERROR;
        }

        CHECK_EQ(
            ASRT_BUSY_ERR,
            asrt_stream_client_define(
                &client, 0, fields, sizeof( fields ), 1, nullptr, nullptr ) );
}

// --- tick ---

TEST_CASE_FIXTURE( strm_client_ctx, "strm_client_tick: idle is noop" )
{
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, 0 ) );
        CHECK_EQ( ASRT_STRM_IDLE, client.state );
        CHECK( coll.data.empty() );
}

TEST_CASE_FIXTURE( strm_client_ctx, "strm_client_tick: WAIT is noop" )
{
        client.state = ASRT_STRM_WAIT;
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, 0 ) );
        CHECK_EQ( ASRT_STRM_WAIT, client.state );
}

TEST_CASE_FIXTURE( strm_client_ctx, "strm_client_tick: ERROR is noop" )
{
        client.state = ASRT_STRM_ERROR;
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, 0 ) );
        CHECK_EQ( ASRT_STRM_ERROR, client.state );
}

TEST_CASE_FIXTURE( strm_client_ctx, "strm_client_tick: DEFINE_SEND sends and sync-completes" )
{
        uint8_t     fields[]   = { ASRT_STRM_FIELD_U32, ASRT_STRM_FIELD_I8 };
        bool        done_fired = false;
        asrt_status done_st    = ASRT_SIZE_ERR;
        auto        done_cb    = []( void* p, enum asrt_status s ) {
                auto* pair    = static_cast< std::pair< bool*, asrt_status* >* >( p );
                *pair->first  = true;
                *pair->second = s;
        };
        auto pair = std::make_pair( &done_fired, &done_st );
        CHECK_EQ(
            ASRT_SUCCESS,
            asrt_stream_client_define( &client, 3, fields, sizeof( fields ), 2, done_cb, &pair ) );

        // Tick enqueues DEFINE message → state WAIT
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, 0 ) );
        CHECK_EQ( ASRT_STRM_WAIT, client.state );
        CHECK( !done_fired );

        // Drain fires send_done → state DONE
        drain();
        CHECK_EQ( ASRT_STRM_DONE, client.state );
        CHECK( !done_fired );

        // Second tick fires the done_cb
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, 0 ) );
        CHECK_EQ( ASRT_STRM_IDLE, client.state );
        CHECK( done_fired );
        CHECK_EQ( ASRT_SUCCESS, done_st );
        // One message should have been sent
        REQUIRE_EQ( 1U, coll.data.size() );
        CHECK_EQ( ASRT_STRM, coll.data[0].id );
        coll.data.clear();
}

TEST_CASE_FIXTURE( strm_client_ctx, "strm_client_tick: DEFINE_SEND with deferred done" )
{
        uint8_t fields[] = { ASRT_STRM_FIELD_U8 };
        CHECK_EQ(
            ASRT_SUCCESS,
            asrt_stream_client_define(
                &client, 0, fields, sizeof( fields ), 1, nullptr, nullptr ) );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, 0 ) );
        // Should be in WAIT, message enqueued but not yet drained
        CHECK_EQ( ASRT_STRM_WAIT, client.state );

        // Drain fires send_done → state DONE
        drain();
        CHECK_EQ( ASRT_STRM_DONE, client.state );

        // Tick should fire the (NULL) done_cb and return to IDLE
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, 0 ) );
        CHECK_EQ( ASRT_STRM_IDLE, client.state );
        coll.data.clear();
}

TEST_CASE_FIXTURE( strm_client_ctx, "strm_client_tick: DEFINE_SEND send failure" )
{
        uint8_t fields[] = { ASRT_STRM_FIELD_U8 };
        CHECK_EQ(
            ASRT_SUCCESS,
            asrt_stream_client_define(
                &client, 0, fields, sizeof( fields ), 1, nullptr, nullptr ) );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, 0 ) );
        CHECK_EQ( ASRT_STRM_WAIT, client.state );
        // Drain with error simulates send failure → state DONE with error send_status
        drain( ASRT_SEND_ERR );
        CHECK_EQ( ASRT_STRM_DONE, client.state );
        CHECK_EQ( ASRT_SEND_ERR, client.op.done.send_status );
}

TEST_CASE_FIXTURE( strm_client_ctx, "strm_client_tick: DONE fires user callback" )
{
        uint8_t     fields[] = { ASRT_STRM_FIELD_U8 };
        bool        cb_fired = false;
        asrt_status cb_st    = ASRT_INTERNAL_ERR;
        auto        done_cb  = []( void* p, enum asrt_status s ) {
                auto* pair    = static_cast< std::pair< bool*, asrt_status* >* >( p );
                *pair->first  = true;
                *pair->second = s;
        };
        auto pair = std::make_pair( &cb_fired, &cb_st );
        CHECK_EQ(
            ASRT_SUCCESS,
            asrt_stream_client_define( &client, 0, fields, sizeof( fields ), 1, done_cb, &pair ) );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, 0 ) );
        CHECK_EQ( ASRT_STRM_WAIT, client.state );

        drain();
        CHECK_EQ( ASRT_STRM_DONE, client.state );
        CHECK( !cb_fired );

        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, 0 ) );
        CHECK( cb_fired );
        CHECK_EQ( ASRT_SUCCESS, cb_st );
        CHECK_EQ( ASRT_STRM_IDLE, client.state );
        coll.data.clear();
}

TEST_CASE_FIXTURE( strm_client_ctx, "strm_client_tick: DONE with NULL callback" )
{
        uint8_t fields[] = { ASRT_STRM_FIELD_U8 };
        CHECK_EQ(
            ASRT_SUCCESS,
            asrt_stream_client_define(
                &client, 0, fields, sizeof( fields ), 1, nullptr, nullptr ) );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, 0 ) );
        drain();
        CHECK_EQ( ASRT_STRM_DONE, client.state );

        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, 0 ) );
        CHECK_EQ( ASRT_STRM_IDLE, client.state );
        coll.data.clear();
}

// --- record ---

TEST_CASE_FIXTURE( strm_client_ctx, "strm_client_emit: null client" )
{
        uint8_t data[] = { 1 };
        CHECK_EQ( ASRT_ARG_ERR, asrt_stream_client_emit( nullptr, 0, data, 1, nullptr, nullptr ) );
}

TEST_CASE_FIXTURE( strm_client_ctx, "strm_client_emit: null data with nonzero size" )
{
        CHECK_EQ(
            ASRT_ARG_ERR, asrt_stream_client_emit( &client, 0, nullptr, 5, nullptr, nullptr ) );
}

TEST_CASE_FIXTURE( strm_client_ctx, "strm_client_emit: null data with zero size is error" )
{
        CHECK_EQ(
            ASRT_ARG_ERR, asrt_stream_client_emit( &client, 0, nullptr, 0, nullptr, nullptr ) );
}

TEST_CASE_FIXTURE( strm_client_ctx, "strm_client_emit: error state" )
{
        client.state   = ASRT_STRM_ERROR;
        uint8_t data[] = { 1 };
        CHECK_EQ(
            ASRT_INTERNAL_ERR, asrt_stream_client_emit( &client, 0, data, 1, nullptr, nullptr ) );
}

TEST_CASE_FIXTURE( strm_client_ctx, "strm_client_emit: send failure" )
{
        uint8_t data[] = { 1 };
        CHECK_EQ( ASRT_SUCCESS, asrt_stream_client_emit( &client, 0, data, 1, nullptr, nullptr ) );
        CHECK_EQ( ASRT_STRM_WAIT, client.state );
        // Drain with error simulates send failure → state DONE with error send_status
        drain( ASRT_SEND_ERR );
        CHECK_EQ( ASRT_STRM_DONE, client.state );
        CHECK_EQ( ASRT_SEND_ERR, client.op.done.send_status );
}

TEST_CASE_FIXTURE( strm_client_ctx, "strm_client_emit: valid sends DATA message" )
{
        uint8_t data[] = { 0xAA, 0xBB };
        CHECK_EQ( ASRT_SUCCESS, asrt_stream_client_emit( &client, 7, data, 2, nullptr, nullptr ) );
        // emit enqueues DATA message → state WAIT; drain fires send_done → DONE; tick → IDLE
        CHECK_EQ( ASRT_STRM_WAIT, client.state );
        drain();
        CHECK_EQ( ASRT_STRM_DONE, client.state );
        REQUIRE_EQ( 1U, coll.data.size() );
        CHECK_EQ( ASRT_STRM, coll.data[0].id );
        REQUIRE_EQ( 4U, coll.data[0].data.size() );
        CHECK_EQ( ASRT_STRM_MSG_DATA, coll.data[0].data[0] );
        CHECK_EQ( 7, coll.data[0].data[1] );
        CHECK_EQ( 0xAA, coll.data[0].data[2] );
        CHECK_EQ( 0xBB, coll.data[0].data[3] );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, 0 ) );
        CHECK_EQ( ASRT_STRM_IDLE, client.state );
        coll.data.clear();
}

TEST_CASE_FIXTURE( strm_client_ctx, "strm_client_emit: done_cb fires via tick" )
{
        asrt_status cb_status = ASRT_INTERNAL_ERR;
        auto        cb        = []( void* p, enum asrt_status s ) {
                *static_cast< asrt_status* >( p ) = s;
        };
        uint8_t data[] = { 1 };
        CHECK_EQ( ASRT_SUCCESS, asrt_stream_client_emit( &client, 0, data, 1, cb, &cb_status ) );
        CHECK_EQ( ASRT_STRM_WAIT, client.state );
        drain();
        CHECK_EQ( ASRT_STRM_DONE, client.state );

        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, 0 ) );
        CHECK_EQ( ASRT_SUCCESS, cb_status );
        CHECK_EQ( ASRT_STRM_IDLE, client.state );
        CHECK_EQ( nullptr, client.done_cb );
        CHECK_EQ( nullptr, client.done_cb_ptr );
        coll.data.clear();
}

// --- recv ---

TEST_CASE_FIXTURE( strm_client_ctx, "strm_client_recv: empty buffer is noop" )
{
        uint8_t          buf[1];
        struct asrt_span sp = { .b = buf, .e = buf };
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_recv( &client.node, sp ) );
        CHECK_EQ( ASRT_STRM_IDLE, client.state );
}

TEST_CASE_FIXTURE( strm_client_ctx, "strm_client_recv: error message sets ERROR state" )
{
        uint8_t          buf[] = { ASRT_STRM_MSG_ERROR, ASRT_STRM_ERR_UNKNOWN_SCHEMA };
        struct asrt_span sp    = { .b = buf, .e = buf + 2 };
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_recv( &client.node, sp ) );
        CHECK_EQ( ASRT_STRM_ERROR, client.state );
        CHECK_EQ( ASRT_STRM_ERR_UNKNOWN_SCHEMA, client.err_code );
}

TEST_CASE_FIXTURE( strm_client_ctx, "strm_client_recv: error message too short" )
{
        uint8_t          buf[] = { ASRT_STRM_MSG_ERROR };
        struct asrt_span sp    = { .b = buf, .e = buf + 1 };
        CHECK_EQ( ASRT_RECV_ERR, asrt_chann_recv( &client.node, sp ) );
}

TEST_CASE_FIXTURE( strm_client_ctx, "strm_client_recv: unknown message id" )
{
        uint8_t          buf[] = { 0xFF };
        struct asrt_span sp    = { .b = buf, .e = buf + 1 };
        CHECK_EQ( ASRT_RECV_ERR, asrt_chann_recv( &client.node, sp ) );
}

// --- send_done preserves ERROR ---

TEST_CASE_FIXTURE( strm_client_ctx, "strm_client: send_done preserves ERROR state" )
{
        uint8_t fields[] = { ASRT_STRM_FIELD_U8 };
        CHECK_EQ(
            ASRT_SUCCESS,
            asrt_stream_client_define(
                &client, 0, fields, sizeof( fields ), 1, nullptr, nullptr ) );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, 0 ) );
        CHECK_EQ( ASRT_STRM_WAIT, client.state );

        // Simulate controller error arriving before send completes
        uint8_t          err_buf[] = { ASRT_STRM_MSG_ERROR, ASRT_STRM_ERR_INVALID_DEFINE };
        struct asrt_span sp        = { .b = err_buf, .e = err_buf + 2 };
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_recv( &client.node, sp ) );
        CHECK_EQ( ASRT_STRM_ERROR, client.state );

        // Now send completes — should NOT overwrite ERROR
        drain();
        CHECK_EQ( ASRT_STRM_ERROR, client.state );
        coll.data.clear();
}

// --- reset ---

TEST_CASE( "strm_client_reset: null client" )
{
        CHECK_EQ( ASRT_INIT_ERR, asrt_stream_client_reset( nullptr ) );
}

TEST_CASE_FIXTURE( strm_client_ctx, "strm_client_reset: IDLE" )
{
        CHECK_EQ( ASRT_SUCCESS, asrt_stream_client_reset( &client ) );
        CHECK_EQ( ASRT_STRM_IDLE, client.state );
}

TEST_CASE_FIXTURE( strm_client_ctx, "strm_client_reset: DEFINE_SEND returns BUSY" )
{
        uint8_t fields[] = { ASRT_STRM_FIELD_U8 };
        asrt_stream_client_define( &client, 0, fields, sizeof( fields ), 1, nullptr, nullptr );
        CHECK_EQ( ASRT_BUSY_ERR, asrt_stream_client_reset( &client ) );
}

TEST_CASE_FIXTURE( strm_client_ctx, "strm_client_reset: WAIT returns BUSY" )
{
        client.state = ASRT_STRM_WAIT;
        CHECK_EQ( ASRT_BUSY_ERR, asrt_stream_client_reset( &client ) );
}

TEST_CASE_FIXTURE( strm_client_ctx, "strm_client_reset: DONE resets to IDLE" )
{
        client.state = ASRT_STRM_DONE;
        CHECK_EQ( ASRT_SUCCESS, asrt_stream_client_reset( &client ) );
        CHECK_EQ( ASRT_STRM_IDLE, client.state );
}

TEST_CASE_FIXTURE( strm_client_ctx, "strm_client_reset: ERROR resets to IDLE" )
{
        client.state    = ASRT_STRM_ERROR;
        client.err_code = ASRT_STRM_ERR_UNKNOWN_SCHEMA;
        CHECK_EQ( ASRT_SUCCESS, asrt_stream_client_reset( &client ) );
        CHECK_EQ( ASRT_STRM_IDLE, client.state );
        CHECK_EQ( ASRT_STRM_ERR_SUCCESS, client.err_code );
}

TEST_CASE_FIXTURE( strm_client_ctx, "strm_client_reset: clears done_cb" )
{
        client.state       = ASRT_STRM_DONE;
        client.done_cb     = []( void*, enum asrt_status ) {};
        client.done_cb_ptr = (void*) 0xBEEF;
        CHECK_EQ( ASRT_SUCCESS, asrt_stream_client_reset( &client ) );
        CHECK_EQ( nullptr, client.done_cb );
        CHECK_EQ( nullptr, client.done_cb_ptr );
}

// --- full define→tick→idle cycle ---

TEST_CASE_FIXTURE( strm_client_ctx, "strm_client: full define→tick cycle with callback" )
{
        asrt_status cb_status = ASRT_INTERNAL_ERR;
        auto        cb        = []( void* p, enum asrt_status s ) {
                *static_cast< asrt_status* >( p ) = s;
        };
        uint8_t fields[] = { ASRT_STRM_FIELD_U16 };
        CHECK_EQ(
            ASRT_SUCCESS,
            asrt_stream_client_define( &client, 2, fields, sizeof( fields ), 1, cb, &cb_status ) );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, 0 ) );
        drain();  // fires asrt_stream_send_done → ASRT_STRM_DONE
        CHECK_EQ( ASRT_STRM_DONE, client.state );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, 0 ) );
        CHECK_EQ( ASRT_STRM_IDLE, client.state );
        CHECK_EQ( ASRT_SUCCESS, cb_status );

        // Verify define message payload
        REQUIRE_EQ( 1U, coll.data.size() );
        auto& msg = coll.data[0].data;
        REQUIRE_GE( msg.size(), 4U );
        CHECK_EQ( ASRT_STRM_MSG_DEFINE, msg[0] );
        CHECK_EQ( 2, msg[1] );
        CHECK_EQ( 1, msg[2] );
        CHECK_EQ( ASRT_STRM_FIELD_U16, msg[3] );
        coll.data.clear();
}

// --- define then record ---

TEST_CASE_FIXTURE( strm_client_ctx, "strm_client: define then record" )
{
        uint8_t fields[] = { ASRT_STRM_FIELD_U8 };
        CHECK_EQ(
            ASRT_SUCCESS,
            asrt_stream_client_define(
                &client, 1, fields, sizeof( fields ), 1, nullptr, nullptr ) );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, 0 ) );
        drain();  // fires asrt_stream_send_done → ASRT_STRM_DONE
        CHECK_EQ( ASRT_STRM_DONE, client.state );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, 0 ) );
        CHECK_EQ( ASRT_STRM_IDLE, client.state );
        coll.data.clear();

        uint8_t rec_data[] = { 42 };
        CHECK_EQ(
            ASRT_SUCCESS, asrt_stream_client_emit( &client, 1, rec_data, 1, nullptr, nullptr ) );
        drain();  // fires asrt_stream_send_done → ASRT_STRM_DONE
        CHECK_EQ( ASRT_STRM_DONE, client.state );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &client.node, 0 ) );
        CHECK_EQ( ASRT_STRM_IDLE, client.state );
        REQUIRE_EQ( 1U, coll.data.size() );
        CHECK_EQ( ASRT_STRM_MSG_DATA, coll.data[0].data[0] );
        CHECK_EQ( 1, coll.data[0].data[1] );
        CHECK_EQ( 42, coll.data[0].data[2] );
        coll.data.clear();
}
