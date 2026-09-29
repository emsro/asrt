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
#include "../asrtc/collect.h"
#include "../asrtc/controller.h"
#include "../asrtc/diag.h"
#include "../asrtc/param.h"
#include "../asrtl/collect_proto.h"
#include "../asrtl/core_proto.h"
#include "../asrtl/log.h"
#include "../asrtl/proto_version.h"
#include "../asrtl/util.h"
#include "../asrtlpp/flat_type_traits.hpp"
#include "../asrtr/collect.h"
#include "../asrtr/param.h"
#include "./collector.hpp"
#include "./stub_allocator.hpp"
#include "./util.hpp"

#include <doctest/doctest.h>

static ASRT_DEFINE_GPOS_LOG()

    namespace
{
}

//---------------------------------------------------------------------
// lib
namespace
{
void check_recv( auto* c, struct asrt_span msg )
{
        enum asrt_status st = asrt_chann_recv( &c->node, msg );
        CHECK_EQ( ASRT_SUCCESS, st );
}

void check_tick( auto* c, uint32_t now )
{
        enum asrt_status st = asrt_chann_tick( &c->node, now );
        CHECK_EQ( ASRT_SUCCESS, st );
}

void check_recv_and_spin( struct asrt_controller* c, uint8_t* beg, uint8_t* end, uint32_t* now )
{
        check_recv( c, ( struct asrt_span ){ .b = beg, .e = end } );
        int       i = 0;
        int const n = 1000;
        for ( ; i < n && !asrt_cntr_idle( c ); i++ )
                check_tick( c, ( *now )++ );
        CHECK_NE( i, n );
}

struct controller_ctx
{
        struct asrt_controller    cntr;
        collector                 coll;
        struct asrt_send_req_list send_queue = {};
        enum asrt_status          init_status;
        uint32_t                  t = 1;

        controller_ctx() {}
        ~controller_ctx() { CHECK_EQ( coll.data.size(), 0 ); }
        void drain() { drain_send_queue( &send_queue, &coll ); }
};

struct asrt_span flatten_req( uint8_t* buf, struct asrt_send_req const* req )
{
        uint8_t* p = buf;
        memcpy( p, req->buff.b, req->buff.e - req->buff.b );
        p += req->buff.e - req->buff.b;
        for ( asrt_rec_span const* s = req->buff.next; s != nullptr; s = s->next ) {
                size_t sz = (size_t) ( s->e - s->b );
                memcpy( p, s->b, sz );
                p += sz;
        }
        return ( struct asrt_span ){ .b = buf, .e = p };
}

enum asrt_status record_init_cb( void* ptr, enum asrt_status s )
{
        enum asrt_status* p = (enum asrt_status*) ptr;
        *p                  = s;
        return ASRT_SUCCESS;
}

void check_cntr_full_init( controller_ctx* ctx )
{
        enum asrt_status st =
            asrt_cntr_init( &ctx->cntr, &ctx->send_queue, asrt_default_allocator() );
        CHECK_EQ( ASRT_SUCCESS, st );
        st = asrt_cntr_start( &ctx->cntr, &record_init_cb, &ctx->init_status, 1000 );
        CHECK_EQ( ASRT_SUCCESS, st );
        check_tick( &ctx->cntr, ctx->t++ );
        ctx->drain();

        assert_collected_core_hdr( ctx->coll.data.back(), 0x02, ASRT_MSG_PROTO_VERSION );
        ctx->coll.data.pop_back();

        struct asrt_u8d8msg   msg = {};
        struct asrt_send_req* req = asrt_msg_rtoc_proto_version( &msg, 0, 1, 0 );
        check_recv_and_spin( &ctx->cntr, req->buff.b, req->buff.e, &ctx->t );
        CHECK_EQ( ASRT_SUCCESS, ctx->init_status );
}
}  // namespace
//---------------------------------------------------------------------
// tests

TEST_CASE_FIXTURE( controller_ctx, "cntr_init" )
{
        enum asrt_status st;
        st = asrt_cntr_init( NULL, &send_queue, asrt_default_allocator() );
        CHECK_EQ( ASRT_INIT_ERR, st );

        st = asrt_cntr_init( &cntr, &send_queue, asrt_default_allocator() );
        CHECK_EQ( ASRT_SUCCESS, st );
        CHECK_EQ( ASRT_CORE, cntr.node.chid );
        CHECK( asrt_cntr_idle( &cntr ) );

        st = asrt_cntr_start( &cntr, &record_init_cb, &init_status, 1000 );
        CHECK_EQ( ASRT_SUCCESS, st );
        CHECK_EQ( ASRT_CNTR_INIT, cntr.state );
        CHECK( !asrt_cntr_idle( &cntr ) );

        check_tick( &cntr, t++ );
        drain();

        assert_collected_core_hdr( coll.data.back(), 0x02, ASRT_MSG_PROTO_VERSION );
        coll.data.pop_back();

        {
                struct asrt_u8d8msg   msg = {};
                struct asrt_send_req* req = asrt_msg_rtoc_proto_version( &msg, 0, 1, 0 );
                check_recv_and_spin( &cntr, req->buff.b, req->buff.e, &t );
        }

        CHECK( asrt_cntr_idle( &cntr ) );
        CHECK_EQ( ASRT_SUCCESS, init_status );
}

namespace
{

enum asrt_status cpy_desc_cb( void* ptr, enum asrt_status s, char const* desc )
{
        (void) s;
        std::string* p = (std::string*) ptr;
        *p             = desc;
        return ASRT_SUCCESS;
}

struct test_info_result
{
        uint16_t    tid;
        std::string desc;
};

enum asrt_status cpy_test_info_cb( void* ptr, enum asrt_status s, uint16_t tid, char const* desc )
{
        (void) s;
        struct test_info_result* r = (struct test_info_result*) ptr;
        r->tid                     = tid;
        r->desc                    = desc;
        return ASRT_SUCCESS;
}

struct test_info_cb_capture
{
        enum asrt_status status = ASRT_SUCCESS;
        uint16_t         tid    = 0;
        std::string      desc;
};

enum asrt_status cpy_test_info_capture_cb(
    void*            ptr,
    enum asrt_status s,
    uint16_t         tid,
    char const*      desc )
{
        auto* c   = (test_info_cb_capture*) ptr;
        c->status = s;
        c->tid    = tid;
        c->desc   = desc ? desc : "";
        return ASRT_SUCCESS;
}

}  // namespace

TEST_CASE_FIXTURE( controller_ctx, "cntr_desc" )
{
        enum asrt_status st;
        check_cntr_full_init( this );

        std::string desc;
        st = asrt_cntr_desc( &cntr, &cpy_desc_cb, (void*) &desc, 1000 );
        CHECK_EQ( ASRT_SUCCESS, st );
        check_tick( &cntr, t++ );
        drain();

        assert_collected_core_hdr( coll.data.back(), 0x02, ASRT_MSG_DESC );
        coll.data.pop_back();

        char const* msg = "wololo1";
        {
                struct asrt_core_desc_msg dmsg = {};
                struct asrt_send_req*     req  = asrt_msg_rtoc_desc( &dmsg, msg, strlen( msg ) );
                uint8_t                   flat[128];
                auto                      fsp = flatten_req( flat, req );
                check_recv_and_spin( &cntr, fsp.b, fsp.e, &t );
        }

        CHECK( desc != "" );
        CHECK( msg == desc );
}

static enum asrt_status cpy_u32_cb( void* ptr, enum asrt_status s, uint16_t x )
{
        (void) s;
        uint32_t* p = (uint32_t*) ptr;
        *p          = x;
        return ASRT_SUCCESS;
}

TEST_CASE_FIXTURE( controller_ctx, "cntr_test_count" )
{
        enum asrt_status st;
        check_cntr_full_init( this );

        uint32_t p = 0;
        st         = asrt_cntr_test_count( &cntr, &cpy_u32_cb, (void*) &p, 1000 );
        CHECK_EQ( ASRT_SUCCESS, st );
        check_tick( &cntr, t++ );
        drain();

        assert_collected_core_hdr( coll.data.back(), 0x02, ASRT_MSG_TEST_COUNT );
        coll.data.pop_back();

        {
                struct asrt_u8d4msg   msg = {};
                struct asrt_send_req* req = asrt_msg_rtoc_test_count( &msg, 42 );
                check_recv_and_spin( &cntr, req->buff.b, req->buff.e, &t );
        }

        CHECK_EQ( 42, p );
}

TEST_CASE_FIXTURE( controller_ctx, "cntr_test_info" )
{
        enum asrt_status st;
        check_cntr_full_init( this );

        struct test_info_result p = {};
        st = asrt_cntr_test_info( &cntr, 42, &cpy_test_info_cb, (void*) &p, 1000 );
        CHECK_EQ( ASRT_SUCCESS, st );
        check_tick( &cntr, t++ );
        drain();

        assert_collected_core_hdr( coll.data.back(), 0x04, ASRT_MSG_TEST_INFO );
        assert_u16( 42, coll.data.back().data.data() + 2 );
        coll.data.pop_back();

        char const* desc = "barbaz";
        {
                struct asrt_core_test_info_msg timsg = {};
                struct asrt_send_req*          req   = asrt_msg_rtoc_test_info(
                    &timsg, 42, ASRT_TEST_INFO_SUCCESS, desc, strlen( desc ) );
                uint8_t flat[128];
                auto    fsp = flatten_req( flat, req );
                check_recv_and_spin( &cntr, fsp.b, fsp.e, &t );
        }

        CHECK( desc == p.desc );
        CHECK_EQ( 42, p.tid );
}

TEST_CASE_FIXTURE( controller_ctx, "cntr_test_info_tid_mismatch" )
{
        check_cntr_full_init( this );

        struct test_info_result p = {};
        enum asrt_status st = asrt_cntr_test_info( &cntr, 42, &cpy_test_info_cb, (void*) &p, 1000 );
        CHECK_EQ( ASRT_SUCCESS, st );
        check_tick( &cntr, t++ );
        drain();
        coll.data.pop_back();

        // Reply with a different tid (99 instead of 42)
        char const* desc = "barbaz";
        {
                struct asrt_core_test_info_msg timsg = {};
                struct asrt_send_req*          req   = asrt_msg_rtoc_test_info(
                    &timsg, 99, ASRT_TEST_INFO_SUCCESS, desc, strlen( desc ) );
                uint8_t          flat[128];
                auto             fsp = flatten_req( flat, req );
                enum asrt_status rst =
                    asrt_chann_recv( &cntr.node, ( struct asrt_span ){ .b = fsp.b, .e = fsp.e } );
                CHECK_EQ( ASRT_RECV_ERR, rst );
        }

        CHECK( p.desc.empty() );
}

TEST_CASE_FIXTURE( controller_ctx, "cntr_test_info_missing_status" )
{
        check_cntr_full_init( this );

        test_info_cb_capture capture{};
        enum asrt_status     st =
            asrt_cntr_test_info( &cntr, 42, &cpy_test_info_capture_cb, &capture, 1000 );
        CHECK_EQ( ASRT_SUCCESS, st );
        check_tick( &cntr, t++ );
        drain();
        coll.data.pop_back();

        {
                struct asrt_core_test_info_msg timsg = {};
                struct asrt_send_req*          req =
                    asrt_msg_rtoc_test_info( &timsg, 42, ASRT_TEST_INFO_MISSING_TEST_ERR, "", 0 );
                uint8_t flat[64];
                auto    fsp = flatten_req( flat, req );
                check_recv_and_spin( &cntr, fsp.b, fsp.e, &t );
        }

        CHECK_EQ( ASRT_RECV_ERR, capture.status );
        CHECK_EQ( 42, capture.tid );
        CHECK( capture.desc.empty() );
}

static enum asrt_status result_cb( void* ptr, enum asrt_status s, struct asrt_result* res )
{
        (void) s;
        struct asrt_result* r1 = (struct asrt_result*) ptr;
        *r1                    = *res;
        return ASRT_SUCCESS;
}

static void* failing_alloc( void* ptr, uint32_t size )
{
        (void) ptr;
        (void) size;
        return NULL;
}
static void failing_free( void* ptr, void* mem )
{
        (void) ptr;
        (void) mem;
}
static struct asrt_allocator failing_allocator( void )
{
        return ( struct asrt_allocator ){
            .ptr   = NULL,
            .alloc = &failing_alloc,
            .free  = &failing_free,
        };
}

TEST_CASE_FIXTURE( controller_ctx, "cntr_desc_alloc_failure" )
{
        enum asrt_status st = asrt_cntr_init( &cntr, &send_queue, failing_allocator() );
        CHECK_EQ( ASRT_SUCCESS, st );
        st = asrt_cntr_start( &cntr, &record_init_cb, &init_status, 1000 );
        CHECK_EQ( ASRT_SUCCESS, st );
        check_tick( &cntr, t++ );
        drain();
        coll.data.pop_back();  // discard PROTO_VERSION request

        // Simulate receiving a proto-version reply to advance to IDLE
        {
                struct asrt_u8d8msg   msg = {};
                struct asrt_send_req* req = asrt_msg_rtoc_proto_version( &msg, 0, 1, 0 );
                check_recv_and_spin( &cntr, req->buff.b, req->buff.e, &t );
        }
        CHECK_EQ( ASRT_SUCCESS, init_status );

        std::string desc;
        st = asrt_cntr_desc( &cntr, &cpy_desc_cb, (void*) &desc, 1000 );
        CHECK_EQ( ASRT_SUCCESS, st );
        check_tick( &cntr, t++ );
        drain();
        coll.data.pop_back();  // discard DESC request

        // Send a DESC reply — alloc will return NULL, recv must return an error
        char const* msg2 = "hello";
        {
                struct asrt_core_desc_msg dmsg = {};
                struct asrt_send_req*     req  = asrt_msg_rtoc_desc( &dmsg, msg2, strlen( msg2 ) );
                uint8_t                   flat[64];
                auto                      fsp = flatten_req( flat, req );
                enum asrt_status          rst =
                    asrt_chann_recv( &cntr.node, ( struct asrt_span ){ .b = fsp.b, .e = fsp.e } );
                CHECK_NE( ASRT_SUCCESS, rst );
        }
}

TEST_CASE_FIXTURE( controller_ctx, "cntr_run_test" )
{
        enum asrt_status st;
        check_cntr_full_init( this );

        struct asrt_result res;
        st = asrt_cntr_test_exec( &cntr, 42, result_cb, &res, 1000 );
        CHECK_EQ( ASRT_SUCCESS, st );
        check_tick( &cntr, t++ );
        drain();

        assert_collected_core_hdr( coll.data.back(), 0x08, ASRT_MSG_TEST_START );
        assert_u16( 42, coll.data.back().data.data() + 2 );
        assert_u32( 0, coll.data.back().data.data() + 4 );
        coll.data.pop_back();

        for ( int i = 0; i < 4; i++ )
                check_tick( &cntr, t++ );

        {
                struct asrt_u8d8msg   tsmsg = {};
                struct asrt_send_req* tsreq = asrt_msg_rtoc_test_start( &tsmsg, 42, 0 );
                check_recv( &cntr, ( struct asrt_span ){ .b = tsreq->buff.b, .e = tsreq->buff.e } );
        }

        for ( int i = 0; i < 4; i++ )
                check_tick( &cntr, t++ );

        CHECK_EQ( coll.data.empty(), true );

        {
                struct asrt_u8d8msg   trmsg = {};
                struct asrt_send_req* trreq =
                    asrt_msg_rtoc_test_result( &trmsg, 0, ASRT_TEST_RESULT_SUCCESS );
                check_recv_and_spin( &cntr, trreq->buff.b, trreq->buff.e, &t );
        }

        CHECK_EQ( res.test_id, 42 );
        CHECK_EQ( res.run_id, 0 );
        CHECK_EQ( res.res, ASRT_TEST_RESULT_SUCCESS );
        CHECK_EQ( coll.data.empty(), true );
}

TEST_CASE_FIXTURE( controller_ctx, "realloc_str_long_string" )
{
        // Strings longer than 10000 bytes must be handled — the span bound is sufficient
        uint32_t len  = 10001;
        uint8_t* data = (uint8_t*) malloc( len );
        CHECK_NE( data, nullptr );
        memset( data, 'x', len );
        struct asrt_span      sp    = { .b = data, .e = data + len };
        struct asrt_allocator alloc = asrt_default_allocator();
        char*                 res   = asrt_realloc_str( &alloc, &sp );
        CHECK_NE( res, nullptr );
        CHECK_EQ( 'x', res[0] );
        CHECK_EQ( '\0', res[len] );
        free( res );
        free( data );
}

TEST_CASE_FIXTURE( controller_ctx, "cntr_version_mismatch" )
{
        enum asrt_status st = asrt_cntr_init( &cntr, &send_queue, asrt_default_allocator() );
        CHECK_EQ( ASRT_SUCCESS, st );
        st = asrt_cntr_start( &cntr, &record_init_cb, &init_status, 1000 );
        CHECK_EQ( ASRT_SUCCESS, st );
        check_tick( &cntr, t++ );
        drain();
        coll.data.pop_back();

        // reply with a mismatched major version
        {
                struct asrt_u8d8msg   msg = {};
                struct asrt_send_req* req =
                    asrt_msg_rtoc_proto_version( &msg, ASRT_PROTO_MAJOR + 1, 0, 0 );
                check_recv( &cntr, ( struct asrt_span ){ .b = req->buff.b, .e = req->buff.e } );
        }

        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &cntr.node, t++ ) );
        CHECK_EQ( ASRT_VERSION_ERR, init_status );
        CHECK( asrt_cntr_idle( &cntr ) );
}

// ---------------------------------------------------------------------------
// timeout tests

static enum asrt_status record_status_cb( void* ptr, enum asrt_status s )
{
        enum asrt_status* p = (enum asrt_status*) ptr;
        *p                  = s;
        return ASRT_SUCCESS;
}

static enum asrt_status record_tc_cb( void* ptr, enum asrt_status s, uint16_t count )
{
        (void) count;
        return record_status_cb( ptr, s );
}

static enum asrt_status record_desc_cb( void* ptr, enum asrt_status s, char const* desc )
{
        (void) desc;
        return record_status_cb( ptr, s );
}

static enum asrt_status record_test_info_cb(
    void*            ptr,
    enum asrt_status s,
    uint16_t         tid,
    char const*      desc )
{
        (void) tid;
        (void) desc;
        return record_status_cb( ptr, s );
}

static enum asrt_status record_result_cb( void* ptr, enum asrt_status s, struct asrt_result* res )
{
        (void) res;
        return record_status_cb( ptr, s );
}

TEST_CASE_FIXTURE( controller_ctx, "cntr_timeout_init" )
{
        enum asrt_status st = asrt_cntr_init( &cntr, &send_queue, asrt_default_allocator() );
        CHECK_EQ( ASRT_SUCCESS, st );
        st = asrt_cntr_start( &cntr, &record_init_cb, &init_status, 3 );
        CHECK_EQ( ASRT_SUCCESS, st );

        // now=0: sends request, enters WAITING, deadline = 0+3 = 3
        check_tick( &cntr, 0 );
        drain();
        coll.data.pop_back();

        // now=1,2: still waiting
        check_tick( &cntr, 1 );
        check_tick( &cntr, 2 );
        CHECK( !asrt_cntr_idle( &cntr ) );

        // now=3: deadline reached → timeout
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &cntr.node, 3 ) );
        CHECK_EQ( ASRT_TIMEOUT_ERR, init_status );
        CHECK( asrt_cntr_idle( &cntr ) );
}

TEST_CASE_FIXTURE( controller_ctx, "cntr_timeout_test_count" )
{
        check_cntr_full_init( this );

        enum asrt_status cb_status = ASRT_SUCCESS;
        enum asrt_status st        = asrt_cntr_test_count( &cntr, &record_tc_cb, &cb_status, 3 );
        CHECK_EQ( ASRT_SUCCESS, st );

        check_tick( &cntr, 0 );
        drain();
        coll.data.pop_back();

        check_tick( &cntr, 1 );
        check_tick( &cntr, 2 );
        CHECK( !asrt_cntr_idle( &cntr ) );

        check_tick( &cntr, 3 );
        CHECK_EQ( ASRT_TIMEOUT_ERR, cb_status );
        CHECK( asrt_cntr_idle( &cntr ) );
}

TEST_CASE_FIXTURE( controller_ctx, "cntr_timeout_desc" )
{
        check_cntr_full_init( this );

        enum asrt_status cb_status = ASRT_SUCCESS;
        enum asrt_status st        = asrt_cntr_desc( &cntr, &record_desc_cb, &cb_status, 3 );
        CHECK_EQ( ASRT_SUCCESS, st );

        check_tick( &cntr, 0 );
        drain();
        coll.data.pop_back();

        check_tick( &cntr, 1 );
        check_tick( &cntr, 2 );
        CHECK( !asrt_cntr_idle( &cntr ) );

        check_tick( &cntr, 3 );
        CHECK_EQ( ASRT_TIMEOUT_ERR, cb_status );
        CHECK( asrt_cntr_idle( &cntr ) );
}

TEST_CASE_FIXTURE( controller_ctx, "cntr_timeout_test_info" )
{
        check_cntr_full_init( this );

        enum asrt_status cb_status = ASRT_SUCCESS;
        enum asrt_status st = asrt_cntr_test_info( &cntr, 0, &record_test_info_cb, &cb_status, 3 );
        CHECK_EQ( ASRT_SUCCESS, st );

        check_tick( &cntr, 0 );
        drain();
        coll.data.pop_back();

        check_tick( &cntr, 1 );
        check_tick( &cntr, 2 );
        CHECK( !asrt_cntr_idle( &cntr ) );

        check_tick( &cntr, 3 );
        CHECK_EQ( ASRT_TIMEOUT_ERR, cb_status );
        CHECK( asrt_cntr_idle( &cntr ) );
}

TEST_CASE_FIXTURE( controller_ctx, "cntr_timeout_exec" )
{
        check_cntr_full_init( this );

        enum asrt_status cb_status = ASRT_SUCCESS;
        enum asrt_status st = asrt_cntr_test_exec( &cntr, 0, &record_result_cb, &cb_status, 3 );
        CHECK_EQ( ASRT_SUCCESS, st );

        // now=0: STAGE_INIT → sends, enters WAITING, deadline = 0+3 = 3
        check_tick( &cntr, 0 );
        drain();
        coll.data.pop_back();

        // now=1,2: still waiting
        check_tick( &cntr, 1 );
        check_tick( &cntr, 2 );
        CHECK( !asrt_cntr_idle( &cntr ) );

        // now=3: deadline reached → timeout
        check_tick( &cntr, 3 );
        CHECK_EQ( ASRT_TIMEOUT_ERR, cb_status );
        CHECK( asrt_cntr_idle( &cntr ) );
}

TEST_CASE_FIXTURE( controller_ctx, "cntr_busy_err" )
{
        check_cntr_full_init( this );

        // Start an operation to put the controller into a non-idle state
        std::string      desc;
        enum asrt_status st = asrt_cntr_desc( &cntr, &cpy_desc_cb, (void*) &desc, 1000 );
        CHECK_EQ( ASRT_SUCCESS, st );
        CHECK( !asrt_cntr_idle( &cntr ) );

        // Any subsequent operation while busy must return BUSY_ERR
        st = asrt_cntr_desc( &cntr, &cpy_desc_cb, (void*) &desc, 1000 );
        CHECK_EQ( ASRT_BUSY_ERR, st );

        uint32_t x;
        st = asrt_cntr_test_count( &cntr, &cpy_u32_cb, (void*) &x, 1000 );
        CHECK_EQ( ASRT_BUSY_ERR, st );

        st = asrt_cntr_test_info( &cntr, 0, &cpy_test_info_cb, (void*) &desc, 1000 );
        CHECK_EQ( ASRT_BUSY_ERR, st );
}

TEST_CASE_FIXTURE( controller_ctx, "cntr_recv_test_error_result" )
{
        check_cntr_full_init( this );

        struct asrt_result res = {};
        enum asrt_status   st  = asrt_cntr_test_exec( &cntr, 42, result_cb, &res, 1000 );
        CHECK_EQ( ASRT_SUCCESS, st );
        check_tick( &cntr, t++ );
        drain();
        coll.data.pop_back();

        // A direct TEST_RESULT(ERROR) reply should complete the exec as an error.
        struct asrt_u8d8msg   msg = {};
        struct asrt_send_req* req = asrt_msg_rtoc_test_result( &msg, 0, ASRT_TEST_RESULT_ERROR );
        check_recv_and_spin( &cntr, req->buff.b, req->buff.e, &t );
        CHECK_EQ( ASRT_TEST_RESULT_ERROR, res.res );
}

// wrong run_id in TEST_RESULT → ASRT_TEST_RESULT_ERROR in callback
TEST_CASE_FIXTURE( controller_ctx, "cntr_test_exec_wrong_run_id" )
{
        check_cntr_full_init( this );

        struct asrt_result res = {};
        enum asrt_status   st  = asrt_cntr_test_exec( &cntr, 42, result_cb, &res, 1000 );
        CHECK_EQ( ASRT_SUCCESS, st );
        check_tick( &cntr, t++ );  // sends TEST_START request
        drain();
        coll.data.pop_back();

        // Send TEST_RESULT with wrong run_id (controller expects 0, send 99)
        struct asrt_u8d8msg   msg = {};
        struct asrt_send_req* req = asrt_msg_rtoc_test_result( &msg, 99, ASRT_TEST_RESULT_SUCCESS );
        check_recv_and_spin( &cntr, req->buff.b, req->buff.e, &t );

        CHECK_EQ( ASRT_TEST_RESULT_ERROR, res.res );
        CHECK_EQ( coll.data.empty(), true );
}

// recv while controller is IDLE → ASRT_RECV_ERR
TEST_CASE_FIXTURE( controller_ctx, "cntr_recv_idle" )
{
        check_cntr_full_init( this );

        struct asrt_u8d8msg   msg = {};
        struct asrt_send_req* req = asrt_msg_rtoc_proto_version( &msg, 0, 1, 0 );
        enum asrt_status      rst = asrt_chann_recv(
            &cntr.node, ( struct asrt_span ){ .b = req->buff.b, .e = req->buff.e } );
        CHECK_EQ( ASRT_RECV_ERR, rst );
        CHECK( asrt_cntr_idle( &cntr ) );
}

// empty buffer — top-level header truncation
TEST_CASE_FIXTURE( controller_ctx, "cntr_recv_truncated_hdr" )
{
        enum asrt_status st = asrt_cntr_init( &cntr, &send_queue, asrt_default_allocator() );
        CHECK_EQ( ASRT_SUCCESS, st );
        st = asrt_cntr_start( &cntr, &record_init_cb, &init_status, 1000 );
        CHECK_EQ( ASRT_SUCCESS, st );

        uint8_t          buf[1];
        enum asrt_status rst =
            asrt_chann_recv( &cntr.node, ( struct asrt_span ){ .b = buf, .e = buf } );
        CHECK_EQ( ASRT_RECV_ERR, rst );

        // Drain: tick to send request then satisfy it
        check_tick( &cntr, t++ );
        drain();
        coll.data.pop_back();
        struct asrt_u8d8msg   vmsg = {};
        struct asrt_send_req* vreq = asrt_msg_rtoc_proto_version( &vmsg, 0, 1, 0 );
        check_recv_and_spin( &cntr, vreq->buff.b, vreq->buff.e, &t );
}

// truncated proto-version reply while in INIT/WAITING
TEST_CASE_FIXTURE( controller_ctx, "cntr_recv_truncated_init" )
{
        enum asrt_status st = asrt_cntr_init( &cntr, &send_queue, asrt_default_allocator() );
        CHECK_EQ( ASRT_SUCCESS, st );
        st = asrt_cntr_start( &cntr, &record_init_cb, &init_status, 1000 );
        CHECK_EQ( ASRT_SUCCESS, st );
        check_tick( &cntr, t++ );
        drain();
        coll.data.pop_back();

        // ID + only major(2) — missing minor(2) + patch(2) = too short
        uint8_t          buf[16];
        struct asrt_span sp = { .b = buf, .e = buf + sizeof buf };
        asrt_add_u16( &sp.b, ASRT_MSG_PROTO_VERSION );
        asrt_add_u16( &sp.b, 0 );  // major only, 4 bytes total
        enum asrt_status rst =
            asrt_chann_recv( &cntr.node, ( struct asrt_span ){ .b = buf, .e = sp.b } );
        CHECK_EQ( ASRT_RECV_ERR, rst );

        // Satisfy properly to clean up
        struct asrt_u8d8msg   vmsg = {};
        struct asrt_send_req* vreq = asrt_msg_rtoc_proto_version( &vmsg, 0, 1, 0 );
        check_recv_and_spin( &cntr, vreq->buff.b, vreq->buff.e, &t );
}

// truncated test-count reply while in HNDL_TC/WAITING
TEST_CASE_FIXTURE( controller_ctx, "cntr_recv_truncated_test_count" )
{
        check_cntr_full_init( this );

        enum asrt_status cb_st = ASRT_SUCCESS;
        enum asrt_status st    = asrt_cntr_test_count( &cntr, &record_tc_cb, &cb_st, 1000 );
        CHECK_EQ( ASRT_SUCCESS, st );
        check_tick( &cntr, t++ );
        drain();
        coll.data.pop_back();

        // Just the message ID, no u16 count
        uint8_t          buf[16];
        struct asrt_span sp = { .b = buf, .e = buf + sizeof buf };
        asrt_add_u16( &sp.b, ASRT_MSG_TEST_COUNT );
        enum asrt_status rst =
            asrt_chann_recv( &cntr.node, ( struct asrt_span ){ .b = buf, .e = sp.b } );
        CHECK_EQ( ASRT_RECV_ERR, rst );

        // Satisfy properly to clean up
        struct asrt_u8d4msg   msg = {};
        struct asrt_send_req* req = asrt_msg_rtoc_test_count( &msg, 0 );
        check_recv_and_spin( &cntr, req->buff.b, req->buff.e, &t );
}

// truncated test-info reply while in HNDL_TI/WAITING
TEST_CASE_FIXTURE( controller_ctx, "cntr_recv_truncated_test_info" )
{
        check_cntr_full_init( this );

        struct test_info_result p  = {};
        enum asrt_status        st = asrt_cntr_test_info( &cntr, 7, &cpy_test_info_cb, &p, 1000 );
        CHECK_EQ( ASRT_SUCCESS, st );
        check_tick( &cntr, t++ );
        drain();
        coll.data.pop_back();

        // Just the message ID, no u16 tid
        uint8_t          buf[16];
        struct asrt_span sp = { .b = buf, .e = buf + sizeof buf };
        asrt_add_u16( &sp.b, ASRT_MSG_TEST_INFO );
        enum asrt_status rst =
            asrt_chann_recv( &cntr.node, ( struct asrt_span ){ .b = buf, .e = sp.b } );
        CHECK_EQ( ASRT_RECV_ERR, rst );

        // Satisfy properly to clean up
        char const*                    desc = "x";
        struct asrt_core_test_info_msg tmsg = {};
        struct asrt_send_req*          treq =
            asrt_msg_rtoc_test_info( &tmsg, 7, ASRT_TEST_INFO_SUCCESS, desc, strlen( desc ) );
        uint8_t flat[128];
        auto    fsp = flatten_req( flat, treq );
        check_recv_and_spin( &cntr, fsp.b, fsp.e, &t );
}

//---------------------------------------------------------------------
// diag channel — controller side
namespace
{

struct diag_ctx
{
        struct asrt_node   head      = {};
        stub_allocator_ctx alloc_ctx = {};
        asrt_allocator     alloc     = {};
        asrt_diag_server   diag      = {};

        diag_ctx()
        {
                head.chid = ASRT_CORE;
                alloc     = asrt_stub_allocator( &alloc_ctx );
                REQUIRE_EQ( ASRT_SUCCESS, asrt_diag_server_init( &diag, &head, alloc ) );
        }
};

}  // namespace

TEST_CASE( "diag_init" )
{
        struct asrt_node head = {};
        head.chid             = ASRT_CORE;

        // diag = NULL
        CHECK_EQ( ASRT_INIT_ERR, asrt_diag_server_init( NULL, &head, asrt_default_allocator() ) );

        // prev = NULL
        struct asrt_diag_server diag = {};
        CHECK_EQ( ASRT_INIT_ERR, asrt_diag_server_init( &diag, NULL, asrt_default_allocator() ) );

        // valid
        CHECK_EQ( ASRT_SUCCESS, asrt_diag_server_init( &diag, &head, asrt_default_allocator() ) );
        CHECK_EQ( ASRT_DIAG, diag.node.chid );
        CHECK_NE( nullptr, (void*) (uintptr_t) diag.node.e_cb_ptr );
        CHECK_EQ( nullptr, diag.first_rec );
        CHECK_EQ( nullptr, diag.last_rec );
        CHECK_EQ( &diag.node, head.next );
        CHECK_EQ( nullptr, diag.node.next );
        asrt_diag_server_deinit( &diag );
}

TEST_CASE_FIXTURE( diag_ctx, "diag_recv" )
{
        uint8_t  buf[64];
        uint8_t* p;

        // empty buffer → SUCCESS, no record queued

        check_recv( &diag, ( struct asrt_span ){ .b = buf, .e = buf } );
        CHECK_EQ( nullptr, diag.first_rec );

        // valid RECORD line=7, file="foo"
        p    = buf;
        *p++ = ASRT_DIAG_MSG_RECORD;
        asrt_add_u32( &p, 7 );
        *p++ = 3;  // file_len
        *p++ = 'f';
        *p++ = 'o';
        *p++ = 'o';
        check_recv( &diag, ( struct asrt_span ){ .b = buf, .e = p } );
        {
                auto* rec = asrt_diag_server_take_record( &diag );
                REQUIRE_NE( nullptr, rec );
                CHECK_EQ( 7U, rec->line );
                REQUIRE_NE( nullptr, rec->file );
                CHECK_EQ( 0, strcmp( rec->file, "foo" ) );
                asrt_diag_free_record( &diag.alloc, rec );
        }

        // empty filename → file is empty string
        p    = buf;
        *p++ = ASRT_DIAG_MSG_RECORD;
        asrt_add_u32( &p, 42 );
        *p++ = 0;  // file_len
        check_recv( &diag, ( struct asrt_span ){ .b = buf, .e = p } );
        {
                auto* rec = asrt_diag_server_take_record( &diag );
                REQUIRE_NE( nullptr, rec );
                CHECK_EQ( 42U, rec->line );
                REQUIRE_NE( nullptr, rec->file );
                CHECK_EQ( 0U, strlen( rec->file ) );
                asrt_diag_free_record( &diag.alloc, rec );
        }

        // two RECORDs → FIFO order
        p    = buf;
        *p++ = ASRT_DIAG_MSG_RECORD;
        asrt_add_u32( &p, 1 );
        *p++ = 1;  // file_len
        *p++ = 'a';
        check_recv( &diag, ( struct asrt_span ){ .b = buf, .e = p } );
        p    = buf;
        *p++ = ASRT_DIAG_MSG_RECORD;
        asrt_add_u32( &p, 2 );
        *p++ = 1;  // file_len
        *p++ = 'b';
        check_recv( &diag, ( struct asrt_span ){ .b = buf, .e = p } );
        {
                auto* r1 = asrt_diag_server_take_record( &diag );
                auto* r2 = asrt_diag_server_take_record( &diag );
                REQUIRE_NE( nullptr, r1 );
                REQUIRE_NE( nullptr, r2 );
                CHECK_EQ( 1U, r1->line );
                CHECK_EQ( 2U, r2->line );
                asrt_diag_free_record( &diag.alloc, r1 );
                asrt_diag_free_record( &diag.alloc, r2 );
        }

        asrt_diag_server_deinit( &diag );
}

TEST_CASE_FIXTURE( diag_ctx, "diag_recv_errors" )
{
        uint8_t buf[8];

        // msgid present but line is truncated (only 3 of 4 bytes)
        uint8_t* p = buf;
        *p++       = ASRT_DIAG_MSG_RECORD;
        *p++       = 0x00;
        *p++       = 0x00;
        *p++       = 0x07;  // 3 bytes for line, need 4
        CHECK_EQ(
            ASRT_RECV_ERR,
            asrt_chann_recv( &diag.node, ( struct asrt_span ){ .b = buf, .e = p } ) );
        CHECK_EQ( nullptr, diag.first_rec );

        // unknown ID 0x00
        buf[0] = 0x00;
        CHECK_EQ(
            ASRT_RECV_ERR,
            asrt_chann_recv( &diag.node, ( struct asrt_span ){ .b = buf, .e = buf + 1 } ) );
        CHECK_EQ( nullptr, diag.first_rec );

        // unknown ID 0xFF
        buf[0] = 0xFF;
        CHECK_EQ(
            ASRT_RECV_ERR,
            asrt_chann_recv( &diag.node, ( struct asrt_span ){ .b = buf, .e = buf + 1 } ) );
        CHECK_EQ( nullptr, diag.first_rec );

        asrt_diag_server_deinit( &diag );
}

TEST_CASE_FIXTURE( diag_ctx, "diag_recv_alloc_failure" )
{
        uint8_t  buf[8];
        uint8_t* p;

        // first alloc (rec struct) fails
        alloc_ctx.fail_at_call = 1;
        p                      = buf;
        *p++                   = ASRT_DIAG_MSG_RECORD;
        asrt_add_u32( &p, 10 );
        *p++ = 1;  // file_len
        *p++ = 'x';
        CHECK_EQ(
            ASRT_ALLOC_ERR,
            asrt_chann_recv( &diag.node, ( struct asrt_span ){ .b = buf, .e = p } ) );
        CHECK_EQ( nullptr, diag.first_rec );
        CHECK_EQ( 0U, alloc_ctx.free_calls );

        // second alloc (file string) fails; rec is freed
        alloc_ctx.alloc_calls  = 0;
        alloc_ctx.free_calls   = 0;
        alloc_ctx.fail_at_call = 2;
        p                      = buf;
        *p++                   = ASRT_DIAG_MSG_RECORD;
        asrt_add_u32( &p, 20 );
        *p++ = 1;  // file_len
        *p++ = 'y';
        CHECK_EQ(
            ASRT_ALLOC_ERR,
            asrt_chann_recv( &diag.node, ( struct asrt_span ){ .b = buf, .e = p } ) );
        CHECK_EQ( nullptr, diag.first_rec );
        CHECK_EQ( 1U, alloc_ctx.free_calls );

        alloc_ctx.fail_at_call = 0;
        asrt_diag_server_deinit( &diag );
}

TEST_CASE_FIXTURE( diag_ctx, "diag_take_record" )
{
        uint8_t  buf[8];
        uint8_t* p;

        // NULL diag
        CHECK_EQ( nullptr, asrt_diag_server_take_record( NULL ) );

        // empty queue
        CHECK_EQ( nullptr, asrt_diag_server_take_record( &diag ) );

        // one record → returned, queue empty after
        p    = buf;
        *p++ = ASRT_DIAG_MSG_RECORD;
        asrt_add_u32( &p, 99 );
        *p++ = 1;  // file_len
        *p++ = 'z';
        CHECK_EQ(
            ASRT_SUCCESS, asrt_chann_recv( &diag.node, ( struct asrt_span ){ .b = buf, .e = p } ) );
        {
                auto* rec = asrt_diag_server_take_record( &diag );
                REQUIRE_NE( nullptr, rec );
                CHECK_EQ( 99U, rec->line );
                CHECK_EQ( nullptr, diag.first_rec );
                CHECK_EQ( nullptr, diag.last_rec );
                asrt_diag_free_record( &diag.alloc, rec );
        }

        // C-TAKE-4,5: three records A(1) B(2) C(3) → FIFO order
        for ( uint32_t i = 1; i <= 3; i++ ) {
                p    = buf;
                *p++ = ASRT_DIAG_MSG_RECORD;
                asrt_add_u32( &p, i );
                *p++ = 1;  // file_len
                *p++ = 'a';
                CHECK_EQ(
                    ASRT_SUCCESS,
                    asrt_chann_recv( &diag.node, ( struct asrt_span ){ .b = buf, .e = p } ) );
        }
        for ( uint32_t i = 1; i <= 3; i++ ) {
                auto* rec = asrt_diag_server_take_record( &diag );
                REQUIRE_NE( nullptr, rec );
                CHECK_EQ( i, rec->line );
                asrt_diag_free_record( &diag.alloc, rec );
        }

        // queue empty; take returns NULL
        CHECK_EQ( nullptr, asrt_diag_server_take_record( &diag ) );

        // take, insert, take → last_rec stays consistent
        p    = buf;
        *p++ = ASRT_DIAG_MSG_RECORD;
        asrt_add_u32( &p, 10 );
        *p++ = 1;  // file_len
        *p++ = 'x';
        check_recv( &diag, ( struct asrt_span ){ .b = buf, .e = p } );
        auto* rec_a = asrt_diag_server_take_record( &diag );
        REQUIRE_NE( nullptr, rec_a );
        p    = buf;
        *p++ = ASRT_DIAG_MSG_RECORD;
        asrt_add_u32( &p, 20 );
        *p++ = 1;  // file_len
        *p++ = 'y';
        check_recv( &diag, ( struct asrt_span ){ .b = buf, .e = p } );
        auto* rec_b = asrt_diag_server_take_record( &diag );
        REQUIRE_NE( nullptr, rec_b );
        CHECK_EQ( 20U, rec_b->line );
        CHECK_EQ( nullptr, diag.first_rec );
        CHECK_EQ( nullptr, diag.last_rec );
        asrt_diag_free_record( &diag.alloc, rec_a );
        asrt_diag_free_record( &diag.alloc, rec_b );

        asrt_diag_server_deinit( &diag );
}

// C-FREE-1,2
TEST_CASE( "diag_free_record" )
{
        struct asrt_node head         = {};
        head.chid                     = ASRT_CORE;
        stub_allocator_ctx      sctx  = {};
        asrt_allocator          alloc = asrt_stub_allocator( &sctx );
        struct asrt_diag_server diag  = {};
        REQUIRE_EQ( ASRT_SUCCESS, asrt_diag_server_init( &diag, &head, alloc ) );

        uint8_t  buf[16];
        uint8_t* p = buf;
        *p++       = ASRT_DIAG_MSG_RECORD;
        asrt_add_u32( &p, 5 );
        *p++ = 2;  // file_len
        *p++ = 'h';
        *p++ = 'i';
        check_recv( &diag, ( struct asrt_span ){ .b = buf, .e = p } );

        // record with non-NULL file → two free calls
        auto* rec = asrt_diag_server_take_record( &diag );
        REQUIRE_NE( nullptr, rec );
        REQUIRE_NE( nullptr, rec->file );
        sctx.free_calls = 0;
        asrt_diag_free_record( &alloc, rec );
        CHECK_EQ( 2U, sctx.free_calls );

        // record with file == NULL → one free call
        auto* rec2 =
            static_cast< asrt_diag_record* >( asrt_alloc( &alloc, sizeof( asrt_diag_record ) ) );
        REQUIRE_NE( nullptr, rec2 );
        *rec2           = { .file = nullptr, .extra = nullptr, .line = 0, .next = nullptr };
        sctx.free_calls = 0;
        asrt_diag_free_record( &alloc, rec2 );
        CHECK_EQ( 1U, sctx.free_calls );

        asrt_diag_server_deinit( &diag );
}

// C-DEINIT-1..5
TEST_CASE_FIXTURE( diag_ctx, "diag_deinit" )
{
        uint8_t  buf[8];
        uint8_t* p;


        // empty queue → success
        {
                struct asrt_node head2     = {};
                head2.chid                 = ASRT_CORE;
                struct asrt_diag_server d2 = {};
                asrt_diag_server_init( &d2, &head2, asrt_default_allocator() );
                asrt_diag_server_deinit( &d2 );
        }

        // one record → freed
        {
                struct asrt_node head3        = {};
                head3.chid                    = ASRT_CORE;
                stub_allocator_ctx      sctx3 = {};
                asrt_allocator          a3    = asrt_stub_allocator( &sctx3 );
                struct asrt_diag_server d3    = {};
                asrt_diag_server_init( &d3, &head3, a3 );
                p    = buf;
                *p++ = ASRT_DIAG_MSG_RECORD;
                asrt_add_u32( &p, 1 );
                *p++ = 1;  // file_len
                *p++ = 'a';
                check_recv( &d3, ( struct asrt_span ){ .b = buf, .e = p } );
                asrt_diag_server_deinit( &d3 );
                CHECK_EQ( nullptr, d3.first_rec );
                CHECK_EQ( 2U, sctx3.free_calls );  // file + rec
        }

        // three records → all freed
        {
                struct asrt_node head4        = {};
                head4.chid                    = ASRT_CORE;
                stub_allocator_ctx      sctx4 = {};
                asrt_allocator          a4    = asrt_stub_allocator( &sctx4 );
                struct asrt_diag_server d4    = {};
                asrt_diag_server_init( &d4, &head4, a4 );
                for ( uint32_t i = 0; i < 3; i++ ) {
                        p    = buf;
                        *p++ = ASRT_DIAG_MSG_RECORD;
                        asrt_add_u32( &p, i );
                        *p++ = 1;  // file_len
                        *p++ = 'x';
                        check_recv( &d4, ( struct asrt_span ){ .b = buf, .e = p } );
                }
                asrt_diag_server_deinit( &d4 );
                CHECK_EQ( nullptr, d4.first_rec );
                CHECK_EQ( 6U, sctx4.free_calls );  // 3 × (file + rec)
        }

        // partial take then deinit frees remaining
        p    = buf;
        *p++ = ASRT_DIAG_MSG_RECORD;
        asrt_add_u32( &p, 1 );
        *p++ = 1;  // file_len
        *p++ = 'a';
        check_recv( &diag, ( struct asrt_span ){ .b = buf, .e = p } );

        p    = buf;
        *p++ = ASRT_DIAG_MSG_RECORD;
        asrt_add_u32( &p, 2 );
        *p++ = 1;  // file_len
        *p++ = 'b';
        check_recv( &diag, ( struct asrt_span ){ .b = buf, .e = p } );
        auto* taken = asrt_diag_server_take_record( &diag );
        REQUIRE_NE( nullptr, taken );
        asrt_diag_free_record( &diag.alloc, taken );
        asrt_diag_server_deinit( &diag );
        CHECK_EQ( nullptr, diag.first_rec );
}

// truncated exec messages while in HNDL_EXEC/WAITING
TEST_CASE_FIXTURE( controller_ctx, "cntr_recv_truncated_exec" )
{
        check_cntr_full_init( this );

        struct asrt_result res = {};
        enum asrt_status   st  = asrt_cntr_test_exec( &cntr, 1, result_cb, &res, 1000 );
        CHECK_EQ( ASRT_SUCCESS, st );
        check_tick( &cntr, t++ );
        drain();
        coll.data.pop_back();

        uint8_t          buf[16];
        struct asrt_span sp;

        // Truncated TEST_RESULT: ID + run_id(4) only — missing res(2)
        sp = ( struct asrt_span ){ .b = buf, .e = buf + sizeof buf };
        asrt_add_u16( &sp.b, ASRT_MSG_TEST_RESULT );
        asrt_add_u32( &sp.b, 0 );  // run_id, but no res u16
        enum asrt_status rst =
            asrt_chann_recv( &cntr.node, ( struct asrt_span ){ .b = buf, .e = sp.b } );
        CHECK_EQ( ASRT_RECV_ERR, rst );

        // Satisfy properly to clean up
        struct asrt_u8d8msg   msg = {};
        struct asrt_send_req* req = asrt_msg_rtoc_test_result( &msg, 0, ASRT_TEST_RESULT_SUCCESS );
        check_recv_and_spin( &cntr, req->buff.b, req->buff.e, &t );
        CHECK_EQ( ASRT_TEST_RESULT_SUCCESS, res.res );
        CHECK_EQ( coll.data.empty(), true );
}

// ---------------------------------------------------------------------------
// diag: protocol-level round-trip tests (expose file/extra parsing bug)

TEST_CASE_FIXTURE( diag_ctx, "diag_recv_proto_no_extra" )
{
        // Serialize with the real protocol encoder (extra=NULL)
        struct asrt_diag_record_msg dmsg = {};
        struct asrt_send_req*       req  = asrt_msg_rtoc_diag_record( &dmsg, "foo.c", 7, NULL );
        uint8_t                     flat[128];
        auto                        fsp = flatten_req( flat, req );

        check_recv( &diag, fsp );

        auto* rec = asrt_diag_server_take_record( &diag );
        REQUIRE_NE( nullptr, rec );
        CHECK_EQ( 7U, rec->line );
        REQUIRE_NE( nullptr, rec->file );
        CHECK_EQ( std::string( "foo.c" ), std::string( rec->file ) );
        asrt_diag_free_record( &diag.alloc, rec );

        asrt_diag_server_deinit( &diag );
}

TEST_CASE_FIXTURE( diag_ctx, "diag_recv_proto_with_extra" )
{
        // Serialize with the real protocol encoder, file + extra
        struct asrt_diag_record_msg dmsg = {};
        struct asrt_send_req*       req = asrt_msg_rtoc_diag_record( &dmsg, "test.c", 42, "x > 0" );
        uint8_t                     flat[128];
        auto                        fsp = flatten_req( flat, req );

        check_recv( &diag, fsp );

        auto* rec = asrt_diag_server_take_record( &diag );
        REQUIRE_NE( nullptr, rec );
        CHECK_EQ( 42U, rec->line );
        REQUIRE_NE( nullptr, rec->file );
        // file must be exactly "test.c", not polluted by the file_len prefix or extra
        CHECK_EQ( std::string( "test.c" ), std::string( rec->file ) );
        asrt_diag_free_record( &diag.alloc, rec );

        asrt_diag_server_deinit( &diag );
}

// ============================================================================
// asrt_param_server — controller PARAM channel (Phase 2)
// ============================================================================

static struct asrt_flat_tree make_param_tree()
{
        struct asrt_allocator alloc = asrt_default_allocator();
        struct asrt_flat_tree tree;
        asrt_flat_tree_init( &tree, alloc, 8, 16 );
        asrt_flat_tree_append_cont( &tree, 0, 1, NULL, ASRT_FLAT_CTYPE_OBJECT );
        asrt_flat_tree_append_scalar(
            &tree, 1, 2, "alpha", ASRT_FLAT_STYPE_U32, { .u32_val = 10 } );
        asrt_flat_tree_append_scalar(
            &tree, 1, 3, "beta", ASRT_FLAT_STYPE_STR, { .str_val = "hi" } );
        asrt_flat_tree_append_scalar(
            &tree, 1, 4, "gamma", ASRT_FLAT_STYPE_BOOL, { .bool_val = 1 } );
        return tree;
}

// Build a raw READY_ACK message into buf; return past-end pointer.
static uint8_t* build_ready_ack( uint8_t* buf, uint32_t max_msg_size )
{
        uint8_t* p = buf;
        *p++       = ASRT_PARAM_MSG_READY_ACK;
        asrt_add_u32( &p, max_msg_size );
        return p;
}

// Build a raw QUERY message into buf; return past-end pointer.
static uint8_t* build_query( uint8_t* buf, asrt::flat_id node_id )
{
        uint8_t* p = buf;
        *p++       = ASRT_PARAM_MSG_QUERY;
        asrt_add_u32( &p, node_id );
        return p;
}

namespace
{

struct param_ctx
{
        struct asrt_node          head      = {};
        stub_allocator_ctx        alloc_ctx = {};
        asrt_allocator            alloc     = {};
        struct asrt_param_server  param     = {};
        collector                 coll;
        struct asrt_send_req_list send_queue = {};
        uint32_t                  t          = 1;

        param_ctx()
        {
                head.chid       = ASRT_CORE;
                alloc           = asrt_stub_allocator( &alloc_ctx );
                head.send_queue = &send_queue;
                REQUIRE_EQ( ASRT_SUCCESS, asrt_param_server_init( &param, &head, alloc ) );
        }
        ~param_ctx() { asrt_param_server_deinit( &param ); }
        void drain() { drain_send_queue( &send_queue, &coll ); }
};

}  // namespace

TEST_CASE( "asrt_param_server_init" )
{
        struct asrt_node head           = {};
        head.chid                       = ASRT_CORE;
        struct asrt_param_server param2 = {};

        CHECK_EQ( ASRT_INIT_ERR, asrt_param_server_init( NULL, &head, asrt_default_allocator() ) );
        CHECK_EQ(
            ASRT_INIT_ERR, asrt_param_server_init( &param2, NULL, asrt_default_allocator() ) );

        CHECK_EQ(
            ASRT_SUCCESS, asrt_param_server_init( &param2, &head, asrt_default_allocator() ) );
        CHECK_EQ( ASRT_PARA, param2.node.chid );
        CHECK_NE( nullptr, (void*) (uintptr_t) param2.node.e_cb_ptr );
        CHECK_EQ( &param2.node, head.next );
        CHECK_EQ( nullptr, param2.tree );
        CHECK_EQ( nullptr, param2.enc_buff );
        CHECK_EQ( 0, param2.ack_received );
        asrt_param_server_deinit( &param2 );
}

TEST_CASE_FIXTURE( param_ctx, "asrt_param_server_send_ready_no_tree" )
{
        CHECK_EQ( ASRT_ARG_ERR, asrt_param_server_send_ready( &param, 1U, 1000, NULL, NULL ) );
        CHECK( coll.data.empty() );
}

TEST_CASE_FIXTURE( param_ctx, "asrt_param_server_send_ready_invalid_id" )
{
        struct asrt_flat_tree tree = make_param_tree();
        asrt_param_server_set_tree( &param, &tree );
        CHECK_EQ( ASRT_ARG_ERR, asrt_param_server_send_ready( &param, 999U, 1000, NULL, NULL ) );
        CHECK( coll.data.empty() );
        asrt_flat_tree_deinit( &tree );
}

TEST_CASE_FIXTURE( param_ctx, "asrt_param_server_send_ready_encodes_correctly" )
{
        struct asrt_flat_tree tree = make_param_tree();
        asrt_param_server_set_tree( &param, &tree );

        CHECK_EQ( ASRT_SUCCESS, asrt_param_server_send_ready( &param, 1U, 1000, NULL, NULL ) );
        drain();
        REQUIRE_EQ( 1U, coll.data.size() );

        auto& msg = coll.data.front();
        CHECK_EQ( ASRT_PARA, msg.id );
        REQUIRE_EQ( 5U, msg.data.size() );
        CHECK_EQ( ASRT_PARAM_MSG_READY, msg.data[0] );
        // root_id = 1 as big-endian u32
        CHECK_EQ( 0U, msg.data[1] );
        CHECK_EQ( 0U, msg.data[2] );
        CHECK_EQ( 0U, msg.data[3] );
        CHECK_EQ( 1U, msg.data[4] );

        CHECK_EQ( 0, param.ack_received );  // reset on send_ready
        asrt_flat_tree_deinit( &tree );
}

TEST_CASE_FIXTURE( param_ctx, "asrt_param_server_recv_ready_ack_allocates_buffer" )
{
        uint8_t buf[8];
        check_recv( &param, ( struct asrt_span ){ .b = buf, .e = build_ready_ack( buf, 256U ) } );
        CHECK_EQ( ASRT_SUCCESS, asrt_chann_tick( &param.node, t++ ) );
        CHECK_EQ( 1, param.ack_received );
        CHECK_EQ( 256U, param.max_msg_size );
        CHECK_NE( nullptr, param.enc_buff );
        CHECK_EQ( 1U, alloc_ctx.alloc_calls );
}

// BUG: handle_ready_ack does not guard against overwriting an already-pending
// event.  A second recv (without a tick) should return ASRT_RECV_ERR but
// currently returns ASRT_SUCCESS, so this test is expected to FAIL.
TEST_CASE_FIXTURE( param_ctx, "asrt_param_server_recv_ready_ack_while_pending_returns_error" )
{
        uint8_t buf[8];
        check_recv( &param, ( struct asrt_span ){ .b = buf, .e = build_ready_ack( buf, 256U ) } );
        // pending == PENDING_READY_ACK; tick not called yet
        CHECK_EQ(
            ASRT_RECV_ERR,
            asrt_chann_recv(
                &param.node,
                ( struct asrt_span ){ .b = buf, .e = build_ready_ack( buf, 512U ) } ) );
        // Consume the first pending so the fixture destructor stays clean
        check_tick( &param, t++ );
}

// BUG: handle_query does not guard against overwriting an already-pending
// event.  A second recv (without a tick) should return ASRT_RECV_ERR but
// currently returns ASRT_SUCCESS, so this test is expected to FAIL.
TEST_CASE_FIXTURE( param_ctx, "asrt_param_server_recv_query_while_pending_returns_error" )
{
        struct asrt_flat_tree tree = make_param_tree();
        asrt_param_server_set_tree( &param, &tree );

        uint8_t buf[8];
        // Handshake so ack_received=1 and enc_buff is ready
        check_recv( &param, ( struct asrt_span ){ .b = buf, .e = build_ready_ack( buf, 256U ) } );
        check_tick( &param, t++ );

        // First query stores pending
        check_recv( &param, ( struct asrt_span ){ .b = buf, .e = build_query( buf, 2U ) } );
        // pending == PENDING_QUERY; tick not called yet
        CHECK_EQ(
            ASRT_RECV_ERR,
            asrt_chann_recv(
                &param.node, ( struct asrt_span ){ .b = buf, .e = build_query( buf, 3U ) } ) );
        check_tick( &param, t++ );  // second tick to consume the second pending if it were
                                    // erroneously accepted
        asrt_flat_tree_deinit( &tree );
}

TEST_CASE_FIXTURE( param_ctx, "asrt_param_server_query_before_ack_returns_error" )
{
        struct asrt_flat_tree tree = make_param_tree();
        asrt_param_server_set_tree( &param, &tree );

        uint8_t buf[8];
        CHECK_EQ(
            ASRT_RECV_ERR,
            asrt_chann_recv(
                &param.node, ( struct asrt_span ){ .b = buf, .e = build_query( buf, 2U ) } ) );
        CHECK( coll.data.empty() );
        asrt_flat_tree_deinit( &tree );
}

TEST_CASE_FIXTURE( param_ctx, "asrt_param_server_query_produces_response" )
{
        struct asrt_flat_tree tree = make_param_tree();
        asrt_param_server_set_tree( &param, &tree );

        uint8_t buf[8];
        // Handshake
        check_recv( &param, ( struct asrt_span ){ .b = buf, .e = build_ready_ack( buf, 256U ) } );
        check_tick( &param, t++ );

        // Query node 2 ("alpha", u32=10)
        check_recv( &param, ( struct asrt_span ){ .b = buf, .e = build_query( buf, 2U ) } );
        check_tick( &param, t++ );
        drain();
        REQUIRE_EQ( 1U, coll.data.size() );

        auto& msg = coll.data.front();
        CHECK_EQ( ASRT_PARA, msg.id );
        CHECK_EQ( ASRT_PARAM_MSG_RESPONSE, msg.data[0] );

        // Verify wire format: first node id should be 2 (big-endian u32 at offset 1)
        REQUIRE( msg.data.size() > 5U );
        uint32_t first_id = ( (uint32_t) msg.data[1] << 24 ) | ( (uint32_t) msg.data[2] << 16 ) |
                            ( (uint32_t) msg.data[3] << 8 ) | (uint32_t) msg.data[4];
        CHECK_EQ( 2U, first_id );

        asrt_flat_tree_deinit( &tree );
}

TEST_CASE_FIXTURE( param_ctx, "asrt_param_server_query_multi_batch" )
{
        // Use a small max_msg_size so only one node fits per batch.
        // Node 2: id(4) + "alpha\0"(6) + type(1) + u32(4) = 15 node bytes
        // Full message: msg_id(1) + 15 + next_sib(4) = 20 bytes.
        struct asrt_flat_tree tree = make_param_tree();
        asrt_param_server_set_tree( &param, &tree );

        uint8_t buf[8];
        check_recv( &param, ( struct asrt_span ){ .b = buf, .e = build_ready_ack( buf, 20U ) } );
        check_tick( &param, t++ );

        // First query: only node 2 fits → next_sibling_id points to node 3
        check_recv( &param, ( struct asrt_span ){ .b = buf, .e = build_query( buf, 2U ) } );
        check_tick( &param, t++ );
        drain();
        REQUIRE_EQ( 1U, coll.data.size() );

        auto& msg = coll.data.front();
        CHECK_EQ( ASRT_PARAM_MSG_RESPONSE, msg.data[0] );
        REQUIRE_EQ( 20U, msg.data.size() );  // msg_id(1) + node(15) + next_sib(4)
        // First node id = 2
        uint32_t nid = ( (uint32_t) msg.data[1] << 24 ) | ( (uint32_t) msg.data[2] << 16 ) |
                       ( (uint32_t) msg.data[3] << 8 ) | (uint32_t) msg.data[4];
        CHECK_EQ( 2U, nid );
        // Trailing next_sibling_id = 3 (last 4 bytes)
        uint32_t nsib = ( (uint32_t) msg.data[16] << 24 ) | ( (uint32_t) msg.data[17] << 16 ) |
                        ( (uint32_t) msg.data[18] << 8 ) | (uint32_t) msg.data[19];
        CHECK_EQ( 3U, nsib );  // next batch starts at node 3

        asrt_flat_tree_deinit( &tree );
}

TEST_CASE_FIXTURE( param_ctx, "asrt_param_server_query_oversize_returns_error" )
{
        // max_msg_size=11 — minimum struct size but key "alpha" doesn't fit
        struct asrt_flat_tree tree = make_param_tree();
        asrt_param_server_set_tree( &param, &tree );

        uint8_t buf[8];
        check_recv( &param, ( struct asrt_span ){ .b = buf, .e = build_ready_ack( buf, 11U ) } );
        check_tick( &param, t++ );
        check_recv( &param, ( struct asrt_span ){ .b = buf, .e = build_query( buf, 2U ) } );
        check_tick( &param, t++ );
        drain();
        REQUIRE_EQ( 1U, coll.data.size() );
        auto& msg = coll.data.front();
        CHECK_EQ( ASRT_PARA, msg.id );
        REQUIRE_EQ( 6U, msg.data.size() );
        CHECK_EQ( ASRT_PARAM_MSG_ERROR, msg.data[0] );
        CHECK_EQ( ASRT_PARAM_ERR_RESPONSE_TOO_LARGE, msg.data[1] );
        // node_id = 2 as big-endian u32
        CHECK_EQ( 0U, msg.data[2] );
        CHECK_EQ( 0U, msg.data[3] );
        CHECK_EQ( 0U, msg.data[4] );
        CHECK_EQ( 2U, msg.data[5] );

        asrt_flat_tree_deinit( &tree );
}

TEST_CASE_FIXTURE( param_ctx, "asrt_param_server_deinit_frees_buffer" )
{
        uint8_t buf[8];
        check_recv( &param, ( struct asrt_span ){ .b = buf, .e = build_ready_ack( buf, 64U ) } );
        check_tick( &param, t++ );
        CHECK_NE( nullptr, param.enc_buff );
        CHECK_EQ( 1U, alloc_ctx.alloc_calls );
        CHECK_EQ( 0U, alloc_ctx.free_calls );

        asrt_param_server_deinit( &param );
        CHECK_EQ( nullptr, param.enc_buff );
        CHECK_EQ( 1U, alloc_ctx.free_calls );

        // Prevent double-free in fixture destructor
        alloc_ctx.free_calls = 0;
}

namespace
{

template < typename Server, typename Client >
struct server_client_base
{
        Server   server = {};
        Client   client = {};
        uint32_t t      = 1;

        static void deliver( struct asrt_send_req_list* sq, struct asrt_node* to )
        {
                while ( sq->head ) {
                        struct asrt_send_req* req = sq->head;
                        sq->head                  = req->next;
                        if ( !sq->head )
                                sq->tail = NULL;
                        req->next = NULL;
                        uint8_t  flat[512];
                        uint8_t* p = flat;
                        memcpy( p, req->buff.b, req->buff.e - req->buff.b );
                        p += req->buff.e - req->buff.b;
                        for ( asrt_rec_span const* s = req->buff.next; s != nullptr; s = s->next ) {
                                size_t sz = (size_t) ( s->e - s->b );
                                memcpy( p, s->b, sz );
                                p += sz;
                        }
                        auto st = asrt_chann_recv( to, ( struct asrt_span ){ .b = flat, .e = p } );
                        if ( req->done_cb )
                                req->done_cb( req->done_ptr, st );
                }
        }

        // Tick both sides up to N times, routing queued messages between them
        void spin( int max_iter = 100 )
        {
                for ( int i = 0; i < max_iter; i++ ) {
                        asrt_chann_tick( &server.node, t++ );
                        deliver( server.node.send_queue, &client.node );
                        asrt_chann_tick( &client.node, t++ );
                        deliver( client.node.send_queue, &server.node );
                }
        }
};
using param_base = server_client_base< asrt_param_server, asrt_param_client >;

struct param_loopback_ctx : param_base
{
        // Both sides share one head each (CORE placeholder + PARAM node)
        struct asrt_node          srv_head               = {};
        struct asrt_node          cli_head               = {};
        stub_allocator_ctx        alloc_ctx              = {};
        asrt_allocator            alloc                  = {};
        static constexpr uint32_t cli_buff_size          = 256;
        uint8_t                   cli_buf[cli_buff_size] = {};

        struct asrt_send_req_list srv_sq = {};
        struct asrt_send_req_list cli_sq = {};

        // Response callback state
        struct received_node
        {
                asrt::flat_id   id;
                std::string     key;
                asrt_flat_value value;
                asrt::flat_id   next_sibling;
        };
        std::vector< received_node > received;
        int                          error_called = 0;
        struct asrt_param_query      query        = {};

        static void query_cb(
            struct asrt_param_client*,
            struct asrt_param_query* q,
            struct asrt_flat_value   val )
        {
                auto* ctx = (param_loopback_ctx*) q->cb_ptr;
                if ( q->error_code != 0 ) {
                        ctx->error_called++;
                } else {
                        ctx->received.push_back(
                            { q->node_id, q->key ? q->key : "", val, q->next_sibling } );
                }
        }


        param_loopback_ctx()
        {
                srv_head.chid       = ASRT_CORE;
                cli_head.chid       = ASRT_CORE;
                srv_head.send_queue = &srv_sq;
                cli_head.send_queue = &cli_sq;
                alloc               = asrt_stub_allocator( &alloc_ctx );
                REQUIRE_EQ( ASRT_SUCCESS, asrt_param_server_init( &server, &srv_head, alloc ) );
                struct asrt_span mb = { .b = cli_buf, .e = cli_buf + cli_buff_size };
                REQUIRE_EQ( ASRT_SUCCESS, asrt_param_client_init( &client, &cli_head, mb, 10 ) );
        }

        ~param_loopback_ctx()
        {
                asrt_param_client_deinit( &client );
                asrt_param_server_deinit( &server );
        }
};

}  // namespace

TEST_CASE_FIXTURE( param_loopback_ctx, "param_loopback_full_tree_traversal" )
{
        // Build 3-level tree:
        //   root (OBJECT, id=1)
        //     ├── "sub" (OBJECT, id=2)
        //     │     ├── "x" (U32=100, id=4)
        //     │     └── "y" (STR="hello", id=5)
        //     └── "b" (BOOL=1, id=3)
        struct asrt_flat_tree tree;
        asrt_flat_tree_init( &tree, asrt_default_allocator(), 4, 16 );
        asrt_flat_tree_append_cont( &tree, 0, 1, NULL, ASRT_FLAT_CTYPE_OBJECT );
        asrt_flat_tree_append_cont( &tree, 1, 2, "sub", ASRT_FLAT_CTYPE_OBJECT );
        asrt_flat_tree_append_scalar( &tree, 1, 3, "b", ASRT_FLAT_STYPE_BOOL, { .bool_val = 1 } );
        asrt_flat_tree_append_scalar( &tree, 2, 4, "x", ASRT_FLAT_STYPE_U32, { .u32_val = 100 } );
        asrt_flat_tree_append_scalar(
            &tree, 2, 5, "y", ASRT_FLAT_STYPE_STR, { .str_val = "hello" } );

        asrt_param_server_set_tree( &server, &tree );

        // Server sends READY
        CHECK_EQ( ASRT_SUCCESS, asrt_param_server_send_ready( &server, 1U, 1000, NULL, NULL ) );
        // Spin: READY → client gets root_id, sends READY_ACK → server allocates
        spin();
        CHECK_EQ( 1, client.ready );
        CHECK_EQ( 1U, asrt_param_client_root_id( &client ) );

        // Recursive traversal: collect all nodes
        // Query root
        CHECK_EQ(
            ASRT_SUCCESS, asrt_param_client_fetch_any( &query, &client, 1U, query_cb, this ) );
        spin();
        REQUIRE_EQ( 1U, received.size() );
        CHECK_EQ( 1U, received[0].id );
        CHECK_EQ( (uint8_t) ASRT_FLAT_CTYPE_OBJECT, (uint8_t) received[0].value.type );
        asrt::flat_id first_child = received[0].value.data.cont.first_child;
        CHECK_NE( 0U, first_child );  // should be 2

        // Query first child of root ("sub", id=2)
        CHECK_EQ(
            ASRT_SUCCESS,
            asrt_param_client_fetch_any( &query, &client, first_child, query_cb, this ) );
        spin();
        REQUIRE_EQ( 2U, received.size() );
        CHECK_EQ( 2U, received[1].id );
        CHECK_EQ( "sub", received[1].key );
        CHECK_EQ( (uint8_t) ASRT_FLAT_CTYPE_OBJECT, (uint8_t) received[1].value.type );
        asrt::flat_id sub_first_child = received[1].value.data.cont.first_child;
        asrt::flat_id sub_next_sib    = received[1].next_sibling;
        CHECK_NE( 0U, sub_first_child );  // should be 4
        CHECK_NE( 0U, sub_next_sib );     // should be 3

        // Query next sibling of "sub" → "b" (id=3)
        CHECK_EQ(
            ASRT_SUCCESS,
            asrt_param_client_fetch_any( &query, &client, sub_next_sib, query_cb, this ) );
        spin();
        REQUIRE_EQ( 3U, received.size() );
        CHECK_EQ( 3U, received[2].id );
        CHECK_EQ( "b", received[2].key );
        CHECK_EQ( (uint8_t) ASRT_FLAT_STYPE_BOOL, (uint8_t) received[2].value.type );
        CHECK_EQ( 1U, received[2].value.data.s.u32_val );
        CHECK_EQ( 0U, received[2].next_sibling );  // last sibling

        // Query children of "sub": "x" (id=4)
        CHECK_EQ(
            ASRT_SUCCESS,
            asrt_param_client_fetch_any( &query, &client, sub_first_child, query_cb, this ) );
        spin();
        REQUIRE_EQ( 4U, received.size() );
        CHECK_EQ( 4U, received[3].id );
        CHECK_EQ( "x", received[3].key );
        CHECK_EQ( (uint8_t) ASRT_FLAT_STYPE_U32, (uint8_t) received[3].value.type );
        CHECK_EQ( 100U, received[3].value.data.s.u32_val );
        asrt::flat_id x_next_sib = received[3].next_sibling;
        CHECK_NE( 0U, x_next_sib );  // should be 5

        // Query "y" (id=5)
        CHECK_EQ(
            ASRT_SUCCESS,
            asrt_param_client_fetch_any( &query, &client, x_next_sib, query_cb, this ) );
        spin();
        REQUIRE_EQ( 5U, received.size() );
        CHECK_EQ( 5U, received[4].id );
        CHECK_EQ( "y", received[4].key );
        CHECK_EQ( (uint8_t) ASRT_FLAT_STYPE_STR, (uint8_t) received[4].value.type );
        CHECK_EQ( 0U, received[4].next_sibling );  // last sibling

        // No errors throughout
        CHECK_EQ( 0, error_called );

        asrt_flat_tree_deinit( &tree );
}

TEST_CASE_FIXTURE( param_base, "param_loopback_multi_batch" )
{
        // Small buffer (25 bytes) forces server to split siblings across batches.
        // Node wire size for "cN" U32 child: id(4)+"cN\0"(3)+type(1)+val(4) = 12
        // max_msg_size=25: msg_id(1) + 20 node space + 4 trailer → fits 1 node (12<20).
        // Actually 2×12=24>20 so only 1 fits per batch.  4 children → 4 batches.
        // Use 30 to fit 2: 30-1-4=25, 2×12=24<25 → 2 per batch, 4 children → 2 batches.
        static constexpr uint32_t small_buff_size = 30;

        struct asrt_node   srv_head                 = {};
        struct asrt_node   cli_head                 = {};
        stub_allocator_ctx alloc_ctx                = {};
        asrt_allocator     alloc                    = {};
        uint8_t            cli_buf[small_buff_size] = {};

        srv_head.chid = ASRT_CORE;
        cli_head.chid = ASRT_CORE;
        alloc         = asrt_stub_allocator( &alloc_ctx );

        struct asrt_send_req_list srv_sq2 = {};
        struct asrt_send_req_list cli_sq2 = {};
        srv_head.send_queue               = &srv_sq2;
        cli_head.send_queue               = &cli_sq2;

        REQUIRE_EQ( ASRT_SUCCESS, asrt_param_server_init( &server, &srv_head, alloc ) );
        struct asrt_span mb = { .b = cli_buf, .e = cli_buf + small_buff_size };
        REQUIRE_EQ( ASRT_SUCCESS, asrt_param_client_init( &client, &cli_head, mb, 100 ) );

        // Tree: root(OBJECT,1) → 4 children (U32, ids 2..5)
        struct asrt_flat_tree tree;
        asrt_flat_tree_init( &tree, asrt_default_allocator(), 4, 16 );
        asrt_flat_tree_append_cont( &tree, 0, 1, NULL, ASRT_FLAT_CTYPE_OBJECT );
        asrt_flat_tree_append_scalar( &tree, 1, 2, "c1", ASRT_FLAT_STYPE_U32, { .u32_val = 10 } );
        asrt_flat_tree_append_scalar( &tree, 1, 3, "c2", ASRT_FLAT_STYPE_U32, { .u32_val = 20 } );
        asrt_flat_tree_append_scalar( &tree, 1, 4, "c3", ASRT_FLAT_STYPE_U32, { .u32_val = 30 } );
        asrt_flat_tree_append_scalar( &tree, 1, 5, "c4", ASRT_FLAT_STYPE_U32, { .u32_val = 40 } );
        asrt_param_server_set_tree( &server, &tree );

        // Handshake
        CHECK_EQ( ASRT_SUCCESS, asrt_param_server_send_ready( &server, 1U, 1000, NULL, NULL ) );
        spin();
        CHECK_EQ( 1, client.ready );
        struct rn
        {
                asrt::flat_id id;
                uint32_t      val;
                asrt::flat_id next_sib;
        };
        std::vector< rn >       results;
        struct asrt_param_query q = {};

        auto resp_cb = []( struct asrt_param_client*,
                           struct asrt_param_query* qq,
                           struct asrt_flat_value   val ) {
                auto* r = (std::vector< rn >*) qq->cb_ptr;
                r->push_back( { qq->node_id, val.data.s.u32_val, qq->next_sibling } );
        };

        // Query root to get first_child
        asrt::flat_id first_child = 0;
        auto          root_resp   = []( struct asrt_param_client*,
                             struct asrt_param_query* qq,
                             struct asrt_flat_value   val ) {
                *(asrt::flat_id*) qq->cb_ptr = val.data.cont.first_child;
        };
        CHECK_EQ(
            ASRT_SUCCESS, asrt_param_client_fetch_any( &q, &client, 1U, root_resp, &first_child ) );
        spin();
        REQUIRE_NE( 0U, first_child );

        // Walk all children via next_sibling_id chain
        asrt::flat_id query_id = first_child;
        while ( query_id != 0U ) {
                CHECK_EQ(
                    ASRT_SUCCESS,
                    asrt_param_client_fetch_any( &q, &client, query_id, resp_cb, &results ) );
                spin();
                REQUIRE_FALSE( results.empty() );
                query_id = results.back().next_sib;
        }

        // Verify all 4 children received in order
        REQUIRE_EQ( 4U, results.size() );
        CHECK_EQ( 2U, results[0].id );
        CHECK_EQ( 10U, results[0].val );
        CHECK_EQ( 3U, results[1].id );
        CHECK_EQ( 20U, results[1].val );
        CHECK_EQ( 4U, results[2].id );
        CHECK_EQ( 30U, results[2].val );
        CHECK_EQ( 5U, results[3].id );
        CHECK_EQ( 40U, results[3].val );
        CHECK_EQ( 0U, results[3].next_sib );

        asrt_param_client_deinit( &client );
        asrt_param_server_deinit( &server );
        asrt_flat_tree_deinit( &tree );
}

// ============================================================================
// Phase 5 — find-by-key loopback tests
// ============================================================================

TEST_CASE_FIXTURE( param_loopback_ctx, "param_find_by_key_u32" )
{
        struct asrt_flat_tree tree;
        asrt_flat_tree_init( &tree, asrt_default_allocator(), 4, 16 );
        asrt_flat_tree_append_cont( &tree, 0, 1, NULL, ASRT_FLAT_CTYPE_OBJECT );
        asrt_flat_tree_append_scalar(
            &tree, 1, 2, "alpha", ASRT_FLAT_STYPE_U32, { .u32_val = 42 } );
        asrt_flat_tree_append_scalar(
            &tree, 1, 3, "beta", ASRT_FLAT_STYPE_STR, { .str_val = "hi" } );
        asrt_flat_tree_append_scalar(
            &tree, 1, 4, "gamma", ASRT_FLAT_STYPE_BOOL, { .bool_val = 1 } );
        asrt_param_server_set_tree( &server, &tree );

        CHECK_EQ( ASRT_SUCCESS, asrt_param_server_send_ready( &server, 1U, 1000, NULL, NULL ) );
        spin();
        REQUIRE( client.ready );

        // Find "alpha" by key in root object
        CHECK_EQ(
            ASRT_SUCCESS,
            asrt_param_client_find_any( &query, &client, 1U, "alpha", query_cb, this ) );
        spin();
        REQUIRE_EQ( 1U, received.size() );
        CHECK_EQ( 2U, received[0].id );
        CHECK_EQ( "alpha", std::string( received[0].key ) );
        CHECK_EQ( (uint8_t) ASRT_FLAT_STYPE_U32, (uint8_t) received[0].value.type );
        CHECK_EQ( 42U, received[0].value.data.s.u32_val );
        CHECK_EQ( 0, error_called );

        asrt_flat_tree_deinit( &tree );
}

TEST_CASE_FIXTURE( param_loopback_ctx, "param_find_by_key_not_found" )
{
        struct asrt_flat_tree tree;
        asrt_flat_tree_init( &tree, asrt_default_allocator(), 4, 16 );
        asrt_flat_tree_append_cont( &tree, 0, 1, NULL, ASRT_FLAT_CTYPE_OBJECT );
        asrt_flat_tree_append_scalar(
            &tree, 1, 2, "alpha", ASRT_FLAT_STYPE_U32, { .u32_val = 42 } );
        asrt_param_server_set_tree( &server, &tree );

        CHECK_EQ( ASRT_SUCCESS, asrt_param_server_send_ready( &server, 1U, 1000, NULL, NULL ) );
        spin();
        REQUIRE( client.ready );

        // Find a key that doesn't exist → error callback
        CHECK_EQ(
            ASRT_SUCCESS,
            asrt_param_client_find_any( &query, &client, 1U, "missing", query_cb, this ) );
        spin();
        CHECK_EQ( 0U, received.size() );
        CHECK_EQ( 1, error_called );

        asrt_flat_tree_deinit( &tree );
}

TEST_CASE_FIXTURE( param_loopback_ctx, "param_find_by_key_nested" )
{
        // root (OBJECT, id=1)
        //   ├── "sub" (OBJECT, id=2)
        //   │     ├── "x" (U32=100, id=4)
        //   │     └── "y" (STR="hello", id=5)
        //   └── "b" (BOOL=1, id=3)
        struct asrt_flat_tree tree;
        asrt_flat_tree_init( &tree, asrt_default_allocator(), 4, 16 );
        asrt_flat_tree_append_cont( &tree, 0, 1, NULL, ASRT_FLAT_CTYPE_OBJECT );
        asrt_flat_tree_append_cont( &tree, 1, 2, "sub", ASRT_FLAT_CTYPE_OBJECT );
        asrt_flat_tree_append_scalar( &tree, 1, 3, "b", ASRT_FLAT_STYPE_BOOL, { .bool_val = 1 } );
        asrt_flat_tree_append_scalar( &tree, 2, 4, "x", ASRT_FLAT_STYPE_U32, { .u32_val = 100 } );
        asrt_flat_tree_append_scalar(
            &tree, 2, 5, "y", ASRT_FLAT_STYPE_STR, { .str_val = "hello" } );
        asrt_param_server_set_tree( &server, &tree );

        CHECK_EQ( ASRT_SUCCESS, asrt_param_server_send_ready( &server, 1U, 1000, NULL, NULL ) );
        spin();
        REQUIRE( client.ready );

        // Find "sub" in root, then find "y" inside "sub"
        CHECK_EQ(
            ASRT_SUCCESS,
            asrt_param_client_find_any( &query, &client, 1U, "sub", query_cb, this ) );
        spin();
        REQUIRE_EQ( 1U, received.size() );
        CHECK_EQ( 2U, received[0].id );
        CHECK_EQ( (uint8_t) ASRT_FLAT_CTYPE_OBJECT, (uint8_t) received[0].value.type );

        asrt::flat_id sub_id = received[0].id;

        // Now find "y" inside the sub object
        CHECK_EQ(
            ASRT_SUCCESS,
            asrt_param_client_find_any( &query, &client, sub_id, "y", query_cb, this ) );
        spin();
        REQUIRE_EQ( 2U, received.size() );
        CHECK_EQ( 5U, received[1].id );
        CHECK_EQ( "y", std::string( received[1].key ) );
        CHECK_EQ( (uint8_t) ASRT_FLAT_STYPE_STR, (uint8_t) received[1].value.type );

        CHECK_EQ( 0, error_called );
        asrt_flat_tree_deinit( &tree );
}

TEST_CASE_FIXTURE( param_loopback_ctx, "param_find_then_query" )
{
        struct asrt_flat_tree tree;
        asrt_flat_tree_init( &tree, asrt_default_allocator(), 4, 16 );
        asrt_flat_tree_append_cont( &tree, 0, 1, NULL, ASRT_FLAT_CTYPE_OBJECT );
        asrt_flat_tree_append_scalar(
            &tree, 1, 2, "alpha", ASRT_FLAT_STYPE_U32, { .u32_val = 42 } );
        asrt_flat_tree_append_scalar(
            &tree, 1, 3, "beta", ASRT_FLAT_STYPE_STR, { .str_val = "hi" } );
        asrt_param_server_set_tree( &server, &tree );

        CHECK_EQ( ASRT_SUCCESS, asrt_param_server_send_ready( &server, 1U, 1000, NULL, NULL ) );
        spin();
        REQUIRE( client.ready );

        // Find by key
        CHECK_EQ(
            ASRT_SUCCESS,
            asrt_param_client_find_any( &query, &client, 1U, "alpha", query_cb, this ) );
        spin();
        REQUIRE_EQ( 1U, received.size() );
        CHECK_EQ( 2U, received[0].id );

        // Then a normal query by id works correctly
        CHECK_EQ(
            ASRT_SUCCESS, asrt_param_client_fetch_any( &query, &client, 3U, query_cb, this ) );
        spin();
        REQUIRE_EQ( 2U, received.size() );
        CHECK_EQ( 3U, received[1].id );
        CHECK_EQ( "beta", std::string( received[1].key ) );

        CHECK_EQ( 0, error_called );
        asrt_flat_tree_deinit( &tree );
}

// ============================================================================
// asrt_collect_server — controller COLLECT channel
// ============================================================================


// Build a raw READY_ACK message; return past-end pointer.
static uint8_t* build_collect_ready_ack( uint8_t* buf )
{
        *buf = ASRT_COLLECT_MSG_READY_ACK;
        return buf + 1;
}

// Build a raw APPEND message; return past-end pointer.
static uint8_t* build_collect_append(
    uint8_t*                      buf,
    asrt::flat_id                 parent_id,
    asrt::flat_id                 node_id,
    char const*                   key,
    struct asrt_flat_value const* value )
{
        struct asrt_collect_append_msg msg = {};
        struct asrt_send_req*          req =
            asrt_msg_rtoc_collect_append( &msg, parent_id, node_id, key, value );
        uint8_t* p = buf;
        memcpy( p, req->buff.b, req->buff.e - req->buff.b );
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

struct collect_ctx
{
        struct asrt_node           head      = {};
        stub_allocator_ctx         alloc_ctx = {};
        asrt_allocator             alloc     = {};
        struct asrt_collect_server server    = {};
        collector                  coll;
        struct asrt_send_req_list  send_queue = {};
        uint32_t                   t          = 1;

        collect_ctx()
        {
                head.chid       = ASRT_CORE;
                alloc           = asrt_stub_allocator( &alloc_ctx );
                head.send_queue = &send_queue;
                REQUIRE_EQ(
                    ASRT_SUCCESS, asrt_collect_server_init( &server, &head, alloc, 4, 16 ) );
        }
        ~collect_ctx() { asrt_collect_server_deinit( &server ); }
        void drain() { drain_send_queue( &send_queue, &coll ); }
};

}  // namespace

TEST_CASE( "asrt_collect_server_init" )
{
        struct asrt_node head        = {};
        head.chid                    = ASRT_CORE;
        struct asrt_collect_server s = {};

        CHECK_EQ(
            ASRT_INIT_ERR,
            asrt_collect_server_init( NULL, &head, asrt_default_allocator(), 4, 16 ) );
        CHECK_EQ(
            ASRT_INIT_ERR, asrt_collect_server_init( &s, NULL, asrt_default_allocator(), 4, 16 ) );

        CHECK_EQ(
            ASRT_SUCCESS, asrt_collect_server_init( &s, &head, asrt_default_allocator(), 4, 16 ) );
        CHECK_EQ( ASRT_COLL, s.node.chid );
        CHECK_NE( nullptr, (void*) (uintptr_t) s.node.e_cb_ptr );
        CHECK_EQ( &s.node, head.next );
        CHECK_EQ( ASRT_COLLECT_SERVER_IDLE, s.state );
        asrt_collect_server_deinit( &s );
}

TEST_CASE_FIXTURE( collect_ctx, "asrt_collect_server_send_ready_encodes_correctly" )
{
        CHECK_EQ( ASRT_SUCCESS, asrt_collect_server_send_ready( &server, 1U, 1000, NULL, NULL ) );
        drain();
        REQUIRE_EQ( 1U, coll.data.size() );

        auto& msg = coll.data.front();
        CHECK_EQ( ASRT_COLL, msg.id );
        REQUIRE_EQ( 9U, msg.data.size() );
        CHECK_EQ( ASRT_COLLECT_MSG_READY, msg.data[0] );
        assert_u32( 1U, msg.data.data() + 1 );
        assert_u32( 1U, msg.data.data() + 5 );
        CHECK_EQ( ASRT_COLLECT_SERVER_READY_SENT, server.state );
        coll.data.pop_front();
}

TEST_CASE_FIXTURE( collect_ctx, "asrt_collect_server_recv_ready_ack_activates" )
{
        CHECK_EQ( ASRT_SUCCESS, asrt_collect_server_send_ready( &server, 1U, 1000, NULL, NULL ) );
        drain();
        coll.data.pop_front();

        uint8_t buf[8];
        check_recv(
            &server, ( struct asrt_span ){ .b = buf, .e = build_collect_ready_ack( buf ) } );
        check_tick( &server, t++ );
        CHECK_EQ( ASRT_COLLECT_SERVER_ACTIVE, server.state );
}

TEST_CASE_FIXTURE( collect_ctx, "asrt_collect_server_recv_ready_ack_while_pending_returns_error" )
{
        CHECK_EQ( ASRT_SUCCESS, asrt_collect_server_send_ready( &server, 1U, 1000, NULL, NULL ) );
        drain();
        coll.data.pop_front();

        uint8_t buf[8];
        check_recv(
            &server, ( struct asrt_span ){ .b = buf, .e = build_collect_ready_ack( buf ) } );
        CHECK_EQ(
            ASRT_RECV_ERR,
            asrt_chann_recv(
                &server.node,
                ( struct asrt_span ){ .b = buf, .e = build_collect_ready_ack( buf ) } ) );
        check_tick( &server, t++ );
}

TEST_CASE_FIXTURE( collect_ctx, "asrt_collect_server_append_builds_tree" )
{
        CHECK_EQ( ASRT_SUCCESS, asrt_collect_server_send_ready( &server, 1U, 1000, NULL, NULL ) );
        drain();
        coll.data.pop_front();

        // Handshake
        uint8_t buf[8];
        check_recv(
            &server, ( struct asrt_span ){ .b = buf, .e = build_collect_ready_ack( buf ) } );
        check_tick( &server, t++ );

        // Append root object (parent=0, node=1, no key)
        uint8_t abuf[128];
        struct asrt_flat_value obj
        {
                .type = ASRT_FLAT_CTYPE_OBJECT, .data = {}
        };
        uint8_t* ae = build_collect_append( abuf, 0, 1, NULL, &obj );
        check_recv( &server, ( struct asrt_span ){ .b = abuf, .e = ae } );
        check_tick( &server, t++ );

        // Append child u32 (parent=1, node=2, key="alpha", value=42)
        struct asrt_flat_value val
        {
                .type = ASRT_FLAT_STYPE_U32, .data = {}
        };
        val.data.s.u32_val = 42;
        ae                 = build_collect_append( abuf, 1, 2, "alpha", &val );
        check_recv( &server, ( struct asrt_span ){ .b = abuf, .e = ae } );
        check_tick( &server, t++ );

        // Verify tree
        struct asrt_flat_tree const*  tree = asrt_collect_server_tree( &server );
        struct asrt_flat_query_result qr;
        CHECK_EQ( ASRT_SUCCESS, asrt_flat_tree_query( (struct asrt_flat_tree*) tree, 1, &qr ) );
        CHECK_EQ( ASRT_FLAT_CTYPE_OBJECT, qr.value.type );

        CHECK_EQ( ASRT_SUCCESS, asrt_flat_tree_query( (struct asrt_flat_tree*) tree, 2, &qr ) );
        CHECK_EQ( ASRT_FLAT_STYPE_U32, qr.value.type );
        CHECK_EQ( 42U, qr.value.data.s.u32_val );
        CHECK_EQ( std::string( "alpha" ), std::string( qr.key ) );
}

TEST_CASE_FIXTURE( collect_ctx, "asrt_collect_server_append_string_value" )
{
        CHECK_EQ( ASRT_SUCCESS, asrt_collect_server_send_ready( &server, 1U, 1000, NULL, NULL ) );
        drain();
        coll.data.pop_front();

        uint8_t buf[8];
        check_recv(
            &server, ( struct asrt_span ){ .b = buf, .e = build_collect_ready_ack( buf ) } );
        check_tick( &server, t++ );

        // Append root object
        uint8_t abuf[128];
        struct asrt_flat_value obj
        {
                .type = ASRT_FLAT_CTYPE_OBJECT, .data = {}
        };
        uint8_t* ae = build_collect_append( abuf, 0, 1, NULL, &obj );
        check_recv( &server, ( struct asrt_span ){ .b = abuf, .e = ae } );
        check_tick( &server, t++ );

        // Append string child
        struct asrt_flat_value str_val
        {
                .type = ASRT_FLAT_STYPE_STR, .data = {}
        };
        str_val.data.s.str_val = "hello";
        ae                     = build_collect_append( abuf, 1, 2, "msg", &str_val );
        check_recv( &server, ( struct asrt_span ){ .b = abuf, .e = ae } );
        check_tick( &server, t++ );

        struct asrt_flat_tree const*  tree = asrt_collect_server_tree( &server );
        struct asrt_flat_query_result qr;
        CHECK_EQ( ASRT_SUCCESS, asrt_flat_tree_query( (struct asrt_flat_tree*) tree, 2, &qr ) );
        CHECK_EQ( ASRT_FLAT_STYPE_STR, qr.value.type );
        CHECK_EQ( std::string( "hello" ), std::string( qr.value.data.s.str_val ) );
}

TEST_CASE_FIXTURE( collect_ctx, "asrt_collect_server_append_before_active_returns_error" )
{
        uint8_t abuf[128];
        struct asrt_flat_value obj
        {
                .type = ASRT_FLAT_CTYPE_OBJECT, .data = {}
        };
        uint8_t* ae = build_collect_append( abuf, 0, 1, NULL, &obj );
        CHECK_EQ(
            ASRT_RECV_ERR,
            asrt_chann_recv( &server.node, ( struct asrt_span ){ .b = abuf, .e = ae } ) );
}

TEST_CASE_FIXTURE( collect_ctx, "asrt_collect_server_back_to_back_appends" )
{
        CHECK_EQ( ASRT_SUCCESS, asrt_collect_server_send_ready( &server, 1U, 1000, NULL, NULL ) );
        drain();
        coll.data.pop_front();

        uint8_t buf[8];
        check_recv(
            &server, ( struct asrt_span ){ .b = buf, .e = build_collect_ready_ack( buf ) } );
        check_tick( &server, t++ );

        // Append root object
        uint8_t abuf[128];
        struct asrt_flat_value obj
        {
                .type = ASRT_FLAT_CTYPE_OBJECT, .data = {}
        };
        uint8_t* ae = build_collect_append( abuf, 0, 1, NULL, &obj );
        check_recv( &server, ( struct asrt_span ){ .b = abuf, .e = ae } );

        // Second append WITHOUT tick() in between — simulates burst arrival
        struct asrt_flat_value val
        {
                .type = ASRT_FLAT_STYPE_U32, .data = {}
        };
        val.data.s.u32_val = 42;
        ae                 = build_collect_append( abuf, 1, 2, "alpha", &val );
        check_recv( &server, ( struct asrt_span ){ .b = abuf, .e = ae } );

        // Tick to finalize
        check_tick( &server, t++ );
        CHECK_EQ( ASRT_COLLECT_SERVER_ACTIVE, server.state );

        // Verify both nodes made it into the tree
        struct asrt_flat_tree const*  tree = asrt_collect_server_tree( &server );
        struct asrt_flat_query_result qr;
        CHECK_EQ( ASRT_SUCCESS, asrt_flat_tree_query( (struct asrt_flat_tree*) tree, 1, &qr ) );
        CHECK_EQ( ASRT_FLAT_CTYPE_OBJECT, qr.value.type );

        CHECK_EQ( ASRT_SUCCESS, asrt_flat_tree_query( (struct asrt_flat_tree*) tree, 2, &qr ) );
        CHECK_EQ( ASRT_FLAT_STYPE_U32, qr.value.type );
        CHECK_EQ( 42U, qr.value.data.s.u32_val );
}

TEST_CASE_FIXTURE( collect_ctx, "asrt_collect_server_append_duplicate_sends_error" )
{
        CHECK_EQ( ASRT_SUCCESS, asrt_collect_server_send_ready( &server, 1U, 1000, NULL, NULL ) );
        drain();
        coll.data.pop_front();

        uint8_t buf[8];
        check_recv(
            &server, ( struct asrt_span ){ .b = buf, .e = build_collect_ready_ack( buf ) } );
        check_tick( &server, t++ );

        // Append root object
        uint8_t abuf[128];
        struct asrt_flat_value obj
        {
                .type = ASRT_FLAT_CTYPE_OBJECT, .data = {}
        };
        uint8_t* ae = build_collect_append( abuf, 0, 1, NULL, &obj );
        check_recv( &server, ( struct asrt_span ){ .b = abuf, .e = ae } );
        check_tick( &server, t++ );

        // Duplicate node_id=1
        ae = build_collect_append( abuf, 0, 1, NULL, &obj );
        check_recv( &server, ( struct asrt_span ){ .b = abuf, .e = ae } );
        check_tick( &server, t++ );

        // Error should have been sent and server deactivated
        CHECK_EQ( ASRT_COLLECT_SERVER_IDLE, server.state );
        drain();
        REQUIRE_GE( coll.data.size(), 1U );
        auto& err_msg = coll.data.back();
        CHECK_EQ( ASRT_COLL, err_msg.id );
        CHECK_EQ( ASRT_COLLECT_MSG_ERROR, err_msg.data[0] );
        coll.data.pop_back();
}

TEST_CASE_FIXTURE( collect_ctx, "asrt_collect_server_ack_cb_fires" )
{
        enum asrt_status ack_status = ASRT_INTERNAL_ERR;
        auto             ack_cb     = []( void* ptr, enum asrt_status s ) {
                *static_cast< enum asrt_status* >( ptr ) = s;
        };

        CHECK_EQ(
            ASRT_SUCCESS,
            asrt_collect_server_send_ready( &server, 1U, 1000, ack_cb, &ack_status ) );
        drain();
        coll.data.pop_front();

        uint8_t buf[8];
        check_recv(
            &server, ( struct asrt_span ){ .b = buf, .e = build_collect_ready_ack( buf ) } );
        check_tick( &server, t++ );
        CHECK_EQ( ASRT_SUCCESS, ack_status );
}

TEST_CASE_FIXTURE( collect_ctx, "asrt_collect_server_timeout_fires_ack_cb" )
{
        enum asrt_status ack_status = ASRT_INTERNAL_ERR;
        auto             ack_cb     = []( void* ptr, enum asrt_status s ) {
                *static_cast< enum asrt_status* >( ptr ) = s;
        };

        CHECK_EQ(
            ASRT_SUCCESS, asrt_collect_server_send_ready( &server, 1U, 10, ack_cb, &ack_status ) );
        drain();
        coll.data.pop_front();

        // Tick past deadline without receiving READY_ACK
        for ( uint32_t i = 0; i < 20; i++ )
                check_tick( &server, t++ );

        CHECK_EQ( ASRT_TIMEOUT_ERR, ack_status );
}

TEST_CASE_FIXTURE( collect_ctx, "asrt_collect_server_send_ready_double_call_returns_error" )
{
        CHECK_EQ( ASRT_SUCCESS, asrt_collect_server_send_ready( &server, 1U, 1000, NULL, NULL ) );
        drain();
        coll.data.pop_front();

        CHECK_EQ( ASRT_ARG_ERR, asrt_collect_server_send_ready( &server, 1U, 1000, NULL, NULL ) );
        CHECK( coll.data.empty() );

        // Consume handshake so fixture can clean up
        uint8_t buf[8];
        check_recv(
            &server, ( struct asrt_span ){ .b = buf, .e = build_collect_ready_ack( buf ) } );
        check_tick( &server, t++ );
}

TEST_CASE_FIXTURE( collect_ctx, "asrt_collect_server_send_ready_after_timeout_allowed" )
{
        enum asrt_status ack_status = ASRT_INTERNAL_ERR;
        auto             ack_cb     = []( void* ptr, enum asrt_status s ) {
                *static_cast< enum asrt_status* >( ptr ) = s;
        };

        CHECK_EQ(
            ASRT_SUCCESS, asrt_collect_server_send_ready( &server, 1U, 5, ack_cb, &ack_status ) );
        drain();
        coll.data.pop_front();

        for ( uint32_t i = 0; i < 10; i++ )
                check_tick( &server, t++ );
        CHECK_EQ( ASRT_TIMEOUT_ERR, ack_status );

        // Should be allowed to call send_ready again after timeout
        CHECK_EQ( ASRT_SUCCESS, asrt_collect_server_send_ready( &server, 1U, 1000, NULL, NULL ) );
        drain();
        coll.data.pop_front();

        uint8_t buf[8];
        check_recv(
            &server, ( struct asrt_span ){ .b = buf, .e = build_collect_ready_ack( buf ) } );
        check_tick( &server, t++ );
        CHECK_EQ( ASRT_COLLECT_SERVER_ACTIVE, server.state );
}

TEST_CASE_FIXTURE( collect_ctx, "asrt_collect_server_ready_ack_reinits_tree" )
{
        CHECK_EQ( ASRT_SUCCESS, asrt_collect_server_send_ready( &server, 1U, 1000, NULL, NULL ) );
        drain();
        coll.data.pop_front();

        uint8_t buf[8];
        check_recv(
            &server, ( struct asrt_span ){ .b = buf, .e = build_collect_ready_ack( buf ) } );
        check_tick( &server, t++ );

        // Append a node
        uint8_t abuf[128];
        struct asrt_flat_value obj
        {
                .type = ASRT_FLAT_CTYPE_OBJECT, .data = {}
        };
        uint8_t* ae = build_collect_append( abuf, 0, 1, NULL, &obj );
        check_recv( &server, ( struct asrt_span ){ .b = abuf, .e = ae } );
        check_tick( &server, t++ );

        // Verify node exists
        struct asrt_flat_tree const*  tree = asrt_collect_server_tree( &server );
        struct asrt_flat_query_result qr;
        CHECK_EQ( ASRT_SUCCESS, asrt_flat_tree_query( (struct asrt_flat_tree*) tree, 1, &qr ) );

        // Second handshake should reinit tree (clearing old data)
        CHECK_EQ( ASRT_SUCCESS, asrt_collect_server_send_ready( &server, 1U, 1000, NULL, NULL ) );
        drain();
        coll.data.pop_front();
        check_recv(
            &server, ( struct asrt_span ){ .b = buf, .e = build_collect_ready_ack( buf ) } );
        check_tick( &server, t++ );

        // Old node should no longer exist in the fresh tree
        tree = asrt_collect_server_tree( &server );
        CHECK_NE( ASRT_SUCCESS, asrt_flat_tree_query( (struct asrt_flat_tree*) tree, 1, &qr ) );
}

TEST_CASE_FIXTURE( collect_ctx, "asrt_collect_server_append_alloc_failure_sends_error_and_resets" )
{
        CHECK_EQ( ASRT_SUCCESS, asrt_collect_server_send_ready( &server, 1U, 1000, NULL, NULL ) );
        drain();
        coll.data.pop_front();

        uint8_t buf[8];
        check_recv(
            &server, ( struct asrt_span ){ .b = buf, .e = build_collect_ready_ack( buf ) } );
        check_tick( &server, t++ );
        CHECK_EQ( ASRT_COLLECT_SERVER_ACTIVE, server.state );

        // Make the next allocation fail (will hit inside flat_tree_append)
        uint32_t calls_before  = alloc_ctx.alloc_calls;
        alloc_ctx.fail_at_call = calls_before + 1;

        uint8_t abuf[128];
        struct asrt_flat_value obj
        {
                .type = ASRT_FLAT_CTYPE_OBJECT, .data = {}
        };
        uint8_t* ae = build_collect_append( abuf, 0, 1, NULL, &obj );
        check_recv( &server, ( struct asrt_span ){ .b = abuf, .e = ae } );

        // Tree alloc failure → error sent to reactor, server resets to idle
        CHECK_EQ( ASRT_COLLECT_SERVER_IDLE, server.state );
        drain();
        REQUIRE_EQ( 1U, coll.data.size() );
        CHECK_EQ( ASRT_COLL, coll.data.front().id );
        CHECK_EQ( ASRT_COLLECT_MSG_ERROR, coll.data.front().data[0] );
        CHECK_EQ( ASRT_COLLECT_ERR_APPEND_FAILED, coll.data.front().data[1] );
        coll.data.clear();
}

namespace
{

using collect_base = server_client_base< asrt_collect_server, asrt_collect_client >;

struct collect_loopback_ctx : collect_base
{
        struct asrt_node   srv_head  = {};
        struct asrt_node   cli_head  = {};
        stub_allocator_ctx alloc_ctx = {};
        asrt_allocator     alloc     = {};

        struct asrt_send_req_list srv_sq = {};
        struct asrt_send_req_list cli_sq = {};

        collect_loopback_ctx()
        {
                srv_head.chid       = ASRT_CORE;
                cli_head.chid       = ASRT_CORE;
                srv_head.send_queue = &srv_sq;
                cli_head.send_queue = &cli_sq;
                alloc               = asrt_stub_allocator( &alloc_ctx );
                REQUIRE_EQ(
                    ASRT_SUCCESS, asrt_collect_server_init( &server, &srv_head, alloc, 4, 16 ) );
                REQUIRE_EQ( ASRT_SUCCESS, asrt_collect_client_init( &client, &cli_head ) );
        }

        ~collect_loopback_ctx() { asrt_collect_server_deinit( &server ); }

        void handshake( asrt::flat_id root_id = 1U )
        {
                REQUIRE_EQ(
                    ASRT_SUCCESS,
                    asrt_collect_server_send_ready( &server, root_id, 1000, NULL, NULL ) );
                spin();
                REQUIRE_EQ( ASRT_COLLECT_SERVER_ACTIVE, server.state );
                REQUIRE_EQ( ASRT_COLLECT_CLIENT_ACTIVE, client.state );
        }
};

}  // namespace

TEST_CASE_FIXTURE( collect_loopback_ctx, "collect_loopback_handshake" )
{
        handshake();
        CHECK_EQ( 1U, asrt_collect_client_root_id( &client ) );
}

TEST_CASE_FIXTURE( collect_loopback_ctx, "collect_loopback_multi_level_tree" )
{
        // Build tree via reactor:
        //   root (OBJECT, id=1)
        //     ├── "nums" (ARRAY, id=2)
        //     │     ├── (U32=10, id=3)
        //     │     └── (U32=20, id=4)
        //     ├── "name" (STR="hello", id=5)
        //     └── "flag" (BOOL=1, id=6)
        handshake( 1U );

        // parent_id=0 = virtual root for top-level nodes.
        // spin(1) per append: deliver client→server + fire done_cb (client→ACTIVE)
        asrt::flat_id root, nums;
        CHECK_EQ(
            ASRT_SUCCESS,
            asrt_collect_client_insert_object( &client, 0, NULL, &root, NULL, NULL ) );
        spin( 1 );

        CHECK_EQ(
            ASRT_SUCCESS,
            asrt_collect_client_insert_array( &client, root, "nums", &nums, NULL, NULL ) );
        spin( 1 );

        CHECK_EQ(
            ASRT_SUCCESS, asrt_collect_client_insert_u32( &client, nums, NULL, 10, NULL, NULL ) );
        spin( 1 );

        CHECK_EQ(
            ASRT_SUCCESS, asrt_collect_client_insert_u32( &client, nums, NULL, 20, NULL, NULL ) );
        spin( 1 );

        CHECK_EQ(
            ASRT_SUCCESS,
            asrt_collect_client_insert_str( &client, root, "name", "hello", NULL, NULL ) );
        spin( 1 );

        CHECK_EQ(
            ASRT_SUCCESS, asrt_collect_client_insert_bool( &client, root, "flag", 1, NULL, NULL ) );
        spin( 1 );

        // Verify the tree built on the controller side
        struct asrt_flat_tree const*  tree = asrt_collect_server_tree( &server );
        struct asrt_flat_query_result qr;

        // Root object (id=1)
        CHECK_EQ( ASRT_SUCCESS, asrt_flat_tree_query( (struct asrt_flat_tree*) tree, 1, &qr ) );
        CHECK_EQ( ASRT_FLAT_CTYPE_OBJECT, qr.value.type );

        // "nums" array (id=2)
        CHECK_EQ( ASRT_SUCCESS, asrt_flat_tree_query( (struct asrt_flat_tree*) tree, 2, &qr ) );
        CHECK_EQ( ASRT_FLAT_CTYPE_ARRAY, qr.value.type );
        CHECK_EQ( std::string( "nums" ), std::string( qr.key ) );

        // Array children: U32=10 (id=3), U32=20 (id=4)
        CHECK_EQ( ASRT_SUCCESS, asrt_flat_tree_query( (struct asrt_flat_tree*) tree, 3, &qr ) );
        CHECK_EQ( ASRT_FLAT_STYPE_U32, qr.value.type );
        CHECK_EQ( 10U, qr.value.data.s.u32_val );

        CHECK_EQ( ASRT_SUCCESS, asrt_flat_tree_query( (struct asrt_flat_tree*) tree, 4, &qr ) );
        CHECK_EQ( ASRT_FLAT_STYPE_U32, qr.value.type );
        CHECK_EQ( 20U, qr.value.data.s.u32_val );

        // "name" = "hello" (id=5)
        CHECK_EQ( ASRT_SUCCESS, asrt_flat_tree_query( (struct asrt_flat_tree*) tree, 5, &qr ) );
        CHECK_EQ( ASRT_FLAT_STYPE_STR, qr.value.type );
        CHECK_EQ( std::string( "hello" ), std::string( qr.value.data.s.str_val ) );
        CHECK_EQ( std::string( "name" ), std::string( qr.key ) );

        // "flag" = true (id=6)
        CHECK_EQ( ASRT_SUCCESS, asrt_flat_tree_query( (struct asrt_flat_tree*) tree, 6, &qr ) );
        CHECK_EQ( ASRT_FLAT_STYPE_BOOL, qr.value.type );
        CHECK_EQ( 1U, qr.value.data.s.bool_val );
        CHECK_EQ( std::string( "flag" ), std::string( qr.key ) );
}

TEST_CASE_FIXTURE( collect_loopback_ctx, "collect_loopback_duplicate_node_sends_error" )
{
        handshake();

        // Append first node (parent=0 = virtual root)
        CHECK_EQ(
            ASRT_SUCCESS, asrt_collect_client_insert_object( &client, 0, NULL, NULL, NULL, NULL ) );
        spin( 1 );  // deliver append to server so node 1 is in the tree

        // The client auto-assigns node_ids starting at 1.  The second append
        // gets id=2 — which is unique, so we cannot trigger a duplicate via the
        // public API alone.  Instead, send a raw APPEND with the same node_id=1
        // through the loopback to test the server ERROR path.
        struct asrt_flat_value dup
        {
                .type = ASRT_FLAT_STYPE_U32, .data = {}
        };
        dup.data.s.u32_val = 42;
        uint8_t          buf[256];
        uint8_t*         end = build_collect_append( buf, 0, 1, "dup", &dup );
        struct asrt_span msg = { .b = buf, .e = end };
        check_recv( &server, msg );  // duplicate → server IDLE + ERROR enqueued
        spin( 1 );                   // deliver ERROR from server to client

        // Server should have sent ERROR and gone idle
        CHECK_EQ( ASRT_COLLECT_SERVER_IDLE, server.state );

        // Client should have received the ERROR and be in error state
        // (the cli_to_srv / srv_to_cli wiring delivers it synchronously)
        CHECK_EQ( ASRT_COLLECT_CLIENT_ERROR, client.state );
}

TEST_CASE_FIXTURE( collect_loopback_ctx, "collect_loopback_append_after_error_rejected" )
{
        handshake();

        // Append one valid node (parent=0 = virtual root)
        CHECK_EQ(
            ASRT_SUCCESS, asrt_collect_client_insert_object( &client, 0, NULL, NULL, NULL, NULL ) );
        spin( 1 );  // deliver append to server so node 1 is in the tree

        // Force duplicate via raw message
        struct asrt_flat_value dup
        {
                .type = ASRT_FLAT_STYPE_U32, .data = {}
        };
        dup.data.s.u32_val = 1;
        uint8_t          buf[256];
        uint8_t*         end = build_collect_append( buf, 0, 1, "x", &dup );
        struct asrt_span msg = { .b = buf, .e = end };
        check_recv( &server, msg );  // duplicate → server IDLE + ERROR enqueued
        spin( 1 );                   // deliver ERROR from server to client

        CHECK_EQ( ASRT_COLLECT_CLIENT_ERROR, client.state );

        // Further appends from the client should fail
        CHECK_EQ( ASRT_ARG_ERR, asrt_collect_client_insert_u32( &client, 0, "y", 99, NULL, NULL ) );
}

// =====================================================================
// stream server tests
// =====================================================================

#include "../asrtc/stream.h"
#include "../asrtr/stream.h"

namespace
{

/// Fixture for isolated controller-side stream server tests.
struct strm_server_ctx
{
        collector                 coll;
        struct asrt_send_req_list send_queue = {};
        stub_allocator_ctx        alloc_ctx;
        asrt_allocator            alloc  = {};
        asrt_node                 root   = {};
        asrt_stream_server        server = {};

        strm_server_ctx()
        {
                alloc           = asrt_stub_allocator( &alloc_ctx );
                root.send_queue = &send_queue;
                CHECK_EQ( ASRT_SUCCESS, asrt_stream_server_init( &server, &root, alloc ) );
        }
        ~strm_server_ctx()
        {
                asrt_stream_server_deinit( &server );
                coll.data.clear();
        }
        void drain() { drain_send_queue( &send_queue, &coll ); }
};

}  // namespace

/// Helper: build a raw DEFINE message and deliver it to the server's recv_cb.
static enum asrt_status strm_deliver_define(
    asrt_stream_server*                server,
    uint8_t                            schema_id,
    enum asrt_strm_field_type_e const* fields,
    uint8_t                            field_count )
{
        // [MSG_DEFINE, schema_id, field_count, fields...]
        std::vector< uint8_t > buf;
        buf.push_back( ASRT_STRM_MSG_DEFINE );
        buf.push_back( schema_id );
        buf.push_back( field_count );
        for ( uint8_t i = 0; i < field_count; i++ )
                buf.push_back( static_cast< uint8_t >( fields[i] ) );
        struct asrt_span sp = { .b = buf.data(), .e = buf.data() + buf.size() };
        return asrt_chann_recv( &server->node, sp );
}

/// Helper: build a raw DATA message and deliver it.
static enum asrt_status strm_deliver_data(
    asrt_stream_server* server,
    uint8_t             schema_id,
    uint8_t const*      data,
    uint16_t            size )
{
        std::vector< uint8_t > buf;
        buf.push_back( ASRT_STRM_MSG_DATA );
        buf.push_back( schema_id );
        buf.insert( buf.end(), data, data + size );
        struct asrt_span sp = { .b = buf.data(), .e = buf.data() + buf.size() };
        return asrt_chann_recv( &server->node, sp );
}

/// Helper: build a DEFINE message from a raw field-definition byte sequence.
/// @p field_count is the number of top-level logical fields (what goes in the
/// protocol header).  @p field_bytes is the full encoded field definition
/// sequence (which may include ARRAY tags followed by their u16 count and
/// element type).
static enum asrt_status strm_deliver_define_raw(
    asrt_stream_server* server,
    uint8_t             schema_id,
    uint8_t             field_count,
    uint8_t const*      field_bytes,
    uint16_t            field_bytes_size )
{
        std::vector< uint8_t > buf;
        buf.push_back( ASRT_STRM_MSG_DEFINE );
        buf.push_back( schema_id );
        buf.push_back( field_count );
        buf.insert( buf.end(), field_bytes, field_bytes + field_bytes_size );
        struct asrt_span sp = { .b = buf.data(), .e = buf.data() + buf.size() };
        return asrt_chann_recv( &server->node, sp );
}

// --- init ---

TEST_CASE( "strm_server_init: null server" )
{
        asrt_node      root  = {};
        asrt_allocator alloc = {};
        CHECK_EQ( ASRT_INIT_ERR, asrt_stream_server_init( nullptr, &root, alloc ) );
}

TEST_CASE( "strm_server_init: null prev" )
{
        asrt_stream_server server = {};
        asrt_allocator     alloc  = {};
        CHECK_EQ( ASRT_INIT_ERR, asrt_stream_server_init( &server, nullptr, alloc ) );
}

TEST_CASE_FIXTURE( strm_server_ctx, "strm_server_init: valid" )
{
        CHECK_EQ( ASRT_STRM, server.node.chid );
        CHECK_EQ( &server.node, root.next );
}

// --- recv: empty buffer ---

TEST_CASE_FIXTURE( strm_server_ctx, "strm_server_recv: empty buffer" )
{
        uint8_t          buf[1];
        struct asrt_span sp = { .b = buf, .e = buf };
        check_recv( &server, sp );
}

// --- recv: unknown message ---

TEST_CASE_FIXTURE( strm_server_ctx, "strm_server_recv: unknown message id" )
{
        uint8_t          buf[] = { 0xFF };
        struct asrt_span sp    = { .b = buf, .e = buf + 1 };
        CHECK_EQ( ASRT_RECV_ERR, asrt_chann_recv( &server.node, sp ) );
}

// --- DEFINE handling ---

TEST_CASE_FIXTURE( strm_server_ctx, "strm_server_define: valid single field" )
{
        enum asrt_strm_field_type_e fields[] = { ASRT_STRM_FIELD_U32 };
        CHECK_EQ( ASRT_SUCCESS, strm_deliver_define( &server, 0, fields, 1 ) );
        CHECK_NE( nullptr, server.lookup[0] );
        CHECK_EQ( 0, server.lookup[0]->schema_id );
        CHECK_EQ( 1, server.lookup[0]->field_count );
        CHECK_EQ( 4, server.lookup[0]->record_size );
        CHECK_EQ( 0U, server.lookup[0]->count );
}

TEST_CASE_FIXTURE( strm_server_ctx, "strm_server_define: multi-field record_size" )
{
        enum asrt_strm_field_type_e fields[] = {
            ASRT_STRM_FIELD_U8, ASRT_STRM_FIELD_U16, ASRT_STRM_FIELD_FLOAT };
        CHECK_EQ( ASRT_SUCCESS, strm_deliver_define( &server, 10, fields, 3 ) );
        // 1+2+4 = 7
        CHECK_EQ( 7, server.lookup[10]->record_size );
}

TEST_CASE_FIXTURE( strm_server_ctx, "strm_server_define: structural fields have zero size" )
{
        enum asrt_strm_field_type_e fields[] = {
            ASRT_STRM_FIELD_LBRACKET, ASRT_STRM_FIELD_U8, ASRT_STRM_FIELD_RBRACKET };
        CHECK_EQ( ASRT_SUCCESS, strm_deliver_define( &server, 1, fields, 3 ) );
        // 0+1+0 = 1
        CHECK_EQ( 1, server.lookup[1]->record_size );
}

TEST_CASE_FIXTURE( strm_server_ctx, "strm_server_define: array field uint32_t[5] record_size" )
{
        // Field definition: [ARRAY_TAG][0x00][0x05][U32_TAG]
        // = 1 logical field, 5 uint32_t elements → record_size = 5×4 = 20
        uint8_t field_bytes[] = {
            ASRT_STRM_FIELD_ARRAY,
            0x00,
            0x05,  // count = 5 (big-endian u16)
            ASRT_STRM_FIELD_U32,
        };
        CHECK_EQ(
            ASRT_SUCCESS,
            strm_deliver_define_raw( &server, 0, 1, field_bytes, sizeof field_bytes ) );
        REQUIRE_NE( nullptr, server.lookup[0] );
        CHECK_EQ( 1, server.lookup[0]->field_count );
        CHECK_EQ( 20, server.lookup[0]->record_size );
}

TEST_CASE_FIXTURE( strm_server_ctx, "strm_server_define: nested array uint32_t[3][4] record_size" )
{
        // Field definition: [ARRAY, 0x00, 0x03, [ARRAY, 0x00, 0x04, U32]]
        // = 1 logical field, 3 outer × 4 inner × 4 bytes = 48
        uint8_t field_bytes[] = {
            ASRT_STRM_FIELD_ARRAY,
            0x00,
            0x03,  // outer: 3
            ASRT_STRM_FIELD_ARRAY,
            0x00,
            0x04,  // inner: 4
            ASRT_STRM_FIELD_U32,
        };
        CHECK_EQ(
            ASRT_SUCCESS,
            strm_deliver_define_raw( &server, 0, 1, field_bytes, sizeof field_bytes ) );
        REQUIRE_NE( nullptr, server.lookup[0] );
        CHECK_EQ( 1, server.lookup[0]->field_count );
        CHECK_EQ( 48, server.lookup[0]->record_size );
}

TEST_CASE_FIXTURE( strm_server_ctx, "strm_server_define: array mixed with scalar fields" )
{
        // Schema: [U8, ARRAY(3 × U32), BOOL] → record_size = 1 + 12 + 1 = 14
        uint8_t field_bytes[] = {
            ASRT_STRM_FIELD_U8,
            ASRT_STRM_FIELD_ARRAY,
            0x00,
            0x03,
            ASRT_STRM_FIELD_U32,
            ASRT_STRM_FIELD_BOOL,
        };
        CHECK_EQ(
            ASRT_SUCCESS,
            strm_deliver_define_raw( &server, 0, 3, field_bytes, sizeof field_bytes ) );
        REQUIRE_NE( nullptr, server.lookup[0] );
        CHECK_EQ( 3, server.lookup[0]->field_count );
        CHECK_EQ( 14, server.lookup[0]->record_size );
}

TEST_CASE_FIXTURE( strm_server_ctx, "strm_server_define: array with zero count rejected" )
{
        // count=0 is meaningless; server should send INVALID_DEFINE
        uint8_t field_bytes[] = { ASRT_STRM_FIELD_ARRAY, 0x00, 0x00, ASRT_STRM_FIELD_U32 };
        CHECK_EQ(
            ASRT_SUCCESS,
            strm_deliver_define_raw( &server, 0, 1, field_bytes, sizeof field_bytes ) );
        drain();
        REQUIRE_EQ( 1U, coll.data.size() );
        CHECK_EQ( ASRT_STRM_MSG_ERROR, coll.data[0].data[0] );
        CHECK_EQ( ASRT_STRM_ERR_INVALID_DEFINE, coll.data[0].data[1] );
        coll.data.clear();
}

TEST_CASE_FIXTURE( strm_server_ctx, "strm_server_define: truncated array tag rejected" )
{
        // ARRAY tag present but u16 count is cut off
        uint8_t field_bytes[] = { ASRT_STRM_FIELD_ARRAY, 0x00 };  // missing count_lo and elem
        CHECK_EQ(
            ASRT_SUCCESS,
            strm_deliver_define_raw( &server, 0, 1, field_bytes, sizeof field_bytes ) );
        drain();
        REQUIRE_EQ( 1U, coll.data.size() );
        CHECK_EQ( ASRT_STRM_MSG_ERROR, coll.data[0].data[0] );
        CHECK_EQ( ASRT_STRM_ERR_INVALID_DEFINE, coll.data[0].data[1] );
        coll.data.clear();
}

TEST_CASE_FIXTURE( strm_server_ctx, "strm_server_data: array schema stores and retrieves record" )
{
        // Define schema: uint32_t[3] → record_size = 12
        uint8_t field_bytes[] = { ASRT_STRM_FIELD_ARRAY, 0x00, 0x03, ASRT_STRM_FIELD_U32 };
        CHECK_EQ(
            ASRT_SUCCESS,
            strm_deliver_define_raw( &server, 0, 1, field_bytes, sizeof field_bytes ) );
        REQUIRE_NE( nullptr, server.lookup[0] );
        CHECK_EQ( 12, server.lookup[0]->record_size );

        // Emit one record: [1,0,0,0, 2,0,0,0, 3,0,0,0] (big-endian u32 values 1,2,3)
        uint8_t data[] = { 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3 };
        CHECK_EQ( ASRT_SUCCESS, strm_deliver_data( &server, 0, data, sizeof data ) );

        auto* schema = server.lookup[0];
        REQUIRE_EQ( 1U, schema->count );
        REQUIRE_NE( nullptr, schema->first );
        CHECK_EQ( 0, memcmp( schema->first->data, data, 12 ) );
}

// ---------------------------------------------------------------------------
// Array define: more field configurations
// ---------------------------------------------------------------------------

TEST_CASE_FIXTURE( strm_server_ctx, "strm_server_define: array(4×u8) between two scalars" )
{
        // [I16, ARRAY(4×U8), U32] → record_size = 2+4+4 = 10, field_count = 3
        uint8_t field_bytes[] = {
            ASRT_STRM_FIELD_I16,
            ASRT_STRM_FIELD_ARRAY,
            0x00,
            0x04,
            ASRT_STRM_FIELD_U8,
            ASRT_STRM_FIELD_U32,
        };
        CHECK_EQ(
            ASRT_SUCCESS,
            strm_deliver_define_raw( &server, 1, 3, field_bytes, sizeof field_bytes ) );
        REQUIRE_NE( nullptr, server.lookup[1] );
        CHECK_EQ( 3, server.lookup[1]->field_count );
        CHECK_EQ( 10, server.lookup[1]->record_size );
}

TEST_CASE_FIXTURE( strm_server_ctx, "strm_server_define: single array(2×float)" )
{
        // [ARRAY(2×FLOAT)] → record_size = 2×4 = 8, field_count = 1
        uint8_t field_bytes[] = {
            ASRT_STRM_FIELD_ARRAY,
            0x00,
            0x02,
            ASRT_STRM_FIELD_FLOAT,
        };
        CHECK_EQ(
            ASRT_SUCCESS,
            strm_deliver_define_raw( &server, 0, 1, field_bytes, sizeof field_bytes ) );
        REQUIRE_NE( nullptr, server.lookup[0] );
        CHECK_EQ( 1, server.lookup[0]->field_count );
        CHECK_EQ( 8, server.lookup[0]->record_size );
}

TEST_CASE_FIXTURE( strm_server_ctx, "strm_server_define: two adjacent arrays" )
{
        // [ARRAY(2×U32), ARRAY(3×U8)] → record_size = 8+3 = 11, field_count = 2
        uint8_t field_bytes[] = {
            ASRT_STRM_FIELD_ARRAY,
            0x00,
            0x02,
            ASRT_STRM_FIELD_U32,
            ASRT_STRM_FIELD_ARRAY,
            0x00,
            0x03,
            ASRT_STRM_FIELD_U8,
        };
        CHECK_EQ(
            ASRT_SUCCESS,
            strm_deliver_define_raw( &server, 2, 2, field_bytes, sizeof field_bytes ) );
        REQUIRE_NE( nullptr, server.lookup[2] );
        CHECK_EQ( 2, server.lookup[2]->field_count );
        CHECK_EQ( 11, server.lookup[2]->record_size );
}

TEST_CASE_FIXTURE( strm_server_ctx, "strm_server_define: scalar array nested scalar" )
{
        // [U8, ARRAY(2×ARRAY(3×U8)), U16] → record_size = 1 + 2×3 + 2 = 9, field_count = 3
        uint8_t field_bytes[] = {
            ASRT_STRM_FIELD_U8,
            ASRT_STRM_FIELD_ARRAY,
            0x00,
            0x02,
            ASRT_STRM_FIELD_ARRAY,
            0x00,
            0x03,
            ASRT_STRM_FIELD_U8,
            ASRT_STRM_FIELD_U16,
        };
        CHECK_EQ(
            ASRT_SUCCESS,
            strm_deliver_define_raw( &server, 0, 3, field_bytes, sizeof field_bytes ) );
        REQUIRE_NE( nullptr, server.lookup[0] );
        CHECK_EQ( 3, server.lookup[0]->field_count );
        CHECK_EQ( 9, server.lookup[0]->record_size );
}

TEST_CASE_FIXTURE( strm_server_ctx, "strm_server_define: array count uses full u16 (256×u8)" )
{
        // count = 0x0100 = 256; tests big-endian u16 beyond a single byte
        uint8_t field_bytes[] = {
            ASRT_STRM_FIELD_ARRAY,
            0x01,
            0x00,
            ASRT_STRM_FIELD_U8,
        };
        CHECK_EQ(
            ASRT_SUCCESS,
            strm_deliver_define_raw( &server, 0, 1, field_bytes, sizeof field_bytes ) );
        REQUIRE_NE( nullptr, server.lookup[0] );
        CHECK_EQ( 1, server.lookup[0]->field_count );
        CHECK_EQ( 256, server.lookup[0]->record_size );
}

TEST_CASE_FIXTURE( strm_server_ctx, "strm_server_define: array count 1 (minimum)" )
{
        // ARRAY(1×U32) — count of 1 is valid; record_size = 4
        uint8_t field_bytes[] = {
            ASRT_STRM_FIELD_ARRAY,
            0x00,
            0x01,
            ASRT_STRM_FIELD_U32,
        };
        CHECK_EQ(
            ASRT_SUCCESS,
            strm_deliver_define_raw( &server, 0, 1, field_bytes, sizeof field_bytes ) );
        REQUIRE_NE( nullptr, server.lookup[0] );
        CHECK_EQ( 4, server.lookup[0]->record_size );
}

TEST_CASE_FIXTURE( strm_server_ctx, "strm_server_define: array of all integer types" )
{
        // [ARRAY(1×I8), ARRAY(1×I16), ARRAY(1×I32), ARRAY(1×U8), ARRAY(1×U16)]
        // record_size = 1+2+4+1+2 = 10, field_count = 5
        uint8_t field_bytes[] = {
            ASRT_STRM_FIELD_ARRAY, 0x00, 0x01, ASRT_STRM_FIELD_I8,
            ASRT_STRM_FIELD_ARRAY, 0x00, 0x01, ASRT_STRM_FIELD_I16,
            ASRT_STRM_FIELD_ARRAY, 0x00, 0x01, ASRT_STRM_FIELD_I32,
            ASRT_STRM_FIELD_ARRAY, 0x00, 0x01, ASRT_STRM_FIELD_U8,
            ASRT_STRM_FIELD_ARRAY, 0x00, 0x01, ASRT_STRM_FIELD_U16,
        };
        CHECK_EQ(
            ASRT_SUCCESS,
            strm_deliver_define_raw( &server, 0, 5, field_bytes, sizeof field_bytes ) );
        REQUIRE_NE( nullptr, server.lookup[0] );
        CHECK_EQ( 5, server.lookup[0]->field_count );
        CHECK_EQ( 10, server.lookup[0]->record_size );
}

TEST_CASE_FIXTURE( strm_server_ctx, "strm_server_define: array element missing tag rejected" )
{
        // ARRAY tag + count present but no element tag follows
        uint8_t field_bytes[] = { ASRT_STRM_FIELD_ARRAY, 0x00, 0x03 };
        CHECK_EQ(
            ASRT_SUCCESS,
            strm_deliver_define_raw( &server, 0, 1, field_bytes, sizeof field_bytes ) );
        drain();
        REQUIRE_EQ( 1U, coll.data.size() );
        CHECK_EQ( ASRT_STRM_MSG_ERROR, coll.data[0].data[0] );
        CHECK_EQ( ASRT_STRM_ERR_INVALID_DEFINE, coll.data[0].data[1] );
        coll.data.clear();
}

TEST_CASE_FIXTURE(
    strm_server_ctx,
    "strm_server_define: nested array with truncated inner count rejected" )
{
        // outer ARRAY tag + count OK, inner ARRAY tag present but count_lo cut off
        uint8_t field_bytes[] = {
            ASRT_STRM_FIELD_ARRAY,
            0x00,
            0x02,
            ASRT_STRM_FIELD_ARRAY,
            0x00,  // missing count_lo and elem
        };
        CHECK_EQ(
            ASRT_SUCCESS,
            strm_deliver_define_raw( &server, 0, 1, field_bytes, sizeof field_bytes ) );
        drain();
        REQUIRE_EQ( 1U, coll.data.size() );
        CHECK_EQ( ASRT_STRM_MSG_ERROR, coll.data[0].data[0] );
        CHECK_EQ( ASRT_STRM_ERR_INVALID_DEFINE, coll.data[0].data[1] );
        coll.data.clear();
}

// ---------------------------------------------------------------------------
// Array data: store and retrieve with various mixed schemas
// ---------------------------------------------------------------------------

TEST_CASE_FIXTURE( strm_server_ctx, "strm_server_data: u8 + array(3×u16) + bool" )
{
        // Schema: [U8, ARRAY(3×U16), BOOL] → record_size = 1+6+1 = 8
        uint8_t field_bytes[] = {
            ASRT_STRM_FIELD_U8,
            ASRT_STRM_FIELD_ARRAY,
            0x00,
            0x03,
            ASRT_STRM_FIELD_U16,
            ASRT_STRM_FIELD_BOOL,
        };
        CHECK_EQ(
            ASRT_SUCCESS,
            strm_deliver_define_raw( &server, 0, 3, field_bytes, sizeof field_bytes ) );
        REQUIRE_NE( nullptr, server.lookup[0] );
        CHECK_EQ( 8, server.lookup[0]->record_size );

        // Record: u8=0x07, u16[0]=100, u16[1]=200, u16[2]=300, bool=1
        uint8_t  data[8];
        uint8_t* p = data;
        *p++       = 0x07;
        asrt_add_u16( &p, 100 );
        asrt_add_u16( &p, 200 );
        asrt_add_u16( &p, 300 );
        *p++ = 0x01;
        CHECK_EQ( ASRT_SUCCESS, strm_deliver_data( &server, 0, data, sizeof data ) );

        REQUIRE_EQ( 1U, server.lookup[0]->count );
        CHECK_EQ( 0, memcmp( server.lookup[0]->first->data, data, 8 ) );
}

TEST_CASE_FIXTURE( strm_server_ctx, "strm_server_data: array(2×u32) + i8" )
{
        // Schema: [ARRAY(2×U32), I8] → record_size = 8+1 = 9
        uint8_t field_bytes[] = {
            ASRT_STRM_FIELD_ARRAY,
            0x00,
            0x02,
            ASRT_STRM_FIELD_U32,
            ASRT_STRM_FIELD_I8,
        };
        CHECK_EQ(
            ASRT_SUCCESS,
            strm_deliver_define_raw( &server, 0, 2, field_bytes, sizeof field_bytes ) );
        REQUIRE_NE( nullptr, server.lookup[0] );
        CHECK_EQ( 9, server.lookup[0]->record_size );

        // Record: u32[0]=1000, u32[1]=2000, i8=-5
        uint8_t  data[9];
        uint8_t* p = data;
        asrt_add_u32( &p, 1000 );
        asrt_add_u32( &p, 2000 );
        *p++ = static_cast< uint8_t >( static_cast< int8_t >( -5 ) );
        CHECK_EQ( ASRT_SUCCESS, strm_deliver_data( &server, 0, data, sizeof data ) );

        REQUIRE_EQ( 1U, server.lookup[0]->count );
        CHECK_EQ( 0, memcmp( server.lookup[0]->first->data, data, 9 ) );
}

TEST_CASE_FIXTURE( strm_server_ctx, "strm_server_data: i32 + array(4×u8) + u16" )
{
        // Schema: [I32, ARRAY(4×U8), U16] → record_size = 4+4+2 = 10
        uint8_t field_bytes[] = {
            ASRT_STRM_FIELD_I32,
            ASRT_STRM_FIELD_ARRAY,
            0x00,
            0x04,
            ASRT_STRM_FIELD_U8,
            ASRT_STRM_FIELD_U16,
        };
        CHECK_EQ(
            ASRT_SUCCESS,
            strm_deliver_define_raw( &server, 0, 3, field_bytes, sizeof field_bytes ) );
        REQUIRE_NE( nullptr, server.lookup[0] );
        CHECK_EQ( 10, server.lookup[0]->record_size );

        // Record: i32=-1, u8[4]={10,20,30,40}, u16=9999
        uint8_t  data[10];
        uint8_t* p = data;
        asrt_add_i32( &p, -1 );
        *p++ = 10;
        *p++ = 20;
        *p++ = 30;
        *p++ = 40;
        asrt_add_u16( &p, 9999 );
        CHECK_EQ( ASRT_SUCCESS, strm_deliver_data( &server, 0, data, sizeof data ) );

        REQUIRE_EQ( 1U, server.lookup[0]->count );
        CHECK_EQ( 0, memcmp( server.lookup[0]->first->data, data, 10 ) );
}

TEST_CASE_FIXTURE( strm_server_ctx, "strm_server_data: two adjacent arrays" )
{
        // Schema: [ARRAY(2×U32), ARRAY(3×U8)] → record_size = 8+3 = 11
        uint8_t field_bytes[] = {
            ASRT_STRM_FIELD_ARRAY,
            0x00,
            0x02,
            ASRT_STRM_FIELD_U32,
            ASRT_STRM_FIELD_ARRAY,
            0x00,
            0x03,
            ASRT_STRM_FIELD_U8,
        };
        CHECK_EQ(
            ASRT_SUCCESS,
            strm_deliver_define_raw( &server, 0, 2, field_bytes, sizeof field_bytes ) );
        REQUIRE_NE( nullptr, server.lookup[0] );
        CHECK_EQ( 11, server.lookup[0]->record_size );

        // Record: u32[0]=0xDEAD, u32[1]=0xBEEF, u8[0..2]={1,2,3}
        uint8_t  data[11];
        uint8_t* p = data;
        asrt_add_u32( &p, 0xDEAD );
        asrt_add_u32( &p, 0xBEEF );
        *p++ = 1;
        *p++ = 2;
        *p++ = 3;
        CHECK_EQ( ASRT_SUCCESS, strm_deliver_data( &server, 0, data, sizeof data ) );

        REQUIRE_EQ( 1U, server.lookup[0]->count );
        CHECK_EQ( 0, memcmp( server.lookup[0]->first->data, data, 11 ) );
}

TEST_CASE_FIXTURE( strm_server_ctx, "strm_server_data: nested array(2×array(3×u8)) + u16" )
{
        // Schema: [ARRAY(2×ARRAY(3×U8)), U16] → record_size = 2×3 + 2 = 8
        uint8_t field_bytes[] = {
            ASRT_STRM_FIELD_ARRAY,
            0x00,
            0x02,
            ASRT_STRM_FIELD_ARRAY,
            0x00,
            0x03,
            ASRT_STRM_FIELD_U8,
            ASRT_STRM_FIELD_U16,
        };
        CHECK_EQ(
            ASRT_SUCCESS,
            strm_deliver_define_raw( &server, 0, 2, field_bytes, sizeof field_bytes ) );
        REQUIRE_NE( nullptr, server.lookup[0] );
        CHECK_EQ( 8, server.lookup[0]->record_size );

        // Record: u8[2][3]={ {10,20,30},{40,50,60} }, u16=777
        uint8_t  data[8];
        uint8_t* p = data;
        *p++       = 10;
        *p++       = 20;
        *p++       = 30;
        *p++       = 40;
        *p++       = 50;
        *p++       = 60;
        asrt_add_u16( &p, 777 );
        CHECK_EQ( ASRT_SUCCESS, strm_deliver_data( &server, 0, data, sizeof data ) );

        REQUIRE_EQ( 1U, server.lookup[0]->count );
        CHECK_EQ( 0, memcmp( server.lookup[0]->first->data, data, 8 ) );
}

TEST_CASE_FIXTURE( strm_server_ctx, "strm_server_data: multiple records with array schema" )
{
        // Schema: [U8, ARRAY(2×U16)] → record_size = 1+4 = 5
        uint8_t field_bytes[] = {
            ASRT_STRM_FIELD_U8,
            ASRT_STRM_FIELD_ARRAY,
            0x00,
            0x02,
            ASRT_STRM_FIELD_U16,
        };
        CHECK_EQ(
            ASRT_SUCCESS,
            strm_deliver_define_raw( &server, 0, 2, field_bytes, sizeof field_bytes ) );
        REQUIRE_NE( nullptr, server.lookup[0] );
        CHECK_EQ( 5, server.lookup[0]->record_size );

        // Emit three records
        uint8_t  rec0[5];
        uint8_t* p0 = rec0;
        *p0++       = 1;
        asrt_add_u16( &p0, 100 );
        asrt_add_u16( &p0, 200 );
        uint8_t  rec1[5];
        uint8_t* p1 = rec1;
        *p1++       = 2;
        asrt_add_u16( &p1, 300 );
        asrt_add_u16( &p1, 400 );
        uint8_t  rec2[5];
        uint8_t* p2 = rec2;
        *p2++       = 3;
        asrt_add_u16( &p2, 500 );
        asrt_add_u16( &p2, 600 );

        CHECK_EQ( ASRT_SUCCESS, strm_deliver_data( &server, 0, rec0, 5 ) );
        CHECK_EQ( ASRT_SUCCESS, strm_deliver_data( &server, 0, rec1, 5 ) );
        CHECK_EQ( ASRT_SUCCESS, strm_deliver_data( &server, 0, rec2, 5 ) );

        auto* schema = server.lookup[0];
        REQUIRE_EQ( 3U, schema->count );
        auto* r = schema->first;
        REQUIRE_NE( nullptr, r );
        CHECK_EQ( 0, memcmp( r->data, rec0, 5 ) );
        r = r->next;
        REQUIRE_NE( nullptr, r );
        CHECK_EQ( 0, memcmp( r->data, rec1, 5 ) );
        r = r->next;
        REQUIRE_NE( nullptr, r );
        CHECK_EQ( 0, memcmp( r->data, rec2, 5 ) );
        CHECK_EQ( nullptr, r->next );
}


TEST_CASE_FIXTURE( strm_server_ctx, "strm_server_recv: minimal define packet" )
{
        // Only message id, no schema_id or field_count
        uint8_t          buf[] = { ASRT_STRM_MSG_DEFINE };
        struct asrt_span sp    = { .b = buf, .e = buf + 1 };
        CHECK_EQ( ASRT_RECV_ERR, asrt_chann_recv( &server.node, sp ) );
}

TEST_CASE_FIXTURE( strm_server_ctx, "strm_server_define: zero field_count sends error" )
{
        enum asrt_strm_field_type_e fields[] = { ASRT_STRM_FIELD_U8 };
        CHECK_EQ( ASRT_SUCCESS, strm_deliver_define( &server, 0, fields, 0 ) );
        // Server should have sent an ERROR message back
        drain();
        REQUIRE_EQ( 1U, coll.data.size() );
        CHECK_EQ( ASRT_STRM, coll.data[0].id );
        CHECK_EQ( ASRT_STRM_MSG_ERROR, coll.data[0].data[0] );
        CHECK_EQ( ASRT_STRM_ERR_INVALID_DEFINE, coll.data[0].data[1] );
        coll.data.clear();
}

TEST_CASE_FIXTURE( strm_server_ctx, "strm_server_define: duplicate schema sends error" )
{
        enum asrt_strm_field_type_e fields[] = { ASRT_STRM_FIELD_U8 };
        CHECK_EQ( ASRT_SUCCESS, strm_deliver_define( &server, 5, fields, 1 ) );
        // Second define with same schema_id
        CHECK_EQ( ASRT_SUCCESS, strm_deliver_define( &server, 5, fields, 1 ) );
        drain();
        REQUIRE_EQ( 1U, coll.data.size() );
        CHECK_EQ( ASRT_STRM_MSG_ERROR, coll.data[0].data[0] );
        CHECK_EQ( ASRT_STRM_ERR_DUPLICATE_SCHEMA, coll.data[0].data[1] );
        coll.data.clear();
}

TEST_CASE_FIXTURE( strm_server_ctx, "strm_server_define: invalid field type tag" )
{
        // Build raw message with an invalid tag
        uint8_t          buf[] = { ASRT_STRM_MSG_DEFINE, 0, 1, 0xFF };
        struct asrt_span sp    = { .b = buf, .e = buf + 4 };
        check_recv( &server, sp );
        drain();
        REQUIRE_EQ( 1U, coll.data.size() );
        CHECK_EQ( ASRT_STRM_MSG_ERROR, coll.data[0].data[0] );
        CHECK_EQ( ASRT_STRM_ERR_INVALID_DEFINE, coll.data[0].data[1] );
        coll.data.clear();
}

TEST_CASE_FIXTURE( strm_server_ctx, "strm_server_define: truncated field tags" )
{
        // Says 3 fields but only provides 1 byte of tags
        uint8_t          buf[] = { ASRT_STRM_MSG_DEFINE, 0, 3, ASRT_STRM_FIELD_U8 };
        struct asrt_span sp    = { .b = buf, .e = buf + 4 };
        check_recv( &server, sp );
        drain();
        REQUIRE_EQ( 1U, coll.data.size() );
        CHECK_EQ( ASRT_STRM_MSG_ERROR, coll.data[0].data[0] );
        CHECK_EQ( ASRT_STRM_ERR_INVALID_DEFINE, coll.data[0].data[1] );
        coll.data.clear();
}

TEST_CASE_FIXTURE( strm_server_ctx, "strm_server_define: alloc failure on schema" )
{
        alloc_ctx.fail_at_call               = 1;  // fail first alloc (schema struct)
        enum asrt_strm_field_type_e fields[] = { ASRT_STRM_FIELD_U8 };
        CHECK_EQ( ASRT_SUCCESS, strm_deliver_define( &server, 0, fields, 1 ) );
        drain();
        REQUIRE_EQ( 1U, coll.data.size() );
        CHECK_EQ( ASRT_STRM_MSG_ERROR, coll.data[0].data[0] );
        CHECK_EQ( ASRT_STRM_ERR_ALLOC_FAILURE, coll.data[0].data[1] );
        coll.data.clear();
}

TEST_CASE_FIXTURE( strm_server_ctx, "strm_server_define: alloc failure on fields array" )
{
        alloc_ctx.fail_at_call               = 2;  // pass schema alloc, fail fields alloc
        enum asrt_strm_field_type_e fields[] = { ASRT_STRM_FIELD_U8 };
        CHECK_EQ( ASRT_SUCCESS, strm_deliver_define( &server, 0, fields, 1 ) );
        drain();
        REQUIRE_EQ( 1U, coll.data.size() );
        CHECK_EQ( ASRT_STRM_MSG_ERROR, coll.data[0].data[0] );
        CHECK_EQ( ASRT_STRM_ERR_ALLOC_FAILURE, coll.data[0].data[1] );
        coll.data.clear();
}

// --- DATA handling ---

TEST_CASE_FIXTURE( strm_server_ctx, "strm_server_data: unknown schema" )
{
        uint8_t data[] = { 0x42 };
        CHECK_EQ( ASRT_SUCCESS, strm_deliver_data( &server, 10, data, 1 ) );
        drain();
        REQUIRE_EQ( 1U, coll.data.size() );
        CHECK_EQ( ASRT_STRM_MSG_ERROR, coll.data[0].data[0] );
        CHECK_EQ( ASRT_STRM_ERR_UNKNOWN_SCHEMA, coll.data[0].data[1] );
        coll.data.clear();
}

TEST_CASE_FIXTURE( strm_server_ctx, "strm_server_data: size mismatch" )
{
        enum asrt_strm_field_type_e fields[] = { ASRT_STRM_FIELD_U32 };  // record_size=4
        CHECK_EQ( ASRT_SUCCESS, strm_deliver_define( &server, 0, fields, 1 ) );
        uint8_t data[] = { 1, 2 };  // only 2 bytes, not 4
        CHECK_EQ( ASRT_SUCCESS, strm_deliver_data( &server, 0, data, 2 ) );
        drain();
        REQUIRE_EQ( 1U, coll.data.size() );
        CHECK_EQ( ASRT_STRM_MSG_ERROR, coll.data[0].data[0] );
        CHECK_EQ( ASRT_STRM_ERR_SIZE_MISMATCH, coll.data[0].data[1] );
        coll.data.clear();
}

TEST_CASE_FIXTURE( strm_server_ctx, "strm_server_data: valid single record" )
{
        enum asrt_strm_field_type_e fields[] = { ASRT_STRM_FIELD_U8 };
        CHECK_EQ( ASRT_SUCCESS, strm_deliver_define( &server, 0, fields, 1 ) );

        uint8_t data[] = { 0xAB };
        CHECK_EQ( ASRT_SUCCESS, strm_deliver_data( &server, 0, data, 1 ) );

        auto* schema = server.lookup[0];
        REQUIRE_NE( nullptr, schema );
        CHECK_EQ( 1U, schema->count );
        REQUIRE_NE( nullptr, schema->first );
        CHECK_EQ( 0xAB, schema->first->data[0] );
        CHECK_EQ( schema->first, schema->last );
}

TEST_CASE_FIXTURE( strm_server_ctx, "strm_server_data: multiple records linked list" )
{
        enum asrt_strm_field_type_e fields[] = { ASRT_STRM_FIELD_U8 };
        CHECK_EQ( ASRT_SUCCESS, strm_deliver_define( &server, 0, fields, 1 ) );

        for ( uint8_t i = 0; i < 5; i++ )
                CHECK_EQ( ASRT_SUCCESS, strm_deliver_data( &server, 0, &i, 1 ) );

        auto* schema = server.lookup[0];
        CHECK_EQ( 5U, schema->count );

        // Walk the linked list
        auto* rec = schema->first;
        for ( uint8_t i = 0; i < 5; i++ ) {
                REQUIRE_NE( nullptr, rec );
                CHECK_EQ( i, rec->data[0] );
                rec = rec->next;
        }
        CHECK_EQ( nullptr, rec );
}

TEST_CASE_FIXTURE( strm_server_ctx, "strm_server_data: alloc failure on record node" )
{
        enum asrt_strm_field_type_e fields[] = { ASRT_STRM_FIELD_U8 };
        CHECK_EQ( ASRT_SUCCESS, strm_deliver_define( &server, 0, fields, 1 ) );

        // schema alloc=1, fields alloc=2 already done; next is record node=3
        alloc_ctx.fail_at_call = 3;
        uint8_t data[]         = { 0x01 };
        CHECK_EQ( ASRT_SUCCESS, strm_deliver_data( &server, 0, data, 1 ) );
        drain();
        REQUIRE_EQ( 1U, coll.data.size() );
        CHECK_EQ( ASRT_STRM_MSG_ERROR, coll.data[0].data[0] );
        CHECK_EQ( ASRT_STRM_ERR_ALLOC_FAILURE, coll.data[0].data[1] );
        coll.data.clear();
}

TEST_CASE_FIXTURE( strm_server_ctx, "strm_server_data: alloc failure on data buffer" )
{
        enum asrt_strm_field_type_e fields[] = { ASRT_STRM_FIELD_U8 };
        CHECK_EQ( ASRT_SUCCESS, strm_deliver_define( &server, 0, fields, 1 ) );

        // schema=1, fields=2, record node=3 OK, data buf=4 FAIL
        alloc_ctx.fail_at_call = 4;
        uint8_t data[]         = { 0x01 };
        CHECK_EQ( ASRT_SUCCESS, strm_deliver_data( &server, 0, data, 1 ) );
        drain();
        REQUIRE_EQ( 1U, coll.data.size() );
        CHECK_EQ( ASRT_STRM_MSG_ERROR, coll.data[0].data[0] );
        CHECK_EQ( ASRT_STRM_ERR_ALLOC_FAILURE, coll.data[0].data[1] );
        coll.data.clear();
}

TEST_CASE_FIXTURE( strm_server_ctx, "strm_server_data: truncated (missing schema_id)" )
{
        uint8_t          buf[] = { ASRT_STRM_MSG_DATA };
        struct asrt_span sp    = { .b = buf, .e = buf + 1 };
        CHECK_EQ( ASRT_RECV_ERR, asrt_chann_recv( &server.node, sp ) );
}

// --- take ---

TEST_CASE_FIXTURE( strm_server_ctx, "strm_server_take: empty returns zero" )
{
        auto result = asrt_stream_server_take( &server );
        CHECK_EQ( 0U, result.schema_count );
        CHECK_EQ( nullptr, result.schemas );
}

TEST_CASE_FIXTURE( strm_server_ctx, "strm_server_take: takes ownership, server cleared" )
{
        enum asrt_strm_field_type_e fields[] = { ASRT_STRM_FIELD_U8 };
        CHECK_EQ( ASRT_SUCCESS, strm_deliver_define( &server, 3, fields, 1 ) );
        uint8_t data[] = { 42 };
        CHECK_EQ( ASRT_SUCCESS, strm_deliver_data( &server, 3, data, 1 ) );

        auto result = asrt_stream_server_take( &server );
        REQUIRE_EQ( 1U, result.schema_count );
        CHECK_EQ( 3, result.schemas[0].schema_id );
        CHECK_EQ( 1U, result.schemas[0].count );
        CHECK_EQ( 42, result.schemas[0].first->data[0] );

        // Server lookup should be cleared
        CHECK_EQ( nullptr, server.lookup[3] );

        asrt_stream_schemas_free( &result );
}

TEST_CASE_FIXTURE( strm_server_ctx, "strm_server_take: multiple schemas" )
{
        enum asrt_strm_field_type_e f1[] = { ASRT_STRM_FIELD_U8 };
        enum asrt_strm_field_type_e f2[] = { ASRT_STRM_FIELD_U16 };
        CHECK_EQ( ASRT_SUCCESS, strm_deliver_define( &server, 0, f1, 1 ) );
        CHECK_EQ( ASRT_SUCCESS, strm_deliver_define( &server, 1, f2, 1 ) );

        auto result = asrt_stream_server_take( &server );
        REQUIRE_EQ( 2U, result.schema_count );
        // Schemas are ordered by ID (scan from 0)
        CHECK_EQ( 0, result.schemas[0].schema_id );
        CHECK_EQ( 1, result.schemas[1].schema_id );
        CHECK_EQ( 1, result.schemas[0].record_size );
        CHECK_EQ( 2, result.schemas[1].record_size );

        asrt_stream_schemas_free( &result );
}

// --- clear ---

TEST_CASE_FIXTURE( strm_server_ctx, "strm_server_clear: frees all schemas and records" )
{
        enum asrt_strm_field_type_e fields[] = { ASRT_STRM_FIELD_U8 };
        CHECK_EQ( ASRT_SUCCESS, strm_deliver_define( &server, 0, fields, 1 ) );
        uint8_t data[] = { 1 };
        CHECK_EQ( ASRT_SUCCESS, strm_deliver_data( &server, 0, data, 1 ) );

        asrt_stream_server_clear( &server );
        CHECK_EQ( nullptr, server.lookup[0] );

        // After clear, can re-define same schema
        CHECK_EQ( ASRT_SUCCESS, strm_deliver_define( &server, 0, fields, 1 ) );
        CHECK_NE( nullptr, server.lookup[0] );
}

// --- loopback: reactor client → controller server ---

namespace
{

struct strm_loopback_ctx : server_client_base< asrt_stream_server, asrt_stream_client >
{
        collector                 coll;
        asrt_node                 root_r = {};
        asrt_node                 root_c = {};
        struct asrt_send_req_list cli_sq = {};
        struct asrt_send_req_list srv_sq = {};

        stub_allocator_ctx alloc_ctx;

        strm_loopback_ctx()
        {
                root_r.send_queue = &cli_sq;
                root_c.send_queue = &srv_sq;
                auto alloc        = asrt_stub_allocator( &alloc_ctx );
                CHECK_EQ( ASRT_SUCCESS, asrt_stream_client_init( &client, &root_r ) );
                CHECK_EQ( ASRT_SUCCESS, asrt_stream_server_init( &server, &root_c, alloc ) );
        }
        ~strm_loopback_ctx()
        {
                asrt_stream_server_deinit( &server );
                coll.data.clear();
        }
        void client_tick( uint32_t now )
        {
                check_tick( &client, now );
                deliver( &cli_sq, &server.node );
        }
};

}  // namespace

TEST_CASE_FIXTURE( strm_loopback_ctx, "strm_loopback: define and verify schema received" )
{
        uint8_t     fields[] = { ASRT_STRM_FIELD_U8, ASRT_STRM_FIELD_FLOAT };
        asrt_status done_st  = ASRT_INTERNAL_ERR;
        auto        done_cb  = []( void* p, enum asrt_status s ) {
                *static_cast< asrt_status* >( p ) = s;
        };
        CHECK_EQ(
            ASRT_SUCCESS,
            asrt_stream_client_define(
                &client, 1, fields, sizeof( fields ), 2, done_cb, &done_st ) );
        client_tick( 1 );
        CHECK_EQ( ASRT_STRM_DONE, client.state );
        check_tick( &client, 2 );
        CHECK_EQ( ASRT_STRM_IDLE, client.state );
        CHECK_EQ( ASRT_SUCCESS, done_st );

        // Verify server received the schema
        auto* schema = server.lookup[1];
        REQUIRE_NE( nullptr, schema );
        CHECK_EQ( 1, schema->schema_id );
        CHECK_EQ( 2, schema->field_count );
        CHECK_EQ( 5, schema->record_size );  // 1 + 4
}

TEST_CASE_FIXTURE( strm_loopback_ctx, "strm_loopback: define + record + take" )
{
        uint8_t fields[] = { ASRT_STRM_FIELD_U8 };
        CHECK_EQ(
            ASRT_SUCCESS,
            asrt_stream_client_define(
                &client, 0, fields, sizeof( fields ), 1, nullptr, nullptr ) );
        spin( 2 );

        for ( uint8_t i = 10; i < 13; i++ ) {
                CHECK_EQ(
                    ASRT_SUCCESS, asrt_stream_client_emit( &client, 0, &i, 1, nullptr, nullptr ) );
                spin( 2 );
        }

        // Take from server
        auto result = asrt_stream_server_take( &server );
        REQUIRE_EQ( 1U, result.schema_count );
        CHECK_EQ( 3U, result.schemas[0].count );

        auto* rec = result.schemas[0].first;
        for ( uint8_t i = 10; i < 13; i++ ) {
                REQUIRE_NE( nullptr, rec );
                CHECK_EQ( i, rec->data[0] );
                rec = rec->next;
        }

        asrt_stream_schemas_free( &result );
}

TEST_CASE_FIXTURE( strm_loopback_ctx, "strm_loopback: duplicate define → error propagation" )
{
        uint8_t fields[] = { ASRT_STRM_FIELD_U8 };
        // First define succeeds
        CHECK_EQ(
            ASRT_SUCCESS,
            asrt_stream_client_define(
                &client, 0, fields, sizeof( fields ), 1, nullptr, nullptr ) );
        spin( 2 );
        CHECK_EQ( ASRT_STRM_IDLE, client.state );

        // Second define with same ID → controller sends ERROR → client enters ERROR
        CHECK_EQ(
            ASRT_SUCCESS,
            asrt_stream_client_define(
                &client, 0, fields, sizeof( fields ), 1, nullptr, nullptr ) );
        spin( 2 );
        CHECK_EQ( ASRT_STRM_ERROR, client.state );
        CHECK_EQ( ASRT_STRM_ERR_DUPLICATE_SCHEMA, client.err_code );
}

TEST_CASE_FIXTURE( strm_loopback_ctx, "strm_loopback: data size mismatch → error" )
{
        uint8_t fields[] = { ASRT_STRM_FIELD_U32 };  // expects 4 bytes
        CHECK_EQ(
            ASRT_SUCCESS,
            asrt_stream_client_define(
                &client, 0, fields, sizeof( fields ), 1, nullptr, nullptr ) );
        spin( 2 );

        // Send 1 byte instead of 4 → server sends error → client enters ERROR
        uint8_t bad_data[] = { 0x01 };
        CHECK_EQ(
            ASRT_SUCCESS, asrt_stream_client_emit( &client, 0, bad_data, 1, nullptr, nullptr ) );
        spin( 2 );
        CHECK_EQ( ASRT_STRM_ERROR, client.state );
        CHECK_EQ( ASRT_STRM_ERR_SIZE_MISMATCH, client.err_code );
}

TEST_CASE_FIXTURE( strm_loopback_ctx, "strm_loopback: record after error rejected" )
{
        uint8_t fields[] = { ASRT_STRM_FIELD_U32 };
        CHECK_EQ(
            ASRT_SUCCESS,
            asrt_stream_client_define(
                &client, 0, fields, sizeof( fields ), 1, nullptr, nullptr ) );
        spin( 2 );

        // Force error
        uint8_t bad[] = { 0x01 };
        CHECK_EQ( ASRT_SUCCESS, asrt_stream_client_emit( &client, 0, bad, 1, nullptr, nullptr ) );
        spin( 2 );
        CHECK_EQ( ASRT_STRM_ERROR, client.state );

        // Further records should fail
        uint8_t good[] = { 1, 2, 3, 4 };
        CHECK_EQ(
            ASRT_INTERNAL_ERR, asrt_stream_client_emit( &client, 0, good, 4, nullptr, nullptr ) );
}

TEST_CASE_FIXTURE( strm_loopback_ctx, "strm_loopback: reset after error, redefine" )
{
        uint8_t fields[] = { ASRT_STRM_FIELD_U32 };
        CHECK_EQ(
            ASRT_SUCCESS,
            asrt_stream_client_define(
                &client, 0, fields, sizeof( fields ), 1, nullptr, nullptr ) );
        spin( 2 );

        // Force error
        uint8_t bad[] = { 0x01 };
        CHECK_EQ( ASRT_SUCCESS, asrt_stream_client_emit( &client, 0, bad, 1, nullptr, nullptr ) );
        spin( 2 );
        CHECK_EQ( ASRT_STRM_ERROR, client.state );

        // Reset client
        CHECK_EQ( ASRT_SUCCESS, asrt_stream_client_reset( &client ) );
        CHECK_EQ( ASRT_STRM_IDLE, client.state );

        // Clear server and redefine
        asrt_stream_server_clear( &server );
        uint8_t fields2[] = { ASRT_STRM_FIELD_U8 };
        CHECK_EQ(
            ASRT_SUCCESS,
            asrt_stream_client_define(
                &client, 0, fields2, sizeof( fields2 ), 1, nullptr, nullptr ) );
        spin( 2 );
        CHECK_EQ( ASRT_STRM_IDLE, client.state );

        uint8_t good[] = { 42 };
        CHECK_EQ( ASRT_SUCCESS, asrt_stream_client_emit( &client, 0, good, 1, nullptr, nullptr ) );
        spin( 2 );

        auto result = asrt_stream_server_take( &server );
        REQUIRE_EQ( 1U, result.schema_count );
        CHECK_EQ( 1U, result.schemas[0].count );
        CHECK_EQ( 42, result.schemas[0].first->data[0] );
        asrt_stream_schemas_free( &result );
}
