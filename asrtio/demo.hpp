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

/// @file demo.hpp
/// @brief Demo test types for the rsim simulator.
///
/// Each factory function returns a `demo_test` instance demonstrating a
/// specific testing pattern.  Register them with the reactor through
/// `unit<demo_test>` exactly like any other test type.

#include "../asrtr/diag.h"
#include "../asrtr/record.h"
#include "../asrtrpp/collect.hpp"
#include "../asrtrpp/diag.hpp"
#include "../asrtrpp/param.hpp"
#include "../asrtrpp/stream.hpp"
#include "../asrtrpp/task_unit.hpp"
#include "./task.hpp"

#include <functional>
#include <random>
#include <string>
#include <string_view>

namespace asrtio
{

struct demo_test;

struct demo_spec
{
        using body_fn = std::function< asrt_status( demo_test&, asrt::record& ) >;

        std::string tname;
        body_fn     body;
};

struct demo_test
{
        demo_spec          spec;
        asrt_diag_client&  diag;
        asrt_param_client& param;
        std::mt19937&      rng;
        int                counter = 0;

        demo_test( demo_spec s, asrt_diag_client& d, asrt_param_client& p, std::mt19937& r )
          : spec( std::move( s ) )
          , diag( d )
          , param( p )
          , rng( r )
        {
        }

        char const* name() const { return spec.tname.c_str(); }

        asrt_status operator()( asrt::record& r ) { return spec.body( *this, r ); }
};

// ---------------------------------------------------------------------------
// Factory functions — each one demonstrates a different testing pattern.

/// Simplest possible test: immediately passes.
inline demo_spec make_demo_pass()
{
        return { .tname = "demo_pass", .body = []( demo_test&, asrt::record& r ) -> asrt_status {
                        r.state = ASRT_TEST_PASS;
                        return ASRT_SUCCESS;
                } };
}

/// Immediately fails and records a diagnostic message.
inline demo_spec make_demo_fail()
{
        return {
            .tname = "demo_fail", .body = []( demo_test& self, asrt::record& r ) -> asrt_status {
                    r.state = ASRT_TEST_FAIL;
                    asrt::rec_diag( self.diag, "demo.hpp", __LINE__, "intentional", {} );
                    return ASRT_SUCCESS;
            } };
}

/// Uses ASRT_RECORD_CHECK with a passing condition.
/// Shows the soft-assertion macro in the success case.
inline demo_spec make_demo_check()
{
        return { .tname = "demo_check", .body = []( demo_test&, asrt::record& r ) -> asrt_status {
                        int sum = 2 + 3;
                        ASRT_RECORD_CHECK( &r, sum == 5 );
                        if ( r.state != ASRT_TEST_FAIL )
                                r.state = ASRT_TEST_PASS;
                        return ASRT_SUCCESS;
                } };
}

/// Uses ASRT_RECORD_CHECK with a failing condition.
/// The check fails (soft failure — execution continues), and a diagnostic
/// is recorded so the controller can report the location.
inline demo_spec make_demo_check_fail()
{
        return {
            .tname = "demo_check_fail",
            .body  = []( demo_test& self, asrt::record& r ) -> asrt_status {
                    int sum = 2 + 2;
                    ASRT_RECORD_CHECK( &r, sum == 5 );
                    if ( r.state == ASRT_TEST_FAIL )
                            asrt::rec_diag( self.diag, "demo.hpp", __LINE__, "sum == 5", {} );
                    if ( r.state != ASRT_TEST_FAIL )
                            r.state = ASRT_TEST_PASS;
                    return ASRT_SUCCESS;
            } };
}

/// Uses ASRT_RECORD_REQUIRE with a failing condition.
/// The require macro fails and returns immediately — the lines after it
/// are never executed.
inline demo_spec make_demo_require_fail()
{
        return {
            .tname = "demo_require_fail", .body = []( demo_test&, asrt::record& r ) -> asrt_status {
                    ASRT_RECORD_REQUIRE( &r, 1 == 2 );
                    // Never reached — REQUIRE returned early.
                    r.state = ASRT_TEST_PASS;
                    return ASRT_SUCCESS;
            } };
}

/// Multi-step test: stays RUNNING for a few ticks, then passes.
/// Demonstrates the state-machine pattern where the test callback
/// is invoked repeatedly until it reports a final state.
inline demo_spec make_demo_counter()
{
        return {
            .tname = "demo_counter", .body = []( demo_test& self, asrt::record& r ) -> asrt_status {
                    if ( self.counter < 3 ) {
                            ++self.counter;
                            r.state = ASRT_TEST_RUNNING;
                    } else {
                            r.state = ASRT_TEST_PASS;
                    }
                    return ASRT_SUCCESS;
            } };
}

/// Nondeterministic test: randomly passes or fails based on the RNG seed.
/// Demonstrates seed-controlled randomisation — the same seed always
/// produces the same outcome.
inline demo_spec make_demo_random()
{
        return {
            .tname = "demo_random", .body = []( demo_test& self, asrt::record& r ) -> asrt_status {
                    std::uniform_int_distribution< int > pick( 0, 2 );
                    static constexpr asrt_test_state     k_results[] = {
                        ASRT_TEST_PASS, ASRT_TEST_FAIL, ASRT_TEST_ERROR };
                    r.state = k_results[pick( self.rng )];
                    if ( r.state != ASRT_TEST_PASS )
                            asrt::rec_diag( self.diag, "demo.hpp", __LINE__, "random outcome", {} );
                    return ASRT_SUCCESS;
            } };
}

/// Nondeterministic multi-step test: runs for a random number of ticks
/// (1–6), then passes.  Combines the counter pattern with RNG-driven
/// duration.
inline demo_spec make_demo_random_counter()
{
        return {
            .tname = "demo_random_counter",
            .body  = []( demo_test& self, asrt::record& r ) -> asrt_status {
                    // On the first tick, pick a random target count.
                    if ( self.counter == 0 ) {
                            std::uniform_int_distribution< int > d( 1, 6 );
                            self.counter = -d( self.rng );  // negative = target
                    }
                    if ( self.counter < 0 ) {
                            ++self.counter;
                            r.state = ASRT_TEST_RUNNING;
                            if ( self.counter == 0 )
                                    r.state = ASRT_TEST_PASS;
                    }
                    return ASRT_SUCCESS;
            } };
}

// ---------------------------------------------------------------------------
// Param-aware demo tests — demonstrate reading parameters from the tree.

namespace detail
{

struct param_qr
{
        asrt::flat_id    first_child = 0;
        uint32_t         u32_val     = 0;
        asrt::flat_id    next_sib    = 0;
        asrt_param_query q           = {};
};

inline void param_obj_qr_cb( asrt_param_client*, asrt_param_query* qq, asrt_flat_child_list val )
{
        auto& s = *static_cast< param_qr* >( qq->cb_ptr );
        if ( qq->error_code == 0 ) {
                s.first_child = val.first_child;
                s.next_sib    = qq->next_sibling;
        }
}

inline void param_u32_qr_cb( asrt_param_client*, asrt_param_query* qq, uint32_t val )
{
        auto& s = *static_cast< param_qr* >( qq->cb_ptr );
        if ( qq->error_code == 0 ) {
                s.u32_val  = val;
                s.next_sib = qq->next_sibling;
        }
}

inline void param_any_qr_cb( asrt_param_client*, asrt_param_query* qq, asrt_flat_value )
{
        auto& s = *static_cast< param_qr* >( qq->cb_ptr );
        if ( qq->error_code == 0 )
                s.next_sib = qq->next_sibling;
}

}  // namespace detail

/// Reads the first u32 child of the param root object and checks > 0.
/// If no param tree is loaded, passes trivially.
/// Demonstrates reading a single param value.
inline demo_spec make_demo_param_value()
{
        return {
            .tname = "demo_param_value",
            .body  = [pq = detail::param_qr{}](
                        demo_test& self, asrt::record& r ) mutable -> asrt_status {
                    if ( r.state == ASRT_TEST_INIT ) {
                            self.counter = 0;
                            pq           = {};
                    }
                    if ( asrt::query_pending( self.param ) ) {
                            r.state = ASRT_TEST_RUNNING;
                            return ASRT_SUCCESS;
                    }
                    switch ( self.counter ) {
                    case 0:
                            if ( !asrt::ready( self.param ) ) {
                                    r.state = ASRT_TEST_PASS;
                                    return ASRT_SUCCESS;
                            }
                            pq = {};
                            if ( auto s = asrt::fetch< asrt::obj >(
                                     self.param,
                                     &pq.q,
                                     asrt::root_id( self.param ),
                                     detail::param_obj_qr_cb,
                                     &pq );
                                 s != ASRT_SUCCESS ) {
                                    asrt::rec_diag(
                                        self.diag, "demo.hpp", __LINE__, "fetch failed", {} );
                                    r.state = ASRT_TEST_FAIL;
                                    return ASRT_SUCCESS;
                            }
                            self.counter = 1;
                            r.state      = ASRT_TEST_RUNNING;
                            return ASRT_SUCCESS;
                    case 1: {
                            ASRT_RECORD_REQUIRE( &r, pq.q.error_code == 0 );
                            auto first = pq.first_child;
                            pq         = {};
                            if ( auto s = asrt::fetch< uint32_t >(
                                     self.param, &pq.q, first, detail::param_u32_qr_cb, &pq );
                                 s != ASRT_SUCCESS ) {
                                    asrt::rec_diag(
                                        self.diag, "demo.hpp", __LINE__, "fetch failed", {} );
                                    r.state = ASRT_TEST_FAIL;
                                    return ASRT_SUCCESS;
                            }
                            self.counter = 2;
                            r.state      = ASRT_TEST_RUNNING;
                            return ASRT_SUCCESS;
                    }
                    case 2:
                            ASRT_RECORD_CHECK( &r, pq.q.error_code == 0 );
                            if ( r.state == ASRT_TEST_FAIL ) {
                                    asrt::rec_diag(
                                        self.diag, "demo.hpp", __LINE__, "expected u32", {} );
                            } else {
                                    ASRT_RECORD_CHECK( &r, pq.u32_val > 0 );
                                    if ( r.state == ASRT_TEST_FAIL )
                                            asrt::rec_diag(
                                                self.diag, "demo.hpp", __LINE__, "val > 0", {} );
                                    else
                                            r.state = ASRT_TEST_PASS;
                            }
                            return ASRT_SUCCESS;
                    default:
                            break;
                    }
                    return ASRT_SUCCESS;
            } };
}

/// Counts children of the param root object. Passes if count > 0.
/// Demonstrates walking the sibling chain.
inline demo_spec make_demo_param_count()
{
        return {
            .tname = "demo_param_count",
            .body  = [pq          = detail::param_qr{},
                     child_count = 0]( demo_test& self, asrt::record& r ) mutable -> asrt_status {
                    if ( r.state == ASRT_TEST_INIT ) {
                            self.counter = 0;
                            child_count  = 0;
                            pq           = {};
                    }
                    if ( asrt::query_pending( self.param ) ) {
                            r.state = ASRT_TEST_RUNNING;
                            return ASRT_SUCCESS;
                    }
                    switch ( self.counter ) {
                    case 0:
                            if ( !asrt::ready( self.param ) ) {
                                    r.state = ASRT_TEST_PASS;
                                    return ASRT_SUCCESS;
                            }
                            pq = {};
                            if ( auto s = asrt::fetch< asrt::obj >(
                                     self.param,
                                     &pq.q,
                                     asrt::root_id( self.param ),
                                     detail::param_obj_qr_cb,
                                     &pq );
                                 s != ASRT_SUCCESS ) {
                                    asrt::rec_diag(
                                        self.diag, "demo.hpp", __LINE__, "fetch failed", {} );
                                    r.state = ASRT_TEST_FAIL;
                                    return ASRT_SUCCESS;
                            }
                            self.counter = 1;
                            r.state      = ASRT_TEST_RUNNING;
                            return ASRT_SUCCESS;
                    case 1: {
                            ASRT_RECORD_REQUIRE( &r, pq.q.error_code == 0 );
                            auto first = pq.first_child;
                            pq         = {};
                            if ( auto s = asrt::fetch< void >(
                                     self.param, &pq.q, first, detail::param_any_qr_cb, &pq );
                                 s != ASRT_SUCCESS ) {
                                    asrt::rec_diag(
                                        self.diag, "demo.hpp", __LINE__, "fetch failed", {} );
                                    r.state = ASRT_TEST_FAIL;
                                    return ASRT_SUCCESS;
                            }
                            self.counter = 2;
                            r.state      = ASRT_TEST_RUNNING;
                            return ASRT_SUCCESS;
                    }
                    case 2:
                            ++child_count;
                            if ( pq.next_sib != 0 ) {
                                    auto next = pq.next_sib;
                                    pq        = {};
                                    if ( auto s = asrt::fetch< void >(
                                             self.param,
                                             &pq.q,
                                             next,
                                             detail::param_any_qr_cb,
                                             &pq );
                                         s != ASRT_SUCCESS ) {
                                            asrt::rec_diag(
                                                self.diag,
                                                "demo.hpp",
                                                __LINE__,
                                                "fetch failed",
                                                {} );
                                            r.state = ASRT_TEST_FAIL;
                                            return ASRT_SUCCESS;
                                    }
                                    r.state = ASRT_TEST_RUNNING;
                                    return ASRT_SUCCESS;
                            }
                            ASRT_RECORD_CHECK( &r, child_count > 0 );
                            if ( r.state == ASRT_TEST_FAIL )
                                    asrt::rec_diag(
                                        self.diag, "demo.hpp", __LINE__, "child_count > 0", {} );
                            else
                                    r.state = ASRT_TEST_PASS;
                            return ASRT_SUCCESS;
                    default:
                            break;
                    }
                    return ASRT_SUCCESS;
            } };
}

/// Finds a child by key in the param root object and checks > 0.
/// Demonstrates the find-by-key API for direct key lookup.
inline demo_spec make_demo_param_find()
{
        return {
            .tname = "demo_param_find",
            .body  = [pq = detail::param_qr{}](
                        demo_test& self, asrt::record& r ) mutable -> asrt_status {
                    if ( r.state == ASRT_TEST_INIT ) {
                            self.counter = 0;
                            pq           = {};
                    }
                    if ( asrt::query_pending( self.param ) ) {
                            r.state = ASRT_TEST_RUNNING;
                            return ASRT_SUCCESS;
                    }
                    switch ( self.counter ) {
                    case 0:
                            if ( !asrt::ready( self.param ) ) {
                                    r.state = ASRT_TEST_PASS;
                                    return ASRT_SUCCESS;
                            }
                            pq = {};
                            if ( auto s = asrt::find< uint32_t >(
                                     self.param,
                                     &pq.q,
                                     asrt::root_id( self.param ),
                                     "count",
                                     detail::param_u32_qr_cb,
                                     &pq );
                                 s != ASRT_SUCCESS ) {
                                    asrt::rec_diag(
                                        self.diag, "demo.hpp", __LINE__, "find failed", {} );
                                    r.state = ASRT_TEST_FAIL;
                                    return ASRT_SUCCESS;
                            }
                            self.counter = 1;
                            r.state      = ASRT_TEST_RUNNING;
                            return ASRT_SUCCESS;
                    case 1:
                            ASRT_RECORD_CHECK( &r, pq.q.error_code == 0 );
                            if ( r.state == ASRT_TEST_FAIL ) {
                                    asrt::rec_diag(
                                        self.diag, "demo.hpp", __LINE__, "find 'count'", {} );
                            } else {
                                    ASRT_RECORD_CHECK( &r, pq.u32_val > 0 );
                                    if ( r.state == ASRT_TEST_FAIL )
                                            asrt::rec_diag(
                                                self.diag, "demo.hpp", __LINE__, "count > 0", {} );
                                    else
                                            r.state = ASRT_TEST_PASS;
                            }
                            return ASRT_SUCCESS;
                    default:
                            break;
                    }
                    return ASRT_SUCCESS;
            } };
}


/// Immediately passes — simplest possible coroutine test.
struct pass_demo_task : asrt::task_test
{
        char const* name = "pass_demo_task";

        asrt::task< void > exec() { co_return; }
};

/// Immediately fails via test_fail.
struct fail_demo_task : asrt::task_test
{
        char const* name = "fail_demo_task";

        asrt::task< void > exec() { co_yield asrt::with_error{ asrt::test_fail }; }
};

/// Reports a test infrastructure error via test_error.
struct error_demo_task : asrt::task_test
{
        char const* name = "error_demo_task";

        asrt::task< void > exec() { co_yield asrt::with_error{ ASRT_INIT_ERR }; }
};

/// Multi-step pass: suspends 3 times, then completes.
/// Coroutine equivalent of the demo_counter state machine.
struct counter_demo_task : asrt::task_test
{
        char const* name = "counter_demo_task";

        asrt::task< void > exec()
        {
                for ( int i = 0; i < 3; ++i )
                        co_await asrt::suspend;
        }
};

/// Checks a passing condition, then completes successfully.
struct check_demo_task : asrt::task_test
{
        char const* name = "check_demo_task";

        asrt::task< void > exec()
        {
                if ( 2 + 3 != 5 )
                        co_yield asrt::with_error{ asrt::test_fail };
        }
};

/// Checks a failing condition — yields test_fail.
struct check_fail_demo_task : asrt::task_test
{
        char const* name = "check_fail_demo_task";

        asrt::task< void > exec()
        {
                if ( 2 + 2 != 5 )
                        co_yield asrt::with_error{ asrt::test_fail };
        }
};

/// Suspends several times, then fails — failure after async work.
struct multi_step_fail_demo_task : asrt::task_test
{
        char const* name = "multi_step_fail_demo_task";

        asrt::task< void > exec()
        {
                for ( int i = 0; i < 3; ++i )
                        co_await asrt::suspend;
                co_yield asrt::with_error{ asrt::test_fail };
        }
};


/// Parameter integration into coroutine
struct param_query_demo_task : asrt::task_test
{
        char const*        name = "param_query_demo_task";
        asrt_param_client& pc;

        param_query_demo_task( task_ctx& ctx, asrt_param_client& p )
          : task_test( ctx )
          , pc( p )
        {
        }

        asrt::task< void > exec()
        {
                // Query param node with ID 1
                auto x = co_await asrt::fetch< uint32_t >( pc, 1 );
                auto y = co_await asrt::fetch< int32_t >( pc, 1 );
                if ( x != y )
                        co_yield asrt::with_error{ asrt::test_fail };
        }
};


/// Search by name in object - coroutine
struct param_query_find_demo_task : asrt::task_test
{
        char const*        name = "param_query_find_demo_task";
        asrt_param_client& pc;

        param_query_find_demo_task( task_ctx& ctx, asrt_param_client& p )
          : task_test( ctx )
          , pc( p )
        {
        }

        asrt::task< void > exec()
        {
                // Query param node with key "count" under root
                auto x = co_await asrt::find< uint32_t >( pc, asrt::root_id( pc ), "count" );
                auto y = co_await asrt::find< int32_t >( pc, asrt::root_id( pc ), "count" );
                if ( x != y )
                        co_yield asrt::with_error{ asrt::test_fail };
        }
};


/// Type overview: fetches every supported param type by key.
/// The param tree is expected to have children under root with keys:
///   "u32" (uint32_t), "i32" (int32_t), "flt" (float), "str" (string),
///   "bln" (bool), "obj" (object), "arr" (array).
struct param_type_overview_task : asrt::task_test
{
        char const*        name = "param_type_overview_task";
        asrt_param_client& pc;

        param_type_overview_task( task_ctx& ctx, asrt_param_client& p )
          : task_test( ctx )
          , pc( p )
        {
        }

        asrt::task< void > exec()
        {
                auto root = asrt::root_id( pc );

                auto u = co_await asrt::find< uint32_t >( pc, root, "u32" );
                if ( u != 42 )
                        co_yield asrt::with_error{ asrt::test_fail };

                auto i = co_await asrt::find< int32_t >( pc, root, "i32" );
                if ( i != -7 )
                        co_yield asrt::with_error{ asrt::test_fail };

                auto f = co_await asrt::find< float >( pc, root, "flt" );
                if ( f < 3.13F || f > 3.15F )
                        co_yield asrt::with_error{ asrt::test_fail };

                auto s = co_await asrt::find< char const* >( pc, root, "str" );
                if ( std::string_view{ s } != "hello" )
                        co_yield asrt::with_error{ asrt::test_fail };

                auto b = co_await asrt::find< bool >( pc, root, "bln" );
                if ( !b )
                        co_yield asrt::with_error{ asrt::test_fail };

                asrt_flat_child_list obj = co_await asrt::find< asrt::obj >( pc, root, "obj" );
                if ( obj.first_child == 0 )
                        co_yield asrt::with_error{ asrt::test_fail };

                asrt_flat_child_list arr = co_await asrt::find< asrt::arr >( pc, root, "arr" );
                if ( arr.first_child == 0 )
                        co_yield asrt::with_error{ asrt::test_fail };

                // Iterate over array elements
                uint32_t      count = 0;
                asrt::flat_id id    = arr.first_child;
                while ( id != 0 ) {
                        auto [val, key, next_sibling] = co_await asrt::fetch< uint32_t >( pc, id );
                        ++count;
                        id = next_sibling;
                }
                if ( count == 0 )
                        co_yield asrt::with_error{ asrt::test_fail };

                // Iterate over object children and touch their keys
                count = 0;
                id    = obj.first_child;
                while ( id != 0 ) {
                        auto [val, key, next_sibling] =
                            co_await asrt::fetch< asrt_flat_value >( pc, id );
                        if ( !key || key[0] == '\0' )
                                co_yield asrt::with_error{ asrt::test_fail };
                        ++count;
                        id = next_sibling;
                }
                if ( count == 0 )
                        co_yield asrt::with_error{ asrt::test_fail };
        }
};


/// Collector demo: appends a small tree via the collect channel.
/// Exercises every collect type: obj, arr, u32, i32, float, str, bool, u32d2.
/// Produces:
///   root OBJECT
///     "value"  U32    = 42
///     "offset" I32    = -7
///     "ratio"  FLOAT  = 1.5
///     "tag"    STR    = "demo"
///     "flag"   BOOL   = true
///     "ts"     U32D2  = { hi=0xDEADBEEF, lo=0xCAFEBABE }
///     "items"  ARRAY  → [U32=1, U32=2, U32=3]
///     "sub"    OBJECT → "x" U32=0
struct collect_demo_task : asrt::task_test
{
        char const*          name = "collect_demo_task";
        asrt_collect_client& cc;

        collect_demo_task( task_ctx& ctx, asrt_collect_client& c )
          : task_test( ctx )
          , cc( c )
        {
        }

        asrt::task< void > exec()
        {
                auto root = asrt::root_id( cc );
                auto obj  = co_await asrt::append< asrt::obj >( cc, root );
                co_await asrt::set( cc, obj, "value", 42U );
                co_await asrt::set( cc, obj, "offset", -7 );
                co_await asrt::set( cc, obj, "ratio", 1.5F );
                co_await asrt::set( cc, obj, "tag", "demo" );
                co_await asrt::set( cc, obj, "flag", true );
                co_await asrt::set(
                    cc, obj, "ts", asrt::u32d2{ .hi = 0xDEADBEEFU, .lo = 0xCAFEBABEU } );
                auto arr = co_await asrt::set< asrt::arr >( cc, obj, "items" );
                co_await asrt::append( cc, arr, 1U );
                co_await asrt::append( cc, arr, 2U );
                co_await asrt::append( cc, arr, 3U );
                auto sub = co_await asrt::set< asrt::obj >( cc, obj, "sub" );
                co_await asrt::set( cc, sub, "x", 0U );
        }
};


/// Stream demo: defines a schema with two fields (U32 + FLOAT) and sends
/// three records.
struct stream_demo_task : asrt::task_test
{
        char const*         name = "stream_demo_task";
        asrt_stream_client& sc;

        stream_demo_task( task_ctx& ctx, asrt_stream_client& s )
          : task_test( ctx )
          , sc( s )
        {
        }

        asrt::task< void > exec()
        {
                auto schema = co_await asrt::define< uint32_t, float >( sc, 0 );
                for ( uint32_t i = 0; i < 3; ++i )
                        co_await asrt::emit( schema, i * 100, 1.5F * (float) i );
        }
};

/// Stream sensor demo: defines a sensor schema (U32 timestamp, FLOAT
/// temperature, U8 humidity, U8 status) and streams 100 synthetic records.
struct stream_sensor_demo_task : asrt::task_test
{
        char const*         name = "stream_sensor_demo_task";
        asrt_stream_client& sc;

        stream_sensor_demo_task( task_ctx& ctx, asrt_stream_client& s )
          : task_test( ctx )
          , sc( s )
        {
        }

        asrt::task< void > exec()
        {
                auto schema = co_await asrt::define< uint32_t, float, uint8_t, uint8_t >( sc, 0 );
                for ( uint32_t i = 0; i < 100; ++i )
                        co_await asrt::emit(
                            schema,
                            i * 10,
                            20.0F + 0.1F * (float) i,
                            (uint8_t) ( 50 + i % 50 ),
                            (uint8_t) ( i % 4 ) );
        }
};

}  // namespace asrtio
