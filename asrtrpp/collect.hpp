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

#include "../asrtl/asrt_assert.h"
#include "../asrtl/log.h"
#include "../asrtl/status_to_str.h"
#include "../asrtlpp/callback.hpp"
#include "../asrtlpp/flat_type_traits.hpp"
#include "../asrtlpp/task.hpp"
#include "../asrtlpp/util.hpp"
#include "../asrtr/collect.h"

namespace asrt
{

/// Trait mapping C++ types to flat_value scalar member pointers.
/// Specialised for uint32_t, int32_t, float, char const*, bool,
/// and the container tags obj / arr.
template < typename T >
struct collect_append_traits;

template <>
struct collect_append_traits< uint32_t > : flat_type_traits< uint32_t >
{
        static constexpr auto member = &asrt_flat_scalar::u32_val;
};

template <>
struct collect_append_traits< int32_t > : flat_type_traits< int32_t >
{
        static constexpr auto member = &asrt_flat_scalar::i32_val;
};

template <>
struct collect_append_traits< float > : flat_type_traits< float >
{
        static constexpr auto member = &asrt_flat_scalar::float_val;
};

template <>
struct collect_append_traits< char const* > : flat_type_traits< char const* >
{
        static constexpr auto member = &asrt_flat_scalar::str_val;
};

template <>
struct collect_append_traits< bool > : flat_type_traits< bool >
{
        static constexpr auto member = &asrt_flat_scalar::bool_val;
};

template <>
struct collect_append_traits< u32d2 > : flat_type_traits< u32d2 >
{
        static constexpr auto member = &asrt_flat_scalar::u32d2_val;
};

template <>
struct collect_append_traits< obj > : flat_type_traits< obj >
{
};

template <>
struct collect_append_traits< arr > : flat_type_traits< arr >
{
};

/// Concept: T is a scalar type with a flat_value member pointer.
template < typename T >
concept collect_scalar = collect_append_traits< T >::is_scalar;

/// Concept: T is a container tag (OBJECT or ARRAY).
template < typename T >
concept collect_container = !collect_append_traits< T >::is_scalar;

ASRT_NODISCARD inline status init( asrt_collect_client& client, asrt_node& prev )
{
        return asrt_collect_client_init( &client, &prev );
}

ASRT_NODISCARD inline flat_id root_id( asrt_collect_client const& cc )
{
        return asrt_collect_client_root_id( &cc );
}

/// Sender that appends a single node to the collect client's tree.
inline asrt_flat_value _make_collect_scalar_value( auto val, auto member, auto type )
{
        using member_type = decltype( asrt_flat_scalar{}.*member );
        asrt_flat_value v = { .type = static_cast< asrt_flat_value_type >( type ) };
        v.data.s.*member  = static_cast< member_type >( val );
        return v;
}

struct _collect_insert_scalar_ctx
{
        using completion_signatures =
            ecor::completion_signatures< ecor::set_value_t(), ecor::set_error_t( status ) >;

        asrt_collect_client* client;
        flat_id              parent;
        char const*          key;
        asrt_flat_value      value;

        template < typename OP >
        void start( OP& op )
        {
                auto s = asrt_collect_client_insert(
                    client,
                    parent,
                    key,
                    &value,
                    nullptr,
                    +[]( void* ptr, enum asrt_status st ) {
                            auto* o = static_cast< OP* >( ptr );
                            if ( st == ASRT_SUCCESS )
                                    o->receiver.set_value();
                            else
                                    o->receiver.set_error( static_cast< status >( st ) );
                    },
                    &op );
                if ( s != ASRT_SUCCESS )
                        op.receiver.set_error( static_cast< status >( s ) );
        }
};

struct _collect_insert_container_ctx
{
        using completion_signatures = ecor::
            completion_signatures< ecor::set_value_t( flat_id ), ecor::set_error_t( status ) >;

        asrt_collect_client* client;
        flat_id              parent;
        char const*          key;
        asrt_flat_value      value;
        flat_id              out = 0;

        template < typename OP >
        void start( OP& op )
        {
                auto s = asrt_collect_client_insert(
                    client,
                    parent,
                    key,
                    &value,
                    &out,
                    +[]( void* ptr, enum asrt_status st ) {
                            auto* o = static_cast< OP* >( ptr );
                            if ( st == ASRT_SUCCESS )
                                    o->receiver.set_value( o->ctx.out );
                            else
                                    o->receiver.set_error( static_cast< status >( st ) );
                    },
                    &op );
                if ( s != ASRT_SUCCESS )
                        op.receiver.set_error( static_cast< status >( s ) );
        }
};

using collect_scalar_insert_sender    = ecor::sender_from< _collect_insert_scalar_ctx >;
using collect_container_insert_sender = ecor::sender_from< _collect_insert_container_ctx >;

/// Scalar insert with key: set<uint32_t>(parent, "key", 42, cb).
template < collect_scalar T >
status set(
    asrt_collect_client&          cc,
    flat_id                       parent,
    char const*                   key,
    T                             val,
    callback< asrt_send_done_cb > done_cb )
{
        using traits             = collect_append_traits< T >;
        using member_type        = decltype( asrt_flat_scalar{}.*traits::member );
        asrt_flat_value v        = { .type = traits::flat_type };
        v.data.s.*traits::member = static_cast< member_type >( val );
        return asrt_collect_client_insert( &cc, parent, key, &v, NULL, done_cb.fn, done_cb.ptr );
}

/// Scalar insert with key: co_await set<uint32_t>(parent, "key", 42).
template < collect_scalar T >
ecor::sender auto set( asrt_collect_client& cc, flat_id parent, char const* key, T val )
{
        using traits = collect_append_traits< T >;
        return collect_scalar_insert_sender{
            { &cc,
              parent,
              key,
              _make_collect_scalar_value( val, traits::member, traits::flat_type ) } };
}

/// Scalar append without key (array child): append<uint32_t>(parent, 42).
template < collect_scalar T >
status append(
    asrt_collect_client&          cc,
    flat_id                       parent,
    T                             val,
    callback< asrt_send_done_cb > done_cb )
{
        return set< T >( cc, parent, nullptr, val, done_cb );
}

/// Container insert with key: set<obj>(parent, "key", out_id).
/// @param out Receives the auto-assigned node ID for the new container.
template < collect_container T >
status set(
    asrt_collect_client&          cc,
    flat_id                       parent,
    char const*                   key,
    flat_id&                      out,
    callback< asrt_send_done_cb > done_cb )
{
        using traits      = collect_append_traits< T >;
        asrt_flat_value v = { .type = traits::flat_type };
        return asrt_collect_client_insert( &cc, parent, key, &v, &out, done_cb.fn, done_cb.ptr );
}

/// co_await set<T>(client, parent, key) — container with key; returns flat_id.
template < collect_container T >
ecor::sender auto set( asrt_collect_client& client, flat_id parent, char const* key )
{
        using traits = collect_append_traits< T >;
        return collect_container_insert_sender{
            { &client, parent, key, { .type = traits::flat_type } } };
}

/// Container append without key (array child): append<obj>(parent, out_id).
template < collect_container T >
status append(
    asrt_collect_client&          cc,
    flat_id                       parent,
    flat_id&                      out,
    callback< asrt_send_done_cb > done_cb )
{
        return set< T >( cc, parent, nullptr, out, done_cb );
}

/// co_await append(client, parent, val) — scalar without key (array child); returns void.
template < collect_scalar T >
ecor::sender auto append( asrt_collect_client& cc, flat_id parent, T val )
{
        return set( cc, parent, nullptr, val );
}

/// co_await append<T>(client, parent) — container without key (array child); returns flat_id.
template < collect_container T >
ecor::sender auto append( asrt_collect_client& client, flat_id parent )
{
        return set< T >( client, parent, nullptr );
}

inline void deinit( asrt_collect_client& client )
{
        asrt_collect_client_deinit( &client );
}

}  // namespace asrt
