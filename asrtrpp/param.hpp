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
#include "../asrtlpp/flat_type_traits.hpp"
#include "../asrtlpp/util.hpp"
#include "../asrtr/param.h"
#include "./task_unit.hpp"

namespace asrt
{

using status = asrt_status;

/// Trait mapping a C++ type T to its corresponding C callback type and
/// asrt_param_cb union member pointer.  Specialised for all param value types.
template < typename T >
struct param_query_traits;

template <>
struct param_query_traits< uint32_t > : flat_type_traits< uint32_t >
{
        using cb_type                   = asrt_param_u32_cb;
        static constexpr auto cb_member = &asrt_param_cb::u32;
};

template <>
struct param_query_traits< int32_t > : flat_type_traits< int32_t >
{
        using cb_type                   = asrt_param_i32_cb;
        static constexpr auto cb_member = &asrt_param_cb::i32;
};

template <>
struct param_query_traits< float > : flat_type_traits< float >
{
        using cb_type                   = asrt_param_float_cb;
        static constexpr auto cb_member = &asrt_param_cb::flt;
};

template <>
struct param_query_traits< char const* > : flat_type_traits< char const* >
{
        using cb_type                   = asrt_param_str_cb;
        static constexpr auto cb_member = &asrt_param_cb::str;
};

template <>
struct param_query_traits< bool > : flat_type_traits< bool >
{
        using cb_type                   = asrt_param_bool_cb;
        static constexpr auto cb_member = &asrt_param_cb::bln;
};

template <>
struct param_query_traits< obj > : flat_type_traits< obj >
{
        using cb_type                   = asrt_param_obj_cb;
        static constexpr auto cb_member = &asrt_param_cb::obj;
};

template <>
struct param_query_traits< arr > : flat_type_traits< arr >
{
        using cb_type                   = asrt_param_arr_cb;
        static constexpr auto cb_member = &asrt_param_cb::arr;
};

template <>
struct param_query_traits< asrt_flat_value >
{
        using raw_type                  = asrt_flat_value;
        using value_type                = asrt_flat_value;
        using cb_type                   = asrt_param_any_cb;
        static constexpr auto flat_type = ASRT_FLAT_STYPE_NONE;
        static constexpr auto cb_member = &asrt_param_cb::any;
};

// Convenience aliases
template <>
struct param_query_traits< void > : param_query_traits< asrt_flat_value >
{
};

template < typename T >
concept has_param_query_traits = requires { typename param_query_traits< T >; };

/// Concept: CB is a typed callback for T (callable as cb(client*, query*, value_type)).
template < typename CB, typename T >
concept typed_param_query_callable =
    has_param_query_traits< T > && requires(
                                       CB                                  cb,
                                       asrt_param_client*                  c,
                                       asrt_param_query*                   q,
                                       param_query_traits< T >::value_type v ) {
            { cb( c, q, v ) } -> std::same_as< void >;
    };

/// Initialise the param client, link it after @p prev, and set the response cache buffer.
/// @p timeout is how long the client waits for a READY from the controller.
ASRT_NODISCARD inline status init(
    asrt_param_client& client,
    asrt_node&         prev,
    asrt_span          msg_buffer,
    uint32_t           timeout )
{
        return asrt_param_client_init( &client, &prev, msg_buffer, timeout );
}

/// Returns true once the controller's READY has been received.
ASRT_NODISCARD inline bool ready( asrt_param_client const& client )
{
        return client.ready != 0;
}

/// Returns true if a query is currently in flight.
ASRT_NODISCARD inline bool query_pending( asrt_param_client const& client )
{
        return asrt_param_query_pending( &client ) != 0;
}

/// Return the root node ID received in the last READY message.
ASRT_NODISCARD inline flat_id root_id( asrt_param_client const& client )
{
        return asrt_param_client_root_id( &client );
}
/// Submit a typed QUERY or FIND_BY_KEY.  T selects the expected value type; if key is non-null,
/// a FIND_BY_KEY is sent; otherwise a QUERY by node_id.  Callable callback overload.
template < has_param_query_traits T, typed_param_query_callable< T > CB >
ASRT_NODISCARD asrt_status
query( asrt_param_client& cl, asrt_param_query* q, flat_id node_id, char const* key, CB& cb )
{
        using traits     = param_query_traits< T >;
        q->expected_type = traits::flat_type;
        q->cb.*traits::cb_member =
            []( asrt_param_client* c, asrt_param_query* qq, traits::raw_type raw ) {
                    ( *reinterpret_cast< CB* >( qq->cb_ptr ) )(
                        c, qq, static_cast< traits::value_type >( raw ) );
            };
        q->cb_ptr = &cb;
        return asrt_param_client_query( q, &cl, node_id, key );
}
/// Submit a typed QUERY or FIND_BY_KEY.  Raw function pointer + void* context overload.
template < has_param_query_traits T >
ASRT_NODISCARD asrt_status query(
    asrt_param_client&                        cl,
    asrt_param_query*                         q,
    flat_id                                   node_id,
    char const*                               key,
    typename param_query_traits< T >::cb_type cb,
    void*                                     cb_ptr )
{
        using traits             = param_query_traits< T >;
        q->expected_type         = traits::flat_type;
        q->cb.*traits::cb_member = cb;
        q->cb_ptr                = cb_ptr;
        return asrt_param_client_query( q, &cl, node_id, key );
}

/// Submit a QUERY without type filtering.  Raw C callback + void* overload.
ASRT_NODISCARD inline asrt_status query(
    asrt_param_client& cl,
    asrt_param_query*  q,
    flat_id            node_id,
    char const*        key,
    asrt_param_any_cb  cb,
    void*              cb_ptr )
{
        q->expected_type = ASRT_FLAT_STYPE_NONE;
        q->cb.any        = cb;
        q->cb_ptr        = cb_ptr;
        return asrt_param_client_query( q, &cl, node_id, key );
}

/// Fetch information about node node_id from param client, call cb with the value if
/// successful. cb will be called even on error, with an appropriate error code in
/// q->error_code and possibly an invalid value. T is the expected type of the value, if
/// value type doesn't match, cb will be called with an error.
template < has_param_query_traits T, typed_param_query_callable< T > CB >
ASRT_NODISCARD asrt_status
fetch( asrt_param_client& cl, asrt_param_query* q, flat_id node_id, CB& cb )
{
        return query< T >( cl, q, node_id, nullptr, cb );
}

/// Fetch information about node node_id from param client, call cb with the value if
/// successful. This overload expects raw function pointer and void* context.
template < has_param_query_traits T >
ASRT_NODISCARD asrt_status fetch(
    asrt_param_client&                        cl,
    asrt_param_query*                         q,
    flat_id                                   node_id,
    typename param_query_traits< T >::cb_type cb,
    void*                                     cb_ptr )
{
        return query< T >( cl, q, node_id, nullptr, cb, cb_ptr );
}

/// Raw fetch without explicit expected type and C callback + void* context.
ASRT_NODISCARD inline asrt_status fetch(
    asrt_param_client& cl,
    asrt_param_query*  q,
    flat_id            node_id,
    asrt_param_any_cb  cb,
    void*              cb_ptr )
{
        return query( cl, q, node_id, nullptr, cb, cb_ptr );
}

/// Find child with key under parent_id, call cb with the value if successful.
template < has_param_query_traits T, typed_param_query_callable< T > CB >
ASRT_NODISCARD asrt_status
find( asrt_param_client& cl, asrt_param_query* q, flat_id parent_id, char const* key, CB& cb )
{
        return query< T >( cl, q, parent_id, key, cb );
}

/// Find child with key under parent_id. Raw function pointer + void* context overload.
template < has_param_query_traits T >
ASRT_NODISCARD asrt_status find(
    asrt_param_client&                        cl,
    asrt_param_query*                         q,
    flat_id                                   parent_id,
    char const*                               key,
    typename param_query_traits< T >::cb_type cb,
    void*                                     cb_ptr )
{
        return query< T >( cl, q, parent_id, key, cb, cb_ptr );
}

/// Raw find without explicit expected type and C callback + void* context.
ASRT_NODISCARD inline asrt_status find(
    asrt_param_client& cl,
    asrt_param_query*  q,
    flat_id            parent_id,
    char const*        key,
    asrt_param_any_cb  cb,
    void*              cb_ptr )
{
        return query( cl, q, parent_id, key, cb, cb_ptr );
}

/// Unlink and release the param client.
inline void deinit( asrt_param_client& client )
{
        asrt_param_client_deinit( &client );
}

/// Result of a completed param query.
/// @p key points into the param client's internal cache — valid until the next query.
/// @p node_id is the ID of the node that was queried or found — use this as
/// the parent_id for subsequent find/fetch calls into this node's children.
template < has_param_query_traits T >
struct param_result
{
        using value_type = param_query_traits< T >::value_type;

        value_type  value;
        char const* key;  // Points to internal buffer of param client, valid until next query
        flat_id     next_sibling;
        flat_id     node_id;  // ID of the node that was queried/found

        operator value_type() const { return value; }
};


/// Context for param_query_sender: submits a typed QUERY or FIND_BY_KEY request.
/// @p q is mutable op state filled by the C layer; it lives in the sender_from op.
template < has_param_query_traits T >
struct _param_query_ctx
{
        using completion_signatures = ecor::completion_signatures<
            ecor::set_value_t( param_result< T > ),
            ecor::set_error_t( status ) >;

        asrt_param_client* client;
        char const*        key;
        flat_id            node_id;
        asrt_param_query   q = {};

        template < typename OP >
        void start( OP& op )
        {
                using traits = param_query_traits< T >;
                auto cb = +[]( asrt_param_client*, asrt_param_query* q, traits::raw_type raw ) {
                        auto* o = static_cast< OP* >( q->cb_ptr );
                        if ( q->error_code != ASRT_PARAM_ERR_NONE )
                                o->receiver.set_error( ASRT_RECV_ERR );
                        else
                                o->receiver.set_value( param_result< T >{
                                    static_cast< traits::value_type >( raw ),
                                    q->key,
                                    q->next_sibling,
                                    q->node_id } );
                };
                auto s = query< T >( *client, &q, node_id, key, cb, &op );
                if ( s != ASRT_SUCCESS )
                        op.receiver.set_error( ASRT_RECV_ERR );
        }
};

/// Sender backing co_await fetch<T> and co_await find<T>.
/// Submits a QUERY or FIND_BY_KEY request and completes with param_result<T>.
template < has_param_query_traits T >
using param_query_sender = ecor::sender_from< _param_query_ctx< T > >;

/// co_await fetch<T>(client, node_id) — query a node by ID; completes with param_result<T>.
template < typename T >
ecor::sender auto fetch( asrt_param_client& client, flat_id node_id )
{
        return param_query_sender< T >{ { &client, nullptr, node_id } };
}

/// co_await find<T>(client, parent_id, key) — find a child by key; completes with param_result<T>.
template < typename T >
ecor::sender auto find( asrt_param_client& client, flat_id parent_id, char const* key )
{
        return param_query_sender< T >{ { &client, key, parent_id } };
}

/// co_await fetch(client, node_id) — typeless query; completes with param_result<void>
/// (asrt_flat_value with .type and .data populated).
inline ecor::sender auto fetch( asrt_param_client& client, flat_id node_id )
{
        return param_query_sender< void >{ { &client, nullptr, node_id } };
}

/// co_await find(client, parent_id, key) — typeless find by key; completes with param_result<void>
/// (asrt_flat_value with .type and .data populated).
inline ecor::sender auto find( asrt_param_client& client, flat_id parent_id, char const* key )
{
        return param_query_sender< void >{ { &client, key, parent_id } };
}

}  // namespace asrt
