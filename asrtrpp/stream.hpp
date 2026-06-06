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
#include "../asrtl/util.h"
#include "../asrtlpp/callback.hpp"
#include "../asrtlpp/task.hpp"
#include "../asrtlpp/util.hpp"
#include "../asrtr/stream.h"
#include "./task_unit.hpp"

namespace asrt
{

template < typename T >
struct strm_field_traits;

template <>
struct strm_field_traits< uint8_t >
{
        using value_type                            = uint8_t;
        static constexpr auto        tag            = ASRT_STRM_FIELD_U8;
        static constexpr auto        size           = 1;
        static constexpr std::size_t field_def_size = 1;
        static constexpr void encode_def( uint8_t*& p ) { *p++ = static_cast< uint8_t >( tag ); }
        static void           encode( uint8_t*& p, uint8_t v ) { *p++ = v; }
};

template <>
struct strm_field_traits< uint16_t >
{
        using value_type                            = uint16_t;
        static constexpr auto        tag            = ASRT_STRM_FIELD_U16;
        static constexpr auto        size           = 2;
        static constexpr std::size_t field_def_size = 1;
        static constexpr void encode_def( uint8_t*& p ) { *p++ = static_cast< uint8_t >( tag ); }
        static void           encode( uint8_t*& p, uint16_t v ) { asrt_add_u16( &p, v ); }
};

template <>
struct strm_field_traits< uint32_t >
{
        using value_type                            = uint32_t;
        static constexpr auto        tag            = ASRT_STRM_FIELD_U32;
        static constexpr auto        size           = 4;
        static constexpr std::size_t field_def_size = 1;
        static constexpr void encode_def( uint8_t*& p ) { *p++ = static_cast< uint8_t >( tag ); }
        static void           encode( uint8_t*& p, uint32_t v ) { asrt_add_u32( &p, v ); }
};

template <>
struct strm_field_traits< int8_t >
{
        using value_type                            = int8_t;
        static constexpr auto        tag            = ASRT_STRM_FIELD_I8;
        static constexpr auto        size           = 1;
        static constexpr std::size_t field_def_size = 1;
        static constexpr void encode_def( uint8_t*& p ) { *p++ = static_cast< uint8_t >( tag ); }
        static void encode( uint8_t*& p, int8_t v ) { *p++ = static_cast< uint8_t >( v ); }
};

template <>
struct strm_field_traits< int16_t >
{
        using value_type                            = int16_t;
        static constexpr auto        tag            = ASRT_STRM_FIELD_I16;
        static constexpr auto        size           = 2;
        static constexpr std::size_t field_def_size = 1;
        static constexpr void encode_def( uint8_t*& p ) { *p++ = static_cast< uint8_t >( tag ); }
        static void           encode( uint8_t*& p, int16_t v )
        {
                asrt_add_u16( &p, static_cast< uint16_t >( v ) );
        }
};

template <>
struct strm_field_traits< int32_t >
{
        using value_type                            = int32_t;
        static constexpr auto        tag            = ASRT_STRM_FIELD_I32;
        static constexpr auto        size           = 4;
        static constexpr std::size_t field_def_size = 1;
        static constexpr void encode_def( uint8_t*& p ) { *p++ = static_cast< uint8_t >( tag ); }
        static void           encode( uint8_t*& p, int32_t v ) { asrt_add_i32( &p, v ); }
};

template <>
struct strm_field_traits< float >
{
        using value_type                            = float;
        static constexpr auto        tag            = ASRT_STRM_FIELD_FLOAT;
        static constexpr auto        size           = 4;
        static constexpr std::size_t field_def_size = 1;
        static constexpr void encode_def( uint8_t*& p ) { *p++ = static_cast< uint8_t >( tag ); }
        static void           encode( uint8_t*& p, float v )
        {
                uint32_t u;
                std::memcpy( &u, &v, 4 );
                asrt_add_u32( &p, u );
        }
};

template <>
struct strm_field_traits< bool >
{
        using value_type                            = bool;
        static constexpr auto        tag            = ASRT_STRM_FIELD_BOOL;
        static constexpr auto        size           = 1;
        static constexpr std::size_t field_def_size = 1;
        static constexpr void encode_def( uint8_t*& p ) { *p++ = static_cast< uint8_t >( tag ); }
        static void           encode( uint8_t*& p, bool v ) { *p++ = v ? 1 : 0; }
};

template <>
struct strm_field_traits< char >
{
        using value_type                            = char;
        static constexpr auto        tag            = ASRT_STRM_FIELD_U8;
        static constexpr auto        size           = 1;
        static constexpr std::size_t field_def_size = 1;
        static constexpr void encode_def( uint8_t*& p ) { *p++ = static_cast< uint8_t >( tag ); }
        static void           encode( uint8_t*& p, char v ) { *p++ = static_cast< uint8_t >( v ); }
};

template < typename T, std::size_t N >
struct strm_field_traits< T[N] >
{
        using value_type = typename strm_field_traits< T >::value_type[N];
        static constexpr std::size_t field_def_size = 3 + strm_field_traits< T >::field_def_size;
        static constexpr std::size_t size           = N * strm_field_traits< T >::size;
        static constexpr void        encode_def( uint8_t*& p )
        {
                *p++ = static_cast< uint8_t >( ASRT_STRM_FIELD_ARRAY );
                *p++ = static_cast< uint8_t >( N >> 8 );
                *p++ = static_cast< uint8_t >( N & 0xFF );
                strm_field_traits< T >::encode_def( p );
        }
        static void encode( uint8_t*& p, value_type const& arr )
        {
                for ( auto const& v : arr )
                        strm_field_traits< T >::encode( p, v );
        }
};

/// Initialise a stream client and link it into the channel chain after @p prev.
ASRT_NODISCARD inline status init( asrt_stream_client& client, asrt_node& prev )
{
        return asrt_stream_client_init( &client, &prev );
}

/// Send a DEFINE message registering @p schema_id with pre-encoded field bytes.
/// @p done_cb fires once the send completes.
ASRT_NODISCARD inline status define(
    asrt_stream_client&             client,
    uint8_t                         schema_id,
    uint8_t const*                  field_bytes,
    uint16_t                        field_bytes_size,
    uint8_t                         field_count,
    callback< asrt_stream_done_cb > done_cb )
{
        return asrt_stream_client_define(
            &client,
            schema_id,
            field_bytes,
            field_bytes_size,
            field_count,
            done_cb.fn,
            done_cb.ptr );
}
/// Send one DATA record for @p schema_id.  @p data must be exactly @p data_size bytes.
/// @p done_cb fires once the send completes.
ASRT_NODISCARD inline status emit(
    asrt_stream_client&             client,
    uint8_t                         schema_id,
    uint8_t const*                  data,
    uint16_t                        data_size,
    callback< asrt_stream_done_cb > done_cb )
{
        return asrt_stream_client_emit(
            &client, schema_id, data, data_size, done_cb.fn, done_cb.ptr );
}

/// Clear all pending state on the client (e.g. at a test boundary).
ASRT_NODISCARD inline status reset( asrt_stream_client& client )
{
        return asrt_stream_client_reset( &client );
}

/// Unlink and release the stream client.
inline void deinit( asrt_stream_client& client )
{
        asrt_stream_client_deinit( &client );
}

template < std::size_t N >
struct _field_def_bytes_t
{
        uint8_t                  data[N > 0 ? N : 1]{};
        constexpr std::size_t    size() const noexcept { return N; }
        constexpr uint8_t const& operator[]( std::size_t i ) const noexcept { return data[i]; }
};

/// Compile-time typed stream schema.
///
/// Constructed by stream_define_sender after a successful DEFINE.  Provides
/// emit(args..., done_cb, done_ptr) which encodes field values into a
/// fixed-size buffer and sends a DATA message.
template < typename... Ts >
struct stream_schema
{
        static constexpr uint16_t emit_size = ( strm_field_traits< Ts >::size + ... );

        /// XXX: error handling in constructor is bad
        stream_schema(
            asrt_stream_client&             client,
            uint8_t                         schema_id,
            callback< asrt_stream_done_cb > done_cb )
          : _client( &client )
          , _schema_id( schema_id )
        {
                auto s = define(
                    *_client,
                    _schema_id,
                    field_def_bytes.data,
                    static_cast< uint16_t >( field_def_bytes_size ),
                    static_cast< uint8_t >( sizeof...( Ts ) ),
                    done_cb );
                if ( s != ASRT_SUCCESS ) {
                        ASRT_ERR_LOG( "asrt_stream_schema", "define failed" );
                        ASRT_ASSERT( false );
                }
        }

        stream_schema( asrt_stream_client& c, uint8_t id )
          : _client( &c )
          , _schema_id( id )
        {
        }


        stream_schema( stream_schema const& )            = delete;
        stream_schema& operator=( stream_schema const& ) = delete;

        stream_schema( stream_schema&& o ) noexcept
          : _client( o._client )
          , _schema_id( o._schema_id )
        {
                o._client = nullptr;
        }

        stream_schema& operator=( stream_schema&& o ) noexcept
        {
                _client    = o._client;
                _schema_id = o._schema_id;
                o._client  = nullptr;
                return *this;
        }

        /// Encode @p args in-place and send a DATA message using the underlying emit function.
        /// Encode @p args in-place and send a DATA message using the underlying emit function.
        ASRT_NODISCARD status emit(
            typename strm_field_traits< Ts >::value_type const&... args,
            callback< asrt_stream_done_cb > done_cb )
        {
                uint8_t* p = _emit_buf;
                ( strm_field_traits< Ts >::encode( p, args ), ... );
                return asrt::emit( *_client, _schema_id, _emit_buf, emit_size, done_cb );
        }

        /// Emit from a pre-encoded buffer; used internally by stream_emit_sender.
        ASRT_NODISCARD status
        emit_raw( uint8_t const* buf, callback< asrt_stream_done_cb > done_cb )
        {
                return asrt::emit( *_client, _schema_id, buf, emit_size, done_cb );
        }

        ~stream_schema() = default;

        static constexpr std::size_t field_def_bytes_size =
            ( strm_field_traits< Ts >::field_def_size + ... );
        static constexpr auto field_def_bytes = []() constexpr {
                _field_def_bytes_t< field_def_bytes_size > arr{};
                uint8_t*                                   p = arr.data;
                ( strm_field_traits< Ts >::encode_def( p ), ... );
                return arr;
        }();

private:
        asrt_stream_client* _client;
        uint8_t             _schema_id;
        uint8_t             _emit_buf[emit_size > 0 ? emit_size : 1];
};

/// Context for stream_define_sender: registers a schema and returns it on completion.
template < typename... Ts >
struct _stream_define_ctx
{
        using completion_signatures = ecor::completion_signatures<
            ecor::set_value_t( stream_schema< Ts... > ),
            ecor::set_error_t( status ) >;

        asrt_stream_client* client;
        uint8_t             schema_id;

        template < typename OP >
        void start( OP& op )
        {
                auto cb = +[]( void* ptr, enum asrt_status s ) {
                        auto& o = *static_cast< OP* >( ptr );
                        if ( s == ASRT_SUCCESS )
                                o.receiver.set_value(
                                    stream_schema< Ts... >{ *o.ctx.client, o.ctx.schema_id } );
                        else
                                o.receiver.set_error( s );
                };
                auto s = define(
                    *client,
                    schema_id,
                    stream_schema< Ts... >::field_def_bytes.data,
                    static_cast< uint16_t >( stream_schema< Ts... >::field_def_bytes_size ),
                    static_cast< uint8_t >( sizeof...( Ts ) ),
                    { cb, &op } );
                if ( s != ASRT_SUCCESS )
                        op.receiver.set_error( s );
        }
};

/// Sender for co_await define<Ts...>(client, schema_id).
/// Registers a schema and completes with a stream_schema<Ts...> once the
/// DEFINE message is acknowledged via done_cb.
template < typename... Ts >
using stream_define_sender = ecor::sender_from< _stream_define_ctx< Ts... > >;

/// Define a stream schema.  Returns a sender that completes with a
/// stream_schema<Ts...> once the DEFINE message has been acknowledged.
template < typename... Ts >
ecor::sender auto define( asrt_stream_client& client, uint8_t schema_id )
{
        return stream_define_sender< Ts... >{ { &client, schema_id } };
}


/// Context for stream_emit_sender: encodes args into buf and sends one DATA message.
template < typename... Ts >
struct _stream_emit_ctx
{
        using completion_signatures =
            ecor::completion_signatures< ecor::set_value_t(), ecor::set_error_t( status ) >;

        stream_schema< Ts... >* schema;
        uint8_t                 buf[stream_schema< Ts... >::emit_size];

        template < typename OP >
        void start( OP& op )
        {
                auto cb = +[]( void* ptr, enum asrt_status s ) {
                        auto& o = *static_cast< OP* >( ptr );
                        if ( s == ASRT_SUCCESS )
                                o.receiver.set_value();
                        else
                                o.receiver.set_error( s );
                };
                auto st = schema->emit_raw( buf, { cb, &op } );
                if ( st != ASRT_SUCCESS )
                        op.receiver.set_error( st );
        }
};

/// Sender for co_await emit(schema, args...).
/// Encodes @p args into a fixed-size buffer and sends one DATA message,
/// completing once done_cb fires.
template < typename... Ts >
using stream_emit_sender = ecor::sender_from< _stream_emit_ctx< Ts... > >;

/// Emit one record.  Returns a sender that completes once the DATA message
/// has been sent.
template < typename... Ts >
ecor::sender auto emit(
    stream_schema< Ts... >& schema,
    typename strm_field_traits< Ts >::value_type const&... args )
{
        _stream_emit_ctx< Ts... > ctx{ &schema, {} };
        uint8_t*                  p = ctx.buf;
        ( strm_field_traits< Ts >::encode( p, args ), ... );
        return stream_emit_sender< Ts... >{ std::move( ctx ) };
}


}  // namespace asrt
