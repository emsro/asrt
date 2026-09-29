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

#include "../asrtc/test_result_to_str.h"
#include "../asrtcpp/controller.hpp"
#include "../asrtl/flat_tree.h"
#include "../asrtl/log.h"
#include "../asrtl/status_to_str.h"
#include "../asrtl/util.h"
#include "../asrtlpp/flat_type_traits.hpp"
#include "../asrtlpp/util.hpp"

#include <cstring>
#include <deque>
#include <functional>
#include <limits>
#include <nlohmann/json.hpp>
#include <span>
#include <uv.h>
#include <vector>

namespace asrtio
{

struct clock
{
        virtual std::chrono::milliseconds now() const = 0;
};

struct steady_clock : clock
{
        std::chrono::milliseconds now() const override
        {
                return std::chrono::duration_cast< std::chrono::milliseconds >(
                    std::chrono::steady_clock::now().time_since_epoch() );
        }
};

/// Callbacks for the bytes read from a libuv stream.
struct stream_reader
{
        std::function< void( std::span< uint8_t > ) > on_data;
        std::function< void( ssize_t ) >              on_error;
        char const*                                   module = "asrtio";
};

/// Start reading @p client into @p reader, which must stay valid while reading.
void start_stream_read( uv_stream_t* client, stream_reader& reader );

/// Write a copy of @p data to @p client.
asrt::status write_stream( uv_stream_t* client, std::span< uint8_t const > data );

struct cobs_node
{
        asrt_node*                       node;
        asrt_cobs_ibuffer                recv;
        uint8_t                          ibuffer[4096];
        char const*                      module = "asrtio";
        std::function< void( ssize_t ) > on_error;
        stream_reader                    reader;

        void init( asrt_node* node, char const* mod, std::function< void( ssize_t ) > on_error )
        {
                asrt_cobs_ibuffer_init(
                    &recv, ( struct asrt_span ){ .b = ibuffer, .e = ibuffer + sizeof ibuffer } );
                this->node     = node;
                this->module   = mod;
                this->on_error = std::move( on_error );
        }

        /// COBS-encode the channel header followed by @p buff into @p frame.
        asrt::status encode(
            asrt::chann_id          id,
            asrt_rec_span const&    buff,
            std::vector< uint8_t >& frame ) const
        {
                uint8_t  hdr_buf[2];
                uint8_t* pp = hdr_buf;
                asrt_add_u16( &pp, id );

                // Prepend the channel header as a leading node in the span chain.
                struct asrt_rec_span hdr_span = {
                    .b    = hdr_buf,
                    .e    = hdr_buf + 2,
                    .next = const_cast< struct asrt_rec_span* >( &buff ) };

                uint8_t buffer[1024];
                struct asrt_span sp
                {
                        .b = buffer, .e = buffer + sizeof buffer
                };
                auto s = asrt_cobs_encode_buffer( &hdr_span, &sp );
                if ( s != ASRT_SUCCESS ) {
                        ASRT_ERR_LOG( module, "COBS encoding failed: %s", asrt_status_to_str( s ) );
                        return ASRT_SEND_ERR;
                }
                ASRT_DBG_LOG(
                    module,
                    "Sending to channel %u: %zu bytes encoded",
                    id,
                    (size_t) ( sp.e - sp.b ) );
                frame.assign( sp.b, sp.e );
                return ASRT_SUCCESS;
        }

        asrt::status write( uv_stream_t* client, asrt::chann_id id, asrt_rec_span const& buff )
            const
        {
                std::vector< uint8_t > frame;
                if ( auto s = encode( id, buff, frame ); s != ASRT_SUCCESS )
                        return s;
                return write_stream( client, frame );
        }

        void on_data( std::span< uint8_t > data )
        {
                struct asrt_span sp
                {
                        .b = data.data(), .e = data.data() + data.size()
                };
                auto s = asrt_chann_cobs_dispatch( &recv, node, sp );
                if ( s != ASRT_SUCCESS ) {
                        ASRT_ERR_LOG( module, "COBS dispatch failed: %s", asrt_status_to_str( s ) );
                        on_error( UV_UNKNOWN );
                }
        }

        void start(
            uv_stream_t*                     client,
            asrt_node*                       node,
            char const*                      mod,
            std::function< void( ssize_t ) > on_error )
        {
                init( node, mod, std::move( on_error ) );
                reader = stream_reader{
                    .on_data =
                        [this]( std::span< uint8_t > data ) {
                                on_data( data );
                        },
                    .on_error =
                        [this]( ssize_t nread ) {
                                this->on_error( nread );
                        },
                    .module = module };
                start_stream_read( client, reader );
        }
};


bool flat_tree_from_json( asrt_flat_tree& tree, nlohmann::json const& j, asrt::flat_id& next_id );

bool flat_tree_to_json( asrt_flat_tree& tree, nlohmann::json& out );

bool flat_tree_to_json( asrt_flat_tree& tree, asrt::flat_id node_id, nlohmann::json& out );

}  // namespace asrtio
