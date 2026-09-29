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

#include "./util.hpp"

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <uv.h>

namespace asrtio
{

// ---------------------------------------------------------------------------
// serial_config
// ---------------------------------------------------------------------------

struct serial_config
{
        std::string path;

        uint32_t baud = 115200;

        enum class parity_t : uint8_t
        {
                none,
                odd,
                even
        } parity = parity_t::none;

        enum class stop_bits_t : uint8_t
        {
                one,
                two
        } stop = stop_bits_t::one;

        enum class flow_t : uint8_t
        {
                none,
                rtscts,
                xonxoff
        } flow = flow_t::none;
};

// ---------------------------------------------------------------------------
// open_serial_port
//
// Opens the device at cfg.path and applies the requested line settings via
// POSIX termios (Linux / macOS).  The fd is set O_NONBLOCK so libuv can
// manage it with uv_pipe_open.
//
// Returns a valid fd (>= 0) on success.
// Returns -1 on failure; errno is set and errmsg is populated with a
// human-readable description.
// ---------------------------------------------------------------------------

#ifndef _WIN32
int open_serial_port( serial_config const& cfg, std::string& errmsg );
#endif

// ---------------------------------------------------------------------------
// serial_worker
//
// Owns an open serial port and the thread that reads from it and writes to
// it. One implementation per platform: serial_posix.cpp (libuv) and
// serial_win.cpp (Win32).
// ---------------------------------------------------------------------------

struct serial_worker
{
        /// Receives what the worker reads; called on the worker thread.
        struct sink
        {
                virtual void on_rx( std::span< uint8_t const > data ) = 0;
                /// @p err is a negative libuv error code; UV_EOF when the port closes.
                virtual void on_fail( int err ) = 0;
                virtual ~sink()                 = default;
        };

        /// Start the thread; @p s must outlive the worker.
        virtual void start( sink& s ) = 0;

        /// Queue @p data for sending; callable from any thread.
        virtual void write( std::vector< uint8_t > data ) = 0;

        /// Stops the thread and closes the port.
        virtual ~serial_worker() = default;
};

/// Open and configure the port. Returns nullptr on failure, with errmsg populated.
std::unique_ptr< serial_worker > open_serial_worker(
    serial_config const& cfg,
    std::string&         errmsg );

// ---------------------------------------------------------------------------
// uv_stream_transport
//
// Byte transport over a libuv stream handle (uv_tcp_t, uv_pipe_t), as used by
// cntr_stream_sys: start_read, write, close, and stop() for the final
// awaited close.
// ---------------------------------------------------------------------------

template < typename H >
struct uv_stream_transport
{
        explicit uv_stream_transport( std::shared_ptr< H > handle )
          : h( std::move( handle ) )
        {
        }

        std::shared_ptr< H >             h;
        std::unique_ptr< stream_reader > reader;

        uv_loop_t* loop() const { return h->loop; }

        void start_read(
            char const*                                   module,
            std::function< void( std::span< uint8_t > ) > on_data,
            std::function< void( ssize_t ) >              on_error )
        {
                reader = std::make_unique< stream_reader >(
                    stream_reader{ std::move( on_data ), std::move( on_error ), module } );
                start_stream_read( stream(), *reader );
        }

        asrt::status write( std::span< uint8_t const > data )
        {
                return write_stream( stream(), data );
        }

        /// Stop activity and return the handle whose close completes shutdown.
        uv_handle_t* stop() { return reinterpret_cast< uv_handle_t* >( h.get() ); }

        void close() { uv_close( stop(), nullptr ); }

private:
        uv_stream_t* stream() const { return reinterpret_cast< uv_stream_t* >( h.get() ); }
};

// ---------------------------------------------------------------------------
// tcp_transport
//
// Wraps a connected uv_tcp_t. The caller is responsible for constructing
// the shared_ptr and completing the connection (e.g. via tcp_connect sender)
// before constructing this transport.
// ---------------------------------------------------------------------------

using tcp_transport = uv_stream_transport< uv_tcp_t >;

// ---------------------------------------------------------------------------
// serial_transport
//
// Serial I/O runs on a serial_worker thread. Received bytes are queued and
// handed to the loop thread through a uv_async_t; writes are queued to the
// worker. Use the static open() factory to construct.
// ---------------------------------------------------------------------------

class serial_transport
{
public:
        // Factory function — no exceptions.  Returns nullopt on failure;
        // errmsg is populated with a human-readable description.
        static std::optional< serial_transport > open(
            uv_loop_t*           loop,
            serial_config const& cfg,
            std::string&         errmsg );

        uv_loop_t* loop() const;

        void start_read(
            char const*                                   module,
            std::function< void( std::span< uint8_t > ) > on_data,
            std::function< void( ssize_t ) >              on_error );

        asrt::status write( std::span< uint8_t const > data );

        /// Stop the worker thread and return the handle whose close completes shutdown.
        uv_handle_t* stop();

        void close();

private:
        struct state;

        explicit serial_transport( std::shared_ptr< state > s )
          : _s( std::move( s ) )
        {
        }

        std::shared_ptr< state > _s;
};

}  // namespace asrtio
