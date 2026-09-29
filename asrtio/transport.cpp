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
#include "./transport.hpp"

#include <mutex>

namespace asrtio
{

struct serial_transport::state : serial_worker::sink
{
        uv_async_t                                    async;
        std::mutex                                    mtx;
        std::vector< uint8_t >                        rx;       // guarded by mtx
        int                                           err = 0;  // guarded by mtx
        std::function< void( std::span< uint8_t > ) > data_cb;
        std::function< void( ssize_t ) >              error_cb;
        std::unique_ptr< serial_worker >              worker;

        void on_rx( std::span< uint8_t const > data ) override
        {
                {
                        std::lock_guard lk{ mtx };
                        rx.insert( rx.end(), data.begin(), data.end() );
                }
                uv_async_send( &async );
        }

        void on_fail( int e ) override
        {
                {
                        std::lock_guard lk{ mtx };
                        err = e;
                }
                uv_async_send( &async );
        }

        // Runs on the loop thread. Bytes stay queued until start_read sets the callbacks.
        void deliver()
        {
                if ( !data_cb )
                        return;
                std::vector< uint8_t > data;
                int                    e = 0;
                {
                        std::lock_guard lk{ mtx };
                        data.swap( rx );
                        std::swap( e, err );
                }
                if ( !data.empty() )
                        data_cb( data );
                if ( e != 0 )
                        error_cb( e );
        }
};

std::optional< serial_transport > serial_transport::open(
    uv_loop_t*           loop,
    serial_config const& cfg,
    std::string&         errmsg )
{
        auto worker = open_serial_worker( cfg, errmsg );
        if ( !worker )
                return std::nullopt;

        auto s = std::make_shared< state >();
        uv_async_init( loop, &s->async, []( uv_async_t* a ) {
                static_cast< state* >( a->data )->deliver();
        } );
        s->async.data = s.get();
        s->worker     = std::move( worker );
        s->worker->start( *s );
        return serial_transport{ std::move( s ) };
}

uv_loop_t* serial_transport::loop() const
{
        return _s->async.loop;
}

void serial_transport::start_read(
    char const* /*module*/,
    std::function< void( std::span< uint8_t > ) > on_data,
    std::function< void( ssize_t ) >              on_error )
{
        _s->data_cb  = std::move( on_data );
        _s->error_cb = std::move( on_error );
        uv_async_send( &_s->async );
}

asrt::status serial_transport::write( std::span< uint8_t const > data )
{
        if ( !_s->worker )
                return ASRT_SEND_ERR;
        _s->worker->write( std::vector< uint8_t >( data.begin(), data.end() ) );
        return ASRT_SUCCESS;
}

uv_handle_t* serial_transport::stop()
{
        // Joining the worker first ensures nothing signals the async handle after it closes.
        _s->worker.reset();
        return reinterpret_cast< uv_handle_t* >( &_s->async );
}

void serial_transport::close()
{
        uv_close( stop(), nullptr );
}

}  // namespace asrtio
