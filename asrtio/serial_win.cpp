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
#include <thread>
#include <vector>
#include <windows.h>

namespace asrtio
{

namespace
{

std::string win_error( std::string const& what )
{
        DWORD err = GetLastError();
        char  msg[256];
        DWORD len = FormatMessageA(
            FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
            nullptr,
            err,
            0,
            msg,
            sizeof msg,
            nullptr );
        while ( len > 0 && ( msg[len - 1] == '\r' || msg[len - 1] == '\n' ) )
                --len;
        return what + ": " + std::string( msg, len ) + " (" + std::to_string( err ) + ")";
}

bool configure_port( HANDLE port, serial_config const& cfg, std::string& errmsg )
{
        DCB dcb{};
        dcb.DCBlength = sizeof dcb;
        if ( !GetCommState( port, &dcb ) ) {
                errmsg = win_error( "GetCommState" );
                return false;
        }
        dcb.BaudRate        = cfg.baud;
        dcb.ByteSize        = 8;
        dcb.fBinary         = TRUE;
        dcb.fDtrControl     = DTR_CONTROL_ENABLE;
        dcb.fOutxDsrFlow    = FALSE;
        dcb.fDsrSensitivity = FALSE;
        dcb.fNull           = FALSE;
        dcb.fErrorChar      = FALSE;
        dcb.fAbortOnError   = FALSE;

        switch ( cfg.parity ) {
        case serial_config::parity_t::none:
                dcb.Parity  = NOPARITY;
                dcb.fParity = FALSE;
                break;
        case serial_config::parity_t::odd:
                dcb.Parity  = ODDPARITY;
                dcb.fParity = TRUE;
                break;
        case serial_config::parity_t::even:
                dcb.Parity  = EVENPARITY;
                dcb.fParity = TRUE;
                break;
        }

        dcb.StopBits = cfg.stop == serial_config::stop_bits_t::two ? TWOSTOPBITS : ONESTOPBIT;

        dcb.fOutxCtsFlow = cfg.flow == serial_config::flow_t::rtscts;
        dcb.fRtsControl =
            cfg.flow == serial_config::flow_t::rtscts ? RTS_CONTROL_HANDSHAKE : RTS_CONTROL_ENABLE;
        dcb.fOutX = cfg.flow == serial_config::flow_t::xonxoff;
        dcb.fInX  = cfg.flow == serial_config::flow_t::xonxoff;

        if ( !SetCommState( port, &dcb ) ) {
                errmsg = win_error( "SetCommState" );
                return false;
        }

        // A read returns as soon as at least one byte is available, or empty after the
        // constant timeout; writes do not time out.
        COMMTIMEOUTS to{};
        to.ReadIntervalTimeout        = MAXDWORD;
        to.ReadTotalTimeoutMultiplier = MAXDWORD;
        to.ReadTotalTimeoutConstant   = 1000;
        if ( !SetCommTimeouts( port, &to ) ) {
                errmsg = win_error( "SetCommTimeouts" );
                return false;
        }

        PurgeComm( port, PURGE_RXCLEAR | PURGE_TXCLEAR );
        return true;
}

// One thread waits on the pending overlapped read and on _wake, which is
// signalled for queued writes and to stop. Writes complete synchronously on
// that thread.
class win_serial_worker final : public serial_worker
{
public:
        bool init( serial_config const& cfg, std::string& errmsg )
        {
                // The \\.\ prefix is required for COM10 and above and accepted for all ports.
                std::string path =
                    cfg.path.rfind( "\\\\.\\", 0 ) == 0 ? cfg.path : "\\\\.\\" + cfg.path;
                _port = CreateFileA(
                    path.c_str(),
                    GENERIC_READ | GENERIC_WRITE,
                    0,
                    nullptr,
                    OPEN_EXISTING,
                    FILE_FLAG_OVERLAPPED,
                    nullptr );
                if ( _port == INVALID_HANDLE_VALUE ) {
                        errmsg = win_error( "CreateFile(\"" + cfg.path + "\")" );
                        return false;
                }
                if ( !configure_port( _port, cfg, errmsg ) )
                        return false;
                _read_ev  = CreateEventA( nullptr, TRUE, FALSE, nullptr );
                _write_ev = CreateEventA( nullptr, TRUE, FALSE, nullptr );
                _wake_ev  = CreateEventA( nullptr, FALSE, FALSE, nullptr );
                if ( !_read_ev || !_write_ev || !_wake_ev ) {
                        errmsg = win_error( "CreateEvent" );
                        return false;
                }
                return true;
        }

        void start( sink& s ) override
        {
                _sink   = &s;
                _thread = std::thread( [this] {
                        run();
                } );
        }

        void write( std::vector< uint8_t > data ) override
        {
                {
                        std::lock_guard lk{ _mtx };
                        _tx.push_back( std::move( data ) );
                }
                SetEvent( _wake_ev );
        }

        ~win_serial_worker() override
        {
                if ( _thread.joinable() ) {
                        {
                                std::lock_guard lk{ _mtx };
                                _stopping = true;
                        }
                        SetEvent( _wake_ev );
                        _thread.join();
                }
                for ( HANDLE h : { _read_ev, _write_ev, _wake_ev } )
                        if ( h )
                                CloseHandle( h );
                if ( _port != INVALID_HANDLE_VALUE )
                        CloseHandle( _port );
        }

private:
        void run()
        {
                uint8_t    buf[1024];
                OVERLAPPED rd{};
                rd.hEvent = _read_ev;
                for ( ;; ) {
                        ResetEvent( _read_ev );
                        if ( !ReadFile( _port, buf, sizeof buf, nullptr, &rd ) &&
                             GetLastError() != ERROR_IO_PENDING )
                                return fail();

                        // Completed or pending, the read signals _read_ev when it finishes.
                        for ( bool pending = true; pending; ) {
                                HANDLE evs[2] = { _read_ev, _wake_ev };
                                DWORD  w      = WaitForMultipleObjects( 2, evs, FALSE, INFINITE );
                                if ( w == WAIT_OBJECT_0 ) {
                                        DWORD n = 0;
                                        if ( !GetOverlappedResult( _port, &rd, &n, FALSE ) )
                                                return fail();
                                        if ( n > 0 )
                                                _sink->on_rx(
                                                    std::span< uint8_t const >{ buf, n } );
                                        pending = false;
                                } else if ( w == WAIT_OBJECT_0 + 1 ) {
                                        if ( !flush_or_stop() ) {
                                                CancelIoEx( _port, &rd );
                                                DWORD n = 0;
                                                GetOverlappedResult( _port, &rd, &n, TRUE );
                                                return;
                                        }
                                } else {
                                        return fail();
                                }
                        }
                }
        }

        // Writes the queued data; returns false when the worker is stopping.
        bool flush_or_stop()
        {
                std::vector< std::vector< uint8_t > > tx;
                {
                        std::lock_guard lk{ _mtx };
                        if ( _stopping )
                                return false;
                        tx.swap( _tx );
                }
                for ( auto& data : tx ) {
                        OVERLAPPED wr{};
                        wr.hEvent = _write_ev;
                        ResetEvent( _write_ev );
                        DWORD n = 0;
                        if ( ( !WriteFile(
                                   _port, data.data(), (DWORD) data.size(), nullptr, &wr ) &&
                               GetLastError() != ERROR_IO_PENDING ) ||
                             !GetOverlappedResult( _port, &wr, &n, TRUE ) )
                                ASRT_ERR_LOG(
                                    "asrtio_serial", "%s", win_error( "WriteFile" ).c_str() );
                }
                return true;
        }

        // Reports a read failure, then waits until the worker is stopped.
        void fail()
        {
                ASRT_ERR_LOG( "asrtio_serial", "%s", win_error( "ReadFile" ).c_str() );
                _sink->on_fail( UV_EIO );
                for ( ;; ) {
                        WaitForSingleObject( _wake_ev, INFINITE );
                        std::lock_guard lk{ _mtx };
                        if ( _stopping )
                                return;
                        _tx.clear();
                }
        }

        HANDLE                                _port     = INVALID_HANDLE_VALUE;
        HANDLE                                _read_ev  = nullptr;
        HANDLE                                _write_ev = nullptr;
        HANDLE                                _wake_ev  = nullptr;
        sink*                                 _sink     = nullptr;
        std::mutex                            _mtx;
        std::vector< std::vector< uint8_t > > _tx;                // guarded by _mtx
        bool                                  _stopping = false;  // guarded by _mtx
        std::thread                           _thread;
};

}  // namespace

std::unique_ptr< serial_worker > open_serial_worker( serial_config const& cfg, std::string& errmsg )
{
        auto worker = std::make_unique< win_serial_worker >();
        if ( !worker->init( cfg, errmsg ) )
                return nullptr;
        return worker;
}

}  // namespace asrtio
