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

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <termios.h>
#include <unistd.h>

#if defined( __APPLE__ )
#include <AvailabilityMacros.h>
#include <IOKit/serial/ioss.h>
#include <sys/ioctl.h>
#endif

#if defined( __linux__ )
#include <linux/serial.h>
#include <sys/ioctl.h>
#endif

namespace asrtio
{

namespace
{

// Returns the Bxxx constant for the given numeric baud rate, or -1 if the
// rate is not in the standard table (needs platform-specific ioctl).
int to_baud_constant( uint32_t baud )
{
        switch ( baud ) {
        case 0:
                return B0;
        case 50:
                return B50;
        case 75:
                return B75;
        case 110:
                return B110;
        case 134:
                return B134;
        case 150:
                return B150;
        case 200:
                return B200;
        case 300:
                return B300;
        case 600:
                return B600;
        case 1200:
                return B1200;
        case 1800:
                return B1800;
        case 2400:
                return B2400;
        case 4800:
                return B4800;
        case 9600:
                return B9600;
        case 19200:
                return B19200;
        case 38400:
                return B38400;
        case 57600:
                return B57600;
        case 115200:
                return B115200;
        case 230400:
                return B230400;
#if defined( __linux__ )
        case 460800:
                return B460800;
        case 500000:
                return B500000;
        case 576000:
                return B576000;
        case 921600:
                return B921600;
        case 1000000:
                return B1000000;
        case 1152000:
                return B1152000;
        case 1500000:
                return B1500000;
        case 2000000:
                return B2000000;
        case 2500000:
                return B2500000;
        case 3000000:
                return B3000000;
        case 3500000:
                return B3500000;
        case 4000000:
                return B4000000;
#endif
        default:
                return -1;
        }
}

// Apply baud rate after base termios settings are in place.
// Returns true on success, false on error (errmsg set).
// Three platform-specific definitions follow; only one is compiled.

#if defined( __APPLE__ ) && defined( MAC_OS_X_VERSION_10_4 ) && \
    ( MAC_OS_X_VERSION_MIN_REQUIRED >= MAC_OS_X_VERSION_10_4 )

bool apply_baud( int fd, uint32_t baud, struct termios& t, std::string& errmsg )
{
        int bc = to_baud_constant( baud );
        if ( bc == -1 ) {
                // Custom baud via IOSSIOSPEED (must be called after tcsetattr)
                speed_t speed = static_cast< speed_t >( baud );
                if ( ioctl( fd, IOSSIOSPEED, &speed ) == -1 ) {
                        errmsg = std::string( "ioctl(IOSSIOSPEED) failed: " ) + strerror( errno );
                        return false;
                }
                return true;
        }
        cfsetispeed( &t, static_cast< speed_t >( bc ) );
        cfsetospeed( &t, static_cast< speed_t >( bc ) );
        return true;
}

#elif defined( __linux__ )

bool apply_baud( int /*fd*/, uint32_t baud, struct termios& t, std::string& errmsg )
{
        int bc = to_baud_constant( baud );
        if ( bc == -1 ) {
                errmsg =
                    "Baud rate " + std::to_string( baud ) + " is not supported on this platform";
                return false;
        }
        cfsetispeed( &t, static_cast< speed_t >( bc ) );
        cfsetospeed( &t, static_cast< speed_t >( bc ) );
        return true;
}

#endif

}  // namespace

int open_serial_port( serial_config const& cfg, std::string& errmsg )
{
        int fd = open( cfg.path.c_str(), O_RDWR | O_NOCTTY | O_NONBLOCK | O_CLOEXEC );
        if ( fd == -1 ) {
                errmsg = std::string( "open(\"" ) + cfg.path + "\"): " + strerror( errno );
                return -1;
        }

        struct termios t;
        if ( tcgetattr( fd, &t ) == -1 ) {
                errmsg = std::string( "tcgetattr: " ) + strerror( errno );
                close( fd );
                return -1;
        }

        // Raw input: ignore parity errors, nothing else
        t.c_iflag = IGNPAR;

        // Raw output
        t.c_oflag = 0;

        // No line discipline, no echo, no signals
        t.c_lflag = 0;

        // 8 data bits
        t.c_cflag &= ~static_cast< tcflag_t >( CSIZE );
        t.c_cflag |= CS8;

        // No parity (default; overridden below)
        t.c_cflag &= ~static_cast< tcflag_t >( PARENB );
        t.c_cflag &= ~static_cast< tcflag_t >( PARODD );

        // 1 stop bit (default; overridden below)
        t.c_cflag &= ~static_cast< tcflag_t >( CSTOPB );

        // No hardware flow control (default; overridden below)
        t.c_cflag &= ~static_cast< tcflag_t >( CRTSCTS );

        // No software flow control (default; overridden below)
        t.c_iflag &= ~static_cast< tcflag_t >( IXON | IXOFF | IXANY );

        // Enable receiver, ignore modem status lines
        t.c_cflag |= ( CLOCAL | CREAD );

        // Non-blocking reads
        t.c_cc[VMIN]  = 0;
        t.c_cc[VTIME] = 0;

        // Parity
        switch ( cfg.parity ) {
        case serial_config::parity_t::none:
                break;
        case serial_config::parity_t::odd:
                t.c_cflag |= PARENB;
                t.c_cflag |= PARODD;
                break;
        case serial_config::parity_t::even:
                t.c_cflag |= PARENB;
                t.c_cflag &= ~static_cast< tcflag_t >( PARODD );
                break;
        }

        // Stop bits
        if ( cfg.stop == serial_config::stop_bits_t::two )
                t.c_cflag |= CSTOPB;

        // Flow control
        switch ( cfg.flow ) {
        case serial_config::flow_t::none:
                break;
        case serial_config::flow_t::rtscts:
                t.c_cflag |= CRTSCTS;
                break;
        case serial_config::flow_t::xonxoff:
                t.c_iflag |= ( IXON | IXOFF );
                break;
        }

        // Baud rate (may modify t or use ioctl, depends on platform)
        if ( !apply_baud( fd, cfg.baud, t, errmsg ) ) {
                close( fd );
                return -1;
        }

        tcflush( fd, TCIOFLUSH );

        if ( tcsetattr( fd, TCSANOW, &t ) == -1 ) {
                errmsg = std::string( "tcsetattr: " ) + strerror( errno );
                close( fd );
                return -1;
        }

        return fd;
}

std::optional< serial_transport > serial_transport::open(
    uv_loop_t*           loop,
    serial_config const& cfg,
    std::string&         errmsg )
{
        int fd = open_serial_port( cfg, errmsg );
        if ( fd < 0 )
                return std::nullopt;

        auto pipe = std::make_shared< uv_pipe_t >();
        uv_pipe_init( loop, pipe.get(), /*ipc=*/0 );
        if ( int r = uv_pipe_open( pipe.get(), fd ); r != 0 ) {
                errmsg = std::string( "uv_pipe_open failed: " ) + uv_strerror( r );
                ::close( fd );
                return std::nullopt;
        }

        return serial_transport{ { std::move( pipe ) } };
}

}  // namespace asrtio
