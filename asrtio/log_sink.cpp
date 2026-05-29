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
#include "./log_sink.hpp"

#include <chrono>
#include <cstdarg>
#include <cstdio>
#include <ostream>
#include <string>

namespace asrtio
{

namespace
{
std::string plain_log_line(
    enum asrt_log_level level,
    char const*         module,
    char const*         fmt,
    va_list             args )
{
        char msgbuf[1024];
        vsnprintf( msgbuf, sizeof( msgbuf ), fmt, args );
        char const* ls;
        if ( level == ASRT_LOG_ERROR )
                ls = "ERROR";
        else if ( level == ASRT_LOG_INFO )
                ls = "INFO ";
        else
                ls = "DEBUG";
        auto now   = std::chrono::system_clock::now();
        auto now_t = std::chrono::system_clock::to_time_t( now );
        auto us =
            std::chrono::duration_cast< std::chrono::microseconds >( now.time_since_epoch() ) %
            std::chrono::seconds( 1 );
        struct tm ti
        {
        };
        localtime_r( &now_t, &ti );
        char ts[16];
        std::snprintf(
            ts,
            sizeof( ts ),
            "%02d%02d%02d.%06d",
            ti.tm_hour,
            ti.tm_min,
            ti.tm_sec,
            static_cast< int >( us.count() ) );
        return std::string( ts ) + "  " + ( module ? module : "-" ) + "  " + ls + "  " + msgbuf;
}

std::string pbar_format_log(
    enum asrt_log_level level,
    char const*         module,
    char const*         fmt,
    va_list             args )
{
        char msgbuf[1024];
        vsnprintf( msgbuf, sizeof( msgbuf ), fmt, args );
        pbar::color lc;
        char const* ls;
        if ( level == ASRT_LOG_ERROR ) {
                lc = pbar::colors::red;
                ls = "ERROR";
        } else if ( level == ASRT_LOG_INFO ) {
                lc = pbar::colors::green;
                ls = "INFO ";
        } else {
                lc = pbar::colors::dim_gray;
                ls = "DEBUG";
        }
        return pbar::colored_wall_time() + "  " + pbar::dim( module ? module : "-" ) + "  " +
               pbar::fg( ls, lc ) + "  " + msgbuf;
}
}  // namespace

void log_sink_write(
    asrt_log_level           level,
    asrt_log_level           min_level,
    char const*              module,
    char const*              fmt,
    va_list                  args,
    std::ostream*            file,
    pbar::terminal_progress* bar )
{
        if ( file ) {
                va_list fargs;
                va_copy( fargs, args );
                auto fline = plain_log_line( level, module, fmt, fargs );
                va_end( fargs );
                *file << fline << '\n';
        }
        if ( level < min_level )
                return;
        auto line = pbar_format_log( level, module, fmt, args );
        if ( bar )
                bar->log( line );
        else
                std::printf( "%s\n", line.c_str() );
}

}  // namespace asrtio
