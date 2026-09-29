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

#ifndef ASRT_LOG_H
#define ASRT_LOG_H

#include <stdarg.h>
#include <stdio.h>
#include <time.h>

#ifdef __cplusplus
extern "C" {
#endif

/// Log severity levels.
enum asrt_log_level
{
        ASRT_LOG_DEBUG = 1,  ///< Verbose trace messages.
        ASRT_LOG_INFO  = 2,  ///< Informational messages.
        ASRT_LOG_ERROR = 4,  ///< Error conditions.
};

/// Emit a log message.  The implementation must be provided by the application
/// via ASRT_DEFINE_GPOS_LOG() or a custom definition of asrt_log_impl.
void asrt_log( enum asrt_log_level level, char const* module, char const* fmt, ... );

/// Log a DEBUG message from the given @p module.
#define ASRT_DBG_LOG( module, ... ) asrt_log( ASRT_LOG_DEBUG, module, __VA_ARGS__ )

/// Log an INFO message from the given @p module.
#define ASRT_INF_LOG( module, ... ) asrt_log( ASRT_LOG_INFO, module, __VA_ARGS__ )

/// Log an ERROR message from the given @p module.
#define ASRT_ERR_LOG( module, ... ) asrt_log( ASRT_LOG_ERROR, module, __VA_ARGS__ )

/// Fill the `struct tm` at @p tm with the local time of the `time_t` at @p t.
#if defined( _WIN32 )
#define ASRT_LOCALTIME( t, tm ) localtime_s( ( tm ), ( t ) )
#else
#define ASRT_LOCALTIME( t, tm ) localtime_r( ( t ), ( tm ) )
#endif

/// Define the log backend implementation (asrt_log_impl) with timestamps and
/// ANSI colour-coded level prefixes, writing to stderr.
#define ASRT_DEFINE_GPOS_LOG_IMPL                                                             \
        void asrt_log_impl(                                                                   \
            enum asrt_log_level level, char const* module, char const* fmt, va_list args )    \
        {                                                                                     \
                                                                                              \
                char const* level_str = "UNK";                                                \
                switch ( level ) {                                                            \
                case ASRT_LOG_DEBUG:                                                          \
                        level_str = "\033[36mDEBUG\033[0m";                                   \
                        break;                                                                \
                case ASRT_LOG_INFO:                                                           \
                        level_str = "\033[32mINFO\033[0m";                                    \
                        break;                                                                \
                case ASRT_LOG_ERROR:                                                          \
                        level_str = "\033[31mERROR\033[0m";                                   \
                        break;                                                                \
                default:                                                                      \
                        break;                                                                \
                }                                                                             \
                                                                                              \
                char      timebuf[32];                                                        \
                time_t    t = time( NULL );                                                   \
                struct tm tm;                                                                 \
                ASRT_LOCALTIME( &t, &tm );                                                    \
                strftime( timebuf, sizeof( timebuf ), "%Y%m%d %H%M%S", &tm );                 \
                                                                                              \
                fprintf( stderr, "%s %s %s :: ", timebuf, module ? module : "-", level_str ); \
                vfprintf( stderr, fmt, args );                                                \
                fprintf( stderr, "\n" );                                                      \
        }

/// Define both asrt_log_impl and the public asrt_log() dispatcher in one
/// translation unit.  Include this macro in exactly one .c/.cpp file.
#define ASRT_DEFINE_GPOS_LOG()                                                               \
        ASRT_DEFINE_GPOS_LOG_IMPL                                                            \
        void asrt_log( enum asrt_log_level level, char const* module, char const* fmt, ... ) \
        {                                                                                    \
                va_list args;                                                                \
                va_start( args, fmt );                                                       \
                asrt_log_impl( level, module, fmt, args );                                   \
                va_end( args );                                                              \
        }

#ifdef __cplusplus
}
#endif

#endif
