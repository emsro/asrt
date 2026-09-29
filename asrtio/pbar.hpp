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

// pbar.hpp — single-header terminal progress bar with scrolling log
//
// Just #include "pbar.hpp" — no macros needed.  All functions are inline.
//
// Output:
//
//   224714  PASS  test_allocator_basic.............................  143 ms
//   224715  FAIL  test_callbacks_timeout..........................   10 ms
//   000002  ── Running: test_reactor_dispatch ─────────────────────────────  ← status line
//            ↑ per-test elapsed                                              (resets each
//            set_status)
//   000018  [--------------------->                ] 42/100  │  my_suite     ← progress line
//            ↑ total elapsed

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <ctime>
#include <format>
#include <string>
#include <string_view>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#else
#include <sys/ioctl.h>
#include <unistd.h>
#endif

namespace pbar
{

// ─── Color ───────────────────────────────────────────────────────────────────

struct color
{
        uint8_t r{}, g{}, b{};
};

namespace colors
{
inline constexpr color green{ 80, 200, 80 };
inline constexpr color red{ 220, 60, 60 };
inline constexpr color yellow{ 220, 200, 60 };
inline constexpr color cyan{ 80, 200, 220 };
inline constexpr color white{ 220, 220, 220 };
inline constexpr color dim_gray{ 110, 110, 110 };
inline constexpr color orange{ 230, 130, 30 };
}  // namespace colors

// Returns text wrapped in ANSI fg-color + reset
[[nodiscard]] std::string fg( std::string_view text, color c );

// Returns text in bold
[[nodiscard]] std::string bold( std::string_view text );

// Returns text dimmed (dark gray)
[[nodiscard]] std::string dim( std::string_view text );

// ─── Config ───────────────────────────────────────────────────────────────────

struct bar_config
{
        int         total       = 0;         // 0 → indeterminate spinner
        std::string suite_label = "assert";  // shown in middle of progress line
        color       fail_color  = colors::red;
        char        fill_char   = '-';
        char        head_char   = '-';
        char        empty_char  = ' ';
};

// ─── Main class ───────────────────────────────────────────────────────────────

class terminal_progress
{
public:
        explicit terminal_progress( bar_config cfg = {} );
        ~terminal_progress();

        // Emit a permanent log line above the bar (accepts ANSI sequences)
        void log( std::string_view line );

        // Log a test-result line.
        // Format: HHMMSS  PASS/FAIL  <name>  <duration ms>
        void log_result( std::string_view name, bool passed, double duration_ms );

        // Update the status line (second from bottom)
        void set_status( std::string_view line );

        // Update the progress bar.
        //   done   – number of completed items
        //   failed – how many of those failed (shown in right section)
        void set_progress( int done, int failed = 0 );

        // Finalise: leave cursor below bar, reset ANSI formatting, flush.
        void finish();

        // Update the total (e.g. once test count is known).
        void set_total( int n );

        // Returns terminal column width (falls back to 80)
        [[nodiscard]] static int terminal_width();

private:
        void        initial_draw();
        void        redraw_bar();
        std::string render_status_line() const;
        std::string render_progress_line() const;

        bar_config                            _cfg;
        std::string                           _status;
        int                                   _done          = 0;
        int                                   _failed        = 0;
        int                                   _spinner_frame = 0;
        bool                                  _started       = false;
        bool                                  _finished      = false;
        std::chrono::steady_clock::time_point _start;
        std::chrono::steady_clock::time_point _test_start;  // reset each set_status()
};

}  // namespace pbar


// ─── Implementation (inline) ─────────────────────────────────────────────────

namespace pbar
{

// ── ANSI helpers ──────────────────────────────────────────────────────────────

inline std::string fg( std::string_view text, color c )
{
        return std::format( "\x1b[38;2;{};{};{}m{}\x1b[0m", (int) c.r, (int) c.g, (int) c.b, text );
}

inline std::string bold( std::string_view text )
{
        return std::format( "\x1b[1m{}\x1b[0m", text );
}

inline std::string dim( std::string_view text )
{
        return std::format( "\x1b[2m{}\x1b[0m", text );
}

// ── Platform: terminal width ──────────────────────────────────────────────────

inline int terminal_progress::terminal_width()
{
#ifdef _WIN32
        CONSOLE_SCREEN_BUFFER_INFO csbi;
        if ( GetConsoleScreenBufferInfo( GetStdHandle( STD_OUTPUT_HANDLE ), &csbi ) )
                return (int) ( csbi.srWindow.Right - csbi.srWindow.Left + 1 );
        return 80;
#else
        struct winsize ws
        {
        };
        if ( ::ioctl( STDOUT_FILENO, TIOCGWINSZ, &ws ) == 0 && ws.ws_col > 0 )
                return (int) ws.ws_col;
        return 80;
#endif
}

// ── String helpers ────────────────────────────────────────────────────────────

// Counts visible characters, skipping ANSI escape sequences
inline int visible_len( std::string_view s )
{
        int  len       = 0;
        bool in_escape = false;
        for ( char c : s ) {
                if ( c == '\x1b' ) {
                        in_escape = true;
                        continue;
                }
                if ( in_escape ) {
                        if ( ( c >= 'A' && c <= 'Z' ) || ( c >= 'a' && c <= 'z' ) )
                                in_escape = false;
                        continue;
                }
                ++len;
        }
        return len;
}

// Pad or truncate s so its visible length equals exactly width.
// ANSI sequences are preserved; invisible characters don't count.
inline std::string fit_to_width( std::string_view s, int width )
{
        int vlen = visible_len( s );
        if ( vlen == width )
                return std::string( s );
        if ( vlen < width )
                return std::string( s ) + std::string( width - vlen, ' ' );

        // Truncate: keep enough visible chars to fill (width-1), append '…'
        std::string result;
        result.reserve( s.size() );
        int  count     = 0;
        bool in_escape = false;
        for ( char c : s ) {
                if ( c == '\x1b' ) {
                        in_escape = true;
                        result += c;
                        continue;
                }
                if ( in_escape ) {
                        result += c;
                        if ( ( c >= 'A' && c <= 'Z' ) || ( c >= 'a' && c <= 'z' ) )
                                in_escape = false;
                        continue;
                }
                if ( count >= width - 1 ) {
                        result += "\xe2\x80\xa6";  // UTF-8 '…'
                        result += "\x1b[0m";
                        // pad remaining
                        int leftover = width - visible_len( result );
                        if ( leftover > 0 )
                                result += std::string( leftover, ' ' );
                        return result;
                }
                result += c;
                ++count;
        }
        // Shouldn't reach here, but pad just in case
        int leftover = width - visible_len( result );
        if ( leftover > 0 )
                result += std::string( leftover, ' ' );
        return result;
}

// Pad s on the LEFT so its visible length equals exactly width (right-justify).
// Truncation falls back to fit_to_width.
inline std::string rfit_to_width( std::string_view s, int width )
{
        int vlen = visible_len( s );
        if ( vlen == width )
                return std::string( s );
        if ( vlen < width )
                return std::string( width - vlen, ' ' ) + std::string( s );
        return fit_to_width( s, width );  // truncate (rare: very long duration)
}

// ── Time rendering ───────────────────────────────────────────────────────────

// Three shades of gray: hours (darkest) → minutes → seconds (lightest)
inline constexpr color k_color_hours{ 100, 100, 100 };
inline constexpr color k_color_minutes{ 148, 148, 148 };
inline constexpr color k_color_seconds{ 200, 200, 200 };

// Renders HHMMSS (6 visible chars) with three gray shades
inline std::string colored_hms( int h, int m, int s )
{
        return fg( std::format( "{:02}", h ), k_color_hours ) +
               fg( std::format( "{:02}", m ), k_color_minutes ) +
               fg( std::format( "{:02}", s ), k_color_seconds );
}

// Wall-clock time colored HHMMSS
inline std::string colored_wall_time()
{
        auto now_t = std::chrono::system_clock::to_time_t( std::chrono::system_clock::now() );
        struct tm tm_info
        {
        };
#ifdef _WIN32
        localtime_s( &tm_info, &now_t );
#else
        localtime_r( &now_t, &tm_info );
#endif
        return colored_hms( tm_info.tm_hour, tm_info.tm_min, tm_info.tm_sec );
}

// Elapsed duration since `start` as colored HHMMSS
inline std::string colored_elapsed( std::chrono::steady_clock::time_point start )
{
        using namespace std::chrono;
        auto s = (long long) duration_cast< seconds >( steady_clock::now() - start ).count();
        return colored_hms( (int) ( s / 3600 ), (int) ( ( s % 3600 ) / 60 ), (int) ( s % 60 ) );
}

// ── Rendering ─────────────────────────────────────────────────────────────────

inline std::string terminal_progress::render_status_line() const
{
        int w = terminal_width();
        // 6-char colored elapsed for the current running test + 2-space gap = 8 visible
        std::string timer     = colored_elapsed( _test_start ) + "  ";
        int         content_w = std::max( w - 8, 4 );

        std::string content;
        if ( _status.empty() ) {
                content = dim( std::string( content_w, '-' ) );
        } else {
                std::string leader     = dim( "── " );
                int         dash_count = std::max( 0, content_w - visible_len( _status ) - 6 );
                std::string trail      = dim( " " + std::string( dash_count, '-' ) );
                content                = leader + std::string( _status ) + trail;
        }
        return timer + fit_to_width( content, content_w );
}

inline std::string terminal_progress::render_progress_line() const
{
        int w     = terminal_width();
        int total = _cfg.total;

        // ── Left prefix: colored total elapsed + gap (8 visible) ─────────────
        std::string left         = colored_elapsed( _start ) + "  ";
        int         left_visible = 8;

        // ── Right section: "42/100  │  suite" ────────────────────────────────
        std::string right;
        if ( total > 0 )
                if ( _failed > 0 )
                        right = std::format(
                            "{}/{} ({} {})", _done, total, _failed, fg( "✗", _cfg.fail_color ) );
                else
                        right = std::format( "{}/{}", _done, total );
        else
                right = std::format( "{} done", _done );
        if ( !_cfg.suite_label.empty() )
                right += "  │  " + _cfg.suite_label;

        // ── Bar section: [=====>   ] ──────────────────────────────────────────
        // Layout: left(8) + "[" + bar_inner + "] " + right
        int right_visible = visible_len( right );
        int bar_inner     = std::max( w - left_visible - right_visible - 3, 4 );

        std::string bar_content;
        if ( total > 0 ) {
                int filled  = (int) ( (double) _done / total * bar_inner );
                filled      = std::clamp( filled, 0, bar_inner );
                bool at_end = ( _done >= total );
                int  head   = ( !at_end && filled < bar_inner ) ? 1 : 0;
                int  empty  = bar_inner - filled - head;

                // filled and head rendered in plain terminal default; empty is dimmed
                bar_content = std::string( filled, _cfg.fill_char );
                if ( head )
                        bar_content += _cfg.head_char;
                bar_content += dim( std::string( empty, _cfg.empty_char ) );
        } else {
                // Indeterminate spinner — plain character
                static constexpr char const* frames[] = {
                    "⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏" };
                bar_content = std::string( frames[_spinner_frame % 10] ) +
                              dim( std::string( bar_inner - 1, _cfg.empty_char ) );
        }

        std::string line = left + "[" + bar_content + "] " + right;
        return fit_to_width( line, w );
}

// ── Public API ────────────────────────────────────────────────────────────────

inline terminal_progress::terminal_progress( bar_config cfg )
  : _cfg( std::move( cfg ) )
  , _start( std::chrono::steady_clock::now() )
  , _test_start( _start )
{
#ifdef _WIN32
        HANDLE h    = GetStdHandle( STD_OUTPUT_HANDLE );
        DWORD  mode = 0;
        if ( GetConsoleMode( h, &mode ) )
                SetConsoleMode( h, mode | ENABLE_VIRTUAL_TERMINAL_PROCESSING );
#endif
        initial_draw();
}

inline terminal_progress::~terminal_progress()
{
        finish();
}

inline void terminal_progress::initial_draw()
{
        // Print the two bar lines with a trailing newline each so they occupy
        // two real terminal rows.  Cursor ends just after the progress line.
        std::printf( "%s\n%s\n", render_status_line().c_str(), render_progress_line().c_str() );
        std::fflush( stdout );
        _started = true;
}

inline void terminal_progress::redraw_bar()
{
        // Jump back to start of the status line, overwrite both lines.
        std::printf(
            "\x1b[2A\r%s\n%s\n", render_status_line().c_str(), render_progress_line().c_str() );
        std::fflush( stdout );
}

inline void terminal_progress::log( std::string_view line )
{
        if ( !_started || _finished ) {
                std::printf( "%.*s\n", (int) line.size(), line.data() );
                std::fflush( stdout );
                return;
        }
        // Jump back to start of the status line,
        // erase from cursor to end of screen, then print: log + status + progress
        std::printf(
            "\x1b[2A\r\x1b[J%.*s\n%s\n%s\n",
            (int) line.size(),
            line.data(),
            render_status_line().c_str(),
            render_progress_line().c_str() );
        std::fflush( stdout );
}

inline void terminal_progress::log_result( std::string_view name, bool passed, double duration_ms )
{
        // Wall-clock time as colored HHMMSS (6 visible chars)
        std::string time_str = colored_wall_time();

        // Duration: always rounded to ms, padded to 7 visible chars (e.g. "1234 ms")
        std::string dur_str = std::format( "{} ms", (long long) std::round( duration_ms ) );

        // Status tag  (4 visible chars)
        std::string status_tag = passed ? fg( "PASS", colors::green ) : fg( "FAIL", colors::red );

        // Layout: "HHMMSS  STATUS  <name padded>  <dur padded>"
        // Fixed visible: 6(time) + 2 + 4(status) + 2 + name_w + 2 + 7(dur) = w
        // => name_w = w - 23
        int w      = terminal_width();
        int name_w = std::max( w - 23, 12 );

        std::string line = time_str + "  " + status_tag + "  " + fit_to_width( name, name_w ) +
                           "  " + dim( rfit_to_width( dur_str, 7 ) );
        log( line );
}

inline void terminal_progress::set_status( std::string_view line )
{
        _status     = std::string( line );
        _test_start = std::chrono::steady_clock::now();
        if ( _started && !_finished )
                redraw_bar();
}

inline void terminal_progress::set_progress( int done, int failed )
{
        _done   = done;
        _failed = failed;
        ++_spinner_frame;
        if ( _started && !_finished )
                redraw_bar();
}

inline void terminal_progress::finish()
{
        if ( _finished )
                return;
        _finished = true;
        if ( _started )
                redraw_bar();
}

inline void terminal_progress::set_total( int n )
{
        _cfg.total = n;
        if ( _started && !_finished )
                redraw_bar();
}

}  // namespace pbar
