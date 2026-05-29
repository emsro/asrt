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

#include "../asrtl/log.h"
#include "./pbar.hpp"

#include <cstdarg>
#include <iosfwd>

namespace asrtio
{

/// Format and dispatch one log line.
///
/// @param level      Level of the incoming message.
/// @param min_level  Minimum level to forward to terminal/bar output.
/// @param module     Module tag string (may be null).
/// @param fmt        printf-style format string.
/// @param args       Variadic argument list; consumed by the function.
/// @param file       Optional plain-text log stream (always written, no level
///                   filter). Pass nullptr to skip.
/// @param bar        Optional active progress bar. Pass nullptr to fall back
///                   to stdout.
void log_sink_write(
    asrt_log_level           level,
    asrt_log_level           min_level,
    char const*              module,
    char const*              fmt,
    va_list                  args,
    std::ostream*            file,
    pbar::terminal_progress* bar );

}  // namespace asrtio
