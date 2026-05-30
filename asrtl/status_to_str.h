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
#ifndef ASRT_STATUS_TO_STR_H
#define ASRT_STATUS_TO_STR_H

#ifdef __cplusplus
extern "C" {
#endif

#include "./status.h"

inline static char const* asrt_status_to_str( enum asrt_status st )
{
        switch ( st ) {
        case ASRT_UNHANDLED_EXCEPTION:
                return "unhandled exception";
        case ASRT_VERSION_ERR:
                return "version mismatch";
        case ASRT_KEY_FORBIDDEN_ERR:
                return "key forbidden on array child";
        case ASRT_KEY_REQUIRED_ERR:
                return "key required on object child";
        case ASRT_CHANN_NOT_FOUND:
                return "channel not found";
        case ASRT_INTERNAL_ERR:
                return "internal error";
        case ASRT_TIMEOUT_ERR:
                return "timeout";
        case ASRT_BUSY_ERR:
                return "busy";
        case ASRT_SEND_ERR:
                return "send error";
        case ASRT_RECV_ERR:
                return "receive error";
        case ASRT_SIZE_ERR:
                return "size error";
        case ASRT_ALLOC_ERR:
                return "allocation error";
        case ASRT_ARG_ERR:
                return "argument error";
        case ASRT_INIT_ERR:
                return "lifecycle stage error";
        case ASRT_SUCCESS:
                return "success";
        case ASRT_FAILURE:
                return "test failure";
        }
        return "unknown error";
}

#ifdef __cplusplus
}
#endif

#endif
