
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
#ifndef ASRT_STATUS_H
#define ASRT_STATUS_H

#ifdef __cplusplus
extern "C" {
#endif

enum asrt_status
{
        /// An unhandled exception was thrown inside a coroutine task.
        ASRT_UNHANDLED_EXCEPTION = -14,

        /// Protocol version reported by the reactor does not match what the controller expects.
        ASRT_VERSION_ERR = -13,

        /// A child node key was supplied but the parent is an array
        /// (keys are forbidden on array children in the flat-tree schema).
        ASRT_KEY_FORBIDDEN_ERR = -12,

        /// A child node key was omitted but the parent is an object
        /// (keys are required on object children in the flat-tree schema).
        ASRT_KEY_REQUIRED_ERR = -11,

        /// The requested channel ID is not registered in this node chain.
        ASRT_CHANN_NOT_FOUND = -10,

        /// An unexpected internal library state was reached, indicating a library bug
        /// (e.g. a tree node referenced by ID is missing, unknown flag bits are set,
        /// or the protocol state machine is in an inconsistent stage).
        ASRT_INTERNAL_ERR = -9,

        /// An operation did not complete within the allowed number of ticks
        /// (e.g. waiting for the controller init reply).
        ASRT_TIMEOUT_ERR = -8,

        /// A non-idempotent operation was requested while a previous one is still in progress
        /// (e.g. starting a new send while a send is outstanding).
        ASRT_BUSY_ERR = -7,

        /// A send buffer or request could not be enqueued
        /// (e.g. the send queue is full or the network layer reported failure).
        ASRT_SEND_ERR = -6,

        /// A message failed to decode or an unexpected/unknown message was received:
        /// malformed encoding, unexpected end of buffer, trailing bytes after decode,
        /// message not expected in the current protocol state, or unknown message/test ID.
        ASRT_RECV_ERR = -5,

        /// An output or intermediate buffer is too small for the data to be encoded or stored
        /// (e.g. message too large for the send buffer).
        ASRT_SIZE_ERR = -4,

        /// Memory allocation failed (e.g. failed to allocate buffer for sending a message).
        ASRT_ALLOC_ERR = -3,

        /// A function was called with an invalid argument
        /// (e.g. NULL pointer, out-of-range value, conflicting argument combination,
        /// or an event type that is not handled by this module).
        ASRT_ARG_ERR = -2,

        /// A function was called at the wrong lifecycle stage
        /// (e.g. using an uninitialised module, or calling init when already initialised).
        ASRT_INIT_ERR = -1,

        /// Operation completed successfully.
        ASRT_SUCCESS = 1,

        /// A test failed (e.g. an assertion failure in a test task).
        ASRT_FAILURE = 2,
};

#ifdef __cplusplus
}
#endif

#endif
