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
#ifndef ASRT_COLLECT_CLIENT_H
#define ASRT_COLLECT_CLIENT_H

#ifdef __cplusplus
extern "C" {
#endif

#include "../asrtl/chann.h"
#include "../asrtl/collect_proto.h"
#include "../asrtl/flat_tree.h"

/// Callback fired when an append send completes (success or failure).
typedef asrt_send_done_cb asrt_collect_done_cb;

enum asrt_collect_client_state
{
        ASRT_COLLECT_CLIENT_IDLE = 0,
        ASRT_COLLECT_CLIENT_READY_RECV,
        ASRT_COLLECT_CLIENT_READY_SENT,
        ASRT_COLLECT_CLIENT_ACTIVE,
        ASRT_COLLECT_CLIENT_INSERT_SENT,
        ASRT_COLLECT_CLIENT_ERROR,
};

/// Reactor-side collect client (ASRT_COLL channel).
///
/// Receives READY from the controller, replies with READY_ACK, then
/// provides fire-and-forget append() calls to push tree nodes upstream.
/// Node IDs are auto-assigned from next_node_id (received in READY).
struct asrt_collect_client
{
        struct asrt_node node;

        enum asrt_collect_client_state state;
        asrt_flat_id                   root_id;       ///< Root ID from READY message.
        asrt_flat_id                   next_node_id;  ///< Next auto-assigned node ID.

        struct asrt_u8d1msg            ready_ack_msg;
        struct asrt_collect_append_msg insert_msg;

        asrt_collect_done_cb insert_done_cb;  ///< User callback, fired when insert send completes.
        void*                insert_done_ptr;
};

/// Initialise a collect client and link it into the node chain.
///
enum asrt_status asrt_collect_client_init(
    struct asrt_collect_client* client,
    struct asrt_node*           prev );

/// Deinitialise a collect client and unlink it from the node chain.
///
void asrt_collect_client_deinit( struct asrt_collect_client* client );

/// Insert a single node into the controller's tree.
///
/// The node_id is auto-assigned from the internal counter.  For container
/// types (OBJECT, ARRAY) the assigned ID is written to @p out_id so the
/// caller can use it as parent_id for child inserts.
///
/// @p done_cb is called (with @p done_ptr) when the underlying send
/// completes.  Pass NULL to ignore.  Returns ASRT_BUSY_ERR when a previous
/// insert is still in flight.
///
enum asrt_status asrt_collect_client_insert(
    struct asrt_collect_client*   client,
    asrt_flat_id                  parent_id,
    char const*                   key,
    struct asrt_flat_value const* value,
    asrt_flat_id*                 out_id,
    asrt_collect_done_cb          done_cb,
    void*                         done_ptr );

/// Convenience: insert an OBJECT container node.
static inline enum asrt_status asrt_collect_client_insert_object(
    struct asrt_collect_client* client,
    asrt_flat_id                parent_id,
    char const*                 key,
    asrt_flat_id*               out_id,
    asrt_collect_done_cb        done_cb,
    void*                       done_ptr )
{
        struct asrt_flat_value v = { .type = ASRT_FLAT_CTYPE_OBJECT, .data = { .cont = { 0, 0 } } };
        return asrt_collect_client_insert( client, parent_id, key, &v, out_id, done_cb, done_ptr );
}

/// Convenience: insert an ARRAY container node.
static inline enum asrt_status asrt_collect_client_insert_array(
    struct asrt_collect_client* client,
    asrt_flat_id                parent_id,
    char const*                 key,
    asrt_flat_id*               out_id,
    asrt_collect_done_cb        done_cb,
    void*                       done_ptr )
{
        struct asrt_flat_value v = { .type = ASRT_FLAT_CTYPE_ARRAY, .data = { .cont = { 0, 0 } } };
        return asrt_collect_client_insert( client, parent_id, key, &v, out_id, done_cb, done_ptr );
}

/// Convenience: insert a U32 scalar node.
static inline enum asrt_status asrt_collect_client_insert_u32(
    struct asrt_collect_client* client,
    asrt_flat_id                parent_id,
    char const*                 key,
    uint32_t                    val,
    asrt_collect_done_cb        done_cb,
    void*                       done_ptr )
{
        struct asrt_flat_value v = {
            .type = ASRT_FLAT_STYPE_U32, .data = { .s = { .u32_val = val } } };
        return asrt_collect_client_insert( client, parent_id, key, &v, NULL, done_cb, done_ptr );
}

/// Convenience: insert an I32 scalar node.
static inline enum asrt_status asrt_collect_client_insert_i32(
    struct asrt_collect_client* client,
    asrt_flat_id                parent_id,
    char const*                 key,
    int32_t                     val,
    asrt_collect_done_cb        done_cb,
    void*                       done_ptr )
{
        struct asrt_flat_value v = {
            .type = ASRT_FLAT_STYPE_I32, .data = { .s = { .i32_val = val } } };
        return asrt_collect_client_insert( client, parent_id, key, &v, NULL, done_cb, done_ptr );
}

/// Convenience: insert a STR scalar node.
static inline enum asrt_status asrt_collect_client_insert_str(
    struct asrt_collect_client* client,
    asrt_flat_id                parent_id,
    char const*                 key,
    char const*                 val,
    asrt_collect_done_cb        done_cb,
    void*                       done_ptr )
{
        struct asrt_flat_value v = {
            .type = ASRT_FLAT_STYPE_STR, .data = { .s = { .str_val = val } } };
        return asrt_collect_client_insert( client, parent_id, key, &v, NULL, done_cb, done_ptr );
}

/// Convenience: insert a BOOL scalar node.
static inline enum asrt_status asrt_collect_client_insert_bool(
    struct asrt_collect_client* client,
    asrt_flat_id                parent_id,
    char const*                 key,
    uint32_t                    val,
    asrt_collect_done_cb        done_cb,
    void*                       done_ptr )
{
        struct asrt_flat_value v = {
            .type = ASRT_FLAT_STYPE_BOOL, .data = { .s = { .bool_val = val } } };
        return asrt_collect_client_insert( client, parent_id, key, &v, NULL, done_cb, done_ptr );
}

/// Convenience: insert a FLOAT scalar node.
static inline enum asrt_status asrt_collect_client_insert_float(
    struct asrt_collect_client* client,
    asrt_flat_id                parent_id,
    char const*                 key,
    float                       val,
    asrt_collect_done_cb        done_cb,
    void*                       done_ptr )
{
        struct asrt_flat_value v = {
            .type = ASRT_FLAT_STYPE_FLOAT, .data = { .s = { .float_val = val } } };
        return asrt_collect_client_insert( client, parent_id, key, &v, NULL, done_cb, done_ptr );
}

/// Convenience: insert a NULL scalar node.
static inline enum asrt_status asrt_collect_client_insert_null(
    struct asrt_collect_client* client,
    asrt_flat_id                parent_id,
    char const*                 key,
    asrt_collect_done_cb        done_cb,
    void*                       done_ptr )
{
        struct asrt_flat_value v = { .type = ASRT_FLAT_STYPE_NULL, .data = { .cont = { 0, 0 } } };
        return asrt_collect_client_insert( client, parent_id, key, &v, NULL, done_cb, done_ptr );
}

/// Return the root_id received in the READY message.
static inline asrt_flat_id asrt_collect_client_root_id( struct asrt_collect_client const* client )
{
        return client->root_id;
}

/// Return true if the client is busy (READY received and acknowledged, no errors).
static inline uint8_t asrt_collect_client_is_busy( struct asrt_collect_client const* client )
{
        return client->state == ASRT_COLLECT_CLIENT_READY_SENT ||
               client->state == ASRT_COLLECT_CLIENT_INSERT_SENT;
}

#ifdef __cplusplus
}
#endif

#endif  // ASRT_COLLECT_CLIENT_H
