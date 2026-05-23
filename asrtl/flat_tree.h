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

/// Arena-allocated flat tree for JSON-like structured data.
///
/// Nodes are addressed by integer IDs (asrt_flat_id). ID 0 is a virtual root
/// sentinel — never stores data, serves as parent for top-level nodes.
///
/// Storage is split into fixed-size blocks, lazily allocated on first use.
/// Blocks grow by doubling when a node_id exceeds current capacity.
///
/// Children form a singly-linked list via next_sibling. Container nodes
/// (OBJECT, ARRAY) track first_child/last_child for O(1) append.
///
/// Strings (keys and STR values) are copied via the allocator and freed on
/// tree deinit — callers need not keep the originals alive.
///
/// Each node_id may be appended exactly once; duplicate appends are rejected.

#ifndef ASRT_FLAT_TREE_H
#define ASRT_FLAT_TREE_H

#ifdef __cplusplus
extern "C" {
#endif

#include "./allocator.h"
#include "./status.h"
#include "./util.h"

#include <stdint.h>
#include <string.h>

typedef uint32_t asrt_flat_id;

/// Two-word 64-bit value stored as two big-endian uint32_t halves.
/// hi is the most-significant word, lo the least-significant.
struct asrt_flat_u32d2
{
        uint32_t hi;
        uint32_t lo;
};

/// Scalar value types — leaf data carried by nodes.
enum asrt_flat_stype
{
        ASRT_FLAT_STYPE_NONE  = 0,
        ASRT_FLAT_STYPE_STR   = 1,
        ASRT_FLAT_STYPE_U32   = 2,
        ASRT_FLAT_STYPE_FLOAT = 3,
        ASRT_FLAT_STYPE_BOOL  = 4,
        ASRT_FLAT_STYPE_NULL  = 5,
        ASRT_FLAT_STYPE_I32   = 6,
        ASRT_FLAT_STYPE_U32D2 = 9,  ///< Two-word value: hi(u32) + lo(u32), 8 bytes on wire.
};

/// Container value types — nodes that hold child lists.
enum asrt_flat_ctype
{
        ASRT_FLAT_CTYPE_NONE   = 0,
        ASRT_FLAT_CTYPE_OBJECT = 7,
        ASRT_FLAT_CTYPE_ARRAY  = 8,
};

/// Common type for both scalar and container type tags.  The enum values do
/// not overlap (except NONE = 0), so a single uint8_t can hold either.
typedef uint8_t asrt_flat_value_type;

struct asrt_flat_child_list
{
        asrt_flat_id first_child;
        asrt_flat_id last_child;
};

union asrt_flat_scalar
{
        char const*            str_val;
        uint32_t               u32_val;
        int32_t                i32_val;
        float                  float_val;
        uint32_t               bool_val;
        struct asrt_flat_u32d2 u32d2_val;
};

union asrt_flat_data
{
        union asrt_flat_scalar      s;
        struct asrt_flat_child_list cont;
};

struct asrt_flat_value
{
        asrt_flat_value_type type;
        union asrt_flat_data data;
};

struct asrt_flat_node
{
        struct asrt_flat_value value;
        char const*  key;  ///< Owned copy. Non-NULL for OBJECT children, NULL for ARRAY children.
        asrt_flat_id next_sibling;  ///< 0 = no next sibling.
};

/// Contiguous array of nodes. Lazily allocated per block.
struct asrt_flat_block
{
        struct asrt_flat_node* nodes;
        uint32_t               node_count;
};

enum asrt_status asrt_flat_block_init(
    struct asrt_flat_block* block,
    struct asrt_allocator*  alloc,
    uint32_t                node_capacity );

enum asrt_status asrt_flat_block_deinit(
    struct asrt_flat_block* block,
    struct asrt_allocator*  alloc,
    uint32_t                node_capacity );

/// node_id maps to blocks[node_id / node_capacity][node_id % node_capacity].
struct asrt_flat_tree
{
        struct asrt_allocator alloc;

        struct asrt_flat_block* blocks;
        uint32_t                block_capacity;

        uint32_t node_capacity;  ///< Nodes per block, fixed at init.
};

enum asrt_status asrt_flat_tree_init(
    struct asrt_flat_tree* tree,
    struct asrt_allocator  alloc,
    uint32_t               block_capacity,
    uint32_t               node_capacity );

/// Insert a scalar leaf node. Grows blocks if node_id exceeds capacity.
/// parent_id=0 for root nodes. OBJECT parents require non-NULL key,
/// ARRAY parents require NULL key. Rejects duplicate node_ids.
enum asrt_status asrt_flat_tree_append_scalar(
    struct asrt_flat_tree* tree,
    asrt_flat_id           parent_id,
    asrt_flat_id           node_id,
    char const*            key,
    asrt_flat_value_type   type,
    union asrt_flat_scalar scalar );

/// Insert a container node (OBJECT or ARRAY). Same rules as append_scalar.
enum asrt_status asrt_flat_tree_append_cont(
    struct asrt_flat_tree* tree,
    asrt_flat_id           parent_id,
    asrt_flat_id           node_id,
    char const*            key,
    asrt_flat_value_type   type );

struct asrt_flat_query_result
{
        asrt_flat_id           id;
        char const*            key;
        struct asrt_flat_value value;
        asrt_flat_id           next_sibling;  ///< 0 = no next sibling.
};

/// Read a single node by ID.
enum asrt_status asrt_flat_tree_query(
    struct asrt_flat_tree const*   tree,
    asrt_flat_id                   node_id,
    struct asrt_flat_query_result* result );

/// Find a child of parent_id whose key matches. Returns the child's query result.
enum asrt_status asrt_flat_tree_find_by_key(
    struct asrt_flat_tree*         tree,
    asrt_flat_id                   parent_id,
    char const*                    key,
    struct asrt_flat_query_result* result );

enum asrt_status asrt_flat_tree_deinit( struct asrt_flat_tree* tree );

/// Returns the number of bytes the value payload occupies on the wire (not
/// including node_id, key, or type byte).
size_t asrt_flat_value_wire_size( struct asrt_flat_value v );

/// Serialize a flat_value into the buffer at *p, advancing *p past the written
/// bytes.  Does not write the type byte — caller is responsible for that.
void asrt_flat_value_write( uint8_t** p, struct asrt_flat_value v );

/// Decode a flat_value from the buffer, advancing buff->b past the consumed
/// bytes.  raw_type is the type byte already read by the caller.
enum asrt_status asrt_flat_value_decode(
    struct asrt_span*       buff,
    uint8_t                 raw_type,
    struct asrt_flat_value* val );

#ifdef __cplusplus
}
#endif

#endif
