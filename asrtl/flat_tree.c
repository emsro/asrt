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
#include "./flat_tree.h"

#include "./log.h"

static inline char* asrt_flat_strdup( struct asrt_allocator* alloc, char const* str )
{
        uint32_t len  = (uint32_t) strlen( str ) + 1;
        char*    copy = (char*) asrt_alloc( alloc, len );
        if ( copy )
                memcpy( copy, str, len );
        return copy;
}

enum asrt_status asrt_flat_block_init(
    struct asrt_flat_block* block,
    struct asrt_allocator*  alloc,
    uint32_t                node_capacity )
{
        if ( !block || node_capacity == 0 ) {
                ASRT_ERR_LOG( "asrt_flat_tree", "block_init: NULL block or zero capacity" );
                return ASRT_INIT_ERR;
        }

        block->nodes = (struct asrt_flat_node*) asrt_alloc(
            alloc, sizeof( struct asrt_flat_node ) * node_capacity );
        if ( !block->nodes ) {
                ASRT_ERR_LOG(
                    "asrt_flat_tree", "block_init: alloc failed for %u nodes", node_capacity );
                return ASRT_ALLOC_ERR;
        }
        for ( uint32_t i = 0; i < node_capacity; i++ )
                block->nodes[i] =
                    ( struct asrt_flat_node ){ .value = { .type = ASRT_FLAT_STYPE_NONE } };
        block->node_count = 0;
        return ASRT_SUCCESS;
}

static void asrt_flat_free_node_strings( struct asrt_allocator* alloc, struct asrt_flat_node* node )
{
        if ( node->key )
                asrt_free( alloc, (void**) &node->key );
        if ( node->value.type == ASRT_FLAT_STYPE_STR && node->value.data.s.str_val )
                asrt_free( alloc, (void**) &node->value.data.s.str_val );
}

enum asrt_status asrt_flat_block_deinit(
    struct asrt_flat_block* block,
    struct asrt_allocator*  alloc,
    uint32_t                node_capacity )
{
        if ( !block ) {
                ASRT_ERR_LOG( "asrt_flat_tree", "block_deinit: NULL block" );
                return ASRT_INIT_ERR;
        }
        if ( block->nodes != NULL ) {
                for ( uint32_t i = 0; i < node_capacity; i++ )
                        if ( block->nodes[i].value.type != ASRT_FLAT_STYPE_NONE )
                                asrt_flat_free_node_strings( alloc, &block->nodes[i] );
                asrt_free( alloc, (void**) &block->nodes );
        }
        block->nodes      = NULL;
        block->node_count = 0;
        return ASRT_SUCCESS;
}

static inline struct asrt_flat_block* asrt_flat_realloc_blocks(
    struct asrt_allocator*  alloc,
    struct asrt_flat_block* blocks,
    uint32_t                blocks_capacity,
    uint32_t                new_capacity )
{
        struct asrt_flat_block* new_blocks = (struct asrt_flat_block*) asrt_alloc(
            alloc, sizeof( struct asrt_flat_block ) * new_capacity );
        if ( !new_blocks )
                return NULL;
        uint32_t i = 0;
        for ( ; i < blocks_capacity; i++ )
                new_blocks[i] = blocks[i];
        for ( ; i < new_capacity; i++ )
                new_blocks[i] = ( struct asrt_flat_block ){ .nodes = NULL, .node_count = 0 };
        asrt_free( alloc, (void**) &blocks );
        return new_blocks;
}

enum asrt_status asrt_flat_tree_init(
    struct asrt_flat_tree* t,
    struct asrt_allocator  alloc,
    uint32_t               block_capacity,
    uint32_t               node_capacity )
{
        if ( !t ) {
                ASRT_ERR_LOG( "asrt_flat_tree", "init: NULL tree" );
                return ASRT_INIT_ERR;
        }

        t->alloc  = alloc;
        t->blocks = (struct asrt_flat_block*) asrt_alloc(
            &t->alloc, sizeof( struct asrt_flat_block ) * block_capacity );
        if ( !t->blocks ) {
                ASRT_ERR_LOG(
                    "asrt_flat_tree", "init: alloc failed for %u blocks", block_capacity );
                return ASRT_ALLOC_ERR;
        }
        for ( uint32_t i = 0; i < block_capacity; i++ )
                t->blocks[i] = ( struct asrt_flat_block ){ .nodes = NULL, .node_count = 0 };
        t->block_capacity = block_capacity;
        t->node_capacity  = node_capacity;

        return ASRT_SUCCESS;
}

static inline struct asrt_flat_node* asrt_flat_get_node(
    struct asrt_flat_tree const* t,
    asrt_flat_id                 id )
{
        uint32_t capacity = t->node_capacity * t->block_capacity;
        if ( id >= capacity )
                return NULL;
        struct asrt_flat_block* block = &t->blocks[id / t->node_capacity];
        if ( !block->nodes )
                return NULL;
        return &block->nodes[id % t->node_capacity];
}

static inline enum asrt_status asrt_flat_set_node(
    struct asrt_flat_node* node,
    struct asrt_allocator* alloc,
    char const*            key,
    asrt_flat_value_type   type,
    union asrt_flat_data   data )
{
        if ( !node ) {
                ASRT_ERR_LOG( "asrt_flat_tree", "set_node: NULL node" );
                return ASRT_ARG_ERR;
        }
        if ( key ) {
                node->key = asrt_flat_strdup( alloc, key );
                if ( !node->key ) {
                        ASRT_ERR_LOG( "asrt_flat_tree", "set_node: key alloc failed" );
                        return ASRT_ALLOC_ERR;
                }
        } else {
                node->key = NULL;
        }
        node->value.type = type;
        node->value.data = data;
        if ( type == ASRT_FLAT_STYPE_STR && data.s.str_val ) {
                node->value.data.s.str_val = asrt_flat_strdup( alloc, data.s.str_val );
                if ( !node->value.data.s.str_val ) {
                        ASRT_ERR_LOG( "asrt_flat_tree", "set_node: str_val alloc failed" );
                        asrt_free( alloc, (void**) &node->key );
                        return ASRT_ALLOC_ERR;
                }
        }
        node->next_sibling = 0;
        return ASRT_SUCCESS;
}

static enum asrt_status asrt_flat_ensure_capacity( struct asrt_flat_tree* t, asrt_flat_id node_id )
{
        uint32_t capacity = t->node_capacity * t->block_capacity;
        if ( node_id >= capacity ) {
                uint32_t new_block_capacity = t->block_capacity == 0 ? 1 : t->block_capacity * 2;
                while ( node_id >= t->node_capacity * new_block_capacity )
                        new_block_capacity *= 2;
                struct asrt_flat_block* new_blocks = asrt_flat_realloc_blocks(
                    &t->alloc, t->blocks, t->block_capacity, new_block_capacity );
                if ( !new_blocks ) {
                        ASRT_ERR_LOG( "asrt_flat_tree", "ensure_capacity: realloc blocks failed" );
                        return ASRT_ALLOC_ERR;
                }
                t->blocks         = new_blocks;
                t->block_capacity = new_block_capacity;
        }
        struct asrt_flat_block* block = &t->blocks[node_id / t->node_capacity];
        if ( block->nodes == NULL ) {
                enum asrt_status st = asrt_flat_block_init( block, &t->alloc, t->node_capacity );
                if ( st != ASRT_SUCCESS )
                        return st;
        }
        return ASRT_SUCCESS;
}

/// Validate parent/key constraints and locate the insertion points.
/// Walk keyed children to find the tail and reject duplicate keys.
/// Returns the next_sibling slot of the last child, or error on duplicate/broken chain.
static enum asrt_status asrt_flat_find_keyed_tail(
    struct asrt_flat_tree* t,
    asrt_flat_id           first_child,
    asrt_flat_id           parent_id,
    char const*            key,
    asrt_flat_id**         link )
{
        asrt_flat_id           cid   = first_child;
        struct asrt_flat_node* child = NULL;
        while ( cid != 0 ) {
                child = asrt_flat_get_node( t, cid );
                if ( !child ) {
                        ASRT_ERR_LOG(
                            "asrt_flat_tree", "prepare_link: child node %u missing", cid );
                        return ASRT_INTERNAL_ERR;
                }
                if ( child->key && strcmp( child->key, key ) == 0 ) {
                        ASRT_ERR_LOG(
                            "asrt_flat_tree",
                            "append: duplicate key '%s' under parent_id=%u",
                            key,
                            parent_id );
                        return ASRT_ARG_ERR;
                }
                if ( child->next_sibling == 0 )
                        break;
                cid = child->next_sibling;
        }
        *link = &child->next_sibling;
        return ASRT_SUCCESS;
}

/// On success, *link and *tail point to the two uint32_t slots that must
/// receive the new node_id to complete the linkage.  For parent_id==0
/// (root-level), both are set to NULL and the caller skips the write.
static enum asrt_status asrt_flat_prepare_link(
    struct asrt_flat_tree* t,
    asrt_flat_id           parent_id,
    char const*            key,
    asrt_flat_id**         link,
    asrt_flat_id**         tail )
{
        *link = NULL;
        *tail = NULL;

        if ( parent_id == 0 )
                return ASRT_SUCCESS;

        struct asrt_flat_node* parent = asrt_flat_get_node( t, parent_id );
        if ( !parent ) {
                ASRT_ERR_LOG( "asrt_flat_tree", "prepare_link: parent_id=%u not found", parent_id );
                return ASRT_ARG_ERR;
        }

        if ( parent->value.type == ASRT_FLAT_CTYPE_OBJECT ) {
                if ( key == NULL ) {
                        ASRT_ERR_LOG( "asrt_flat_tree", "object node must have a key" );
                        return ASRT_KEY_REQUIRED_ERR;
                }
        } else if ( parent->value.type == ASRT_FLAT_CTYPE_ARRAY ) {
                if ( key != NULL ) {
                        ASRT_ERR_LOG( "asrt_flat_tree", "array node cannot have a key" );
                        return ASRT_KEY_FORBIDDEN_ERR;
                }
        } else {
                ASRT_ERR_LOG(
                    "asrt_flat_tree",
                    "parent_id=%u is not a container (type=%d)",
                    parent_id,
                    parent->value.type );
                return ASRT_ARG_ERR;
        }

        struct asrt_flat_child_list* cl = &parent->value.data.cont;
        if ( cl->first_child == 0 ) {
                *link = &cl->first_child;
        } else if ( key ) {
                enum asrt_status s =
                    asrt_flat_find_keyed_tail( t, cl->first_child, parent_id, key, link );
                if ( s != ASRT_SUCCESS )
                        return s;
        } else {
                struct asrt_flat_node* child = asrt_flat_get_node( t, cl->last_child );
                if ( !child ) {
                        ASRT_ERR_LOG(
                            "asrt_flat_tree",
                            "prepare_link: last child node %u missing",
                            cl->last_child );
                        return ASRT_INTERNAL_ERR;
                }
                *link = &child->next_sibling;
        }
        *tail = &cl->last_child;
        return ASRT_SUCCESS;
}

static enum asrt_status asrt_flat_tree_append_impl(
    struct asrt_flat_tree* t,
    asrt_flat_id           parent_id,
    asrt_flat_id           node_id,
    char const*            key,
    asrt_flat_value_type   type,
    union asrt_flat_data   data )
{
        if ( !t ) {
                ASRT_ERR_LOG( "asrt_flat_tree", "append: NULL tree" );
                return ASRT_INIT_ERR;
        }
        if ( node_id == 0 || node_id == parent_id ) {
                ASRT_ERR_LOG(
                    "asrt_flat_tree",
                    "append: invalid node_id=%u parent_id=%u",
                    node_id,
                    parent_id );
                return ASRT_ARG_ERR;
        }

        enum asrt_status st = asrt_flat_ensure_capacity( t, node_id );
        if ( st != ASRT_SUCCESS )
                return st;

        struct asrt_flat_node* node = asrt_flat_get_node( t, node_id );
        if ( !node ) {
                ASRT_ERR_LOG( "asrt_flat_tree", "append: node_id=%u not reachable", node_id );
                return ASRT_ARG_ERR;
        }
        if ( node->value.type != ASRT_FLAT_STYPE_NONE ) {
                ASRT_ERR_LOG( "asrt_flat_tree", "append: node_id=%u already exists", node_id );
                return ASRT_ARG_ERR;
        }

        asrt_flat_id* link = NULL;
        asrt_flat_id* tail = NULL;
        st                 = asrt_flat_prepare_link( t, parent_id, key, &link, &tail );
        if ( st != ASRT_SUCCESS )
                return st;

        st = asrt_flat_set_node( node, &t->alloc, key, type, data );
        if ( st != ASRT_SUCCESS )
                return st;

        if ( link && tail ) {
                *link = node_id;
                *tail = node_id;
        }
        return ASRT_SUCCESS;
}

enum asrt_status asrt_flat_tree_append_scalar(
    struct asrt_flat_tree* tree,
    asrt_flat_id           parent_id,
    asrt_flat_id           node_id,
    char const*            key,
    asrt_flat_value_type   type,
    union asrt_flat_scalar scalar )
{
        union asrt_flat_data data = { .s = scalar };
        return asrt_flat_tree_append_impl( tree, parent_id, node_id, key, type, data );
}

enum asrt_status asrt_flat_tree_append_cont(
    struct asrt_flat_tree* tree,
    asrt_flat_id           parent_id,
    asrt_flat_id           node_id,
    char const*            key,
    asrt_flat_value_type   type )
{
        union asrt_flat_data data = { .cont = { 0, 0 } };
        return asrt_flat_tree_append_impl( tree, parent_id, node_id, key, type, data );
}

enum asrt_status asrt_flat_tree_query(
    struct asrt_flat_tree const*   t,
    asrt_flat_id                   node_id,
    struct asrt_flat_query_result* result )
{
        if ( !t || !result ) {
                ASRT_ERR_LOG( "asrt_flat_tree", "query: NULL tree or result" );
                return ASRT_INIT_ERR;
        }
        struct asrt_flat_node* node = asrt_flat_get_node( t, node_id );
        if ( !node ) {
                ASRT_ERR_LOG( "asrt_flat_tree", "query: node_id=%u not found", node_id );
                return ASRT_ARG_ERR;
        }
        result->id           = node_id;
        result->key          = node->key;
        result->value        = node->value;
        result->next_sibling = node->next_sibling;
        return ASRT_SUCCESS;
}

enum asrt_status asrt_flat_tree_find_by_key(
    struct asrt_flat_tree*         t,
    asrt_flat_id                   parent_id,
    char const*                    key,
    struct asrt_flat_query_result* result )
{
        if ( !t || !key || !result ) {
                ASRT_ERR_LOG( "asrt_flat_tree", "find_by_key: NULL argument" );
                return ASRT_INIT_ERR;
        }
        struct asrt_flat_node* parent = asrt_flat_get_node( t, parent_id );
        if ( !parent ) {
                ASRT_ERR_LOG( "asrt_flat_tree", "find_by_key: parent_id=%u not found", parent_id );
                return ASRT_ARG_ERR;
        }

        asrt_flat_id child_id = 0;
        if ( parent->value.type == ASRT_FLAT_CTYPE_OBJECT ) {
                child_id = parent->value.data.cont.first_child;
        } else {
                ASRT_ERR_LOG(
                    "asrt_flat_tree", "find_by_key: parent_id=%u is not an object", parent_id );
                return ASRT_ARG_ERR;
        }

        while ( child_id != 0 ) {
                struct asrt_flat_node* child = asrt_flat_get_node( t, child_id );
                if ( !child )
                        break;
                if ( child->key && strcmp( child->key, key ) == 0 ) {
                        result->id           = child_id;
                        result->key          = child->key;
                        result->value        = child->value;
                        result->next_sibling = child->next_sibling;
                        return ASRT_SUCCESS;
                }
                child_id = child->next_sibling;
        }

        ASRT_ERR_LOG(
            "asrt_flat_tree", "find_by_key: key '%s' not found in parent %u", key, parent_id );
        return ASRT_ARG_ERR;
}

enum asrt_status asrt_flat_tree_deinit( struct asrt_flat_tree* t )
{
        if ( !t ) {
                ASRT_ERR_LOG( "asrt_flat_tree", "deinit: NULL tree" );
                return ASRT_INIT_ERR;
        }

        for ( uint32_t i = 0; i < t->block_capacity; i++ )
                asrt_flat_block_deinit( &t->blocks[i], &t->alloc, t->node_capacity );
        asrt_free( &t->alloc, (void**) &t->blocks );
        return ASRT_SUCCESS;
}

enum asrt_status asrt_flat_value_decode(
    struct asrt_span*       buff,
    uint8_t                 raw_type,
    struct asrt_flat_value* val )
{
        *val        = ( struct asrt_flat_value ){ 0 };
        val->type   = (asrt_flat_value_type) raw_type;
        size_t need = asrt_flat_value_wire_size( *val );
        if ( need > 0 && asrt_span_unfit_for( buff, (uint32_t) need ) )
                return ASRT_RECV_ERR;
        switch ( raw_type ) {
        case ASRT_FLAT_STYPE_NONE:
        case ASRT_FLAT_STYPE_NULL:
                break;
        case ASRT_FLAT_STYPE_STR: {
                size_t   search_len = (size_t) ( buff->e - buff->b );
                uint8_t* snul       = (uint8_t*) memchr( buff->b, '\0', search_len );
                if ( !snul )
                        return ASRT_RECV_ERR;
                val->data.s.str_val = (char const*) buff->b;
                buff->b             = snul + 1;
                break;
        }
        case ASRT_FLAT_STYPE_U32:
                asrt_cut_u32( &buff->b, &val->data.s.u32_val );
                break;
        case ASRT_FLAT_STYPE_I32:
                asrt_cut_i32( &buff->b, &val->data.s.i32_val );
                break;
        case ASRT_FLAT_STYPE_BOOL:
                asrt_cut_u32( &buff->b, &val->data.s.bool_val );
                break;
        case ASRT_FLAT_STYPE_FLOAT: {
                uint32_t bits;
                asrt_cut_u32( &buff->b, &bits );
                memcpy( &val->data.s.float_val, &bits, sizeof bits );
                break;
        }
        case ASRT_FLAT_STYPE_U32D2:
                asrt_cut_u32( &buff->b, &val->data.s.u32d2_val.hi );
                asrt_cut_u32( &buff->b, &val->data.s.u32d2_val.lo );
                break;
        case ASRT_FLAT_CTYPE_OBJECT:
        case ASRT_FLAT_CTYPE_ARRAY:
                asrt_cut_u32( &buff->b, &val->data.cont.first_child );
                asrt_cut_u32( &buff->b, &val->data.cont.last_child );
                break;
        default:
                break;
        }
        return ASRT_SUCCESS;
}

size_t asrt_flat_value_wire_size( struct asrt_flat_value v )
{
        switch ( v.type ) {
        case ASRT_FLAT_STYPE_STR:
                return v.data.s.str_val ? strlen( v.data.s.str_val ) + 1UL : 1UL;
        case ASRT_FLAT_STYPE_U32:
        case ASRT_FLAT_STYPE_BOOL:
        case ASRT_FLAT_STYPE_FLOAT:
        case ASRT_FLAT_STYPE_I32:
                return 4U;
        case ASRT_FLAT_STYPE_U32D2:
        case ASRT_FLAT_CTYPE_OBJECT:
        case ASRT_FLAT_CTYPE_ARRAY:
                return 8U;
        default:
                return 0U;
        }
}

void asrt_flat_value_write( uint8_t** p, struct asrt_flat_value v )
{
        switch ( v.type ) {
        case ASRT_FLAT_STYPE_NONE:
        case ASRT_FLAT_STYPE_NULL:
                break;
        case ASRT_FLAT_STYPE_STR: {
                size_t sl = strlen( v.data.s.str_val );
                memcpy( *p, v.data.s.str_val, sl );
                *p += sl;
                *( *p )++ = '\0';
                break;
        }
        case ASRT_FLAT_STYPE_U32:
                asrt_add_u32( p, v.data.s.u32_val );
                break;
        case ASRT_FLAT_STYPE_I32:
                asrt_add_i32( p, v.data.s.i32_val );
                break;
        case ASRT_FLAT_STYPE_BOOL:
                asrt_add_u32( p, v.data.s.bool_val );
                break;
        case ASRT_FLAT_STYPE_FLOAT: {
                uint32_t bits;
                memcpy( &bits, &v.data.s.float_val, sizeof bits );
                asrt_add_u32( p, bits );
                break;
        }
        case ASRT_FLAT_STYPE_U32D2:
                asrt_add_u32( p, v.data.s.u32d2_val.hi );
                asrt_add_u32( p, v.data.s.u32d2_val.lo );
                break;
        case ASRT_FLAT_CTYPE_OBJECT:
        case ASRT_FLAT_CTYPE_ARRAY:
                asrt_add_u32( p, v.data.cont.first_child );
                asrt_add_u32( p, v.data.cont.last_child );
                break;
        default:
                break;
        }
}
