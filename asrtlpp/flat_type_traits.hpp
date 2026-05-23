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

#include "../asrtl/flat_tree.h"

#include <cstdint>

namespace asrt
{

using flat_id = asrt_flat_id;

/// Tag type representing a flat-tree OBJECT container node.
struct obj
{
};
/// Tag type representing a flat-tree ARRAY container node.
struct arr
{
};

/// Trait mapping a C++ type to its flat_tree metadata.
/// Each specialisation provides:
///   raw_type   — the type stored inside the C union
///   value_type — the C++ facing type (often the same)
///   flat_type  — the asrt_flat_value_type constant
///   is_scalar  — true for leaf nodes, false for containers
template < typename T >
struct flat_type_traits;

template <>
struct flat_type_traits< uint32_t >
{
        using raw_type                  = uint32_t;
        using value_type                = uint32_t;
        static constexpr auto flat_type = ASRT_FLAT_STYPE_U32;
        static constexpr bool is_scalar = true;
};

template <>
struct flat_type_traits< int32_t >
{
        using raw_type                  = int32_t;
        using value_type                = int32_t;
        static constexpr auto flat_type = ASRT_FLAT_STYPE_I32;
        static constexpr bool is_scalar = true;
};

template <>
struct flat_type_traits< float >
{
        using raw_type                  = float;
        using value_type                = float;
        static constexpr auto flat_type = ASRT_FLAT_STYPE_FLOAT;
        static constexpr bool is_scalar = true;
};

template <>
struct flat_type_traits< char const* >
{
        using raw_type                  = char const*;
        using value_type                = char const*;
        static constexpr auto flat_type = ASRT_FLAT_STYPE_STR;
        static constexpr bool is_scalar = true;
};

template <>
struct flat_type_traits< bool >
{
        using raw_type                  = uint32_t;
        using value_type                = bool;
        static constexpr auto flat_type = ASRT_FLAT_STYPE_BOOL;
        static constexpr bool is_scalar = true;
};

template <>
struct flat_type_traits< obj >
{
        using raw_type                  = asrt_flat_child_list;
        using value_type                = asrt_flat_child_list;
        static constexpr auto flat_type = ASRT_FLAT_CTYPE_OBJECT;
        static constexpr bool is_scalar = false;
};

template <>
struct flat_type_traits< arr >
{
        using raw_type                  = asrt_flat_child_list;
        using value_type                = asrt_flat_child_list;
        static constexpr auto flat_type = ASRT_FLAT_CTYPE_ARRAY;
        static constexpr bool is_scalar = false;
};

using u32d2 = asrt_flat_u32d2;

template <>
struct flat_type_traits< u32d2 >
{
        using raw_type                  = asrt_flat_u32d2;
        using value_type                = asrt_flat_u32d2;
        static constexpr auto flat_type = ASRT_FLAT_STYPE_U32D2;
        static constexpr bool is_scalar = true;
};

}  // namespace asrt
