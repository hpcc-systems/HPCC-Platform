#ifndef CMCPP_STORE_HPP
#define CMCPP_STORE_HPP

#include "context.hpp"
#include "float.hpp"
#include "integer.hpp"
#include "string.hpp"
#include "list.hpp"
#include "util.hpp"

#include <tuple>
#include <cassert>

namespace cmcpp
{
    template <Boolean T>
    inline void store(CallContext &cx, const T &v, uint32_t ptr)
    {
        integer::store<T>(cx, v, ptr);
    }

    template <Integer T>
    inline void store(CallContext &cx, const T &v, uint32_t ptr)
    {
        integer::store<T>(cx, v, ptr);
    }

    template <Float T>
    inline void store(CallContext &cx, const T &v, uint32_t ptr)
    {
        float_::store<T>(cx, v, ptr);
    }

    template <String T>
    inline void store(CallContext &cx, const T &v, uint32_t ptr)
    {
        string::store(cx, v, ptr);
    }
}

#endif
