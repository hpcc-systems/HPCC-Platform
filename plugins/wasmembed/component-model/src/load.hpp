#ifndef CMCPP_LOAD_HPP
#define CMCPP_LOAD_HPP

#include "context.hpp"
#include "integer.hpp"
#include "float.hpp"
#include "string.hpp"
#include "util.hpp"

namespace cmcpp
{
    template <Boolean T>
    inline T load(const CallContext &cx, uint32_t ptr)
    {
        return convert_int_to_bool(integer::load<uint8_t>(cx, ptr));
    }

    template <Integer T>
    inline uint8_t load(const CallContext &cx, uint32_t ptr)
    {
        return integer::load<T>(cx, ptr);
    }

    template <Float T>
    inline float32_t load(const CallContext &cx, uint32_t ptr)
    {
        return float_::load<T>(cx, ptr);
    }

    template <String T>
    inline string_t load(const CallContext &cx, uint32_t ptr)
    {
        return string::load(cx, ptr);
    }
}

#endif
