#ifndef CMCPP_LOWER_HPP
#define CMCPP_LOWER_HPP

#include "context.hpp"
#include "integer.hpp"
#include "float.hpp"
#include "string.hpp"
#include "list.hpp"
#include "record.hpp"
#include "util.hpp"

#include <tuple>
#include <cassert>

namespace cmcpp
{
    template <Boolean T>
    inline WasmValVector lower_flat(CallContext &cx, const T &v)
    {
        return {static_cast<ValTrait<T>::flat_type>(v)};
    }

    template <UnsignedInteger T>
    inline WasmValVector lower_flat(CallContext &cx, const T &v)
    {
        using FT = ValTrait<T>::flat_type;
        FT fv = v;
        return {fv};
    }

    template <SignedInteger T>
    inline WasmValVector lower_flat(CallContext &cx, const T &v)
    {
        using FT = ValTrait<T>::flat_type;
        return integer::lower_flat_signed(v, ValTrait<FT>::size * 8);
    }

    template <Float T>
    inline WasmValVector lower_flat(CallContext &cx, const T &v)
    {
        return {float_::maybe_scramble_nan<T>(v)};
    }

    template <String T>
    inline WasmValVector lower_flat(CallContext &cx, const T &v)
    {
        return string::lower_flat(cx, v);
    }

    template <List T>
    inline WasmValVector lower_flat(CallContext &cx, const T &v)
    {
        return list::lower_flat(cx, v);
    }

    template <Record T>
    inline WasmValVector lower_flat(CallContext &cx, const T &v)
    {
        return record::lower_flat(cx, v);
    }
}

#endif
