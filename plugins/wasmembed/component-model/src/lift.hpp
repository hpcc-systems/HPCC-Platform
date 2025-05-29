#ifndef CMCPP_LIFT_HPP
#define CMCPP_LIFT_HPP

#include "context.hpp"
#include "integer.hpp"
#include "float.hpp"
#include "string.hpp"
#include "list.hpp"
#include "record.hpp"
#include "util.hpp"

namespace cmcpp
{
    template <Boolean T>
    inline T lift_flat(const CallContext &cx, const WasmValVectorIterator &vi)
    {
        return convert_int_to_bool(vi.next<int32_t>());
    }

    template <UnsignedInteger T>
    inline T lift_flat(const CallContext &cx, const WasmValVectorIterator &vi)
    {
        return integer::lift_flat_unsigned<T>(vi, ValTrait<T>::size * 8, 8);
    }

    template <SignedInteger T>
    inline T lift_flat(const CallContext &cx, const WasmValVectorIterator &vi)
    {
        return integer::lift_flat_signed<T>(vi, ValTrait<T>::size * 8, 8);
    }

    template <Float T>
    inline T lift_flat(const CallContext &cx, const WasmValVectorIterator &vi)
    {
        return float_::canonicalize_nan<T>(vi.next<T>());
    }

    template <String T>
    inline T lift_flat(const CallContext &cx, const WasmValVectorIterator &vi)
    {
        return string::lift_flat(cx, vi);
    }

    template <List T>
    inline T lift_flat(const CallContext &cx, const WasmValVectorIterator &vi, std::size_t maybe_length)
    {
        return list::lift_flat<typename ValTrait<T>::inner_type>(cx, vi, maybe_length);
    }

    template <List T>
    inline T lift_flat(const CallContext &cx, const WasmValVectorIterator &vi)
    {
        return list::lift_flat<typename ValTrait<T>::inner_type>(cx, vi);
    }

    template <Record T>
    T lift_flat(const CallContext &cx, const WasmValVectorIterator &vi)
    {
        auto x = record::lift_flat<T>(cx, vi);
        return x;
    }
}

#endif
