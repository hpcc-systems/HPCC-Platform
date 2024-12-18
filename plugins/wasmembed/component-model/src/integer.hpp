#ifndef CMCPP_INTEGER_HPP
#define CMCPP_INTEGER_HPP

#include "context.hpp"

#include <cstring>
#include <iostream>
#include <cassert>

namespace cmcpp
{
    using offset = uint32_t;

    namespace integer
    {
        template <typename T>
        void store(CallContext &cx, const T &v, offset ptr)
        {
            std::memcpy(&cx.memory[ptr], &v, sizeof(T));
        }

        template <typename T>
        WasmValVector lower_flat_signed(const T &v, uint32_t core_bits)
        {
            using WasmValType = ValTrait<T>::flat_type;
            WasmValType retVal = v;
            return {v};
        }

        template <typename T>
        T load(const CallContext &cx, offset ptr)
        {
            T retVal;
            std::memcpy(&retVal, &cx.memory[ptr], sizeof(T));
            return retVal;
        }

        template <typename T>
        T lift_flat_unsigned(const WasmValVectorIterator &vi, uint32_t core_width, uint32_t t_width)
        {
            using WasmValType = ValTrait<T>::flat_type;
            auto retVal = vi.next<WasmValType>();
            assert(ValTrait<WasmValType>::LOW_VALUE <= retVal && retVal < ValTrait<WasmValType>::HIGH_VALUE);
            return retVal;
        }

        template <typename T>
        T lift_flat_signed(const WasmValVectorIterator &vi, uint32_t core_width, uint32_t t_width)
        {
            using WasmValType = ValTrait<T>::flat_type;
            auto retVal = static_cast<T>(vi.next<WasmValType>());
            assert(ValTrait<T>::LOW_VALUE <= retVal && retVal <= ValTrait<T>::HIGH_VALUE);
            return retVal;
        }
    }
}

#endif
