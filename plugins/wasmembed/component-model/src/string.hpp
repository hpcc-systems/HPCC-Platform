#ifndef CMCPP_STRING_HPP
#define CMCPP_STRING_HPP

#include "context.hpp"

namespace cmcpp
{
    namespace string
    {
        std::pair<offset, bytes> store_into_range(CallContext &cx, const string_t &v);
        void store(CallContext &cx, const string_t &v, uint32_t ptr);
        WasmValVector lower_flat(CallContext &cx, const string_t &v);

        string_t load(const CallContext &cx, offset offset);
        string_t lift_flat(const CallContext &cx, const WasmValVectorIterator &vi);
    }
}

#endif
