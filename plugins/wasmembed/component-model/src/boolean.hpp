#ifndef CMCPP_BOOLEAN_HPP
#define CMCPP_BOOLEAN_HPP

#include "context.hpp"

namespace cmcpp
{

    namespace boolean
    {
        void store(CallContext &cx, const bool_t &v, offset ptr);

        bool_t load(const CallContext &cx, uint32_t ptr);
    }
}

#endif
