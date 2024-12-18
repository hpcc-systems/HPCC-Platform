#include "context.hpp"

namespace cmcpp
{
    std::unique_ptr<CallContext> InstanceContext::createCallContext(const GuestMemory &memory, const Encoding &guest_encoding, const GuestPostReturn &post_return)
    {
        auto retVal = std::make_unique<CallContext>();
        retVal->trap = trap;
        retVal->convert = convert;
        retVal->realloc = realloc;
        retVal->memory = memory;
        retVal->guest_encoding = guest_encoding;
        retVal->post_return = post_return;
        return retVal;
    }

    std::unique_ptr<InstanceContext> createInstanceContext(const HostTrap &trap, HostUnicodeConversion convert, const GuestRealloc &realloc)
    {
        auto retVal = std::make_unique<InstanceContext>();
        retVal->trap = trap;
        retVal->convert = convert;
        retVal->realloc = realloc;
        return retVal;
    }
}