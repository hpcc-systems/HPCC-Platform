#ifndef CMCPP_UTIL_HPP
#define CMCPP_UTIL_HPP

#include "context.hpp"

namespace cmcpp
{
    const uint32_t UTF16_TAG = 1U << 31;
    const bool DETERMINISTIC_PROFILE = false;

    void trap_if(const CallContext &cx, bool condition, const char *message = nullptr) noexcept(false);

    uint32_t align_to(uint32_t ptr, uint8_t alignment);

    bool convert_int_to_bool(uint8_t i);
}

#endif
