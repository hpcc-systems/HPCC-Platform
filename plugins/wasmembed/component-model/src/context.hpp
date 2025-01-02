#ifndef CMCPP_CONTEXT_HPP
#define CMCPP_CONTEXT_HPP

#if __has_include(<span>)
#include <span>
#else
#include <string>
#include <sstream>
#endif

#include "traits.hpp"

#include <functional>
#include <memory>
#include <optional>

namespace cmcpp
{
    using HostTrap = std::function<void(const char *msg) noexcept(false)>;
    using GuestRealloc = std::function<int(int ptr, int old_size, int align, int new_size)>;
    using GuestMemory = std::span<uint8_t>;
    using GuestPostReturn = std::function<void()>;
    using HostUnicodeConversion = std::function<std::pair<char8_t *, size_t>(char8_t *dest, const char8_t *src, uint32_t byte_len, Encoding from_encoding, Encoding to_encoding)>;

    struct CallContext
    {
        HostTrap trap;
        GuestRealloc realloc;
        GuestMemory memory;
        Encoding guest_encoding;
        HostUnicodeConversion convert;
        std::optional<GuestPostReturn> post_return;
        bool sync = true;
        bool always_task_return = false;
    };

    struct InstanceContext
    {
        HostTrap trap;
        HostUnicodeConversion convert;
        GuestRealloc realloc;
        std::unique_ptr<CallContext> createCallContext(const GuestMemory &memory, const Encoding &encoding = Encoding::Utf8, const GuestPostReturn &post_return = nullptr);
    };

    std::unique_ptr<InstanceContext> createInstanceContext(const HostTrap &trap, HostUnicodeConversion convert, const GuestRealloc &realloc);
}

#endif
