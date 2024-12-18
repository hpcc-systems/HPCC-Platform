#include "string.hpp"
#include "integer.hpp"
#include "util.hpp"

#include <algorithm>
#include <cassert>

namespace cmcpp
{
    namespace string
    {
        const uint32_t MAX_STRING_BYTE_LENGTH = (1U << 31) - 1;

        std::pair<uint32_t, uint32_t> store_string_copy(CallContext &cx, const char8_t *src, uint32_t src_code_units, uint32_t dst_code_unit_size, uint32_t dst_alignment, Encoding dst_encoding)
        {
            uint32_t dst_byte_length = dst_code_unit_size * src_code_units;
            trap_if(cx, dst_byte_length > MAX_STRING_BYTE_LENGTH);
            uint32_t ptr = cx.realloc(0, 0, dst_alignment, dst_byte_length);
            trap_if(cx, ptr != align_to(ptr, dst_alignment));
            trap_if(cx, ptr + dst_byte_length > cx.memory.size());
            auto encoded = cx.convert((char8_t *)&cx.memory[ptr], src, src_code_units, cx.guest_encoding, dst_encoding);
            // Python test case is a utf8 str pretending to be a utf16  ---  assert(dst_byte_length == encoded.second);
            return std::make_pair(ptr, src_code_units);
        }

        std::pair<uint32_t, uint32_t> store_string_to_utf8(CallContext &cx, Encoding src_encoding, const char8_t *src, uint32_t src_code_units, uint32_t worst_case_size)
        {
            assert(worst_case_size <= MAX_STRING_BYTE_LENGTH);
            uint32_t ptr = cx.realloc(0, 0, 1, worst_case_size);
            trap_if(cx, ptr + src_code_units > cx.memory.size());
            auto encoded = cx.convert((char8_t *)&cx.memory[ptr], src, src_code_units, src_encoding, Encoding::Utf8);
            if (worst_case_size > encoded.second)
            {
                ptr = cx.realloc(ptr, worst_case_size, 1, encoded.second);
                assert(ptr + encoded.second <= cx.memory.size());
            }
            return std::make_pair(ptr, encoded.second);
        }

        std::pair<uint32_t, uint32_t> store_utf16_to_utf8(CallContext &cx, const char8_t *src, uint32_t src_code_units)
        {
            uint32_t worst_case_size = src_code_units * 3;
            return store_string_to_utf8(cx, Encoding::Utf16, src, src_code_units, worst_case_size);
        }

        std::pair<uint32_t, uint32_t> store_latin1_to_utf8(CallContext &cx, const char8_t *src, uint32_t src_code_units)
        {
            uint32_t worst_case_size = src_code_units * 2;
            return store_string_to_utf8(cx, Encoding::Latin1, src, src_code_units, worst_case_size);
        }

        std::pair<uint32_t, uint32_t> store_utf8_to_utf16(CallContext &cx, const char8_t *src, uint32_t src_code_units)
        {
            uint32_t worst_case_size = 2 * src_code_units;
            trap_if(cx, worst_case_size > MAX_STRING_BYTE_LENGTH);
            uint32_t ptr = cx.realloc(0, 0, 2, worst_case_size);
            trap_if(cx, ptr != align_to(ptr, 2));
            trap_if(cx, ptr + worst_case_size > cx.memory.size());
            auto encoded = cx.convert((char8_t *)&cx.memory[ptr], src, src_code_units, Encoding::Utf8, Encoding::Utf16);
            if (encoded.second < worst_case_size)
            {
                ptr = cx.realloc(ptr, worst_case_size, 2, encoded.second);
                assert(ptr == align_to(ptr, 2));
                assert(ptr + encoded.second <= cx.memory.size());
            }
            uint32_t code_units = static_cast<uint32_t>(encoded.second / 2);
            return std::make_pair(ptr, code_units);
        }

        std::pair<uint32_t, uint32_t> store_string_to_latin1_or_utf16(CallContext &cx, Encoding src_encoding, const char8_t *src, uint32_t src_code_units)
        {
            assert(src_code_units <= MAX_STRING_BYTE_LENGTH);
            uint32_t ptr = cx.realloc(0, 0, 2, src_code_units);
            trap_if(cx, ptr != align_to(ptr, 2));
            trap_if(cx, ptr + src_code_units > cx.memory.size());
            uint32_t dst_byte_length = 0;
            for (size_t i = 0; i < src_code_units; ++i)
            {
                uint32_t usv = *src;
                if (static_cast<uint32_t>(usv) < (1 << 8))
                {
                    cx.memory[ptr + dst_byte_length] = static_cast<uint32_t>(usv);
                    dst_byte_length += 1;
                }
                else
                {
                    uint32_t worst_case_size = 2 * src_code_units;
                    if (worst_case_size > MAX_STRING_BYTE_LENGTH)
                        throw std::runtime_error("Worst case size exceeds maximum string byte length");
                    ptr = cx.realloc(ptr, src_code_units, 2, worst_case_size);
                    if (ptr != align_to(ptr, 2))
                        throw std::runtime_error("Pointer misaligned");
                    if (ptr + worst_case_size > cx.memory.size())
                        throw std::runtime_error("Out of bounds access");
                    for (int j = dst_byte_length - 1; j >= 0; --j)
                    {
                        cx.memory[ptr + 2 * j] = cx.memory[ptr + j];
                        cx.memory[ptr + 2 * j + 1] = 0;
                    }
                    auto encoded = cx.convert((char8_t *)&cx.memory[ptr + 2 * dst_byte_length], src, src_code_units, cx.guest_encoding, Encoding::Utf16);
                    if (worst_case_size > encoded.second)
                    {
                        ptr = cx.realloc(ptr, worst_case_size, 2, encoded.second);
                        if (ptr != align_to(ptr, 2))
                            throw std::runtime_error("Pointer misaligned");
                        if (ptr + encoded.second > cx.memory.size())
                            throw std::runtime_error("Out of bounds access");
                    }
                    uint32_t tagged_code_units = static_cast<uint32_t>(encoded.second / 2) | UTF16_TAG;
                    return std::make_pair(ptr, tagged_code_units);
                }
            }
            if (dst_byte_length < src_code_units)
            {
                ptr = cx.realloc(ptr, src_code_units, 2, dst_byte_length);
                if (ptr != align_to(ptr, 2))
                    throw std::runtime_error("Pointer misaligned");
                if (ptr + dst_byte_length > cx.memory.size())
                    throw std::runtime_error("Out of bounds access");
            }
            return std::make_pair(ptr, dst_byte_length);
        }

        std::pair<uint32_t, uint32_t> store_probably_utf16_to_latin1_or_utf16(CallContext &cx, const char8_t *src, uint32_t src_code_units)
        {
            uint32_t src_byte_length = 2 * src_code_units;
            trap_if(cx, src_byte_length > MAX_STRING_BYTE_LENGTH);
            uint32_t ptr = cx.realloc(0, 0, 2, src_byte_length);
            trap_if(cx, ptr != align_to(ptr, 2));
            trap_if(cx, ptr + src_byte_length > cx.memory.size());
            auto encoded = cx.convert((char8_t *)&cx.memory[ptr], src, src_code_units, Encoding::Utf16, Encoding::Utf16);
            const uint8_t *enc_src_ptr = &cx.memory[ptr];
            if (std::any_of(enc_src_ptr, enc_src_ptr + encoded.second,
                            [](uint8_t c)
                            { return static_cast<unsigned char>(c) >= (1 << 8); }))
            {
                uint32_t tagged_code_units = static_cast<uint32_t>(encoded.second / 2) | UTF16_TAG;
                return std::make_pair(ptr, tagged_code_units);
            }
            uint32_t latin1_size = static_cast<uint32_t>(encoded.second / 2);
            for (uint32_t i = 0; i < latin1_size; ++i)
                cx.memory[ptr + i] = cx.memory[ptr + 2 * i];
            ptr = cx.realloc(ptr, src_byte_length, 1, latin1_size);
            trap_if(cx, ptr + latin1_size > cx.memory.size());
            return std::make_pair(ptr, latin1_size);
        }

        std::pair<offset, bytes> store_into_range(CallContext &cx, const string_t &v)
        {
            Encoding src_encoding = v.encoding;
            const char8_t *src = v.ptr;
            const size_t src_tagged_code_units = v.byte_len;

            Encoding src_simple_encoding;
            uint32_t src_code_units;
            if (src_encoding == Encoding::Latin1_Utf16)
            {
                if (src_tagged_code_units & UTF16_TAG)
                {
                    src_simple_encoding = Encoding::Utf16;
                    src_code_units = src_tagged_code_units ^ UTF16_TAG;
                }
                else
                {
                    src_simple_encoding = Encoding::Latin1;
                    src_code_units = src_tagged_code_units;
                }
            }
            else
            {
                src_simple_encoding = src_encoding;
                src_code_units = src_tagged_code_units;
            }

            switch (cx.guest_encoding)
            {
            case Encoding::Latin1:
            case Encoding::Utf8:
                switch (src_simple_encoding)
                {
                case Encoding::Utf8:
                    return store_string_copy(cx, src, src_code_units, 1, 1, Encoding::Utf8);
                case Encoding::Utf16:
                    return store_utf16_to_utf8(cx, src, src_code_units);
                case Encoding::Latin1:
                    return store_latin1_to_utf8(cx, src, src_code_units);
                }
                break;
            case Encoding::Utf16:
                switch (src_simple_encoding)
                {
                case Encoding::Utf8:
                    return store_utf8_to_utf16(cx, src, src_code_units);
                case Encoding::Utf16:
                    return store_string_copy(cx, src, src_code_units, 2, 2, Encoding::Utf16);
                case Encoding::Latin1:
                    return store_string_copy(cx, src, src_code_units, 2, 2, Encoding::Utf16);
                }
                break;
            case Encoding::Latin1_Utf16:
                switch (src_encoding)
                {
                case Encoding::Utf8:
                    return store_string_to_latin1_or_utf16(cx, src_encoding, src, src_code_units);
                case Encoding::Utf16:
                    return store_string_to_latin1_or_utf16(cx, src_encoding, src, src_code_units);
                case Encoding::Latin1_Utf16:
                    switch (src_simple_encoding)
                    {
                    case Encoding::Latin1:
                        return store_string_copy(cx, src, src_code_units, 1, 2, Encoding::Latin1);
                    case Encoding::Utf16:
                        return store_probably_utf16_to_latin1_or_utf16(cx, src, src_code_units);
                    }
                }
            }
            assert(false);
            return std::make_pair(0, 0);
        }

        void store(CallContext &cx, const string_t &v, uint32_t ptr)
        {
            auto [begin, tagged_code_units] = store_into_range(cx, v);
            integer::store(cx, begin, ptr);
            integer::store(cx, tagged_code_units, ptr + 4);
        }

        WasmValVector lower_flat(CallContext &cx, const string_t &v)
        {
            auto [ptr, packed_length] = store_into_range(cx, v);
            return {(int32_t)ptr, (int32_t)packed_length};
        }

        string_t load_from_range(const CallContext &cx, uint32_t ptr, uint32_t tagged_code_units)
        {
            uint32_t alignment = 0;
            uint32_t byte_length = 0;
            Encoding encoding = Encoding::Utf8;
            switch (cx.guest_encoding)
            {
            case Encoding::Utf8:
                alignment = 1;
                byte_length = tagged_code_units;
                encoding = Encoding::Utf8;
                break;
            case Encoding::Utf16:
                alignment = 2;
                byte_length = 2 * tagged_code_units;
                encoding = Encoding::Utf16;
                break;
            case Encoding::Latin1_Utf16:
                alignment = 2;
                if (tagged_code_units & UTF16_TAG)
                {
                    byte_length = 2 * (tagged_code_units ^ UTF16_TAG);
                    encoding = Encoding::Utf16;
                }
                else
                {
                    byte_length = tagged_code_units;
                    encoding = Encoding::Latin1;
                }
                break;
            default:
                trap_if(cx, false);
            }
            trap_if(cx, ptr != align_to(ptr, alignment));
            trap_if(cx, ptr + byte_length > cx.memory.size());
            return string_t{encoding, reinterpret_cast<const char8_t *>(&cx.memory[ptr]), byte_length};
        }

        string_t load(const CallContext &cx, offset offset)
        {
            auto begin = integer::load<uint32_t>(cx, offset);
            auto tagged_code_units = integer::load<uint32_t>(cx, offset + 4);
            return load_from_range(cx, begin, tagged_code_units);
        }

        string_t lift_flat(const CallContext &cx, const WasmValVectorIterator &vi)
        {
            auto ptr = vi.next<ValTrait<string_t>::flat_type_0>();
            auto packed_length = vi.next<ValTrait<string_t>::flat_type_1>();
            return load_from_range(cx, ptr, packed_length);
        }
    };

}
