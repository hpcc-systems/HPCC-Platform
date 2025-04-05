#include "host-util.hpp"
#include <cstring>
#include <cassert>
// #include "utf8.h"

void trap(const char *msg)
{
    throw new std::runtime_error(msg);
}

// bool isLatin1(const std::string &str)
// {
//     return utf8::is_valid(str);
// }

std::pair<char8_t *, size_t> convert(char8_t *dest, const char8_t *src, uint32_t byte_len, Encoding from_encoding, Encoding to_encoding)
{
    switch (from_encoding)
    {
    case Encoding::Latin1:
    case Encoding::Utf8:
        switch (to_encoding)
        {
        case Encoding::Latin1:
        case Encoding::Utf8:
            std::memcpy(dest, src, byte_len);
            return std::make_pair(reinterpret_cast<char8_t *>(dest), byte_len);
        case Encoding::Utf16:
        case Encoding::Latin1_Utf16:
        // {
        //     std::u16string s = utf8::utf8to16(std::string_view((const char *)src, byte_len));
        //     std::memcpy(dest, s.data(), s.size() * 2);
        //     return std::make_pair(reinterpret_cast<char8_t *>(dest), s.size() * 2);
        // }
        default:
            throw std::runtime_error("Invalid encoding");
        }
        break;
    case Encoding::Utf16:
    case Encoding::Latin1_Utf16:
        switch (to_encoding)
        {
        case Encoding::Latin1:
        case Encoding::Utf8:
        // {
        //     std::string s = utf8::utf16to8(std::u16string_view((const char16_t *)src, byte_len));
        //     std::memcpy(dest, s.data(), s.size());
        //     return std::make_pair(reinterpret_cast<char8_t *>(dest), byte_len);
        // }
        case Encoding::Utf16:
        case Encoding::Latin1_Utf16:
            std::memcpy(dest, src, byte_len);
            return std::make_pair(reinterpret_cast<char8_t *>(dest), byte_len);
        default:
            throw std::runtime_error("Invalid encoding");
        }
        break;
    }
    throw std::runtime_error("Invalid encoding");
}
