/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2016 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

#ifndef TOKENSERIALIZATION_HPP
#define TOKENSERIALIZATION_HPP

#include "jlog.hpp"
#include "jstring.hpp"
#include <cmath>
#include <cerrno>
#include <limits>
#include <type_traits>

#if !defined(ERANGE)
# define ERANGE    34    /* Math result not representable.  */
#endif

// The template methods in TokenDeserializer must be redefined by each subclass
// to dispatch requests correctly.
#define EXTEND_TOKENDESERIALIZER(base) \
    template <typename TValue> \
    DeserializationResult operator () (const char* buffer, TValue& value) \
    { \
        return deserialize(buffer, value); \
    } \
    template <typename TValue> \
    DeserializationResult deserialize(const char* buffer, TValue& value) const \
    { \
        return base::deserialize(buffer, value); \
    }

class TokenSerializer
{
public:
    // Produce a buffer suitable for use by the serialize method. Used by
    // template methods that do not inherently know the buffer type.
    StringBuffer makeBuffer() const
    {
        return StringBuffer();
    }

    // Write any type of data to a given text buffer. There must be an
    // overloaded operator << to insert the value type into the buffer type.
    //
    // While this does allow multiple tokens to be serialized into a buffer,
    // it is the caller's responsibility to add any delimiters necessary for
    // subsequent deserialization.
    template <typename TValue>
    StringBuffer& serialize(const TValue& value, StringBuffer& buffer) const
    {
        buffer << value;
        return buffer;
    }

    // Convert a buffer to a character array. Used by template methods that do
    // not inherently know the buffer type.
    const char* str(const StringBuffer& buffer) const
    {
        return buffer.str();
    }
};

enum DeserializationResult
{
    Deserialization_UNKNOWN = -1,  // no deserialization attempted
    Deserialization_SUCCESS,
    Deserialization_BAD_TYPE,      // receiving value cannot be const
    Deserialization_UNSUPPORTED,   // receiving value type not handled
    Deserialization_INVALID_TOKEN, // token cannot be NULL, empty, or all whitespace
    Deserialization_NOT_A_NUMBER,  // non-numeric characters found in numeric conversion
    Deserialization_OVERFLOW,      // number too large to be represented by receiving value
    Deserialization_UNDERFLOW,     // number too small to be represented by receiving value
};

class TokenDeserializer
{
public:
    // Convert the contents of buffer from text to the requested numeric type.
    // The conversion fails if:
    //  - value is a const type
    //  - value is not a numeric type
    //  - buffer is NULL, empty or entirely whitespace characters
    //  - buffer contains any character not valid for the receiving type
    //  - buffer contains a number too large for the receiving type
    //  - buffer contains a number too small for the receiving type
    template <typename TValue>
    DeserializationResult deserialize(const char* buffer, TValue& value) const
    {
        DeserializationResult result = Deserialization_UNKNOWN;

        if (std::is_const<TValue>())
        {
            result = Deserialization_BAD_TYPE;
        }
        else if (std::is_arithmetic<TValue>())
        {
            const char* ptr = buffer;

            skipWhitespace(ptr);
            if (!*ptr)
            {
                result = Deserialization_INVALID_TOKEN;
            }
            else
            {
                if (std::is_integral<TValue>())
                {
                    if (std::is_signed<TValue>())
                    {
                        result = deserializeSigned(ptr, value);
                    }
                    else
                    {
                        result = deserializeUnsigned(ptr, value);
                    }
                }
                else if (std::is_floating_point<TValue>())
                {
                    result = deserializeFloatingPoint(ptr, value);
                }
            }
        }
        else
        {
            result = Deserialization_UNSUPPORTED;
        }

        logResult<TValue>(buffer, result);

        return result;
    }

    // Convert the contents of buffer from text to the bool data type. The
    // conversion fails if buffer contains any character that is not a valid
    // Boolean representation.
    //
    // Supported representations:
    //     true: "true"|"yes"|"on"|non-zero integer
    //     false: "false"|"no"|"off"|zero
    DeserializationResult deserialize(const char* buffer, bool& value) const
    {
        const char* ptr = buffer;
        DeserializationResult result = Deserialization_UNKNOWN;

        skipWhitespace(ptr);
        if (!*ptr)
        {
            value = false;
            result = Deserialization_INVALID_TOKEN;
        }
        else
        {
            result = deserializeBool(ptr, value);
        }

        logResult<bool>(buffer, result);

        return result;
    }

    // Allow an instance of this class to be used as a functor.
    template <typename TValue>
    DeserializationResult operator () (const char* buffer, TValue& value)
    {
        return deserialize(buffer, value);
    }

private:
    DeserializationResult deserializeBool(const char* buffer, bool& value) const
    {
        const char* ptr = buffer;
        bool tmp = false;
        DeserializationResult result = Deserialization_UNKNOWN;

        switch (*ptr)
        {
        case 't': case 'T':
            if (strnicmp(ptr + 1, "rue", 3) == 0)
            {
                result = Deserialization_SUCCESS;
                tmp = true;
                ptr += 4;
            }
            break;
        case 'f': case 'F':
            if (strnicmp(ptr + 1, "alse", 4) == 0)
            {
                result = Deserialization_SUCCESS;
                ptr += 5;
            }
            break;

        case 'y': case 'Y':
            if (strnicmp(ptr + 1, "es", 2) == 0)
            {
                result = Deserialization_SUCCESS;
                tmp = true;
                ptr += 3;
            }
            break;
        case 'n': case 'N':
            if (('o' == ptr[1]) || ('O' == ptr[1]))
            {
                result = Deserialization_SUCCESS;
                ptr += 2;
            }
            break;

        case 'o': case 'O':
            switch (ptr[1])
            {
            case 'n': case 'N':
                result = Deserialization_SUCCESS;
                tmp = true;
                ptr += 2;
                break;
            case 'f': case 'F':
                if (('f' == ptr[2]) || ('F' == ptr[2]))
                {
                    result = Deserialization_SUCCESS;
                    ptr += 3;
                }
                break;
            default:
                break;
            }
            break;

        case '0':
        case '1':
        case '2':
        case '3':
        case '4':
        case '5':
        case '6':
        case '7':
        case '8':
        case '9':
            result = Deserialization_SUCCESS;
            do
            {
                if (*ptr != '0')
                    tmp = true;
            }
            while (isdigit(*++ptr));
            break;

        default:
            break;
        }

        if (Deserialization_SUCCESS == result && !isEmptyOrWhitespace(ptr))
            result = Deserialization_NOT_A_NUMBER;
        if (Deserialization_SUCCESS == result)
            value = tmp;

        return result;
    }
    template <typename TValue>
    DeserializationResult deserializeSigned(const char* buffer, TValue& value) const
    {
        char* end = NULL;
        long long tmp = strtoll(buffer, &end, 0);
        DeserializationResult result = Deserialization_UNKNOWN;

        if (end == buffer)
        {
            result = Deserialization_NOT_A_NUMBER;
        }
        else if (LLONG_MIN == tmp && ERANGE == errno)
        {
            result = Deserialization_UNDERFLOW;
        }
        else if (LLONG_MAX == tmp && ERANGE == errno)
        {
            result = Deserialization_OVERFLOW;
        }
        else if (!isEmptyOrWhitespace(end))
        {
            result = Deserialization_NOT_A_NUMBER;
        }
#if defined(_WIN32)
// VS2015 generates this sign mismatch warning when TValue is unsigned, and
// unsigned values are not process here.
#pragma warning(suppress:4018)
#endif
        else if (tmp < std::numeric_limits<TValue>::min())
        {
            result = Deserialization_UNDERFLOW;
        }
#if defined(_WIN32)
// VS2015 generates this sign mismatch warning when TValue is unsigned, and
// unsigned values are not process here.
#pragma warning(suppress:4018)
#endif
        else if (tmp > std::numeric_limits<TValue>::max())
        {
            result = Deserialization_OVERFLOW;
        }
        else
        {
            value = TValue(tmp);
            result = Deserialization_SUCCESS;
        }

        return result;
    }
    template <typename TValue>
    DeserializationResult deserializeUnsigned(const char* buffer, TValue& value) const
    {
        char* end = NULL;
        unsigned long long tmp = strtoull(buffer, &end, 0);
        DeserializationResult result = Deserialization_UNKNOWN;

        if (end == buffer)
        {
            result = Deserialization_NOT_A_NUMBER;
        }
        else if (ULLONG_MAX == tmp && ERANGE == errno)
        {
            result = Deserialization_OVERFLOW;
        }
        else if (!isEmptyOrWhitespace(end))
        {
            result = Deserialization_NOT_A_NUMBER;
        }
        else if ('-' == buffer[0])
        {
            result = Deserialization_UNDERFLOW;
        }
#if defined(_WIN32)
// VS2015 generates this sign mismatch warning when TValue is unsigned, and tmp
// is always unsigned.
#pragma warning(suppress:4018)
#endif
        else if (tmp > std::numeric_limits<TValue>::max())
        {
            result = Deserialization_OVERFLOW;
        }
        else
        {
            value = TValue(tmp);
            result = Deserialization_SUCCESS;
        }

        return result;
    }
    template <typename TValue>
    DeserializationResult deserializeFloatingPoint(const char* buffer, TValue& value) const
    {
        char* end = NULL;
        long double tmp = strtold(buffer, &end);
        DeserializationResult result = Deserialization_UNKNOWN;

        if (0 == tmp && end == buffer)
        {
            result = Deserialization_NOT_A_NUMBER;
        }
        else if (0 == tmp && ERANGE == errno)
        {
            result = Deserialization_UNDERFLOW;
        }
        else if ((-HUGE_VALL == tmp || HUGE_VALL == tmp) && ERANGE == errno)
        {
            result = Deserialization_OVERFLOW;
        }
        else if (!isEmptyOrWhitespace(end))
        {
            result = Deserialization_NOT_A_NUMBER;
        }
#if defined(_WIN32)
// VS2015 generates this warning (as an error) with unsigned integral types,
// even though this method does not process integral types.
#pragma warning(suppress:4146)
#endif
        else if (tmp < -std::numeric_limits<TValue>::max())
        {
            result = Deserialization_OVERFLOW;
        }
        else if (tmp > std::numeric_limits<TValue>::max())
        {
            result = Deserialization_OVERFLOW;
        }
        else
        {
            value = TValue(tmp);
            result = Deserialization_SUCCESS;
        }

        return result;
    }
    template <typename TValue>
    void logResult(const char* buffer, DeserializationResult result) const
    {
        bool success = false;
        if (Deserialization_SUCCESS == result)
#if !defined(_DEBUG)
            return;
#else
            success = true;
#endif

        const char* typeStr = typeid(TValue).name();
        const char* resultStr = NULL;

        switch (result)
        {
        case Deserialization_UNKNOWN:       resultStr = "unknown"; break;
        case Deserialization_SUCCESS:       resultStr = "success"; break;
        case Deserialization_BAD_TYPE:      resultStr = "bad type"; break;
        case Deserialization_UNSUPPORTED:   resultStr = "unsupported"; break;
        case Deserialization_INVALID_TOKEN: resultStr = "invalid token"; break;
        case Deserialization_NOT_A_NUMBER:  resultStr = "not a number"; break;
        case Deserialization_OVERFLOW:      resultStr = "overflow"; break;
        case Deserialization_UNDERFLOW:     resultStr = "underflow"; break;
        default:                            resultStr = "unexpected"; break;
        }

        if (success)
            DBGLOG("Result of deserializing '%s' to type '%s': %s", buffer, typeStr, resultStr);
        else
            OERRLOG("Result of deserializing '%s' to type '%s': %s", buffer, typeStr, resultStr);
    }
    bool isEmptyOrWhitespace(const char* buffer) const
    {
        skipWhitespace(buffer);
        return (!*buffer);
    }
    void skipWhitespace(const char*& buffer) const
    {
        while (isspace(*buffer)) buffer++;
    }
};

#endif // TOKENSERIALIZATION_HPP
