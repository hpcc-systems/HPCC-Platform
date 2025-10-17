/*##############################################################################

    Copyright (C) 2025 HPCC Systems®.

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

#pragma once

#include <cmath>
#include <limits>

// Flags for strToBytes behavior control
enum class StrToBytesFlags : unsigned
{
    None = 0,
    ThrowOnError = 1,           // Throw exceptions on error (default: return max value)
    UseBinaryUnits = 2,         // Use 1024-based units for "K"/"KB", "M"/"MB", etc.

    // Convenience combinations
    Default = UseBinaryUnits,                           // Default HPCC behavior
    ThrowBinary = ThrowOnError | UseBinaryUnits,       // Legacy behavior
    LegacyCompatible = ThrowOnError | UseBinaryUnits   // Full backward compatibility
};

inline StrToBytesFlags operator|(StrToBytesFlags a, StrToBytesFlags b)
{
    return static_cast<StrToBytesFlags>(static_cast<unsigned>(a) | static_cast<unsigned>(b));
}

inline bool operator&(StrToBytesFlags a, StrToBytesFlags b)
{
    return (static_cast<unsigned>(a) & static_cast<unsigned>(b)) != 0;
}

#define handleError(message) \
    { \
        if (throwOnError) \
            throw makeStringException(0, message); \
        return errorValue; \
    }

#define handleErrorV(format, arg) \
    { \
        if (throwOnError) \
            throw makeStringExceptionV(0, format, arg); \
        return errorValue; \
    }

/**
 * Converts a size string to bytes with support for various units and storage types.
 * Supports both binary (1024-based) and decimal (1000-based) unit systems.
 *
 * @tparam SizeType The numeric type to store the result (e.g., size_t, uint64_t, uint32_t)
 * @param str Input string like "100MB", "2.5GB", "1024", etc.
 * @param flags Bitmask controlling behavior (error handling, unit interpretation)
 * @return Converted size in bytes, or max SizeType value on error (if ThrowOnError not set)
 * @throws makeStringException on invalid input, negative values, or overflow (if ThrowOnError set)
 */
template<typename SizeType = size_t>
SizeType strToBytes(const char* str, StrToBytesFlags flags = StrToBytesFlags::Default)
{
    constexpr SizeType errorValue = std::numeric_limits<SizeType>::max();
    bool throwOnError = flags & StrToBytesFlags::ThrowOnError;

    if (!str || !*str)
        handleError("Empty size string");

    // Parse the numeric part
    char* endPtr = nullptr;
    double value = strtod(str, &endPtr);

    if (endPtr == str)
        handleErrorV("Invalid size string: %s", str);

    if (value < 0)
        handleErrorV("Negative size not allowed: %s", str);

    // Skip whitespace after number
    while (*endPtr && isspace(*endPtr))
        endPtr++;

    // Pre-calculated multipliers for efficiency (avoids runtime calculations)
    // Index: 0=KB, 1=MB, 2=GB, 3=TB, 4=PB, 5=EB, 6=ZB (Bytes uses default multiplier value of 1)
    static constexpr size_t binaryMultipliers[] = {
        1024ULL,                                    // KB
        1024ULL * 1024,                            // MB
        1024ULL * 1024 * 1024,                     // GB
        1024ULL * 1024 * 1024 * 1024,              // TB
        1024ULL * 1024 * 1024 * 1024 * 1024,       // PB
        1024ULL * 1024 * 1024 * 1024 * 1024 * 1024, // EB
        1024ULL * 1024 * 1024 * 1024 * 1024 * 1024 * 1024 // ZB
    };
    static constexpr size_t decimalMultipliers[] = {
        1000ULL,                                    // KB
        1000ULL * 1000,                            // MB
        1000ULL * 1000 * 1000,                     // GB
        1000ULL * 1000 * 1000 * 1000,              // TB
        1000ULL * 1000 * 1000 * 1000 * 1000,       // PB
        1000ULL * 1000 * 1000 * 1000 * 1000 * 1000, // EB
        1000ULL * 1000 * 1000 * 1000 * 1000 * 1000 * 1000 // ZB
    };
    bool useBinaryUnits = flags & StrToBytesFlags::UseBinaryUnits;
    size_t multiplier = 1;

    // Helper function to check for whitespace only
    // Uses error handling based on flags
    auto ensureOnlyWhitespace = [&](const char* ptr) -> bool {
        while (*ptr && isspace(*ptr)) ptr++;
        if (*ptr != '\0') {
            if (throwOnError)
                throw makeStringExceptionV(0, "Unknown size unit: %s", endPtr);
            return false;
        }
        return true;
    };

    // Helper function to parse unit suffix and return appropriate multiplier
    // Supports 'B'/'iB' suffix, treats "K" and "KB" as synonyms
    // Returns 0 on error if exceptions disabled
    auto getUnitMultiplier = [&](const char* ptr, size_t unitIndex) -> size_t {
        bool explicitBinary = false;  // Found 'i' suffix (KiB, MiB, etc.)

        if (toupper(*ptr) == 'I') {
            explicitBinary = true;
            ptr++; // Skip the 'i'
        }
        if (toupper(*ptr) == 'B') {
            ptr++; // Skip the 'B' (treat "K" and "KB" as synonyms)
        }

        if (!ensureOnlyWhitespace(ptr))
            return 0; // Error already handled by ensureOnlyWhitespace

        // Determine which multiplier set to use
        bool shouldUseBinary = explicitBinary || useBinaryUnits;

        const size_t* unitMultipliers = shouldUseBinary ? binaryMultipliers : decimalMultipliers;
        return unitMultipliers[unitIndex];
    };

    // Parse unit - supports single char (K, M, G, T, P, E, Z, B) and short forms (KB, MB, GB, TB, PB, EB, ZB)
    switch (toupper(*endPtr))
    {
    case 'B':
        // Single 'B' or 'b' with optional trailing whitespace (no double B like other units)
        if (!ensureOnlyWhitespace(endPtr + 1))
            return errorValue;
        // Fall through to shared bytes validation
    case '\0':
        // No unit specified - assume bytes
        if (value != floor(value)) {
            handleErrorV("Decimal byte counts not allowed: %s", str);
        }
        // multiplier already = 1 (default)
        break;
    case 'K':
        // 'K', 'KB' (decimal), or 'KiB' (binary) with optional trailing whitespace
        multiplier = getUnitMultiplier(endPtr + 1, 0);
        if (multiplier == 0) return errorValue;
        break;
    case 'M':
        // 'M', 'MB' (decimal), or 'MiB' (binary) with optional trailing whitespace
        multiplier = getUnitMultiplier(endPtr + 1, 1);
        if (multiplier == 0) return errorValue;
        break;
    case 'G':
        // 'G', 'GB' (decimal), or 'GiB' (binary) with optional trailing whitespace
        multiplier = getUnitMultiplier(endPtr + 1, 2);
        if (multiplier == 0) return errorValue;
        break;
    case 'T':
        // 'T', 'TB' (decimal), or 'TiB' (binary) with optional trailing whitespace
        multiplier = getUnitMultiplier(endPtr + 1, 3);
        if (multiplier == 0) return errorValue;
        break;
    case 'P':
        // 'P', 'PB' (decimal), or 'PiB' (binary) with optional trailing whitespace
        multiplier = getUnitMultiplier(endPtr + 1, 4);
        if (multiplier == 0) return errorValue;
        break;
    case 'E':
        // 'E', 'EB' (decimal), or 'EiB' (binary) with optional trailing whitespace
        multiplier = getUnitMultiplier(endPtr + 1, 5);
        if (multiplier == 0) return errorValue;
        break;
    case 'Z':
        // 'Z', 'ZB' (decimal), or 'ZiB' (binary) with optional trailing whitespace
        multiplier = getUnitMultiplier(endPtr + 1, 6);
        if (multiplier == 0) return errorValue;
        break;
    default:
        handleErrorV("Unknown size unit: %s", endPtr);
    }

    // Calculate result and check for overflow
    double result = value * multiplier;

    // Check for overflow against the template type's limits
    constexpr double maxValue = static_cast<double>(std::numeric_limits<SizeType>::max());
    if (result > maxValue)
        handleErrorV("Size too large for storage type: %s", str);

    return static_cast<SizeType>(result);
}
