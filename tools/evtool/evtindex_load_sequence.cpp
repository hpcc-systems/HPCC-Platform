/*##############################################################################

    Copyright (C) 2026 HPCC Systems®.

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

#include "evtindex_load_sequence.hpp"
#include "eventindex.hpp"
#include "eventindexloadsequence.h"
#include "eventutility.hpp"
#include "jstring.hpp"
#include <cctype>

constexpr __uint64 bareReadSizePageCountThreshold = indexPageSize;

class CEvtIndexLoadSequenceCommand : public TEventConsumingCommand<CIndexLoadSequenceOp>
{
public:
    virtual bool acceptKVOption(const char* key, const char* value) override
    {
        if (streq(key, "read-size"))
        {
            op.setReadSize(parseReadSize(value));
            return true;
        }
        return TEventConsumingCommand<CIndexLoadSequenceOp>::acceptKVOption(key, value);
    }

    virtual unsigned acceptLongOption(const char* key, const char* nextArg) override
    {
        if (streq(key, "read-size"))
        {
            if (!nextArg)
                throw makeStringException(0, "--read-size requires a value");
            op.setReadSize(parseReadSize(nextArg));
            return 2;
        }
        return TEventConsumingCommand<CIndexLoadSequenceOp>::acceptLongOption(key, nextArg);
    }

    virtual const char* getVerboseDescription() const override
    {
        return "Report sequential index leaf-load runs and intervening branch/blob loads.";
    }

    virtual const char* getBriefDescription() const override
    {
        return "analyze sequential index loads";
    }

    virtual void usageSyntax(StringBuffer& helpText) override
    {
        helpText.append("--read-size ( '=' | ' ' ) <pages-or-bytes> [filters] <filename>\n");
    }

    virtual void usageOptions(IBufferedSerialOutputStream& out) override
    {
        TEventConsumingCommand<CIndexLoadSequenceOp>::usageOptions(out);
        constexpr const char* usage = R"!!!(    --read-size ( '=' | ' ' ) <value>
                              Read window. Bare values below one page (%llu
                              bytes) are page counts; values at or above one
                              page are byte counts. Values with a size
                              suffix, such as 32KiB, are always byte counts.
)!!!";
        VStringBuffer usageText(usage, indexPageSize);
        out.put(usageText.length(), usageText.str());
    }

private:
    static __uint64 parseReadSize(const char* value)
    {
        if (!value || !*value)
            throw makeStringException(0, "--read-size requires a value");
        if ('-' == value[0] || '+' == value[0])
            throw makeStringException(0, "--read-size must be an unsigned positive number");

        const char* alpha = value;
        while (*alpha && !std::isalpha(static_cast<unsigned char>(*alpha)))
            ++alpha;

        __uint64 bytes = 0;
        if (*alpha)
            bytes = strToBytes(value, StrToBytesFlags::ThrowOnError);
        else
        {
            char* end = nullptr;
            __uint64 amount = strtoull(value, &end, 10);
            while (*end && std::isspace(static_cast<unsigned char>(*end)))
                ++end;
            if (end == value || *end != '\0' || !amount)
                throw makeStringExceptionV(0, "invalid --read-size '%s': expected a positive integer, optionally with a size suffix (e.g. 32KiB)", value);
            if (amount < bareReadSizePageCountThreshold)
                bytes = amount * indexPageSize;
            else
                bytes = amount;
        }

        if (!bytes || (bytes % indexPageSize) != 0)
            throw makeStringExceptionV(0, "--read-size '%s' must be a positive multiple of %llu bytes (bare values below %llu are page counts, at or above are byte counts)", value, indexPageSize, bareReadSizePageCountThreshold);
        return bytes;
    }
};

IEvToolCommand* createIndexLoadSequenceCommand()
{
    return new CEvtIndexLoadSequenceCommand;
}
