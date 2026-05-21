/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2026 HPCC Systems®.

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

#include "ldapsanitization.hpp"

#include <cctype>
#include <cstring>

static void appendHexEscape(StringBuffer &output, unsigned char value)
{
    output.append('\\').appendhex(value, true);
}

void appendEscapedLdapFilter(const char *input, StringBuffer &output)
{
    if (!input)
        return;

    appendEscapedLdapFilter(strlen(input), input, output);
}

void appendEscapedLdapFilter(size_t inputLength, const char *input, StringBuffer &output)
{
    if (!inputLength)
        return;

    assertex(input);

    for (size_t index = 0; index < inputLength; ++index)
    {
        unsigned char next = static_cast<unsigned char>(input[index]);
        switch (next)
        {
            case '*':
            case '(':
            case ')':
            case '\\':
            case '\0':
                appendHexEscape(output, next);
                break;
            default:
                // Conservatively hex-escape all non-ASCII bytes (> 0x7F) to keep the output
                // safe for LDAP filters without attempting UTF-8 sequence validation.
                if (next > 0x7F)
                    appendHexEscape(output, next);
                else
                    output.append(static_cast<char>(next));
                break;
        }
    }
}

void escapeLdapDistinguishedName(const char *input, StringBuffer &output)
{
    if (!input)
        return;

    escapeLdapDistinguishedName(strlen(input), input, output);
}

void escapeLdapDistinguishedName(size_t inputLength, const char *input, StringBuffer &output)
{
    if (!input || !inputLength)
        return;

    // Pre-compute leading and trailing space run lengths so that every space
    // in those runs is escaped, not just the outermost one (RFC 4514).
    size_t leadingSpaces = 0;
    while (leadingSpaces < inputLength && input[leadingSpaces] == ' ')
        ++leadingSpaces;

    size_t trailingSpaces = 0;
    while (trailingSpaces < inputLength && input[inputLength - 1 - trailingSpaces] == ' ')
        ++trailingSpaces;

    for (size_t index = 0; index < inputLength; ++index)
    {
        unsigned char next = static_cast<unsigned char>(input[index]);
        bool inLeadingRun = (index < leadingSpaces);
        bool inTrailingRun = (index >= inputLength - trailingSpaces);

        switch (next)
        {
            case '\0':
                appendHexEscape(output, next);
                break;
            case ' ':
                if (inLeadingRun || inTrailingRun)
                    output.append('\\');
                output.append(' ');
                break;
            case '#':
                if (index == 0)
                    output.append('\\');
                output.append('#');
                break;
            // RFC 4514 §2.4 permits '=' unescaped in attribute values (stringchar).
            case '"':
            case '+':
            case ',':
            case ';':
            case '<':
            case '>':
            case '\\':
                output.append('\\');
                output.append(static_cast<char>(next));
                break;
            default:
                output.append(static_cast<char>(next));
                break;
        }
    }
}

bool validateLdapUsername(const char *username)
{
    if (!username || !*username)
        return false;

    for (const unsigned char *cursor = reinterpret_cast<const unsigned char *>(username); *cursor; ++cursor)
    {
        // Keep locale-sensitive alnum validation for compatibility. A strict
        // ASCII allowlist would be more deterministic, but it could reject
        // existing users whose usernames rely on locale-specific characters.
        if (std::isalnum(*cursor) || strchr("._-@\\/", *cursor))
            continue;

        return false;
    }

    return true;
}
