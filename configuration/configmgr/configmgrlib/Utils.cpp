/*##############################################################################

HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.

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

#include <vector>
#include <string>

#include "Utils.hpp"
#include "Exceptions.hpp"

bool getEnclosedString(const std::string &source, std::string &result, std::size_t startPos, char endDelim, bool throwIfError);

std::vector<std::string> splitString(const std::string &input, const std::string &delim)
{
    size_t  start = 0, end = 0, delimLen = delim.length();
    std::vector<std::string> list;

    while (end != std::string::npos)
    {
        end = input.find(delim, start);
        std::string item = input.substr(start, (end == std::string::npos) ? std::string::npos : end - start);
        if (!item.empty())
            list.push_back(item);

        if (end != std::string::npos)
        {
            start = end + delimLen;
            if (start >= input.length())
                end = std::string::npos;
        }
    }
    return list;
}


bool extractEnclosedString(const std::string &source, std::string &result, char startDelim, char endDelim, bool optional)
{
    bool rc = false;
    std::size_t startPos = source.find_first_of(startDelim);
    if (startPos != std::string::npos)
    {
        rc = getEnclosedString(source, result, startPos, endDelim, true);
    }
    else if (!optional)
    {
        std::string msg = "Bad string parse, expected string enclosed in '" + std::string(1, startDelim) + std::string(1, endDelim) + "' at or around: " + source;
        throw(ParseException(msg));
    }
    else
    {
        result = source;
    }
    return rc;
}


// throwIfError allows the caller to ignore an error finding the enclosed string and simply return the input string as the result string.
// If true, the caller expects there to be an ending delimiter and wants an exception since this is an error condition as determined by the caller.
// The return value will always reflect whether the result string was enclosed in the delimiter or not.
bool getEnclosedString(const std::string &source, std::string &result, std::size_t startPos, char endDelim, bool throwIfError)
{
    bool rc = false;
    std::size_t endPos = source.find_first_of(endDelim, startPos+1);
    if (endPos != std::string::npos)
    {
        result = source.substr(startPos+1, endPos-startPos-1);
        rc = true;
    }
    else if (throwIfError)
    {
        std::string delim(1, endDelim);
        std::string msg = "Bad string parse, expectd '" + delim + "' at or around: " + source;
        throw(ParseException(msg));
    }
    else
    {
        result = source;
    }
    return rc;
}


std::string trim(const std::string& str, const std::string& whitespace)
{
    const auto strBegin = str.find_first_not_of(whitespace);
    if (strBegin == std::string::npos)
        return ""; // no content

    const auto strEnd = str.find_last_not_of(whitespace);
    const auto strRange = strEnd - strBegin + 1;
    return str.substr(strBegin, strRange);
}
