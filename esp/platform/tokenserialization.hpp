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

#include "jstring.hpp"

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

#endif // TOKENSERIALIZATION_HPP
