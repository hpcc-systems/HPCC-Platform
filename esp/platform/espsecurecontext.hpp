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

#ifndef ESPSECURECONTEXT_HPP
#define ESPSECURECONTEXT_HPP

#include "jiface.hpp"
#include "tokenserialization.hpp"

class CTxSummary;

// Declares a protocol-independent interface to give security managers read
// access to protocol-dependent data values. Subclasses determine the types
// of data to which they will provide access. Callers are assumed to know
// which data types they may request.
//
// For example, consider an HTTP request. A caller may need access to HTTP
// cookies. In such a case, the caller may call getProtocol() to confirm that
// the supported protocol is "http" and then may assume that a property type
// of "cookie" will be supported.
interface IEspSecureContext : extends IInterface
{
    // Return a protocol-specific identifier. Callers may use this value to
    // confirm availability of required data types.
    virtual const char* getProtocol() const = 0;

    // Returns the TxSummary object to be used for a request.
    virtual CTxSummary* queryTxSummary() const = 0;

    // Fetches a data value based on a given type and name. If the requested
    // value exists it is stored in the supplied value buffer and true is
    // returned. If the requested value does not exist, the value buffer is
    // unchanged and false is returned.
    //
    // Acceptable values of the 'type' parameter are defined by protocol-
    // specific subclasses. E.g., an HTTP specific subclass might support
    // cookie data.
    //
    // Acceptable values of the 'name' parameter are dependent on the caller.
    virtual bool getProp(int type, const char* name, StringBuffer& value) = 0;

    // Implementation-independent wrapper of the abstract getProp method
    // providing convenient access to non-string values. Non-string means
    // numeric (including Boolean) values by default, but callers do have
    // the option to replace the default deserializer with one capable of
    // supporting more complex types.
    //
    // True is returned if the requested property exists and its value is
    // convertible to TValue. False is returned in all other cases. The
    // caller may request the conversion result code to understand why a
    // request failed.
    //
    // Template Parameters
    // - TValue: a type supported by of TDeserializer
    // - TDefault[TValue]: a type from which TValue can be initialized
    // - TDeserializer[TokenDeserializer]: a callback function, lambda, or
    //       functor with signature:
    //           DeserializationResult (*pfn)(const char*, TValue&)
    template <typename TValue, typename TDefault = TValue, class TDeserializer = TokenDeserializer>
    bool getProp(int type, const char* name, TValue& value, const TDefault dflt = TDefault(), DeserializationResult* deserializationResult = NULL, TDeserializer& deserializer = TDeserializer());
};

template <typename TValue, typename TDefault, class TDeserializer>
inline bool IEspSecureContext::getProp(int type, const char* name, TValue& value, const TDefault dflt, DeserializationResult* deserializationResult, TDeserializer& deserializer)
{
    DeserializationResult result = Deserialization_UNKNOWN;
    StringBuffer prop;
    bool found = getProp(type, name, prop);

    if (found)
    {
        result = deserializer(prop, value);
        found = (Deserialization_SUCCESS == result);
    }

    if (!found)
    {
        value = TValue(dflt);
    }

    if (deserializationResult)
    {
        *deserializationResult = result;
    }

    return found;
}

#endif // ESPSECURECONTEXT_HPP

