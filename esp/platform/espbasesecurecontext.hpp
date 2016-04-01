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

#ifndef ESPBASESECURECONTEXT_HPP
#define ESPBASESECURECONTEXT_HPP

#include "espsecurecontext.hpp"
#include "jexcept.hpp"
#include "esp.hpp"

// Concrete implementation of IEspSecureContext interface that is abstract, with
// subclasses required to provide access to key protocol-independent data.
// Handles protocol-independent requests, such as accessing the TxSummary
// instance. Requests for protocol-dependent data raise exceptions.
class CEspBaseSecureContext : extends CInterface, implements IEspSecureContext
{
public:
    IMPLEMENT_IINTERFACE;

    virtual const char* getProtocol() const override { UNIMPLEMENTED; }

    virtual CTxSummary* queryTxSummary() const override { return (queryContext() ? queryContext()->queryTxSummary() : NULL); }

    virtual bool getProp(int type, const char* name, StringBuffer& value) override { UNIMPLEMENTED; }

protected:
    // Abstract method providing internal access to the full ESP context.
    virtual IEspContext* queryContext() const = 0;
};

#endif // ESPBASESECURECONTEXT_HPP
