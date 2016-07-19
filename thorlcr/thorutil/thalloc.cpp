/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

#include "platform.h"

#include "jlib.hpp"
#include "jlog.hpp"
#include "jmutex.hpp"
#include "jcrc.hpp"
#include "thexception.hpp"
#include "thorcommon.hpp"
#include "thalloc.hpp"


#ifdef _DEBUG
#define ASSERTEX(c) assertex(c)
#else
#define ASSERTEX(c)
#endif

inline size32_t pow2roundupmin1k(size32_t sz)
{
    assertex(sz<0x80000000);
    size32_t ret = 1024;
    while (ret<sz)
        ret *= 2;
    return ret;
}


class DECL_EXCEPTION CCRCException : public CSimpleInterface, implements IException
{
private:
    const void *ptr;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CCRCException(const void * _ptr) { ptr = _ptr; }

// IThorException
    virtual int errorCode() const { return TE_RowCrc; }

    virtual StringBuffer & errorMessage(StringBuffer & str) const
    { 
        return str.appendf("Row CRC error at address %p",ptr);
    }
    MessageAudience errorAudience() const { return MSGAUD_operator; }   // indicates memory corruption

};


size32_t thorRowMemoryFootprint(IOutputRowSerializer *serializer, const void *row)
{
    if (!row)
        return 0;
    assertex(serializer);
    CSizingSerializer ssz;
    serializer->serialize(ssz, (const byte *)row);
    return ssz.size();
}
