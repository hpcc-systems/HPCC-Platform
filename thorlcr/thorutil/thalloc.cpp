/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

#include "platform.h"

#include "jlib.hpp"
#include "jlog.hpp"
#include "jmalloc.hpp"
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


interface ICRCException : extends IException
{
};

class CCRCException : public CSimpleInterface, implements ICRCException
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
    // JCSMORE
    if (!serializer)
        return 100;
    CSizingSerializer ssz;
    serializer->serialize(ssz, (const byte *)row);
    return ssz.size();
}
