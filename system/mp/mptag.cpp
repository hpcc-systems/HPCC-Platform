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

#define mp_decl __declspec(dllexport)

#include "mptag.hpp"
#include "jmutex.hpp"
#include <jhash.hpp> 
#include <jarray.hpp> 
#include "jexcept.hpp"

//for later reincluding mptag.hpp so default tags loaded into table
#undef DEFTAG
#undef DEFTAGRANGE
#undef _DEFTAG
#undef TAGENUM     
#undef TAGENUMEND  
#undef DEFSTDTAG 
#define DEFSTDTAG(t, v)
#define TAGENUM     
#define TAGENUMEND  
#define DEFTAG(x)   associateMPtag(x,#x,0); //tag,tracename,traceid
#define DEFTAGRANGE(x,r)    associateMPtag(x,#x,0); //tag,tracename,traceid     // MORETBD


typedef UnsignedArray FreedTagsArray;

class CMPtagAllocator: public CInterface, implements IMPtagAllocator
{
    CriticalSection crit;
    FreedTagsArray freedtags;
    unsigned counter;
    unsigned max;
public:
    IMPLEMENT_IINTERFACE;
    CMPtagAllocator(mptag_t base,unsigned count)
    {
        counter = (unsigned)base;
        max = counter+count-1;
    }
    mptag_t alloc()
    {
        CriticalBlock block(crit);
        if (freedtags.length()>0)
            return (mptag_t)(freedtags.pop());
        if(counter>max)
            throw MakeStringException(MSGAUD_operator,-1,"Out of message tags");
        mptag_t t = (mptag_t)counter;
        counter++;
        return t;
    }
    void release(mptag_t tag)
    {
        CriticalBlock block(crit);
        freedtags.append(tag);
    }

};


IMPtagAllocator *createMPtagRangeAllocator(mptag_t base,unsigned count)
{
    return new CMPtagAllocator(base,count);

}
