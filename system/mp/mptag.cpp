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

#define mp_decl DECL_EXPORT

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

class CMPtagAllocator: implements IMPtagAllocator, public CInterface
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
            return (mptag_t)(freedtags.popGet());
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
