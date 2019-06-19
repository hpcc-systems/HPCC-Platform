/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

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

#ifndef JROWSTREAM_INCL
#define JROWSTREAM_INCL

#include "jiface.hpp"

//The following values are used as values for special rows which are returned in the row stream
enum class SpecialRow : memsize_t
{
    eof = 0,    // end of file
    eog = 1,    // end of group
    eos = 2,    // end of stream
    //more: We could have eogs == eog|eos, but simpler for the callers if they come through separately.
    max
};

//These would be cleaner defined as constexpr, but c++x does not allow reinterpret_cast class in a constexpr
#define eofRow ((const byte * )(memsize_t)SpecialRow::eof)
#define eogRow ((const byte * )(memsize_t)SpecialRow::eog)
#define eosRow ((const byte * )(memsize_t)SpecialRow::eos)

inline bool isSpecialRow(const void * row) { return unlikely((memsize_t)row < (memsize_t)SpecialRow::max); }
inline bool isEndOfFile(const void * row) { return unlikely((memsize_t)row == (memsize_t)SpecialRow::eof); }       // checking row against null is also valid
inline bool isEndOfGroup(const void * row) { return unlikely((memsize_t)row == (memsize_t)SpecialRow::eog); }
inline bool isEndOfStream(const void * row) { return unlikely((memsize_t)row == (memsize_t)SpecialRow::eos); }
inline SpecialRow getSpecialRowType(const void * row) { return (SpecialRow)(memsize_t)row; }


//Base interface for reading a stream of rows
class MemoryBuffer;
interface IRowStreamBase : extends IInterface
{
    virtual bool getCursor(MemoryBuffer & cursor) = 0;
    virtual void setCursor(MemoryBuffer & cursor) = 0;
    virtual void stop() = 0;                              // after stop called NULL is returned
};

//An interface for reading rows from which are not cloned
interface IRawRowStream : extends IRowStreamBase
{
    virtual const void *nextRow(size32_t & size)=0;       // rows returned are only valid until next call.  Size is the number of bytes in the row.

    inline const void *ungroupedNextRow(size32_t & size)  // size will not include the size of the eog
    {
        for (;;)
        {
            const void *ret = nextRow(size);
            if (likely(!isEndOfGroup(ret)))
                return ret;
        }
    }
};

//An interface for reading rows which have been allocated
interface IAllocRowStream : extends IInterface
{
    virtual const void *nextRow()=0;                      // rows returned must be freed

    inline const void *ungroupedNextRow()
    {
        for (;;)
        {
            const void *ret = nextRow();
            if (likely(!isEndOfGroup(ret)))
                return ret;
        }
    }
};


extern jlib_decl IRawRowStream * queryNullRawRowStream();
extern jlib_decl IAllocRowStream * queryNullAllocatedRowStream();
extern jlib_decl IRawRowStream * createNullRawRowStream();

#endif
