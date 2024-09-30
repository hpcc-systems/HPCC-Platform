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
#include "jio.hpp"

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
class MemoryBufferBuilder;

//An interface for reading rows - which can request the row in the most efficient way for the caller.
interface ILogicalRowStream : extends IRowStream
{
// Defined in IRowStream, here for documentation:
// Request a row which is owned by the caller, and must be freed once it is finished with.
    virtual const void *nextRow() override =0;
    virtual void stop() override = 0;                              // after stop called NULL is returned

    virtual bool getCursor(MemoryBuffer & cursor) = 0;
    virtual void setCursor(MemoryBuffer & cursor) = 0;

// rows returned are only valid until next call.  Size is the number of bytes in the row.
    virtual const void *nextRow(size32_t & size)=0;

    inline const void *ungroupedNextRow(size32_t & size)  // size will not include the size of the eog
    {
        for (;;)
        {
            const void *ret = nextRow(size);
            if (likely(!isEndOfGroup(ret)))
                return ret;
        }
    }

    virtual const void *nextRow(MemoryBufferBuilder & builder)=0;
    // rows returned are created in the target buffer.  This should be generalized to an ARowBuilder
};
using IDiskRowStream = ILogicalRowStream;  // MORE: Replace these in the code, but alias for now to avoid compile problems


//An interface for writing rows - with separate functions whether or not ownership of the row is being passed
interface ILogicalRowSink : extends IRowWriterEx
{
// Defined in IRowWriterEx, here for documentation:
    virtual void putRow(const void *row) override = 0;  // takes ownership of row.  rename to putOwnedRow?
    virtual void flush() override = 0;
    virtual void noteStopped() override = 0;

    virtual void writeRow(const void *row) = 0;         // does not take ownership of row
};


extern jlib_decl IDiskRowStream * queryNullDiskRowStream();

#endif
