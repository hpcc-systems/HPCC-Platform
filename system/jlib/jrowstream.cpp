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


#include "platform.h"

#include "jrowstream.hpp"

class NullRawRowStream : public CInterfaceOf<IRawRowStream>
{
    virtual bool getCursor(MemoryBuffer & cursor)
    {
        return true;
    }
    virtual void setCursor(MemoryBuffer & cursor)
    {
    }
    virtual void stop()
    {
    }
    virtual const void *nextRow(size32_t & size)
    {
        size = 0;
        return eofRow;
    }
};
static NullRawRowStream nullRawStream;

IRawRowStream * queryNullRawRowStream()
{
    return &nullRawStream;
}

IRawRowStream * createNullRawRowStream()
{
    return new NullRawRowStream;
}


//---------------------------------------------------------------------------------------------------------------------

class NullAllocRowStream : public CInterfaceOf<IAllocRowStream>
{
    virtual const void *nextRow()
    {
        return eofRow;
    }
};
static NullAllocRowStream nullAllocStream;

IAllocRowStream * queryNullAllocatedRowStream()
{
    return &nullAllocStream;
}
