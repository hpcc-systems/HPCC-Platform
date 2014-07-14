/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

#ifndef rtlread_imp_hpp
#define rtlread_imp_hpp

#include "eclhelper.hpp"
#include "jfile.hpp"
#include "eclrtl.hpp"

//This file references jlib directly, so it should not be #included from generated code
//It is shared with the implementation of eclrtl and the engines.

//This class is designed to be used as the source parameter when deserializing data.
class ECLRTL_API CThorStreamDeserializerSource : implements IRowDeserializerSource
{
public:
    CThorStreamDeserializerSource(ISerialStream * _in = NULL);
    CThorStreamDeserializerSource(size32_t len, const void * data); // a short cut for creating a createMemorySerialStream()

    inline void setStream(ISerialStream *_in) { in.set(_in); }

    virtual const byte * peek(size32_t maxSize);
    virtual offset_t beginNested();
    virtual bool finishedNested(offset_t & len);

    virtual size32_t read(size32_t len, void * ptr);
    virtual size32_t readSize();
    virtual size32_t readPackedInt(void * ptr);
    virtual size32_t readUtf8(ARowBuilder & target, size32_t offset, size32_t fixedSize, size32_t len);
    virtual size32_t readVStr(ARowBuilder & target, size32_t offset, size32_t fixedSize);
    virtual size32_t readVUni(ARowBuilder & target, size32_t offset, size32_t fixedSize);

    //These shouldn't really be called since this class is meant to be used for a deserialize.
    //If we allowed padding/alignment fields in the input then the first function would make sense.
    virtual void skip(size32_t size);
    virtual void skipPackedInt();
    virtual void skipUtf8(size32_t len);
    virtual void skipVStr();
    virtual void skipVUni();

    inline bool eos()
    {
        return in->eos();
    }

    inline offset_t tell()
    {
        return in->tell();
    }

    inline void clearStream()
    {
        in.clear();
    }

    inline void reset(offset_t offset, offset_t flen = (offset_t)-1)
    {
        in->reset(offset, flen);
    }

private:
    inline const byte * doPeek(size32_t minSize, size32_t & available)
    {
        const byte * ret = static_cast<const byte *>(in->peek(minSize, available));
        if (minSize > available) 
            reportReadFail();
        return ret;
    }
    inline void doRead(size32_t len, void * ptr)
    {
        in->get(len, ptr);
//      if (read != len)
//          reportReadFail();
    }
    virtual void reportReadFail();


protected:
    Linked<ISerialStream> in;           // could use a CStreamSerializer class (with inlines to improve)
};


#endif
