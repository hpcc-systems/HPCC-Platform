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


#ifndef JIO_IPP
#define JIO_IPP

#include "jiface.hpp"
#include "jexcept.hpp"

#include "jio.hpp"

#define DEFAULT_BUFFER_SIZE     0x10000




class CUnbufferedReadWriteSeq : public IWriteSeq, public IReadSeq, public CInterface
{
private:
    offset_t offset;
    offset_t fpos;
    size32_t size;
    int fh;

public:
    IMPLEMENT_IINTERFACE;

    CUnbufferedReadWriteSeq(int _fh, offset_t offset, size32_t _size);

    virtual void put(const void *src);
    virtual void putn(const void *src, unsigned n);
    virtual void flush();
    virtual size32_t getRecordSize() { return size; }
    virtual offset_t getPosition();

    virtual bool get(void *dst);
    virtual unsigned getn(void *dst, unsigned n);
    virtual void reset();
    virtual void stop() {} // no action required
};

class CTeeWriteSeq : public IWriteSeq, public CInterface
{
protected:
    IWriteSeq *w1;
    IWriteSeq *w2;

public:
    IMPLEMENT_IINTERFACE;

    CTeeWriteSeq(IWriteSeq *f1, IWriteSeq *f2);
    ~CTeeWriteSeq();

    virtual void put(const void *src);
    virtual void putn(const void *src, unsigned n);
    virtual void flush();
    virtual size32_t getRecordSize();
    virtual offset_t getPosition();
};



class CBufferedIOStreamBase: public CInterface
{
public:
    CBufferedIOStreamBase(unsigned _bufferSize);


protected:
    inline size32_t  bytesRemaining() { return numInBuffer + curBufferOffset; }
    inline void writeToBuffer(size32_t len, const void * data)
    {
        memcpy(buffer + curBufferOffset, data, len);
        curBufferOffset += len;
        if (curBufferOffset > numInBuffer)
            numInBuffer = curBufferOffset;
    }
    inline size32_t readFromBuffer(size32_t len, void * data) 
    {
        size32_t sizeGot = numInBuffer - curBufferOffset;
        if (sizeGot > len)
            sizeGot = len;
        memcpy(data, buffer+curBufferOffset, sizeGot);
        curBufferOffset += sizeGot;
        return sizeGot;
    }

    virtual bool fillBuffer()=0;
    virtual size32_t directRead(size32_t len, void * data)=0;
    virtual size32_t directWrite(size32_t len, const void * data)=0;
    virtual void doflush()=0;
    size32_t doread(size32_t len, void * data);
    size32_t dowrite(size32_t len, const void * data);


protected:
    byte *                  buffer;
    size32_t                bufferSize;
    size32_t                minDirectSize;
    size32_t                numInBuffer;
    size32_t                curBufferOffset;
    bool                    reading;
};

extern size32_t chunked_fwrite(void *buffer, size32_t count, FILE *stream);



#endif
