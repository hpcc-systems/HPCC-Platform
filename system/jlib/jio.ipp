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


#ifndef JIO_IPP
#define JIO_IPP

#include "jiface.hpp"
#include "jexcept.hpp"

#include "jio.hpp"

#define DEFAULT_BUFFER_SIZE     0x10000




class CUnbufferedReadWriteSeq : public CInterface, public IWriteSeq, public IReadSeq
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

class CTeeWriteSeq : public CInterface, public IWriteSeq
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
    IMPLEMENT_IINTERFACE
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
