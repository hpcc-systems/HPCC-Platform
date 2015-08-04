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
#include "jstream.ipp"
#include "jstring.hpp"
#include "jexcept.hpp"
#include "jlog.hpp"
#include <assert.h>
#include <stdio.h>
#ifdef _WIN32
#include <io.h>
#endif

CByteInputStream::CByteInputStream()
{
    eofseen = false;
    pushedback = false;
    ungot = 0;
}

bool CByteInputStream::eof()
{
    try
    {
        unget(readByte());
    }
    catch (IException *c)
    {
        c->Release();
        return true;
    }
    return eofseen;
}

int CByteInputStream::readByte()
{
    if (pushedback)
    {
        pushedback = false;
        return ungot;
    }
    if (eofseen)
        return -1;
    int ret = readNext();
    if (ret==-1)
        eofseen = true;
    return ret;
}

int CByteInputStream::readBytes(void *vbuf, int size)
{
    int read = 0;
    if (eofseen)
        return 0;
    char *buf = (char *) vbuf;
    while (read < size)
    {
        char c = readByte();
        if (eofseen)
            return read;
        buf[read] = c;
        read++;
    }
    return read;
}

void CByteInputStream::unget(int c)
{
    assertex(!pushedback);
    pushedback = true;
    ungot = c;
}

//================================================================================

CStringBufferInputStream::CStringBufferInputStream(const char *s) : pos(0), str(s)
{
}

int CStringBufferInputStream::readNext()
{
    if (pos < str.length())
        return str.charAt(pos++);
    else
        return -1;
}

extern jlib_decl IByteInputStream *createInputStream(const char *string)
{
    return new CStringBufferInputStream(string);
}

//================================================================================

CUserStringBufferInputStream::CUserStringBufferInputStream(StringBuffer &s) : pos(0), str(s)
{
}

int CUserStringBufferInputStream::readNext()
{
    if (pos < str.length())
        return str.charAt(pos++);
    else
        return -1;
}

extern jlib_decl IByteInputStream *createInputStream(StringBuffer &from)
{
    return new CUserStringBufferInputStream(from);
}

//================================================================================
void CStringBufferOutputStream::writeByte(byte b)
{
    out.append((char)b);
}

void CStringBufferOutputStream::writeBytes(const void * data, int len)
{
    out.append(len, (const char *)data);
}

extern jlib_decl IByteOutputStream *createOutputStream(StringBuffer &to)
{
    return new CStringBufferOutputStream(to);
}
//================================================================================

CSocketInputStream::CSocketInputStream(ISocket *s)
{
    sock = s;
    s->Link();
    remaining = 0;
    next = 0;
}

CSocketInputStream::~CSocketInputStream()
{
    sock->Release();
}

void CSocketInputStream::refill()
{
    if (!eofseen)
    {
        next = 0;
        sock->read(buffer, 1, sizeof(buffer), remaining);
        if (remaining <= 0)
        {
            remaining = 0;
            eofseen = true;
        }
    }
}

int CSocketInputStream::readNext()
{
    if (!remaining)
        refill();
    if (eofseen)
        return -1;
    else
    {
        int ret = buffer[next];
        assertex(ret != -1);
        next++;
        remaining--;
        return ret;
    }
}

//===========================================================================

CSocketOutputStream::CSocketOutputStream(ISocket *s)
{
    sock = s;
    s->Link();
}

CSocketOutputStream::~CSocketOutputStream()
{
    this->sock->Release();
}

void CSocketOutputStream::writeByte(byte b)
{
    this->sock->write((const char *) &b, 1);
}

void CSocketOutputStream::writeBytes(const void *b, int len)
{
    if (len)
        this->sock->write((const char *) b, len);
}

//===========================================================================

CFileOutputStream::CFileOutputStream(int _handle)
{
    handle = _handle;
}

void CFileOutputStream::writeByte(byte b)
{
    if (_write(handle, &b, 1) != 1)
        throw MakeStringException(-1, "Error while writing byte 0x%x\n", (unsigned)b);
}

void CFileOutputStream::writeBytes(const void *b, int len)
{
    ssize_t written = _write(handle, b, len);
    if (written < 0)
        throw MakeStringException(-1, "Error while writing %d bytes\n", len);
    if (written != len)
        throw MakeStringException(-1, "Truncated (%d) while writing %d bytes\n", (int)written, len);
}

extern jlib_decl IByteOutputStream *createOutputStream(int handle)
{
    return new CFileOutputStream(handle);
}

//===========================================================================

CFileInputStream::CFileInputStream(int _handle)
{
    handle = _handle;
    remaining = 0;
}

void CFileInputStream::refill()
{
    if (!eofseen)
    {
        next = 0;
        remaining = _read(handle, buffer, sizeof(buffer));
        if(remaining == 0)
            eofseen = true;
        else if (remaining < 0)
        {
            remaining = 0;
            perror("error reading:");
            eofseen = true;
        }
    }
}

int CFileInputStream::readNext()
{
    if (!remaining)
        refill();
    if (eofseen)
        return -1;
    else
    {
        int ret = buffer[next];
        assertex(ret != -1);
        next++;
        remaining--;
        return ret;
    }
}

IByteInputStream *createInputStream(int handle)
{
    return new CFileInputStream(handle);
}

