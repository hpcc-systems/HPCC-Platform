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


#ifndef JSTREAM_IPP
#define JSTREAM_IPP

#include "jsocket.hpp"
#include "jstream.hpp"
#include "jstring.hpp"
#ifdef _WIN32
#include <winsock2.h>
#endif

// Use this for input streams where you can only see a byte at a time

class jlib_decl CByteInputStream : public IByteInputStream
{
private:
    bool pushedback;
    char ungot;

protected:
    bool eofseen;

public:
    CByteInputStream();

    virtual bool eof();
    virtual int readByte();
    virtual int readBytes(void *buf, int size);
    virtual void unget(int c);

    virtual int readNext() = 0;
};

class jlib_decl CStringBufferInputStream : public CInterface, public CByteInputStream
{
private:
    size32_t pos;
    StringBuffer str;
public:
    IMPLEMENT_IINTERFACE;

    CStringBufferInputStream(const char *);

    virtual int readNext();
};

class jlib_decl CUserStringBufferInputStream : public CInterface, public CByteInputStream
{
private:
    size32_t pos;
    StringBuffer &str;
public:
    IMPLEMENT_IINTERFACE;

    CUserStringBufferInputStream(StringBuffer &);

    virtual int readNext();
};

class jlib_decl CFileInputStream : public CInterface, public CByteInputStream
{
private:
    unsigned char buffer[1024];
    int handle;
    int remaining;
    int next;

    void refill();

public:
    IMPLEMENT_IINTERFACE;

    CFileInputStream(int handle);

    virtual int readNext();
};

class jlib_decl CSocketInputStream : public CInterface, public CByteInputStream
{
private:
    unsigned char buffer[1024];
    ISocket *sock;
    size32_t remaining;
    int next;

    void refill();

public:
    IMPLEMENT_IINTERFACE;

    CSocketInputStream(ISocket *s);
    ~CSocketInputStream();

    virtual int readNext();
};

class jlib_decl CSocketOutputStream : public CInterface, public IByteOutputStream
{
private:
    ISocket *sock;

public:
    IMPLEMENT_IINTERFACE;

    CSocketOutputStream(ISocket *s);
    ~CSocketOutputStream();

    virtual void writeByte(byte b);
    virtual void writeBytes(const void *, int);
    virtual void writeString(const char *str) { writeBytes(str, strlen(str)); }
};

class jlib_decl CFileOutputStream : public CInterface, public IByteOutputStream
{
private:
    int handle;

public:
    IMPLEMENT_IINTERFACE;

    CFileOutputStream(int _handle);

    virtual void writeByte(byte b);
    virtual void writeBytes(const void *, int);
    virtual void writeString(const char *str) { writeBytes(str, strlen(str)); }
};

class jlib_decl CStringBufferOutputStream : public CInterface, public IByteOutputStream
{
private:
    StringBuffer & out;
public:
    IMPLEMENT_IINTERFACE;

    CStringBufferOutputStream(StringBuffer & _out) : out(_out) {}

    virtual void writeByte(byte b);
    virtual void writeBytes(const void *, int);
    virtual void writeString(const char *str) { writeBytes(str, strlen(str)); }
};

#endif
