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

class jlib_decl CStringBufferInputStream : public CByteInputStream, public CInterface
{
private:
    size32_t pos;
    StringBuffer str;
public:
    IMPLEMENT_IINTERFACE;

    CStringBufferInputStream(const char *);

    virtual int readNext();
};

class jlib_decl CUserStringBufferInputStream : public CByteInputStream, public CInterface
{
private:
    size32_t pos;
    StringBuffer &str;
public:
    IMPLEMENT_IINTERFACE;

    CUserStringBufferInputStream(StringBuffer &);

    virtual int readNext();
};

class jlib_decl CFileInputStream : public CByteInputStream, public CInterface
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

class jlib_decl CSocketInputStream : public CByteInputStream, public CInterface
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

class jlib_decl CSocketOutputStream : public IByteOutputStream, public CInterface
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

class jlib_decl CFileOutputStream : public IByteOutputStream, public CInterface
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

class jlib_decl CStringBufferOutputStream : public IByteOutputStream, public CInterface
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
