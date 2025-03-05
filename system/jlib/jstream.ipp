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
