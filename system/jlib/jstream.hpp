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



#ifndef JSTREAM_HPP
#define JSTREAM_HPP

#include "jiface.hpp"
class StringBuffer;

interface jlib_decl IByteOutputStream : public IInterface
{
    virtual void writeByte(byte b) = 0;
    virtual void writeBytes(const void *, int) = 0;
    virtual void writeString(const char *str) = 0;
};

interface jlib_decl IByteInputStream : public IInterface
{
    virtual bool eof() = 0;
    virtual int readByte() = 0;
    virtual int readBytes(void *buf, int size) = 0;
    virtual void unget(int c) = 0;
};

extern jlib_decl IByteInputStream *createInputStream(const char *string);
extern jlib_decl IByteInputStream *createInputStream(StringBuffer &from);
extern jlib_decl IByteInputStream *createInputStream(int handle);
extern jlib_decl IByteOutputStream *createOutputStream(StringBuffer &to);
extern jlib_decl IByteOutputStream *createOutputStream(int handle);
#endif
