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



#ifndef JSTREAM_HPP
#define JSTREAM_HPP

#include "jexpdef.hpp"
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
