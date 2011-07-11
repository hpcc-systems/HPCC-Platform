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

#ifndef HLZWSTRM_INCL
#define HLZWSTRM_INCL

typedef unsigned short KEYRECSIZE_T;

#include "jbuff.hpp"
#include "jlzw.hpp"
#define USE_RANDROWDIFF true

class KeyCompressor 
{
public:
    KeyCompressor() : comp(NULL) {}
    ~KeyCompressor();
    void open(void *blk,int blksize, bool isVariable, bool rowcompression);
    void openBlob(void *blk,int blksize);
    int writekey(offset_t fPtr,const char *key,unsigned datalength, unsigned __int64 sequence);
    unsigned writeBlob(const char *data, unsigned datalength);
    void *bufptr() { return (comp==NULL)?bufp:comp->bufptr();}
    int buflen() { return (comp==NULL)?bufl:comp->buflen();}
    virtual void close();
    unsigned getCurrentOffset() { return (curOffset+0xf) & 0xfffffff0; }
protected:
    bool isVariable;
    bool isBlob;
    unsigned curOffset;
    void *bufp;
    int bufl;
    void testwrite(const void *p,size32_t s);
    ICompressor *comp;
};

#endif

