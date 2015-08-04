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

