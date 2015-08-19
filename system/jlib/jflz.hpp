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



#ifndef JFLZ_INCL
#define JFLZ_INCL

#include "jlzw.hpp"

#define FASTCOMPRESSEDFILEBLOCKSIZE (0x10000)

extern jlib_decl ICompressor *createFastLZCompressor();
extern jlib_decl IExpander *createFastLZExpander();

extern jlib_decl void fastLZCompressToBuffer(MemoryBuffer & out, size32_t len, const void * src);
extern jlib_decl void fastLZDecompressToBuffer(MemoryBuffer & out, const void * src);
extern jlib_decl void fastLZDecompressToBuffer(MemoryBuffer & out, MemoryBuffer & in);
extern jlib_decl void fastLZDecompressToAttr(MemoryAttr & out, const void * src);
extern jlib_decl void fastLZDecompressToBuffer(MemoryAttr & out, MemoryBuffer & in);


// basic routines 

extern jlib_decl size32_t fastlz_compress(const void* input, size32_t inlength, void* output); 
        // returns compressed size
        // output length must be at least inlength+fastlzSlack(inlength)
extern jlib_decl size32_t fastlz_decompress(const void* input, size32_t inlength, void* output, size32_t maxout);

inline size32_t fastlzSlack(size32_t sz) { size32_t ret=sz/16; return (ret<66)?66:ret; }

extern jlib_decl IFileIOStream *createFastLZStreamRead(IFileIO *base);
extern jlib_decl IFileIOStream *createFastLZStreamWrite(IFileIO *base);


#endif
