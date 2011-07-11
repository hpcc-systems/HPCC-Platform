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



#ifndef JFLZ_INCL
#define JFLZ_INCL

#include "jlzw.hpp"


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
