/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems.

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

#ifndef JLZ4_INCL
#define JLZ4_INCL

#include "jlzw.hpp"
#include "lz4.h"

#define LZ4COMPRESSEDFILEBLOCKSIZE (0x10000)

extern jlib_decl ICompressor *createLZ4Compressor();
extern jlib_decl IExpander   *createLZ4Expander();

extern jlib_decl void LZ4CompressToBuffer(MemoryBuffer & out, size32_t len, const void * src);
extern jlib_decl void LZ4DecompressToBuffer(MemoryBuffer & out, const void * src);
extern jlib_decl void LZ4DecompressToBuffer(MemoryBuffer & out, MemoryBuffer & in);
extern jlib_decl void LZ4DecompressToAttr(MemoryAttr & out, const void * src);
extern jlib_decl void LZ4DecompressToBuffer(MemoryAttr & out, MemoryBuffer & in);

extern jlib_decl IFileIOStream *createLZ4StreamRead(IFileIO *base);
extern jlib_decl IFileIOStream *createLZ4StreamWrite(IFileIO *base);

#endif
