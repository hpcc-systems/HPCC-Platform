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

#ifndef JDEN_INCL
#define JDEN_INCL

#include "jlzw.hpp"

#define DENCOMPRESSEDFILEBLOCKSIZE (0x100000)

extern jlib_decl ICompressor *createDENCompressor();
extern jlib_decl IExpander   *createDENExpander();

extern jlib_decl void DENCompressToBuffer(MemoryBuffer & out, size32_t len, const void * src);
extern jlib_decl void DENDecompressToBuffer(MemoryBuffer & out, const void * src);
extern jlib_decl void DENDecompressToBuffer(MemoryBuffer & out, MemoryBuffer & in);
extern jlib_decl void DENDecompressToAttr(MemoryAttr & out, const void * src);
extern jlib_decl void DENDecompressToBuffer(MemoryAttr & out, MemoryBuffer & in);

extern jlib_decl IFileIOStream *createDENStreamRead(IFileIO *base);
extern jlib_decl IFileIOStream *createDENStreamWrite(IFileIO *base);

#endif
