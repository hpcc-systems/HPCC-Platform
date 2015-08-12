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



#ifndef JLZMA_INCL
#define JLZMA_INCL

#include "jlzw.hpp"

extern jlib_decl void LZMACompressToBuffer(MemoryBuffer & out, size32_t len, const void * src);
extern jlib_decl void LZMADecompressToBuffer(MemoryBuffer & out, const void * src);
extern jlib_decl void LZMADecompressToBuffer(MemoryBuffer & out, MemoryBuffer & in);
extern jlib_decl void LZMADecompressToAttr(MemoryAttr & out, const void * src);
extern jlib_decl void LZMALZDecompressToBuffer(MemoryAttr & out, MemoryBuffer & in);


#endif
