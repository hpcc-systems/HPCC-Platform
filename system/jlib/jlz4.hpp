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

#define LZ4COMPRESSEDFILEBLOCKSIZE (0x100000)

extern jlib_decl ICompressor *createLZ4Compressor(const char * options, bool hc=false);
extern jlib_decl IExpander   *createLZ4Expander();
extern jlib_decl ICompressor *createLZ4StreamCompressor(const char * options, bool hc=false);
extern jlib_decl IExpander   *createLZ4StreamExpander();

extern jlib_decl ICompressor *createZStdStreamCompressor(const char * options);
extern jlib_decl IExpander   *createZStdStreamExpander();

#endif
