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



#ifndef JLZMA_INCL
#define JLZMA_INCL

#include "jlzw.hpp"

extern jlib_decl void LZMACompressToBuffer(MemoryBuffer & out, size32_t len, const void * src);
extern jlib_decl void LZMADecompressToBuffer(MemoryBuffer & out, const void * src);
extern jlib_decl void LZMADecompressToBuffer(MemoryBuffer & out, MemoryBuffer & in);
extern jlib_decl void LZMADecompressToAttr(MemoryAttr & out, const void * src);
extern jlib_decl void LZMALZDecompressToBuffer(MemoryAttr & out, MemoryBuffer & in);


#endif
