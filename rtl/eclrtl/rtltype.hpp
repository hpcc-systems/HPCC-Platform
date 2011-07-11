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

#ifndef rtltype_incl
#define rtltype_incl
#include "eclrtl.hpp"

ECLRTL_API unsigned rtlGetPascalLength(unsigned len, const void * data);
ECLRTL_API void rtlPascalToString(unsigned & tgtLen, char * & tgt, unsigned srcLen, const void * src);
ECLRTL_API void rtlStringToPascal(unsigned & tgtLen, void * & tgt, unsigned srcLen, const char * src);

#endif
