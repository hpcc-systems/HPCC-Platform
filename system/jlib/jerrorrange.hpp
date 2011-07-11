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


#ifndef JERRORRANGE_HPP
#define JERRORRANGE_HPP

//Error ranges used throughout the system...

#define ERR_EXE_FIRST       1     // All .exes can safely use the same range.
#define ERR_EXE_LAST        999   // Codes 1000+ should be used in .dlls.

#define ERR_JLIB_FIRST      1000
#define ERR_JLIB_LAST       1999
#define ERR_FILEVIEW_FIRST  2000
#define ERR_FILEVIEW_LAST   2049
#endif
