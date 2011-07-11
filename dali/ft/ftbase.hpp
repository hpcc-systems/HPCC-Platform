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

#ifndef FTBASE_HPP
#define FTBASE_HPP

#ifdef _WIN32
#ifdef DALIFT_EXPORTS
#define DALIFT_API __declspec(dllexport)
#else
#define DALIFT_API __declspec(dllimport)
#endif
#else
#define DALIFT_API
#endif

typedef __int32 crc32_t;

bool daftAbortHandler();

#endif
