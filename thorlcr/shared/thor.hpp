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

#ifndef __THOR__
#define __THOR__

#define THOR_VERSION_MAJOR 4
#define THOR_VERSION_MINOR 1

typedef unsigned activity_id;
typedef unsigned graph_id;
#define ACTPF
#define GIDPF

#ifdef __64BIT__
typedef unsigned __int64 rowcount_t;
#define RCPF I64F
#define RCMAX ((rowcount_t)(__int64)-1)
#else
typedef unsigned rowcount_t;
#define RCPF ""
#define RCMAX UINT_MAX
#endif
#define RCUNBOUND RCMAX
#define RCUNSET RCMAX

template <class T>
inline rowcount_t validRC(T X)
{
    assertex(X == (rowcount_t)X);
    return (rowcount_t)X;
}


#if 0
    #define CATCHALL ...
#else
    struct DummyCatchAll{ int i; };
    #define CATCHALL DummyCatchAll
#endif


#endif
