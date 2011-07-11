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

#ifndef _THROLLING_HPP
#define _THROLLING_HPP

#ifdef _WIN32
    #ifdef GRAPH_EXPORTS
        #define graph_decl __declspec(dllexport)
    #else
        #define graph_decl __declspec(dllimport)
    #endif
#else
    #define graph_decl
#endif

#include "jmutex.hpp"

class graph_decl RollingArray
{
public:
    RollingArray(unsigned _size=0);
    ~RollingArray();
    bool add(void *item);
    bool get(void * &item);
    unsigned entries();
public: // data
    static const unsigned defaultSize;
private:
    void **arr;
    unsigned size, count, nip, nop;
    CriticalSection crit;
};

#endif

