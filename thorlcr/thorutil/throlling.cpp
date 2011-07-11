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

#include "platform.h"
#include "jlib.hpp"

#include "throlling.hpp"

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/thorlcr/thorutil/throlling.cpp $ $Id: throlling.cpp 62376 2011-02-04 21:59:58Z sort $");

const unsigned RollingArray::defaultSize = 20;

RollingArray::RollingArray(unsigned _size)
{
    if (0 == _size) size = defaultSize;
    else size = _size;
    arr = new void *[size];
    nip = nop = 0;
    count = 0;
}

RollingArray::~RollingArray()
{
    delete [] arr;
}

bool RollingArray::add(void *item)
{
    CriticalBlock b(crit);

    if (count>=size)
        return false;

    arr[nip++] = item;
    if (nip>=size) nip = 0;
    count++;
    return true;
}

bool RollingArray::get(void * &item)
{
    CriticalBlock b(crit);

    if (!count)
        return false;

    item = arr[nop++];
    if (nop>=size) nop = 0;
    count--;
    return true;
}

unsigned RollingArray::entries()
{
    CriticalBlock b(crit);
    return count;
}


