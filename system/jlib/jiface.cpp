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


#include "jiface.hpp"
#include "jlib.hpp"
#include <assert.h>
#include "jmutex.hpp"

//===========================================================================

void CInterface::beforeDispose()
{
}

//===========================================================================

#if !defined(_WIN32) && !defined(__GNUC__)

static CriticalSection *ICrit;

MODULE_INIT(INIT_PRIORITY_JIFACE)
{
    ICrit = new CriticalSection();
    return true;
}
MODULE_EXIT()
{
//  delete ICrit;  - need to make sure this is deleted after anything that uses it
}

bool poor_atomic_dec_and_test(atomic_t * v)
{
    ICrit->enter();
    bool ret = (--(*v) == 0);
    ICrit->leave();
    return ret;
}

bool poor_atomic_inc_and_test(atomic_t * v)
{
    ICrit->enter();
    bool ret = (++(*v) == 0);
    ICrit->leave();
    return ret;
}

int poor_atomic_xchg(int i, atomic_t * v)
{
    ICrit->enter();
    int prev = (*v);
    (*v)=i;
    ICrit->leave();
    return prev;
}

void poor_atomic_add(atomic_t * v, int i)
{
    ICrit->enter();
    (*v) += i;
    ICrit->leave();
}

int poor_atomic_add_exchange(atomic_t * v, int i)       
{
    ICrit->enter();
    int prev = (*v);
    (*v)=prev+i;
    ICrit->leave();
    return prev;
}

bool poor_atomic_cas(atomic_t * v, int newvalue, int expectedvalue)
{
    ICrit->enter();
    bool ret = false;
    if ((*v)==expectedvalue)
    {
        *v=newvalue;
        ret = true;
    }
    ICrit->leave();
    return ret;
}

void *poor_atomic_xchg_ptr(void *p, void** v)
{
    ICrit->enter();
    void * prev = (*v);
    (*v)=p;
    ICrit->leave();
    return prev;
}

bool poor_atomic_cas_ptr(void ** v, void * newvalue, void * expectedvalue)
{
    ICrit->enter();
    bool ret = false;
    if ((*v)==expectedvalue)
    {
        *v=newvalue;
        ret = true;
    }
    ICrit->leave();
    return ret;
}

//Hopefully the function call will be enough to stop the compiler reordering any operations
void poor_compiler_memory_barrier()
{
}


#endif
