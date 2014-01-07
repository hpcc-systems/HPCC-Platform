/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

int poor_atomic_dec_and_read(atomic_t * v)
{
    ICrit->enter();
    int ret = --(*v);
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

int poor_atomic_add_and_read(atomic_t * v, int i)
{
    ICrit->enter();
    (*v) += i;
    int ret = (*v);
    ICrit->leave();
    return ret;
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
