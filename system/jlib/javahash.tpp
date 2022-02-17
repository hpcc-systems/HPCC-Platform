/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

#ifndef __JAVAHASH_TPP__
#define __JAVAHASH_TPP__

template <class ELEMENT>
void JavaHashTableOf<ELEMENT>::onAdd(void * et)
{
    if(keep) static_cast<ELEMENT *>(et)->Link();
    else static_cast<ELEMENT *>(et)->addObserver(*this);
}

template <class ELEMENT>
void JavaHashTableOf<ELEMENT>::onRemove(void * et)
{
    if(keep) static_cast<ELEMENT *>(et)->Release();
    else static_cast<ELEMENT *>(et)->removeObserver(*this);
}

template <class ELEMENT>
bool JavaHashTableOf<ELEMENT>::addOwn(ELEMENT & donor)
{
    if(this->add(donor))
    {
        donor.Release();
        return true;
    }
    return false;
}

template <class ELEMENT>
bool JavaHashTableOf<ELEMENT>::replaceOwn(ELEMENT & donor)
{
    if(this->replace(donor))
    {
        donor.Release();
        return true;
    }
    return false;
}

template <class ELEMENT>
ELEMENT * JavaHashTableOf<ELEMENT>::findLink(const ELEMENT & findParam) const
{
    ELEMENT * found = SuperHashTableOf<ELEMENT, ELEMENT>::find(&findParam);
    if(found) found->Link();
    return found;
}

template <class ELEMENT>
bool JavaHashTableOf<ELEMENT>::onNotify(INotification & notify)
{
    bool ret = true;
    if (notify.getAction() == NotifyOnDispose)
    {
        ELEMENT * mapping = (ELEMENT *)(notify.querySource());
        ret = this->removeExact(mapping);
        assertex(ret);
    }
    return ret;
}

#if defined(IHASH_DEFINED)&&defined(ICOMPARE_DEFINED)

template <class ELEMENT>
ELEMENT *JavaHashTableOf<ELEMENT>::findCompare(ICompare *icmp,void * (ELEMENT::*getPtr)() const,unsigned hash,const void *val) const
{
    unsigned v;
#ifdef HASHSIZE_POWER2
    v = hash & _SELF::tablemask;
#else
    v = hash % _SELF::tablesize;
#endif
    unsigned vs = v;
    do
    {
    ELEMENT * et = static_cast<ELEMENT *>(_SELF::table[v]);
    if (!et)
        break;
    if(icmp->docompare(val, (et->*getPtr)()) == 0)
    {
        _SELF::setCache(v);
        return et;
    }
    v++;
    if(v == _SELF::tablesize)
        v = 0;
    } while (v != vs);
    return NULL;
}

#endif
#endif