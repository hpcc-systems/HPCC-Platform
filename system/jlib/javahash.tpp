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
    if(add(donor))
    {
        donor.Release();
        return true;
    }
    return false;
}

template <class ELEMENT>
bool JavaHashTableOf<ELEMENT>::replaceOwn(ELEMENT & donor)
{
    if(replace(donor))
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
        ret = removeExact(mapping);
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
