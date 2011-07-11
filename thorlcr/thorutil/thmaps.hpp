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

#ifndef __THMAPS__
#define __THMAPS__

#include "jhash.hpp"
#include "thor.hpp"

#if !defined(thutil_decl)
#define thutil_decl __declspec(dllimport)
#endif

typedef void *voidPtr;
MAKEMAPPING(int, int, voidPtr, voidPtr, IntToVoidPMap);
MAKEMAPPING(activity_id, activity_id, IInterfacePtr, IInterfacePtr, ActIdIIMap);
MAKEMAPPING(IInterfacePtr, IInterfacePtr, voidPtr, voidPtr, IIVPMap);
MAKEMAPPING(IInterfacePtr, IInterfacePtr, IInterfacePtr, IInterfacePtr, IIIIMap);
MAKEMAPPING(IInterfacePtr, IInterfacePtr, CInterfacePtr, CInterfacePtr, IICIMap);
MAKEMAPPING(CInterfacePtr, CInterfacePtr, IInterfacePtr, IInterfacePtr, CIIIMap);
MAKEMAPPING(CInterfacePtr, CInterfacePtr, CInterfacePtr, CInterfacePtr, CICIMap);

// Linked CInterface mapping impl. (should define a macro template etc. for this kind of thing in jhash.ipp)
template <class K, class KI, class LinkedVP>
class thutil_decl LinkedMapBetween  : public MappingBetween<K, KI, LinkedVP, LinkedVP>
{
    typedef LinkedMapBetween<K, KI, LinkedVP> SELF;
public:
    LinkedMapBetween(KI ki, LinkedVP vi) : MappingBetween<K, KI, LinkedVP, LinkedVP>(ki, vi) { }
    ~LinkedMapBetween() { SELF::val->Release(); }
};

#define MAKELINKEDMAPPING(KTYPE, KITYPE, LTYPE, NAME)                                                           \
class NAME##KTYPE##KITYPE##LTYPE : public MappingBetween<KTYPE, KITYPE, LTYPE, LTYPE>               \
{                                                                                                               \
public:                                                                                                         \
    NAME##KTYPE##KITYPE##LTYPE(KITYPE ki, LTYPE vi) : MappingBetween<KTYPE, KITYPE, LTYPE, LTYPE>(ki, vi) { }   \
    ~NAME##KTYPE##KITYPE##LTYPE() { val->Release(); }                                                           \
};                                                                                                              \
class NAME : public MapBetween<KTYPE, KITYPE, LTYPE, LTYPE, NAME##KTYPE##KITYPE##LTYPE> { };

MAKELINKEDMAPPING(activity_id, activity_id, IInterfacePtr, LinkedActIdIIMap);
MAKELINKEDMAPPING(graph_id, graph_id, IInterfacePtr, GraphIdIInterfaceMapping);
MAKELINKEDMAPPING(int, int, IInterfacePtr, IntIInterfaceMapping);
MAKELINKEDMAPPING(int, int, CInterfacePtr, IntCInterfaceMapping);
MAKELINKEDMAPPING(voidPtr, voidPtr, IInterfacePtr, VPIInterfaceMapping);

#endif

