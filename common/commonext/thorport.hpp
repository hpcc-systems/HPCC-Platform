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

#ifndef __THORPORT__
#define __THORPORT__

#include "jarray.hpp"
#include "jutil.hpp"
#include "jsocket.hpp"

#ifdef _WIN32
    #ifdef COMMONEXT_EXPORTS
        #define thcommonext_decl __declspec(dllexport)
    #else
        #define thcommonext_decl __declspec(dllimport)
    #endif
#else
    #define thcommonext_decl
#endif

enum ThorPortKind
{
    TPORT_watchdog,
    TPORT_mp
};

thcommonext_decl unsigned short getFixedPort(ThorPortKind category);
thcommonext_decl unsigned short getFixedPort(unsigned short base, ThorPortKind category);
thcommonext_decl unsigned short getExternalFixedPort(unsigned short masterbase, unsigned short machinebase, ThorPortKind category);
thcommonext_decl unsigned short allocPort(unsigned num=1);
thcommonext_decl void           freePort(unsigned short,unsigned num=1);
thcommonext_decl void           setMachinePortBase(unsigned short base);
thcommonext_decl void           setMasterPortBase(unsigned short base);
thcommonext_decl unsigned short         getMasterPortBase();
thcommonext_decl unsigned short         getMachinePortBase();

typedef UnsignedShortArray PortArray;

class CPortGroup
{
public:
    unsigned short allocPort(unsigned n=1)
    {
        unsigned short p=::allocPort(n);
        while (n--)
            portsinuse.append(p+n);
        return p;
    }
    void freePort(unsigned short p,unsigned n=1)
    {
        unsigned i;
        for (i=0;i<n;i++)
            portsinuse.zap(p+i);
        ::freePort(p,n);
    }
    virtual ~CPortGroup()
    {
        ForEachItemIn(i,portsinuse) {
            freePort(portsinuse.item(i));
        }
    }
protected:
    PortArray portsinuse;
};


#endif
