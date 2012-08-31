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
