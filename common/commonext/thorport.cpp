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

// Thor Port Allocation

#include "platform.h"

#include <stddef.h>
#include <stdlib.h>
#include <assert.h>
#include <stdio.h> 
#include <stdlib.h> 

#include "jlib.hpp"
#include "jdebug.hpp"
#include "jexcept.hpp"
#include "jmutex.hpp"
#include "jset.hpp"
#include "jmisc.hpp"

#include "portlist.h"
#include "thorport.hpp"

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/thorlcr/shared/thorport.cpp $ $Id: thorport.cpp 62376 2011-02-04 21:59:58Z sort $");

#define WATCHDOGINC        1

static CriticalSection *portallocsection;
static IBitSet *portmap;
MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    portallocsection = new CriticalSection;
    portmap = createBitSet();
    portmap->set(THOR_MP_INC, true);
    portmap->set(WATCHDOGINC, true);
    return true;
}
MODULE_EXIT()
{
    portmap->Release();
    delete portallocsection;
}

static unsigned short masterportbase=0;
static unsigned short machineportbase=0;


unsigned short getFixedPort(ThorPortKind category)
{
    return getFixedPort(machineportbase, category);
}

unsigned short getFixedPort(unsigned short machineBase, ThorPortKind category)
{
    return getExternalFixedPort(masterportbase, machineBase, category);
}

unsigned short getExternalFixedPort(unsigned short masterBase, unsigned short machineBase, ThorPortKind category)
{
    if (!masterBase) masterBase = THOR_BASE_PORT;
    if (!machineBase) machineBase = THOR_BASESLAVE_PORT;
    switch (category) {
    case TPORT_watchdog:
        return machineBase+WATCHDOGINC;
    case TPORT_mp:
        return machineBase+THOR_MP_INC; 
    }
    LOG(MCerror,unknownJob,"getFixedPort: Unknown Port Kind!");
    return 0;
}

unsigned short allocPort(unsigned num)
{
    CriticalBlock proc(*portallocsection);
    if (num==0)
        num = 1;
    unsigned sp=0;
    unsigned p;
    loop {
        p = portmap->scan(sp,false);
        unsigned q;
        for (q=p+1;q<p+num;q++) {
            if (portmap->test(q))
                break;
        }
        if (q==p+num) {
            while (q!=p)
                portmap->set(--q);
            break;
        }
        sp=p+1;
    }

    return (unsigned short)(p+machineportbase);
}

void freePort(unsigned short p,unsigned num)
{
    CriticalBlock proc(*portallocsection);
    if (!p)
        return;
    if (!portmap) 
        return;
    if (num==0)
        num = 1;
    while (num--) 
        portmap->set(p-machineportbase+num,false);
}
        
void setMachinePortBase(unsigned short base)
{
    machineportbase = base?base:THOR_BASESLAVE_PORT;
}

void setMasterPortBase(unsigned short base)
{
    masterportbase = base?base:THOR_BASE_PORT;
}

unsigned short getMasterPortBase()
{
    return masterportbase?masterportbase:THOR_BASE_PORT;
}

unsigned short getMachinePortBase()
{
    return machineportbase?machineportbase:THOR_BASESLAVE_PORT;
}
