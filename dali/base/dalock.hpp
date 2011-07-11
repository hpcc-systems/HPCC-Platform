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

#ifndef DALOCK_HPP
#define DALOCK_HPP

#ifndef da_decl
#define da_decl __declspec(dllimport)
#endif

#include "platform.h"
#include "jiface.hpp"

#include "dasess.hpp"


typedef __int64 DistributedLockId;

extern da_decl DistributedLockId createDistributedLockId();
extern da_decl void releaseDistributedLockId(DistributedLockId id);
extern da_decl DistributedLockId lookupDistributedLockId(const char *name); // Named Locks (TBD)

interface IDistributedLock: extends IInterface
{
    virtual bool lock(bool exclusive=true,long timeout=-1) = 0; // returns false if timed out
    virtual void unlock() = 0;
    virtual bool relock(bool exclusive=true,long timeout=-1) = 0; // changes exclusive mode (TBD)
    virtual DistributedLockId getID() = 0;
    virtual SessionId getSession() = 0; // owning session of lock
};

extern da_decl IDistributedLock *createDistributedLock(DistributedLockId id, SessionId sessionid=0);

interface IDaliServer;
extern da_decl IDaliServer *createDaliLockServer(); // called for coven members



#endif


