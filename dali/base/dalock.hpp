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

#ifndef DALOCK_HPP
#define DALOCK_HPP

#ifndef da_decl
#define da_decl DECL_IMPORT
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


