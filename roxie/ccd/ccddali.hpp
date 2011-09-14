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

#ifndef _CCDDALI_INCL
#define _CCDDALI_INCL

#include "deftype.hpp"
#include "eclhelper.hpp"
#include "workunit.hpp"
#include "dllserver.hpp"
#include "dadfs.hpp"
#include "daclient.hpp"

// MORE - this is duplicated here and eclagent - move into workunit ?

struct WorkunitUpdate : public Owned<IWorkUnit>
{
public:
    WorkunitUpdate(IWorkUnit *wu) : Owned<IWorkUnit>(wu) { }
    ~WorkunitUpdate() { if (get()) get()->commit(); }
};

extern void addWuException(IConstWorkUnit *workUnit, IException *E);

interface IQuerySetWatcher : extends IInterface
{
    virtual void unsubscribe() = 0;
};

interface IRoxieDaliHelper : extends IInterface
{
    virtual bool connected() const = 0;
    virtual IDistributedFile *resolveLFN(const char *filename) = 0;
    virtual IFileDescriptor *resolveCachedLFN(const char *filename) = 0;
    virtual IConstWorkUnit *attachWorkunit(const char *wuid, ILoadedDllEntry *source) = 0;
    virtual IPropertyTree *getQuerySet(const char *id) = 0;
    virtual IQuerySetWatcher *getQuerySetSubscription(const char *id, ISDSSubscription *notifier) = 0;
    virtual IPropertyTree *getPackageSet(const char *id) = 0;
};


extern IRoxieDaliHelper *connectToDali();
extern void releaseRoxieStateCache();
extern IDllServer &queryRoxieDllServer();

#endif
