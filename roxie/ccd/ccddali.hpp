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

#ifndef _CCDDALI_INCL
#define _CCDDALI_INCL

#include "deftype.hpp"
#include "eclhelper.hpp"
#include "workunit.hpp"
#include "dllserver.hpp"
#include "dadfs.hpp"
#include "daclient.hpp"

extern void addWuException(IConstWorkUnit *workUnit, IException *E);

interface IDaliPackageWatcher : extends IInterface
{
    virtual void subscribe(bool exact) = 0;
    virtual void unsubscribe() = 0;
    virtual const char *queryName() const = 0;
    virtual void onReconnect() = 0;
};

interface IRoxieDaliHelper : extends IInterface
{
    virtual void commitCache() = 0;
    virtual bool connected() const = 0;
    virtual IFileDescriptor *checkClonedFromRemote(const char *id, IFileDescriptor *fdesc, bool cacheIt) = 0;
    virtual IDistributedFile *resolveLFN(const char *filename, bool cacheIt, bool writeAccess) = 0;
    virtual IFileDescriptor *resolveCachedLFN(const char *filename) = 0;
    virtual IConstWorkUnit *attachWorkunit(const char *wuid, ILoadedDllEntry *source) = 0;
    virtual IPropertyTree *getQuerySet(const char *id) = 0;
    virtual IDaliPackageWatcher *getQuerySetSubscription(const char *id, ISDSSubscription *notifier) = 0;
    virtual IPropertyTree *getPackageSets() = 0;
    virtual IDaliPackageWatcher *getPackageSetsSubscription(ISDSSubscription *notifier) = 0;
    virtual IDaliPackageWatcher *getPackageMapsSubscription(ISDSSubscription *notifier) = 0;
    virtual IPropertyTree *getPackageMap(const char *id) = 0;
    virtual IDaliPackageWatcher *getSuperFileSubscription(const char *lfn, ISDSSubscription *notifier) = 0;
    virtual void releaseSubscription(IDaliPackageWatcher *subscription) = 0;
    virtual bool connect(unsigned timeout) = 0;
    virtual void disconnect() = 0;
    virtual void noteQueuesRunning(const char *queueNames) = 0;
    virtual void noteWorkunitRunning(const char *wu, bool running) = 0;
};


extern IRoxieDaliHelper *connectToDali(unsigned waitToConnect=0);
extern void releaseRoxieStateCache();
extern IDllServer &queryRoxieDllServer();

#endif
