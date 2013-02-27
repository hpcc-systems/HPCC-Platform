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

#ifndef WUPACKAGE_H
#define WUPACKAGE_H

#include "errorlist.h"
#include "dadfs.hpp"
#include "workunit.hpp"

// error codes
#define PACKAGE_TARGET_NOT_FOUND      PACKAGE_ERROR_START
#define PACKAGE_MISSING_ID            PACKAGE_ERROR_START+1
#define PACKAGE_NO_SUBFILES           PACKAGE_ERROR_START+2
#define PACKAGE_NOT_FOUND             PACKAGE_ERROR_START+3


interface IHpccPackage : extends IInterface
{
    virtual ISimpleSuperFileEnquiry *resolveSuperFile(const char *superFileName) const = 0;
    virtual bool hasSuperFile(const char *superFileName) const = 0;
    virtual const char *queryEnv(const char *varname) const = 0;
    virtual bool getEnableFieldTranslation() const = 0;
    virtual const IPropertyTree *queryTree() const = 0;
    virtual hash64_t queryHash() const = 0;
};

interface IHpccPackageMap : extends IInterface
{
    virtual const IHpccPackage *queryPackage(const char *name) const = 0;
    virtual const IHpccPackage *matchPackage(const char *name) const = 0;
    virtual const char *queryPackageId() const = 0;
    virtual bool isActive() const = 0;
    virtual bool validate(StringArray &warn, StringArray &err, StringArray &unmatchedQueries, StringArray &unusedPackages) const = 0;
};

interface IHpccPackageSet : extends IInterface
{
     virtual const IHpccPackageMap *queryActiveMap(const char *queryset) const = 0;
};

extern WORKUNIT_API IHpccPackageMap *createPackageMapFromXml(const char *xml, const char *queryset, const char *id);
extern WORKUNIT_API IHpccPackageMap *createPackageMapFromPtree(IPropertyTree *t, const char *queryset, const char *id);

extern WORKUNIT_API IHpccPackageSet *createPackageSet(const char *process);
extern WORKUNIT_API IPropertyTree * getPackageMapById(const char * id, bool readonly);
extern WORKUNIT_API IPropertyTree * getPackageSetById(const char * id, bool readonly);
extern WORKUNIT_API IPropertyTree * resolvePackageSetRegistry(const char *process, bool readonly);
extern WORKUNIT_API hash64_t pkgHash64Data(size32_t len, const void *buf, hash64_t hval);


#endif
