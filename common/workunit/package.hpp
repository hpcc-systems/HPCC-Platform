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

#ifndef WUPACKAGE_HPP
#define WUPACKAGE_HPP

#include "dadfs.hpp"
#include "workunit.hpp"

interface IHpccPackage : extends IInterface
{
    virtual ISimpleSuperFileEnquiry *resolveSuperFile(const char *superFileName) const = 0;
    virtual bool hasSuperFile(const char *superFileName) const = 0;
};

interface IHpccPackageMap : extends IInterface
{
     virtual const IHpccPackage *queryPackage(const char *name) const = 0;
     virtual const IHpccPackage *matchPackage(const char *name) const = 0;
};

interface IHpccPackageSet : extends IInterface
{
     virtual const IHpccPackageMap *queryActiveMap(const char *queryset) const = 0;
};

extern WORKUNIT_API IHpccPackageSet *createPackageSet(const char *process);

#endif
