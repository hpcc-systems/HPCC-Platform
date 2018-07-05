/*##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
##############################################################################
 */

#ifndef ECLHELPER_DYN_HPP
#define ECLHELPER_DYN_HPP

#include "jptree.hpp"
#include "eclrtl.hpp"
#include "eclhelper.hpp"

extern ECLRTL_API IOutputMetaData *createTypeInfoOutputMetaData(MemoryBuffer &mb, bool isGroupedPersist);
extern ECLRTL_API IOutputMetaData *createTypeInfoOutputMetaData(IPropertyTree &jsonTree, bool isGroupedPersist);
extern ECLRTL_API IOutputMetaData *createTypeInfoOutputMetaData(const char *json, bool isGroupedPersist);

interface IDynamicIndexReadArg
{
    virtual void addFilter(const char *filter) = 0;
};

extern ECLRTL_API IHThorDiskReadArg *createDiskReadArg(const char *fileName, IOutputMetaData *in, IOutputMetaData *projected, IOutputMetaData *out, unsigned __int64 chooseN, unsigned __int64 skipN, unsigned __int64 rowLimit);
extern ECLRTL_API IHThorIndexReadArg *createIndexReadArg(const char *fileName, IOutputMetaData *in, IOutputMetaData *projecte, IOutputMetaData *out, unsigned __int64 chooseN, unsigned __int64 skipN, unsigned __int64 rowLimit, IVirtualFieldCallback &callback);
extern ECLRTL_API IEclProcess* createDynamicEclProcess();

#endif
