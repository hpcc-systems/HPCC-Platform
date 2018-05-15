/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.

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
#ifndef ESDL_MONITOR_HPP

#include "esdl_def.hpp"
#include "esdl_transformer.hpp"
#include "esdl_store.hpp"

interface IEsdlShare : implements IInterface
{
    virtual void add(const char* defId, IEsdlDefinition* defobj) = 0;
    virtual void remove(const char* defId) = 0;
    virtual Linked<IEsdlDefinition> lookup(const char* defId) = 0;
};

interface IEsdlMonitor : implements IInterface
{
    virtual void registerBinding(const char* bindingId, IEspRpcBinding* binding) = 0;
    virtual void subscribe() = 0;
    virtual void unsubscribe() = 0;
};

extern "C" esdl_engine_decl void startEsdlMonitor();
extern "C" esdl_engine_decl void stopEsdlMonitor();
esdl_engine_decl IEsdlMonitor* queryEsdlMonitor();
esdl_engine_decl IEsdlShare* queryEsdlShare();

#define ESDL_MONITOR_HPP

#endif //ESDL_MONITOR_HPP
