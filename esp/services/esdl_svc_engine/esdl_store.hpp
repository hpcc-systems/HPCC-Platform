/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems®.

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

#ifndef _ESDL_STORE_HPP__
#define _ESDL_REGISTRY_HPP__
#include "jlib.hpp"
#include "jstring.hpp"
#include "jptree.hpp"

interface IEsdlStore : public IInterface
{
    virtual void fetchDefinition(const char* id, StringBuffer& exsdl) = 0;
    virtual void fetchDefinition(const char* esdlServiceName, unsigned targetVersion, StringBuffer& exsdl) = 0;
    virtual IPropertyTree* fetchBinding(const char* espProcess, const char* bindingName) = 0;
};

enum EsdlNotifyType
{
    BindingUpdate = 1,
    BindingDelete = 2,
    DefinitionUpdate = 3,
};

struct EsdlNotifyData
{
    EsdlNotifyType type;
    StringBuffer id;
    StringBuffer espBinding;
    StringBuffer espProcess;
};

interface IEsdlListener
{
    virtual void onNotify(EsdlNotifyData* data) = 0;
};

interface IEsdlSubscription : public IInterface
{
    virtual void unsubscribe() = 0;
};

IEsdlStore* createEsdlCentralStore();
IEsdlSubscription* createEsdlSubscription(IEsdlListener* listener, const char* process, const char* binding);

#endif // _ESDL_STORE_HPP__
