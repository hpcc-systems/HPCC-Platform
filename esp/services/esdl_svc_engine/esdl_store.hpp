/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.

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
#define _ESDL_STORE_HPP__

#include "jlib.hpp"
#include "jstring.hpp"
#include "jptree.hpp"

#ifdef ESDL_ENGINE_EXPORTS
 #define esdl_engine_decl DECL_EXPORT
#else
 #define esdl_engine_decl
#endif

interface IEsdlStore : public IInterface
{
    virtual void fetchDefinition(const char* definitionId, StringBuffer& esxdl) = 0;
    virtual void fetchLatestDefinition(const char* definitionName, StringBuffer& esxdl) = 0;
    virtual IPropertyTree* fetchBinding(const char* espProcess, const char* espStaticBinding) = 0;
    virtual bool definitionExists(const char* definitionId) = 0;
    virtual bool isMethodDefined(const char* definitionId, StringBuffer & esdlServiceName, const char* methodName) = 0;
    virtual bool addDefinition(IPropertyTree* definitionRegistry, const char* definitionName, IPropertyTree* definitionInfo, StringBuffer &newId, unsigned &newSeq, const char* userid, bool deleteprev, StringBuffer & message) = 0;
    virtual int configureMethod(const char* espProcName, const char* espBindingName, const char* definitionId, const char* methodName, IPropertyTree* configTree, bool overwrite, StringBuffer& message) = 0;
    virtual int bindService(const char* bindingName, IPropertyTree* methodsConfig, const char* espProcName, const char* espPort, const char* definitionId,
                       const char* esdlServiceName, StringBuffer& message, bool overwrite, const char* user) = 0;
    virtual bool deleteDefinition(const char* definitionId, StringBuffer& errmsg, StringBuffer* defxml) = 0;
    virtual bool deleteBinding(const char* bindingId, StringBuffer& errmsg, StringBuffer* bindingxml) = 0;
    virtual IPropertyTree* getDefinitionRegistry(bool readonly) = 0;
    virtual IPropertyTree* getBindingTree(const char* espProcName, const char* espBindingName, StringBuffer& msg) = 0;
    virtual IPropertyTree* getDefinitions() = 0;
    virtual IPropertyTree* getBindings() = 0;
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

esdl_engine_decl IEsdlStore* createEsdlCentralStore();
esdl_engine_decl IEsdlSubscription* createEsdlSubscription(IEsdlListener* listener, const char* process, const char* binding);

#endif // _ESDL_STORE_HPP__
