/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

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

#pragma warning (disable : 4786)

#ifndef _ESPSTORESHARE_HPP__
#define _ESPSTORESHARE_HPP__

#include "SecureUser.hpp"
#include "eclwatch_errorlist.hpp"

interface IEspStore : extends IInterface
{
    virtual bool init(const char * name, const char * type, IPropertyTree * cfg) = 0;
    virtual IPropertyTree * getStores(const char * namefilter, const char * ownerfilter, const char * typefilter, ISecUser * user) = 0;
    virtual bool set(const char * storename, const char * thenamespace, const char * key, const char * value, ISecUser * owner, bool global) = 0;
    virtual bool fetchKeySet(StringArray & keyset, const char * storename, const char * ns, ISecUser * user, bool global) = 0;
    virtual bool fetch(const char * storename, const char * ns, const char * key, StringBuffer & value, ISecUser * username, bool global) = 0;
    virtual IPropertyTree * getAllPairs(const char * storename, const char * ns, ISecUser * user, bool global) = 0;
    virtual bool fetchAllNamespaces(StringArray & namespaces, const char * storename, ISecUser * user, bool global) = 0;
    virtual bool deletekey(const char * storename, const char * thenamespace, const char * key, ISecUser * user, bool global) = 0;
    virtual bool deleteNamespace(const char * storename, const char * thenamespace, ISecUser * user, bool global) = 0;
    virtual bool addNamespace(const char * storename, const char * thenamespace, ISecUser * owner, bool global) = 0;
    virtual bool createStore(const char * apptype, const char * storename, const char * description, ISecUser * owner, unsigned int maxvalsize) = 0;
    virtual bool setOfflineMode(bool offline) = 0 ;
    virtual bool fetchKeyProperty(StringBuffer & propval , const char * storename, const char * ns, const char * key, const char * property, ISecUser * username, bool global) = 0;
    virtual IPropertyTree * getAllKeyProperties(const char * storename, const char * ns, const char * key, ISecUser * username, bool global) =0;
};

#endif  //_ESPSTOREBASE_SHARE__
