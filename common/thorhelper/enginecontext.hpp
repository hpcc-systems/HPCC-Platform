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

#ifndef ENGINECONTEXT_HPP
#define ENGINECONTEXT_HPP

#include "jsocket.hpp"
#include "dacoven.hpp"

typedef void (* QueryTermCallback)(const char *queryId);

class TerminationCallbackInfo : public CInterface
{
public:
    TerminationCallbackInfo(QueryTermCallback _callback, const char *_id) : callback(_callback), id(_id) {}
    ~TerminationCallbackInfo()
    {
        callback(id);
    }
protected:
    QueryTermCallback callback;
    StringAttr id;
};

interface IEngineContext
{
    virtual DALI_UID getGlobalUniqueIds(unsigned num, SocketEndpoint *_foreignNode) = 0;
    virtual bool allowDaliAccess() const = 0;
    virtual StringBuffer &getQueryId(StringBuffer &result, bool isShared) const = 0;
    virtual void onTermination(QueryTermCallback callback, const char *key, bool isShared) const = 0;
    virtual void getManifestFiles(const char *type, StringArray &files) const = 0;
    virtual bool allowSashaAccess() const = 0;
};

#endif // ENGINECONTEXT_HPP
