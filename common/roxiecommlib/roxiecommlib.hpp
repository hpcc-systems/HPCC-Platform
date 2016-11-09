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

#ifndef ROXIECOMMLIB_INCL
#define ROXIECOMMLIB_INCL

#ifdef ROXIECOMMLIB_EXPORTS
    #define ROXIECOMMLIB_API DECL_EXPORT
#else
    #define ROXIECOMMLIB_API DECL_IMPORT
#endif


#define ROXIECOMM_SOCKET_ERROR 1450


#include "esp.hpp"
#include "jptree.hpp"
#include "jsocket.hpp"

interface IUserDescriptor;

interface IRoxieCommunicationClient : extends IInterface
{
    virtual IPropertyTree *sendRoxieControlRequest(const char *xml, bool lockAll) = 0;
    virtual const char *sendRoxieOnDemandRequest(const char *xml, SCMStringBuffer &repsonse, bool lockAll) = 0;
    virtual void sendRoxieReloadControlRequest() = 0;

    virtual IPropertyTree *retrieveQuery(const char *id) = 0;
    virtual IPropertyTree *retrieveAllQueryInfo(const char *id) = 0;
    virtual IPropertyTree *retrieveState() = 0;

    virtual IPropertyTree *retrieveQueryStats(const char *queryName, const char *action, const char *graphName, bool lockAll) = 0;
    virtual const char *queryRoxieClusterName(SCMStringBuffer &clusterName) = 0;
    virtual IPropertyTree *retrieveQueryWuInfo(const char *queryName) = 0;
    virtual IPropertyTree *retrieveTopology() = 0;

    virtual bool updateQueryComment(const char *id, const char *comment, bool append) = 0;
    virtual bool updateACLInfo(bool allow, const char *restrict_ip, const char *mask, const char *query, const char *errorMsg, int errorCode, const char *roxieAddress, SCMStringBuffer &status) = 0;
    virtual IPropertyTree *queryACLInfo() = 0;
    virtual IPropertyTree *retrieveRoxieQueryMetrics(SCMStringBuffer &queryName, SCMStringBuffer &startDateTime, SCMStringBuffer &endDateTime) = 0;
    virtual IPropertyTree *retrieveRoxieMetrics(StringArray &ipList) = 0;
    virtual IPropertyTree *listLibrariesUsedByQuery(const char *id, bool useAliasNames) = 0;
    virtual IPropertyTree *listQueriesUsingLibrary(const char *id) = 0;
    virtual IPropertyTree *retrieveQueryActivityInfo(const char *id, int activityId) = 0;

    virtual unsigned retrieveRoxieStateRevision() = 0;
    virtual IPropertyTree *getRoxieBuildVersion() = 0;

    virtual IPropertyTree *sendRoxieControlAllNodes(const char *msg, bool allOrNothing) = 0;
    virtual IPropertyTree *sendRoxieControlAllNodes(ISocket *sock, const char *msg, bool allOrNothing, unsigned wait) = 0;
    virtual IPropertyTree *sendRoxieControlQuery(ISocket *sock, const char *msg, unsigned wait) = 0;
};

extern "C" ROXIECOMMLIB_API IRoxieCommunicationClient *createRoxieCommunicationClient(const SocketEndpoint &roxieEP, unsigned roxieTimeout);

#endif
