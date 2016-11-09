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

#ifndef _THGRAPHMASTER_HPP
#define _THGRAPHMASTER_HPP

#ifdef GRAPHMASTER_EXPORTS
    #define graphmaster_decl DECL_EXPORT
#else
    #define graphmaster_decl DECL_IMPORT
#endif

#include "mptag.hpp"
#include "thor.hpp"
#include "eclhelper.hpp"

class SocketEndpoint;
class StringBuffer;
interface ICodeContext;
interface IConstWorkUnit;
interface IConstWUQuery;
interface IUserDescriptor;

interface IGlobalCodeContext;

interface IException;

class CJobMaster;
interface IDeMonServer;
interface IJobManager : extends IInterface
{
    virtual void stop() = 0;
    virtual void replyException(CJobMaster &job, IException *e) = 0;
    virtual void setWuid(const char *wuid, const char *cluster=NULL) = 0;
    virtual IDeMonServer *queryDeMonServer() = 0;
    virtual void fatal(IException *e) = 0;
    virtual void addCachedSo(const char *name) = 0;
    virtual void updateWorkUnitLog(IWorkUnit &workunit) = 0;
};

interface ILoadedDllEntry;
extern graphmaster_decl CJobMaster *createThorGraph(const char *graphName, IConstWorkUnit &workunit, ILoadedDllEntry *querySo, bool sendSo, const SocketEndpoint &agentEp);
extern graphmaster_decl void setJobManager(IJobManager *jobManager);
extern graphmaster_decl IJobManager *getJobManager();
extern graphmaster_decl IJobManager &queryJobManager();
interface IFatalHandler : extends IInterface
{
    virtual void inform(IException *e) = 0;
    virtual void clear() = 0;
};
interface IBarrier;
interface ICommunicator;

#endif

