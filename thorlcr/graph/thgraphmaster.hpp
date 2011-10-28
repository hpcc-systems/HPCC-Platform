/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

#ifndef _THGRAPHMASTER_HPP
#define _THGRAPHMASTER_HPP

#ifdef _WIN32
    #ifdef GRAPHMASTER_EXPORTS
        #define graphmaster_decl __declspec(dllexport)
    #else
        #define graphmaster_decl __declspec(dllimport)
    #endif
#else
    #define graphmaster_decl
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
};

interface ILoadedDllEntry;
extern graphmaster_decl CJobMaster *createThorGraph(const char *graphName, IPropertyTree *xgmml, IConstWorkUnit &workunit, const char *querySo, bool sendSo, const SocketEndpoint &agentEp);
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

