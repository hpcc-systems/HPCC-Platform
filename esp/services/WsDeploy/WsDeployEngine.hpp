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

#ifndef __WS_DEPLOY_ENGINE__
#define __WS_DEPLOY_ENGINE__

#pragma warning (disable : 4786)

#include "deploy.hpp"
#include <map>
#include <string>

using std::string;
using std::map;

class CWsDeployEngine : public CInterface,
                                implements IDeploymentCallback
{
public:
    CWsDeployEngine(CWsDeployExCE& service, IConstDeployInfo& depInfo, IEspContext* ctx);
    CWsDeployEngine( CWsDeployExCE& service, IEspContext* ctx, IConstDeployInfo& deployInfo, const char* selComps, short version);
    virtual ~CWsDeployEngine() {}

    void deploy(CDeployOptions& pOptions);
    void deploy();

    IMPLEMENT_IINTERFACE;

    // TODO
    virtual void installFileListChanged() {  }
    virtual void fileSizeCopied(offset_t size, bool bWholeFileDone) { }

    virtual void printStatus(IDeployTask* task);
    virtual void printStatus(StatusType type, const char* processType, const char* process, 
                                     const char* instance, const char* format=NULL, ...);
   virtual bool onDisconnect(const char* target);
    bool getAbortStatus() const
    {
        CriticalBlock block(m_critSection);
        return m_bAbort;
    }

    void setAbortStatus(bool bAbort)
    {
        CriticalBlock block(m_critSection);
        m_bAbort = bAbort;
    }

   virtual void setEnvironmentUpdated()
    {
        m_bEnvUpdated = true;
    }

   virtual void getSshAccountInfo(StringBuffer& userid, StringBuffer& password) const;
   //the following throws exception on abort, returns true for ignore
   virtual bool processException(const char* processType, const char* process, const char* instance, 
                                           IException* e, const char* szMessage=NULL, const char* szCaption=NULL,
                                            IDeployTask* pTask = NULL);
    virtual IEnvDeploymentEngine* getEnvDeploymentEngine() const { return m_pEnvDepEngine.getLink(); }
    virtual IPropertyTree* getDeployResult() const { return m_pResponseXml.getLink(); }
    virtual const char* getDeployStatus() const { return m_deployStatus.str(); }

    void IncrementErrorCount() { m_errorCount++; }
   int  GetErrorCount() const  { return m_errorCount; }

   void* getWindowHandle() const { return NULL; }

private:
    void initComponents(IArrayOf<IConstComponent>& components);
    void initDeploymentOptions(IConstDeployOptions& options);
    IPropertyTree* findTasksForComponent(const char* comp, const char* inst) const;

    CWsDeployExCE&         m_service;
    IConstDeployInfo&    m_depInfo;
    StringBuffer         m_deployStatus;
    IPropertyTree*       m_pSelComps;
    bool                        m_bAbort;
    bool                        m_bEnvUpdated;
    short                m_version;
    unsigned int            m_errorCount;
    Owned<IPropertyTree> m_pDeploy;
    Owned<IPropertyTree> m_pOptions;
    Owned<IPropertyTree> m_pResponseXml;
    Owned<IEnvDeploymentEngine> m_pEnvDepEngine;
    mutable CriticalSection      m_critSection;

    typedef pair<string, IPropertyTree*> StringIptPair;
    struct CStringToIptMap : public map<string, IPropertyTree*>
    {
    };
    CStringToIptMap     m_comp2TasksMap;
};

#endif
