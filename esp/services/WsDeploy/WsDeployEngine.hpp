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
                             const char* instance, const char* format=NULL, ...) __attribute__((format(printf,6,7)));
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
