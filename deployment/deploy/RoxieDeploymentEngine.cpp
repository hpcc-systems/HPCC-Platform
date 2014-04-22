/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
#include "jexcept.hpp"
#include "jfile.hpp"
#include "jptree.hpp"
#include "xslprocessor.hpp"
#include "RoxieDeploymentEngine.hpp"

//---------------------------------------------------------------------------
// CRoxieDeploymentEngine
//---------------------------------------------------------------------------
CRoxieDeploymentEngine::CRoxieDeploymentEngine(IEnvDeploymentEngine& envDepEngine,
                                               IDeploymentCallback& callback,
                                               IPropertyTree& process)
 : CDeploymentEngine(envDepEngine, callback, process, "*")
{
   ForEachItemIn(idx, m_instances)
    {
      IPropertyTree& instance = m_instances.item(idx);
      if (!strcmp(instance.queryName(), "RoxieServerProcess"))
         m_allInstances.append( *LINK(&instance) );
   }
}

void CRoxieDeploymentEngine::checkInstance(IPropertyTree& node) const
{
    CDeploymentEngine::checkInstance(node);
}

//---------------------------------------------------------------------------
//  start
//---------------------------------------------------------------------------
void CRoxieDeploymentEngine::start()
{   
    if (m_instances.ordinality() > 0)
    {
      checkAbort();

        char tempPath[_MAX_PATH];
        getTempPath(tempPath, sizeof(tempPath), m_name);

        ensurePath(tempPath);
        m_envDepEngine.addTempDirectory( tempPath );

      int nInstances = m_instances.ordinality();
      if ( nInstances == m_allInstances.ordinality())
      {
         IPropertyTree& instance = m_instances.item(0);
         m_curInstance = instance.queryProp("@name");
           startInstance(instance); //only start the first Roxie server - it starts the rest
      }
      else
      {
         for (int i=0; i<nInstances; i++)
         {
            checkAbort();
            IPropertyTree& instance = m_instances.item(i);
            m_curInstance = instance.queryProp("@name");
              startInstance(instance, "start_one_roxie");
         }
      }
      m_curInstance = NULL;
    }
}

//---------------------------------------------------------------------------
//  stop
//---------------------------------------------------------------------------
void CRoxieDeploymentEngine::stop()
{
    if (m_instances.ordinality() > 0)
    {
      checkAbort();
        char tempPath[_MAX_PATH];
        getTempPath(tempPath, sizeof(tempPath), m_name);

        ensurePath(tempPath);
        m_envDepEngine.addTempDirectory( tempPath );

      int nInstances = m_instances.ordinality();
      if ( nInstances == m_allInstances.ordinality())
      {
         //only stop the first Roxie server - it stops the rest
         IPropertyTree& instance = m_instances.item(0);
         m_curInstance = instance.queryProp("@name");
           stopInstance(instance);
      }
      else
      {
         for (int i=0; i<nInstances; i++)
         {
            checkAbort();
            IPropertyTree& instance = m_instances.item(i);
            m_curInstance = instance.queryProp("@name");
              stopInstance(instance, "stop_one_roxie");
         }
      }
      m_curInstance = NULL;
    }
}
