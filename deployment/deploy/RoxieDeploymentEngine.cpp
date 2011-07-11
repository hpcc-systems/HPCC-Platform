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
    if (!m_process.getPropInt("@numChannels", 0))
        throw MakeStringException(0, "Number of channels must be set for process %s", m_name.get());
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
