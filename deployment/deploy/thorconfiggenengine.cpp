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
#include "thorconfiggenengine.hpp"

//---------------------------------------------------------------------------
// CThorConfigGenEngine
//---------------------------------------------------------------------------
CThorConfigGenEngine::CThorConfigGenEngine(IEnvDeploymentEngine& envDepEngine,
                                             IDeploymentCallback& callback,
                                             IPropertyTree& process,
                                             const char* inputDir, 
                                             const char* outputDir)
 : CConfigGenEngine(envDepEngine, callback, process, inputDir, outputDir)
{
}

//---------------------------------------------------------------------------
//  checkInstance
//---------------------------------------------------------------------------
void CThorConfigGenEngine::check()
{
   CDeploymentEngine::check();

   const char* dali  = m_process.queryProp("@daliServers");
   if (!dali || !*dali )
      throw MakeStringException(0, "No dali server is defined for thor %s", m_process.queryProp("@name"));
}


//---------------------------------------------------------------------------
// copyInstallFiles
//---------------------------------------------------------------------------
void CThorConfigGenEngine::copyInstallFiles(IPropertyTree& instanceNode, const char* destPath)
{
   // Copy install files
   CDeploymentEngine::copyInstallFiles(instanceNode, destPath);

   EnvMachineOS os = m_envDepEngine.lookupMachineOS(instanceNode);

   if (!m_compare)
      ensurePath(destPath);

   StringBuffer sbDestDir(StringBuffer(destPath).append(m_process.queryProp("@name")).append(PATHSEPCHAR));
   // Create slaves and spares files
   writeComputerFile("./ThorSlaveProcess", StringBuffer(sbDestDir).append("slaves").str(), os);
   writeComputerFile("./ThorSpareProcess", StringBuffer(sbDestDir).append("spares").str(), os);
}


//---------------------------------------------------------------------------
// writeComputerFile
//---------------------------------------------------------------------------
void CThorConfigGenEngine::writeComputerFile(const char* type, const char* filename, EnvMachineOS os/*=MachineOsUnknown*/)
{
   StringBuffer str;
   Owned<IPropertyTreeIterator> iter = m_process.getElements(type);
    for(iter->first(); iter->isValid(); iter->next())
    {
        StringAttr netAddress;
      if (m_envDepEngine.lookupNetAddress(netAddress, iter->query().queryProp("@computer")).length() > 0)
         str.appendf("%s\n", netAddress.get());
    }
   writeFile(filename, str.str(), os);
}
