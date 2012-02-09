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
#include "ThorDeploymentEngine.hpp"

//---------------------------------------------------------------------------
// CThorDeploymentEngine
//---------------------------------------------------------------------------
CThorDeploymentEngine::CThorDeploymentEngine(IEnvDeploymentEngine& envDepEngine,
                                             IDeploymentCallback& callback,
                                             IPropertyTree& process)
 : CDeploymentEngine(envDepEngine, callback, process)
{
}

//---------------------------------------------------------------------------
//  checkInstance
//---------------------------------------------------------------------------
void CThorDeploymentEngine::check()
{
   CDeploymentEngine::check();

   const char* dali  = m_process.queryProp("@daliServers");
   if (!dali || !*dali )
      throw MakeStringException(0, "No dali server is defined for thor %s", m_process.queryProp("@name"));
}
