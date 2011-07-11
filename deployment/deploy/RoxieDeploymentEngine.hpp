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
#ifndef ROXIEDEPLOYMENTENGINE_HPP_INCL
#define ROXIEDEPLOYMENTENGINE_HPP_INCL

#include "DeploymentEngine.hpp"

//---------------------------------------------------------------------------
// CRoxieDeploymentEngine
//---------------------------------------------------------------------------
class CRoxieDeploymentEngine : public CDeploymentEngine
{
public:
    IMPLEMENT_IINTERFACE;
    CRoxieDeploymentEngine(IEnvDeploymentEngine& envDepEngine, 
                          IDeploymentCallback& callback,
                          IPropertyTree& process);

    virtual void checkInstance(IPropertyTree& node) const;
   virtual void start();
   virtual void stop ();

private:
   IArrayOf<IPropertyTree> m_allInstances;
};
//---------------------------------------------------------------------------
#endif // ROXIEDEPLOYMENTENGINE_HPP_INCL
