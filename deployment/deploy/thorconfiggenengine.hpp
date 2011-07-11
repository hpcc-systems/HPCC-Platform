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
#ifndef THORCONFIGGENENGINE_HPP_INCL
#define THORCONFIGGENENGINE_HPP_INCL

#include "configgenengine.hpp"

//---------------------------------------------------------------------------
// CThorDeploymentEngine
//---------------------------------------------------------------------------
class CThorConfigGenEngine : public CConfigGenEngine
{
public:
    IMPLEMENT_IINTERFACE;
    CThorConfigGenEngine(IEnvDeploymentEngine& envDepEngine,
                         IDeploymentCallback& callback,
                         IPropertyTree& process, 
                         const char* inputDir="", 
                         const char* outputDir="");

protected:
   void check();
   virtual void copyInstallFiles(IPropertyTree& instanceNode, const char* destPath);

private:
   void writeComputerFile(const char* type, const char* filename, EnvMachineOS os=MachineOsUnknown);
};
//---------------------------------------------------------------------------
#endif // THORCONFIGGENENGINE_HPP_INCL
