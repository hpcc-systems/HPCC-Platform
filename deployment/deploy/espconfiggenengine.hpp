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
#ifndef ESPCONFIGGENENGINE_HPP_INCL
#define ESPCONFIGGENENGINE_HPP_INCL

#include "configgenengine.hpp"

//---------------------------------------------------------------------------
//  CEspConfigGenEngine
//---------------------------------------------------------------------------
class CEspConfigGenEngine : public CConfigGenEngine
{
public:
    IMPLEMENT_IINTERFACE;
    CEspConfigGenEngine(IEnvDeploymentEngine& envDepEngine, 
        IDeploymentCallback& callback,
        IPropertyTree& process, const char* inputDir="", const char* outputDir="" );
    
    virtual void xslTransform(const char *xsl, const char *outputFile, const char *instanceName,
        EnvMachineOS os=MachineOsUnknown, const char* processName=NULL, bool isEspModuleOrPlugin=false);
    
    virtual void processCustomMethod(const char *method, const char *source, const char *outputFile, 
        const char *instanceName, EnvMachineOS os);
    
protected:
    void check();
    int  determineInstallFiles(IPropertyTree& node, CInstallFiles& installFiles) const;
    void processServiceModules(const char* moduleType, StringBuffer& serviceXsltOutputFiles,
        const char* instanceName, EnvMachineOS os);
private:

};
//---------------------------------------------------------------------------
#endif // ESPCONFIGGENENGINE_HPP_INCL

