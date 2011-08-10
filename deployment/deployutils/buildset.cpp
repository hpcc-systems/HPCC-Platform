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
//disable the harmless warning about very long symbol names > 255 chars in debug mode
//this is typical with STL
#pragma warning( disable : 4786 )

#include "jliball.hpp"
#include "environment.hpp"
#include "XMLTags.h"
#include "buildset.hpp"
#include "Constants.h"
#include <set>
#include <string>

using std::set;
using std::string;

static set<string> s_failedConnections;

//for getcwd
#ifdef _WINDOWS
#else
#include <dirent.h>
#include <unistd.h>
#endif

//---------------------------------------------------------------------------
//  getAccountInfo
//---------------------------------------------------------------------------
void getAccountInfo(const char* computer, StringAttr& user, StringAttr& pwd, IConstEnvironment* pConstEnv) 
{
    if (!pConstEnv)
        throw MakeStringException(-1, "No environment is available!");

    Owned<IConstMachineInfo> machine = pConstEnv->getMachine(computer);
    if (!machine)
    {
        StringBuffer sComputer(computer);
        StringBuffer sExtra;
        if (sExtra.length() == 0)
            machine.setown( pConstEnv->getMachineByAddress(computer) );

        if (!machine)
            throw MakeStringException(-1, "The computer '%s' is undefined!", computer);
    }

    Owned<IConstDomainInfo> domain = machine->getDomain();
    if (!domain)
        throw MakeStringException(-1, "The computer '%s' does not have any domain information!", computer);

    StringBuffer x;
    domain->getName(StringBufferAdaptor(x));
    if (x.length()) 
        x.append(PATHSEPCHAR);
    domain->getAccountInfo(StringBufferAdaptor(x), StringAttrAdaptor(pwd));
    user.set(x.str());
}

bool connectionRemoteMachine(const StringBuffer& sPath, IConstEnvironment* pConstEnv)
{
    bool rc = true;
    if (sPath.length() > 2 && sPath[0] == '\\' && sPath[1] == '\\')
    {
        const char* spath = sPath.str();
        const char* cpos = strchr(spath + 2, '\\');
        int pos = cpos? cpos - spath : -1;

        if (pos != -1)
        {
            char szComp[128];
            strncpy(szComp, spath + 2, pos);
            StringBuffer computer(szComp);
            StringAttr userid;
            StringAttr pswd;

            try {
                //if computer is defined in hardware section then use its associated
                //login information, if any
                getAccountInfo(computer, userid, pswd, pConstEnv);
            } catch (...)
            {
                userid.clear();
                pswd.clear();
            }
        }
    }
    return rc;
}

// Various helper functions for managing build sets
bool connectBuildSet(IPropertyTree* pBuild, IPropertyTree* pBuildSet, StringBuffer& buildSetPath, IConstEnvironment* pConstEnv)
{
    bool rc = true;
    buildSetPath.clear();

    // Get InstallSet file name from BuildSet node
    const char* szVal = pBuild->queryProp(XML_ATTR_URL);
    if (szVal && *szVal)
    {
        buildSetPath = szVal;
        rc = connectionRemoteMachine(buildSetPath, pConstEnv);
        buildSetPath.append(PATHSEPCHAR);
    }
    //szVal = pBuildSet->queryProp(XML_ATTR_PATH);
    //if (szVal && *szVal)
    //buildSetPath.append(szVal).append(PATHSEPCHAR);
    buildSetPath.append("componentfiles").append(PATHSEPCHAR).append("configxml").append(PATHSEPCHAR);
    buildSetPath.replace('\\', PATHSEPCHAR);

    return rc;
}

IPropertyTree *loadInstallSet(IPropertyTree *pBuild, IPropertyTree *pBuildSet, IConstEnvironment* pConstEnv)
{
    // Create fully qualified path to InstallSet file
    IPropertyTree* pInstallSet = NULL;

    StringBuffer sFilename;
    if (connectBuildSet(pBuild, pBuildSet, sFilename, pConstEnv))
    { 
        // Get InstallSet file name from BuildSet node
        const char* szVal = pBuildSet->queryProp(XML_ATTR_INSTALLSET);
        if (szVal && *szVal)
            sFilename.append(szVal);
        else
            sFilename.append(DEFAULT_INSTALLSET);

        pInstallSet = createPTreeFromXMLFile(sFilename);
    }
    return pInstallSet;
}

IPropertyTree *loadSchema(IPropertyTree *pBuild, IPropertyTree *pBuildSet, StringBuffer& sSchemaPath, IConstEnvironment* pConstEnv)
{
    IPropertyTree* pSchema = NULL;
    sSchemaPath.clear();

    try
    {
        if (pBuild && pBuildSet && connectBuildSet(pBuild, pBuildSet, sSchemaPath, pConstEnv))
        {
            const char *schemaName = pBuildSet->queryProp("@schema");
            if (schemaName && *schemaName)
            {
                sSchemaPath.append(schemaName);
                pSchema = createPTreeFromXMLFile(sSchemaPath);
            }
            else
            {
                const char *processName = pBuildSet->queryProp("@processName");
                if (processName && *processName)
                {
                    char szXsdPath[_MAX_PATH+1];
#ifdef _WINDOWS
                    if (GetModuleFileName(NULL, szXsdPath, _MAX_PATH))
                    {
                        char* pchSlash = strrchr(szXsdPath, PATHSEPCHAR);
#else
                    if (getcwd(szXsdPath,_MAX_PATH))
                    {
                        char* pchSlash = szXsdPath + strlen(szXsdPath);
                        strcat(pchSlash, PATHSEPSTR);
#endif

                        strcpy(pchSlash+1, "generic.xsd");
                        sSchemaPath = szXsdPath;
                    }

                    pSchema = createPTreeFromXMLFile(sSchemaPath);
                    if (pSchema)
                    {
                        IPropertyTree* pNode = pSchema->queryPropTree("xs:element");
                        pNode->setProp("@name", processName);
                        pNode = pNode->queryPropTree("xs:complexType/xs:sequence/xs:element/xs:complexType/xs:attribute[@name=\"directory\"]");
                        if (pNode)
                        {
                            const char *name = pBuildSet->queryProp("@name");
                            StringBuffer sPath("c$\\");
                            if (name)
                                sPath.append(name);

                            pNode->setProp("@default", sPath);
                        }
                    }
                }
            }
        }
    }
    catch(IException *E)
    {
        StringBuffer buf;
        (E->errorMessage(buf).str());
        E->Release();
    }
    catch(...) 
    {
    }
    return pSchema;
}

IPropertyTree *loadDefaultSchema()
{
    return createPTreeFromXMLString(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<xs:schema xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" elementFormDefault=\"qualified\">"
        "   <xs:element name=\"ThorCluster\">"
        "       <xs:complexType>"
        "           <xs:attribute name=\"build\" type=\"xs:string\" use=\"required\">"
        "               <xs:annotation><xs:appinfo>"
        "                   <tooltip>The build name to be deployed</tooltip>"
        "               </xs:appinfo></xs:annotation>"
        "           </xs:attribute>"
        "           <xs:attribute name=\"buildSet\" type=\"seisint:buildSet\" use=\"required\">"
        "               <xs:annotation><xs:appinfo>"
        "                   <viewType>hidden</viewType>"
        "               </xs:appinfo></xs:annotation>"
        "           </xs:attribute>"
        "           <xs:attribute name=\"name\" type=\"xs:string\" use=\"required\">"
        "               <xs:annotation><xs:appinfo>"
        "                   <tooltip>Name for this process</tooltip>"
        "               </xs:appinfo></xs:annotation>"
        "           </xs:attribute>"
        "           <xs:attribute name=\"description\" type=\"xs:string\" use=\"optional\">"
        "               <xs:annotation><xs:appinfo>"
        "                   <tooltip>Description for this process</tooltip>"
        "               </xs:appinfo></xs:annotation>"
        "           </xs:attribute>"
        "           <xs:attribute name=\"daliServers\" type=\"seisint:daliServers\" use=\"optional\">"
        "               <xs:annotation><xs:appinfo>"
        "                   <tooltip>Select the dali process to be used</tooltip>"
        "               </xs:appinfo></xs:annotation>"
        "           </xs:attribute>"
        "       </xs:complexType>"
        "   </xs:element>"
        "</xs:schema>"
        );
}
