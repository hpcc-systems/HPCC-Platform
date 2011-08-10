/*#############################################################################

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
#############################################################################*/

/////////////////////////////////////////////////////////////////////////////
//
// WizardInputs.cpp : implementation file
//
/////////////////////////////////////////////////////////////////////////////
#include "wizardInputs.hpp"
#include "XMLTags.h"
#include "jencrypt.hpp"
#include "buildset.hpp"
#include "build-config.h"

#define STANDARD_CONFIGXMLDIR COMPONENTFILES_DIR"/configxml/"
#define STANDARD_CONFIG_BUILDSETFILE "buildset.xml"
#define STANDARD_CONFIG_DIR CONFIG_DIR
#define STANDARD_CONFIG_ALGORITHMFILE "genenvrules.conf"

//---------------------------------------------------------------------------
//  CWizardInputs
//---------------------------------------------------------------------------
CWizardInputs::CWizardInputs(const char* xmlArg,const char *service, 
                             IPropertyTree * cfg, 
                             MapStringTo<StringBuffer>* dirMap): m_service(service), 
                             m_cfg(cfg), m_overrideDirs(dirMap)
{
    m_pXml.setown(createPTreeFromXMLString(xmlArg && *xmlArg ? xmlArg : "<XmlArgs/>"));
}
//----------------------------------------------------------------------
CWizardInputs::~CWizardInputs()
{
    m_pXml.clear();
    HashIterator info(m_invalidServerCombo);
    for(info.first();info.isValid();info.next())
    {
        StringArray *a = *m_invalidServerCombo.mapToValue(&info.query());
        delete a;
    }
    HashIterator iter(m_compIpMap);
    ForEach(iter)
    {
        IMapping &cur = iter.query();
        CInstDetails* pInfo = m_compIpMap.mapToValue(&cur);
        pInfo->Release();
    }
    HashIterator sIter(m_compForTopology);
    for(sIter.first();sIter.isValid();sIter.next())
    {
        StringArray* a = *m_compForTopology.mapToValue(&sIter.query());
        delete a;
    }
}
//-----------------------------------------------------------------------
// SetEnvironment
//-----------------------------------------------------------------------
void CWizardInputs::setEnvironment()
{
    StringBuffer xpath;
    if(m_pXml->hasProp("@ipList"))
        formIPList(m_pXml->queryProp("@ipList"), m_ipaddress);

    if(m_pXml->hasProp("@roxieNodes"))
        m_roxieNodes = atoi(m_pXml->queryProp("@roxieNodes"));

    if(m_pXml->hasProp("@thorNodes"))
        m_thorNodes = atoi(m_pXml->queryProp("@thorNodes"));

    if(m_pXml->hasProp("@dbuser"))
        m_dbuser = m_pXml->queryProp("@dbuser");

    if(m_pXml->hasProp("@dbpassword"))
        m_dbpassword = m_pXml->queryProp("@dbpassword");

    m_thorSlavesPerNode = 1;
    if(m_pXml->hasProp("@slavesPerNode"))
        m_thorSlavesPerNode = atoi( m_pXml->queryProp("@slavesPerNode"));

    if (m_thorSlavesPerNode < 1)
        m_thorSlavesPerNode = 1;

    xpath.clear().appendf("Software/EspProcess/EspService[@name='%s']/LocalConfFile", m_service.str());
    const char* pConfFile = m_cfg->queryProp(xpath.str());
    xpath.clear().appendf("Software/EspProcess/EspService[@name='%s']/LocalEnvConfFile",  m_service.str());
    const char* pEnvConfFile = m_cfg->queryProp(xpath.str());

    if (pConfFile && *pConfFile && pEnvConfFile && *pEnvConfFile)
    {
        Owned<IProperties> pParams = createProperties(pConfFile);
        Owned<IProperties> pEnvParams = createProperties(pEnvConfFile);
        StringBuffer sb, fileName;

        fileName.append((pEnvParams->queryProp("path")!= NULL ? (sb.clear().append(pEnvParams->queryProp("path")).append("/componentfiles/configxml/")) : STANDARD_CONFIGXMLDIR));
        fileName.append((pParams->queryProp("buildset") != NULL ? (sb.clear().append(pParams->queryProp("buildset"))) : STANDARD_CONFIG_BUILDSETFILE));

        if(fileName.length() && checkFileExists(fileName.str()))
            m_buildSetTree.setown(createPTreeFromXMLFile(fileName.str()));
        else
            throw MakeStringException( -1 , "The buildSetFile %s does not exists", fileName.str());

        fileName.clear().append((pEnvParams->queryProp("configs") != NULL ? (sb.clear().append(pEnvParams->queryProp("configs")).append("/")): STANDARD_CONFIG_DIR));
        fileName.append((pParams->queryProp("wizardalgorithm") != NULL ? (sb.clear().append(pParams->queryProp("wizardalgorithm"))) : STANDARD_CONFIG_ALGORITHMFILE));

        if(fileName.length() && checkFileExists(fileName.str()))
            m_algProp.setown(createProperties(fileName.str()));
        else
            throw MakeStringException( -1 , "The algorithm file %s does not exists", fileName.str());
    }
    setWizardRules();
    setTopologyParam();
}

void CWizardInputs::setWizardRules()
{ 
    const char* roxieRedTypes[] = {"Full", "Circular", "None", "Overloaded"};
    m_roxieAgentRedType.clear().append("Circular");
    m_roxieAgentRedChannels = 2;
    m_roxieAgentRedOffset = 1;
    m_genOptForAllComps = GENOPTIONAL_ALL;
    if(m_algProp)
    {
        Owned<IPropertyIterator> iter = m_algProp->getIterator();
        StringBuffer prop;
        ForEach(*iter)
        {
            m_algProp->getProp(iter->getPropKey(), prop.clear());
            if(prop.length() && prop.charAt(prop.length()-1) == ',')
                prop.setCharAt((prop.length()-1),' ');

            if(!strcmp(iter->getPropKey(), "max_comps_per_node"))
            {
                m_maxCompOnNode = atoi(prop.str());
            }
            else if(!strcmp(iter->getPropKey(), "avoid_combo"))
            {
                StringArray pairValue;
                DelimToStringArray(prop.str(), pairValue, ",");
                if( pairValue.ordinality() > 0)
                {
                    for( unsigned i = 0; i < pairValue.ordinality() ; i++)
                    {
                        StringArray eachpair;
                        DelimToStringArray(pairValue.item(i), eachpair, "-");
                        if(eachpair.ordinality() == 2 )
                        {
                            StringArray* serverCompArr = 0;
                            ForEachItemIn(x, eachpair)
                            {
                                StringArrayPtr* pairServerArr = m_invalidServerCombo.getValue(eachpair.item(x));
                                if(pairServerArr)  
                                {
                                    serverCompArr = (*pairServerArr);
                                    serverCompArr->append(x == 0 ? eachpair.item(1): eachpair.item(0));
                                }
                                else
                                {
                                    serverCompArr = new StringArray();
                                    serverCompArr->append(x == 0 ? eachpair.item(1): eachpair.item(0));
                                    m_invalidServerCombo.setValue(eachpair.item(x),serverCompArr);
                                    serverCompArr->kill();
                                }
                            }
                        }
                    }
                }
            }
            else if(!strcmp (iter->getPropKey(),"do_not_generate"))
                DelimToStringArray(prop.str(), m_doNotGenComp, ",");
            else if(!strcmp (iter->getPropKey(),"comps_on_all_nodes"))
                DelimToStringArray(prop.str(), m_compOnAllNodes, ",");
            else if(!strcmp (iter->getPropKey(),"do_not_gen_optional"))
            {
                DelimToStringArray(prop.str(), m_doNotGenOptOnComps, ",");
                if (m_doNotGenOptOnComps.length() == 0)
                    m_genOptForAllComps = GENOPTIONAL_ALL;
                else if (m_doNotGenOptOnComps.length() == 1 && !strcmp(m_doNotGenOptOnComps.item(0), "all"))
                    m_genOptForAllComps = GENOPTIONAL_NONE;
                else
                    m_genOptForAllComps = GENOPTIONAL_COMPS;
            }
            else if(!strcmp(iter->getPropKey(), "topology_for_comps"))
                DelimToStringArray(prop.str(), m_clusterForTopology, ",");
            else if (!strcmp(iter->getPropKey(), "roxie_agent_redundancy"))
            {
                StringArray sbarr;
                DelimToStringArray(prop.str(), sbarr, ",");
                if (sbarr.length() > 1)
                {
                    int type = atoi(sbarr.item(0));
                    if (type == 0)
                        continue;

                    if (type > 0 && type < 5)
                        m_roxieAgentRedType.clear().append(roxieRedTypes[type]);
                    else
                        continue;

                    m_roxieAgentRedChannels = atoi(sbarr.item(1));
                    if (m_roxieAgentRedChannels <= 0)
                        m_roxieAgentRedChannels = 1;

                    if (sbarr.length() > 2)
                    {
                        m_roxieAgentRedOffset = atoi(sbarr.item(2));
                        if (m_roxieAgentRedOffset <= 0)
                            m_roxieAgentRedOffset = 1;
                    }
                    else
                        m_roxieAgentRedOffset = 0;
                }
            }
        }
    }
}

CInstDetails* CWizardInputs::getServerIPMap(const char* compName, const char* buildSetName,const IPropertyTree* pEnvTree, unsigned numOfNodes)
{
    StringBuffer xPath;
    xPath.appendf("./Programs/Build/BuildSet[@name=\"%s\"]",buildSetName);
    IPropertyTree* pBuildSet = pEnvTree->queryPropTree(xPath.str());

    CInstDetails* instDetails = NULL;
    if(pBuildSet)
    {
        if(m_doNotGenComp.find(buildSetName) != NotFound)
            return instDetails;

        if(m_compOnAllNodes.find(buildSetName) != NotFound)
            return instDetails;

        if(m_ipaddress.ordinality() == 1)
        {
            instDetails = new CInstDetails(compName, m_ipaddress.item(0));
            m_compIpMap.setValue(buildSetName,instDetails);
            return instDetails;
        }
        else
        {
            for(unsigned x = 0; x < numOfNodes ; x++)
            {
                unsigned numOfIPSAlreadyTaken = getCntForAlreadyAssignedIPS();
                if( numOfIPSAlreadyTaken < m_ipaddress.ordinality())
                {
                    addToCompIPMap(buildSetName, m_ipaddress.item(numOfIPSAlreadyTaken), compName);
                }
                else
                {
                    applyOverlappingRules(compName, buildSetName);
                }
            }

            if(m_compIpMap.find(buildSetName) != NULL)
            {
                instDetails = m_compIpMap.getValue(buildSetName);
                if( (instDetails->getIpAssigned()).ordinality() != numOfNodes) 
                    throw MakeStringException(-1, "Cannot assign [%d] number of nodes for [%s] cluster due to insufficient IP Addresses available after applying given set of rules. Please enter different value", numOfNodes, buildSetName);
                else{
                    return m_compIpMap.getValue(buildSetName);
                }
            }
        }
        return instDetails;
    }
    return NULL;
}

void CWizardInputs::applyOverlappingRules(const char* compName,const char* buildSetName)
{
    StringArray dontAssign , ignoredForOverlap;
    bool assignedIP = false;
    CInstDetails* compPtr = NULL;

    if(m_invalidServerCombo.find(buildSetName) != NULL)
    {
        StringArray* serverCompArr = 0;
        StringArrayPtr* pairServerArr = m_invalidServerCombo.getValue(buildSetName);
        if(pairServerArr)
            serverCompArr = (*pairServerArr);
        for(unsigned i = 0 ; i < serverCompArr->ordinality() ; i++)
        {
            compPtr = m_compIpMap.getValue(serverCompArr->item(i));
            if(compPtr)
            {
                StringArray& ipArr = compPtr->getIpAssigned();
                ForEachItemIn(i, ipArr)
                    dontAssign.append(ipArr.item(i));
            }
        }
    }
    //Since Roxie and thor might already have some ips asssigned we need to ignore those too.
    if(m_compIpMap.find(buildSetName)  != NULL)
    {
        compPtr = m_compIpMap.getValue(buildSetName);
        StringArray& ipArr = compPtr->getIpAssigned();
        ForEachItemIn(i, ipArr)
            dontAssign.append(ipArr.item(i));
    }

    for(unsigned ii = 0; ii < m_ipaddress.ordinality() ; ii++)
    {
        count_t ipAssignedCount = 0;
        ipAssignedCount = getNumOfInstForIP(m_ipaddress.item(ii));
        if(dontAssign.ordinality() > 0)
        {
            if( dontAssign.find(m_ipaddress.item(ii)) == NotFound)
            {
                if(ipAssignedCount >= m_maxCompOnNode )
                {
                    ignoredForOverlap.append(m_ipaddress.item(ii));
                }
                else
                {
                    assignedIP = true;
                    addToCompIPMap(buildSetName, m_ipaddress.item(ii), compName);
                    break;
                }
            }
        }
        else
        {
            if(ipAssignedCount >= m_maxCompOnNode )
            {
                ignoredForOverlap.append(m_ipaddress.item(ii));
            } 
            else
            {
                assignedIP = true;
                addToCompIPMap(buildSetName, m_ipaddress.item(ii), compName);
                break;
            }
        }
    }
    if(!assignedIP && ignoredForOverlap.ordinality() > 0)
    {          
        addToCompIPMap(buildSetName, ignoredForOverlap.item(0), compName);
    }
}

count_t CWizardInputs::getNumOfInstForIP(StringBuffer ip)
{
    count_t cnt = 0;
    HashIterator ips(m_compIpMap);
    ForEach(ips)
    {
        CInstDetails* comp = m_compIpMap.mapToValue(&ips.query());
        StringArray& ipArray = comp->getIpAssigned();
        if(ipArray.find(ip) != NotFound)
            cnt++;  
    }
    return cnt;
}

bool CWizardInputs::generateEnvironment(StringBuffer& envXml)
{
    if(m_algProp)
    {
        Owned<IPropertyTree> pEnvTree = createEnvironment();
        if(pEnvTree)
        {
            toXML(pEnvTree,envXml, 0, XML_SortTags | XML_Format);
        }
    }
    else
    {
        DBGLOG("not yet decided");//use default algorithm
    }
    return true;
}

IPropertyTree* CWizardInputs::createEnvironment()
{
    StringBuffer xpath, sbTemp, name ;

    sbTemp.clear().appendf("<%s><%s></%s>", XML_HEADER, XML_TAG_ENVIRONMENT, XML_TAG_ENVIRONMENT);
    IPropertyTree* pNewEnvTree = createPTreeFromXMLString(sbTemp.str());

    IPropertyTree* pSettings = pNewEnvTree->addPropTree(XML_TAG_ENVSETTINGS, createPTree());
    xpath.clear().appendf("%s/%s/%s[%s='%s']/LocalEnvFile", XML_TAG_SOFTWARE, XML_TAG_ESPPROCESS, XML_TAG_ESPSERVICE, XML_ATTR_NAME, m_service.str());
    const char* pConfFile = m_cfg->queryProp(xpath.str());

    xpath.clear().appendf("%s/%s/%s[%s='%s']/LocalEnvConfFile", XML_TAG_SOFTWARE, XML_TAG_ESPPROCESS, XML_TAG_ESPSERVICE, XML_ATTR_NAME, m_service.str());
    const char* tmp = m_cfg->queryProp(xpath.str());
    if (tmp && *tmp)
    {
        Owned<IProperties> pParams = createProperties(tmp);
        Owned<IPropertyIterator> iter = pParams->getIterator();

        ForEach(*iter)
        {
            StringBuffer prop;
            pParams->getProp(iter->getPropKey(), prop);
            pSettings->addProp(iter->getPropKey(), prop.length() ? prop.str():"");
        }
    }

    Owned<IPropertyTree> pProgramTree = createPTreeFromIPT(m_buildSetTree);
    pNewEnvTree->addPropTree(XML_TAG_PROGRAMS, createPTreeFromIPT(pProgramTree->queryPropTree("./"XML_TAG_PROGRAMS)));

    Owned<IPropertyTree> pCompTree = createPTree(XML_TAG_HARDWARE);
    generateHardwareHeaders(pNewEnvTree, sbTemp, false, pCompTree);
    pCompTree->removeProp(XML_TAG_COMPUTER);
    xpath.clear().appendf("./%s/%s", XML_TAG_COMPUTERTYPE, XML_ATTR_MEMORY);
    pCompTree->removeProp(xpath.str());
    xpath.clear().appendf("./%s/%s", XML_TAG_COMPUTERTYPE, XML_ATTR_NICSPEED);
    pCompTree->removeProp(xpath.str());

    xpath.clear().append(XML_TAG_SWITCH).append("/").append(XML_ATTR_NAME);
    pCompTree->setProp(xpath.str(), "Switch") ;

    xpath.clear().append(XML_TAG_DOMAIN).append("/").append(XML_ATTR_NAME);   
    pCompTree->setProp(xpath.str(), "localdomain");
    xpath.clear().append(XML_TAG_DOMAIN).append("/").append(XML_ATTR_PASSWORD);   
    pCompTree->setProp(xpath.str(), m_pXml->queryProp("@password"));
    xpath.clear().append(XML_TAG_DOMAIN).append("/").append(XML_ATTR_USERNAME);   
    pCompTree->setProp(xpath.str(), m_pXml->queryProp("@username"));

    xpath.clear().appendf("./%s/@snmpSecurityString", XML_TAG_DOMAIN);
    pCompTree->removeProp(xpath.str());

    xpath.clear().append(XML_TAG_COMPUTERTYPE).append("/").append(XML_ATTR_COMPUTERTYPE); 
    pCompTree->setProp(xpath.str(), "linuxmachine"); 
    xpath.clear().append(XML_TAG_COMPUTERTYPE).append("/").append(XML_ATTR_MANUFACTURER); 
    pCompTree->setProp(xpath.str(), "unknown"); 
    xpath.clear().append(XML_TAG_COMPUTERTYPE).append("/").append(XML_ATTR_NAME); 
    pCompTree->setProp(xpath.str(), "linuxmachine"); 
    xpath.clear().append(XML_TAG_COMPUTERTYPE).append("/").append(XML_ATTR_OPSYS);    
    pCompTree->setProp(xpath.str(), "linux"); 
    for(unsigned i = 0; i < m_ipaddress.ordinality(); i++)
    { 
        IPropertyTree* pComputer = pCompTree->addPropTree(XML_TAG_COMPUTER,createPTree());
        name.clear().appendf("node%03d", (i + 1));
        pComputer->addProp(XML_ATTR_COMPUTERTYPE, "linuxmachine");
        pComputer->addProp(XML_ATTR_DOMAIN, "localdomain");
        pComputer->addProp(XML_ATTR_NAME, name.str());
        pComputer->addProp(XML_ATTR_NETADDRESS, m_ipaddress.item(i));
    }
    pNewEnvTree->addPropTree(XML_TAG_HARDWARE, createPTreeFromIPT(pCompTree));
    //Before we generate software tree check for dependencies of component for do_not_generate ,roxie, thor
    checkForDependencies();
    generateSoftwareTree(pNewEnvTree);
    return pNewEnvTree;
}

void CWizardInputs::generateSoftwareTree(IPropertyTree* pNewEnvTree)
{
    StringBuffer xpath , sbNewName;

    if(m_buildSetTree)
    {
        bool ovrLog = true, ovrRun = true;
        if (m_overrideDirs->count() > 0)
        {
            HashIterator iter(*m_overrideDirs);

            ForEach(iter)
            {
                IMapping &cur = iter.query();
                StringBuffer* dirvalue = m_overrideDirs->mapToValue(&cur);
                const char * key = (const char*)cur.getKey();
                xpath.clear().appendf(XML_TAG_SOFTWARE"/Directories/Category[@name='%s']", key);
                if (!strcmp(key, "log"))
                    ovrLog = false;
                else if (!strcmp(key, "run"))
                    ovrRun = false;

                IPropertyTree* pDir = m_buildSetTree->queryPropTree(xpath.str());
                if (pDir)
                    pDir->setProp("@dir", dirvalue->str());
                else
                {
                    pDir = m_buildSetTree->queryPropTree(XML_TAG_SOFTWARE"/Directories/")->addPropTree("Category", createPTree());
                    pDir->setProp(XML_ATTR_NAME, (const char*)cur.getKey());
                    pDir->setProp("@dir", dirvalue->str());
                }
            }
        }

        pNewEnvTree->addPropTree(XML_TAG_SOFTWARE,createPTreeFromIPT(m_buildSetTree->queryPropTree("./"XML_TAG_SOFTWARE)));
        xpath.clear().appendf("%s/%s/%s[%s='%s']/LocalEnvConfFile", XML_TAG_SOFTWARE, XML_TAG_ESPPROCESS, XML_TAG_ESPSERVICE, XML_ATTR_NAME, m_service.str());
        const char* tmp = m_cfg->queryProp(xpath.str());
        if (tmp && *tmp)
        {
            Owned<IProperties> pParams = createProperties(tmp);
            updateDirsWithConfSettings(pNewEnvTree, pParams, ovrLog, ovrRun);
        }

        xpath.clear().appendf("./%s/%s/%s", XML_TAG_PROGRAMS, XML_TAG_BUILD, XML_TAG_BUILDSET);
        Owned<IPropertyTreeIterator> buildSetInsts = m_buildSetTree->getElements(xpath.str());
        ForEach(*buildSetInsts)
        {
            IPropertyTree* pBuildSet = &buildSetInsts->query();
            StringBuffer buildSetPath, compName, assignedIP, sbl;
            const char* buildSetName = pBuildSet->queryProp(XML_ATTR_NAME);
            const char* xsdFileName = pBuildSet->queryProp(XML_ATTR_SCHEMA);
            const char* processName = pBuildSet->queryProp(XML_ATTR_PROCESS_NAME);
            StringBuffer deployable = pBuildSet->queryProp("@"TAG_DEPLOYABLE);
            unsigned numOfIpNeeded = 1;

            if(m_doNotGenComp.find(buildSetName) != NotFound )
                continue;

            if(processName && *processName && buildSetName && * buildSetName && xsdFileName && *xsdFileName)
            { 
                Owned<IPropertyTree> pSchema = loadSchema(m_buildSetTree->queryPropTree("./"XML_TAG_PROGRAMS"/"XML_TAG_BUILD"[1]"), pBuildSet, buildSetPath, NULL);
                IPropertyTree* pCompTree;
                if (m_genOptForAllComps == GENOPTIONAL_ALL || (m_genOptForAllComps == GENOPTIONAL_COMPS && m_doNotGenOptOnComps.find(buildSetName) == NotFound ))
                    pCompTree = generateTreeFromXsd(pNewEnvTree, pSchema, processName, false);
                else if (m_genOptForAllComps == GENOPTIONAL_NONE || (m_genOptForAllComps == GENOPTIONAL_COMPS && m_doNotGenOptOnComps.find(buildSetName) != NotFound ))
                    pCompTree = generateTreeFromXsd(pNewEnvTree, pSchema, processName, false, false, 0, false);

                //Check for my in buildset before appending my to name like mysql
                sbNewName.clear();
                if( strstr(buildSetName ,"my") == NULL && ( strcmp(buildSetName, "topology") != 0) )
                    sbNewName.append("my");

                addComponentToEnv(pNewEnvTree, buildSetName, sbNewName, pCompTree);

                if(!strcmp(processName, XML_TAG_ESPSERVICE) || !strcmp(processName, XML_TAG_PLUGINPROCESS))
                {
                    processName = buildSetName;
                }

                if(strcmp(deployable,"no") != 0)
                {
                    if(m_compOnAllNodes.find(buildSetName) != NotFound)
                    {
                        for(unsigned i = 0; i < m_ipaddress.ordinality(); i++)
                        {
                            sbl.clear().appendf("s").append(i+1);
                            assignedIP.clear().append(m_ipaddress.item(i));
                            addInstanceToTree(pNewEnvTree, assignedIP, processName, buildSetName,sbl.str());
                        }
                    }
                    else if(numOfIpNeeded > 0)
                    {
                        if(!strcmp(buildSetName, "roxie"))
                            numOfIpNeeded = m_roxieNodes;
                        else if(!strcmp(buildSetName, "thor"))
                            numOfIpNeeded = m_thorNodes + 1;

                        CInstDetails* pInstDetail = getServerIPMap(sbNewName.str(), buildSetName, pNewEnvTree, numOfIpNeeded);

                        if(pInstDetail)
                        {
                            if(!strcmp(buildSetName, "roxie") || !strcmp(buildSetName, "thor" ))
                            {
                                addRoxieThorClusterToEnv(pNewEnvTree, pInstDetail, buildSetName);
                            }
                            else
                            {
                                StringArray& ipArr = pInstDetail->getIpAssigned();
                                ForEachItemIn(x, ipArr)
                                {
                                    assignedIP.clear().append(ipArr.item(x));
                                    addInstanceToTree(pNewEnvTree, assignedIP, processName, buildSetName, "s1");
                                }
                            }
                        }
                    }
                }

            }
        }
        getEspBindingInformation(pNewEnvTree);
        addTopology(pNewEnvTree);
        getDefaultsForWizard(pNewEnvTree);
    }
}

void CWizardInputs::addInstanceToTree(IPropertyTree* pNewEnvTree, StringBuffer attrName, const char* processName, const char* buildSetName, const char* instName)
{
    StringBuffer sb, sbl, compName, xpath, nodeName;
    xpath.clear().appendf("./%s/%s[%s=\"%s\"]", XML_TAG_HARDWARE, XML_TAG_COMPUTER, XML_ATTR_NETADDRESS, attrName.str());
    IPropertyTree* pHardTemp = pNewEnvTree->queryPropTree(xpath.str());
    if(pHardTemp)
        nodeName.clear().append(pHardTemp->queryProp("./"XML_ATTR_NAME));//NodeName

    xpath.clear().appendf("./%s/%s[%s=\"%s\"]", XML_TAG_SOFTWARE, processName, XML_ATTR_BUILDSET, buildSetName);

    IPropertyTree* pComp = pNewEnvTree->queryPropTree(xpath.str());
    compName.clear().append(pComp->queryProp(XML_ATTR_NAME));//compName

    sb.clear().appendf("<Instance buildSet=\"%s\" compName=\"%s\" ><Instance name=\"%s\" /></Instance>", buildSetName, compName.str(), nodeName.str());
    Owned<IPropertyTree> pInstance = createPTreeFromXMLString(sb.str());

    if(pInstance)
        addInstanceToCompTree(pNewEnvTree, pInstance, sbl.clear(), sb.clear(),NULL);

    xpath.clear().appendf("./%s/%s[%s=\"%s\"]/%s[%s=\"%s\"]", XML_TAG_SOFTWARE, processName, XML_ATTR_NAME, compName.str(), XML_TAG_INSTANCE, XML_ATTR_COMPUTER, nodeName.str());
    IPropertyTree* pInst = pNewEnvTree->queryPropTree(xpath.str());
    if(pInst)
    {
        pInst->addProp(XML_ATTR_NAME, instName);
    }
}

void CWizardInputs::getDefaultsForWizard(IPropertyTree* pNewEnvTree)
{
    StringBuffer xpath, tempName, value;
    Owned<IPropertyTree> pBuildTree = createPTreeFromIPT(pNewEnvTree->queryPropTree("./"XML_TAG_PROGRAMS));
    xpath.clear().appendf("./%s/%s/", XML_TAG_BUILD, XML_TAG_BUILDSET);
    Owned<IPropertyTreeIterator> buildSetInsts = pBuildTree->getElements(xpath.str());
    bool genOptional = true;
    ForEach(*buildSetInsts)
    {
        IPropertyTree* pBuildSet = &buildSetInsts->query();
        StringBuffer buildSetPath, compName;
        const char* buildSetName = pBuildSet->queryProp(XML_ATTR_NAME);
        const char* xsdFileName = pBuildSet->queryProp(XML_ATTR_SCHEMA);
        const char* processName = pBuildSet->queryProp(XML_ATTR_PROCESS_NAME);
        if(processName && *processName && buildSetName && * buildSetName && xsdFileName && *xsdFileName)
        {
            Owned<IPropertyTree> pSchema = loadSchema(pBuildTree->queryPropTree("./"XML_TAG_BUILD"[1]"), pBuildSet, buildSetPath, NULL);

            if (m_genOptForAllComps == GENOPTIONAL_ALL || (m_genOptForAllComps == GENOPTIONAL_COMPS && m_doNotGenOptOnComps.find(buildSetName) == NotFound ))
                genOptional = true;
            else if (m_genOptForAllComps == GENOPTIONAL_NONE || (m_genOptForAllComps == GENOPTIONAL_COMPS && m_doNotGenOptOnComps.find(buildSetName) != NotFound ))
                genOptional = false;

            Owned<IPropertyTree> pCompTree = generateTreeFromXsd(pNewEnvTree, pSchema, processName, true, true, this, genOptional);
            xpath.clear().appendf("./%s/%s/[%s=\"%s\"]", XML_TAG_SOFTWARE, processName, XML_ATTR_BUILDSET, buildSetName);
            IPropertyTree* pSWCompTree = pNewEnvTree->queryPropTree(xpath.str());

            if(pSWCompTree && pCompTree)
            {
                Owned<IAttributeIterator> iAttr = pCompTree->getAttributes();
                ForEach(*iAttr)
                {
                    if( pSWCompTree->hasProp(iAttr->queryName()) && strcmp(iAttr->queryName(), "@buildSet") != 0)
                    {
                        if (!strcmp(iAttr->queryName(), XML_ATTR_NAME))
                        {
                            StringBuffer sbxpath, sbnew, sbMsg;
                            sbnew.clear().append(iAttr->queryValue());
                            sbxpath.clear().append(processName);
                            getUniqueName(pNewEnvTree, sbnew, sbxpath.str(), XML_TAG_SOFTWARE);
                            bool ret = checkComponentReferences(pNewEnvTree, pSWCompTree, pSWCompTree->queryProp(iAttr->queryName()), sbMsg, sbnew.str());

                            if (ret)
                                pSWCompTree->setProp(iAttr->queryName(), iAttr->queryValue());
                        }
                        else
                            pSWCompTree->setProp(iAttr->queryName(), iAttr->queryValue());
                    }
                }

                //Now adding elements
                Owned<IPropertyTreeIterator> iterElems = pCompTree->getElements("*");
                ForEach (*iterElems)
                {
                    IPropertyTree* pElem = &iterElems->query();

                    Owned<IAttributeIterator> iAttr = pElem->getAttributes();

                    ForEach(*iAttr)
                    {
                        IPropertyTree* pNewSubElem = pSWCompTree->queryPropTree(pElem->queryName());
                        if (!pNewSubElem)
                        {
                            pNewSubElem = pSWCompTree->addPropTree(pElem->queryName(), createPTreeFromIPT(pElem));
                            break;
                        }
                        else
                        {
                            Owned<IPropertyTreeIterator> srcElems = pSWCompTree->getElements(pElem->queryName());
                            IPropertyTree* pSrcElem = NULL;
                            ForEach(*srcElems)
                            {
                                pSrcElem = &srcElems->query();
                                Owned<IAttributeIterator> iAttrElem = pElem->getAttributes();
                                ForEach(*iAttrElem)
                                {
                                    const char* attrName = iAttrElem->queryName();

                                    if (pSrcElem->hasProp(attrName))
                                        pSrcElem->setProp(attrName, iAttrElem->queryValue());

                                    Owned<IPropertyTreeIterator> iterSubElems = pElem->getElements("*");
                                    ForEach (*iterSubElems)
                                    {
                                        IPropertyTree* pSubElem = &iterSubElems->query();

                                        Owned<IPropertyTreeIterator> srcSubElems = pSWCompTree->getElements(pSubElem->queryName());
                                        IPropertyTree* pSrcSubElem = NULL;
                                        ForEach(*srcSubElems)
                                        {
                                            pSrcSubElem = &srcSubElems->query();

                                            Owned<IAttributeIterator> iAttrElem = pSubElem->getAttributes();
                                            ForEach(*iAttrElem)
                                            {
                                                const char* attrName = iAttrElem->queryName();

                                                if (pSrcSubElem->hasProp(attrName))
                                                    pSrcSubElem->setProp(attrName, iAttrElem->queryValue());
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}


void CWizardInputs::addToCompIPMap(const char* buildSetName, const char* value, const char* compName)
{
    CInstDetails* pInst = NULL;
    if(m_compIpMap.find(buildSetName) != NULL)
    {
        pInst = m_compIpMap.getValue(buildSetName);
        (pInst->getIpAssigned()).append(value);
    }
    else
    {
        pInst = new CInstDetails();
        pInst->setParams(compName, value);
        m_compIpMap.setValue(buildSetName, pInst);
    }
}

unsigned CWizardInputs::getCntForAlreadyAssignedIPS()
{
    unsigned cnt = 0;
    HashIterator ips(m_compIpMap);
    ForEach(ips)
    {
        CInstDetails* comp = m_compIpMap.mapToValue(&ips.query());
        StringArray& ipArray = comp->getIpAssigned();
        cnt += ipArray.length();
    }
    return cnt;
}

void CWizardInputs::addRoxieThorClusterToEnv(IPropertyTree* pNewEnvTree, CInstDetails* pInstDetails, const char* buildSetName)
{

    StringBuffer xmlForRoxieServer, xmlForRoxieSlave, xpath, compName, computerName, msg;

    if(!strcmp(buildSetName, "roxie"))
    {
        //Before proceeding remove the roxieserver already added to env via xsd.
        xpath.clear().appendf("./%s/%s/%s", XML_TAG_SOFTWARE, XML_TAG_ROXIECLUSTER, XML_ATTR_NAME);
        compName.clear().append(pNewEnvTree->queryProp(xpath.str()));

        xmlForRoxieServer.clear().appendf("<RoxieData type=\"RoxieFarm\" parentName=\"\" roxieName=\"%s\" >", compName.str());

        if (m_roxieNodes == 1)
            xmlForRoxieSlave.clear().appendf("<RoxieData type=\"None\" val1=\"undefined\" val2=\"undefined\" roxieName=\"%s\" >", compName.str());
        else
            xmlForRoxieSlave.clear().appendf("<RoxieData type=\"%s\" val1=\"%d\" val2=\"%d\" roxieName=\"%s\" >", 
            m_roxieAgentRedType.str(), 
            m_roxieAgentRedChannels, m_roxieAgentRedOffset, compName.str());

        if(pInstDetails)
        {
            StringArray& ipAssignedToComp = pInstDetails->getIpAssigned();

            ForEachItemIn(i, ipAssignedToComp)
            {
                xpath.clear().appendf("./%s/%s/[%s=\"%s\"]", XML_TAG_HARDWARE, XML_TAG_COMPUTER, XML_ATTR_NETADDRESS, ipAssignedToComp.item(i));
                IPropertyTree* pHardTemp = pNewEnvTree->queryPropTree(xpath.str());
                if(pHardTemp){
                    xmlForRoxieServer.appendf("<Component name=\"%s\" />", pHardTemp->queryProp("./@name"));
                    xmlForRoxieSlave.appendf("<Computer name=\"%s\" />", pHardTemp->queryProp("./@name"));
                }
            }
            xmlForRoxieServer.append("</RoxieData>");
            xmlForRoxieSlave.append("</RoxieData>");
            handleRoxieOperation(pNewEnvTree, "AddRoxieFarm", xmlForRoxieServer.str());
            handleRoxieOperation(pNewEnvTree, "RoxieSlaveConfig" ,xmlForRoxieSlave.str());
        }
        xpath.clear().appendf("./%s/%s[%s=\"%s\"]/%s[%s=\"\"]", XML_TAG_SOFTWARE, XML_TAG_ROXIECLUSTER, XML_ATTR_NAME, compName.str(), XML_TAG_ROXIE_SERVER, XML_ATTR_NETADDRESS);
        pNewEnvTree->removeProp(xpath.str());
    }
    else if(!strcmp(buildSetName, "thor"))
    {
        //We need only one master
        StringBuffer masterIP, xml;
        xpath.clear().appendf("./%s/%s/%s", XML_TAG_SOFTWARE, XML_TAG_THORCLUSTER, XML_ATTR_NAME);
        compName.clear().append(pNewEnvTree->queryProp(xpath.str()));

        if(pInstDetails)
        {
            StringArray& ipAssignedToComp = pInstDetails->getIpAssigned();

            if(!ipAssignedToComp.empty())
                masterIP.clear().append(ipAssignedToComp.item(0));

            xpath.clear().appendf("./%s/%s[%s=\"%s\"]", XML_TAG_HARDWARE, XML_TAG_COMPUTER, XML_ATTR_NETADDRESS, masterIP.str());
            IPropertyTree* pHardTemp = pNewEnvTree->queryPropTree(xpath.str());
            if(pHardTemp)
                xml.clear().appendf("<ThorData type=\"Master\" name=\"%s\" validateComputers=\"false\" skipExisting=\"false\" > <Computer name=\"%s\" /></ThorData>", compName.str(), pHardTemp->queryProp("./@name"));
            handleThorTopologyOp(pNewEnvTree, "Add", xml.str(), msg);

            //Now add Slave 
            xml.clear().appendf("<ThorData type=\"Slave\" name=\"%s\" validateComputers=\"false\" slavesPerNode=\"%d\" skipExisting=\"false\" >", compName.str(), m_thorSlavesPerNode);
            unsigned numOfNodes = ipAssignedToComp.ordinality() == 1 ? 0 : 1;

            for( ; numOfNodes < ipAssignedToComp.ordinality() ; numOfNodes++)
            {
                xpath.clear().appendf("./%s/%s[%s=\"%s\"]", XML_TAG_HARDWARE, XML_TAG_COMPUTER, XML_ATTR_NETADDRESS, ipAssignedToComp.item(numOfNodes));
                IPropertyTree* pHardTemp = pNewEnvTree->queryPropTree(xpath.str());
                if(pHardTemp)
                    xml.appendf("<Computer name=\"%s\" />", pHardTemp->queryProp("./@name"));
            }
            xml.append("</ThorData>");
            handleThorTopologyOp(pNewEnvTree, "Add" , xml.str(), msg);
        }
    }
}

void CWizardInputs::getEspBindingInformation(IPropertyTree* pNewEnvTree)
{
    StringBuffer xpath, sbDefn, xmlArg, compName, sbNewName;
    Owned<IPropertyTreeIterator> espProcessIter = pNewEnvTree->getElements("./"XML_TAG_SOFTWARE"/"XML_TAG_ESPPROCESS);

    ForEach(*espProcessIter)
    {
        IPropertyTree* pEspProcess = &espProcessIter->query();
        compName.clear().append(pEspProcess->queryProp(XML_ATTR_NAME));
        xpath.clear().appendf("./%s/%s/%s[@processName=\"%s\"]", XML_TAG_PROGRAMS, XML_TAG_BUILD, XML_TAG_BUILDSET, XML_TAG_ESPSERVICE);
        Owned<IPropertyTreeIterator> espServiceIter = pNewEnvTree->getElements(xpath.str());
        bool genOptional = true;

        ForEach (*espServiceIter)
        {
            IPropertyTree* pEspService = &espServiceIter->query();
            if(pEspService)
            {
                StringBuffer espServiceName;
                espServiceName.appendf("my%s", pEspService->queryProp("@name"));

                xpath.clear().appendf("./%s/%s[%s=\"%s\"]", XML_TAG_SOFTWARE, XML_TAG_ESPSERVICE, XML_ATTR_NAME, espServiceName.str());
                IPropertyTree* pEspServiceInSWTree = pNewEnvTree->queryPropTree(xpath.str());
                if(pEspServiceInSWTree)
                {
                    xpath.clear().append("./Properties/@defaultPort");
                    const char* port = pEspService->queryProp(xpath.str());
                    xpath.clear().append("./Properties/@defaultResourcesBasedn");
                    const char* resourceBasedn = pEspService->queryProp(xpath.str());

                    const char* buildSetName = pEspService->queryProp(XML_ATTR_NAME);
                    const char* processName = pEspService->queryProp(XML_ATTR_PROCESS_NAME);
                    if (m_genOptForAllComps == GENOPTIONAL_ALL || (m_genOptForAllComps == GENOPTIONAL_COMPS && m_doNotGenOptOnComps.find(buildSetName) == NotFound ))
                        genOptional = true;
                    else if (m_genOptForAllComps == GENOPTIONAL_NONE || (m_genOptForAllComps == GENOPTIONAL_COMPS && m_doNotGenOptOnComps.find(buildSetName) != NotFound ))
                        genOptional = false;

                    StringBuffer buildSetPath;
                    Owned<IPropertyTree> pSchema = loadSchema(pNewEnvTree->queryPropTree("./Programs/Build[1]"), pEspService, buildSetPath, NULL);

                    xmlArg.clear().appendf("<EspServiceBindings type=\"EspBinding\" compName=\"%s\" > <Item name=\"%s\" params=\"pcType=EspProcess::pcName=%s::subType=EspBinding::subTypeKey=%s \"/></EspServiceBindings>", compName.str(), espServiceName.str(), compName.str(), espServiceName.str());
                    addEspBindingInformation(xmlArg, pNewEnvTree, sbNewName, NULL);

                    xpath.clear().appendf("./%s/%s/%s/[%s=\"\"]", XML_TAG_SOFTWARE, XML_TAG_ESPPROCESS, XML_TAG_ESPBINDING, XML_ATTR_SERVICE);
                    IPropertyTree* pEspBindingInfo = pNewEnvTree->queryPropTree(xpath.str());

                    pEspBindingInfo->setProp(XML_ATTR_NAME,(espServiceName.toLowerCase()).str());
                    pEspBindingInfo->setProp(XML_ATTR_SERVICE,(espServiceName.toLowerCase()).str());
                    pEspBindingInfo->setProp(XML_ATTR_PORT, port );
                    pEspBindingInfo->setProp("@resourcesBasedn",resourceBasedn);

                    xpath.clear().appendf("%s/%s[%s=\"%s\"]/Properties", XML_TAG_SOFTWARE, XML_TAG_ESPSERVICE, XML_ATTR_NAME, (espServiceName.toLowerCase()).str());
                    IPropertyTree* pSvcProps = pNewEnvTree->queryPropTree(xpath.str());

                    Owned<IPropertyTree> pCompTree = generateTreeFromXsd(pNewEnvTree, pSchema, processName, true, false, 0, genOptional);

                    Owned<IPropertyTreeIterator> i = pSvcProps->getElements("Authenticate");
                    ForEach(*i)
                    {
                        IPropertyTree* pAuthCopy = createPTreeFromIPT(&i->query());
                        mergeAttributes(pAuthCopy, pCompTree->queryPropTree("Authenticate"));
                        IPropertyTree* pNewNode = pEspBindingInfo->addPropTree("Authenticate", pAuthCopy);
                    }

                    i.setown( pSvcProps->getElements("AuthenticateFeature") );
                    ForEach(*i)
                    {
                        IPropertyTree* pAuthCopy = createPTreeFromIPT(&i->query());
                        //Adding authentication to true for espbinding.
                        pAuthCopy->addProp("@authenticate","Yes");
                        mergeAttributes(pAuthCopy, pCompTree->queryPropTree("AuthenticateFeature"));
                        IPropertyTree* pNewNode = pEspBindingInfo->addPropTree("AuthenticateFeature", pAuthCopy);
                    }
                    i.setown( pSvcProps->getElements("AuthenticateSetting") );
                    ForEach(*i)
                    {
                        IPropertyTree* pAuthCopy = createPTreeFromIPT(&i->query());
                        mergeAttributes(pAuthCopy, pCompTree->queryPropTree("AuthenticateSetting"));
                        IPropertyTree* pNewNode = pEspBindingInfo->addPropTree("AuthenticateSetting", pAuthCopy);
                    }
                }
            }
        }
    }
}

void CWizardInputs::addTopology(IPropertyTree* pNewEnvTree)
{
    StringBuffer xpath;
    if(!pNewEnvTree->hasProp("./"XML_TAG_SOFTWARE"/"XML_TAG_TOPOLOGY))
        pNewEnvTree->addPropTree("./"XML_TAG_SOFTWARE"/"XML_TAG_TOPOLOGY, createPTree());

    HashIterator sIter(m_compForTopology);
    for(sIter.first();sIter.isValid();sIter.next())
    {
        IMapping &cur = sIter.query();
        IPropertyTree* pCluster = createTopologyForComp(pNewEnvTree,(const char *) cur.getKey());
        if(pCluster)
            pNewEnvTree->addPropTree("./"XML_TAG_SOFTWARE"/"XML_TAG_TOPOLOGY"/Cluster", pCluster);
    }
}

IPropertyTree* CWizardInputs::createTopologyForComp(IPropertyTree* pNewEnvTree, const char* component)
{
    StringBuffer xmlTag, xpath , compName, clusterStr;
    if(!strcmp(component, "roxie"))
        xmlTag.clear().append(XML_TAG_ROXIECLUSTER);
    else if(!strcmp(component, "thor"))
        xmlTag.clear().append(XML_TAG_THORCLUSTER);
    else if(!strcmp(component, "hthor"))
        xmlTag.clear().append("hthor");


    xpath.clear().appendf("./%s/%s[1]/%s", XML_TAG_SOFTWARE, xmlTag.str(), XML_ATTR_NAME);

    clusterStr.clear().appendf("<Cluster name=\"%s\" prefix=\"%s\"></Cluster>", component, component);

    IPropertyTree* pCluster = createPTreeFromXMLString(clusterStr.str());
    if(pCluster)
    {
        if(pNewEnvTree->hasProp(xpath.str()))
        {
            IPropertyTree* pComponent = pCluster->addPropTree(xmlTag.str(), createPTree());
            pComponent->addProp(XML_ATTR_PROCESS, pNewEnvTree->queryProp(xpath.str()));
        }

        if(m_compForTopology.find(component) != NULL)
        {
            StringArray* clusterCompEle = 0;
            StringArrayPtr* clusterPair = m_compForTopology.getValue(component);
            if(clusterPair)
            {
                clusterCompEle = (*clusterPair);
                for(unsigned i = 0 ; i < clusterCompEle->ordinality() ; i++)
                {
                    const char* eachClusterElem = clusterCompEle->item(i);
                    xpath.clear().appendf("./%s/%s/%s[%s=\"%s\"]", XML_TAG_PROGRAMS, XML_TAG_BUILD, XML_TAG_BUILDSET, XML_ATTR_NAME, eachClusterElem);
                    IPropertyTree* pBuildset = pNewEnvTree->queryPropTree(xpath.str());
                    if(pBuildset)
                    {
                        const char* processName = pBuildset->queryProp(XML_ATTR_PROCESS_NAME);
                        if(processName && *processName)
                        {
                            IPropertyTree* pElement = pCluster->addPropTree(processName,createPTree());
                            xpath.clear().appendf("./%s/%s[1]/%s", XML_TAG_SOFTWARE, processName, XML_ATTR_NAME);
                            if(pElement && pNewEnvTree->hasProp(xpath.str()))
                                pElement->addProp(XML_ATTR_PROCESS, pNewEnvTree->queryProp(xpath.str()));
                        }
                    }
                }

            }
            clusterCompEle->kill();
        }
        return pCluster;
    }
    else
        return NULL;
}

void CWizardInputs::checkForDependencies()
{
    StringBuffer xpath; 

    if(m_buildSetTree)
    {
        xpath.clear().appendf("./%s/%s/%s", XML_TAG_PROGRAMS, XML_TAG_BUILD, XML_TAG_BUILDSET);
        Owned<IPropertyTreeIterator> buildSetInsts = m_buildSetTree->getElements(xpath.str());
        ForEach(*buildSetInsts)
        {
            IPropertyTree* pBuildSet = &buildSetInsts->query();
            const char* buildSetName = pBuildSet->queryProp(XML_ATTR_NAME);
            unsigned numOfNodesNeeded = 1;

            if((!strcmp(buildSetName,"roxie") && m_roxieNodes == 0 )|| (!strcmp(buildSetName,"thor")&& m_thorNodes == 0)){
                numOfNodesNeeded = 0;
                m_doNotGenComp.append(buildSetName);
            }

            if(numOfNodesNeeded == 0 || (m_doNotGenComp.find(buildSetName) != NotFound))
            {
                if(m_compForTopology.find(buildSetName) != NULL )
                    m_compForTopology.remove(buildSetName);
                checkAndAddDependComponent(buildSetName);
            }
        }
    }
}

void CWizardInputs::checkAndAddDependComponent(const char* key)
{
    StringBuffer paramEntry(key);
    paramEntry.append("_dependencies");
    if(m_algProp)
    {
        if(m_algProp->hasProp(paramEntry.str()))
        {
            StringArray sArray;
            DelimToStringArray(m_algProp->queryProp(paramEntry.str()), sArray, ";");
            ForEachItemIn(x, sArray)
            {
                if(m_doNotGenComp.find(sArray.item(x)) == NotFound)
                {
                    m_doNotGenComp.append(sArray.item(x));
                    checkAndAddDependComponent(sArray.item(x));
                }
            }
        }
    }
}

unsigned CWizardInputs::getNumOfNodes(const char* compName)
{
    if(m_compIpMap.find(compName) != NULL)
    {
        CInstDetails* pInst = NULL;
        pInst = m_compIpMap.getValue(compName);
        StringArray& ipArr = pInst->getIpAssigned();
        return ipArr.ordinality();
    }
    return 0;
}

void CWizardInputs::setTopologyParam()
{
    if(m_clusterForTopology.ordinality() > 0)
    {
        StringBuffer topologySec;
        ForEachItemIn(x, m_clusterForTopology)
        { 
            topologySec.clear().appendf("%s_topology",m_clusterForTopology.item(x));
            const char * elemForCluster = m_algProp->queryProp(topologySec.str());
            if(elemForCluster && *elemForCluster)
            {
                StringArray* compClusterArr = new StringArray();

                StringArray clusterElemArr;
                DelimToStringArray( elemForCluster, clusterElemArr, ",");
                ForEachItemIn(y, clusterElemArr)
                    compClusterArr->append(clusterElemArr.item(y));
                m_compForTopology.setValue(m_clusterForTopology.item(x),compClusterArr);
            }
        }
    }
}
