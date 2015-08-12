/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems®.

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

#pragma warning (disable : 4786)

#include "ws_esdlconfigservice.hpp"
#include "exception_util.hpp"
#include "daclient.hpp"
#include "dalienv.hpp"
#include "dadfs.hpp"
#include "dasess.hpp"
#include "esdl_binding.hpp"

IPropertyTree * fetchConfigInfo(const char * config,
                                StringBuffer & espProcName,
                                StringBuffer & espBindingName,
                                StringBuffer & esdlDefId,
                                StringBuffer & esdlServiceName)
{
    IPropertyTree * methodstree = NULL;

    if (!config || !*config)
    {
        throw MakeStringException(-1,"Empty config detected");
    }
    else
    {
        StringBuffer espProcNameFromConfig;
        StringBuffer espBindingNameFromConfig;
        StringBuffer esdlDefIdFromConfig;
        StringBuffer esdlServiceNameFromConfig;
        Owned<IPropertyTree>  configTree = createPTreeFromXMLString(config, ipt_caseInsensitive);
        //Now let's figure out the structure of the configuration passed in...

        StringBuffer rootname;
        configTree->getName(rootname);

        IPropertyTree * deftree = NULL;

        if (stricmp(rootname.str(), "Binding") == 0)
        {
            configTree->getProp("@espprocess", espProcNameFromConfig);
            configTree->getProp("@espbinding", espBindingNameFromConfig);

            if (espProcNameFromConfig.length() != 0)
            {
                if (espProcName.length() == 0)
                    espProcName.set(espProcNameFromConfig);
                else if (stricmp(espProcName.str(), espProcNameFromConfig.str()) != 0)
                    throw MakeStringException(-1,
                            "ESP Process name (%s) does not match espprocess entry submitted in configuration (%s).", espProcName.str(), espProcNameFromConfig.str());
            }

            if (espBindingNameFromConfig.length() != 0)
            {
                if (espBindingName.length() == 0)
                    espBindingName.set(espBindingNameFromConfig);
                else if (stricmp(espBindingName.str(), espBindingNameFromConfig.str()) != 0)
                    throw MakeStringException(-1,
                            "ESP Binding name (%s) does not match espprocess entry submitted in configuration (%s).", espBindingName.str(), espBindingNameFromConfig.str());
            }
        }

        if (stricmp(rootname.str(), "Definition") == 0)
            deftree = configTree;
        else
            deftree = configTree->queryBranch("Definition[1]");

        if (deftree)
        {
            deftree->getProp("@id", esdlDefIdFromConfig);
            deftree->getProp("@esdlservice", esdlServiceNameFromConfig);

            if (esdlDefIdFromConfig.length() != 0)
            {
                if (esdlDefId.length() == 0)
                    esdlDefId.set(esdlDefIdFromConfig);
                else if (stricmp(esdlDefId.str(), esdlDefIdFromConfig.str()) != 0)
                    throw MakeStringException(-1,
                            "ESDL definition id (%s) associated with this service does not match the ID entry submitted in configuration (%s).", esdlDefId.str(), esdlDefIdFromConfig.str());
            }

            if (esdlServiceNameFromConfig.length() != 0)
            {
                if (esdlServiceName.length() == 0)
                    esdlServiceName.set(esdlServiceNameFromConfig);
                else if (stricmp(esdlServiceName.str(), esdlServiceNameFromConfig.str()) != 0)
                    throw MakeStringException(-1,
                            "ESDL Service name (%s) does not match esdlservice entry submitted in configuration (%s).", esdlServiceName.str(), esdlServiceNameFromConfig.str());
            }

            methodstree = deftree->getBranch("Methods");
        }

        if (!methodstree) //if we didn't already find the methods section of the config, let's look at the root
        {
            if (stricmp(rootname.str(), "Methods") == 0)
                methodstree = configTree.getLink();
        }
    }
    return methodstree;
}

bool isESDLDefinitionBound(const char * esdldefid)
{
    if (!esdldefid || !*esdldefid)
           return false;

    Owned<IRemoteConnection> conn = querySDS().connect(ESDL_BINDINGS_ROOT_PATH, myProcessSession(), RTM_LOCK_READ, SDS_LOCK_TIMEOUT);
    if (!conn)
       return false;

    conn->close(false);

    StringBuffer lcName(esdldefid);
    lcName.toLowerCase();

    IPropertyTree * bindings = conn->queryRoot();

    if (!bindings)
       return false;

    VStringBuffer xpath("%s/Definition[@id='%s']", ESDL_BINDING_ENTRY, lcName.str());

    int count = bindings->getCount(xpath);
    bool has = bindings->hasProp(xpath);

    return has;
}

bool isESDLDefinitionBound(const char * esdldefname, int version)
{
    if (!esdldefname || !*esdldefname)
        return false;

    if (version <= 0)
        return false;

    StringBuffer id;
    id.appendf("%s.%d", esdldefname, version);

    return isESDLDefinitionBound(id);
}

bool checkSDSPathExists(const char * sdsPath)
{
    Owned<IRemoteConnection> conn = querySDS().connect(sdsPath, myProcessSession(), RTM_LOCK_READ, 3000);
    if (conn)
    {
        conn->close(false);
        return true;
    }
    return false;
}

bool ensureSDSSubPath(const char * sdsPath)
{
    if (!sdsPath)
        return false;

    Owned<IRemoteConnection> conn = querySDS().connect(sdsPath, myProcessSession(), RTM_LOCK_READ, 4000);
    if (!conn)
    {
        conn.setown(querySDS().connect("/", myProcessSession(), RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT));
        if (!conn.get())
            return false;

        IPropertyTree * sdsRoot = conn->queryRoot();
        if (!sdsRoot)
            return false;

        sdsRoot->addProp(sdsPath, "");
        conn->commit();
    }

    conn->close(false);
    return true;
}

bool ensureSDSPath(const char * sdsPath)
{
    if (!sdsPath)
        return false;

    StringArray paths;
    paths.appendList(sdsPath, PATHSEPSTR);

    StringBuffer fullpath;
    ForEachItemIn(idx, paths)
    {
        if (idx > 0)
            fullpath.append("/"); //Dali paths aren't os dependent... right?
        fullpath.append(paths.item(idx));

        if (!checkSDSPathExists(fullpath))
        {
            if(!ensureSDSSubPath(fullpath))
                return false;
        }
    }

    return true;
}

void CWsESDLConfigEx::init(IPropertyTree *cfg, const char *process, const char *service)
{
    if(cfg == NULL)
        throw MakeStringException(-1, "can't initialize CWsESDLConfigEx, cfg is NULL");

#ifdef _DEBUG
    StringBuffer thexml;
    toXML(cfg, thexml,0,0);
    fprintf(stderr, "%s", thexml.toCharArray());
#endif

    StringBuffer xpath;
    xpath.appendf("Software/EspProcess[@name=\"%s\"]/EspService[@name=\"%s\"]", process, service);
    IPropertyTree* servicecfg = cfg->getPropTree(xpath.str());

    if(servicecfg == NULL)
        throw MakeStringException(-1, "config not found for service %s/%s",process, service);

    if(!ensureSDSPath("ESDL"))
        throw MakeStringException(-1, "Could not ensure '/ESDL' entry in dali configuration");
}

IPropertyTree * CWsESDLConfigEx::getESDLDefinitionRegistry(const char * wsEclId, bool readonly)
{
    if (!ensureSDSPath("ESDL/Definitions"))
        throw MakeStringException(-1, "Unexpected error while attempting to access ESDL definition dali registry.");

    Owned<IRemoteConnection> conn = querySDS().connect(ESDL_DEFS_ROOT_PATH, myProcessSession(), RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT);
    if (!conn)
        throw MakeStringException(-1, "Unexpected error while attempting to access ESDL definition dali registry.");
    return (conn) ? conn->getRoot() : NULL;
}

void CWsESDLConfigEx::addESDLDefinition(IPropertyTree * queryRegistry, const char * name, IPropertyTree *definitionInfo, StringBuffer &newId, unsigned &newSeq, const char *userid, bool deleteprev)
{
    StringBuffer lcName(name);
    lcName.toLowerCase();
    StringBuffer xpath;
    xpath.append(ESDL_DEF_ENTRY).append("[@name=\"").append(lcName.str()).append("\"]");

    Owned<IPropertyTreeIterator> iter = queryRegistry->getElements(xpath);

    newSeq = 1;
    ForEach(*iter)
    {
        IPropertyTree &item = iter->query();
        unsigned thisSeq = item.getPropInt("@seq");
        if (thisSeq >= newSeq)
            newSeq = thisSeq + 1;
    }

    if (deleteprev && newSeq > 1)
    {
        if (!isESDLDefinitionBound(lcName, newSeq -1))
        {
            newSeq--;
            queryRegistry->removeTree(queryRegistry->queryPropTree(xpath.appendf("[@seq='%d']", newSeq)));
        }
        else
        {
            DBGLOG("Will not delete previous ESDL definition version because it is referenced in an ESDL binding.");
        }
    }

    newId.set(lcName).append(".").append(newSeq);
    definitionInfo->setProp("@name", lcName);
    definitionInfo->setProp("@id", newId);
    definitionInfo->setPropInt("@seq", newSeq);
    if (userid && *userid)
        definitionInfo->setProp("@publishedBy", userid);
    queryRegistry->addPropTree(ESDL_DEF_ENTRY, LINK(definitionInfo));
}
bool CWsESDLConfigEx::existsESDLDefinition(const char * servicename, unsigned ver)
{
    bool found = false;
    if (!servicename || !*servicename)
        return false;

    VStringBuffer definitionid("%s.%d", servicename, ver);
    return existsESDLDefinition(definitionid.str());
}

bool CWsESDLConfigEx::existsESDLDefinition(const char * definitionid)
{
    bool found = false;
    if (!definitionid)
        return found;

    StringBuffer lcid (definitionid);
    lcid.toLowerCase();
    VStringBuffer xpath("%s[@id='%s']", ESDL_DEF_PATH, lcid.str());
    Owned<IRemoteConnection> globalLock = querySDS().connect(xpath.str(), myProcessSession(), RTM_LOCK_READ, SDS_LOCK_TIMEOUT);

    if (globalLock)
    {
        found = true;
        globalLock->close(false);
    }

    return found;
}

bool CWsESDLConfigEx::existsESDLMethodDef(const char * esdlDefinitionName, unsigned ver, const char * esdlServiceName, const char * methodName)
{
    bool found = false;
    if (!esdlDefinitionName || !*esdlDefinitionName || !methodName || !*methodName || !esdlServiceName ||!*esdlServiceName)
        return found;

    StringBuffer lcdefname (esdlDefinitionName);
    lcdefname.toLowerCase();
    VStringBuffer xpath("%s[@id='%s.%d']/esxdl", ESDL_DEF_PATH, lcdefname.str(), ver);
    Owned<IRemoteConnection> globalLock = querySDS().connect(xpath.str(), myProcessSession(), RTM_LOCK_READ, SDS_LOCK_TIMEOUT);

    if (globalLock)
    {
        IPropertyTree * esxdl = globalLock->queryRoot();
        if (esxdl)
        {
            VStringBuffer mxpath("EsdlService[@name='%s']", esdlServiceName);

            Owned<IPropertyTreeIterator> it = esxdl->getElements("EsdlService");
            ForEach(*it)
            {
                IPropertyTree* pChildNode = &it->query();
                if (stricmp(pChildNode->queryProp("@name"), esdlServiceName)==0)
                {
                    Owned<IPropertyTreeIterator> it2 = esxdl->getElements("EsdlMethod");
                    ForEach(*it2)
                    {
                        IPropertyTree* pChildNode = &it2->query();
                        if (stricmp(pChildNode->queryProp("@name"), methodName)==0)
                        {
                            found = true;
                            break;
                        }
                    }
                }
            }
        }

        globalLock->close(false);
    }
    globalLock.clear();
    return found;
}

void CWsESDLConfigEx::ensureESDLServiceBindingRegistry(const char * espProcName, const char * espBindingName, bool readonly)
{
    if (!espProcName || !*espProcName)
        throw MakeStringException(-1, "Unable to ensure ESDL service binding registry in dali, esp process name not available");

    if (!espBindingName || !*espBindingName)
        throw MakeStringException(-1, "Unable to ensure ESDL service binding registry in dali, esp binding name not available");

    if (!ensureSDSPath("ESDL/Bindings"))
        throw MakeStringException(-1, "Unable to connect to ESDL Service binding information in dali %s", ESDL_BINDINGS_ROOT_PATH);

    Owned<IRemoteConnection> globalLock = querySDS().connect(ESDL_BINDINGS_ROOT_PATH, myProcessSession(), RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT);

    if (!globalLock)
        throw MakeStringException(-1, "Unable to connect to ESDL Service binding information in dali %s", ESDL_BINDINGS_ROOT_PATH);

    IPropertyTree * esdlDefinitions = globalLock->queryRoot();
    if (!esdlDefinitions)
        throw MakeStringException(-1, "Unable to open ESDL Service binding information in dali %s", ESDL_BINDINGS_ROOT_PATH);

    globalLock->close(false);
}

IPropertyTree * CWsESDLConfigEx::getEspProcessRegistry(const char * espprocname, const char * espbindingport, const char * servicename)
{
    if (!espprocname || !*espprocname)
        return NULL;

    if ((!espbindingport || !*espbindingport) && (!servicename || !*servicename))
           return NULL;

    VStringBuffer xpath("/Environment/Software/EspProcess[@name='%s']", espprocname);
    Owned<IRemoteConnection> globalLock = querySDS().connect(xpath.str(), myProcessSession(), RTM_LOCK_READ, SDS_LOCK_TIMEOUT);

    if (!globalLock)
        throw MakeStringException(-1, "Unable to connect to ESDL Service configuration information in dali %s", ESDL_DEFS_ROOT_PATH);
    IPropertyTree *esdlDefinitions = globalLock->queryRoot();
    if (!esdlDefinitions)
        throw MakeStringException(-1, "Unable to open ESDL Service configuration in dali %s", ESDL_DEFS_ROOT_PATH);

    globalLock->close(false);

    if (espbindingport && *espbindingport)
        xpath.appendf("/EspBinding[@port='%s']", espbindingport);
    else
        xpath.appendf("/EspBinding[@service='%s']", servicename);

    //Only lock the branch for the target we're interested in.
    Owned<IRemoteConnection> conn = querySDS().connect(xpath.str(), myProcessSession(), RTM_LOCK_READ , SDS_LOCK_TIMEOUT);
    if (conn)
    {
        conn->close(false);
        return conn->getRoot();
    }

    return NULL;
}

bool CWsESDLConfigEx::onPublishESDLDefinition(IEspContext &context, IEspPublishESDLDefinitionRequest & req, IEspPublishESDLDefinitionResponse & resp)
{
    try
    {
        if (!context.validateFeatureAccess(FEATURE_URL, SecAccess_Write, false))
            throw MakeStringException(ECLWATCH_ROXIE_QUERY_ACCESS_DENIED, "Failed to Publish ESDL Service definition. Permission denied.");

        Owned<IUserDescriptor> userdesc;
        const char *user = context.queryUserId();
        const char *password = context.queryPassword();
        if (user && *user && *password && *password)
        {
            userdesc.setown(createUserDescriptor());
            userdesc->set(user, password);
        }

        DBGLOG("CWsESDLConfigEx::onPublishESDLDefinition User=%s",user);

        resp.updateStatus().setCode(0);

        StringAttr service(req.getServiceName());
        if (service.isEmpty())
            throw MakeStringException(-1, "Name of Service to be defined is required");

        resp.setServiceName(service.get());

        const char * inxmldef = req.getXMLDefinition();
        if (!inxmldef || !*inxmldef)
            throw MakeStringException(-1, "Service definition (XML ESDL) is missing");

        //much easier than creating a temp tree later on, just to add a root tag...
        StringBuffer xmldefinition;
        xmldefinition.appendf("<Definition>%s</Definition>", inxmldef);

        Owned<IPropertyTree> serviceXMLTree = createPTreeFromXMLString(xmldefinition,ipt_caseInsensitive);

#ifdef _DEBUG
        StringBuffer xml;
        toXML(serviceXMLTree, xml, 0,0);
        fprintf(stderr, "incoming ESDL def: %s", xml.str());
#endif
        StringBuffer serviceXpath;
        serviceXpath.appendf("esxdl/EsdlService[@name=\"%s\"]", service.get());

        if (!serviceXMLTree->hasProp(serviceXpath))
            throw MakeStringException(-1, "Service \"%s\" definition not found in ESDL provided", service.get());

        bool deletePrevious = req.getDeletePrevious();
        resp.setDeletePrevious(deletePrevious);

        StringBuffer newqueryid;
        unsigned newseq = 0;
        StringBuffer msg;

        Owned<IPropertyTree> queryRegistry = getESDLDefinitionRegistry(service.get(), false);

        if (queryRegistry != NULL)
        {
            addESDLDefinition(queryRegistry, service.get(), serviceXMLTree.get(), newqueryid, newseq, user, deletePrevious);
            if (newseq)
                resp.setEsdlVersion(newseq);

            msg.appendf("Successfully published %s", newqueryid.str());
        }
        else
        {
            msg.set("Could not publish ESDL Definition, unable to fetch ESDL Definition registry.");
            resp.updateStatus().setCode(-1);
        }

        resp.updateStatus().setDescription(msg.str());
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, -1);
    }
    catch(...)
    {
        throw MakeStringException(-1, "Unexpected error while attempting to publish ESDL definition.");
    }

    return true;
}

int CWsESDLConfigEx::publishESDLBinding(const char * bindingName,
                                         IPropertyTree * methodsConfig,
                                         const char * espProcName,
                                         const char * espPort,
                                         const char * esdlDefinitionName,
                                         int esdlDefinitionVersion,
                                         const char * esdlServiceName,
                                         StringBuffer & message,
                                         bool overwrite)
{
    if (!esdlDefinitionName || !*esdlDefinitionName)
    {
        message.set("Could not configure DESDL service: Target Esdl definition name not available");
        return -1;
    }

    if (!esdlServiceName || !*esdlServiceName)
    {
        message.set("Could not configure DESDL service: Target Esdl definition service name not available");
        return -1;
    }

    if (!bindingName || !*bindingName)
    {
        message.setf("Could not configure '%s' - Target binding name not available", esdlServiceName);
        return -1;
    }

    if (!ensureSDSPath("ESDL/Bindings"))
           throw MakeStringException(-1, "Unexpected error while attempting to access ESDL definition dali registry.");

    Owned<IRemoteConnection> conn = querySDS().connect(ESDL_BINDINGS_ROOT_PATH, myProcessSession(), RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT);
    if (!conn)
       throw MakeStringException(-1, "Unexpected error while attempting to access ESDL definition dali registry.");

    StringBuffer lcName(esdlDefinitionName);
    lcName.toLowerCase();

    IPropertyTree * bindings = conn->queryRoot();

    StringBuffer xpath;
    xpath.appendf("%s[@id='%s.%s']", ESDL_BINDING_ENTRY, espProcName, bindingName);

    bool duplicateBindings = bindings->hasProp(xpath.str());

    if (duplicateBindings)
    {
        if(overwrite)
           bindings->removeTree(bindings->queryPropTree(xpath));
        else
        {
           message.setf("Could not configure Service '%s' because this service has already been configured for binding '%s' on ESP Process '%s'", esdlServiceName, bindingName, espProcName);
           conn->close(false);
           return -1;
        }
    }

    VStringBuffer qbindingid("%s.%s", espProcName, bindingName);
    Owned<IPropertyTree> bindingtree  = createPTree();
    bindingtree->setProp("@espprocess", espProcName);
    bindingtree->setProp("@espbinding", bindingName);
    bindingtree->setProp("@id", qbindingid.str());

    if (esdlDefinitionVersion <= 0)
        esdlDefinitionVersion = 1;

    StringBuffer newId;
    newId.set(lcName).append(".").append(esdlDefinitionVersion);

    Owned<IPropertyTree> esdldeftree  = createPTree();
    esdldeftree->setProp("@name", lcName);
    esdldeftree->setProp("@id", newId);
    esdldeftree->setProp("@esdlservice", esdlServiceName);

    esdldeftree->addPropTree("Methods", LINK(methodsConfig));

    bindingtree->addPropTree(ESDL_DEF_ENTRY, LINK(esdldeftree));
    bindings->addPropTree(ESDL_BINDING_ENTRY, LINK(bindingtree));

    conn->commit();
    conn->close(false);
    message.setf("Successfully configured Service '%s', associated with ESDL definition '%s', on ESP '%s' and binding '%s'", esdlServiceName, newId.str(), espProcName, bindingName);
    return 0;
}

bool CWsESDLConfigEx::onPublishESDLBinding(IEspContext &context, IEspPublishESDLBindingRequest &req, IEspPublishESDLBindingResponse &resp)
{
    try
    {
        if (!context.validateFeatureAccess(FEATURE_URL, SecAccess_Write, false))
            throw MakeStringException(ECLWATCH_ROXIE_QUERY_ACCESS_DENIED, "Failed to Configure ESDL Service. Permission denied.");

        StringBuffer username;
        context.getUserID(username);
        DBGLOG("CWsESDLConfigEx::onPublishESDLBinding User=%s",username.str());

        StringBuffer espProcName(req.getEspProcName());
        StringBuffer espBindingName(req.getEspBindingName());
        StringBuffer espPort(req.getEspPort());
        StringBuffer espServiceName(req.getEsdlServiceName());
        StringBuffer esdlDefIdSTR(req.getEsdlDefinitionID());

        StringBuffer esdlServiceName(req.getEsdlServiceName());
        bool overwrite = req.getOverwrite();

        StringBuffer config(req.getConfig());

        Owned<IPropertyTree> methodstree;

        if (config.length() > 0)
            methodstree.setown(fetchConfigInfo(config.str(), espProcName, espBindingName, esdlDefIdSTR, esdlServiceName));

        if (esdlServiceName.length() == 0)
            throw MakeStringException(-1, "Must provide the ESDL service name as it appears in the ESDL definition.");

        StringBuffer esdlDefinitionName;

        const char * esdlDefId = esdlDefIdSTR.str();
        while(esdlDefId && *esdlDefId &&*esdlDefId != '.')
            esdlDefinitionName.append(*esdlDefId++);

        if (!esdlDefId || !*esdlDefId || *esdlDefId != '.')
            throw MakeStringException(-1, "Invalid ESDL Definition ID format detected <esdldefname>.<ver>");

        esdlDefId++;

        int esdlver = 1;
        if (esdlDefId)
            esdlver = atoi (esdlDefId);

        if (esdlver <= 0)
            throw MakeStringException(-1, "Invalid ESDL Definition version detected: %d", esdlver);

        if (methodstree->getCount("Method") <= 0)
            throw MakeStringException(-1, "Could not find any method configuration entries.");

        if (espProcName.length() == 0)
            throw MakeStringException(-1, "Must provide ESP Process name");

        Owned<IPropertyTree> espproctree;

        if (espBindingName.length() == 0)
        {
            if (espPort.length() <= 0 && espServiceName.length() <= 0)
                throw MakeStringException(-1, "Must provide either ESP Port, or Service Name");

            espproctree.setown(getEspProcessRegistry(espProcName.str(), espPort.str(), espServiceName.str()));

            if (!espproctree)
            {
                StringBuffer msg;
                msg.appendf("Could not find ESP binding associated with Esp Process Name: %s and port %s or %s Esp Service Name", espProcName.str(), espPort.str(), espServiceName.str());
                resp.updateStatus().setCode(-1);
                resp.updateStatus().setDescription(msg.str());
                return false;
            }
        }

        StringBuffer bindingName(espproctree != NULL ? espproctree->queryProp("@name") : espBindingName.str());

        if (existsESDLDefinition(esdlDefinitionName.str(), esdlver))
        {
            IPropertyTreeIterator * iter = methodstree->getElements("Method");
            ForEach(*iter)
            {
               IPropertyTree &item = iter->query();
               const char * methodName = item.queryProp("@name");
               if (!existsESDLMethodDef(esdlDefinitionName.str(), esdlver, esdlServiceName.str(), methodName))
               {
                   StringBuffer msg;
                   msg.appendf("Could not configure: Invalid Method name detected: '%s'. Does not exist in ESDL Service Definition: '%s' version '%d'", methodName, esdlServiceName.str(), esdlver);
                   resp.updateStatus().setCode(-1);
                   resp.updateStatus().setDescription(msg.str());
                   return false;
               }
            }

            StringBuffer msg;
            resp.updateStatus().setCode(publishESDLBinding(bindingName.str(),
                                                           methodstree.get(),
                                                           espProcName.str(),
                                                           espPort,
                                                           esdlDefinitionName.str(),
                                                           esdlver,
                                                           esdlServiceName.str(),
                                                           msg,
                                                           overwrite
                                                           ));

            resp.updateStatus().setDescription(msg.str());
            resp.setOverwrite(overwrite);
            resp.setEspProcName(espProcName.str());
            resp.setEspPort(espPort.str());
        }
        else
        {
            StringBuffer msg;
            msg.appendf("Could not find ESDL Definition for Service: '%s' version '%d'", esdlServiceName.str(), esdlver);
            resp.updateStatus().setCode(-1);
            resp.updateStatus().setDescription(msg.str());
            return false;
        }
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, -1);
    }

    return true;
}

int CWsESDLConfigEx::publishESDLMethod(const char * espProcName, const char * espBindingName, const char * srvDefId, const char * methodName, IPropertyTree * attributesTree, bool overwrite, StringBuffer & message)
{
    if (!espProcName || !*espProcName)
    {
        message.set("Unable to configure method, ESP Process Name not available");
        return -1;
    }
    if (!espBindingName || !*espBindingName)
    {
        message.setf("Unable to configure method, ESP Binding Name not available");
        return -1;
    }
    if (!srvDefId || !*srvDefId)
    {
        message.setf("Unable to configure method, ESDL Binding ID not available");
        return -1;
    }
    if (!methodName || !*methodName)
    {
        message.setf("Unable to configure method, name not available");
        return -1;
    }
    if (!attributesTree)
    {
        message.setf("Unable to configure method '%s', configuration attributes not available", methodName);
        return -1;
    }

    VStringBuffer rxpath("%sBinding[@espprocess='%s'][@espbinding='%s']/Definition[@id='%s']/Methods", ESDL_BINDINGS_ROOT_PATH, espProcName, espBindingName, srvDefId);

    Owned<IRemoteConnection> conn;

    try
    {
        conn.setown(querySDS().connect(rxpath, myProcessSession(), RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT));
    }
    catch (ISDSException * e)
    {
        if (conn)
            conn->close(true);

        message.setf("Unable to operate on Dali path: %s", rxpath.str());
        e->Release();
        return -1;
    }

    //Only lock the branch for the target we're interested in.
    if (!conn)
        throw MakeStringException(-1, "Unable to connect to %s", rxpath.str());

    Owned<IPropertyTree> root = conn->getRoot();
    if (!root.get())
        throw MakeStringException(-1, "Unable to open %s", rxpath.str());

    VStringBuffer xpath("Method[@name='%s']", methodName);
    Owned<IPropertyTree> oldEnvironment = root->getPropTree(xpath.str());
    if (oldEnvironment.get())
    {
        if (overwrite)
        {
            message.set("Existing method configuration overwritten!");
            root->removeTree(oldEnvironment);
        }
        else
        {
            message.set("Method configuration exists will not overwrite!");
            return -1;
        }
    }

    root->addPropTree("Method", attributesTree);

    conn->commit();
    conn->close(false);

    message.appendf("\nSuccessfully configured Method '%s', associated with ESDL definition '%s', on ESP '%s' and binding '%s'", methodName, srvDefId, espProcName, espBindingName);
    return 0;
}

bool CWsESDLConfigEx::onConfigureESDLBindingMethod(IEspContext &context, IEspConfigureESDLBindingMethodRequest &req, IEspConfigureESDLBindingMethodResponse &resp)
{
    try
    {
        if (!context.validateFeatureAccess(FEATURE_URL, SecAccess_Write, false))
            throw MakeStringException(ECLWATCH_ROXIE_QUERY_ACCESS_DENIED, "Failed to Configure ESDL Method. Permission denied.");

        StringBuffer username;
        context.getUserID(username);
        DBGLOG("CWsESDLConfigEx::onConfigureESDBindingLMethod User=%s",username.str());

        StringBuffer espProcName(req.getEspProcName());
        StringBuffer espBindingName(req.getEspBindingName());
        StringBuffer espPort(req.getEspPort());
        StringBuffer espServiceName(req.getEsdlServiceName());
        StringBuffer esdlServiceName(req.getEsdlServiceName());
        StringBuffer esdlDefIdSTR(req.getEsdlDefinitionID());
        StringBuffer config(req.getConfig());

        Owned<IPropertyTree> methodstree;

        if (config.length() > 0)
            methodstree.setown(fetchConfigInfo(config, espProcName, espBindingName, esdlDefIdSTR, esdlServiceName));

       bool override = req.getOverwrite();

       StringBuffer esdlDefinitionName;

        const char * esdlDefId = esdlDefIdSTR.str();
        while (esdlDefId && *esdlDefId != '.')
            esdlDefinitionName.append(*esdlDefId++);
        if (!esdlDefId || !*esdlDefId || *esdlDefId != '.')
            throw MakeStringException(-1, "Invalid ESDL Definition ID format detected <esdldefname>.<ver>");

        esdlDefId++;

        int esdlver = 1;
        if (esdlDefId)
            esdlver = atoi(esdlDefId);

        if (esdlver <= 0)
            throw MakeStringException(-1, "Invalid ESDL Definition version detected: %d", esdlver);

        if (esdlServiceName.length() == 0)
        {
            if (esdlDefinitionName.length() == 0)
                throw MakeStringException(-1, "Must provide either valid EsdlDefinition ID <esdldefname>.<ver> or EsdlServiceName");
            else
                esdlServiceName.set(esdlDefinitionName.str());
        }

        if (!methodstree || methodstree->getCount("Method") <= 0)
            throw MakeStringException(-1, "Could not find any method configuration entries.");

        if (espProcName.length() == 0)
            throw MakeStringException(-1, "Must provide ESP Process name");

        Owned<IPropertyTree> espproctree;

        if (espBindingName.length() == 0)
        {
            if (espPort.length() <= 0 && espServiceName.length() <= 0)
                throw MakeStringException(-1, "Must provide either ESP Port, or Service Name");

            espproctree.setown(getEspProcessRegistry(espProcName.str(), espPort.str(), espServiceName.str()));

            if (!espproctree)
            {
                StringBuffer msg;
                msg.appendf(
                        "Could not find ESP binding associated with Esp Process Name: %s and port %s or %s Esp Service Name",
                        espProcName.str(), espPort.str(), espServiceName.str());
                resp.updateStatus().setCode(-1);
                resp.updateStatus().setDescription(msg.str());
                return false;
            }
        }

        StringBuffer bindingName(espproctree != NULL ? espproctree->queryProp("@name") : espBindingName.str());

        if (existsESDLDefinition(esdlDefinitionName.str(), esdlver))
        {
            IPropertyTreeIterator * iter = methodstree->getElements("Method");
            ForEach(*iter)
            {
                IPropertyTree &item = iter->query();
                const char * methodName = item.queryProp("@name");
                if (!existsESDLMethodDef(esdlDefinitionName.str(), esdlver, esdlServiceName.str(), methodName))
                {
                    StringBuffer msg;
                    msg.appendf(
                            "Could not configure: Invalid Method name detected: '%s'. Does not exist in ESDL Service Definition: '%s' version '%d'",
                            methodName, esdlServiceName.str(), esdlver);
                    resp.updateStatus().setCode(-1);
                    resp.updateStatus().setDescription(msg.str());
                    return false;
                }
                else
                {
                    StringBuffer msg;
                    publishESDLMethod(espProcName.str(), espBindingName.str(), esdlDefIdSTR.toLowerCase().str(), methodName, &item, override, msg);
                    resp.updateStatus().setDescription(msg.str());
                }
            }
        }
    }
    catch(IException* e)
    {
       FORWARDEXCEPTION(context, e, -1);
    }

    return true;
}

int CWsESDLConfigEx::getBindingXML(const char * espProcName, const char * espBindingName, StringBuffer & bindingXml, StringBuffer & msg)
{
    if (!espProcName || !*espProcName)
    {
        msg.set("Could not get configuration: Target ESP proc name not available");
        return -1;
    }

    if (!espBindingName || !*espBindingName)
    {
        msg.set("Could not get configuration: Target ESP Binding name not available");
        return -1;
    }

    Owned<IRemoteConnection> globalLock = querySDS().connect(ESDL_BINDINGS_ROOT_PATH, myProcessSession(), RTM_LOCK_WRITE|RTM_CREATE_QUERY, SDS_LOCK_TIMEOUT);
    if (!globalLock)
    {
        msg.set("Unable to connect to ESDL Service binding information in dali");
        return -1;
    }

    IPropertyTree *esdlDefinitions = globalLock->queryRoot();
    if (!esdlDefinitions)
    {
        msg.set("Unable to open ESDL Service binding information in dali");
        return -1;
    }

    VStringBuffer xpath("%s[@espprocess='%s'][@espbinding='%s']", ESDL_BINDING_PATH, espProcName, espBindingName);\
    Owned<IRemoteConnection> conn = querySDS().connect(xpath.str(), myProcessSession(), RTM_LOCK_READ , SDS_LOCK_TIMEOUT);
    if (!conn)
    {
        msg.setf("Could not find binding for ESP proc: %s, and binding: %s", espProcName, espBindingName);
        return -1;
    }
    else
    {
        IPropertyTree * esdlBinding = conn->queryRoot();
        if (esdlBinding)
        {
            toXML(esdlBinding, bindingXml, 0,0);
            msg.setf("Successfully fetched binding for ESP proc: %s, and binding: %s", espProcName, espBindingName);
        }
        else
            msg.setf("Could not fetch binding for ESP proc: %s, and binding: %s", espProcName, espBindingName);
    }
    conn->close(false);
    globalLock->close(false);
    return 0;
}

bool CWsESDLConfigEx::onGetESDLBinding(IEspContext &context, IEspGetESDLBindingRequest &req, IEspGetESDLBindingResponse &resp)
{
    try
    {
        if (!context.validateFeatureAccess(FEATURE_URL, SecAccess_Read, false))
            throw MakeStringException(ECLWATCH_ROXIE_QUERY_ACCESS_DENIED, "Failed to fetch ESDL Service Configuration. Permission denied.");

        StringBuffer username;
        context.getUserID(username);
        DBGLOG("CWsESDLConfigEx::onGetESDLServiceConfig User=%s",username.str());

        StringBuffer espProcName(req.getEspProcName());
        StringBuffer espBindingName(req.getEspBindingName());
        StringBuffer espProcNameFromId;
        StringBuffer espBindingNameFromId;

        const char * esdlBindId = req.getEsdlBindingId();
        if (esdlBindId && *esdlBindId)
        {
            while(esdlBindId && *esdlBindId && *esdlBindId != '.') //RODRIGO: we should have a limit on this...
            {
                espProcNameFromId.append(*esdlBindId++);
            }

            if (espProcNameFromId.length() > 0 && (!esdlBindId || !*esdlBindId || *esdlBindId != '.'))
                throw MakeStringException(-1, "Invalid ESDL Binding ID format detected. <espprocessname>.<espbindingname>");
            else
            {
                espBindingNameFromId.set(++esdlBindId);
            }
        }

        if (espProcName.length() == 0)
        {
            if (espProcNameFromId.length() > 0)
                espProcName.set(espProcNameFromId);
            else
                throw MakeStringException(-1, "Must provide ESP Process name");
        }
        else if (espProcNameFromId.length() > 0)
        {
            if (strcmp(espProcName.str(), espProcNameFromId.str())!=0)
                throw MakeStringException(-1, "EspProcessName passed in doesn't match EspProcName from EsdlId passed in, which one is correct?");
        }

        if (espBindingName.length() == 0)
        {
            if (espBindingNameFromId.length() > 0)
                espBindingName.set(espBindingNameFromId);
        }
        else if (espBindingNameFromId.length() > 0)
        {
            if (strcmp(espBindingName.str(), espBindingNameFromId.str())!=0)
                throw MakeStringException(-1, "EspBindingName passed in doesn't match EspBindingName from EsdlId passed in, which one is correct?");
        }

        resp.setEspProcName(espProcName);
        resp.setBindingName(espBindingName);

        StringBuffer espPort = req.getEspPort();
        StringBuffer msg;
        StringBuffer bindingxml;
        StringBuffer serviceName;

        if (espBindingName.length() == 0)
        {
            IPropertyTree * espproctree = getEspProcessRegistry(espProcName.str(), espPort.str(), serviceName.str());
            if (espproctree)
                espBindingName.set(espproctree->queryProp("@name"));
        }

        resp.updateStatus().setCode(getBindingXML(espProcName.str(), espBindingName.str(), bindingxml, msg));
        resp.setConfigXML(bindingxml.str());
        resp.updateStatus().setDescription(msg.str());
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, -1);
    }

    return true;
}

bool CWsESDLConfigEx::onEcho(IEspContext &context, IEspEchoRequest &req, IEspEchoResponse &resp)
{
    resp.setResponse(req.getRequest());
    return true;
}

bool CWsESDLConfigEx::onDeleteESDLDefinition(IEspContext &context, IEspDeleteESDLDefinitionRequest &req, IEspDeleteESDLRegistryEntryResponse &resp)
{
    resp.updateStatus().setCode(-1);
    if (!context.validateFeatureAccess(FEATURE_URL, SecAccess_Full, false))
        throw MakeStringException(ECLWATCH_ROXIE_QUERY_ACCESS_DENIED, "Failed to DELETE ESDL entry. Permission denied.");

    StringBuffer esdlDefinitionId(req.getId());
    if (esdlDefinitionId.length()<=0)
    {
        resp.updateStatus().setDescription("Must provide the target ESDL definition ID (name.version)");
        return false;
    }

    esdlDefinitionId.toLowerCase();

    Owned<IRemoteConnection> conn = querySDS().connect(ESDL_DEFS_ROOT_PATH, myProcessSession(), RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT);
    if (!conn)
        throw MakeStringException(-1, "Unable to connect to %s dali path", ESDL_DEFS_ROOT_PATH);

    Owned<IPropertyTree> root = conn->getRoot();
    if (!root)
        throw MakeStringException(-1, "Unable to open %s dali path", ESDL_DEFS_ROOT_PATH);

    if (isESDLDefinitionBound(esdlDefinitionId.str()))
        throw MakeStringException(-1, "Unable to delete ESDL definition %s - It is currently bound", esdlDefinitionId.str());

    VStringBuffer xpath("%s[@id='%s']", ESDL_DEF_ENTRY, esdlDefinitionId.str());

    Owned<IPropertyTree> oldEnvironment = root->getPropTree(xpath.str());
    if (oldEnvironment.get())
    {
        StringBuffer thexml;
        toXML(oldEnvironment.get(), thexml,0,0);
        fprintf(stderr, "DELETING: \n%s\n", thexml.str());
        resp.setDeletedTree(thexml.str());
        resp.updateStatus().setCode(0);
        resp.updateStatus().setDescription("Deleted ESDL Definition");
        root->removeTree(oldEnvironment);
        conn->commit();
    }
    else
    {
        resp.updateStatus().setDescription("Could not find ESDL definition. Verify Id (name.version).");
    }
    conn->close();

    return true;
}

bool CWsESDLConfigEx::onDeleteESDLBinding(IEspContext &context, IEspDeleteESDLBindingRequest &req, IEspDeleteESDLRegistryEntryResponse &resp)
{
    resp.updateStatus().setCode(-1);

    if (!context.validateFeatureAccess(FEATURE_URL, SecAccess_Full, false))
        throw MakeStringException(ECLWATCH_ROXIE_QUERY_ACCESS_DENIED, "Failed to DELETE ESDL entry. Permission denied.");

    StringBuffer espBindingId(req.getId());
    if (espBindingId.length()<=0)
    {
        resp.updateStatus().setDescription("Must provide the target ESDL Binding Id <espprocessname>.<espbindingname>");
        return false;
    }

    Owned<IRemoteConnection> conn = querySDS().connect(ESDL_BINDINGS_ROOT_PATH, myProcessSession(), 0, SDS_LOCK_TIMEOUT);
    if (!conn)
        throw MakeStringException(-1, "Unable to connect to %s dali path", ESDL_BINDINGS_ROOT_PATH);

    Owned<IPropertyTree> root = conn->getRoot();
    if (!root)
        throw MakeStringException(-1, "Unable to open %s dali path", ESDL_BINDINGS_ROOT_PATH);

    VStringBuffer xpath("%s[@id='%s']", ESDL_BINDING_ENTRY, espBindingId.str());
    Owned<IPropertyTree> oldEnvironment = root->getPropTree(xpath.str());
    if (oldEnvironment.get())
    {
        StringBuffer thexml;
        toXML(oldEnvironment.get(), thexml,0,0);
        fprintf(stderr, "DELETING: \n%s\n", thexml.str());
        resp.setDeletedTree(thexml.str());
        resp.updateStatus().setCode(0);
        root->removeTree(oldEnvironment);
        conn->commit();
    }
    else
    {
        resp.updateStatus().setDescription("Could not find ESDL Binding.");
    }
    conn->close();

    return true;
}

void fetchESDLDefinitionFromDaliById(const char *id, StringBuffer & def)
{
    if (!id || !*id)
        throw MakeStringException(-1, "Unable to fetch ESDL Service definition information, service id is not available");

    DBGLOG("ESDL Binding: Fetching ESDL Definition from Dali: %s ", id);

    Owned<IRemoteConnection> conn = querySDS().connect(ESDL_DEFS_ROOT_PATH, myProcessSession(), RTM_LOCK_READ, SDS_LOCK_TIMEOUT);
    if (!conn)
       throw MakeStringException(-1, "Unable to connect to ESDL Service definition information in dali '%s'", ESDL_DEFS_ROOT_PATH);

    conn->close(false); //release lock right away

    IPropertyTree * esdlDefinitions = conn->queryRoot();
    if (!esdlDefinitions)
       throw MakeStringException(-1, "Unable to open ESDL Service definition information in dali '%s'", ESDL_DEFS_ROOT_PATH);

    //There shouldn't be multiple entries here, but if so, we'll use the first one
    VStringBuffer xpath("%s[@id='%s'][1]/esxdl", ESDL_DEF_ENTRY, id);
    IPropertyTree * deftree = esdlDefinitions->getPropTree(xpath);
    if(deftree)
        toXML(deftree, def, 0,0);
}

bool CWsESDLConfigEx::onGetESDLDefinition(IEspContext &context, IEspGetESDLDefinitionRequest&req, IEspGetESDLDefinitionResponse &resp)
{
    StringBuffer id = req.getId();
    StringBuffer definition;
    fetchESDLDefinitionFromDaliById(id.toLowerCase(), definition);

    resp.setId(id.str());
    resp.setXMLDefinition(definition.str());

    return true;
}

bool CWsESDLConfigEx::onListESDLDefinitions(IEspContext &context, IEspListESDLDefinitionsRequest&req, IEspListESDLDefinitionsResponse &resp)
{
    Owned<IRemoteConnection> conn = querySDS().connect(ESDL_DEFS_ROOT_PATH, myProcessSession(), RTM_LOCK_READ, SDS_LOCK_TIMEOUT);
    if (!conn)
       throw MakeStringException(-1, "Unable to connect to ESDL Service definition information in dali '%s'", ESDL_DEFS_ROOT_PATH);

    conn->close(false); //release lock right away

    IPropertyTree * esdlDefinitions = conn->queryRoot();
    if (!esdlDefinitions)
       throw MakeStringException(-1, "Unable to open ESDL Service definition information in dali '%s'", ESDL_DEFS_ROOT_PATH);

    Owned<IPropertyTreeIterator> iter = esdlDefinitions->getElements("Definition");

    IArrayOf<IEspESDLDefinition> list;
    ForEach(*iter)
    {
        IPropertyTree &item = iter->query();
        Owned<IEspESDLDefinition> esdldefinition = createESDLDefinition("","");
        esdldefinition->setId(item.queryProp("@id"));
        esdldefinition->setName(item.queryProp("@name"));
        esdldefinition->setSeq(item.getPropInt("@seq"));
        list.append(*esdldefinition.getClear());
    }
    resp.setDefinitions(list);

    return true;
}

bool CWsESDLConfigEx::onListESDLBindings(IEspContext &context, IEspListESDLBindingsRequest&req, IEspListESDLBindingsResponse &resp)
{
    Owned<IRemoteConnection> conn = querySDS().connect(ESDL_BINDINGS_ROOT_PATH, myProcessSession(), RTM_LOCK_READ, SDS_LOCK_TIMEOUT);
    if (!conn)
       throw MakeStringException(-1, "Unable to connect to ESDL Service definition information in dali '%s'", ESDL_DEFS_ROOT_PATH);

    conn->close(false); //release lock right away

    IPropertyTree * esdlBindings = conn->queryRoot();
    if (!esdlBindings)
       throw MakeStringException(-1, "Unable to open ESDL Service definition information in dali '%s'", ESDL_DEFS_ROOT_PATH);

    Owned<IPropertyTreeIterator> iter = esdlBindings->getElements("Binding");

    IArrayOf<IEspESDLBinding> list;
    ForEach(*iter)
    {
        IPropertyTree &item = iter->query();
        Owned<IEspESDLBinding> esdlbinding = createESDLBinding("","");
        esdlbinding->setId(item.queryProp("@id"));
        esdlbinding->setEspBinding(item.queryProp("@espbinding"));
        esdlbinding->setEspProcess(item.queryProp("@espprocess"));
        list.append(*esdlbinding.getClear());
    }

    resp.setBindings(list);
    return true;
}
