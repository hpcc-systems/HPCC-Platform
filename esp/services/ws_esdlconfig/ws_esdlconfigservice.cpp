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
#include "TpWrapper.hpp"

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

IPropertyTree * fetchConfigInfo(const char * config,
                                const char* bindingId)
{
    IPropertyTree * methodstree = NULL;

    if (!config || !*config)
    {
        throw MakeStringException(-1,"Empty config detected");
    }
    else
    {
        Owned<IPropertyTree>  configTree = createPTreeFromXMLString(config, ipt_caseInsensitive);
        //Now let's figure out the structure of the configuration passed in...

        StringBuffer rootname;
        configTree->getName(rootname);

        if (stricmp(rootname.str(), "Binding") == 0)
        {
            if(stricmp(bindingId, configTree->queryProp("@id")) != 0)
                throw MakeStringException(-1, "Binding id in the config tree doesn't match binding id provided");
        }

        IPropertyTree * deftree = NULL;
        if (stricmp(rootname.str(), "Definition") == 0)
            deftree = configTree;
        else
            deftree = configTree->queryBranch("Definition[1]");

        if (deftree)
        {
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

void CWsESDLConfigEx::init(IPropertyTree *cfg, const char *process, const char *service)
{
    if(cfg == NULL)
        throw MakeStringException(-1, "can't initialize CWsESDLConfigEx, cfg is NULL");

#ifdef _DEBUG
    StringBuffer thexml;
    toXML(cfg, thexml,0,0);
    fprintf(stderr, "%s", thexml.str());
#endif

    StringBuffer xpath;
    xpath.appendf("Software/EspProcess[@name=\"%s\"]/EspService[@name=\"%s\"]", process, service);
    IPropertyTree* servicecfg = cfg->getPropTree(xpath.str());

    if(servicecfg == NULL)
        throw MakeStringException(-1, "config not found for service %s/%s",process, service);

    m_isDetachedFromDali = false;
    m_esdlStore.setown(createEsdlCentralStore());
}

bool CWsESDLConfigEx::onPublishESDLDefinition(IEspContext &context, IEspPublishESDLDefinitionRequest & req, IEspPublishESDLDefinitionResponse & resp)
{
    try
    {
        if (m_isDetachedFromDali)
          throw MakeStringException(-1, "Cannot publish ESDL Service definition. ESP is currently detached from DALI.");

        context.ensureFeatureAccess(FEATURE_URL, SecAccess_Write, ECLWATCH_ROXIE_QUERY_ACCESS_DENIED, "WsESDLConfigEx::PublishESDLDefinition: Permission denied.");

        Owned<IUserDescriptor> userdesc;
        const char *user = context.queryUserId();
        const char *password = context.queryPassword();
        if (user && *user)
        {
            userdesc.setown(createUserDescriptor());
            userdesc->set(user, password, context.querySignature());
        }

        DBGLOG("CWsESDLConfigEx::onPublishESDLDefinition User=%s",user);

        resp.updateStatus().setCode(0);

        StringBuffer service(req.getServiceName());

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

        if (service.length() == 0)
        {
            Owned<IPropertyTreeIterator> iter = serviceXMLTree->getElements("esxdl/EsdlService");
            StringArray servicenames;
            ForEach (*iter)
            {
                IPropertyTree &item = iter->query();
                StringBuffer lcname(item.queryProp("@name"));
                servicenames.append(lcname.toLowerCase());
            }
            if (servicenames.length() == 0)
                throw MakeStringException(-1, "Could not publish ESDL definition, the definition doesn't contain any service");

            servicenames.sortAscii();
            for (int i = 0; i < servicenames.length(); i++)
            {
                if (i > 0)
                    service.append("-");
                service.append(servicenames.item(i));
            }
            DBGLOG("Constructed esdl definition name %s", service.str());
        }
        else
        {
            StringBuffer serviceXpath;
            serviceXpath.appendf("esxdl/EsdlService[@name=\"%s\"]", service.str());

            if (!serviceXMLTree->hasProp(serviceXpath))
                throw MakeStringException(-1, "Service \"%s\" definition not found in ESDL provided", service.str());
        }

        bool deletePrevious = req.getDeletePrevious();
        resp.setDeletePrevious(deletePrevious);

        StringBuffer newqueryid;
        unsigned newseq = 0;
        StringBuffer msg;

        if (m_esdlStore->addDefinition(service.str(), serviceXMLTree.get(), newqueryid, newseq, user, deletePrevious, msg))
        {
            if (newseq)
                resp.setEsdlVersion(newseq);
        }
        else
        {
            resp.updateStatus().setCode(-1);
            resp.updateStatus().setDescription(msg.str());
            return false;
        }

        msg.appendf("Successfully published %s", newqueryid.str());
        ESPLOG(LogMin, "ESDL Definition '%s' published by user='%s'", newqueryid.str(), (user && *user) ? user : "Anonymous");

        double ver = context.getClientVersion();
        if (ver >= 1.2)
        {
            if (req.getEchoDefinition())
            {
                StringBuffer definitionxml;

                try
                {
                    Owned<IPropertyTree> definitionTree;
                    definitionTree.set(m_esdlStore->fetchDefinition(newqueryid.toLowerCase()));
                    if(definitionTree)
                        toXML(definitionTree, definitionxml);

                    msg.appendf("\nSuccessfully fetched ESDL Defintion: %s from Dali.", newqueryid.str());

                    if (definitionxml.length() == 0 )
                    {
                        //respcode = -1;
                        msg.append("\nDefinition appears to be empty!");
                    }
                    else
                    {
                        if (ver >= 1.4)
                            resp.updateDefinition().setInterface(definitionxml.str());
                        else
                            resp.setXMLDefinition(definitionxml.str());

                        if (definitionTree)
                        {
                            try
                            {
                                if (ver >= 1.4)
                                {
                                    IEspPublishHistory& defhistory = resp.updateDefinition().updateHistory();
                                    addPublishHistory(definitionTree, defhistory);

                                    IArrayOf<IEspESDLService> respservicesresp;
                                    Owned<IPropertyTreeIterator> serviceiter = definitionTree->getElements("esxdl/EsdlService");
                                    ForEach(*serviceiter)
                                    {
                                        Owned<IEspESDLService> esdlservice = createESDLService("","");
                                        IPropertyTree &curservice = serviceiter->query();
                                        esdlservice->setName(curservice.queryProp("@name"));

                                        Owned<IPropertyTreeIterator> methoditer = curservice.getElements("EsdlMethod");
                                        IArrayOf<IEspMethodConfig> respmethodsarray;
                                        ForEach(*methoditer)
                                        {
                                            Owned<IEspMethodConfig> methodconfig = createMethodConfig("","");
                                            IPropertyTree &item = methoditer->query();
                                            methodconfig->setName(item.queryProp("@name"));
                                            respmethodsarray.append(*methodconfig.getClear());
                                        }
                                        esdlservice->setMethods(respmethodsarray);
                                        respservicesresp.append(*esdlservice.getClear());
                                    }

                                    resp.updateDefinition().setServices(respservicesresp);
                                }
                                else
                                {
                                    StringArray esdlServices;
                                    Owned<IPropertyTreeIterator> serviceiter = definitionTree->getElements("Exsdl/EsdlService");
                                    ForEach(*serviceiter)
                                    {
                                        IPropertyTree &curservice = serviceiter->query();
                                        esdlServices.append(curservice.queryProp("@name"));

                                        Owned<IPropertyTreeIterator> iter = curservice.getElements("EsdlMethod");
                                        IArrayOf<IEspMethodConfig> list;
                                        ForEach(*iter)
                                        {
                                            Owned<IEspMethodConfig> methodconfig = createMethodConfig("","");
                                            IPropertyTree &item = iter->query();
                                            methodconfig->setName(item.queryProp("@name"));
                                            list.append(*methodconfig.getClear());
                                        }
                                        resp.setMethods(list);
                                    }
                                    resp.setESDLServices(esdlServices);
                                }
                            }
                            catch (...)
                            {
                                msg.append("\nEncountered error while parsing fetching available methods");
                            }

                        }
                        else
                            msg.append("\nCould not fetch available methods");
                    }
                }
                catch(IException* e)
                {
                    StringBuffer emsg;
                    e->errorMessage(emsg);
                    msg.append("\n").append(emsg.str());
                    resp.updateStatus().setCode(-1);
                    resp.updateStatus().setDescription(msg.str());

                    e->Release();
                    return false;
                }
                catch (...)
                {
                    throw MakeStringException(-1, "Unexpected error while attempting to fetch ESDL definition.");
                }
            }
        }
        resp.setServiceName(service.str());
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

bool CWsESDLConfigEx::onPublishESDLBinding(IEspContext &context, IEspPublishESDLBindingRequest &req, IEspPublishESDLBindingResponse &resp)
{
    try
    {
        if (m_isDetachedFromDali)
            throw MakeStringException(-1, "Cannot publish ESDL Binding. ESP is currently detached from DALI.");

        context.ensureFeatureAccess(FEATURE_URL, SecAccess_Write, ECLWATCH_ROXIE_QUERY_ACCESS_DENIED, "WsESDLConfigEx::PublishESDLBinding: Permission denied.");

        double ver = context.getClientVersion();

        StringBuffer username;
        context.getUserID(username);
        DBGLOG("CWsESDLConfigEx::onPublishESDLBinding User=%s",username.str());

        StringBuffer espProcName(req.getEspProcName());
        StringBuffer espBindingName(req.getEspBindingName());
        StringBuffer espPort(req.getEspPort());
        StringBuffer esdlDefIdSTR(req.getEsdlDefinitionID());
        StringBuffer esdlServiceName(req.getEsdlServiceName());

        bool overwrite = req.getOverwrite();

        StringBuffer config(req.getConfig());

        Owned<IPropertyTree> methodstree;

        if (config.length() != 0)
            methodstree.setown(fetchConfigInfo(config.str(), espProcName, espBindingName, esdlDefIdSTR, esdlServiceName));
        else
        {
            if (ver >= 1.2)
            {
                StringBuffer methodsxml;
                IArrayOf<IConstMethodConfig>& methods = req.getMethods();
                if (methods.ordinality() > 0)
                {
                    methodsxml.set("<Methods>");

                    ForEachItemIn(idx, methods)
                    {
                        IConstMethodConfig& method = methods.item(idx);
                        methodsxml.appendf("<Method name='%s'", method.getName());
                        IArrayOf<IConstNamedValue> & attributes = method.getAttributes();
                        ForEachItemIn(attributesidx, attributes)
                        {
                            IConstNamedValue& att = attributes.item(attributesidx);
                            methodsxml.appendf(" %s='%s'", att.getName(), att.getValue());
                        }
                        methodsxml.append("/>");
                    }
                    methodsxml.append("</Methods>");
                    methodstree.setown(createPTreeFromXMLString(methodsxml.str()));
                }
            }
        }

        if (!methodstree || methodstree->getCount("Method") == 0)
            ESPLOG(LogMin, "Publishing ESDL Binding with no METHODS configured!");

        if (espProcName.length() == 0)
            throw MakeStringException(-1, "Must provide ESP Process name");

        if (espBindingName.length() == 0 && espPort.length() == 0)
            throw MakeStringException(-1, "Must provide either ESP Port, or Binding Name");

        if (m_esdlStore->definitionExists(esdlDefIdSTR.str()))
        {
            if(methodstree)
            {
                Owned<IPropertyTreeIterator> iter = methodstree->getElements("Method");
                StringBuffer methodxpath;
                ForEach(*iter)
                {
                   IPropertyTree &item = iter->query();
                   const char * methodName = item.queryProp("@name");
                   methodxpath.setf("Method[@name='%s']", methodName);
                   if (methodstree->getCount(methodxpath) > 1)
                       throw MakeStringException(-1, "Detected non-unique configuration entry: Method name='%s'", methodName);

                   if (!m_esdlStore->isMethodDefined(esdlDefIdSTR.str(), esdlServiceName, methodName))
                   {
                       StringBuffer msg;
                       if (!esdlServiceName.length())
                           msg.setf("Could not publish ESDL Binding: Please provide target ESDL Service name, and verify method provided is valid: '%s'", methodName);
                       else
                           msg.setf("Could not publish ESDL Binding: Invalid Method name detected: '%s'. Does not exist in ESDL Definition: '%s'", methodName, esdlDefIdSTR.str());
                       resp.updateStatus().setCode(-1);
                       resp.updateStatus().setDescription(msg.str());
                       return false;
                   }
                }
            }

            StringBuffer msg;
            resp.updateStatus().setCode(m_esdlStore->bindService(espBindingName.str(),
                                                           methodstree.get(),
                                                           espProcName.str(),
                                                           espPort.str(),
                                                           esdlDefIdSTR.str(),
                                                           esdlServiceName.str(),
                                                           msg,
                                                           overwrite,
                                                           username.str()
                                                           ));


            if (ver >= 1.2)
            {
                if (req.getEchoBinding())
                {
                    StringBuffer msg;
                    Owned<IPropertyTree> esdlbindingtree = m_esdlStore->getBindingTree(espProcName.str(), espBindingName.str(), msg);
                    if (esdlbindingtree)
                    {
                        if ( ver >= 1.4)
                            addPublishHistory(esdlbindingtree,resp.updateESDLBinding().updateHistory());

                        IArrayOf<IEspMethodConfig> iesmethods;

                        IPropertyTree * def = esdlbindingtree->queryPropTree("Definition[1]");

                        if (def)
                        {
                            StringBuffer defid = def->queryProp("@id");
                            msg.appendf("\nFetched ESDL Biding definition declaration: '%s'.", defid.str());
                            resp.updateESDLBinding().updateDefinition().setId(defid);
                            resp.updateESDLBinding().updateDefinition().setName(def->queryProp("@name"));

                            IArrayOf<IEspMethodConfig> iesmethods;

                            StringBuffer definition;
                            try
                            {
                                m_esdlStore->fetchDefinitionXML(defid.toLowerCase(), definition);
                            }
                            catch (...)
                            {
                                msg.append("\nUnexpected error while attempting to fetch ESDL definition. Will not report available methods");
                            }

                            if (definition.length() > 0)
                            {
                                try
                                {
                                    Owned<IPropertyTree> definitionTree = createPTreeFromXMLString(definition.str(), ipt_caseInsensitive);
                                    Owned<IPropertyTreeIterator> iter = definitionTree->getElements("EsdlService/EsdlMethod");
                                    StringBuffer xpath;
                                    ForEach(*iter)
                                    {
                                        IPropertyTree &item = iter->query();
                                        const char * name = item.queryProp("@name");
                                        xpath.setf("Definition[1]/Methods/Method[@name='%s']", name);
                                        if (!esdlbindingtree->hasProp(xpath.str())) // Adding empty Method entries if we find that those methods have not been configured
                                        {
                                            Owned<IEspMethodConfig> methodconfig = createMethodConfig("","");

                                            methodconfig->setName(name);
                                            iesmethods.append(*methodconfig.getClear());
                                        }
                                    }
                                }
                                catch (...)
                                {
                                    msg.append("\nUnexpected error while attempting to parse ESDL definition. Will not report available methods");
                                }
                            }
                            else
                            {
                                msg.append("\nCould not fetch available methods");
                            }
                        }

                        Owned<IPropertyTreeIterator> iter = esdlbindingtree->getElements("Definition[1]/Methods/Method");
                        ForEach(*iter)
                        {
                            Owned<IEspMethodConfig> methodconfig = createMethodConfig("","");

                            IPropertyTree & cur = iter->query();
                            IArrayOf<IEspNamedValue> iespattributes;
                            Owned<IAttributeIterator> attributes = cur.getAttributes();
                            ForEach(*attributes)
                            {
                                const char * attname = attributes->queryName()+1;
                                if (stricmp(attname, "name")==0)
                                {
                                    methodconfig->setName(attributes->queryValue());
                                }
                                else
                                {
                                    Owned<IEspNamedValue> iespattribute = createNamedValue("","");
                                    iespattribute->setName(attributes->queryName()+1);
                                    iespattribute->setValue(attributes->queryValue());
                                    iespattributes.append(*iespattribute.getClear());
                                }
                            }
                            methodconfig->setAttributes(iespattributes);

                            StringBuffer elementxxml;
                            Owned<IPropertyTreeIterator> elements = cur.getElements("*");
                            ForEach(*elements)
                            {
                                IPropertyTree & element = elements->query();
                                StringBuffer elementxml;
                                toXML(&element, elementxml);
                                elementxxml.append(elementxml);
                            }

                            methodconfig->setElements(elementxxml.str());
                            iesmethods.append(*methodconfig.getClear());
                            resp.updateESDLBinding().updateConfiguration().setMethods(iesmethods);
                        }
                    }
                }
            }

            resp.updateStatus().setDescription(msg.str());
            resp.setOverwrite(overwrite);
            resp.setEspProcName(espProcName.str());
            resp.setEspPort(espPort.str());
        }
        else
        {
            StringBuffer msg;
            msg.appendf("Could not find ESDL Definition for ID : '%s'", esdlDefIdSTR.str());
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

bool CWsESDLConfigEx::onConfigureESDLBindingMethod(IEspContext &context, IEspConfigureESDLBindingMethodRequest &req, IEspConfigureESDLBindingMethodResponse &resp)
{
    int success = 0;
    try
    {
        if (m_isDetachedFromDali)
            throw MakeStringException(-1, "Cannot Configure ESDL Binding Method. ESP is currently detached from DALI.");

        context.ensureFeatureAccess(FEATURE_URL, SecAccess_Write, ECLWATCH_ROXIE_QUERY_ACCESS_DENIED, "WsESDLConfigEx::ConfigureESDLBindingMethod: Permission denied.");

        StringBuffer username;
        context.getUserID(username);
        DBGLOG("CWsESDLConfigEx::onConfigureESDBindingLMethod User=%s",username.str());

        const char* bindingId = req.getEsdlBindingId();
        const char* methodName = req.getMethodName();
        StringBuffer espProcName;
        StringBuffer espBindingName;
        StringBuffer espPort;
        StringBuffer esdlServiceName;
        StringBuffer espServiceName;
        StringBuffer esdlDefIdSTR;
        StringBuffer config(req.getConfig());

        double ver = context.getClientVersion();
        if(ver >= 1.4)
        {
            StringBuffer msg;
            Owned<IPropertyTree> bindingtree = m_esdlStore->getBindingTree(bindingId, msg);
            if(!bindingtree)
                throw MakeStringException(-1, "Can't find esdl binding for id %s", bindingId);
            bindingtree->getProp("@espprocess", espProcName);
            bindingtree->getProp("@espbinding", espBindingName);
            bindingtree->getProp("@port", espPort);
            bindingtree->getProp("Definition[1]/@esdlservice", esdlServiceName);
            bindingtree->getProp("Definition[1]/@id", esdlDefIdSTR);
        }
        else
        {
            espProcName.set(req.getEspProcName());
            espBindingName.set(req.getEspBindingName());
            espPort.set(req.getEspPort());
            espServiceName.set(req.getEsdlServiceName());
            esdlServiceName.set(req.getEsdlServiceName());
            esdlDefIdSTR.set(req.getEsdlDefinitionID());
        }

        Owned<IPropertyTree> methodstree;

        if (config.length() > 0)
            methodstree.setown(fetchConfigInfo(config, bindingId));
        else
        {
            double ver = context.getClientVersion();
            if (ver >= 1.2)
            {

                IConstMethodConfig& method = req.getMethodStructure();
                IArrayOf<IConstNamedValue> & attributes = method.getAttributes();
                const char * methname = method.getName();
                if(methname && *methname && attributes.ordinality() > 0)
                {
                    StringBuffer methodsxml("<Methods>");
                    methodsxml.appendf("<Method name='%s'", method.getName());

                    ForEachItemIn(attributesidx, attributes)
                    {
                        IConstNamedValue& att = attributes.item(attributesidx);
                        methodsxml.appendf(" %s='%s'", att.getName(), att.getValue());
                    }
                    methodsxml.append("/></Methods>");
                    methodstree.setown(createPTreeFromXMLString(methodsxml.str()));
                }
            }
        }

        if (!methodstree || methodstree->getCount("Method") <= 0)
            throw MakeStringException(-1, "Could not find any method configuration entries.");

        bool override = req.getOverwrite();

        StringBuffer esdlDefinitionName;
        int esdlver = 0;
        const char * esdlDefId = esdlDefIdSTR.str();
        if (esdlDefId && *esdlDefId)
        {
            while (esdlDefId && *esdlDefId != '.')
                esdlDefinitionName.append(*esdlDefId++);

            if (!esdlDefId || !*esdlDefId)
                throw MakeStringException(-1, "Invalid ESDL Definition ID format detected: '%s'. Expected format: <esdldefname>.<ver>", esdlDefIdSTR.str());

            esdlDefId++;

            if (esdlDefId)
                esdlver = atoi(esdlDefId);

            if (esdlver <= 0)
                throw MakeStringException(-1, "Invalid ESDL Definition version detected: %d", esdlver);
        }
        else if (ver >= 1.4)
            throw MakeStringException(-1, "Can't find esdl definition for binding %s", bindingId);

        if (ver < 1.4)
        {
            if (esdlServiceName.length() == 0)
            {
               if (esdlDefinitionName.length() == 0)
                    throw MakeStringException(-1, "Must provide either valid EsdlDefinition ID <esdldefname>.<ver> or EsdlServiceName");
                else
                    esdlServiceName.set(esdlDefinitionName.str());
            }

            if (espProcName.length() == 0)
                throw MakeStringException(-1, "Must provide ESP Process name");

            if (espBindingName.length() == 0)
            {
                if (espPort.length() <= 0 && espServiceName.length() <= 0)
                    throw MakeStringException(-1, "Must provide either ESP Port, or Service Name");

                VStringBuffer xpath("/Environment/Software/EspProcess[@name='%s']/EspBinding[@port='%s']", espProcName.str(), espPort.str());
                Owned<IRemoteConnection> conn = querySDS().connect(xpath.str(), myProcessSession(), RTM_LOCK_READ , SDS_LOCK_TIMEOUT_DESDL);
                if (!conn)
                {
                    StringBuffer msg;
                    msg.appendf(
                            "Could not find ESP binding associated with Esp Process '%s' and either port '%s' or Esp Service Name '%s'",
                            espProcName.str(), espPort.isEmpty() ? "N/A" : espPort.str(), espServiceName.isEmpty() ? "N/A" : espServiceName.str());
                    resp.updateStatus().setCode(-1);
                    resp.updateStatus().setDescription(msg.str());
                    return false;
                }
                espBindingName.set(conn->queryRoot()->queryProp("@name"));
            }
        }

        if (m_esdlStore->definitionExists(esdlDefIdSTR.str()))
        {
            StringBuffer methodxpath;
            if (methodName && *methodName)
                methodxpath.appendf("Method[@name='%s']", methodName);
            else
                methodxpath.append("Method");
            Owned<IPropertyTreeIterator> iter = methodstree->getElements(methodxpath.str());
            ForEach(*iter)
            {
                IPropertyTree &item = iter->query();
                StringBuffer methodNameBuf(item.queryProp("@name"));
                const char * methodName = methodNameBuf.str();
                if (!m_esdlStore->isMethodDefined(esdlDefIdSTR.str(), esdlServiceName, methodName))
                {
                    StringBuffer msg;
                    if (!esdlServiceName.length())
                        msg.setf("Could not publish ESDL Binding: Please provide target ESDL Service name, and verify method provided is valid: '%s'", methodName);
                    else
                        msg.setf("Could not publish ESDL Binding: Invalid Method name detected: '%s'. Does not exist in ESDL Service Definition: '%s' version '%d'", methodName, esdlServiceName.str(), esdlver);
                    resp.updateStatus().setCode(-1);
                    resp.updateStatus().setDescription(msg.str());
                    return false;
                }
                else
                {
                    StringBuffer msg;
                    if (ver >= 1.4)
                        success = m_esdlStore->configureMethod(bindingId, methodName, LINK(&item), override, msg);
                    else
                        success = m_esdlStore->configureMethod(espProcName.str(), espBindingName.str(), esdlDefIdSTR.toLowerCase().str(), methodName, LINK(&item), override, msg);
                    resp.updateStatus().setDescription(msg.str());
                    if (success == 0)
                    {
                        double ver = context.getClientVersion();

                        if (ver >= 1.4)
                            ESPLOG(LogMin, "ESDL Binding '%s' configured method '%s' by user='%s' overwrite flag: %s", bindingId, username.isEmpty() ? "Anonymous" : username.str(), methodName, override ? "TRUE" : "FALSE");
                        else
                            ESPLOG(LogMin, "ESDL Binding '%s.%d' configured method '%s' by user='%s' overwrite flag: %s", esdlDefinitionName.str(), esdlver, username.isEmpty() ? "Anonymous" : username.str(), methodName, override ? "TRUE" : "FALSE");

                        if (ver >= 1.2)
                        {
                            StringBuffer msg;
                            Owned<IPropertyTree> esdlbindingtree;
                            if (ver >= 1.4)
                            {
                                esdlbindingtree.setown(m_esdlStore->getBindingTree(bindingId, msg));
                                addPublishHistory(esdlbindingtree, resp.updateESDLBinding().updateHistory());
                            }
                            else
                                esdlbindingtree.setown(m_esdlStore->getBindingTree(espProcName.str(), espBindingName.str(), msg));
                            if (esdlbindingtree)
                            {
                                IArrayOf<IEspMethodConfig> iesmethods;

                                IPropertyTree * def = esdlbindingtree->queryPropTree("Definition[1]");
                                if (def)
                                {
                                    StringBuffer defid = def->queryProp("@id");
                                    msg.appendf("\nFetched ESDL Biding definition declaration: '%s'.", defid.str());
                                    resp.updateESDLBinding().updateDefinition().setId(defid);
                                    resp.updateESDLBinding().updateDefinition().setName(def->queryProp("@name"));

                                    IArrayOf<IEspMethodConfig> iesmethods;

                                    StringBuffer definition;
                                    try
                                    {
                                        m_esdlStore->fetchDefinitionXML(defid.toLowerCase(), definition);
                                    }
                                    catch (...)
                                    {
                                        msg.append("\nUnexpected error while attempting to fetch ESDL definition. Will not report available methods");
                                    }

                                    if (definition.length() > 0)
                                    {
                                        try
                                        {
                                            Owned<IPropertyTree> definitionTree = createPTreeFromXMLString(definition.str(), ipt_caseInsensitive);
                                            Owned<IPropertyTreeIterator> iter = definitionTree->getElements("EsdlService/EsdlMethod");
                                            StringBuffer xpath;
                                            ForEach(*iter)
                                            {
                                                IPropertyTree &item = iter->query();
                                                const char * name = item.queryProp("@name");
                                                xpath.setf("Definition[1]/Methods/Method[@name='%s']", name);
                                                if (!esdlbindingtree->hasProp(xpath.str())) // Adding empty Method entries if we find that those methods have not been configured
                                                {
                                                    Owned<IEspMethodConfig> methodconfig = createMethodConfig("","");

                                                    methodconfig->setName(name);
                                                    iesmethods.append(*methodconfig.getClear());
                                                }
                                            }
                                        }
                                        catch (...)
                                        {
                                            msg.append("\nUnexpected error while attempting to parse ESDL definition. Will not report available methods");
                                        }
                                    }
                                    else
                                    {
                                        msg.append("\nCould not fetch available methods");
                                    }
                                }

                                Owned<IPropertyTreeIterator> iter = esdlbindingtree->getElements("Definition[1]/Methods/Method");
                                ForEach(*iter)
                                {
                                    Owned<IEspMethodConfig> methodconfig = createMethodConfig("","");

                                    IPropertyTree & cur = iter->query();
                                    IArrayOf<IEspNamedValue> iespattributes;
                                    Owned<IAttributeIterator> attributes = cur.getAttributes();
                                    ForEach(*attributes)
                                    {
                                        const char * attname = attributes->queryName()+1;
                                        if (stricmp(attname, "name")==0)
                                        {
                                            methodconfig->setName(attributes->queryValue());
                                        }
                                        else
                                        {
                                            Owned<IEspNamedValue> iespattribute = createNamedValue("","");
                                            iespattribute->setName(attributes->queryName()+1);
                                            iespattribute->setValue(attributes->queryValue());
                                            iespattributes.append(*iespattribute.getClear());
                                        }
                                    }
                                    methodconfig->setAttributes(iespattributes);

                                    StringBuffer elementxxml;
                                    Owned<IPropertyTreeIterator> elements = cur.getElements("*");
                                    ForEach(*elements)
                                    {
                                        IPropertyTree & element = elements->query();
                                        StringBuffer elementxml;
                                        toXML(&element, elementxml);
                                        elementxxml.append(elementxml);
                                    }

                                    methodconfig->setElements(elementxxml.str());
                                    iesmethods.append(*methodconfig.getClear());
                                    resp.updateESDLBinding().updateConfiguration().setMethods(iesmethods);
                                }
                            }
                        }
                    }
                }
            }
        }

        resp.setEspProcName(espProcName.str());
        resp.setEspBindingName(espBindingName.str());
        resp.setEsdlDefinitionID(esdlDefIdSTR.str());
        resp.setEsdlServiceName(esdlServiceName.str());

        if (context.getClientVersion() < 1.2)
        {
            resp.setServiceName(esdlDefinitionName.str());
            resp.setServiceEsdlVersion(esdlver);
        }
    }
    catch(IException* e)
    {
       FORWARDEXCEPTION(context, e, -1);
    }

    resp.updateStatus().setCode(success);

    return true;
}

int CWsESDLConfigEx::getBindingXML(const char * bindingId, StringBuffer & bindingXml, StringBuffer & msg)
{
    Owned<IPropertyTree> esdlBinding = m_esdlStore->getBindingTree(bindingId, msg);
    if (esdlBinding)
    {
        toXML(esdlBinding, bindingXml, 0,0);
        msg.setf("Successfully fetched binding %s", bindingId);
        return 0;
    }
    else
    {
        msg.setf("Could not fetch binding %s", bindingId);
        return -1;
    }
}

bool CWsESDLConfigEx::onGetESDLBinding(IEspContext &context, IEspGetESDLBindingRequest &req, IEspGetESDLBindingResponse &resp)
{
    try
    {
        if (m_isDetachedFromDali)
            throw MakeStringException(-1, "Cannot fetch ESDL Binding. ESP is currently detached from DALI.");

        context.ensureFeatureAccess(FEATURE_URL, SecAccess_Read, ECLWATCH_ROXIE_QUERY_ACCESS_DENIED, "WsESDLConfigEx::GetESDLBinding: Permission denied.");

        double ver = context.getClientVersion();
        StringBuffer username;
        context.getUserID(username);
        DBGLOG("CWsESDLConfigEx::onGetESDLBinding User=%s",username.str());

        StringBuffer espProcName(req.getEspProcName());
        StringBuffer espBindingName(req.getEspBindingName());

        StringBuffer espProcNameFromId;
        StringBuffer espBindingNameFromId;
        const char * esdlBindId = req.getEsdlBindingId();

        if(ver >= 1.4)
        {
            if (!(esdlBindId && *esdlBindId))
            {
                throw MakeStringException(-1, "Must provide EsdlBindingId");
            }
        }
        else
        {
            if (!esdlBindId || !*esdlBindId)
            {
                StringBuffer espPort = req.getEspPort();
                StringBuffer msg;
                StringBuffer serviceName;
                if (espProcName.length() == 0 || (espBindingName.length() == 0 && espPort.length() == 0))
                    throw MakeStringException(-1, "Must provide EsdlBindingId, or EspProcName plus EspBinding or EspPort");
                if (espBindingName.length() == 0)
                {
                    VStringBuffer xpath("/Environment/Software/EspProcess[@name='%s']/EspBinding[@port='%s']", espProcName.str(), espPort.str());
                    Owned<IRemoteConnection> conn = querySDS().connect(xpath.str(), myProcessSession(), RTM_LOCK_READ , SDS_LOCK_TIMEOUT_DESDL);
                    if(conn)
                         espBindingName.set(conn->queryRoot()->queryProp("@name"));
                    else
                        throw MakeStringException(-1, "Can't find any esp binding for port %s", espPort.str());
                }
            }
        }

        StringBuffer msg;
        Owned<IPropertyTree> esdlbindingtree;
        if (esdlBindId && *esdlBindId)
            esdlbindingtree.setown(m_esdlStore->getBindingTree(esdlBindId, msg));
        else
            esdlbindingtree.setown(m_esdlStore->getBindingTree(espProcName.str(), espBindingName.str(), msg));

        if (ver >= 1.1)
        {
            if (esdlbindingtree)
            {
                IPropertyTree * def = esdlbindingtree->queryPropTree("Definition[1]");

                if (def)
                {
                    StringBuffer defid = def->queryProp("@id");
                    msg.appendf("\nFetched ESDL Biding definition declaration: '%s'.", defid.str());
                    resp.updateESDLBinding().updateDefinition().setId(defid);
                    resp.updateESDLBinding().updateDefinition().setName(def->queryProp("@name"));

                    if(ver >= 1.4)
                    {
                        IEspPublishHistory& defhistory = resp.updateESDLBinding().updateDefinition().updateHistory();
                        addPublishHistory(def, defhistory);
                    }

                    IArrayOf<IEspMethodConfig> iesmethods;

                    if (ver >= 1.2 && req.getReportMethodsAvailable())
                    {
                        StringBuffer definition;
                        try
                        {
                            m_esdlStore->fetchDefinitionXML(defid.toLowerCase(), definition);
                        }
                        catch (...)
                        {
                            msg.append("\nUnexpected error while attempting to fetch ESDL definition. Will not report available methods");
                        }

                        if (definition.length() > 0)
                        {
                            try
                            {
                                Owned<IPropertyTree> definitionTree = createPTreeFromXMLString(definition.str(), ipt_caseInsensitive);
                                Owned<IPropertyTreeIterator> iter = definitionTree->getElements("EsdlService/EsdlMethod");
                                StringBuffer xpath;
                                ForEach(*iter)
                                {
                                    IPropertyTree &item = iter->query();
                                    const char * name = item.queryProp("@name");
                                    xpath.setf("Definition[1]/Methods/Method[@name='%s']", name);
                                    if (!esdlbindingtree->hasProp(xpath.str())) // Adding empty Method entries if we find that those methods have not been configured
                                    {
                                        Owned<IEspMethodConfig> methodconfig = createMethodConfig("","");

                                        methodconfig->setName(name);
                                        iesmethods.append(*methodconfig.getClear());
                                    }
                                }
                            }
                            catch (...)
                            {
                                msg.append("\nUnexpected error while attempting to parse ESDL definition. Will not report available methods");
                            }
                        }
                        else
                        {
                            msg.append("\nCould not fetch available methods");
                        }
                    }

                    StringBuffer methodconfigxml;
                    Owned<IPropertyTreeIterator> iter = esdlbindingtree->getElements("Definition[1]/Methods/Method");
                    ForEach(*iter)
                    {
                        Owned<IEspMethodConfig> methodconfig = createMethodConfig("","");

                        IPropertyTree & cur = iter->query();
                        toXML(&cur, methodconfigxml.clear());
                        if (methodconfigxml.length())
                            methodconfig->setXML(methodconfigxml);

                        IArrayOf<IEspNamedValue> iespattributes;
                        Owned<IAttributeIterator> attributes = cur.getAttributes();
                        ForEach(*attributes)
                        {
                            const char * attname = attributes->queryName()+1;
                            if (stricmp(attname, "name")==0)
                            {
                                methodconfig->setName(attributes->queryValue());
                            }
                            else
                            {
                                Owned<IEspNamedValue> iespattribute = createNamedValue("","");
                                iespattribute->setName(attributes->queryName()+1);
                                iespattribute->setValue(attributes->queryValue());
                                iespattributes.append(*iespattribute.getClear());
                            }
                        }
                        methodconfig->setAttributes(iespattributes);
                        iesmethods.append(*methodconfig.getClear());
                    }

                    msg.appendf("\nFetched ESDL Biding Configuration for %d methods.", iesmethods.length());
                    if (req.getIncludeInterfaceDefinition())
                    {
                        StringBuffer definition;
                        try
                        {
                            m_esdlStore->fetchDefinitionXML(defid.toLowerCase(), definition);
                            resp.updateESDLBinding().updateDefinition().setInterface(definition.str());
                            msg.append("\nFetched ESDL Biding definition.");
                        }
                        catch (...)
                        {
                            msg.appendf("\nUnexpected error while attempting to fetch ESDL Definition %s", defid.toLowerCase().str());
                        }
                    }

                    if ( ver >= 1.4)
                        addPublishHistory(esdlbindingtree,resp.updateESDLBinding().updateHistory());

                    resp.updateESDLBinding().updateConfiguration().setMethods(iesmethods);
                    resp.updateStatus().setCode(0);
                }
                else
                {
                    msg.setf("\nCould not find Definition section in ESDL Binding %s", esdlBindId);
                    resp.updateStatus().setCode(-1);
                }
            }
            else
                resp.updateStatus().setCode(-1);
        }

        StringBuffer bindingxml;
        int retcode;
        if (esdlbindingtree)
        {
            toXML(esdlbindingtree, bindingxml, 0,0);
            msg.setf("Successfully fetched binding %s", esdlBindId);
            retcode = 0;
        }
        else
        {
            msg.setf("Could not fetch binding %s", esdlBindId);
            retcode = -1;
        }

        if (ver < 1.1)
            resp.updateStatus().setCode(retcode);
        resp.updateStatus().setDescription(msg.str());
        if (bindingxml.length() > 0)
            resp.setConfigXML(bindingxml.str());
        if (esdlbindingtree)
        {
            resp.setBindingName(esdlbindingtree->queryProp("@espbinding"));
            resp.setEspProcName(esdlbindingtree->queryProp("@espprocess"));
            resp.setEspPort(esdlbindingtree->queryProp("@port"));
            resp.setServiceName(esdlbindingtree->queryProp("Definition/@esdlservice"));
        }
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, -1);
    }

    return true;
}

void CWsESDLConfigEx::addPublishHistory(IPropertyTree * publishedEntryTree, IEspPublishHistory & history)
{
    if (publishedEntryTree)
    {
         history.setLastEditBy(publishedEntryTree->queryProp("@lastEditedBy"));
         history.setCreatedTime(publishedEntryTree->queryProp("@created"));
         history.setPublishBy(publishedEntryTree->queryProp("@publishedBy"));
         history.setLastEditTime(publishedEntryTree->queryProp("@lastEdit"));
    }
    else
        ESPLOG(LogMin, "Could not fetch ESDL publish history!");
}

bool CWsESDLConfigEx::onEcho(IEspContext &context, IEspEchoRequest &req, IEspEchoResponse &resp)
{
    resp.setResponse(req.getRequest());
    return true;
}

bool CWsESDLConfigEx::onDeleteESDLDefinition(IEspContext &context, IEspDeleteESDLDefinitionRequest &req, IEspDeleteESDLRegistryEntryResponse &resp)
{
    resp.updateStatus().setCode(-1);
    if (m_isDetachedFromDali)
        throw MakeStringException(-1, "Cannot delete ESDL Definition. ESP is currently detached from DALI.");

    context.ensureFeatureAccess(FEATURE_URL, SecAccess_Full, ECLWATCH_ROXIE_QUERY_ACCESS_DENIED, "WsESDLConfigEx::DeleteESDLDefinition: Permission denied.");

    StringBuffer esdlDefinitionId(req.getId());
    if (esdlDefinitionId.length()<=0)
    {
        const char * defname = req.getName();
        const char * defver = req.getVersion();

        if( !defname || !*defname ||  !defver || !*defver)
        {
            resp.updateStatus().setDescription("Must provide the target ESDL definition ID or (Definition name and version)");
            return false;
        }
        esdlDefinitionId.setf("%s.%s", defname, defver);
    }

    esdlDefinitionId.toLowerCase();

    StringBuffer errmsg, thexml;
    if (m_esdlStore->deleteDefinition(esdlDefinitionId.str(), errmsg, &thexml))
    {
        StringBuffer username;
        context.getUserID(username);
        ESPLOG(LogMin, "ESDL Definition '%s' Deleted by user='%s'", esdlDefinitionId.str(), username.isEmpty() ? "Anonymous" : username.str());
        resp.setDeletedTree(thexml.str());
        resp.updateStatus().setCode(0);
        VStringBuffer desc("Deleted ESDL Definition %s", esdlDefinitionId.str());
        resp.updateStatus().setDescription(desc.str());
    }
    else
    {
        resp.updateStatus().setDescription(errmsg.str());
    }

    return true;
}

bool CWsESDLConfigEx::onDeleteESDLBinding(IEspContext &context, IEspDeleteESDLBindingRequest &req, IEspDeleteESDLRegistryEntryResponse &resp)
{
    resp.updateStatus().setCode(-1);

    if (m_isDetachedFromDali)
        throw MakeStringException(-1, "Cannot fetch ESDL Binding. ESP is currently detached from DALI.");

    context.ensureFeatureAccess(FEATURE_URL, SecAccess_Full, ECLWATCH_ROXIE_QUERY_ACCESS_DENIED, "WsESDLConfigEx::DeleteESDLBinding: Permission denied.");

    double ver = context.getClientVersion();
    StringBuffer esdlBindingId(req.getId());
    if (esdlBindingId.length()<=0)
    {
        if (ver >= 1.4)
        {
            resp.updateStatus().setDescription("Must provide the target ESDL Binding Id");
            return false;
        }
        else
        {
            const char * espprocname = req.getEspProcess();
            const char * espbindingname = req.getEspBinding();
            if( !espprocname || !*espprocname ||  !espbindingname || !*espbindingname)
            {
                resp.updateStatus().setDescription("Must provide the target ESDL Binding ID, or espprocessname and espbindingname");
                return false;
            }
            Owned<IPropertyTree> esdlBinding = m_esdlStore->fetchBinding(espprocname, espbindingname);
            if (esdlBinding)
                esdlBinding->getProp("@id", esdlBindingId);
            else
            {
                resp.updateStatus().setDescription("No esdl binding found for the esp process and binding provided");
                return false;
            }
        }
    }
    else
    {
        if(ver < 1.4)
        {
            Owned<IPropertyTree> esdlBinding = m_esdlStore->fetchBinding(esdlBindingId.str());
            if (!esdlBinding)
            {
                bool isOldFormat = false;
                int i = 0, j = 0;
                for (i = 0; i < esdlBindingId.length(); i++)
                {
                    if (esdlBindingId.charAt(i) == '.')
                        break;
                }
                if (i < esdlBindingId.length())
                {
                    for (j = i+1; j < esdlBindingId.length(); j++)
                    {
                        if (esdlBindingId.charAt(j) == '.')
                            break;
                    }
                    if (j == esdlBindingId.length())
                        isOldFormat = true;
                }
                if (isOldFormat)
                {
                    StringBuffer proc, binding;
                    proc.append(i, esdlBindingId.str());
                    binding.append(j-i-1, esdlBindingId.str()+i+1);
                    Owned<IPropertyTree> esdlBinding = m_esdlStore->fetchBinding(proc.str(), binding.str());
                    if (esdlBinding)
                        esdlBinding->getProp("@id", esdlBindingId.clear());
                    else
                    {
                        resp.updateStatus().setDescription("No esdl binding found for the esp process and binding provided");
                        return false;
                    }
                }
            }
        }
    }

    StringBuffer errmsg, thexml;
    bool deleted = m_esdlStore->deleteBinding(esdlBindingId.str(), errmsg, &thexml);
    if (deleted)
    {
        StringBuffer username;
        context.getUserID(username);
        ESPLOG(LogMin, "ESDL binding '%s' Deleted by user='%s'", esdlBindingId.str(), username.isEmpty() ? "Anonymous" : username.str());

        resp.setDeletedTree(thexml.str());
        resp.updateStatus().setDescription("Service successfully unbound");
        resp.updateStatus().setCode(0);
    }
    else
    {
        resp.updateStatus().setDescription(errmsg.str());
    }

    return true;
}

bool CWsESDLConfigEx::onGetESDLDefinition(IEspContext &context, IEspGetESDLDefinitionRequest&req, IEspGetESDLDefinitionResponse &resp)
{
     if (m_isDetachedFromDali)
         throw MakeStringException(-1, "Cannot fetch ESDL Definition. ESP is currently detached from DALI.");

    context.ensureFeatureAccess(FEATURE_URL, SecAccess_Read, ECLWATCH_ROXIE_QUERY_ACCESS_DENIED, "WsESDLConfigEx::GetESDLDefinition: Permission denied.");

    StringBuffer id = req.getId();
    StringBuffer definition;
    const char* serviceName = req.getServiceName();

    double ver = context.getClientVersion();

    StringBuffer message;
    int respcode = 0;

    Owned<IPropertyTree> definitionTree;
    try
    {
        if (ver >= 1.3)
        {
            if (!id.length())
            {
                id.set(req.getName());
                if(id.length() > 0)
                {
                    if (!req.getSeq_isNull())
                        id.append(".").append(req.getSeq());
                }
            }
        }
        if (ver > 1.3)
            resp.updateDefinition().setId(id.str());
        else
            resp.setId(id.str());

        definitionTree.set(m_esdlStore->fetchDefinition(id.toLowerCase()));

        if (definitionTree)
        {
            toXML(definitionTree, definition, 0,0);

            if (definition.length() == 0 )
            {
                respcode = -1;
                message.append("\nDefinition appears to be empty!");
            }
            else
                message.setf("Successfully fetched ESDL Defintion: %s from Dali.", id.str());
        }
        else
            throw MakeStringException(-1, "Could not fetch ESDL definition '%s' from Dali.", id.str());
    }
    catch(IException* e)
    {
        StringBuffer msg;
        e->errorMessage(msg);

        resp.updateStatus().setCode(-1);
        resp.updateStatus().setDescription(msg.str());

        e->Release();
        return false;
    }
    catch (...)
    {
        throw MakeStringException(-1, "Unexpected error while attempting to fetch ESDL definition.");
    }

    if (ver >= 1.2)
    {
        if(ver >= 1.4)
        {
            resp.updateDefinition().setInterface(definition.str());

            IEspPublishHistory& defhistory = resp.updateDefinition().updateHistory();
            addPublishHistory(definitionTree, defhistory);
        }
        else
            resp.setXMLDefinition(definition.str());

        if (definition.length() != 0)
        {
            if (req.getReportMethodsAvailable())
            {
                try
                {
                    VStringBuffer xpath("esxdl/EsdlService");
                    if (serviceName && *serviceName)
                        xpath.appendf("[@name='%s']", serviceName);

                    if ( ver >= 1.4)
                    {
                        Owned<IPropertyTreeIterator> services = definitionTree->getElements(xpath.str());
                        IArrayOf<IEspESDLService> servicesarray;
                        ForEach(*services)
                        {
                            IPropertyTree &curservice = services->query();
                            Owned<IEspESDLService> esdlservice = createESDLService("","");
                            esdlservice->setName(curservice.queryProp("@name"));

                            Owned<IPropertyTreeIterator> methods = curservice.getElements("EsdlMethod");
                            IArrayOf<IEspMethodConfig> methodsarray;
                            ForEach(*methods)
                            {
                                Owned<IEspMethodConfig> methodconfig = createMethodConfig("","");
                                IPropertyTree &method = methods->query();
                                methodconfig->setName(method.queryProp("@name"));
                                methodsarray.append(*methodconfig.getClear());
                            }
                            esdlservice->setMethods(methodsarray);
                            servicesarray.append(*esdlservice.getClear());
                        }
                        resp.updateDefinition().setServices(servicesarray);
                    }
                    else
                    {
                        xpath.append("/EsdlMethod");
                        Owned<IPropertyTreeIterator> iter = definitionTree->getElements(xpath.str());
                        IArrayOf<IEspMethodConfig> list;
                        ForEach(*iter)
                        {
                            Owned<IEspMethodConfig> methodconfig = createMethodConfig("","");
                            IPropertyTree &item = iter->query();
                            methodconfig->setName(item.queryProp("@name"));
                            list.append(*methodconfig.getClear());
                        }
                        resp.setMethods(list);
                    }
                }
                catch (...)
                {
                    message.append("\nEncountered error while parsing fetching available methods");
                }
            }

            if (definitionTree)
            {
                try
                {
                    StringArray esdlServices;
                    StringBuffer xpath;
                    if (serviceName && *serviceName)
                        xpath.appendf("EsdlService[@name='%s']", serviceName);
                    else
                        xpath.set("EsdlService");
                    Owned<IPropertyTreeIterator> serviceiter = definitionTree->getElements(xpath.str());
                    ForEach(*serviceiter)
                    {
                        IPropertyTree &item = serviceiter->query();
                        esdlServices.append(item.queryProp("@name"));
                    }
                    resp.setESDLServices(esdlServices);
                }
                catch (...)
                {
                    message.append("\nEncountered error while parsing fetching EsdlServices");
                }
            }
            else
                message.append("\nCould not fetch ESDLServices");
        }
        else
            message.append("\nCould not fetch ESDL services definition details");
    }

    resp.updateStatus().setCode(respcode);
    resp.updateStatus().setDescription(message.str());

    return true;
}

bool CWsESDLConfigEx::onListESDLDefinitions(IEspContext &context, IEspListESDLDefinitionsRequest&req, IEspListESDLDefinitionsResponse &resp)
{
    if (m_isDetachedFromDali)
        throw MakeStringException(-1, "Cannot list ESDL Definitions. ESP is currently detached from DALI.");

    Owned<IPropertyTree> esdlDefinitions = m_esdlStore->getDefinitions();
    if(esdlDefinitions.get() == nullptr)
        return false;

    double ver = context.getClientVersion();

    Owned<IPropertyTreeIterator> iter = esdlDefinitions->getElements("Definition");
    IArrayOf<IEspESDLDefinition> list;
    ForEach(*iter)
    {
        IPropertyTree &item = iter->query();
        Owned<IEspESDLDefinition> esdldefinition = createESDLDefinition("","");
        esdldefinition->setId(item.queryProp("@id"));
        esdldefinition->setName(item.queryProp("@name"));
        esdldefinition->setSeq(item.getPropInt("@seq"));

        if(ver >= 1.4)
        {
            IEspPublishHistory& defhistory = esdldefinition->updateHistory();
            addPublishHistory(&item, defhistory);
        }

        list.append(*esdldefinition.getClear());

    }
    resp.setDefinitions(list);

    return true;
}

bool CWsESDLConfigEx::onListDESDLEspBindings(IEspContext &context, IEspListDESDLEspBindingsReq&req, IEspListDESDLEspBindingsResp &resp)
{
    if (m_isDetachedFromDali)
        throw MakeStringException(-1, "Cannot list ESDL ESP Bindings. ESP is currently detached from DALI.");

    double ver = context.getClientVersion();

    bool includeESDLBindings = req.getIncludeESDLBindingInfo();
    IArrayOf<IEspESPServerEx> allESPServers;
    IArrayOf<IEspESPServerEx> desdlESPServers;
    CTpWrapper dummy;

    IArrayOf<IConstTpEspServer> espServers;
    dummy.getTpEspServers(espServers);
    ForEachItemIn(idx, espServers)
    {
        IConstTpEspServer& server = espServers.item(idx);
        Owned<IEspESPServerEx> desdlespserver = createESPServerEx("","");
        desdlespserver->setName(server.getName());
        desdlespserver->setBuild(server.getBuild());
        desdlespserver->setType(server.getType());
        desdlespserver->setPath(server.getPath());
        desdlespserver->setLogDirectory(server.getLogDirectory());

        IArrayOf<IConstTpBinding> & bindings = server.getTpBindings();
        IArrayOf<IConstTpBindingEx> desdlbindings;
        ForEachItemIn(bindingidx, bindings)
        {
            IConstTpBinding& binding = bindings.item(bindingidx);
            if (stricmp(binding.getServiceType(), "DynamicESDL")==0)
            {
                Owned<IEspTpBindingEx> desdlespbinding = createTpBindingEx("","");
                desdlespbinding->setPort(binding.getPort());
                desdlespbinding->setName(binding.getName());
                desdlespbinding->setProtocol(binding.getProtocol());
                desdlespbinding->setServiceType(binding.getServiceType());
                desdlespbinding->setBindingType(binding.getBindingType());
                desdlespbinding->setService(binding.getService());

                if(includeESDLBindings) //this whole block should be in its own function
                {
                    StringBuffer msg;
                    Owned<IPropertyTree> esdlbindingtree = m_esdlStore->getBindingTree(server.getName(), binding.getName(), msg);
                    if (esdlbindingtree)
                    {
                        IPropertyTree * def = esdlbindingtree->queryPropTree("Definition[1]");

                        StringBuffer defid = def->queryProp("@id");
                        msg.appendf("\nFetched ESDL Biding definition declaration: '%s'.", defid.str());
                        desdlespbinding->updateESDLBinding().updateDefinition().setId(defid);
                        desdlespbinding->updateESDLBinding().updateDefinition().setName(def->queryProp("@name"));

                        if (ver >= 1.4)
                            addPublishHistory(esdlbindingtree,desdlespbinding->updateESDLBinding().updateHistory());

                        IArrayOf<IEspMethodConfig> iesmethods;
                        Owned<IPropertyTreeIterator> iter = esdlbindingtree->getElements("Definition[1]/Methods/Method");
                        ForEach(*iter)
                        {
                            Owned<IEspMethodConfig> methodconfig = createMethodConfig("","");

                            IPropertyTree & cur = iter->query();
                            IArrayOf<IEspNamedValue> iespattributes;
                            Owned<IAttributeIterator> attributes = cur.getAttributes();
                            ForEach(*attributes)
                            {
                                const char * attname = attributes->queryName()+1;
                                if (stricmp(attname, "name")==0)
                                {
                                    methodconfig->setName(attributes->queryValue());
                                }
                                else
                                {
                                    Owned<IEspNamedValue> iespattribute = createNamedValue("","");
                                    iespattribute->setName(attname);
                                    iespattribute->setValue(attributes->queryValue());
                                    iespattributes.append(*iespattribute.getClear());
                                }
                            }
                            methodconfig->setAttributes(iespattributes);
                            iesmethods.append(*methodconfig.getClear());
                        }

                        msg.appendf("\nFetched ESDL Biding Configuration for %d methods.", iesmethods.length());

                        StringBuffer definition;
                        try
                        {
                            m_esdlStore->fetchDefinitionXML(defid.toLowerCase(), definition);
                            if (definition.length() != 0)
                            {
                                desdlespbinding->updateESDLBinding().updateDefinition().setInterface(definition.str());
                                msg.append("\nFetched ESDL Biding definition.");

                                Owned<IPropertyTree> definitionTree = createPTreeFromXMLString(definition.str(), ipt_caseInsensitive);
                                if (definitionTree)
                                {
                                    try
                                    {
                                        Owned<IPropertyTreeIterator> serviceiter = definitionTree->getElements("EsdlService");
                                        StringArray esdlServices;
                                        ForEach(*serviceiter)
                                        {
                                            IPropertyTree &item = serviceiter->query();
                                            esdlServices.append(item.queryProp("@name"));
                                        }
                                        desdlespbinding->updateESDLBinding().updateDefinition().setESDLServices(esdlServices);
                                    }
                                    catch (...)
                                    {
                                        msg.append("\nEncountered error while parsing ");
                                    }
                                }
                                else
                                    msg.append("\nCould not parse ESDL Definition");
                            }
                            else
                                msg.append("\nCould not fetch ESDL Definition");
                        }
                        catch (...)
                        {
                            msg.appendf("\nUnexpected error while attempting to fetch ESDL Definition %s", defid.toLowerCase().str());
                        }

                        desdlespbinding->updateESDLBinding().updateConfiguration().setMethods(iesmethods);
                    }
                }

                desdlbindings.append(*desdlespbinding.getClear());
            }
        }
        if (desdlbindings.ordinality()>0)
        {
            desdlespserver->setTpBindingEx(desdlbindings);
            desdlESPServers.append(*desdlespserver.getClear());
        }
    }

    resp.setESPServers(desdlESPServers);

    return true;
}

static int bindingCompareFunc(IInterface * const *_itm1, IInterface * const* _itm2)
{
    IConstESDLBinding* itm1 = static_cast<IConstESDLBinding*>(*_itm1);
    IConstESDLBinding* itm2 = static_cast<IConstESDLBinding*>(*_itm2);
    int cmp1 = strcmp(itm1->getEspProcess(), itm2->getEspProcess());
    if (cmp1 != 0)
        return cmp1;
    if (itm1->getPort() < itm2->getPort())
        return -1;
    else if (itm1->getPort() > itm2->getPort())
        return 1;
    else
        return 0;
}


void getAllEspProcessesSorted(StringArray& processes)
{
    Owned<IRemoteConnection> conn = querySDS().connect("/Environment/Software", myProcessSession(), RTM_LOCK_READ , SDS_LOCK_TIMEOUT_DESDL);
    if (!conn)
        throw MakeStringException(-1, "Unable to connect to /Environment/Software dali path");
    Owned<IPropertyTreeIterator> iter = conn->queryRoot()->getElements("EspProcess");
    ForEach (*iter)
    {
        IPropertyTree &item = iter->query();
        processes.append(item.queryProp("@name"));
    }

    processes.sortAscii();
}

bool CWsESDLConfigEx::onListESDLBindings(IEspContext &context, IEspListESDLBindingsRequest&req, IEspListESDLBindingsResponse &resp)
{
    if (m_isDetachedFromDali)
        throw MakeStringException(-1, "Cannot list ESDL Bindings. ESP is currently detached from DALI.");

    double ver = context.getClientVersion();
    Owned<IPropertyTree> esdlBindings = m_esdlStore->getBindings();
    IArrayOf<IEspESDLBinding> list;
    if (esdlBindings.get() != nullptr)
    {
        Owned<IPropertyTreeIterator> iter = esdlBindings->getElements("Binding");
        ForEach(*iter)
        {
            IPropertyTree &item = iter->query();
            Owned<IEspESDLBinding> esdlbinding = createESDLBinding("","");
            esdlbinding->setId(item.queryProp("@id"));
            esdlbinding->setEspProcess(item.queryProp("@espprocess"));
            const char* portstr = item.queryProp("@port");
            if (portstr && *portstr)
                esdlbinding->setPort(atoi(portstr));
            else
                esdlbinding->setPort(0);
            esdlbinding->setEspBinding(item.queryProp("@espbinding"));

            if( ver >= 1.4)
            {
                IEspPublishHistory& bindhistory = esdlbinding->updateHistory();
                addPublishHistory(&item, bindhistory);
            }
            list.append(*esdlbinding.getClear());
        }
        list.sort(bindingCompareFunc);
    }

    if (ver >= 1.4)
    {
        StringArray allProcNamesSorted;
        getAllEspProcessesSorted(allProcNamesSorted);
        unsigned procNameInd = 0;
        IArrayOf<IConstEspProcessStruct>& processes = resp.getEspProcesses();
        IConstESDLBinding* lastBinding = nullptr;
        Owned<IEspEspProcessStruct> currentProcess = nullptr;
        Owned<IEspEspPortStruct> currentPort = nullptr;
        for (int i = 0; i < list.length(); i++)
        {
            IConstESDLBinding* binding = &list.item(i);
            bool processCreated = false;
            if (!lastBinding || (strcmp(lastBinding->getEspProcess(), binding->getEspProcess()) != 0))
            {
                const char* curProc = binding->getEspProcess();

                //Include empty ESP processes that are alphabetically smaller than current non-empty process
                for ( ; procNameInd < allProcNamesSorted.length() && strcmp(allProcNamesSorted.item(procNameInd), curProc) < 0; procNameInd++)
                {
                    currentProcess.setown(createEspProcessStruct());
                    currentProcess->setName(allProcNamesSorted.item(procNameInd));
                    processes.append(*currentProcess.getLink());
                }

                if (procNameInd < allProcNamesSorted.length() && streq(allProcNamesSorted.item(procNameInd), curProc))
                {
                    procNameInd++;
                }

                currentProcess.setown(createEspProcessStruct());
                currentProcess->setName(curProc);
                processes.append(*currentProcess.getLink());
                processCreated = true;
            }

            if (processCreated || lastBinding->getPort() != binding->getPort())
            {
                IArrayOf<IConstEspPortStruct>& ports = currentProcess->getPorts();
                currentPort.setown(createEspPortStruct());
                currentPort->setValue(binding->getPort());
                ports.append(*currentPort.getLink());
            }

            IArrayOf<IConstESDLBinding>& bindings = currentPort->getBindings();
            bindings.append(*LINK(binding));
            lastBinding = binding;
        }

        //Include remaining empty ESP processes
        for ( ; procNameInd < allProcNamesSorted.length(); procNameInd++)
        {
            currentProcess.setown(createEspProcessStruct());
            currentProcess->setName(allProcNamesSorted.item(procNameInd));
            processes.append(*currentProcess.getLink());
        }
    }
    else
        resp.setBindings(list);
    return true;
}
