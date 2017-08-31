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

#ifndef _ESPWIZ_WsESDLConfig_HPP__
#define _ESPWIZ_WsESDLConfig_HPP__

#include "ws_esdlconfig_esp.ipp"
#include "ws_topology_esp.ipp"
#include "esdlconfig_errors.h"
#include "dautils.hpp"

static const char* FEATURE_URL="ESDLConfigAccess";

class CWsESDLConfigSoapBindingEx : public CWsESDLConfigSoapBinding
{
public:
    CWsESDLConfigSoapBindingEx(IPropertyTree *cfg, const char *name, const char *process, http_soap_log_level llevel=hsl_none) : CWsESDLConfigSoapBinding(cfg, name, process, llevel) {}
    virtual void getNavigationData(IEspContext &context, IPropertyTree & data){};
};

class CWsESDLConfigEx : public CWsESDLConfig
{
public:
    IMPLEMENT_IINTERFACE;
    virtual ~CWsESDLConfigEx(){};
    virtual void init(IPropertyTree *cfg, const char *process, const char *service);
    static bool existsESDLDefinition(const char * servicename, unsigned ver);
    static bool existsESDLDefinition(const char * definitionid);
    static bool existsESDLMethodDef(const char * esdlDefinitionName, unsigned ver, StringBuffer & esdlServiceName, const char * methodName);
    static int publishESDLBinding(const char * bindingName, IPropertyTree * methodsConfig, const char * espProcName, const char * espPort, const char * esdlDefinitionName, int esdlDefinitionVersion, const char * esdlServiceName, StringBuffer & message, bool overwrite, const char * user);
    static IPropertyTree * getEspProcessRegistry(const char * espprocname, const char * espbingingport, const char * servicename);
    static IPropertyTree * getESDLDefinitionRegistry(const char * wsEclId, bool readonly);
    static void addESDLDefinition(IPropertyTree * queryRegistry, const char * name, IPropertyTree * definitionInfo, StringBuffer &newId, unsigned &newSeq, const char *userid, bool deleteprev);
    static int publishESDLMethod(const char * espProcName, const char * espBindingName, const char * srvDefId, const char * methodName, IPropertyTree * attributesTree, bool readonly, StringBuffer & message);
    static int getBindingXML(const char * espProcName, const char * espBindingName, StringBuffer & bindingXml, StringBuffer & msg);
    static IPropertyTree * getBindingTree(const char * espProcName, const char * espBindingName, StringBuffer & msg);

    bool onGetESDLBinding(IEspContext &context, IEspGetESDLBindingRequest &req, IEspGetESDLBindingResponse &resp);
    bool onEcho(IEspContext &context, IEspEchoRequest &req, IEspEchoResponse &resp);
    bool onPublishESDLDefinition(IEspContext &context, IEspPublishESDLDefinitionRequest &req, IEspPublishESDLDefinitionResponse &resp);
    bool onPublishESDLBinding(IEspContext &context, IEspPublishESDLBindingRequest &req, IEspPublishESDLBindingResponse &resp);
    bool onConfigureESDLBindingMethod(IEspContext &context, IEspConfigureESDLBindingMethodRequest &req, IEspConfigureESDLBindingMethodResponse &resp);
    bool onDeleteESDLBinding(IEspContext &context, IEspDeleteESDLBindingRequest &req, IEspDeleteESDLRegistryEntryResponse &resp);
    bool onDeleteESDLDefinition(IEspContext &context, IEspDeleteESDLDefinitionRequest &req, IEspDeleteESDLRegistryEntryResponse &resp);
    bool onGetESDLDefinition(IEspContext &context, IEspGetESDLDefinitionRequest&req, IEspGetESDLDefinitionResponse &resp);
    bool onListESDLDefinitions(IEspContext &context, IEspListESDLDefinitionsRequest&req, IEspListESDLDefinitionsResponse &resp);
    bool onListESDLBindings(IEspContext &context, IEspListESDLBindingsRequest&req, IEspListESDLBindingsResponse &resp);
    bool onListDESDLEspBindings(IEspContext &context, IEspListDESDLEspBindingsReq&req, IEspListDESDLEspBindingsResp &resp);
};

#endif //_ESPWIZ_WsESDLConfig_HPP__
