/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2021 HPCC Systems®.

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

#include "ws_resourcesService.hpp"
#include "exception_util.hpp"

constexpr const char* svcSpec = "spec";
constexpr const char* svcSpecType = "type";
constexpr const char* svcSpecExternalIPs = "externalIPs";
constexpr const char* svcSpecExternalName = "externalName";
constexpr const char* svcSpecClusterIPs = "clusterIPs";
constexpr const char* svcSpecPorts = "ports";
constexpr const char* svcSpecPortsName = "name";
constexpr const char* svcSpecPortsProtocol = "protocol";
constexpr const char* svcSpecPortsPort = "port";
constexpr const char* svcSpecPortsNodePort = "nodePort";
constexpr const char* svcSpecPortsTargetPort = "targetPort";
constexpr const char* svcSpecTypeServiceTypeClusterIP = "ClusterIP";
constexpr const char* svcSpecTypeServiceTypeNodePort = "NodePort";
constexpr const char* svcSpecTypeServiceTypeLoadBalancer = "LoadBalancer";
constexpr const char* svcSpecTypeServiceTypeExternalName = "ExternalName";

constexpr const char* svcStatusLoadBalancerIngress = "status/loadBalancer/ingress";
constexpr const char* svcStatusLoadBalancerIngressIP = "ip";
constexpr const char* svcStatusLoadBalancerIngressHostName = "hostname";

bool CWsResourcesEx::onServiceQuery(IEspContext& context, IEspServiceQueryRequest& req, IEspServiceQueryResponse& resp)
{
    try
    {
        tpWrapper.getServices(context.getClientVersion(), req.getType(), req.getName(), resp.getServices());
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return false;
}

//1. Get Web links defined in ComponentConfig() service/links.
//2. Get Web links using 'kubectl get svc -o json': 
//  the links defined in metadata/annotations using 'hpcc.[application name].io/...' tag.
bool CWsResourcesEx::onWebLinksQuery(IEspContext& context, IEspWebLinksQueryRequest& req, IEspWebLinksQueryResponse& resp)
{
    try
    {
        //Get Web links defined in ComponentConfig() service/links.
        IArrayOf<IConstConfiguredWebLink>& configuredWebLinks = resp.getConfiguredWebLinks();
        Owned<IPropertyTreeIterator> serviceLinkItr = getComponentConfig()->getElements("service/links");
        ForEach(*serviceLinkItr)
        {
            IPropertyTree& serviceLinkTree = serviceLinkItr->query();
            Owned<IEspConfiguredWebLink> configuredWebLink = createConfiguredWebLink();
            configuredWebLink->setName(serviceLinkTree.queryProp("@name"));
            configuredWebLink->setDescription(serviceLinkTree.queryProp("@description"));
            configuredWebLink->setURL(serviceLinkTree.queryProp("@url"));
            configuredWebLinks.append(*configuredWebLink.getLink());
        }

#ifdef _CONTAINERIZED
        //Get Web links using 'kubectl get svc -o json'.
        StringBuffer command, output, error;
        command.append("kubectl get services -o=json");
        unsigned ret = runExternalCommand(output, error, command, nullptr);
        if (ret != 0)
            throw makeStringExceptionV(ECLWATCH_INTERNAL_ERROR, "Failed to run '%s': '%s'", command.str(), error.str());

        if (output.isEmpty())
            throw makeStringExceptionV(ECLWATCH_INTERNAL_ERROR, "The command '%s' returned empty response.", command.str());

        const char* applicationName = getComponentConfig()->queryProp("@application");
        if (isEmptyString(applicationName))
            return false;

        VStringBuffer enabledTag("hpcc.%s.io_fenabled", applicationName);
        VStringBuffer annotationNamePrefix("hpcc.%s.io_f", applicationName);
        IArrayOf<IConstDiscoveredWebLink>& discoveredWebLinks = resp.getDiscoveredWebLinks();
        Owned<IPropertyTree> outputTree = createPTreeFromJSONString(output.str());
        Owned<IPropertyTreeIterator> outputItemItr = outputTree->getElements("items");
        ForEach(*outputItemItr)
        {
            IPropertyTree& outputItemTree = outputItemItr->query();
            IPropertyTree* annotations = outputItemTree.queryPropTree("metadata/annotations");
            if (!annotations || !annotations->getPropBool(enabledTag, false))
                continue;

            Owned<IEspDiscoveredWebLink> discoveredWebLink = createDiscoveredWebLink();
            //Add general information about the items, such as name, port, etc.
            discoveredWebLink->setServiceName(outputItemTree.queryProp("metadata/name"));
            discoveredWebLink->setNameSpace(outputItemTree.queryProp("metadata/namespace"));

            IArrayOf<IConstNamedValue>& annotationList = discoveredWebLink->getAnnotations();
            Owned<IPropertyTreeIterator> annotationsItr = annotations->getElements("*");
            ForEach(*annotationsItr)
            {
                StringBuffer annotationName, decodedAnnotationName;
                annotationsItr->query().getName(annotationName);
                if (!hasPrefix(annotationName, annotationNamePrefix, false))
                    continue;

                //Add all hpcc.[application name].io annotations.
                Owned<IEspNamedValue> annotation = createNamedValue();
                annotation->setValue(annotations->queryProp(annotationName));
                decodePtreeName(decodedAnnotationName, annotationName);
                annotation->setName(decodedAnnotationName);
                annotationList.append(*annotation.getLink());
            }
            double version = context.getClientVersion();
            if (version >= 1.02)
                getServiceConnection(outputItemTree, discoveredWebLink->updateConnection());
            discoveredWebLinks.append(*discoveredWebLink.getLink());
        }
#endif
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return false;
}

void CWsResourcesEx::getServiceConnection(IPropertyTree& rawDataTree, IEspServiceConnection& connection)
{
    IPropertyTree* specTree = rawDataTree.queryPropTree(svcSpec);
    if (!specTree)
        return;

    connection.setType(specTree->queryProp(svcSpecType));
    getServiceExternalIP(rawDataTree, connection);
    getServiceConnectionPorts(specTree, connection);
}

void CWsResourcesEx::getServiceExternalIP(IPropertyTree& rawDataTree, IEspServiceConnection& connection)
{
    IPropertyTree* specTree = rawDataTree.queryPropTree(svcSpec);
    const char* type = specTree->queryProp(svcSpecType);
    if (isEmptyString(type))
        return;

    StringArray externalIPs;
    if (strieq(type, svcSpecTypeServiceTypeClusterIP) || strieq(type, svcSpecTypeServiceTypeNodePort))
    {
        if (specTree->hasProp(svcSpecExternalIPs))
            externalIPs.append(specTree->queryProp(svcSpecExternalIPs));
        else
            connection.setExternalIPStatus(CExternalIPStatus_None);
    }
    else if (strieq(type, svcSpecTypeServiceTypeLoadBalancer))
    {
        Owned<IPropertyTreeIterator> itr= rawDataTree.getElements(svcStatusLoadBalancerIngress);
        ForEach(*itr)
        {
            IPropertyTree& tree = itr->query();
            if (tree.hasProp(svcStatusLoadBalancerIngressIP))
                externalIPs.append(tree.queryProp(svcStatusLoadBalancerIngressIP));
            else if (tree.hasProp(svcStatusLoadBalancerIngressHostName))
                externalIPs.append(tree.queryProp(svcStatusLoadBalancerIngressHostName));
        }
        if (specTree->hasProp(svcSpecExternalIPs))
            externalIPs.append(specTree->queryProp(svcSpecExternalIPs));
        if (externalIPs.length() == 0)
            connection.setExternalIPStatus(CExternalIPStatus_Pending);
    }
    else if (strieq(type, svcSpecTypeServiceTypeExternalName))
        externalIPs.append(specTree->queryProp(svcSpecExternalName));
    else
        connection.setExternalIPStatus(CExternalIPStatus_Unknown);
    if (externalIPs.length() > 0)
        connection.setExternalIPs(externalIPs);
}

void CWsResourcesEx::getServiceConnectionPorts(IPropertyTree* specTree, IEspServiceConnection& connection)
{
    IArrayOf<IConstServicePorts> ports;
    Owned<IPropertyTreeIterator> itr= specTree->getElements(svcSpecPorts);
    ForEach(*itr)
    {
        IPropertyTree& tree = itr->query();
        Owned<IEspServicePorts> port = createServicePorts();
        port->setName(tree.queryProp(svcSpecPortsName));
        port->setProtocol(tree.queryProp(svcSpecPortsProtocol));
        port->setPort(tree.getPropInt(svcSpecPortsPort));
        ports.append(*port.getClear());
    }
    connection.setPorts(ports);
}
