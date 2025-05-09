/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2022 HPCC Systems®.

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

#ifdef _USE_OPENLDAP
#include "ldapsecurity.ipp"
#endif

#include "ws_cloudService.hpp"
#include "exception_util.hpp"

constexpr const char* svcs = "items";
constexpr const char* svcsPublic = "publicServices";
constexpr const char* svcSpec = "spec";
constexpr const char* svcSpecType = "type";
constexpr const char* svcMetadata = "metadata";
constexpr const char* svcMetadataManagedFields = "managedFields";
constexpr const char* svcSpecExternalIPs = "externalIPs";
constexpr const char* svcSpecTypeServiceTypeLoadBalancer = "LoadBalancer";
constexpr const char* svcSpecTypeServiceTypeExternalName = "ExternalName";
constexpr const char* svcSpecTypeServiceTypeClusterIP = "ClusterIP";
constexpr const char* svcSpecTypeServiceTypeNodePort = "NodePort";

void CWsCloudEx::init(IPropertyTree* cfg, const char* process, const char* service)
{
    if(cfg == nullptr)
        throw makeStringException(-1, "Can't initialize CWsCloudEx. The cfg is NULL.");

    VStringBuffer xpath("Software/EspProcess[@name=\"%s\"]/EspService[@name=\"%s\"]/PODInfoCacheSeconds", process, service);
    unsigned k8sResourcesInfoCacheSeconds  = cfg->getPropInt(xpath.str(), defaultK8sResourcesInfoCacheForceBuildSeconds);
    xpath.setf("Software/EspProcess[@name=\"%s\"]/EspService[@name=\"%s\"]/PODInfoCacheAutoRebuildSeconds", process, service);
    if (cfg->hasProp(xpath.str()))
        WARNLOG("Found PODInfoCacheAutoRebuildSeconds in ESP config. This is deprecated and will be ignored.");
    k8sResourcesInfoCacheReader.setown(new CK8sResourcesInfoCacheReader("K8s Resource Reader", k8sResourcesInfoCacheSeconds));
    k8sResourcesInfoCacheReader->init();
}

bool CWsCloudEx::onGetPODs(IEspContext& context, IEspGetPODsRequest& req, IEspGetPODsResponse& resp)
{
    try
    {

        Owned<CK8sResourcesInfoCache> k8sResourcesInfoCache = (CK8sResourcesInfoCache*) k8sResourcesInfoCacheReader->getCachedInfo();
        if (k8sResourcesInfoCache == nullptr)
            throw makeStringException(ECLWATCH_INTERNAL_ERROR, "Failed to get POD Info. Please try later.");

        const char* podInfo = k8sResourcesInfoCache->queryPODs();
        if (isEmptyString(podInfo))
            throw makeStringException(ECLWATCH_INTERNAL_ERROR, "Unable to query POD Info. Please try later.");

        if (context.getClientVersion() < 1.02)
        {
            resp.setResult(podInfo);
        } else {
            Owned<IPropertyTree> podsTree = createPTreeFromJSONString(podInfo);
            if (podsTree == nullptr)
                throw makeStringException(ECLWATCH_INTERNAL_ERROR, "Unable to parse POD Info.");

            Owned<IPropertyTreeIterator> podsItr = podsTree->getElements("items");
            IArrayOf<IEspPodItem> respPods;
            ForEach(*podsItr)
            {
                Owned<IEspPodItem> respPod = createPodItem();
                IPropertyTree& podTree = podsItr->query();

                respPod->setName(podTree.queryProp("metadata/name"));
                respPod->setStatus(podTree.queryProp("status/phase"));
                respPod->setCreationTimestamp(podTree.queryProp("metadata/creationTimestamp"));

                Owned<IPropertyTreeIterator> statuses = podTree.getElements("status/containerStatuses");
                int containerCount = 0;
                int totalRestarts = 0;
                int readyCount = 0;
                StringBuffer containerName;
                ForEach(*statuses)
                {
                    containerCount++;
                    IPropertyTree& status = statuses->query();
                    if (status.getPropBool("ready"))
                        readyCount++;
                    totalRestarts += status.getPropInt("restartCount");
                    if (isEmptyString(containerName) && !streq(status.queryProp("name"), "postrun"))
                        containerName.set(status.queryProp("name"));
                }

                respPod->setContainerCount(containerCount);
                respPod->setContainerRestartCount(totalRestarts);
                respPod->setContainerReadyCount(readyCount);
                respPod->setContainerName(containerName.str());

                Owned<IPropertyTreeIterator> ports = podTree.getElements("//ports");
                IArrayOf<IEspPort> respPorts;
                ForEach(*ports)
                {
                    IPropertyTree& port = ports->query();
                    Owned<IEspPort> respPort = createPort();
                    respPort->setContainerPort(port.getPropInt("containerPort"));
                    respPort->setName(port.queryProp("name"));
                    respPort->setProtocol(port.queryProp("protocol"));
                    respPorts.append(*respPort.getLink());
                }
                respPod->setPorts(respPorts);
                respPods.append(*respPod.getLink());
            }

            resp.setPods(respPods);
        }
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

void CWsCloudEx::addJsonPublicService(IPropertyTree& serviceTree, StringBuffer& publicServices)
{
    delimitJSON(publicServices);

    //Remove managedFields from the serviceTree.
    IPropertyTree* metadataTree = serviceTree.queryPropTree(svcMetadata);
    if (metadataTree)
    {
        ICopyArrayOf<IPropertyTree> managedFieldsTrees;
        Owned<IPropertyTreeIterator> managedFieldsItr = metadataTree->getElements(svcMetadataManagedFields);
        ForEach(*managedFieldsItr)
            managedFieldsTrees.append(managedFieldsItr->query());
        ForEachItemIn(i, managedFieldsTrees)
            metadataTree->removeTree(&managedFieldsTrees.item(i));
    }

    toJSON(&serviceTree, publicServices);
}

//The code below reads 'public' services. A 'public' service is a service if the EXTERNAL-IP returned by
// 'kubectl get svc' for the service is not <none> or <unknown>. The code is based on the getServiceExternalIP() in
// https://github.com/kubernetes/kubernetes/blob/759785ea147bc13945d521eaba4a6592cbc0675f/pkg/printers/internalversion/printers.go#L1125.
// The getServiceExternalIP() returns <unknown> if the service type is not "LoadBalancer", "ExternalName", "ClusterIP" or "NodePort".
// When the service type is "ClusterIP" or "NodePort", the getServiceExternalIP() returns <none> if the "externalIPs" is not defined.
const char* CWsCloudEx::buildJsonPublicServices(const char* allServices, StringBuffer& publicServices)
{
    publicServices.set("{\n");
    appendJSONName(publicServices, svcsPublic);
    publicServices.append("[");

    Owned<IPropertyTree> servicesTree = createPTreeFromJSONString(allServices);
    Owned<IPropertyTreeIterator> servicesItr = servicesTree->getElements(svcs);
    ForEach(*servicesItr)
    {
        IPropertyTree& serviceTree = servicesItr->query();
        IPropertyTree* specTree = serviceTree.queryPropTree(svcSpec);
        if (!specTree)
            continue;

        const char* type = specTree->queryProp(svcSpecType);
        if (isEmptyString(type))
            continue;

        if (strieq(type, svcSpecTypeServiceTypeLoadBalancer) || strieq(type, svcSpecTypeServiceTypeExternalName))
            addJsonPublicService(serviceTree, publicServices);
        else if ((strieq(type, svcSpecTypeServiceTypeClusterIP) || strieq(type, svcSpecTypeServiceTypeNodePort)) && specTree->hasProp(svcSpecExternalIPs))
            addJsonPublicService(serviceTree, publicServices);
    }

    publicServices.append("\n]\n");
    publicServices.append("}");
    return publicServices.str();
}

bool CWsCloudEx::onGetServices(IEspContext& context, IEspGetServicesRequest& req, IEspGetServicesResponse& resp)
{
    try
    {
        Owned<CK8sResourcesInfoCache> k8sResourcesInfoCache = (CK8sResourcesInfoCache*) k8sResourcesInfoCacheReader->getCachedInfo();
        if (k8sResourcesInfoCache == nullptr)
            throw makeStringException(ECLWATCH_INTERNAL_ERROR, "Failed to get Service Info. Please try later.");

        const char* allServices = k8sResourcesInfoCache->queryServices();
        if (isEmptyString(allServices))
            throw makeStringException(ECLWATCH_INTERNAL_ERROR, "Unable to query Service Info. Please try later.");

        StringBuffer publicServices;
        resp.setResult(buildJsonPublicServices(allServices, publicServices));
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

// Note: metadata.namespace is only included to ensure the hpcc4j regression tests pass.
// The metadata.labels.app.kubernetes.io/part-of is included to ensure compatibility with
// older client code targeting versions <1.02. Because of the slash it does not render
// correctly when converted to PTree and output as XML, which is OK because its not used
// in the structured XML/SOAP response.
constexpr const char* jsonpath = R"!!(
{"{"}"items": [{range .items[*]}
    {"{"}
        "metadata": {"{"}
            "name": "{.metadata.name}",
            "namespace": "{.metadata.namespace}",
            "creationTimestamp": "{.metadata.creationTimestamp}",
            "labels": {"{"} "app.kubernetes.io/part-of": "{.metadata.labels.app\.kubernetes\.io/part-of}" {"}"}
        {"}"},
        "status": {"{"}
            "phase": "{.status.phase}",
            "containerStatuses": [{range .status.containerStatuses[*]}{"{"}"name": "{.name}","ready": {.ready},"restartCount": {.restartCount}{"}"},{end}]
        {"}"},
        "spec": {"{"}
            "containers": [{range .spec.containers[*]} {"{"}
                "ports": [{range .ports[*]} {"{"}
                    "containerPort": {.containerPort},
                    "name": "{.name}",
                    "protocol": "{.protocol}"
                {"}"},{end}]
            {"}"},{end}]
        {"}"}
    {"}"},{end}]
{"}"}
)!!";


void CK8sResourcesInfoCache::read()
{
    StringBuffer podsBuf, servicesBuf;
    // Due to how IPipeProcess parses parameters we need the jsonpath argument
    // quoted as shown below, or it will be treated as multiple arguments.
    VStringBuffer command("kubectl get pods -o 'jsonpath=%s'", jsonpath);
    readToBuffer(command.str(), podsBuf);
    readToBuffer("kubectl get svc --output=json", servicesBuf);

    if (podsBuf.isEmpty() && servicesBuf.isEmpty())
        return;

    timeCached.setNow();
    if (!podsBuf.isEmpty())
    {
        // Remove trailing commas from json arrays to make it compatible with older client code
        podsBuf.replaceString(",]", " ]");
        pods.swapWith(podsBuf);
    }
    if (!servicesBuf.isEmpty())
        services.swapWith(servicesBuf);
}

void CK8sResourcesInfoCache::readToBuffer(const char* command, StringBuffer& output)
{
    StringBuffer error;
    unsigned ret = runExternalCommand(output, error, command, nullptr);
    if (!error.isEmpty())
        OWARNLOG("runExternalCommand '%s', error: %s", command, error.str());
}
