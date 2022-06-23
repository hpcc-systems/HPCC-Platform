/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

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

#include <algorithm>
#include "CheckPortBindings.hpp"
#include <vector>
#include <set>
#include <map>
#include <memory>
#include <algorithm>


extern "C" DECL_EXPORT HPCCConfigSupport* getCfgMgrSupportInstance(EnvironmentMgr *pEnvMgr)
{
    HPCCConfigSupport *pInstance = new CheckPortBindings(pEnvMgr);
    return pInstance;
}


void  CheckPortBindings::validate(const std::shared_ptr<SchemaItem> &pSchema, const std::shared_ptr<EnvironmentNode> &pEnvironment, Status &status) const
{
    checkEspPortBindings(pSchema, pEnvironment, status);
    checkProcessPortBindings(pSchema, pEnvironment, status);
    m_hwInstancePortUsage.clear();  // clear for next time
}


void CheckPortBindings::checkEspPortBindings(const std::shared_ptr<SchemaItem> &pSchema, const std::shared_ptr<EnvironmentNode> &pEnvironment, Status &status) const
{
    //
    // Collect the ESP process nodes
    std::vector<std::shared_ptr<EnvironmentNode>> espNodes;
    m_pEnvMgr->fetchNodes("/Environment/Software/EspProcess", espNodes);

    //
    // Collect all the EspService nodes into a map based on the service name to make lookup easier later. Also create
    // map for service name to service type
    std::vector<std::shared_ptr<EnvironmentNode>> espServiceNodes;
    std::map<std::string, std::shared_ptr<EnvironmentNode>> espServiceMap;
    std::map<std::string, std::string> espServiceNameToTypeMap;
    m_pEnvMgr->fetchNodes("/Environment/Software/EspService", espServiceNodes);
    for (auto &pEspService: espServiceNodes)
    {
        std::string serviceName = pEspService->getAttributeValue("name");
        auto retVal = espServiceMap.insert({serviceName, pEspService});
        if (!retVal.second)
        {
            status.addMsg(statusMsg::error, pEspService->getId(), "name", "Duplicate ESP Service name detected");
            return;  // any additional validation is not valid because of the name conflict
        }
        espServiceNameToTypeMap[serviceName] = pEspService->getSchemaItem()->getItemType();
    }

    //
    // For each ESP process, validate esp service binding ports are unique by service type
    for (auto &pEspNode: espNodes)
    {
        std::set<std::string> espBindingPorts;
        std::string espProcessName = pEspNode->getAttributeValue("name");

        //
        // Validate esp service binding ports are unique by service type
        std::map<std::string, std::set<std::string>> espBindingPortsByServiceType;
        std::vector<std::shared_ptr<EnvironmentNode>> espBindingNodes;
        m_pEnvMgr->fetchNodes("EspBinding", espBindingNodes, pEspNode);

        for (auto &pEspBindingNode: espBindingNodes)
        {
            std::string serviceName = pEspBindingNode->getAttributeValue("service");
            auto findIt = espServiceMap.find(serviceName);
            if (findIt != espServiceMap.end())
            {
                std::string bindingPort = pEspBindingNode->getAttributeValue("port");
                std::string serviceType = espServiceNameToTypeMap[serviceName];
                auto portIt = espBindingPortsByServiceType.find(serviceType);
                if (portIt == espBindingPortsByServiceType.end())
                {
                    espBindingPortsByServiceType[serviceType] = std::set<std::string>();
                }

                auto retVal = espBindingPortsByServiceType[serviceType].insert(bindingPort);
                if (!retVal.second)
                {
                    status.addMsg(statusMsg::error, pEspBindingNode->getId(), "port", "Binding port (" + bindingPort + ") must be unique for this service type");
                }
                espBindingPorts.insert(bindingPort);  // duplicates ok
            }
            else
            {
                status.addMsg(statusMsg::error, pEspBindingNode->getId(), "service", "Unable to find ESP service");
                return;  // any additional validation is not valid because unable to find the service
            }
        }

        //
        // Now add all of the binding ports used by this ESP to the hw instances
        std::string espControlPort = pEspNode->getAttributeValue("controlPort");
        bool addEspControlPort = espBindingPorts.find(espControlPort) == espBindingPorts.end();
        std::vector<std::shared_ptr<EnvironmentNode>> hwInstanceNodes;
        m_pEnvMgr->fetchNodes("Instance", hwInstanceNodes, pEspNode);
        for (auto &pHwInstanceNode: hwInstanceNodes)
        {
            for (auto &bindingPort: espBindingPorts)
            {
                addPortToHwInstance(pHwInstanceNode->getAttributeValue("netAddress"), bindingPort, pEspNode->getId(), status);
            }
            if (addEspControlPort)
            {
                addPortToHwInstance(pHwInstanceNode->getAttributeValue("netAddress"), espControlPort, pEspNode->getId(), status);
            }
        }
    }
}


void CheckPortBindings::checkProcessPortBindings(const std::shared_ptr<SchemaItem> &pSchema, const std::shared_ptr<EnvironmentNode> &pEnvironment, Status &status) const
{
    struct pathInfo {
        std::string pathToProcessNode;     // path to unique process
        std::string pathToPortDefNode;     // relative path to where port binding is defined for the process
        std::string portAttributeName;     // name of attribute defining port binding
        std::string pathToInstanceNodes;   // relative path to where hw biding instances are defined
    };

    std::vector<pathInfo> processPaths = {
        { "/Environment/Software/DaliServerProcess", "Instance", "port",   "Instance" },
        { "/Environment/Software/RoxieServerProcess", "RoxieFarmProcess", "port",   "RoxieServerProcess" },
        { "/Environment/Software/SashaServerProcess", "Instance", "port",   "Instance" },
        { "/Environment/Software/ThorCluster", ".", "masterport",   "ThorMasterProcess" },
        { "/Environment/Software/ThorCluster", ".", "masterport",   "ThorSlaveProcess" },
        { "/Environment/Software/ThorCluster", ".", "masterport",   "ThorSpareProcess" },
        { "/Environment/Software/ThorCluster", ".", "slaveport",   "ThorMasterProcess" },
        { "/Environment/Software/ThorCluster", ".", "slaveport",   "ThorSlaveProcess" },
        { "/Environment/Software/ThorCluster", ".", "slaveport",   "ThorMasterProcess" },
        { "/Environment/Software/LDAPServerProcess", ".", "ldapPort",   "Instance" },
        { "/Environment/Software/LDAPServerProcess", ".", "ldapSecurePort",   "Instance" }
    };


    //
    // Check all of these for port conflicts
    for (auto &processPathInfo: processPaths)
    {
        //
        // Get unique process nodes and check each
        std::vector<std::shared_ptr<EnvironmentNode>> processNodes;
        m_pEnvMgr->fetchNodes(processPathInfo.pathToProcessNode, processNodes);
        for (auto const &pProcessNode: processNodes)
        {
            //
            // Within each process node, get the nodes that defines the binding port (which may be the current ndde,
            // or a child node.
            std::vector<std::shared_ptr<EnvironmentNode>> portDefNodes;
            m_pEnvMgr->fetchNodes(processPathInfo.pathToPortDefNode, portDefNodes, pProcessNode);
            for (auto const &pPortDefNode: portDefNodes)
            {
                //
                // Get the binding port. Add it to the list of used ports for each intance to which this process is bound.
                std::string bindingPort = pPortDefNode->getAttributeValue(processPathInfo.portAttributeName);
                std::vector<std::shared_ptr<EnvironmentNode>> hwInstanceNodes;
                m_pEnvMgr->fetchNodes(processPathInfo.pathToInstanceNodes, hwInstanceNodes, pProcessNode);
                for (auto const &pHwInstanceNode: hwInstanceNodes)
                {
                    addPortToHwInstance(pHwInstanceNode->getAttributeValue("netAddress"), bindingPort, pProcessNode->getId(), status);
                }
            }
        }
    }
}


void CheckPortBindings::addPortToHwInstance(std::string netAddress, std::string port, std::string nodeId, Status &status) const
{
    auto hwIt = m_hwInstancePortUsage.find(netAddress);
    if (hwIt == m_hwInstancePortUsage.end())
    {
        m_hwInstancePortUsage[netAddress] = std::set<std::string>();
    }

    //
    // Insert the port for the hw instance. If already present, generate an error
    auto ret = m_hwInstancePortUsage[netAddress].insert(port);
    if (!ret.second)
    {
        std::string msg = "Binding to port ";
        msg.append(port).append("  in this process conflicts with a binding in another process for HW instance with netAddress ").append(netAddress);
        status.addMsg(statusMsg::error, nodeId, "", msg);
    }
}
