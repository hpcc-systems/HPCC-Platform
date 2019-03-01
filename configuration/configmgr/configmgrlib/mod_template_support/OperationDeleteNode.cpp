

#include "OperationDeleteNode.hpp"
#include "EnvironmentMgr.hpp"
#include "TemplateExecutionException.hpp"

void OperationDeleteNode::doExecute(EnvironmentMgr *pEnvMgr, Variables *pVariables)
{
    //
    // If any node IDs found, go delete them
    if (!m_parentNodeIds.empty())
    {
        for (auto &parentNodeId: m_parentNodeIds)
        {
            std::string nodeId = pVariables->doValueSubstitution(parentNodeId);
            pEnvMgr->removeEnvironmentNode(nodeId);
        }
    }
    else if (m_throwOnEmpty)
    {
        throw TemplateExecutionException("No nodes selected for deletion");
    }
}
