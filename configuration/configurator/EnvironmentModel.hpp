/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems®.

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

#ifndef _ENVIRONMENT_MODEL_HPP_
#define _ENVIRONMENT_MODEL_HPP_

#include <cassert>

namespace CONFIGURATOR
{

class CXSDNodeBase;

class CEnvironmentModelNode
{
public:

    CEnvironmentModelNode(const CEnvironmentModelNode *pParent = 0, int index = 0, CXSDNodeBase *pNode = 0);
    virtual ~CEnvironmentModelNode();

    int getNumberOfChildren() const;

    const CXSDNodeBase* getXSDNode() const
    {
        return m_pXSDNode;
    }

    const CEnvironmentModelNode* getParent() const
    {
        return m_pParent;
    }

    const CEnvironmentModelNode* getChild(int index) const;

protected:

    CXSDNodeBase *m_pXSDNode;
    const CEnvironmentModelNode *m_pParent;
    ::PointerArray *m_pArrChildNodes;
} __attribute__((aligned (32)));

class CEnvironmentModel
{
public:

    static CEnvironmentModel* getInstance();

    virtual ~CEnvironmentModel();

    const CEnvironmentModelNode* getParent(CEnvironmentModelNode *pChild);
    const CEnvironmentModelNode* getChild(CEnvironmentModelNode *pParent, int index);
    int getNumberOfRootNodes() const;
    CEnvironmentModelNode* getRoot(int index = 0);
    const char* getData(const CEnvironmentModelNode *pChild) const;
    const char* getInstanceName(const CEnvironmentModelNode *pChild) const;
    const char* getXSDFileName(const CEnvironmentModelNode *pChild) const;

protected:

    CEnvironmentModel();
    CEnvironmentModelNode *m_pRootNode;
};
}
#endif // _ENVIRONMENT_MODEL_HPP_
