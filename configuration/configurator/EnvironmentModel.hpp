#ifndef _ENVIRONMENT_MODEL_HPP_
#define _ENVIRONMENT_MODEL_HPP_

#include <cassert>

class CXSDNodeBase;
class CEnvironmentModelNode;
class PointerArray;

template <class TYPE>
class CIArrayOf;

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
    PointerArray *m_pArrChildNodes;
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
    //const char* getProcessName(const CEnvironmentModelNode *pChild) const;
    const char* getInstanceName(const CEnvironmentModelNode *pChild) const;
    const char* getXSDFileName(const CEnvironmentModelNode *pChild) const;

protected:

    CEnvironmentModel();
    CEnvironmentModelNode *m_pRootNode;
};

#endif // _ENVIRONMENT_MODEL_HPP_
