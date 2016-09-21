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

#include "SchemaUnique.hpp"

using namespace CONFIGURATOR;

CUnique* CUnique::load(CXSDNodeBase* pParentNode, const ::IPropertyTree *pSchemaRoot, const char* xpath)
{
    assert(pSchemaRoot != nullptr);
    assert(pParentNode != nullptr);
    assert(pParentNode->getNodeType() == XSD_UNIQUE_ARRAY);

    if (pSchemaRoot == nullptr || pParentNode == nullptr)
        return nullptr;    // TODO: Throw Exception

    CUnique *pUnique = nullptr;

    if (xpath != nullptr && *xpath != 0)
    {
        IPropertyTree* pTree = pSchemaRoot->queryPropTree(xpath);

        if (pTree == nullptr)
            return nullptr;

        const char* pName = pTree->queryProp(XML_ATTR_NAME);
        assert(pName != nullptr && *pName != 0);

        if (pName == nullptr || *pName == 0)
            assert(!"Throw Exception name can not be empty");     // TODO: throw exception
        else
        {
            pUnique = new CUnique(pParentNode);
            pUnique->setXSDXPath(xpath);
        }

        const char *pID = pTree->queryProp(XML_ATTR_ID);
        if (pID != nullptr)
            pUnique->setID(pID);
    }
    return pUnique;
}

void CUnique::dump(::std::ostream& cout, unsigned int offset) const
{
    offset += STANDARD_OFFSET_1;

    quickOutHeader(cout, XSD_UNIQUE_STR, offset);
    QUICK_OUT(cout, ID, offset);
    QUICK_OUT(cout, Name, offset);
    QUICK_OUT(cout, XSDXPath,  offset);
    quickOutFooter(cout, XSD_UNIQUE_STR, offset);
}

CUniqueArray* CUniqueArray::load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath)
{
    assert(pSchemaRoot != nullptr);
    if (pSchemaRoot == nullptr || xpath == nullptr)
        return nullptr;

    Owned<IPropertyTreeIterator> attributeIter = pSchemaRoot->getElements(xpath, ipt_ordered);
    StringBuffer strXPathExt;

    CUniqueArray *pUniqueArray = new CUniqueArray(pParentNode);
    pUniqueArray->setXSDXPath(xpath);

    int count = 1;
    ForEach(*attributeIter)
    {
        strXPathExt.setf("%s[%d]", xpath, count);

        CUnique *pUnique = CUnique::load(pUniqueArray, pSchemaRoot, strXPathExt.str());

        if (pUnique != nullptr)
            pUniqueArray->append(*pUnique);

        count++;
    }

    if (pUniqueArray->length() == 0)
    {
        delete pUniqueArray;
        pUniqueArray = nullptr;
    }
    return pUniqueArray;
}

void CUniqueArray::dump(::std::ostream &cout, unsigned int offset) const
{
    offset+= STANDARD_OFFSET_1;

    quickOutHeader(cout, XSD_UNIQUE_ARRAY_STR, offset);
    QUICK_OUT(cout, XSDXPath,  offset);
    QUICK_OUT(cout, EnvXPath,  offset);
    QUICK_OUT_ARRAY(cout, offset);
    quickOutFooter(cout, XSD_UNIQUE_ARRAY_STR, offset);
}


