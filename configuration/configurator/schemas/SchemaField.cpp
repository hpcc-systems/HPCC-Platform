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

#include "SchemaField.hpp"

using namespace CONFIGURATOR;

#define IPropertyTree ::IPropertyTree

CField* CField::load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath)
{
    assert(pSchemaRoot != nullptr);
    assert(pParentNode != nullptr);
    assert(pParentNode->getNodeType() == XSD_FIELD_ARRAY);

    if (pSchemaRoot == nullptr || pParentNode == nullptr)
    {
        // TODO: Throw Exception
        return nullptr;
    }

    CField *pField = nullptr;

    if (xpath != nullptr && *xpath != 0)
    {
        IPropertyTree* pTree = pSchemaRoot->queryPropTree(xpath);

        if (pTree == nullptr)
            return nullptr;

        const char* pXPath = pTree->queryProp(XML_ATTR_XPATH);
        assert(pXPath != nullptr && *pXPath != 0);

        if (pXPath == nullptr || *pXPath == 0)
        {
            assert(!"Throw Exception");
            // TODO: throw exception
        }

        if (pXPath != nullptr)
        {
            pField = new CField(pParentNode);
            pField->setXSDXPath(xpath);
            pField->setXPath(pXPath);
        }
        else
        {
            assert(!"xpath can not be be empty!");
            // TODO: throw MakeExceptionFromMap(EX_STR_MISSING_XPATH_IN_FIELD);
        }

        const char *pID = pTree->queryProp(XML_ATTR_ID);

        if (pID != nullptr)
            pField->setID(pID);
   }
   return pField;
}

void CField::dump(::std::ostream& cout, unsigned int offset) const
{
    offset += STANDARD_OFFSET_1;

    quickOutHeader(cout, XSD_FIELD_STR, offset);
    QUICK_OUT(cout, XPath, offset);
    QUICK_OUT(cout, ID, offset);
    QUICK_OUT(cout, XSDXPath,  offset);
    quickOutFooter(cout, XSD_FIELD_STR, offset);
}

CFieldArray* CFieldArray::load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath)
{
    assert(xpath != nullptr);
    assert(pParentNode != nullptr);
    assert(pSchemaRoot != nullptr);
    assert(pParentNode->getNodeType() == XSD_KEY || pParentNode->getNodeType() == XSD_KEYREF || pParentNode->getNodeType() == XSD_UNIQUE);

    if (pSchemaRoot == nullptr || xpath == nullptr || pParentNode == nullptr)
    {
        // TODO: exceptions
        //throw
        return nullptr;
    }

    StringBuffer strXPathExt(xpath);
    CFieldArray *pFieldArray = new CFieldArray(pParentNode);
    pFieldArray->setXSDXPath(xpath);

    Owned<IPropertyTreeIterator> attributeIter = pSchemaRoot->getElements(xpath, ipt_ordered);

    int count = 1;
    ForEach(*attributeIter)
    {
        strXPathExt.setf("%s[%d]", xpath, count);

        CField *pField = CField::load(pFieldArray, pSchemaRoot, strXPathExt.str());

        if (pField != nullptr)
                pFieldArray->append(*pField);

        count++;
    }

    if (pFieldArray->length() == 0)
    {
        delete pFieldArray;
        pFieldArray = nullptr;
    }
    return pFieldArray;
}

void CFieldArray::dump(::std::ostream &cout, unsigned int offset) const
{
    offset+= STANDARD_OFFSET_1;

    quickOutHeader(cout, XSD_FIELD_ARRAY_STR, offset);
    QUICK_OUT(cout, XSDXPath,  offset);
    QUICK_OUT(cout, EnvXPath,  offset);
    QUICK_OUT_ARRAY(cout, offset);
    quickOutFooter(cout, XSD_FIELD_ARRAY_STR, offset);
}


