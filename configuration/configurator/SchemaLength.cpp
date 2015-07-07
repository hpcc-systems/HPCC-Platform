#include "jptree.hpp"
#include "SchemaLength.hpp"
#include "ConfigSchemaHelper.hpp"

CLength* CLength::load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath)
{
    assert(pSchemaRoot != NULL);

    if (pSchemaRoot == NULL)
    {
        return NULL;
    }

    CLength *pLength = NULL;

    if (xpath && *xpath)
    {
        IPropertyTree* pTree = pSchemaRoot->queryPropTree(xpath);

        if (pTree == NULL)
        {
            return NULL; // no xs:length node
        }

        const char* pValue = pSchemaRoot->getPropTree(xpath)->queryProp(XML_ATTR_VALUE);

        if (pValue != NULL)
        {
            if (atoi(pValue) < 0)
            {
                 throw MakeExceptionFromMap(EX_STR_MISSING_VALUE_ATTRIBUTE_IN_LENGTH);
            }

            pLength = new CLength(pParentNode);
            pLength->setXSDXPath(xpath);
            pLength->setValue(pValue);
        }
        else
        {
            assert(!"value attribute can be empty!");
            throw MakeExceptionFromMap(EX_STR_MISSING_VALUE_ATTRIBUTE_IN_LENGTH);
        }
    }

    return pLength;
}

void CLength::dump(std::ostream& cout, unsigned int offset) const
{
    offset += STANDARD_OFFSET_1;

    QuickOutHeader(cout, XSD_LENGTH_STR, offset);

    QUICK_OUT(cout, Value, offset);

    QuickOutFooter(cout, XSD_LENGTH_STR, offset);
}

