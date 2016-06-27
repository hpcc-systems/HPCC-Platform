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

#include "jptree.hpp"
#include "jarray.hpp"
#include "jlog.hpp"
#include "SchemaAttributes.hpp"
#include "SchemaCommon.hpp"
#include "SchemaAppInfo.hpp"
#include "SchemaSimpleType.hpp"
#include "DocumentationMarkup.hpp"
#include "ConfigSchemaHelper.hpp"
#include "ExceptionStrings.hpp"
#include "SchemaMapManager.hpp"
#include "ConfiguratorMain.hpp"
#include "SchemaKey.hpp"
#include "SchemaKeyRef.hpp"
#include "SchemaSimpleType.hpp"
#include "SchemaAppInfo.hpp"
#include "JSONMarkUp.hpp"
#include "SchemaEnumeration.hpp"
#include "SchemaElement.hpp"

using namespace CONFIGURATOR;

#define StringBuffer ::StringBuffer
#define IPropertyTree ::IPropertyTree

CAttribute::~CAttribute()
{
}

bool CAttribute::isHidden()
{
    if(this->getAnnotation()->getAppInfo() != NULL && !stricmp(this->getAnnotation()->getAppInfo()->getViewType(),"hidden"))
        return true;

    return false;
}

const char* CAttribute::getTitle() const
{
    if (this->m_pAnnotation != NULL && this->m_pAnnotation->getAppInfo() != NULL && this->m_pAnnotation->getAppInfo()->getTitle() != NULL && this->m_pAnnotation->getAppInfo()->getTitle()[0] != 0)
        return this->m_pAnnotation->getAppInfo()->getTitle();
    else
        return this->getName();
}

const char* CAttribute::getXML(const char* pComponent)
{
    if (m_strXML.length() == 0)
        m_strXML.append(getName()).append("=\"").append(getDefault()).append("\"");
    return m_strXML.str();
}

void CAttribute::dump(::std::ostream& cout, unsigned int offset) const
{
    offset+= STANDARD_OFFSET_1;

    QuickOutHeader(cout,XSD_ATTRIBUTE_STR, offset);

    QUICK_OUT(cout, Name,   offset);
    QUICK_OUT(cout, Type,   offset);
    QUICK_OUT(cout, Default,offset);
    QUICK_OUT(cout, Use,    offset);
    QUICK_OUT(cout, XSDXPath,  offset);
    QUICK_OUT(cout, EnvXPath,  offset);
    QUICK_OUT(cout, EnvValueFromXML,  offset);

    if (m_pAnnotation != NULL)
        m_pAnnotation->dump(cout, offset);
    if (m_pSimpleTypeArray != NULL)
        m_pSimpleTypeArray->dump(cout, offset);

    QuickOutFooter(cout,XSD_ATTRIBUTE_STR, offset);
}

void CAttribute::getDocumentation(StringBuffer &strDoc) const
{
    const char *pName = this->getTitle();
    const char *pToolTip = NULL;
    const char *pDefaultValue = this->getDefault();
    const char *pRequired = this->getUse();

    if (m_pAnnotation != NULL && m_pAnnotation->getAppInfo() != NULL)
    {
        const CAppInfo *pAppInfo = m_pAnnotation->getAppInfo();
        const char* pViewType = pAppInfo->getViewType();

        if (pViewType != NULL && stricmp("hidden", pViewType) == 0)
            return; // HIDDEN
        else
            pToolTip = pAppInfo->getToolTip();
    }

    strDoc.appendf("<%s>\n", DM_TABLE_ROW);
    strDoc.appendf("<%s>%s</%s>\n", DM_TABLE_ENTRY, pName, DM_TABLE_ENTRY);
    strDoc.appendf("<%s>%s</%s>\n", DM_TABLE_ENTRY, pToolTip, DM_TABLE_ENTRY);

    if (m_pSimpleTypeArray == NULL)
        strDoc.appendf("<%s>%s</%s>\n", DM_TABLE_ENTRY, pDefaultValue, DM_TABLE_ENTRY);
    else
    {
        StringBuffer strDocTemp(pDefaultValue);
        m_pSimpleTypeArray->getDocumentation(strDocTemp);

        strDoc.appendf("<%s>%s</%s>\n", DM_TABLE_ENTRY, strDocTemp.str(), DM_TABLE_ENTRY);
    }

    if (m_pAnnotation != NULL && m_pAnnotation->getAppInfo() != NULL && m_pAnnotation->getAppInfo()->getDocLineBreak() == true)
        strDoc.appendf("<%s>%s%s</%s>\n", DM_TABLE_ENTRY, DM_LINE_BREAK2, pRequired, DM_TABLE_ENTRY);
    else
        strDoc.appendf("<%s>%s</%s>\n", DM_TABLE_ENTRY, pRequired, DM_TABLE_ENTRY);
    strDoc.appendf("</%s>\n", DM_TABLE_ROW);
}

void CAttribute::getJSON(StringBuffer &strJSON, unsigned int offset, int idx) const
{
    StringBuffer strToolTip;
    StringBuffer strValues;

    if (this->getAnnotation() && this->getAnnotation()->getAppInfo() && this->getAnnotation()->getAppInfo()->getToolTip())
    {
        strToolTip.set(this->getAnnotation()->getAppInfo()->getToolTip());
    }
    if (this->getSimpleTypeArray() &&  this->getSimpleTypeArray()->length() > 0 &&
            this->getSimpleTypeArray()->item(0).getRestriction() && this->getSimpleTypeArray()->item(0).getRestriction()->getEnumerationArray() &&
            this->getSimpleTypeArray()->item(0).getRestriction()->getEnumerationArray()->length() > 0)  // why would I have more than 1 simpletype here for a dropdown?
    {
        int nLength = this->getSimpleTypeArray()->item(0).getRestriction()->getEnumerationArray()->length();

        for (int lidx = 0; lidx < nLength; lidx++)
        {
            if (lidx > 0)
            {
                strValues.append("\" , \"");
            }
            strValues.append(this->getSimpleTypeArray()->item(0).getRestriction()->getEnumerationArray()->item(lidx).getValue());
        }
    }

    StringBuffer strInstanceValues;
    StringBuffer strKeys;
    const CElementArray *pElementArray = dynamic_cast<const CElementArray*>(this->getParentNodeByType(XSD_ELEMENT_ARRAY));

    if (pElementArray != NULL && pElementArray->getParentNodeByType(XSD_ELEMENT_ARRAY) != NULL &&  pElementArray->getParentNodeByType(XSD_ELEMENT_ARRAY)->getNodeType() != XSD_SCHEMA)
    {
        for (int i = 0; i < pElementArray->ordinality(); i++)
        {
            if (i != 0)
            {
                strInstanceValues.append(", ");
                strKeys.append(",");
            }
            else
            {
                strInstanceValues.append("[");
                strKeys.append("[");
            }

            const CAttribute *pAttribute = const_cast<const CAttribute*>(&(pElementArray->item(i).getComplexTypeArray()->item(0).getAttributeArray()->item(idx)));
            assert(pAttribute != NULL);

            strInstanceValues.appendf("\"%s\"",pAttribute->getInstanceValue());
            strKeys.appendf("\"%s\"",pAttribute->getEnvXPath());
        }

        strInstanceValues.append("]");
        strKeys.append("]");
    }
    else
    {
        strInstanceValues.appendf("\"%s\"",this->getInstanceValue());
    }

    CJSONMarkUpHelper::createUIContent(strJSON, offset, strValues.length() > 0 ? JSON_TYPE_DROP_DOWN : JSON_TYPE_INPUT, this->getTitle(),\
                                       strKeys.length() > 0 ? strKeys.str() : this->getEnvXPath(), strToolTip.str(), this->getDefault(), strValues.str(), strInstanceValues.str());
}

void CAttribute::populateEnvXPath(StringBuffer strXPath, unsigned int index)
{
    assert(this->getName() != NULL);

    const char *pChar = strrchr(strXPath.str(),'[');
    assert(pChar != NULL && strlen(pChar) >= 3);

    /*strXPath.setLength(strXPath.length()-strlen(pChar));  // remove [N] from XPath;
    strXPath.appendf("[%d]", index);*/
    strXPath.append("/").append("[@").append(this->getName()).append("]");
    this->setEnvXPath(strXPath.str());

    //PROGLOG("Mapping attribute with XPATH of %s to %p", this->getEnvXPath(), this);

    CConfigSchemaHelper::getInstance()->getSchemaMapManager()->addMapOfXPathToAttribute(this->getEnvXPath(), this);
    CConfigSchemaHelper::getInstance()->appendAttributeXPath(this->getEnvXPath());

    if (this->m_pSimpleTypeArray != NULL)
        m_pSimpleTypeArray->populateEnvXPath(strXPath);
}

void CAttribute::loadXMLFromEnvXml(const IPropertyTree *pEnvTree)
{
    assert(this->getEnvXPath() != NULL);
    assert(this->getConstParentNode()->getEnvXPath() != NULL);

    StringBuffer strXPath(this->getConstParentNode()->getEnvXPath());

    if (pEnvTree->hasProp(strXPath.str()) == true)
    {
        StringBuffer strAttribName("@");

        strAttribName.append(this->getName());
        this->setEnvValueFromXML(pEnvTree->queryPropTree(strXPath.str())->queryProp(strAttribName.str()));
        setInstanceAsValid();

        if (this->m_pSimpleTypeArray != NULL)
            m_pSimpleTypeArray->loadXMLFromEnvXml(pEnvTree);
    }
    else if (stricmp(this->getUse(), XML_ENV_VALUE_REQUIRED) == 0) // check if this a required attribute
    {
        assert(false);  // required value missing
        throw MakeExceptionFromMap(EX_STR_MISSING_REQUIRED_ATTRIBUTE);
    }
}

bool CAttribute::setEnvValueFromXML(const char *p)
{
    if (p == NULL)
    {
        if (this->getDefault() != NULL && *(this->getDefault()) != 0)
        {
            this->setInstanceValue(this->getDefault());
            this->setInstanceAsValid(true);
            return true;
        }

        if (this->getUse() != NULL && stricmp(this->getUse(), TAG_REQUIRED) != 0)
            return true;
        else
        {
            this->setInstanceAsValid(false);
            //assert (!"Missing attribute property, property not marked as optional");

            return false;
        }
    }

    if (this->getTypeNode() != NULL)
    {
        const CXSDBuiltInDataType *pNodeBuiltInType = dynamic_cast<const CXSDBuiltInDataType*>(this->getTypeNode());

        if (pNodeBuiltInType != NULL && pNodeBuiltInType->checkConstraint(p) == false)
        {
            this->setInstanceAsValid(false);
            //assert (!"Invalid value for data type");

            return false;
        }

        if (this->getTypeNode()->getNodeType() == XSD_SIMPLE_TYPE)
            const CSimpleType *pNodeSimpleType = dynamic_cast<const CSimpleType*>(this->getTypeNode());
    }
    if (this->m_ReverseKeyRefArray.length() > 0)
    {
        for (int idx = 0; this->m_ReverseKeyRefArray.length(); idx++)
        {
           CKeyRef *pKeyRef = static_cast<CKeyRef*>((this->m_ReverseKeyRefArray.item(idx)));
            assert(pKeyRef != NULL);

            if (pKeyRef->checkConstraint(p) == false)
            {
                this->setInstanceAsValid(false);
                assert (!"Constraint Violated");

                return false;
            }
        }
    }
    this->setInstanceValue(p);
    this->setInstanceAsValid(true);

    return true;
}

void CAttribute::appendReverseKeyRef(const CKeyRef *pKeyRef)
{
    assert(pKeyRef != NULL);

    if (pKeyRef != NULL)
        this->m_ReverseKeyRefArray.append(static_cast<void*>(const_cast<CKeyRef*>(pKeyRef)));
}

void CAttribute::appendReverseKey(const CKey *pKey)
{
    assert(pKey != NULL);

    if (pKey != NULL)
        this->m_ReverseKeyArray.append(static_cast<void*>(const_cast<CKey*>(pKey)));
}

CAttribute* CAttribute::load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath)
{
    assert(pSchemaRoot != NULL);

    if (pSchemaRoot == NULL)
        return NULL;

    CAttribute *pAttribute = new CAttribute(pParentNode);

    pAttribute->setXSDXPath(xpath);

    Owned<IAttributeIterator> iterAttrib = pSchemaRoot->queryPropTree(xpath)->getAttributes(true);

    ForEach(*iterAttrib)
    {
        if (strcmp(iterAttrib->queryName(), XML_ATTR_NAME) == 0)
            pAttribute->setName(iterAttrib->queryValue());
        else if (strcmp(iterAttrib->queryName(), XML_ATTR_DEFAULT) == 0)
            pAttribute->setDefault(iterAttrib->queryValue());
        else if (strcmp(iterAttrib->queryName(), XML_ATTR_USE) == 0)
            pAttribute->setUse(iterAttrib->queryValue());
        else if (strcmp(iterAttrib->queryName(), XML_ATTR_TYPE) == 0)
        {
            const char *pType = iterAttrib->queryValue();
            assert(pType != NULL && *pType != 0);

            if (pType != NULL && *pType != 0)
            {
                pAttribute->setType(pType);
                CConfigSchemaHelper::getInstance()->addNodeForTypeProcessing(pAttribute);
            }
        }
    }


    StringBuffer strXPathExt(xpath);
    strXPathExt.append("/").append(XSD_TAG_ANNOTATION);

    CAnnotation *pAnnotation = CAnnotation::load(pAttribute, pSchemaRoot, strXPathExt.str());
    if (pAnnotation != NULL)
        pAttribute->setAnnotation(pAnnotation);

    strXPathExt.clear().append(xpath);
    strXPathExt.append("/").append(XSD_TAG_SIMPLE_TYPE);

    CSimpleTypeArray *pSimpleTypeArray = CSimpleTypeArray::load(pAttribute, pSchemaRoot, strXPathExt.str());
    if (pSimpleTypeArray != NULL)
        pAttribute->setSimpleTypeArray(pSimpleTypeArray);

    return pAttribute;
}

const char* CAttributeArray::getXML(const char* /*pComponent*/)
{
    if (m_strXML.length() == 0)
    {
        int length = this->length();

        for (int idx = 0; idx < length; idx++)
        {
            CAttribute &Attribute = this->item(idx);
            m_strXML.append(" ").append(Attribute.getXML(NULL));

            if (idx+1 < length)
                m_strXML.append("\n");
        }
    }
    return m_strXML.str();
}

CAttributeArray* CAttributeArray::load(const char* pSchemaFile)
{
    assert(pSchemaFile != NULL);

    if (pSchemaFile == NULL)
        return NULL;

    Linked<IPropertyTree> pSchemaRoot;

    StringBuffer schemaPath;
    schemaPath.appendf("%s%s", DEFAULT_SCHEMA_DIRECTORY, pSchemaFile);
    pSchemaRoot.setown(createPTreeFromXMLFile(schemaPath.str()));

    StringBuffer strXPathExt("./");
    strXPathExt.append(XSD_TAG_ATTRIBUTE);

    return CAttributeArray::load(NULL, pSchemaRoot, strXPathExt.str());
}

CAttributeArray* CAttributeArray::load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath)
{
    assert(pSchemaRoot != NULL);

    if (pSchemaRoot == NULL || xpath == NULL)
        return NULL;

    StringBuffer strXPathExt(xpath);
    CAttributeArray *pAttribArray = new CAttributeArray(pParentNode);
    pAttribArray->setXSDXPath(xpath);

    Owned<IPropertyTreeIterator> attributeIter = pSchemaRoot->getElements(xpath, ipt_ordered);

    int count = 1;
    ForEach(*attributeIter)
    {
        strXPathExt.clear().append(xpath).appendf("[%d]",count);

        CAttribute *pAttrib = CAttribute::load(pAttribArray, pSchemaRoot, strXPathExt.str());

        if (pAttrib != NULL)
            pAttribArray->append(*pAttrib);
        count++;
    }

    if (pAttribArray->length() == 0)
    {
        delete pAttribArray;
        return NULL;
    }
    return pAttribArray;
}

void CAttributeArray::dump(::std::ostream &cout, unsigned int offset) const
{
    offset+= STANDARD_OFFSET_1;

    QuickOutHeader(cout, XSD_ATTRIBUTE_ARRAY_STR, offset);

    QUICK_OUT(cout, XSDXPath,  offset);
    QUICK_OUT(cout, EnvXPath,  offset);
    QUICK_OUT_ARRAY(cout, offset);

    QuickOutFooter(cout, XSD_ATTRIBUTE_ARRAY_STR, offset);
}


void CAttributeArray::getDocumentation(StringBuffer &strDoc) const
{
    assert(this->getConstParentNode() != NULL);

    strDoc.append(DM_SECT4_BEGIN);

    if (this->getConstParentNode()->getNodeType() == XSD_COMPLEX_TYPE && this->getConstParentNode()->getConstParentNode()->getNodeType() != XSD_COMPLEX_TYPE)
    {
        strDoc.appendf("%s%s%s", DM_TITLE_BEGIN, "Attributes", DM_TITLE_END);  // Attributes is hard coded default
        DEBUG_MARK_STRDOC;
    }
    else
    {
        strDoc.append(DM_TITLE_BEGIN).append(DM_TITLE_END);
        DEBUG_MARK_STRDOC;
    }

    const CComplexType *pParentComplexType = dynamic_cast<const CComplexType*>(this->getConstParentNode());
    const CAttributeGroup *pParentAttributeGroup = dynamic_cast<const CAttributeGroup*>(this->getConstParentNode());

    strDoc.append(DM_TABLE_BEGIN);
    DEBUG_MARK_STRDOC;
    strDoc.append(DM_TABLE_ID_BEGIN);
    DEBUG_MARK_STRDOC;

    if (pParentComplexType != NULL && pParentComplexType->getAnnotation() != NULL && pParentComplexType->getAnnotation()->getAppInfo() != NULL && pParentComplexType->getAnnotation()->getAppInfo()->getDocTableID() != NULL)
    {
        strDoc.append(pParentComplexType->getAnnotation()->getAppInfo()->getDocTableID());
        DEBUG_MARK_STRDOC;
    }
    else if  (pParentAttributeGroup != NULL && pParentAttributeGroup->getAnnotation() != NULL && pParentAttributeGroup->getAnnotation()->getAppInfo() != NULL && pParentAttributeGroup->getAnnotation()->getAppInfo()->getDocTableID() != NULL)
    {
        strDoc.append(pParentAttributeGroup->getAnnotation()->getAppInfo()->getDocTableID());
        DEBUG_MARK_STRDOC;
    }
    else
    {
        static unsigned undefined_counter = 1;
        strDoc.append(DM_TABLE_ID_UNDEFINED).append("-").append(undefined_counter++);
        DEBUG_MARK_STRDOC;
    }

    strDoc.append(DM_TABLE_ID_END);
    DEBUG_MARK_STRDOC;

    strDoc.append(DM_TGROUP4_BEGIN);
    strDoc.append(DM_COL_SPEC4);
    strDoc.append(DM_TBODY_BEGIN);

    DEBUG_MARK_STRDOC;
    QUICK_DOC_ARRAY(strDoc);
    DEBUG_MARK_STRDOC;

    strDoc.append(DM_TBODY_END);
    strDoc.append(DM_TGROUP4_END);
    strDoc.append(DM_TABLE_END);

    strDoc.append(DM_SECT4_END);
}

void CAttributeArray::getJSON(StringBuffer &strJSON, unsigned int offset, int idx ) const
{
    offset += STANDARD_OFFSET_2;

    int lidx=0;

    for (lidx=0; lidx < this->length(); lidx++)
    {
        QuickOutPad(strJSON, offset);
        strJSON.append("{");
        (this->item(lidx)).getJSON(strJSON, offset, lidx);

        if (lidx >= 0 && this->length() > 1 && lidx+1 < this->length())
        {
            strJSON.append("},\n");
        }
    }
    strJSON.append("}");
    offset -= STANDARD_OFFSET_2;
}

void CAttributeArray::populateEnvXPath(StringBuffer strXPath, unsigned int index)
{
    this->setEnvXPath(strXPath);
    QUICK_ENV_XPATH_WITH_INDEX(strXPath, index)
}

void CAttributeArray::loadXMLFromEnvXml(const IPropertyTree *pEnvTree)
{
    assert(pEnvTree != NULL);

    if (pEnvTree->hasProp(this->getEnvXPath()) == false)
        throw MakeExceptionFromMap(EX_STR_XPATH_DOES_NOT_EXIST_IN_TREE);
    else
    {
        try
        {
            QUICK_LOAD_ENV_XML(pEnvTree)
        }
        catch (IException *e)
        {
            if (e->errorCode() == EX_STR_MISSING_REQUIRED_ATTRIBUTE)
                throw e;
        }
    }
}

const CAttribute* CAttributeArray::findAttributeWithName(const char *pName, bool bCaseInsensitive) const
{
    assert (pName != NULL && pName[0] != 0);
    if (pName == NULL || pName[0] == 0 || this->length() == 0)
        return NULL;

    for (int idx = 0; idx < this->length(); idx++)
    {
        if (bCaseInsensitive == true)
        {
            if (stricmp(this->item(idx).getName(), pName) == 0)
                return &(this->item(idx));
        }
        else
        {
            if (strcmp(this->item(idx).getName(), pName) == 0)
                return &(this->item(idx));
        }
    }
    return NULL;
}
const void CAttributeArray::getAttributeNames(StringArray &names, StringArray &titles) const
{
    for(int i = 0; i < this->length(); i++)
    {
        if(!this->item(i).isHidden())
        {
            names.append(this->item(i).getName());
            titles.append(this->item(i).getTitle());
        }
    }
}

bool CAttributeArray::getCountOfValueMatches(const char *pValue) const
{
    return false;
}
