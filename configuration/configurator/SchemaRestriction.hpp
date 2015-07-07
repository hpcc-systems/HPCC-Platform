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

#ifndef _SCHEMA_RESTRICTION_HPP_
#define _SCHEMA_RESTRICTION_HPP_

#include "SchemaCommon.hpp"

class CEnumerationArray;
class CFractionDigits;
class CLength;
class CMaxExclusive;
class CMaxInclusive;
class CMinExclusive;
class CMinInclusive;
class CMaxLength;
class CMinLength;
class CPattern;
class CTotalDigits;
class CWhiteSpace;

class CRestriction : public CXSDNodeWithBase
{
public:

    virtual ~CRestriction();

    GETTERSETTER(ID)

    virtual void dump(std::ostream& cout, unsigned int offset = 0) const;
    virtual void getDocumentation(StringBuffer &strDoc) const;
    virtual void getQML(StringBuffer &strQML, int idx = -1) const;
    virtual const char* getXML(const char* /*pComponent*/);
    virtual void populateEnvXPath(StringBuffer strXPath, unsigned int index = 1);
    virtual void loadXMLFromEnvXml(const IPropertyTree *pEnvTree);
    bool checkConstraint(const char *pValue) const;

    static CRestriction* load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath);

protected:

    CRestriction(CXSDNodeBase* pParentNode = NULL, const char* pID = NULL, const char* pBase = NULL) : CXSDNodeWithBase::CXSDNodeWithBase(pParentNode, XSD_RESTRICTION), m_strID(pID), m_pEnumerationArray(NULL)
    {
    }
    GETTERSETTERTYPE(EnumerationArray)
    GETTERSETTERTYPE(FractionDigits)
    GETTERSETTERTYPE(Length)
    GETTERSETTERTYPE(MaxExclusive)
    GETTERSETTERTYPE(MaxInclusive)
    GETTERSETTERTYPE(MinExclusive)
    GETTERSETTERTYPE(MinInclusive)
    GETTERSETTERTYPE(MaxLength)
    GETTERSETTERTYPE(MinLength)
    GETTERSETTERTYPE(Pattern)
    GETTERSETTERTYPE(TotalDigits)
    GETTERSETTERTYPE(WhiteSpace)

protected:

    CXSDNodeBase *m_pXSDNode;

private:

    CRestriction(CXSDNodeBase* pParentNode = NULL) : CXSDNodeWithBase::CXSDNodeWithBase(pParentNode, XSD_RESTRICTION), m_pEnumerationArray(NULL), m_pFractionDigits(NULL), m_pLength(NULL),
        m_pMaxExclusive(NULL), m_pMaxInclusive(NULL), m_pMinExclusive(NULL), m_pMinInclusive(NULL), m_pMaxLength(NULL), m_pMinLength(NULL),
        m_pPattern(NULL), m_pTotalDigits(NULL), m_pWhiteSpace(NULL), m_pXSDNode(NULL)
    {
    }
};

#endif // _SCHEMA_RESTRICTION_HPP_
