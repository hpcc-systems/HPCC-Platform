#ifndef _SCHEMA_EXTENSION_HPP_
#define _SCHEMA_EXTENSION_HPP_

#include "SchemaCommon.hpp"
#include "jstring.hpp"

class IPropertyTree;

class CExtension : public CXSDNodeWithBase
{
public:

    virtual ~CExtension()
    {
    }

    //GETTERSETTER(Base)

    virtual void dump(std::ostream& cout, unsigned int offset = 0) const;

    virtual void getDocumentation(StringBuffer &strDoc) const
    {
        assert(false); // Should not be called directly
    }

    virtual void getDojoJS(StringBuffer &strJS) const
    {
        assert(false);
    }

    virtual const char* getXML(const char* /*pComponent*/);

    virtual void initExtension();

    static CExtension* load(CXSDNodeBase* pParentNode, const IPropertyTree *pSchemaRoot, const char* xpath = NULL);

protected:

    CExtension(CXSDNodeBase* pParentNode, const char* pBase = NULL, CXSDNode *pXSDNode = NULL) : CXSDNodeWithBase::CXSDNodeWithBase(pParentNode, XSD_EXTENSION)
    {
    }

private:

    CExtension() : CXSDNodeWithBase::CXSDNodeWithBase(NULL, XSD_EXTENSION)
    {
    }
};

#endif // _SCHEMA_EXTENSION_HPP_
