#ifndef _CONFIG_SCHEMA_HELPER_HPP_
#define _CONFIG_SCHEMA_HELPER_HPP_

#include "jiface.hpp"
#include "jptree.hpp"
#include "jutil.hpp"
#include "jarray.hpp"
#include "jhash.hpp"
#include "SchemaAttributes.hpp"
#include "SchemaAttributeGroup.hpp"
#include "SchemaElement.hpp"
#include "SchemaSchema.hpp"
#include "SchemaExtension.hpp"
#include "BuildSet.hpp"
#include "ConfiguratorAPI.hpp"

class CSchemaMapManager;
class CSimpleType;

class CConfigSchemaHelper : public CInterface
{
public:

    IMPLEMENT_IINTERFACE

    static CConfigSchemaHelper* getInstance(const char* pDefaultDirOverride =  NULL);
    static CConfigSchemaHelper* getInstance(const char* pBuildSetFileName, const char *pBaseDirectory, const char *pDefaultDirOverride = NULL);

    virtual ~CConfigSchemaHelper();

    bool populateSchema();
    void printConfigSchema(StringBuffer &str) const;

    CSchemaMapManager* getSchemaMapManager()
    {
        return m_pSchemaMapManager;
    }

    void addExtensionToBeProcessed(CExtension *pExtension);
    void processExtensionArr();

    void addAttributeGroupToBeProcessed(CAttributeGroup *pAttributeGroup);
    void processAttributeGroupArr();

    void addNodeForBaseProcessing(CXSDNodeWithBase *pNode);
    void processNodeWithBaseArr();

    void addNodeForTypeProcessing(CXSDNodeWithType *pNode);
    void processNodeWithTypeArr(CXSDNodeBase *pParentNode = NULL);
    //void addRestrictionToBeProcessed(CRestriction *pRestriction);
    //void processRestrictionGroupArr();

    void addElementForRefProcessing(CElement *pElement);
    void processElementArr(CElement *pElement);

    void addKeyRefForReverseAssociation(const CKeyRef *pKeyRef) const;
    void processKeyRefReverseAssociation() const;

    void addKeyForReverseAssociation(const CKeyRef *pKeyRef) const;
    void processKeyReverseAssociation() const;

    //test purposes
    bool getXMLFromSchema(StringBuffer& strXML, const char* pXSD);

    void populateEnvXPath();
    void loadEnvFromConfig(const char *pEnvFile);

    void traverseAndProcessArray(const char *pXSDName = NULL);

    const char* printDocumentation(const char* comp);
    const char* printDojoJS(const char* comp);
    const char* printQML(const char* comp, int nIdx = -1) const;
    void printDump(const char* comp) const;
    void dumpStdOut() const;

    void addToolTip(const char *js);

    const char* getToolTipJS() const;

    const char* getBasePath() const
    {
        return m_pBasePath;
    }

    void setBasePath(const char *pBasePath);

    void setEnvTreeProp(const char *pXPath, const char* pValue);

    const char* getTableValue(const char* pXPath, int nRow = 1) const;

    int getEnvironmentXPathSize() const
    {
        return m_strArrayEnvXPaths.length();
    }

    const char* getEnvironmentXPaths(int idx) const
    {
        assert(idx >= 0);
        assert(m_strArrayEnvXPaths.length() > idx);

        return m_strArrayEnvXPaths.item(idx);
    }

    const char* getAttributeXSDXPathFromEnvXPath(const char* pEnvXPath) const;
    const char* getElementArrayXSDXPathFromEnvXPath(const char* pXSDXPath) const;

    int getElementArraySize(const char *pXPath) const;

    void appendAttributeXPath(const char *pXPath);
    void appendElementXPath(const char *pXPath);

    static int stripXPathIndex(StringBuffer &strXPath);
    static bool isXPathTailAttribute(const StringBuffer &strXPath);

    IPropertyTree* getEnvPropertyTree()
    {
        return m_pEnvPropertyTree;
    }

    const IPropertyTree* getConstEnvPropertyTree() const
    {
        return m_pEnvPropertyTree;
    }

    int getNumberOfTables() const
    {
        return m_nTables;
    }

    void incTables()
    {
        m_nTables++;
    }

    void resetTables() const
    {
        m_nTables = 0;
    }

    bool saveConfigurationFile() const;

protected:

    CConfigSchemaHelper(const char* pBuildSetFile = DEFAULT_BUILD_SET_XML_FILE, const char* pBuildSetDir = DEFAULT_BUILD_SET_DIRECTORY, const char* pDefaultDirOverride = NULL);

    CSchemaMapManager *m_pSchemaMapManager;

    CIArrayOf<CExtension> m_extensionArr;
    CIArrayOf<CAttributeGroup> m_attributeGroupArr;
    CIArrayOf<CXSDNodeWithType> m_nodeWithTypeArr;
    CIArrayOf<CXSDNodeWithBase> m_nodeWithBaseArr;
    CIArrayOf<CElement> m_ElementArr;
    CIArrayOf<CKeyRef> m_KeyRefArr;
    StringArray m_strToolTipsJS;
    StringArray m_strArrayEnvXPaths;
    StringArray m_strArrayEnvXMLComponentInstances;

    void setEnvPropertyTree(IPropertyTree *pEnvTree)
    {
        m_pEnvPropertyTree =  pEnvTree;
    }

    void setEnvFilePath(const char* pEnvFilePath)
    {
        assert(pEnvFilePath != NULL);

        m_strEnvFilePath.set(pEnvFilePath);
    }

    const char* getEnvFilePath() const
    {
        return m_strEnvFilePath.str();
    }

private:

    static CConfigSchemaHelper* s_pCConfigSchemaHelper;
    mutable int m_nTables;
    char *m_pBasePath;

    StringBuffer m_strEnvFilePath;

    IPropertyTree *m_pEnvPropertyTree;
};

#endif // _CONFIG_SCHEMA_HELPER_HPP_
