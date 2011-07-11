/*##############################################################################
## Copyright © 2011 HPCC Systems.  All rights reserved.
############################################################################## */

#ifndef __MSGGENRATOR_HPP__
#define __MSGGENRATOR_HPP__

#include <string>
#include <map>

#include "jstring.hpp"
#include "jptree.hpp"
#include "jprop.hpp"
#include "xsdparser.hpp"

class MessageGenerator
{
public:
    enum SchemaType {WSDL = 0, XSD=1};

private:
    FILE*        m_logfile;
    StringBuffer m_path;
    StringBuffer m_schema;
    StringArray  m_methods;
    Owned<IPropertyTree> m_roxieSchemaRoot;
    Owned<IPropertyTree> m_schemaTree;
    IProperties* m_globals;

    typedef std::map<std::string, std::string>  DefValMap;
    DefValMap m_defaultvalues;
    DefValMap m_cfgDefValues;
    bool         m_keepfile;
    SchemaType   m_schemaType;
    StringAttr   m_xsdNamespace;

    // options
    int          m_items;
    bool         m_isRoxie;
    bool         m_genAllDatasets;
    bool         m_gx;
    bool         m_soapWrap;
    bool         m_ecl2esp;
    Owned<IPropertyTree> m_cfg;
    StringAttr   m_gfile;

    void initDefaultValues();
    void setXsdNamespace();
    const char* xsdNs() { return m_xsdNamespace.get(); }
    int xsdNsLength()   { return m_xsdNamespace.length(); }
    void setDefaultValue(StringBuffer& buf, const char* type, const char* tag);
    void setDefaultValue(StringBuffer& buf, IXmlType* type, const char* tag);
    void initCfgDefValues(const char* method);
    
    void doType(StringStack& parent, int indent, const char* tag, IXmlType* type, IPTree* tmplat, StringBuffer& buf);
    void doType(StringStack& parent, int indent, const char* tag, IXmlType* type, StringBuffer& buf);

    void genRoxieMessage(const char* templatemsg, StringBuffer& message);
    void genNonRoxieMessage(const char* method, const char* templatemsg, StringBuffer& message);

public:
    MessageGenerator(const char* path, bool keepfile, SchemaType st, IProperties* globals);

    StringArray&  getAllMethods() { return m_methods; }
    StringBuffer& getSchema()     { return m_schema;  }
    StringBuffer& generateMessage(const char* method, const char* templatemsg, StringBuffer& message);
};

#endif // #ifdef __MSGGENRATOR_HPP__
