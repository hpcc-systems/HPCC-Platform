/*##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
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
    Owned<IProperties> m_globals;

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
