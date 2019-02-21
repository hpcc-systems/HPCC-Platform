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

#pragma warning(disable:4786)

#include "msggenerator.hpp"
#include "http.hpp"
#include <set>
#include <string>
#include <algorithm>

#ifdef _WIN32
    #define LT "\r\n"
#else
    #define LT "\n"
#endif

static bool loadFile(StringBuffer& s, const char* file)
{
    try {
        s.loadFile(file);
    } catch (IException* e) {
        StringBuffer msg;
        fprintf(stderr,"Exception caught: %s", e->errorMessage(msg).str());
        return false;
    }
    return true;
}

MessageGenerator::MessageGenerator(const char* path, bool keepfile, SchemaType st, IProperties* globals)
    :  m_keepfile(keepfile), m_schemaType(st)
{
    if(globals)
        m_globals = globals;

    m_items = globals->queryProp("items")?atoi(globals->queryProp("items")):1;
    m_isRoxie = globals->queryProp("roxie") ? true : false;
    m_genAllDatasets = globals->queryProp("alldataset") ? true : false;
    m_gx = globals->queryProp("gx") ? true : false;
    m_soapWrap = globals->queryProp("gs") ? false: true;
    m_ecl2esp = globals->getPropBool("ECL2ESP", false);
    if (m_ecl2esp)
        m_items = 1;

    if (globals->queryProp("gf"))
        m_gfile.set(globals->queryProp("gf"));

    if (globals->queryProp("cfg"))
    {
        StringBuffer cfg;
        if (loadFile(cfg, globals->queryProp("cfg")))
            m_cfg.setown(createPTreeFromXMLString(cfg));
        else
            UERRLOG("Can not load cfg file; ignored");
    }

    m_logfile = stdout;

    if(!path || !*path)
    {
        throw MakeStringException(-1, "please provide the path of wsdl");
    }
    
    m_path.append(path);

    if(strnicmp(path, "http:", 5) == 0 || strnicmp(path, "https:", 6) == 0)
    {
        HttpClient client(NULL, path);
        StringBuffer requestbuf;
        client.generateGetRequest(requestbuf);
        client.sendRequest(requestbuf, NULL, NULL, NULL, &m_schema);
        const char* ptr = m_schema.str();
        if(ptr)
        {
            ptr = strchr(ptr, '<');
            if(!ptr)
            {
                if(http_tracelevel > 0)
                    fprintf(m_logfile, "The schema is not valid xml%s", LT);
                return;
            }
            else
                m_schema.remove(0, ptr - m_schema.str());
        }
    }
    else
    {
        m_schema.loadFile(path);
        if(http_tracelevel >= 10)
            fprintf(m_logfile, "Loaded schema:\n%s\n", m_schema.str());
    }

    if(m_schema.length() == 0)
    {
        throw MakeStringException(-1, "wsdl is empty");
    }

    Owned<IPropertyTree> schema = createPTreeFromXMLString(m_schema.str());
    if (m_isRoxie)
        m_roxieSchemaRoot.set(schema->queryBranch("//Result"));
    else
        m_schemaTree.set(schema);

    if(!m_schemaTree.get() && !m_roxieSchemaRoot.get())
        throw MakeStringException(-1, "can't generate property tree from schema");

    setXsdNamespace();

    if (!m_isRoxie)
    {
        if(m_schemaType == WSDL)
        {
            Owned<IPropertyTreeIterator> mi = m_schemaTree->getElements("portType/operation");
            if(mi.get())
            {
                std::set<std::string> methods;
                for (mi->first(); mi->isValid(); mi->next())
                {
                    const char *name = mi->query().queryProp("@name");
                    if(!name || !*name)
                        continue;

                    StringBuffer xpath;
                    xpath.clear().append("portType/operation[@name='").append(name).append("']/input/@message");
                    const char* input = m_schemaTree->queryProp(xpath.str());
                    if(!input || !*input)
                        throw MakeStringException(-1, "can't find input message for method %s", name);
            
                    if(strncmp(input, "tns:", 4) == 0)
                        input += 4;

                    xpath.clear().append("message[@name='").append(input).append("']/part/@element");
                    const char* element = m_schemaTree->queryProp(xpath.str());
                    if(!element || !*element)
                        throw MakeStringException(-1, "can't find message %s\n", input);

                    if(strncmp(element, "tns:", 4) == 0)
                        element += 4;

                    if(methods.find(element) == methods.end())
                    {
                        methods.insert(element);
                        m_methods.append(element);
                    }

                    xpath.clear().append("portType/operation[@name='").append(name).append("']/output/@message");
                    const char* output = m_schemaTree->queryProp(xpath.str());
                    if(!output || !*output)
                        throw MakeStringException(-1, "can't find output message for method %s", name);
            
                    if(strncmp(output, "tns:", 4) == 0)
                        output += 4;

                    xpath.clear().append("message[@name='").append(output).append("']/part/@element");
                    element = m_schemaTree->queryProp(xpath.str());
                    if(!element || !*element)
                        throw MakeStringException(-1, "can't find message %s\n", output);

                    if(strncmp(element, "tns:", 4) == 0)
                        element += 4;

                    if(methods.find(element) == methods.end())
                    {
                        methods.insert(element);
                        m_methods.append(element);
                    }
                }
            }
        }
        else
        {
            Owned<IPropertyTreeIterator> mi = m_schemaTree->getElements(VStringBuffer("%s:element",xsdNs()));
            if(mi.get())
            {
                for (mi->first(); mi->isValid(); mi->next())
                {
                    const char *name = mi->query().queryProp("@name");
                    if(name && *name)
                        m_methods.append(name);
                }
            }       
        }
    }

    initDefaultValues();
}

void MessageGenerator::setXsdNamespace()
{
    const char* s = strstr(m_schema, "=\"http://www.w3.org/2001/XMLSchema\"");
    if (!s)
        s = strstr(m_schema, "=\'http://www.w3.org/2001/XMLSchema\'");
    if (s)
    {
        const char* start = s; 
        while (!isspace(*start)) start--;
        start++;
        if (strncmp(start,"xmlns:",6)==0)
            m_xsdNamespace.set(start+6,s-start-6);
    }

    if (!m_xsdNamespace.get())
        m_xsdNamespace.set("xsd");
}

void MessageGenerator::initDefaultValues()
{
    char dbuf[64];

    CDateTime now;
    now.setNow();
    
    StringBuffer nowstr;
    now.getString(nowstr, true);
    
    unsigned y, m, d;
    now.getDate(y, m, d, true);
    unsigned h, minute, s, nano;
    now.getTime(h, minute, s, nano, true);

    m_defaultvalues["string"] = "string";
    m_defaultvalues["boolean"] = "1";
    m_defaultvalues["decimal"] = "3.1415926535897932384626433832795";
    m_defaultvalues["float"] = "3.14159";
    m_defaultvalues["double"] = "3.14159265358979";
    m_defaultvalues["duration"] = "P1Y2M3DT10H30M";
    m_defaultvalues["dateTime"] = nowstr.str();
    sprintf(dbuf, "%02d:%02d:%02d", h,minute,s);
    m_defaultvalues["time"] = dbuf;
    sprintf(dbuf, "%04d-%02d-%02d", y, m, d);
    m_defaultvalues["date"] = dbuf;
    sprintf(dbuf, "%04d-%02d", y, m);
    m_defaultvalues["gYearMonth"] = dbuf;
    sprintf(dbuf, "%04d", y);
    m_defaultvalues["gYear"] = dbuf;
    sprintf(dbuf, "--%02d-%02d", m, d);
    m_defaultvalues["gMonthDay"] = dbuf;
    sprintf(dbuf, "---%02d", d);
    m_defaultvalues["gDay"] = dbuf;
    sprintf(dbuf, "--%02d--", m);
    m_defaultvalues["gMonth"] = dbuf;
    m_defaultvalues["hexBinary"] = "A9D4C56EFB";
    m_defaultvalues["base64Binary"] = "YmFzZTY0QmluYXJ5";
    m_defaultvalues["anyURI"] = "http://anyURI/";
    m_defaultvalues["QName"] = "q:name";
    m_defaultvalues["NOTATION"] = "NOTATION";
    m_defaultvalues["normalizedString"] = "normalizedString";
    m_defaultvalues["token"] = "token";
    m_defaultvalues["language"] = "en-us";
    m_defaultvalues["integer"] = "0";
    m_defaultvalues["nonPositiveInteger"] = "-1";
    m_defaultvalues["negativeInteger"] = "-2";
    m_defaultvalues["long"] = "2147483647";
    m_defaultvalues["int"] = "32716";
    m_defaultvalues["short"] = "4096";
    m_defaultvalues["byte"] = "127";
    m_defaultvalues["nonNegativeInteger"] = "3";
    m_defaultvalues["positiveInteger"] = "2";
    m_defaultvalues["unsignedLong"] = "4294967295";
    m_defaultvalues["unsignedInt"] = "4";
    m_defaultvalues["unsignedShort"] = "65535";
    m_defaultvalues["unsignedByte"] = "255";
}

void MessageGenerator::initCfgDefValues(const char* method)
{
    if (m_cfg.get())
    {
        m_cfgDefValues.clear();
        
        // Common
        Owned<IPropertyTreeIterator> fs = m_cfg->getElements("Common/Field");
        for (fs->first(); fs->isValid(); fs->next())
        {
            IPropertyTree& f = fs->query();
            m_cfgDefValues[f.queryProp("@name")] = f.queryProp("@value");
        }

        // Service specific
        fs.setown(m_cfg->getElements(VStringBuffer("Services/Service[@name='%s']/Field",method)));
        for (fs->first(); fs->isValid(); fs->next())
        {
            IPropertyTree& f = fs->query();
            m_cfgDefValues[f.queryProp("@name")] = f.queryProp("@value");
        }
    }
}

static bool isNotBlank(const char* s)
{
    for (; *s != 0; s++)
        if (!isspace(*s))
            return true;
    return false;
}

void MessageGenerator::setDefaultValue(StringBuffer& buf, IXmlType* type, const char* tag)
{
    DefValMap::const_iterator it = m_cfgDefValues.find(tag);
    if (it != m_cfgDefValues.end())
        buf.append(it->second.c_str());
    else {
        const char* name = type->queryName();
        if (name && m_defaultvalues.find(name) != m_defaultvalues.end())
            buf.append(m_defaultvalues[name].c_str());
        else
        {
            const char* tName = type->queryName();
            if (m_ecl2esp)
                buf.appendf("[%s]", !strnicmp(tName, "string", 6) ? tName+6 : "?");
            else
                buf.appendf("[%s]", tag);
        }
    }
}

void MessageGenerator::genNonRoxieMessage(const char* method, const char* templatemsg, StringBuffer& message)
{
    if (m_soapWrap)
        message.appendf("<?xml version=\"1.0\" encoding=\"utf-8\"?>"
            "%s<soap:Envelope xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\""
            " xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\""
            " xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\""
            " xmlns:SOAP-ENC=\"http://schemas.xmlsoap.org/soap/encoding/\""
            " xmlns:wsse=\"http://schemas.xmlsoap.org/ws/2002/04/secext\"><soap:Body>%s", LT, LT);

    const char* element = method;
    IPTree* schema = NULL;
    if(m_schemaType == WSDL) 
        schema = m_schemaTree->queryPropTree("types/xsd:schema");
    else
        schema = m_schemaTree;

    if (schema)
    {
        const char* ns = schema->queryProp("@targetNamespace");
        if(ns && ns && !m_ecl2esp)
            message.appendf("  <%s  xmlns=\"%s\">", element, ns);
        else
            message.appendf("  <%s>", element);

        
        Owned<IXmlSchema> s = createXmlSchemaFromPTree(LINK(schema));

        if (s.get())
        {
            IXmlType* type = s->queryElementType(element);
            if (type)
            {
                StringStack parent;

                if (templatemsg && *templatemsg)
                {
                    Owned<IPropertyTree> tmplat = createPTreeFromXMLString(templatemsg);
                    if(!tmplat.get())
                        throw MakeStringException(-1, "can't generate property tree from input, please make sure it's valid xml.");
                    IPropertyTree* tmp = NULL;
                    if (strcmp(tmplat->queryName(),element)==0)
                        tmp = tmplat;
                    else
                        tmp = tmplat->queryPropTree(VStringBuffer("//%s",element)); 
                    if (tmp)
                        doType(parent,2, element, type, tmp, message);
                    else
                        doType(parent,2, element, type, message);
                }
                else
                    doType(parent,2, element, type, message);
            }
        }
        
        message.appendf("</%s>%s", element, LT);
    }

    if (m_soapWrap)
        message.append("</soap:Body></soap:Envelope>");
}

void MessageGenerator::genRoxieMessage(const char* templatemsg, StringBuffer& message)
{
    Owned<IPropertyTree> tmplat;
    StringBuffer root;

    if (templatemsg)
    {
        tmplat.setown(createPTreeFromXMLString(templatemsg));
        if(!tmplat.get())
            throw MakeStringException(-1, "can't generate property tree from input, please make sure it's valid xml.");
        root = tmplat->queryName();
        tmplat.setown(tmplat->getPropTree(VStringBuffer("//Results/Result")));
        if (!tmplat.get())
            throw MakeStringException(-1, "can't find Results/Result in input XML");
    }
    else 
        root = "Unknown"; // TODO: find out the root?

    message.appendf("<!-- <%s> --> %s", root.str(), LT);
    message.appendf(" <!-- <Results> --> %s", LT);
    message.appendf("  <Result>%s", LT);

    Owned<IPropertyTreeIterator> it = m_roxieSchemaRoot->getElements(VStringBuffer("XmlSchema"));
    for (it->first(); it->isValid(); it->next())
    {
        IPropertyTree* ds = &it->query();

        const char* name = ds->queryProp("@name");
        if (!name)
        {
            UERRLOG("XmlSchema without name");
            continue;
        }
        
        IPropertyTree* p = ds->queryBranch(VStringBuffer("%s:schema", xsdNs()));
        m_schemaTree.setown(LINK(p));
        IXmlSchema* xs = createXmlSchemaFromPTree(m_schemaTree);
        IXmlType* type = xs->queryElementType("Dataset");
        if (!type)
        {
            UERRLOG("Can not find type '%s'", name);
            continue;
        }
        // get the Row type
        type = type->queryFieldType(0);
        if (!type)
        {
            UERRLOG("The root element for %s is not an array", name);
            continue;
        }

        IPropertyTree* dsTmplat = tmplat.get() ? tmplat->queryPropTree(VStringBuffer("Dataset[@name='%s']",name)) : NULL;
        if (dsTmplat && dsTmplat->numChildren()>0)
        {
            message.appendf("    <Dataset name=\"%s\">%s", name, LT);
    
            Owned<IPropertyTreeIterator> row = dsTmplat->getElements("Row");
            for (row->first(); row->isValid(); row->next())
            {
                message.appendf("     <Row>%s", LT);
    
                StringStack parent;
                doType(parent,5,"Row",type, &row->query(), message);

                message.appendf("     </Row>%s", LT);
            }

            message.appendf("   </Dataset>%s", LT);
        }
        else if (m_genAllDatasets)
        {
            message.appendf("    <Dataset name=\"%s\">%s", name, LT);
            for (int i=0; i < m_items; i++)
            {
                message.appendf("     <Row>%s", LT);
                StringStack parent;
                doType(parent,6,"Row",type,message);
                message.appendf("     </Row>%s", LT);
            }
            if (m_ecl2esp)
                message.appendf("     <Row/>%s", LT);

            message.appendf("   </Dataset>%s", LT);
        }
    }

    message.appendf("  </Result>%s", LT);
    message.appendf(" <!-- </Results> --> %s", LT);
    message.appendf("<!-- </%s> --> %s", root.str(), LT);
}

StringBuffer& MessageGenerator::generateMessage(const char* method, const char* templatemsg, StringBuffer& message)
{
    if(!method || !*method)
        return message;

    if(http_tracelevel >= 1)
        fprintf(m_logfile, "Automatically generating message from schema for method \"%s\"%s", method, LT);
    
    initCfgDefValues(method);

    if (m_isRoxie)
        genRoxieMessage(templatemsg, message);
    else 
        genNonRoxieMessage(method, templatemsg, message);

    if (m_gfile.get())
    {
        Owned<IFile> tf = createIFile(m_gfile.get());
        {
            Owned<IFileIO> tio = tf->open(IFOcreaterw);
            tio->write(0, message.length(), message.str());
        }
    }

    else if(http_tracelevel > 0)
    {
        fprintf(stderr, "Request for method %s has been generated:\n", method);
        if(http_tracelevel >= 5)
            fprintf(stderr, "%s\n", message.str());

        char c = 'n';
        if(!(m_globals && m_globals->getPropBool("useDefault")))
        {
            fprintf(stderr, "Do you want to modify it?[n/y]");
            c = getchar();
        }

        if(c == 'y' || c == 'Y' || m_keepfile)
        {
            StringBuffer tmpfname;
            tmpfname.append(method);
            addFileTimestamp(tmpfname, false);
#ifdef _WIN32
            tmpfname.insert(0, "c:\\Temp\\");
#else
            tmpfname.insert(0, "/tmp/");
#endif
            Owned<IFile> tf = createIFile(tmpfname.str());
            {
                Owned<IFileIO> tio = tf->open(IFOcreaterw);
                tio->write(0, message.length(), message.str());
            }
            
            if(c == 'y' || c == 'Y')
            {
                StringBuffer cmdline;
#ifdef _WIN32
                cmdline.appendf("notepad.exe %s", tmpfname.str());
                STARTUPINFO sinfo;
                PROCESS_INFORMATION pinfo;
                GetStartupInfo(&sinfo);
                CreateProcess(0, (char*)cmdline.str(), 0, 0, false, 0, 0, 0, &sinfo, &pinfo);
                WaitForSingleObject(pinfo.hProcess, INFINITE);
#else
                cmdline.appendf("vi %s", tmpfname.str());
                if (system(cmdline.str()) == -1)
                    throw MakeStringException(-1, "MessageGenerator::generateMessage: could not execute command %s", cmdline.str());
#endif
                message.clear().loadFile(tmpfname.str(), true);
            }

            if(!m_keepfile)
                tf->remove();
            else
                printf("A copy is saved at %s (unless you specified another location)\n", tmpfname.str());
        }
    }

    return message;
}


void MessageGenerator::doType(StringStack& parent, int indent, const char* tag, IXmlType* type, IPTree* tmplat, StringBuffer& buf)
{
    const char* typeName = type->queryName();
    if (type->isComplexType())
    {
        if (typeName && std::find(parent.begin(),parent.end(),typeName) != parent.end())
        {
            //DBGLOG("Recursive type: %s, ignored", typeName);
        }
        else 
        {
            int flds = type->getFieldCount();
            for (int i=0; i<flds; i++)
            {
                const char* fldName = type->queryFieldName(i);
                buf.appendf("<%s>", fldName);
                IPTree* existing = tmplat->queryBranch(fldName);
                if (typeName)
                    parent.push_back(typeName);
                if (existing)
                    doType(parent,indent+1,fldName, type->queryFieldType(i), existing, buf);
                else
                    doType(parent,indent+1,fldName,type->queryFieldType(i),buf);
                buf.appendf("</%s>", fldName);
                if (typeName)
                    parent.pop_back();
            }
        }
    }
    else if (type->isArray())
    {
        if (typeName && std::find(parent.begin(),parent.end(),typeName) != parent.end())
        {
            //DBGLOG("Recursive type: %s, ignored", typeName);
        }
        else 
        {
            const char* itemName = type->queryFieldName(0);
            IXmlType* itemType = type->queryFieldType(0);
            int childCount = tmplat->numChildren();
            if (typeName)
                parent.push_back(typeName);
            if (childCount>0)
            {
                Owned<IPTreeIterator> items = tmplat->getElements(itemName);
                for (items->first(); items->isValid(); items->next())
                {
                    buf.appendf("<%s>", itemName);
                    doType(parent,indent+2,itemName,itemType,&items->query(),buf);
                    buf.appendf("</%s>", itemName);
                }
            }
            else
            {
                for (int i=0; i<m_items; i++)
                {
                    buf.appendf("<%s>", itemName);
                    doType(parent,indent+2,itemName, itemType, buf);
                    buf.appendf("</%s>", itemName);
                }
                if (m_ecl2esp)
                    buf.appendf("<%s/>", itemName);
            }
            if (typeName)
                parent.pop_back();
        }
    }
    else
    {
        const char* existing = tmplat->queryProp(".");
        if (existing && isNotBlank(existing))
            encodeXML(existing,buf);
        else if (m_gx)
            buf.append("*** MISSING ***");
        else
        {
            DefValMap::const_iterator it = m_cfgDefValues.find(tag);
            if (it != m_cfgDefValues.end())
                buf.append(it->second.c_str());
            else
                type->getSampleValue(buf,tag);
        }
    }
}

void MessageGenerator::doType(StringStack& parent, int indent, const char* tag, IXmlType* type, StringBuffer& buf)
{
    const char* typeName = type->queryName();

    if (type->isComplexType())
    {
        if (typeName && std::find(parent.begin(),parent.end(),typeName) != parent.end())
        {
            //DBGLOG("Recursive type: %s, ignored", type->queryName());
        }
        else 
        {
            int flds = type->getFieldCount();
            for (int i=0; i<flds; i++)
            {
                const char* fldName = type->queryFieldName(i);
                buf.appendf("<%s>", fldName);
                if (typeName)
                    parent.push_back(typeName);
                doType(parent,indent+1,fldName,type->queryFieldType(i),buf);
                buf.appendf("</%s>", fldName);
                if (typeName)
                    parent.pop_back();
            }
        }
    }
    else if (type->isArray())
    {
        if (typeName && std::find(parent.begin(),parent.end(),typeName) != parent.end())
        {
            //DBGLOG("Recursive type: %s, ignored", type->queryName());
        }
        else
        {
            const char* itemName = type->queryFieldName(0);
            IXmlType* itemType = type->queryFieldType(0);
            if (typeName)
                parent.push_back(typeName);
            for (int i=0; i<m_items; i++)
            {
                buf.appendf("<%s>", itemName);
                doType(parent,indent+2,itemName, itemType, buf);
                buf.appendf("</%s>", itemName);
            }
            if (m_ecl2esp)
                buf.appendf("<%s/>", itemName);
            if (typeName)
                parent.pop_back();
        }
    }
    else
    {
        //TODO: handle restriction etc
        if (strcmp(typeName,"string")==0) // string type: [tag-typeName]
        {
            if (m_gx)
                buf.append("*** MISSING ***");
            else
                if (m_ecl2esp)
                    buf.append("[?]");
                else
                    buf.appendf("[%s]", tag);
        }
        else
            setDefaultValue(buf,type,tag);
    }
}
