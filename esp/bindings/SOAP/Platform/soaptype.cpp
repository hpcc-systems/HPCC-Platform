/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

//=======================================================
// Implemenations of classes of ISoapType, ISoapField etc

#pragma warning( disable : 4786)

#include "SOAP/client/soapclient.hpp"
#include "soaptype.hpp"
#include <map>

//=====================================================================================
// field factory

ISoapField* CSoapStringType::createField(const char* name) {  return new CSoapStringField(name); } 
ISoapField* CSoapIntType::createField(const char* name)    {  return new CSoapIntField(name); } 
ISoapField* CSoapShortType::createField(const char* name)    {  return new CSoapShortField(name); } 
ISoapField* CSoapBoolType::createField(const char* name)   {  return new CSoapBoolField(name); } 
ISoapField* CSoapDoubleType::createField(const char* name) {  return new CSoapDoubleField(name); } 
ISoapField* CSoapFloatType::createField(const char* name) {  return new CSoapFloatField(name); } 
ISoapField* CSoapUIntType::createField(const char* name) {  return new CSoapUIntField(name); } 
ISoapField* CSoapInt64Type::createField(const char* name) {  return new CSoapInt64Field(name); } 
ISoapField* CSoapUCharType::createField(const char* name) {  return new CSoapUCharField(name); } 

//=====================================================================================
// class CSoapStructType
/*
ISoapType* CSoapStructType::findFieldType(const char* fldName)
{
    int count = getFieldCount();
    for (int i=0;i<count; i++)
        if (strcmp(queryFieldName(i),fldName)==0)
            return queryFieldType(i);
    return NULL;
}
*/

void CSoapStructType::getFieldsDefinition(IEspContext& ctx, StringBuffer& schema)
{
    for (int fld=0; fld<getFieldCount(); fld++)
    {
        ISoapType* type = queryFieldType(fld);
        bool isPrimitive = type->isPrimitiveType();
        if (type->isArrayType())
        {
            const char* itemName = queryAttribute(fld,"item_tag");
            const char* typeName = queryFieldType(fld)->queryTypeName();
            if (itemName) 
            //TODO: optimize: remove unnecessary type definition 
            //if (itemName && (isPrimitive ? strcmp(itemName,"Item")!=0 : strcmp(itemName,typeName)!=0))
            {
                schema.appendf("<xsd:element minOccurs=\"0\" name=\"%s\">", queryFieldName(fld));
                schema.append(  "<xsd:complexType>");
                schema.append(   "<xsd:sequence>");
                schema.appendf("<xsd:element name='%s' type='%s:%s' minOccurs='0' maxOccurs='unbounded'/>",
                    itemName, isPrimitive?"xsd":"tns",type->queryTypeName());
                schema.append(   "</xsd:sequence>");
                schema.append(  "</xsd:complexType>");
                schema.append( "</xsd:element>");
            }
            else 
            {
                if (isPrimitive)
                {
                    char initial = _toupper(*typeName);
                    schema.appendf("<xsd:element minOccurs='0' name='%s' type='tns:Esp%c%sArray'/>\n",queryFieldName(fld),initial,typeName+1);
                }
                else
                    schema.appendf("<xsd:element minOccurs='0' name='%s' type='tns:ArrayOf%s'/>\n",queryFieldName(fld),typeName);
            }
        }
        else
        {           
            schema.appendf("<xsd:element minOccurs=\"0\" name=\"%s\" type=\"%s:%s\"/>\n",
                queryFieldName(fld), isPrimitive?"xsd":"tns", type->queryTypeName());
        }
    }
}

StringBuffer& CSoapStructType::getXsdDefinition(IEspContext& ctx, const char *msgTypeName, StringBuffer &schema, 
    wsdlIncludedTable &added, const char *xns, const char *wsns, unsigned flags)
{
    if (msgTypeName)
    {
        if(added.getValue(msgTypeName))
            return schema;
        else
            added.setValue(msgTypeName, 1);
    }

    if (flags & 1)
    {
        if (getEspmType() != EspStruct)
            schema.appendf("<xsd:element name=\"%s\" type=\"tns:%s\"/>\n", msgTypeName, msgTypeName);
        schema.appendf("<xsd:complexType name=\"%s\">\n", msgTypeName);                 
    }
    
    schema.append("<xsd:all>");
    getFieldsDefinition(ctx,schema);
    schema.append("</xsd:all>\n");
    
    if (flags & 1)
    {
        schema.append("</xsd:complexType>\n");
        schema.appendf("<xsd:complexType name=\"ArrayOf%s\">\n", queryTypeName());
        schema.append("<xsd:sequence>\n");
        schema.appendf("<xsd:element minOccurs=\"0\" maxOccurs=\"unbounded\" name=\"%s\" type=\"tns:%s\"/>\n", queryTypeName(), queryTypeName());
        schema.append("</xsd:sequence>\n");
        schema.append("</xsd:complexType>\n");
    }

    for (int fld=0; fld<m_fldCount; fld++)
    {       
        if (!queryFieldType(fld)->isPrimitiveType()) 
        {
            const char* typeName = queryFieldType(fld)->queryTypeName();
            if (!added.getValue(typeName))
            {
                queryFieldType(fld)->getXsdDefinition(ctx,typeName,schema,added);
                added.setValue(typeName,1);
            }
        }
    }

    return schema;
}

StringBuffer& CSoapStructType::getHtmlForm(IEspContext &ctx, CHttpRequest* req, const char *serv, 
    const char *method, StringBuffer &form, bool includeFormTag, const char *prefix)
{
    if (includeFormTag)
        form.appendf("\n<form name=\"esp_form\" method=\"POST\" action=\"/%s/%s\">\n", serv, method);
    
    form.append("<table>\n");
    
    for (int i=0; i<getFieldCount(); i++)
    {
        StringBuffer extfix;
        if (prefix && *prefix) 
            extfix.append(prefix).append(".");
        extfix.append(queryFieldName(i));
        
        ISoapType* type = queryFieldType(i);
        
        //TODO: this can be moved to ISoapType 
        if (type->isPrimitiveType())
        {
            //TODO: more on default values
            StringBuffer defValue;      
            bool isBool = strcmp(type->queryTypeName(),"bool")==0;
            if (isBool)
                defValue.append("1");
            
            form.appendf("  <tr><td><b>%s: </b></td><td><input type='%s' name='%s' size='50' value='%s' /> </td></tr>\n", 
                queryFieldName(i), isBool?"checkbox":"text", extfix.str(), defValue.str());
        }
        else
        {
            form.appendf("<tr><td><b>%s: </b></td><td><hr/>", queryFieldName(i));
            type->getHtmlForm(ctx,req,serv,method,form,false,extfix.str());
            form.append("<hr/></td></tr>");
        }
    }
    
    if (includeFormTag)
    {
        form.append("<tr><td></td><td><input type=\"submit\" value=\"Submit\" name=\"S1\" />");
        form.append(" &nbsp; <input type=\"reset\" value=\"Clear\"/> </td> </tr>");
    }
    form.append("</table>");
    
    if (includeFormTag)
        form.append("</form>");
    
    return form;
}

//=====================================================================================
// class CSoapStructTypeEx


//=====================================================================================
//  CSoapStringField

void CSoapStringField::marshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath, 
                                        const char* itemname, const char *xsdtype, const char *xmlns, bool *encodex)
{
    if (!m_isNil || m_nilBH==nilIgnore)
        rpc_call.add_value(basepath, xmlns, tagname, xsdtype, m_value.str(),(!encodex) ? rpc_call.getEncodeXml() : *encodex);
}


void CSoapStringField::marshall(StringBuffer &str, const char *tagname, const char *basepath, bool encodeXml, 
                                        const char* itemname, const char *xsdtype, const char *xmlns)
{
    if (m_nilBH==nilIgnore || !m_isNil)
    {
        str.appendf("<%s>", tagname);
        if (encodeXml)
            encodeUtf8XML(m_value.str(), str);
        else
            str.append(m_value);
        str.appendf("</%s>", tagname);
    }
}

void CSoapStringField::unmarshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath, 
                                          const char* itemname, const char *xsdtype, const char *xmlns)
{
    StringBuffer path(basepath);
    
    if (basepath!=NULL && basepath[0]!=0)
        path.append("/");
    
    path.append(tagname);
    
    m_isNil = !rpc_call.get_value(path.str(), m_value.clear());
}

void CSoapStringField::unmarshall(CSoapValue &soapval, const char *tagname)
{
    m_isNil = !soapval.get_value_str(tagname, m_value.clear());
}

void CSoapStringField::unmarshall(IProperties &params, MapStrToBuf *attachments, const char *tagname, const char *basepath, 
                                          const char* itemname, const char *xsdtype, const char *xmlns)
{
    StringBuffer path;
    if (basepath && *basepath)
        path.append(basepath).append(".");
    path.append(tagname);
    const char *pval = params.queryProp(path.str());
    m_isNil=(pval==NULL);
    esp_convert(pval, m_value.clear());
}

void CSoapStringField::unmarshall(MapStrToBuf *attachments, const char *tagname, const char *basepath, 
                                          const char* itemname, const char *xsdtype, const char *xmlns)
{
    if(attachments)
    {
        StringBuffer key;
        if (basepath && *basepath)
            key.append(basepath).append(".");
        key.append(tagname);
        
        StringBuffer* data = attachments->getValue(key.str());
        if (data)
        {
            StringBuffer path;
            if (basepath && *basepath)
                path.append(basepath).append(".");
            path.append(tagname);
            m_isNil=false;
            m_value.clear().swapWith(*data);
        }
    }
}

//=====================================================================================
//  CSoapBinary Type/Field

ISoapField* CSoapBinaryType::createField(const char* name) {  return new CSoapBinaryField(name, instance()); }

StringBuffer& CSoapBinaryType::getXsdDefinition(IEspContext& ctx, const char *msgTypeName, StringBuffer &schema, wsdlIncludedTable &added, 
                                                const char *xns, const char *wsns, unsigned flags)
{
    //TODO: has binary only needed
    return schema;
}

void CSoapBinaryField::copy(CSoapBinaryField &from)
{
    value_.clear().append(from.value_);
}

void CSoapBinaryField::marshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath,
                                        const char* itemname, const char *xsdtype, const char *xmlns)
{
    StringBuffer b64value;
    JBASE64_Encode(value_.toByteArray(), value_.length(), b64value);
    rpc_call.add_value(basepath, xmlns, tagname, xsdtype, b64value);
}

void CSoapBinaryField::marshall(StringBuffer &str, const char *tagname, const char *basepath,
                                        const char* itemname, bool encodeXml, const char *xsdtype, const char *xmlns)
{
    str.appendf("<%s>", tagname);
    JBASE64_Encode(value_.toByteArray(), value_.length(), str);
    str.appendf("</%s>", tagname);
}

void CSoapBinaryField::marshall(StringBuffer &str, const char *tagname, const char *basepath, 
    const char* itemname, const char *xsdtype, const char *xmlns)
{
    marshall(str,tagname,basepath,itemname,true,xsdtype,xmlns);
}

void CSoapBinaryField::unmarshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath, 
                                          const char* itemname, const char *xsdtype, const char *xmlns)
{
    StringBuffer path(basepath);

    if (basepath!=NULL && basepath[0]!=0)
    path.append("/");

    path.append(tagname);

    StringBuffer b64value;
    rpc_call.get_value(path.str(), b64value);

    if(b64value.length() > 0)
        JBASE64_Decode(b64value.str(), value_);
}

void CSoapBinaryField::unmarshall(CSoapValue &soapval, const char *tagname)
{
    assertex(false);
}

void CSoapBinaryField::unmarshall(IProperties &params, MapStrToBuf *attachments, const char *tagname, const char *basepath,
                                          const char* itemname, const char *xsdtype, const char *xmlns)
{
    StringBuffer path;
    if (basepath && *basepath)
        path.append(basepath).append(".");
    path.append(tagname);

    const char* val = params.queryProp(path.str());
    if(val)
        JBASE64_Decode(params.queryProp(path.str()), value_);
}

//=====================================================================================
//  CSoapStringArray Type/Field

ISoapField* CSoapStringArrayType::createField(const char* name) { return new CSoapStringArrayField(name); }

StringBuffer& CSoapStringArrayType::getXsdDefinition(IEspContext& ctx, const char *msgTypeName, StringBuffer &schema, wsdlIncludedTable &added, const char *xns, const char *wsns, unsigned flags) 
{
    /* TODO: remove the definition from default
    added.stValue("EspStringArray",1);
    schema.append("<xsd:complexType name=\"EspStringArray\">"
            "<xsd:sequence>"
                "<xsd:element name=\"Item\" type=\"xsd:string\" minOccurs=\"0\" maxOccurs=\"unbounded\"/>"
            "</xsd:sequence>"
        "</xsd:complexType>");
    */
    return schema;
}

void CSoapStringArrayField::stValue(FieldValue fv)
{
    array_.kill();
    StringArray* src = fv.stringarrayV;
    assertex(src);
    int count = src->ordinality();
    for (int i=0; i<count; i++)
        array_.append(src->item(i));
}

void CSoapStringArrayField::unmarshall(CSoapValue &soapval, const char *tagname)
{
    CSoapValue *sv= soapval.get_value(tagname);
    if (sv)
    {
        SoapValueArray* children = sv->query_children();
        
        if (children)
        {
            StringBuffer itemval;
            ForEachItemIn(x, *children)
            {
                CSoapValue& onechild = children->item(x);
                onechild.get_value_str("",itemval.clear());
                array_.append(itemval.str());
            }
        }
    }

}

void CSoapStringArrayField::marshall(StringBuffer &str, const char *tagname, const char *basepath, 
                                             const char* itemname, const char* xsdtype, const char *xmlns)
{
    const unsigned nItems = array_.ordinality();
    if (nItems == 0)
        str.appendf("<%s/>", tagname);
    else
    {
        str.appendf("<%s>", tagname);
        for (unsigned  i=0; i<nItems; i++)
        {
            const char* val = array_.item(i);
            StringBuffer encoded;
            encodeUtf8XML(val,encoded);
            str.appendf("<%s>%s</%s>", itemname, encoded.str(), itemname);
        }
        str.appendf("</%s>", tagname);
    }
}

void CSoapStringArrayField::unmarshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath, 
                                               const char* itemname, const char* xsdtype, const char *xmlns)
{
    StringBuffer path(basepath);

    if (basepath && *basepath)
        path.append("/");

    path.append(tagname);

    rpc_call.get_value(path.str(), array_);
}


void CSoapStringArrayField::unmarshallAttach(IProperties &params, MapStrToBuf *attachments, const char *tagname, const char *basepath, const char *xsdtype, const char *xmlns)
{
    if(attachments)
    {
        StringBuffer path;
        buildVarPath(path, tagname, basepath, NULL, "itemlist", NULL);
        if (params.hasProp(path.str()))
        {
            //sparse array encoding
            char *itemlist=strdup(params.queryProp(path.str()));
            char *delim=NULL;
            if (itemlist)
            {
                for(char *finger=itemlist; finger; finger=(delim) ? delim+1 : NULL)
                {
                    if ((delim=strchr(finger, '+'))) 
                        *delim=0;
                    if (*finger)
                    {
                        buildVarPath(path, tagname, basepath, finger, NULL, NULL);
                        StringBuffer* data = attachments->getValue(path.str());
                        if (data)
                            array_.append(data->str());
                    }
                }
                free(itemlist);
            }
        }
        else
        {
            buildVarPath(path, tagname, basepath, NULL, "itemcount", NULL);
            int count=params.getPropInt(path.str(), -1);
            if (count>0)
            {
                for (int idx=0; idx<count; idx++)
                {
                    buildVarPath(path, tagname, basepath, NULL, NULL, &idx);
                    StringBuffer* data = attachments->getValue(path.str());
                    if (data)
                        array_.append(data->str());
                }
            }
        }

    }
}

void CSoapStringArrayField::unserialize(IProperties& params,MapStrToBuf *attachments,const char *basepath,const char *itemname)
{  
    unmarshall(params,attachments,queryFieldName(),basepath,itemname);
}

//void CSoapStringArrayField::unmarshall(IProperties &params, MapStrToBuf *attachments, const char *tagname, const char *basepath, const char *xsdtype, const char *xmlns)
void CSoapStringArrayField::unmarshall(IProperties &params, MapStrToBuf *attachments, const char *tagname, const char *basepath, 
                                               const char* itemname, const char *xsdtype, const char *xmlns)
{
    if (unmarshallRawArray(params, tagname, basepath))
        return;

    //supported encodings
    //property = value
    //--------   ---------
    //tagname_vb_value = boolean
    //tagname_iv_index = value

    StringBuffer path;
    if (basepath && *basepath)
       path.append(basepath).append(".");
    path.append(tagname);
    const char *pathstr=path.str();

    Owned<IPropertyIterator> iter = params.getIterator();

    if (pathstr && *pathstr && iter && iter->first())
    {
        int taglen = strlen(pathstr);
        while (iter->isValid())
        {
            const char *keyname=iter->getPropKey();
            if (strncmp(keyname, pathstr, taglen)==0)
            {
                if (strlen(keyname)==taglen || !strncmp(keyname+taglen, "_rd_", 4))
                {
                    const char *finger = params.queryProp(iter->getPropKey());
                    StringBuffer itemval;
                    while (*finger)
                    {
                        if (*finger=='\r')
                            finger++;

                        if (*finger=='\n')
                        {
                            if (itemval.length())
                                array_.append(itemval.str());
                            itemval.clear();
                        }
                        else
                        {
                            itemval.append(*finger);
                        }
                        finger++;
                    }
                    if (itemval.length())
                        array_.append(itemval.str());
                }
                else if (strncmp(keyname+taglen, "_v", 2)==0)
                {
                    if (params.getPropInt(keyname))
                        array_.append(keyname+taglen+2);
                }
                else if (strncmp(keyname+taglen, "_i", 2)==0)
                {
                    //array_.add(name , val );
                    array_.append(params.queryProp(iter->getPropKey()));
                }
            }

            iter->next();
        }
    }
}

bool CSoapStringArrayField::unmarshallRawArray(IProperties &params, const char *tagname, const char *basepath)
{
    bool rt = false;

    StringBuffer path;
    buildVarPath(path, tagname, basepath, NULL, "itemlist", NULL);
    if (params.hasProp(path.str()))
    {
        //sparse array encoding
        char *itemlist=strdup(params.queryProp(path.str()));
        char *delim=NULL;
        if (itemlist)
        {
            for(char *finger=itemlist; finger; finger=(delim) ? delim+1 : NULL)
            {
                if ((delim=strchr(finger, '+'))) 
                    *delim=0;
                if (*finger)
                {
                    buildVarPath(path, tagname, basepath, finger, NULL, NULL);
                    const char* val = params.queryProp(path);
                    if (val)
                        array_.append(val);
                }
            }
            free(itemlist);

            rt = true;
        }
    }
    else
    {
        buildVarPath(path, tagname, basepath, NULL, "itemcount", NULL);
        int count=params.getPropInt(path.str(), -1);
        if (count>0)
        {
            for (int idx=0; idx<count; idx++)
            {
                buildVarPath(path, tagname, basepath, NULL, NULL, &idx);
                const char* val = params.queryProp(path);
                if (val)
                    array_.append(val);
            }
            rt = true;
        }
    }

    return rt;
}

//=====================================================================================
//  CSoapStructField

CSoapStructField::CSoapStructField(const char* name, CSoapStructType* type) 
    : CSoapFieldBase(name,type), m_nilBH(nilIgnore)//, m_isNil(true)
{
    m_fldCount = type->getFieldCount();
    m_fields = (ISoapField**)malloc(sizeof(ISoapField*)*m_fldCount);
    for (int i=0; i<m_fldCount; i++)
    {
        ISoapType* fldType = type->queryFieldType(i);
        m_fields[i] = fldType->createField(type->queryFieldName(i));
    }
}

CSoapStructField::~CSoapStructField()
{
    /*
    for (int i=0;i<m_fldCount;i++)
        delete m_fields[i];
    free(m_fields);
    */
}

void CSoapStructField::setDerivedType(CSoapStructType* type)
{
    size_t oldCount = m_fldCount;
    m_fldCount = type->getFieldCount();;
    m_fields = (ISoapField**)realloc(m_fields,sizeof(ISoapField*)*(m_fldCount));
    for (int i=oldCount; i<m_fldCount; i++)
    {
        ISoapType* fldType = type->queryFieldType(i);
        m_fields[i] = fldType->createField(type->queryFieldName(i));
    }
    m_type = type;
}

void  CSoapStructField::stValue(FieldValue from)  
{
    size_t fldCount = getFieldCount();
    CSoapStructField* fromFld = dynamic_cast<CSoapStructField*>(from.structV);
    for (int idx=0; idx<fldCount; idx++)
        queryField(idx)->stValue(fromFld->queryField(idx)->gtValue());
}

const char* CSoapStructField::queryItemName(int idx) 
{ 
    const char* itemName = structType()->queryAttribute(idx,"item_tag"); 
    ISoapType* subType = queryField(idx)->queryFieldType();
    if (subType->isPrimitiveType())
        return itemName ? itemName : "Item";
    else
        return itemName ? itemName : subType->queryTypeName();
}

void CSoapStructField::marshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath, 
                                        const char* itemname, const char *xsdtype, const char *xmlns)
{
    StringBuffer xml;
    for (int i=0;i<getFieldCount();i++)
    {
        ISoapField* fld = queryField(i);
        fld->marshall(xml,queryFieldName(i),basepath,queryItemName(i));
    }
    if (xml.length() || m_nilBH==nilIgnore)
        rpc_call.add_value(basepath, xmlns, tagname, xsdtype, xml.str(), false);
}

void CSoapStructField::marshall(StringBuffer &str, const char *tagname, const char *basepath, 
                                        const char* itemname, const char *xsdtype, const char *xmlns)
{
    StringBuffer xml;
    for (int i=0;i<getFieldCount();i++)
        queryField(i)->marshall(xml,queryFieldName(i),basepath, queryItemName(i));
    
    if (xml.length() || m_nilBH==nilIgnore)
        str.appendf("<%s>%s</%s>", tagname, xml.str(), tagname);
}

void CSoapStructField::unmarshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath, 
                                          const char* itemname, const char *xsdtype, const char *xmlns)
{
    StringBuffer path;
    if (basepath && *basepath)
    {
        path.append(basepath);
        if (path.charAt(path.length()-1)!='/')
            path.append("/");
    }
    path.append(tagname);
    
    //TODO: path needs to change for each field?
    for (int i=0;i<getFieldCount();i++)
        queryField(i)->unserialize(rpc_call, queryFieldName(i), path.str(), queryItemName(i));      
}

void CSoapStructField::unmarshall(CSoapValue &soapval, const char *tagname)
{
    CSoapValue *sv = soapval.get_value(tagname);
    if (sv)
    {
        for (int i=0;i<getFieldCount();i++)
            queryField(i)->unserialize(*sv);
    }
}

void CSoapStructField::unmarshall(IProperties &params, MapStrToBuf *attachments, const char *tagname, const char *basepath, 
                                          const char* itemname, const char *xsdtype, const char *xmlns)
{
    StringBuffer path;
    if (basepath && *basepath)
        path.append(basepath).append(".");
    path.append(tagname);
    
    for (int i=0;i<getFieldCount();i++)
    {
        ISoapField* fld = queryField(i);
        //TODO: old format always use Type name as item-name
        // const char* itemname = queryItemName(i);
        const char* itemName = fld->queryFieldType()->queryTypeName();
        fld->unmarshall(params, attachments, fld->queryFieldName(), path.str(), itemName);
    }
}

void CSoapStructField::unserialize(IRpcMessage& rpc_request, const char *tagname, const char *basepath, const char* itemname)
{
    rpc_request.setEncodeXml(false);
    StringBuffer path;
    if (basepath && *basepath)
        path.append(basepath).append(".");
    path.append(tagname);
    for (int i=0;i<getFieldCount();i++)
    {
        ISoapField* fld = queryField(i);
        fld->unmarshall(rpc_request, fld->queryFieldName(), basepath,itemname);
    }
}

void CSoapStructField::unserialize(CSoapValue& soapval)
{
    for (int i=0;i<getFieldCount();i++)
    {
        ISoapField* fld = queryField(i);
        fld->unmarshall(soapval, fld->queryFieldName());
    }
}

void CSoapStructField::unserialize(IProperties& params, MapStrToBuf *attachments, const char *basepath, const char* itemname)
{
    for (int i=0;i<getFieldCount();i++)
    {
        ISoapField* fld = queryField(i);
        fld->unmarshall(params, attachments, fld->queryFieldName(), basepath, itemname);
    }
}

void CSoapStructField::serializeContent(StringBuffer& buffer)
{
    for (int i=0;i<getFieldCount();i++)
    {
        ISoapField* fld = queryField(i);
        fld->marshall(buffer, fld->queryFieldName(),"",NULL);   
    }
}

//=====================================================================================
// CSoapStructArray

ISoapField* CSoapStructArrayType::createField(const char* name) {  return new CSoapStructArrayField(name,this); }

typedef std::map<ISoapType*, CSoapStructArrayType*> SoapTypeHash;

CSoapStructArrayType* CSoapStructArrayType::instance(CSoapStructType* base)
{
    static SoapTypeHash types;

    SoapTypeHash::const_iterator it = types.find(base);
    if (it == types.end())
    {
        CSoapStructArrayType* type = new CSoapStructArrayType(base);
        types[base] = type;
        return type;
    }
    else
        return it->second;
}

StringBuffer& CSoapStructArrayType::getXsdDefinition(IEspContext& ctx, const char *msgTypeName, StringBuffer &schema, wsdlIncludedTable &added, const char *xns, const char *wsns, unsigned flags)
{
    //TODO: add definition for ArrayOf<BaseType> - no need for the type if it is not used
    if (!added.getValue(queryTypeName()))
    {
        m_baseType->getXsdDefinition(ctx,queryTypeName(),schema,added,xns,wsns,flags);
        added.setValue(queryTypeName(),1);
    }
    return schema;
}

void CSoapStructArrayField::copy(CSoapStructArrayField &from)
{
    FieldValue fv(&from.array_);
    stValue(fv);
}

void CSoapStructArrayField::stValue(FieldValue fv)
{
    array_.kill();
    int count= fv.arrayV->ordinality();
    for (int index = 0; index<count; index++)
    {
        fv.arrayV->item(index).Link();
        array_.append(fv.arrayV->item(index));
    }   
}

void CSoapStructArrayField::marshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath, 
                                             const char* itemname, const char *xsdtype, const char *xmlns)
{
    int count= array_.ordinality();
    if (count > 0)
    {
        StringBuffer xml;
        xml.appendf("<%s>", tagname);
        for (int idx = 0; idx<count; idx++)
        {
            ISoapField& fld = array_.item(idx);
            fld.marshall(rpc_call, itemname,basepath,fld.queryFieldName());
        }
        xml.appendf("</%s>", tagname);

        rpc_call.add_value(basepath, xmlns, tagname, itemname, xml.str(), false);
    }
}

void CSoapStructArrayField::marshall(StringBuffer &xml, const char *tagname, const char *basepath, 
                                             const char* itemname, const char *xsdtype, const char *xmlns)
{
    int count= array_.ordinality();
    if (count > 0)
    {
        xml.appendf("<%s>", tagname);
        for (int idx = 0; idx<count; idx++)
        {
            ISoapField& fld = array_.item(idx);
            fld.marshall(xml, itemname,basepath,fld.queryFieldName());
        }
        xml.appendf("</%s>", tagname);
    }
}

void CSoapStructArrayField::unmarshall(CSoapValue &soapval, const char *tagname)
{
    CSoapValue *sv = soapval.get_value(tagname);
    if (sv)
    {
        SoapValueArray* children = sv->query_children();
        if (children)
        {
            ISoapType* type = queryBaseType();
            ForEachItemIn(x, *children)
            {
                CSoapValue& onechild = children->item(x);
                ISoapField* fld = type->createField("Item");
                fld->unserialize(onechild);
                array_.append(*fld);
            }
        }
    }
}

void CSoapStructArrayField::unmarshall(IRpcMessage &rpc_call, const char *tagname, const char *basepath, 
                                               const char* itemname, const char* xsdtype, const char *xmlns)
{
    CRpcMessage *rpcmsg = dynamic_cast<CRpcMessage *>(&rpc_call);
    if (rpcmsg)
    {
        StringBuffer path(basepath);
        if (basepath && *basepath)
            path.append("/");
        path.append(tagname);
        CSoapValue *soapval=rpcmsg->get_value(path.str());
        if (soapval)
            unmarshall(*soapval, NULL);
    }
}

void CSoapStructArrayField::unmarshallArrayItem(IProperties &params,MapStrToBuf *attachments,const char* tagname,const char* basepath)
{
    CSoapStructType* base = queryBaseType();
    ISoapField* fld = base->createField(tagname);
    fld->unserialize(params, attachments, basepath);
    array_.append(*fld);
}

void CSoapStructArrayField::unmarshall(IProperties &params,MapStrToBuf *attachments,const char *tagname,const char *basepath, 
                                               const char* itemname, const char *xsdtype, const char *xmlns)
{
    StringBuffer path;
    buildVarPath(path, tagname, basepath, itemname, "itemlist", NULL);
    if (params.hasProp(path.str()))
    {
        unmarshallArrayItem(params,attachments,tagname,path);
    }
    else
    {
        buildVarPath(path, tagname, basepath, itemname, "itemcount", NULL);
        int count=params.getPropInt(path.str(), -1);
        for (int idx=0; idx<count; idx++)
        {
            buildVarPath(path, tagname, basepath, itemname, NULL, &idx);                
            unmarshallArrayItem(params,attachments,itemname,path);
        }
    }
}

//=====================================================================================
// CSoapRequestField

void CSoapRequestField::post(const char *proxy, const char* url, IRpcResponseBinding& response)
{
    CRpcCall rpccall;
    CRpcResponse rpcresponse;
    
    rpccall.set_url(url);
    rpccall.setProxy(proxy);
    
    serialize(*static_cast<IRpcMessage*>(&rpccall));
    
    CSoapClient soapclient;
    soapclient.setUsernameToken(getUserId(), getPassword(), getRealm());
    int result = soapclient.postRequest(rpccall, rpcresponse);
    
    if(result == SOAP_OK)
    {
        response.setRpcState(RPC_MESSAGE_OK);
        response.unserialize(rpcresponse, NULL, NULL);
    }
    else if(result == SOAP_CONNECTION_ERROR)
    {
        response.setRpcState(RPC_MESSAGE_CONNECTION_ERROR);
    }
    else
    {
        response.setRpcState(RPC_MESSAGE_ERROR);
    }
}

//=====================================================================================
// CSoapResponseField
CSoapResponseField::CSoapResponseField(CSoapStructType* type,const char *serviceName, IRpcMessageBinding *init)
        : CSoapStructField("UnnamedResponse", type)
{
    doInit();
    setMsgName(type->queryTypeName());
    if (init)
    {
        setClientValue(init->getClientValue());
        setReqId(init->getReqId());
        setEventSink(init->getEventSink());
        setState(init->queryState());
        setThunkHandle(init->getThunkHandle());
        setMethod(init->getMethod());
    }
}

CSoapResponseField::CSoapResponseField(CSoapStructType* type, const char *serviceName, IRpcMessage* rpcmsg) 
    : CSoapStructField("UnnamedResponse", type)
{
    doInit();
    CSoapStructField::unserialize(*rpcmsg,NULL,NULL);
}

CSoapResponseField::CSoapResponseField(CSoapStructType* type, const char *serviceName, IProperties *params, MapStrToBuf *attachments)
    : CSoapStructField("UnnamedResponse", type)
{
    doInit();
    CSoapStructField::unserialize(*params,attachments, NULL);
}

void CSoapResponseField::serialize(IRpcMessage& rpc_resp)
{
    IEspContext *ctx=rpc_resp.queryContext();
    double clientVer=(ctx) ? ctx->getClientVersion() : 0.0;
    rpc_resp.set_ns("");
    rpc_resp.set_name(queryMsgName());

    //TODO: dynamically get namesapce
    StringBuffer nsuri;
    nsuri.append("urn:hpccsystems:ws:").appendLower(m_serviceName.length(), m_serviceName.str());
    rpc_resp.set_nsuri(nsuri.str());

    const IMultiException& exceptions = getExceptions();
    if (exceptions.ordinality() > 0)
    {
        StringBuffer xml;
        exceptions.serialize(xml, 0, true, false);
        rpc_resp.add_value("", "", "Exceptions", "", xml.str(), false);
    }
    else
    {
        for (int i=0; i<getFieldCount(); i++)
            queryField(i)->marshall(rpc_resp, queryFieldName(i),"",NULL);
    }
}

void CSoapResponseField::serialize(MemoryBuffer& buffer, StringBuffer &mimetype)
{
    StringBuffer strbuffer;
    strbuffer.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
    serialize(strbuffer);
    buffer.append(strbuffer.length(), strbuffer.str());
    mimetype.clear().append("text/xml; charset=UTF-8");
}

void CSoapResponseField::serialize(StringBuffer& buffer, const char *name)
{
    const char *tname = (name && *name) ? name : m_msgName.str();
    buffer.appendf("<%s>", tname);
    serializeContent(buffer);
    buffer.appendf("</%s>", tname);
}

//=====================================================================================
// CMySoapBinding

CMySoapBinding::CMySoapBinding(const char* svcName, http_soap_log_level level)
: CHttpSoapBinding(NULL,NULL,NULL,level),m_serviceName(svcName)
{
}

CMySoapBinding::CMySoapBinding(const char* svcName, IPropertyTree* cfg, const char *bindname, const char *procname, http_soap_log_level level)
: CHttpSoapBinding(cfg, bindname, procname, level), m_serviceName(svcName)
{
}

int CMySoapBinding::processRequest(IRpcMessage* rpc_call, IRpcMessage* rpc_response)
{
    if(rpc_call == NULL || rpc_response == NULL)
        return -1;

    IEspContext *ctx=rpc_call->queryContext();
    double clientVer=(ctx) ? ctx->getClientVersion() : 0.0;
    CRpcCall* thecall = dynamic_cast<CRpcCall *>(rpc_call);
    CRpcResponse* response = dynamic_cast<CRpcResponse*>(rpc_response);

    if(m_service == NULL)
    {
        response->set_status(SOAP_SERVER_ERROR);
        response->set_err("Service not available");
        DBGLOG("Service not available");
        return -1;
    }
    if (thecall->get_name() == NULL)
    {
        response->set_status(SOAP_CLIENT_ERROR);
        response->set_err("No service method specified");
        ERRLOG("No service method specified");
        return -1;
    }

    CEspMethod* mth = queryMethod(thecall->get_name());
    if (mth)
    {
        Owned<CSoapRequestField> esp_request = (CSoapRequestField*)mth->m_request->createField(m_serviceName, thecall);
        Owned<CSoapResponseField> esp_response = (CSoapResponseField*)mth->m_response->createField(m_serviceName);          
        m_service->runQuery(mth->m_queryId,*rpc_call->queryContext(),*esp_request.get(),*esp_response.get());
        response->set_status(SOAP_OK);
        response->set_name("TestResponse");
        esp_response->serialize(*response);
        return 0;
    }

    response->set_status(SOAP_CLIENT_ERROR);
    StringBuffer msg, svcName;
    msg.appendf("Method %s not available in service %s",thecall->get_name(),getServiceName(svcName).str());
    ERRLOG(msg);
    response->set_err(msg);
    return -1;
}

CEspMethod* CMySoapBinding::queryMethod(const char* method)
{
    for (MethodMap::const_iterator it = m_methods.begin(); it != m_methods.end(); ++it)
    {
        CEspMethod* m = (*it).second;
        if (stricmp(method, m->m_name)==0 || stricmp(method, m->m_request->queryTypeName())==0)
            return m;
    }
    return NULL;
}

int CMySoapBinding::getXsdDefinition(IEspContext& ctx, CHttpRequest *request, StringBuffer &content, const char *service, const char *method, bool mda)
{
    wsdlIncludedTable added;

    //TODO: other ESPuses types
    //CTestStructType::instance()->getXsdDefinition(content, added);

    bool allMethods = (method==NULL || *method==0);

    for (MethodMap::const_iterator it = m_methods.begin(); it != m_methods.end(); ++it)
    {
        CEspMethod* m = (*it).second;
        if (allMethods || Utils::strcasecmp(method, m->m_name)==0)
        {
            m->m_request->getXsdDefinition(ctx, content, added);
            m->m_response->getXsdDefinition(ctx, content, added);
        }
    }

    return 0;
}

int CMySoapBinding::getMethodHtmlForm(IEspContext &context, CHttpRequest* request, const char *serv, const char *method, StringBuffer &page, bool bIncludeFormTag)
{
    for (MethodMap::const_iterator it = m_methods.begin(); it != m_methods.end(); ++it)
    {
        CEspMethod* m = (*it).second;
        if (Utils::strcasecmp(method, m->m_name)==0)
        {
            m->m_request->getHtmlForm(context, request, serv, method, page, true, "");
            break;
        }
    }

    return 0;
}

int CMySoapBinding::getQualifiedNames(IEspContext& ctx,MethodInfoArray & methods)
{
    for (MethodMap::const_iterator it = m_methods.begin(); it!=m_methods.end(); it++)
    {
        CEspMethod* mth = it->second;
        methods.append(*new CMethodInfo(mth->m_name,mth->m_request->queryTypeName(),mth->m_response->queryTypeName()));
    }

    return methods.ordinality();
}

bool CMySoapBinding::qualifyMethodName(IEspContext &context, const char *methname, StringBuffer *methQName)
{
    if (!methQName)
        return true;

    if (!methname)
    {
        methQName->clear();
        return true;
    }

    for (MethodMap::const_iterator it = m_methods.begin(); it!=m_methods.end(); it++)
    {
        CEspMethod* mth = it->second;
        if (Utils::strcasecmp(methname, mth->m_name)==0)
        {
            methQName->set(mth->m_name);
            return true;
        }
    }

    return false;
}

bool CMySoapBinding::qualifyServiceName(IEspContext &context, const char *servname, const char *methname, StringBuffer &servQName, StringBuffer *methQName)
{
    if (Utils::strcasecmp(servname, m_serviceName)==0)
    {
        servQName.clear().append(m_serviceName);
        return qualifyMethodName(context, methname, methQName);
    }
    return false;
}

int CMySoapBinding::onGetFile(IEspContext &context, CHttpRequest* request, CHttpResponse* response, const char *pathex)
{
    if(request == NULL || response == NULL)
        return -1;
    StringBuffer mimetype;
    MemoryBuffer content;

    StringBuffer filepath;
    getBaseFilePath(filepath);
    if (strchr("\\/", filepath.charAt(filepath.length()-1))==NULL)
        filepath.append("/");
    filepath.append(pathex);
    response->httpContentFromFile(filepath.str());
    response->send();
    return 0;
}

int CMySoapBinding::onGetForm(IEspContext &context, CHttpRequest* request, CHttpResponse* response, const char *service, const char *method)
{
    return EspHttpBinding::onGetForm(context, request, response, service, method);
}

int CMySoapBinding::onGetService(IEspContext &context, CHttpRequest* request, CHttpResponse* response, const char *service, const char *method, const char *pathex)
{
    if(request == NULL || response == NULL)
        return -1;
    return onGetQuery(context, request, response, service, method);
}

int CMySoapBinding::onGetInstantQuery(IEspContext &context, CHttpRequest* request, CHttpResponse* response, const char *service, const char *method)
{
    //TODO:
    //context.setClientVersion(m_version);
    if(request == NULL || response == NULL)
        return -1;
    if(m_service == NULL)
    {
        StringBuffer respStr("Service not available");
        response->setContent(respStr.str());
        response->setContentType("text/html");
        response->send();
    }
    else for (MethodMap::const_iterator it = m_methods.begin(); it != m_methods.end(); ++it)
    {
        CEspMethod* m = (*it).second;

        if(stricmp(method, m->m_name)==0 // TestQuery
            || stricmp(method, m->m_request->queryTypeName())==0) // "TestRequest" is OK too
        {       
            Owned<CSoapRequestField> esp_request = dynamic_cast<CSoapRequestField*>(m->m_request->createField(
                m_serviceName, request->queryParameters(), request->queryAttachments()));
            Owned<CSoapResponseField> esp_response = dynamic_cast<CSoapResponseField*>(m->m_response->createField(m_serviceName));          
            m_service->runQuery(m->m_queryId,*request->queryContext(), *esp_request.get(), *esp_response.get());

            if (esp_response->getRedirectUrl() && *esp_response->getRedirectUrl())
            {
                response->redirect(*request, esp_response->getRedirectUrl());
            }
            else
            {
                MemoryBuffer content;
                StringBuffer mimetype;;
                esp_response->serialize(content, mimetype);
                response->setContent(content.length(), content.toByteArray());
                response->setContentType(mimetype.str());
                response->send();
            }
            return 0;
        }
    }
    
    return onGetNotFound(context, request,  response, service);
}

//=====================================================================================
// CServiceClient

interface IEspClient : public IInterface
{
    virtual void setProxyAddress(const char *addr) = 0;
    virtual void addServiceUrl(const char *url)    = 0;
    virtual void removeServiceUrl(const char *url) = 0;
    virtual void setUsernameToken(const char *userid,const char *password,const char *realm) = 0;
};

class CServiceClient : public CInterface, implements IEspClient
{
protected:
    StringBuffer m_proxy;
    StringBuffer m_url;
    StringBuffer m_userid;
    StringBuffer m_password;
    StringBuffer m_realm;
    long m_reqId;
    StringBuffer m_serviceName;

public:
    IMPLEMENT_IINTERFACE;
    
    CServiceClient(const char* svcName) : m_reqId(0), m_serviceName(svcName) { }

    void setProxyAddress(const char *addr) { m_proxy.clear().append(addr); }
    void addServiceUrl(const char *url)    { m_url.clear().append(url); }
    void removeServiceUrl(const char *url) { assertex(false); }
    void setUsernameToken(const char *userid,const char *password,const char *realm)
    {
        m_userid.clear().append(userid);
        m_password.clear().append(password);
        m_realm.clear().append(realm);
    }

    IEspRequest* createRequest(CSoapStructType* type);
};

IEspRequest* CServiceClient::createRequest(CSoapStructType* type)
{
    CSoapRequestField* request = (CSoapRequestField*)type->createField(m_serviceName);
    request->setProxyAddress(m_proxy.str());
    request->setUrl(m_url.str());
    return request;
}

/*
IClientTestRequest * CClientWsTest::createTestQueryRequest()
{
    CTestRequest* request = new CTestRequest("WsTest");
    request->setProxyAddress(m_proxy.str());
    request->setUrl(m_url.str());
    return request;
}

IClientTestResponse * CClientWsTest::TestQuery(IClientTestRequest *request)
{
    if(m_url.length()== 0){ throw MakeStringExceptionDirect(-1, "url not set"); }

    CTestRequest* esprequest = dynamic_cast<CTestRequest*>(request);
    CTestResponse* espresponse = new CTestResponse("WsTest");

    espresponse->setReqId(m_reqId++);
    esprequest->setUserId(m_userid.str());
    esprequest->setPassword(m_password.str());
    esprequest->setRealm(m_realm.str());
    esprequest->post(m_proxy.str(), m_url.str(), *espresponse);
    return espresponse;
}

void CClientWsTest::async_TestQuery(IClientTestRequest *request, IClientWsTestEvents *events,IInterface* state)
{
    if(m_url.length()==0){ throw MakeStringExceptionDirect(-1, "url not set"); }

    CTestRequest* esprequest = dynamic_cast<CTestRequest*>(request);
    esprequest->setMethod("TestQuery");
    esprequest->setReqId(m_reqId++);
    esprequest->setEventSink(events);
    esprequest->setState(state);
    esprequest->setUserId(m_userid.str());
    esprequest->setPassword(m_password.str());
    esprequest->setRealm(m_realm.str());
#ifdef USE_CLIENT_THREAD
    esprequest->setThunkHandle(GetThunkingHandle());
#endif
    esprequest->Link();

    if(state!=NULL)
        state->Link();

    PrintLog("Starting query thread...");

#ifdef _WIN32
    _beginthread(espWorkerThread, 0, (void *)(IRpcRequestBinding *)(esprequest));
#else
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setscope(&attr, PTHREAD_SCOPE_SYSTEM);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    pthread_attr_setstacksize(&attr, 0x10000);
    ThreadId threadid;
    int status;
    do
    {
        status = pthread_create(&threadid, &attr, CClientWsTest::espWorkerThread, (void *)(IRpcRequestBinding *)(esprequest));
    } while (0 != status && (errno == EINTR));
    if (status) {
        Release();
        throw MakeOsException(errno);
    }
#endif
}

IClientTestResponse *CClientWsTest::TestQueryFn(const char * Name_, int Age_, bool UsCitizen_, IConstTestStruct &Data_, IConstTestStruct &Data2_)
{
    Owned<IClientTestRequest> req =  createTestQueryRequest();
    req->setName(Name_);
    req->setAge(Age_);
    req->setUsCitizen(UsCitizen_);
    req->setData(Data_);
    req->setData2(Data2_);
    return TestQuery(req.get());
}

int CClientWsTest::transferThunkEvent(void *data)
{
    IRpcResponseBinding *response = (IRpcResponseBinding *)data;
    if (response!=NULL)
    {
        IClientWsTestEvents *eventSink = (IClientWsTestEvents *)response->getEventSink();
        response->lock();

        if (stricmp(response->getMethod(), "TestQuery")==0)
        {
            if(response->getRpcState() == RPC_MESSAGE_OK)
                eventSink->onTestQueryComplete(dynamic_cast<IClientTestResponse*>(response),response->queryState());
            else
                eventSink->onTestQueryError(dynamic_cast<IClientTestResponse*>(response),response->queryState());
        }
        response->unlock();
    }
    return 0;
}

#ifdef _WIN32
void CClientWsTest::espWorkerThread(void* data)
#else
void *CClientWsTest::espWorkerThread(void *data)
#endif
{
    IRpcRequestBinding *request = (IRpcRequestBinding *) data;

    if (request != NULL)
    {
        request->lock();
        IRpcResponseBinding *response=NULL;

        if (stricmp(request->getMethod(), "TestQuery")==0)
        {
            response = new CTestResponse("WsTest", request);
        }
        if (response!=NULL)
        {
            try{
                request->post(*response);
#ifdef USE_CLIENT_THREAD
            ThunkToClientThread(request->getThunkHandle(), transferThunkEvent, (void *)response);
#else
            transferThunkEvent((void *)response);
#endif
            }
            catch(IException* ex){
                StringBuffer errorStr;
                ex->errorMessage(errorStr);
                PrintLog("Exception caught while posting async request%s",errorStr.str());
                ex->Release();
            }
            catch(...){
                PrintLog("Unknown exception caught while posting async request");
            }
        }
        request->unlock();
        if(request->queryState()!=NULL)
            request->queryState()->Release();

        if(response!=NULL)
            response->Release();

        request->Release();
    }
#if defined(_WIN32)
#else
    return (void *) 0 ;
#endif
}
*/
// END
//=====================================================================================
