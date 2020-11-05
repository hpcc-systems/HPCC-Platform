/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

#pragma warning (disable : 4786)

#include "esphttp.hpp"

//Jlib
#include "jliball.hpp"

//SCM Interface definition includes:
#include "esp.hpp"
#include "espthread.hpp"

//ESP Bindings
#include "http/platform/httpprot.hpp"
#include "http/platform/httptransport.ipp"
#include "http/platform/httpservice.hpp"
#include "SOAP/Platform/soapservice.hpp"

#ifdef WIN32
#define MSGBUILDER_EXPORT _declspec(dllexport)
#else
#define MSGBUILDER_EXPORT
#endif

#include "msgbuilder.hpp"

//  ===========================================================================
CSoapMsgBuilder::CSoapMsgBuilder(const char * xml)
{
    m_properties.setown(createPTreeFromXMLString(xml));
}

void CSoapMsgBuilder::setPropertyValue(const char * key, const char * val)
{
    assertex(m_properties->hasProp(key));
    m_properties->setProp(key, val);
}

void CSoapMsgBuilder::setPropertyValueInt(const char * key, unsigned int val)
{
    char buff[64];
    itoa(val, buff, 10);
    setPropertyValue(key, buff);
}

void CSoapMsgBuilder::setPropertyValueBool(const char * key, bool val)
{
    setPropertyValueInt(key, val == true ? 1 : 0);
}

StringBuffer & CSoapMsgBuilder::getSoapResponse(StringBuffer & soapResponse)
{
    IPropertyTreeIterator * itr = m_properties->getElements("*");
    itr->first();
    while(itr->isValid())
    {
        IPropertyTree & prop = itr->query();
        StringBuffer key, val;
        prop.getName(key);
        m_properties->getProp(key.str(), val);
        soapResponse.appendf("<%s>%s</%s>", key.str(), val.str(), key.str()); 
        itr->next();
    }
    itr->Release();
    return soapResponse;
}

CSoapMsgArrayBuilder::CSoapMsgArrayBuilder(CSoapMsgXsdBuilder * xsd)
{
    m_xsd.set(xsd);
}

unsigned CSoapMsgArrayBuilder::ordinality()
{
    return m_array.ordinality();
}

CSoapMsgBuilder * CSoapMsgArrayBuilder::item(unsigned i)
{
    return &m_array.item(i);
}

CSoapMsgBuilder * CSoapMsgArrayBuilder::newMsgBuilder()
{
    CSoapMsgBuilder * retVal = m_xsd->newMsgBuilder();
    retVal->Link();
    m_array.append(*retVal);
    return retVal;
}

StringBuffer & CSoapMsgArrayBuilder::getSoapResponse(StringBuffer & soapResponse)
{
    StringBuffer label;
    m_xsd->getLabel(label);
    ForEachItemIn(i, m_array)
    {
        CSoapMsgBuilder &msg = m_array.item(i);
        soapResponse.appendf("<%s>", label.str());
        msg.getSoapResponse(soapResponse);
        soapResponse.appendf("</%s>", label.str());
    }
    return soapResponse;
}

CSoapMsgXsdBuilder::CSoapMsgXsdBuilder(const char * structLabel, const char * var)
{
    m_properties.set(createPTree(structLabel));
    m_structLabel.append(structLabel);
    m_var.append(var);
}
void CSoapMsgXsdBuilder::appendProperty(const char * prop, XSD_TYPES type)
{
     m_properties->setPropInt(prop, type);
}

StringBuffer & CSoapMsgXsdBuilder::getLabel(StringBuffer & label)
{
    label.append(m_structLabel.str());
    return label;
}

StringBuffer & CSoapMsgXsdBuilder::getXsd(StringBuffer & wsdlSchema)
{
    wsdlSchema.appendf("<%s:complexType name=\"%s\" >", m_var.str(), m_structLabel.str());
    wsdlSchema.appendf("<%s:sequence>", m_var.str());

    IPropertyTreeIterator * itr = m_properties->getElements("*");
    itr->first();
    while(itr->isValid())
    {
        IPropertyTree & prop = itr->query();
        StringBuffer name;
        prop.getName(name);
        wsdlSchema.appendf("<%s:element minOccurs=\"0\" maxOccurs=\"1\" name=\"%s\" type=\"xsd:%s\"/>", m_var.str(), name.str(), getXsdTypeLabel(static_cast<XSD_TYPES>(m_properties->getPropInt(name.str()))));
        itr->next();
    }
    itr->Release();
    wsdlSchema.appendf("</%s:sequence>", m_var.str());
    wsdlSchema.appendf("</%s:complexType>", m_var.str());

    wsdlSchema.appendf("<%s:complexType name=\"ArrayOf%s\" >", m_var.str(), m_structLabel.str());
    wsdlSchema.appendf("<%s:sequence>", m_var.str());
    wsdlSchema.appendf("<%s:element minOccurs=\"0\" maxOccurs=\"unbounded\" name=\"%s\" type=\"tns:%s\"/>", m_var.str(), m_structLabel.str(), m_structLabel.str());
    wsdlSchema.appendf("</%s:sequence>", m_var.str());
    wsdlSchema.appendf("</%s:complexType>", m_var.str());

    wsdlSchema.appendf("<%s:element name=\"%s\" nillable=\"true\" type=\"tns:%s\" />", m_var.str(), m_structLabel.str(), m_structLabel.str());
    wsdlSchema.appendf("<%s:element name=\"ArrayOf%s\" nillable=\"true\" type=\"tns:ArrayOf%s\" />", m_var.str(), m_structLabel.str(), m_structLabel.str()); 
  return wsdlSchema;
}

const char * const XSD_STRING_DESC = "string";
const char * const XSD_INT_DESC = "int";
const char * const XSD_BOOL_DESC = "boolean";

const char * CSoapMsgXsdBuilder::getXsdTypeLabel(XSD_TYPES type)
{
    switch(type)
    {
    case XSD_STRING:
        return XSD_STRING_DESC;
    case XSD_INT:
        return XSD_INT_DESC;
    case XSD_BOOL:
        return XSD_BOOL_DESC;
    default: 
        return XSD_STRING_DESC;
    }
}

CSoapMsgBuilder * CSoapMsgXsdBuilder::newMsgBuilder()
{
    Owned<IPropertyTree>newXml = createPTree(m_structLabel.str());

    IPropertyTreeIterator * itr = m_properties->getElements("*");
    itr->first();
    while(itr->isValid())
    {
        IPropertyTree & prop = itr->query();
        StringBuffer name;
        prop.getName(name);
        switch(m_properties->getPropInt(name.str()))
        {
        case XSD_INT:
            newXml->setProp(name.str(), "0");
            break;
        case XSD_BOOL:
            newXml->setProp(name.str(), "0");
            break;
        default:
            newXml->setProp(name.str(), "");
        }
        itr->next();
    }
    itr->Release();

    StringBuffer xml;
    return new CSoapMsgBuilder(toXML(newXml, xml).str());
}

CSoapMsgArrayBuilder * CSoapMsgXsdBuilder::newMsgArrayBuilder()
{
    return new CSoapMsgArrayBuilder(this);
}

//  ===========================================================================
