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

#pragma warning (disable : 4786)

#ifdef WIN32
#ifdef ESPHTTP_EXPORTS
    #define esp_http_decl __declspec(dllexport)
#endif
#endif

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

//openssl
#include <openssl/rsa.h>
#include <openssl/crypto.h>


#ifdef WIN32
#define MSGBUILDER_EXPORT _declspec(dllexport)
#else
#define MSGBUILDER_EXPORT
#endif

#include "msgbuilder.hpp"

//  ===========================================================================
CSoapMsgBuilder::CSoapMsgBuilder(const char * xml)
{
    m_properties.setown(createPTreeFromXMLString(xml, false));
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
    m_properties.set(createPTree(structLabel, false));
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
    Owned<IPropertyTree>newXml = createPTree(m_structLabel.str(), false);

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
