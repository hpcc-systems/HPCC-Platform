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

#ifndef __MSGBUILDER_HPP_
#define __MSGBUILDER_HPP_

//Jlib
#include "jliball.hpp"

#ifndef MSGBUILDER_EXPORT
#define MSGBUILDER_EXPORT
#endif

class MSGBUILDER_EXPORT CSoapMsgBuilder : public CInterface
{
protected:
    Owned<IPropertyTree> m_properties;

public:
    IMPLEMENT_IINTERFACE;

    CSoapMsgBuilder(const char * xml);

    void setPropertyValue(const char * key, const char * val);
    void setPropertyValueInt(const char * key, unsigned int val);
    void setPropertyValueBool(const char * key, bool val);
    StringBuffer & getSoapResponse(StringBuffer & soapResponse);
};

class MSGBUILDER_EXPORT CSoapMsgXsdBuilder;
class MSGBUILDER_EXPORT CSoapMsgArrayBuilder : public CInterface
{
protected:
    CIArrayOf<CSoapMsgBuilder> m_array;
    Linked<CSoapMsgXsdBuilder> m_xsd;

public:
    IMPLEMENT_IINTERFACE;
    CSoapMsgArrayBuilder(CSoapMsgXsdBuilder * xsd);

    CSoapMsgBuilder * newMsgBuilder();
    StringBuffer & getSoapResponse(StringBuffer & soapResponse);

    //Shouldn't be needed
    unsigned ordinality();
    CSoapMsgBuilder * item(unsigned i);
};

enum XSD_TYPES
{
    XSD_UNKNOWN,
    XSD_STRING,
    XSD_INT,
    XSD_BOOL
};

class MSGBUILDER_EXPORT CSoapMsgXsdBuilder : public CInterface
{
protected:
    StringBuffer m_method;
    StringBuffer m_structLabel;
    StringBuffer m_var;
    Owned<IPropertyTree> m_properties;
    void appendProperty(const char * prop, XSD_TYPES type = XSD_STRING);

public:
    IMPLEMENT_IINTERFACE;

    CSoapMsgXsdBuilder(const char * structLabel, const char * var = "xsd");
    StringBuffer & getLabel(StringBuffer & label);
    StringBuffer & getXsd(StringBuffer & wsdlSchema);
    static const char * getXsdTypeLabel(XSD_TYPES type);

    CSoapMsgBuilder * newMsgBuilder();
    CSoapMsgArrayBuilder * newMsgArrayBuilder();
};

#endif //__HTMLPAGE_HPP_

