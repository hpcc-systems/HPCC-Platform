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
    CSoapMsgXsdBuilder(const char * structLabel, const char * var = "xsd");
    StringBuffer & getLabel(StringBuffer & label);
    StringBuffer & getXsd(StringBuffer & wsdlSchema);
    static const char * getXsdTypeLabel(XSD_TYPES type);

    CSoapMsgBuilder * newMsgBuilder();
    CSoapMsgArrayBuilder * newMsgArrayBuilder();
};

#endif //__HTMLPAGE_HPP_

