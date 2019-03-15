<?xml version="1.0" encoding="UTF-8"?>
<!--
##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems.
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
##############################################################################
-->

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/" xmlns:http="http://schemas.xmlsoap.org/wsdl/http/" xmlns:mime="http://schemas.xmlsoap.org/wsdl/mime/" xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/">
    <xsl:output method="text" omit-xml-declaration="yes" indent="no"/>

    <xsl:template match="esxdl">
<xsl:variable name="servicename"><xsl:value-of select="EsdlService/@name"/></xsl:variable>
<xsl:variable name="upperService"><xsl:value-of select="translate(EsdlService/@name, 'abcdefghijklmnopqrstuvwxyz', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ')"/></xsl:variable>
<xsl:text>#ifndef </xsl:text><xsl:value-of select="$upperService"/>SERVICEBASE_HPP__
<xsl:text>#define </xsl:text><xsl:value-of select="$upperService"/>SERVICEBASE_HPP__

#include "jlib.hpp"
#include "jptree.hpp"
#include "jarray.hpp"
#include "primitivetypes.hpp"

using namespace std;
using namespace cppplugin;

class EsdlContext : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned&lt;PString&gt; username;
    Owned&lt;Integer&gt; clientMajorVersion;
    Owned&lt;Integer&gt; clientMinorVersion;
};

<xsl:apply-templates select="EsdlEnumType"/>
<xsl:apply-templates select="EsdlStruct"/>
<xsl:apply-templates select="EsdlRequest"/>
<xsl:apply-templates select="EsdlResponse"/>
<xsl:apply-templates select="EsdlService"/>
// User need to implement this function
extern "C" <xsl:value-of select="$servicename"/>ServiceBase* create<xsl:value-of select="$servicename"/>ServiceObj();

<xsl:text>
#endif</xsl:text>
    </xsl:template>

    <xsl:template match="EsdlStruct|EsdlRequest|EsdlResponse">
class <xsl:value-of select="@name"/><xsl:choose><xsl:when test="@base_type"> : public <xsl:value-of select="@base_type"/></xsl:when><xsl:otherwise> : public CInterface, implements IInterface</xsl:otherwise></xsl:choose>
{
public:
    IMPLEMENT_IINTERFACE;

<xsl:apply-templates select="EsdlElement|EsdlArray|EsdlEnum"/><xsl:text>};
</xsl:text>
    </xsl:template>

    <xsl:template name="outputCppPrimitive">
        <xsl:param name="typename"/>
        <xsl:choose>
            <xsl:when test="$typename='bool'"><xsl:value-of select="'Boolean'"/></xsl:when>
            <xsl:when test="$typename='boolean'"><xsl:value-of select="'Boolean'"/></xsl:when>
            <xsl:when test="$typename='decimal'"><xsl:value-of select="'Double'"/></xsl:when>
            <xsl:when test="$typename='float'"><xsl:value-of select="'Float'"/></xsl:when>
            <xsl:when test="$typename='double'"><xsl:value-of select="'Double'"/></xsl:when>
            <xsl:when test="$typename='integer'"><xsl:value-of select="'Integer'"/></xsl:when>
            <xsl:when test="$typename='int64'"><xsl:value-of select="'Integer64'"/></xsl:when>
            <xsl:when test="$typename='uint64'"><xsl:value-of select="'Integer64'"/></xsl:when>
            <xsl:when test="$typename='long'"><xsl:value-of select="'Integer'"/></xsl:when>
            <xsl:when test="$typename='int'"><xsl:value-of select="'Integer'"/></xsl:when>
            <xsl:when test="$typename='short'"><xsl:value-of select="'Integer'"/></xsl:when>
            <xsl:when test="$typename='nonPositiveInteger'"><xsl:value-of select="'Integer'"/></xsl:when>
            <xsl:when test="$typename='negativeInteger'"><xsl:value-of select="'Integer'"/></xsl:when>
            <xsl:when test="$typename='nonNegativeInteger'"><xsl:value-of select="'Integer'"/></xsl:when>
            <xsl:when test="$typename='unsigned'"><xsl:value-of select="'Integer'"/></xsl:when>
            <xsl:when test="$typename='unsignedLong'"><xsl:value-of select="'Integer'"/></xsl:when>
            <xsl:when test="$typename='unsignedInt'"><xsl:value-of select="'Integer'"/></xsl:when>
            <xsl:when test="$typename='unsignedShort'"><xsl:value-of select="'Integer'"/></xsl:when>
            <xsl:when test="$typename='unsignedByte'"><xsl:value-of select="'Integer'"/></xsl:when>
            <xsl:when test="$typename='positiveInteger'"><xsl:value-of select="'Integer'"/></xsl:when>
            <xsl:when test="$typename='base64Binary'"><xsl:value-of select="'PString'"/></xsl:when>
            <xsl:when test="$typename='string'"><xsl:value-of select="'PString'"/></xsl:when>
            <xsl:when test="$typename='xsdString'"><xsl:value-of select="'PString'"/></xsl:when>
            <xsl:when test="$typename='normalizedString'"><xsl:value-of select="'PString'"/></xsl:when>
            <xsl:when test="$typename='binary'"><xsl:value-of select="'PString'"/></xsl:when>
            <xsl:otherwise><xsl:value-of select="$typename"/></xsl:otherwise>
        </xsl:choose>
    </xsl:template>
    <xsl:template match="EsdlArray">
        <xsl:variable name="mytype"><xsl:value-of select="@type"/></xsl:variable>
        <xsl:variable name="primitive">
            <xsl:call-template name="outputCppPrimitive">
               <xsl:with-param name="typename">
                  <xsl:value-of select="@type"/>
               </xsl:with-param>
            </xsl:call-template>
        </xsl:variable>
            <xsl:text>    </xsl:text>
            <xsl:text>IArrayOf&lt;</xsl:text>
        <xsl:value-of select="$primitive"/><xsl:text>&gt; </xsl:text>m_<xsl:value-of select="@name"/>;<xsl:text>
</xsl:text>
    </xsl:template>

    <xsl:template match="EsdlElement">
        <xsl:variable name="primitive">
            <xsl:call-template name="outputCppPrimitive">
               <xsl:with-param name="typename">
            <xsl:choose>
                <xsl:when test="@type"><xsl:value-of select="@type"/></xsl:when>
                <xsl:when test="@complex_type"><xsl:value-of select="@complex_type"/></xsl:when>
            </xsl:choose>
               </xsl:with-param>
            </xsl:call-template>
        </xsl:variable>
        <xsl:text>    </xsl:text>
        <xsl:text>Owned&lt;</xsl:text>
    <xsl:value-of select="$primitive"/>
        <xsl:text>&gt;</xsl:text>
        <xsl:text> </xsl:text>
        <xsl:text>m_</xsl:text><xsl:value-of select="@name"/>
        <xsl:choose>
            <xsl:when test="@default">
        <xsl:text> = new </xsl:text><xsl:value-of select="$primitive"/><xsl:text>(</xsl:text>
            <xsl:choose>
                <xsl:when test="$primitive='PString'">"<xsl:value-of select="@default"/>"</xsl:when>
                <xsl:when test="$primitive='bool'">
            <xsl:choose>
                <xsl:when test="@default='true'"><xsl:value-of select="'true'"/></xsl:when>
                <xsl:when test="@default='1'"><xsl:value-of select="'true'"/></xsl:when>
                <xsl:otherwise><xsl:value-of select="'false'"/></xsl:otherwise>
                    </xsl:choose>
                    </xsl:when>
                <xsl:otherwise><xsl:value-of select="@default"/></xsl:otherwise>
                </xsl:choose>
        <xsl:text>)</xsl:text>
            </xsl:when>
        </xsl:choose>
        <xsl:text>;
</xsl:text>
    </xsl:template>

    <xsl:template match="EsdlEnum">
        <xsl:variable name="enum_type" select="@enum_type"/>
        <xsl:variable name="primitive">
            <xsl:call-template name="outputCppPrimitive">
               <xsl:with-param name="typename">
                     <xsl:value-of select="@enum_type"/>
               </xsl:with-param>
            </xsl:call-template>
        </xsl:variable>
        <xsl:text>    </xsl:text>
    <xsl:value-of select="$primitive"/>
        <xsl:text> </xsl:text>
        <xsl:text>m_</xsl:text><xsl:value-of select="@name"/>
            <xsl:choose>
            <xsl:when test="@default">
                <xsl:text> = EnumHandler</xsl:text><xsl:value-of select="$primitive"/>::fromString<xsl:text>(</xsl:text>"<xsl:value-of select="@default"/><xsl:text>")</xsl:text>
            </xsl:when>
            <xsl:otherwise>
                <xsl:text> = </xsl:text><xsl:value-of select="$primitive"/><xsl:text>::UNSET</xsl:text>
            </xsl:otherwise>
            </xsl:choose>
        <xsl:text>;
</xsl:text>
    </xsl:template>

    <xsl:template match="EsdlService">
class <xsl:value-of select="@name"/>ServiceBase : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;<xsl:text>
</xsl:text>
    <xsl:for-each select="EsdlMethod">
    <xsl:text>    virtual </xsl:text><xsl:value-of select="@response_type"/>*<xsl:text> </xsl:text><xsl:value-of select="@name"/>(EsdlContext* context, <xsl:value-of select="@request_type"/><xsl:text>* request){return nullptr;}
</xsl:text>
    </xsl:for-each>
    <xsl:for-each select="EsdlMethod">
    <xsl:text>    </xsl:text><xsl:text>virtual int </xsl:text><xsl:value-of select="@name"/><xsl:text>(const char* CtxStr, const char* ReqStr, StringBuffer&amp; RespStr);
</xsl:text>
    </xsl:for-each>
<xsl:text>};

</xsl:text>
<xsl:text>// Implemented in generated code
</xsl:text>
    <xsl:for-each select="EsdlMethod">
    <xsl:text>extern "C" int on</xsl:text><xsl:value-of select="../@name"/><xsl:value-of select="@name"/><xsl:text>(const char* CtxStr, const char* ReqStr, StringBuffer&amp; RespStr);
</xsl:text>
        </xsl:for-each>
    </xsl:template>

    <xsl:template match="EsdlEnumType">
      <xsl:if test="EsdlEnumItem">
enum class <xsl:value-of select="@name"/><xsl:text>
{
    UNSET = 0,
</xsl:text>
        <xsl:for-each select="EsdlEnumItem">
          <xsl:text>    </xsl:text><xsl:value-of select="@name"/>
           <xsl:choose>
             <xsl:when test="position() != last()">
              <xsl:text>,
</xsl:text>
             </xsl:when>
             <xsl:otherwise>
             </xsl:otherwise>
           </xsl:choose>
        </xsl:for-each>
        <xsl:text>
};
</xsl:text>

class EnumHandler<xsl:value-of select="@name"/>
{
public:
    static <xsl:value-of select="@name"/> fromString(const char* str)
    {
        <xsl:for-each select="EsdlEnumItem">
        <xsl:choose>
            <xsl:when test="position() = 1">
                <xsl:text>if (strcmp(str, "</xsl:text><xsl:value-of select="@enum"/><xsl:text>") == 0)
</xsl:text>
            </xsl:when>
            <xsl:when test="position() = last()">
                <xsl:text>        else
</xsl:text>
            </xsl:when>
            <xsl:otherwise>
                <xsl:text>        else if (strcmp(str, "</xsl:text><xsl:value-of select="@enum"/><xsl:text>") == 0)
</xsl:text>
            </xsl:otherwise>
        </xsl:choose>
        <xsl:text>            return </xsl:text><xsl:value-of select="../@name"/>::<xsl:value-of select="@name"/><xsl:text>;
</xsl:text>
        </xsl:for-each>
    }

    static const char* toString(<xsl:value-of select="@name"/> val)
    {
        <xsl:for-each select="EsdlEnumItem">
        <xsl:choose>
            <xsl:when test="position() = 1">
                <xsl:text>if (val == </xsl:text><xsl:value-of select="../@name"/>::<xsl:value-of select="@name"/><xsl:text>)
</xsl:text>
            </xsl:when>
            <xsl:when test="position() = last()">
                <xsl:text>        else
</xsl:text>
            </xsl:when>
            <xsl:otherwise>
                <xsl:text>        else if (val == </xsl:text><xsl:value-of select="../@name"/>::<xsl:value-of select="@name"/><xsl:text>)
</xsl:text>
            </xsl:otherwise>
        </xsl:choose>
        <xsl:text>            return "</xsl:text><xsl:value-of select="@enum"/><xsl:text>";
</xsl:text>
        </xsl:for-each>
    }
};
</xsl:if>
    </xsl:template>
</xsl:stylesheet>
