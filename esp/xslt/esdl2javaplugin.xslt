<?xml version="1.0" encoding="UTF-8"?>
<!--
##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
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
import java.util.*;
        <!--xsl:apply-templates select="EsdlEnumType" /-->
        <xsl:apply-templates select="EsdlStruct"/>
        <xsl:apply-templates select="EsdlRequest"/>
        <xsl:apply-templates select="EsdlResponse"/>
        <xsl:apply-templates select="EsdlService"/>

    </xsl:template>

    <xsl:template match="EsdlStruct|EsdlRequest|EsdlResponse">
public class <xsl:value-of select="@name"/><xsl:if test="@base_type"> extends <xsl:value-of select="@base_type"/></xsl:if>
{
<xsl:apply-templates select="EsdlElement|EsdlArray|EsdlEnum"/>
}
    </xsl:template>

    <xsl:template name="ouputJavaPrimitive">
        <xsl:param name="typename"/>
        <xsl:choose>
            <xsl:when test="$typename='bool'"><xsl:value-of select="'Boolean'"/></xsl:when>
            <xsl:when test="$typename='boolean'"><xsl:value-of select="'Boolean'"/></xsl:when>
            <xsl:when test="$typename='decimal'"><xsl:value-of select="'BigDecimal'"/></xsl:when>
            <xsl:when test="$typename='float'"><xsl:value-of select="'Float'"/></xsl:when>
            <xsl:when test="$typename='double'"><xsl:value-of select="'Double'"/></xsl:when>
            <xsl:when test="$typename='integer'"><xsl:value-of select="'Integer'"/></xsl:when>
            <xsl:when test="$typename='int64'"><xsl:value-of select="'BigInteger'"/></xsl:when>
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
            <xsl:when test="$typename='unsignedByte'"><xsl:value-of select="'Byte'"/></xsl:when>
            <xsl:when test="$typename='positiveInteger'"><xsl:value-of select="'Integer'"/></xsl:when>
            <xsl:when test="$typename='base64Binary'"><xsl:value-of select="'String'"/></xsl:when>
            <xsl:when test="$typename='string'"><xsl:value-of select="'String'"/></xsl:when>
            <xsl:when test="$typename='xsdString'"><xsl:value-of select="'String'"/></xsl:when>
            <xsl:when test="$typename='normalizedString'"><xsl:value-of select="'String'"/></xsl:when>
            <xsl:when test="$typename='binary'"><xsl:value-of select="'Byte'"/></xsl:when>
            <xsl:otherwise><xsl:value-of select="$typename"/></xsl:otherwise>
        </xsl:choose>
    </xsl:template>

    <xsl:template match="EsdlElement|EsdlArray|EsdlEnum">
        <xsl:variable name="enum_type" select="@enum_type"/>
        <xsl:text>    public </xsl:text>

        <xsl:variable name="primitive">
	        <xsl:call-template name="ouputJavaPrimitive">
	           <xsl:with-param name="typename">
			<xsl:choose>
			    <xsl:when test="@enum_type"><xsl:value-of select="esxdl/EsdlEnumType[@name=$enum_type]/@base_type"/></xsl:when>
			    <xsl:when test="@type"><xsl:value-of select="@type"/></xsl:when>
			    <xsl:when test="@complex_type"><xsl:value-of select="@complex_type"/></xsl:when>
			</xsl:choose>
	           </xsl:with-param>
	        </xsl:call-template>
        </xsl:variable>
	<xsl:value-of select="$primitive"/>
        <xsl:text> </xsl:text>
        <xsl:value-of select="@name"/>
        <xsl:choose>
            <xsl:when test="@type='binary'"><xsl:value-of select="'[]'"/></xsl:when>
            <xsl:when test="local-name()='EsdlArray'"><xsl:value-of select="'[]'"/></xsl:when>
            <xsl:when test="@default">
		<xsl:text> = new </xsl:text><xsl:value-of select="$primitive"/><xsl:text>(</xsl:text>
		<xsl:choose>
	            <xsl:when test="$primitive='String'">"<xsl:value-of select="@default"/>"</xsl:when>
	            <xsl:when test="$primitive='Boolean'">
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

    <xsl:template match="EsdlService">
public class <xsl:value-of select="@name"/>
{
        <xsl:for-each select="EsdlMethod">
    public void <xsl:value-of select="@name"/>(<xsl:value-of select="@request_type"/> request, <xsl:value-of select="@response_type"/> response);
        </xsl:for-each>
}
    </xsl:template>
    <!--xsl:template match="EsdlEnumItem">
        <xsd:enumeration>
            <xsl:attribute name="value"><xsl:value-of select="@enum"/></xsl:attribute>
        </xsd:enumeration>
    </xsl:template>

    <xsl:template match="EsdlEnumType">
        <xsd:simpleType>
            <xsl:attribute name="name"><xsl:value-of select="@name"/></xsl:attribute>
            <xsl:if test="(EsdlEnumItem[@desc]) and EsdlEnumItem/@desc!=''">
                <xsl:if test="not($no_annot_Param) or boolean($all_annot_Param)">
                    <xsd:annotation>
                        <xsd:appinfo>
                            <xsl:apply-templates select="EsdlEnumItem" mode="annotation" />
                        </xsd:appinfo>
                    </xsd:annotation>
                </xsl:if>
            </xsl:if>
            <xsd:restriction>
                <xsl:attribute name="base">xsd:<xsl:value-of select="@base_type"/></xsl:attribute>
                <xsl:apply-templates select="EsdlEnumItem"/>
            </xsd:restriction>
        </xsd:simpleType>
    </xsl:template>

    <xsl:template match="EsdlEnum">
        <xsd:element>
            <xsl:choose>
                <xsl:when test="@required"></xsl:when>
                <xsl:otherwise>
                    <xsl:attribute name="minOccurs">0</xsl:attribute>
                </xsl:otherwise>
            </xsl:choose>
            <xsl:attribute name="name">
                <xsl:choose>
                    <xsl:when test="@xml_tag"><xsl:value-of select="@xml_tag" /></xsl:when>
                    <xsl:otherwise><xsl:value-of select="@name" /></xsl:otherwise>
                </xsl:choose>
            </xsl:attribute>
            <xsl:attribute name="type">
                <xsl:choose>
                    <xsl:when test="@xsd_type"><xsl:value-of select="@xsd_type" /></xsl:when>
                    <xsl:when test="@enum_type">tns:<xsl:value-of select="@enum_type" /></xsl:when>
                </xsl:choose>
            </xsl:attribute>
            <xsl:if test="@default or (@default='')">
                <xsl:attribute name="default"><xsl:value-of select="@default"/></xsl:attribute>
            </xsl:if>
        </xsd:element>
    </xsl:template-->
</xsl:stylesheet>
