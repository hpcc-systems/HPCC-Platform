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
package <xsl:value-of select="EsdlService/@name"/>;
import java.util.*;
import java.math.*;

class EsdlContext
{
    String username;
    Integer clientMajorVersion;
    Integer clientMinorVersion;
}

        <xsl:apply-templates select="EsdlEnumType"/>
        <xsl:apply-templates select="EsdlStruct"/>
        <xsl:apply-templates select="EsdlRequest"/>
        <xsl:apply-templates select="EsdlResponse"/>
        <xsl:apply-templates select="EsdlService"/>

    </xsl:template>

    <xsl:template match="EsdlStruct|EsdlRequest|EsdlResponse">
class <xsl:value-of select="@name"/><xsl:if test="@base_type"> extends <xsl:value-of select="@base_type"/></xsl:if>
{
<xsl:apply-templates select="EsdlElement|EsdlArray|EsdlEnum"/>}
    </xsl:template>

    <xsl:template name="outputJavaPrimitive">
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
    <xsl:template match="EsdlArray">
        <xsl:variable name="primitive">
	        <xsl:call-template name="outputJavaPrimitive">
	           <xsl:with-param name="typename">
			<xsl:choose>
			    <xsl:when test="@type"><xsl:value-of select="@type"/></xsl:when>
			    <xsl:when test="@complex_type"><xsl:value-of select="@complex_type"/></xsl:when>
			</xsl:choose>
	           </xsl:with-param>
	        </xsl:call-template>
        </xsl:variable>
            <xsl:text>    public </xsl:text>ArrayList&lt;<xsl:value-of select="$primitive"/>&gt;<xsl:text> </xsl:text><xsl:value-of select="@name"/>=new ArrayList&lt;<xsl:value-of select="$primitive"/>&gt;();<xsl:text>
</xsl:text>
    </xsl:template>

    <xsl:template match="EsdlElement">
        <xsl:variable name="primitive">
	        <xsl:call-template name="outputJavaPrimitive">
	           <xsl:with-param name="typename">
			<xsl:choose>
			    <xsl:when test="@type"><xsl:value-of select="@type"/></xsl:when>
			    <xsl:when test="@complex_type"><xsl:value-of select="@complex_type"/></xsl:when>
			</xsl:choose>
	           </xsl:with-param>
	        </xsl:call-template>
        </xsl:variable>
        <xsl:variable name="useQuotes">
          <xsl:choose>
            <xsl:when test="$primitive='String'"><xsl:value-of select="true()"/></xsl:when>
            <xsl:otherwise><xsl:value-of select="false()"/></xsl:otherwise>
          </xsl:choose>
        </xsl:variable>
        <xsl:text>    public </xsl:text>
	<xsl:value-of select="$primitive"/>
        <xsl:text> </xsl:text>
        <xsl:value-of select="@name"/>
        <xsl:choose>
            <xsl:when test="@type='binary'"><xsl:value-of select="'[]'"/></xsl:when>
            <xsl:when test="@default">
		<xsl:text> = new </xsl:text><xsl:value-of select="$primitive"/><xsl:text>(</xsl:text>
		<xsl:choose>
	            <xsl:when test="$useQuotes">"<xsl:value-of select="@default"/>"</xsl:when>
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

    <xsl:template match="EsdlEnum">
        <xsl:variable name="enum_type" select="@enum_type"/>
        <xsl:variable name="primitive">
	        <xsl:call-template name="outputJavaPrimitive">
	           <xsl:with-param name="typename">
                     <xsl:value-of select="@enum_type"/>
	           </xsl:with-param>
	        </xsl:call-template>
        </xsl:variable>
        <xsl:text>    public </xsl:text>
	<xsl:value-of select="$primitive"/>
        <xsl:text> </xsl:text>
        <xsl:value-of select="@name"/>
            <xsl:if test="@default">
                <xsl:text> = </xsl:text><xsl:value-of select="$primitive"/>.fromString<xsl:text>(</xsl:text>"<xsl:value-of select="@default"/>")
            </xsl:if>
        <xsl:text>;
</xsl:text>
    </xsl:template>

    <xsl:template match="EsdlService">
public class <xsl:value-of select="@name"/>ServiceBase
{
        <xsl:for-each select="EsdlMethod">
    public <xsl:value-of select="@response_type"/><xsl:text> </xsl:text><xsl:value-of select="@name"/>(EsdlContext context, <xsl:value-of select="@request_type"/> request){return null;}
        </xsl:for-each>
}
    </xsl:template>

    <xsl:template match="EsdlEnumType">
      <xsl:if test="EsdlEnumItem">
    enum <xsl:value-of select="@name"/><xsl:text> {
</xsl:text>
        <xsl:for-each select="EsdlEnumItem">
          <xsl:text>        </xsl:text><xsl:value-of select="@name"/><xsl:text> </xsl:text>("<xsl:value-of select="@enum"/><xsl:text>")</xsl:text>
           <xsl:choose>
             <xsl:when test="position() != last()">
              <xsl:text>,
</xsl:text>
             </xsl:when>
             <xsl:otherwise>
              <xsl:text>;
</xsl:text>
             </xsl:otherwise>
           </xsl:choose>
        </xsl:for-each>
            private final String name;       

            private<xsl:text> </xsl:text><xsl:value-of select="@name"/>(String s) {
                name = s;
            }

            public boolean equalsName(String otherName) {
                return (otherName == null) ? false : name.equals(otherName);
            }

            public String toString() {
               return this.name;
            }
            public static<xsl:text> </xsl:text><xsl:value-of select="@name"/> fromString(String text)
            {
              if (text != null)
              {
                  for (<xsl:value-of select="@name"/> val :<xsl:text> </xsl:text><xsl:value-of select="@name"/>.values()) {
                    if (text.equalsIgnoreCase(val.toString())) {
                      return val;
                    }
                  }
               }
             return null;
            }        
        }
      </xsl:if>
    </xsl:template>
</xsl:stylesheet>

