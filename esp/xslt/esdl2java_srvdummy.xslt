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
import <xsl:value-of select="EsdlService/@name"/>.*;

        <xsl:apply-templates select="EsdlService"/>

    </xsl:template>

    <xsl:template match="EsdlService">
public class <xsl:value-of select="@name"/>ServiceDummy extends <xsl:value-of select="@name"/>ServiceBase
{
        <xsl:for-each select="EsdlMethod">
    public <xsl:value-of select="@response_type"/><xsl:text> </xsl:text><xsl:value-of select="@name"/>(EsdlContext context, <xsl:value-of select="@request_type"/> request)
    {
        System.out.println("Method <xsl:value-of select="@name"/><xsl:text> </xsl:text>of Service <xsl:value-of select="../@name"/><xsl:text> </xsl:text>called!");
	<xsl:value-of select="@response_type"/><xsl:text> </xsl:text> response = new <xsl:value-of select="@response_type"/>();
        return response;
    }
        </xsl:for-each>
}
    </xsl:template>
</xsl:stylesheet>
