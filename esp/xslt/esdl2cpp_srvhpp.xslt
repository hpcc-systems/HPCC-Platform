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
<xsl:text>#ifndef </xsl:text><xsl:value-of select="$upperService"/>SERVICE_HPP__
<xsl:text>#define </xsl:text><xsl:value-of select="$upperService"/>SERVICE_HPP__

#include "jlib.hpp"
#include "<xsl:value-of select="$servicename"/>ServiceBase.hpp"

using namespace std;

<xsl:apply-templates select="EsdlService"/>
#endif
</xsl:template>

<xsl:template match="EsdlService">
    <xsl:variable name="servicename" select="@name"/>
class <xsl:value-of select="$servicename"/>Service : public <xsl:value-of select="$servicename"/>ServiceBase
{
public:
    <xsl:for-each select="EsdlMethod">
    <xsl:text>    virtual </xsl:text><xsl:value-of select="@response_type"/>*<xsl:text> </xsl:text><xsl:value-of select="@name"/>(EsdlContext* context, <xsl:value-of select="@request_type"/><xsl:text>* request);
</xsl:text>
    </xsl:for-each>
    <xsl:for-each select="EsdlMethod">
    <xsl:text>    </xsl:text><xsl:text>virtual int </xsl:text><xsl:value-of select="@name"/>(const char* CtxStr, const char* ReqStr, StringBuffer&amp; RespStr)
    {
        return <xsl:value-of select="$servicename"/>ServiceBase::<xsl:value-of select="@name"/>(CtxStr, ReqStr, RespStr);
    }<xsl:text>
</xsl:text>
    </xsl:for-each>
<xsl:text>};
</xsl:text>
</xsl:template>

</xsl:stylesheet>
