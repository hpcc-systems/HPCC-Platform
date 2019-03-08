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
<xsl:text>#include "</xsl:text><xsl:value-of select="$servicename"/>Service.hpp"
#include "jliball.hpp"
#include "jlog.hpp"
#include "jptree.hpp"

<xsl:apply-templates select="EsdlService"/>

<xsl:text>extern "C" </xsl:text><xsl:value-of select="$servicename"/>ServiceBase* create<xsl:value-of select="$servicename"/>ServiceObj()
{
    return new <xsl:value-of select="$servicename"/>Service();
}<xsl:text>
</xsl:text>
</xsl:template>

<xsl:template match="EsdlService">
    <xsl:variable name="servicename" select="@name"/>
    <xsl:for-each select="EsdlMethod">
    <xsl:value-of select="@response_type"/>* <xsl:value-of select="$servicename"/>Service::<xsl:value-of select="@name"/>(EsdlContext* context, <xsl:value-of select="@request_type"/>* request)
{
    Owned&lt;<xsl:value-of select="@response_type"/>&gt; resp = new <xsl:value-of select="@response_type"/>();
    //Fill in logic
    return resp.getClear();
}<xsl:text>

</xsl:text>
    </xsl:for-each>
</xsl:template>

</xsl:stylesheet>
