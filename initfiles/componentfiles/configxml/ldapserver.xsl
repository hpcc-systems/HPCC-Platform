<?xml version="1.0" encoding="UTF-8"?>
<!--
################################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
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
################################################################################
-->

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xml:space="default">
<xsl:param name="process" select="'unknown'"/>

<xsl:strip-space elements="*"/>
<xsl:output method="text" indent="no" omit-xml-declaration="yes"/>

<xsl:template match="text()"/>
    <xsl:template match="/">
        <xsl:apply-templates select="Environment/Software/LDAPServerProcess[@name=$process]"/>
    </xsl:template>

   <xsl:template match="LDAPServerProcess">
 host <xsl:value-of select="Instance[1]/@netAddress"/>
 #base dc=internal,dc=sds
 filesBasedn="<xsl:value-of select="@filesBasedn"/>"
 resourcesBasedn="<xsl:value-of select="@resourcesBasedn"/>"
 systemBasedn="<xsl:value-of select="@systemBasedn"/>"
 usersBasedn="<xsl:value-of select="@usersBasedn"/>"
 workunitsBasedn="<xsl:value-of select="@workunitsBasedn"/>"
 sharedCache="<xsl:value-of select="@sharedCache"/>"
   </xsl:template>
</xsl:stylesheet>
