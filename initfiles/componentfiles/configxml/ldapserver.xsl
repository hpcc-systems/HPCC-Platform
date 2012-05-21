<?xml version="1.0" encoding="UTF-8"?>
<!--
################################################################################
#    Copyright (C) 2011 HPCC Systems.
#
#    All rights reserved. This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
 sudoersBasedn="<xsl:value-of select="@sudoersBasedn"/>"
 systemBasedn="<xsl:value-of select="@systemBasedn"/>"
 usersBasedn="<xsl:value-of select="@usersBasedn"/>"
 workunitsBasedn="<xsl:value-of select="@workunitsBasedn"/>"
   </xsl:template>
</xsl:stylesheet>
