<?xml version="1.0" encoding="UTF-8"?>
<!--
##############################################################################
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
##############################################################################
-->

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
<xsl:output method="xml" version="1.0" encoding="UTF-8" indent="yes"/>
    
    
<xsl:template match="/">
    <EspNavigationData viewType="tree">
        <Menu name="m1">
            <MenuItem action="/WsDeploy/NavMenuEvent" name="Deploy" tooltip="Deploy"/>
            <MenuItem action="/WsDeploy/NavMenuEvent" name="start" tooltip="Start"/>
            <MenuItem action="/WsDeploy/NavMenuEvent" name="stop" tooltip="Stop"/>
        </Menu>
        <Menu name="m2">
            <MenuItem action="/WsDeploy/NavMenuEvent" name="Deploy" tooltip="Deploy"/>
        </Menu>
        <Menu name="m3">
            <MenuItem action="/WsDeploy/NavMenuEvent" name="Dependencies" tooltip="Dependencies"/>
        </Menu>
        <xsl:apply-templates select="Environment"/>
    </EspNavigationData>
</xsl:template>
    
    
<xsl:template match="/Environment">
    <Folder name="Environment">
        <xsl:apply-templates select="Software"/>
        <xsl:apply-templates select="Hardware"/>
        <!--xsl:apply-templates select="Programs"/>
        <xsl:apply-templates select="Data"/-->
    </Folder>
</xsl:template>
    
    
<xsl:template match="/Environment/Software">
    <Folder name="Software">
        <xsl:attribute name="menu">m1</xsl:attribute>
        <xsl:for-each select="*">
            <xsl:sort select="name()"/>
            <xsl:variable name="nodeName" select="name()"/>
            <xsl:variable name="precedingCompsSameName" select="preceding-sibling::*[name()=$nodeName]"/>
            <xsl:variable name="followingCompsSameName" select="following-sibling::*[name()=$nodeName]"/>
            <xsl:choose>
                <xsl:when test="not($precedingCompsSameName) and $followingCompsSameName">
                    <Folder params="comp={$nodeName}">
                        <xsl:attribute name="name">
                            <xsl:call-template name="GetDisplayFolderName">
                                <xsl:with-param name="compName" select="$nodeName"/>
                                <xsl:with-param name="plural" select="1"/>
                            </xsl:call-template>
                        </xsl:attribute>
                        <xsl:apply-templates select="." mode="isDeployable"/>
                        <xsl:apply-templates select="."/>
                        <xsl:apply-templates select="$followingCompsSameName"/>
                    </Folder>
                </xsl:when>
                <xsl:when test="not($precedingCompsSameName)">
                    <xsl:apply-templates select=".">
                        <xsl:with-param name="displayCompType" select="1"/>
                    </xsl:apply-templates>
                </xsl:when>
            </xsl:choose>
        </xsl:for-each>
    </Folder>
</xsl:template>
    
    
<xsl:template match="Software/*">
    <xsl:param name="displayCompType" select="0"/>
    <xsl:variable name="nodeName" select="name()"/>
    <Folder params="comp={$nodeName}&amp;name={@name}">
        <xsl:attribute name="name">
            <xsl:if test="$displayCompType">
                <xsl:call-template name="GetDisplayFolderName">
                    <xsl:with-param name="compName" select="$nodeName"/>
                </xsl:call-template>
                <xsl:text> - </xsl:text>
            </xsl:if>
            <xsl:value-of select="@name"/>
        </xsl:attribute>
        <xsl:apply-templates select="." mode="isDeployable"/>
        <xsl:apply-templates select="Instance"/>
    </Folder>
</xsl:template>

<xsl:template match="Software/*" mode="isDeployable">
    <xsl:variable name="buildNode" select="/Environment/Programs/Build[@name=current()/@build]"/>
    <xsl:variable name="buildSetNode" select="$buildNode/BuildSet[@name=current()/@buildSet]"/>
    <xsl:choose>
        <xsl:when test="not($buildNode)">
            <Folder name="Error: Build {@build} is not defined!"/>
        </xsl:when>
        <xsl:when test="not($buildSetNode)">
            <Folder name="Error: Build set {@buildSet} is not defined in build {@build}!"/>
        </xsl:when>
        <xsl:when test="string($buildSetNode/@deployable) != 'no' and string($buildSetNode/@deployable) != 'false'">
            <xsl:attribute name="menu">
                <xsl:choose>
                    <xsl:when test="name()!='DaliServerProcess'">m1</xsl:when>
                    <xsl:otherwise>m2</xsl:otherwise>
                </xsl:choose>
            </xsl:attribute>
        </xsl:when>
    </xsl:choose>
</xsl:template>

<xsl:template match="Software/*/Instance">
    <xsl:variable name="compNode" select=".."/>
    <Folder params="comp={name($compNode)}&amp;name={$compNode/@name}&amp;instType={name()}&amp;inst={@name}&amp;computer={@computer}">
        <xsl:attribute name="name">
            <xsl:value-of select="@name"/>
            <xsl:text> - </xsl:text>
            <xsl:value-of select="@computer"/>
        </xsl:attribute>
        <xsl:apply-templates select=".." mode="isDeployable"/>
    </Folder>
</xsl:template>

<xsl:template match="/Environment/Hardware">
    <Folder name="Hardware">
        <Folder name="Computers">
            <xsl:for-each select="Computer">
                <Link name="{@name}"/>
            </xsl:for-each>
        </Folder>
    </Folder>
</xsl:template>

<xsl:template name="GetDisplayFolderName">
    <xsl:param name="compName"/>
    <xsl:param name="plural" select="0"/>
    
    <xsl:variable name="displayName">
        <xsl:choose>
            <xsl:when test="$compName='DaliServerProcess'">Dali Server</xsl:when>
            <xsl:when test="$compName='EclServerProcess'">ECL Server</xsl:when>
            <xsl:when test="$compName='EspProcess'">ESP Server</xsl:when>
            <xsl:otherwise>
                <xsl:value-of select="$compName"/>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:variable>
    
    <xsl:value-of select="$displayName"/>
    <xsl:if test="$plural">
        <xsl:variable name="len" select="string-length($displayName)"/>
        <xsl:choose>
            <xsl:when test="$len &gt; 1 and substring($displayName, number($len)-1, 2)='ss'">es</xsl:when>
            <xsl:otherwise>s</xsl:otherwise>
        </xsl:choose>
    </xsl:if>
</xsl:template>


</xsl:stylesheet>


<!--EspNavigationData viewType="tree">
 <Folder name="Attribute Servers" tooltip="Attribute Servers">
  <DynamicFolder name="local" params="type=repository&amp;subtype=as&amp;name=local&amp;netAddress=http://2wd66:8145" tooltip="local"/>
 </Folder>
 <Folder name="Roxie Clusters" tooltip="Roxie Clusters">
  <DynamicFolder name="linux_dev" params="type=repository&amp;subtype=rc&amp;name=linux_dev&amp;netAddress=10.150.192.1:9876" tooltip="linux_dev"/>
 </Folder>
 <Menu name="rcmenu1">
  <MenuItem action="/ws_roxieconfig/NavMenuEvent" name="DeployMultiple" tooltip="Deploy"/>
 </Menu>
 <Menu name="rcmenu2">
  <MenuItem action="/ws_roxieconfig/NavMenuEvent" name="DeployMultiple" tooltip="Deploy"/>
  <MenuItem action="/ws_roxieconfig/NavMenuEvent" name="GetVersion" tooltip="Get Version"/>
 </Menu>
</EspNavigationData-->
