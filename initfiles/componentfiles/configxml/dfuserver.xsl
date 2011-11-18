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
    <xsl:output method="xml" indent="yes" omit-xml-declaration="no" encoding="UTF-8"/>
    <xsl:template match="text()"/>
    <xsl:param name="process" select="'sasha'"/>
    <xsl:param name="isLinuxInstance" select="0"/>
    
    <xsl:variable name="oldPathChars">
        <xsl:choose>
            <xsl:when test="$isLinuxInstance = 1">'\\'</xsl:when>
            <xsl:otherwise>'/$'</xsl:otherwise>
        </xsl:choose>   
    </xsl:variable>
    
    <xsl:variable name="newPathChars">
        <xsl:choose>
            <xsl:when test="$isLinuxInstance = 1">'/'</xsl:when>
            <xsl:otherwise>'\:'</xsl:otherwise>
        </xsl:choose>   
    </xsl:variable>
    
    <xsl:template match="/">
        <xsl:apply-templates select="/Environment/Software/DfuServerProcess[@name=$process]"/>
    </xsl:template>
    
    <xsl:template match="DfuServerProcess">
        <DFUSERVER>
            <xsl:attribute name="name">
               <xsl:value-of select="@name"/>
            </xsl:attribute> 

            <xsl:attribute name="daliservers"> <xsl:call-template name="getDaliServers">
                    <xsl:with-param name="daliServer" select="@daliServers"/>
                </xsl:call-template>            
            </xsl:attribute>
            
            <xsl:attribute name="enableSNMP">
               <xsl:call-template name="outputBool">
                  <xsl:with-param name="val" select="@enableSNMP"/>
               </xsl:call-template>
            </xsl:attribute> 
            
            <xsl:attribute name="enableSysLog">
               <xsl:call-template name="outputBool">
                  <xsl:with-param name="val" select="@enableSysLog"/>
               </xsl:call-template>
            </xsl:attribute> 
            
            <xsl:attribute name="queue">
               <xsl:value-of select="@queue"/>
            </xsl:attribute> 
            
            <xsl:attribute name="monitorqueue">
               <xsl:value-of select="@monitorqueue"/>
            </xsl:attribute> 
            
            <xsl:attribute name="monitorinterval">
               <xsl:value-of select="@monitorinterval"/>
            </xsl:attribute> 
            
            <xsl:attribute name="transferBufferSize">
               <xsl:value-of select="@transferBufferSize"/>
            </xsl:attribute> 
            
            <xsl:copy-of select="/Environment/Software/Directories"/>  

            <SSH>
              <xsl:for-each select="SSH/@*">
                <xsl:if test="string(.) != ''">
                  <xsl:copy-of select="."/>
                </xsl:if>
              </xsl:for-each>
              </SSH>
            
        </DFUSERVER>
    </xsl:template>
    
    
    <xsl:template name="outputBool">
        <xsl:param name="val"/>
        <xsl:param name="default" select="0"/>
        <xsl:choose>
            <xsl:when test="$val='true'">1</xsl:when>
            <xsl:when test="$val='false'">0</xsl:when>
            <xsl:otherwise><xsl:value-of select='$default'/></xsl:otherwise>
        </xsl:choose>
    </xsl:template>
    
    
    <xsl:template name="getDaliServers">
        <xsl:param name="daliServer"/>
        <xsl:for-each select="/Environment/Software/DaliServerProcess[@name=$daliServer]/Instance">
            <xsl:call-template name="getNetAddress">
                <xsl:with-param name="computer" select="@computer"/>
            </xsl:call-template>
            <xsl:if test="string(@port) != ''">:<xsl:value-of select="@port"/>
            </xsl:if>
            <xsl:if test="position() != last()">, </xsl:if>
        </xsl:for-each>
    </xsl:template>
    
    
    <xsl:template name="getNetAddress">
        <xsl:param name="computer"/>
        <xsl:value-of select="/Environment/Hardware/Computer[@name=$computer]/@netAddress"/>
    </xsl:template>

</xsl:stylesheet>
