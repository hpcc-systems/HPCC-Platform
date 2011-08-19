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
    <xsl:output method="xml" indent="yes" omit-xml-declaration="no"/>
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
        <xsl:apply-templates select="/Environment/Software/SashaServerProcess[@name=$process]"/>
    </xsl:template>
    
    <xsl:template match="SashaServerProcess">
        <SASHA>
                        <xsl:attribute name="name">
                           <xsl:value-of select="@name"/>
                        </xsl:attribute> 
            <xsl:attribute name="DALISERVERS"> <xsl:call-template name="getDaliServers">
                    <xsl:with-param name="daliServer" select="@daliServers"/>
                </xsl:call-template>            
            </xsl:attribute>
            
            <xsl:attribute name="logDir">
               <xsl:value-of select="translate(@logDir, $oldPathChars, $newPathChars)"/>
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
            
            <xsl:attribute name="snmpSendWarnings">
               <xsl:call-template name="outputBool">
                  <xsl:with-param name="val" select="@snmpSendWarnings"/>
               </xsl:call-template>
            </xsl:attribute> 

            <xsl:attribute name="autoRestartInterval">
               <xsl:value-of select="@autoRestartInterval"/>
            </xsl:attribute>
            
            <xsl:copy-of select="/Environment/Software/Directories"/>  

            <xsl:element name="LDS">
                <xsl:attribute name="rootdir">
                   <xsl:value-of select="@LDSroot"/>
                </xsl:attribute>
            </xsl:element>
            
            <xsl:element name="Archiver">
                <xsl:element name="WorkUnits"> 
                    <xsl:attribute name="limit">
                       <xsl:value-of select="@WUlimit"/>
                    </xsl:attribute>
                    <xsl:attribute name="cutoff">
                       <xsl:value-of select="@WUcutoff"/>
                    </xsl:attribute>
                    <xsl:attribute name="backup">
                       <xsl:value-of select="@WUbackup"/>
                    </xsl:attribute>
                    <xsl:attribute name="interval">
                       <xsl:value-of select="@WUinterval"/>
                    </xsl:attribute>
                    <xsl:attribute name="at">
                       <xsl:value-of select="@WUat"/>
                    </xsl:attribute>
                    <xsl:attribute name="duration">
                       <xsl:value-of select="@WUduration"/>
                    </xsl:attribute>
                    <xsl:attribute name="throttle">
                       <xsl:value-of select="@WUthrottle"/>
                    </xsl:attribute>
                    <xsl:attribute name="retryinterval">
                       <xsl:value-of select="@WUretryinterval"/>
                    </xsl:attribute>
                    <xsl:attribute name="keepResultFiles">
                       <xsl:value-of select="@keepResultFiles"/>
                    </xsl:attribute>
                </xsl:element>
                
                <xsl:element name="DFUrecovery">
                    <xsl:attribute name="limit">
                       <xsl:value-of select="@DFUrecoveryLimit"/>
                    </xsl:attribute>
                    <xsl:attribute name="cutoff">
                       <xsl:value-of select="@DFUrecoveryCutoff"/>
                    </xsl:attribute>
                    <xsl:attribute name="interval">
                       <xsl:value-of select="@DFUrecoveryInterval"/>
                    </xsl:attribute>
                    <xsl:attribute name="at">
                       <xsl:value-of select="@DFUrecoveryAt"/>
                    </xsl:attribute>
                </xsl:element>
                
                <xsl:element name="DFUworkunits">
                    <xsl:attribute name="limit">
                       <xsl:value-of select="@DFUWUlimit"/>
                    </xsl:attribute>
                    <xsl:attribute name="cutoff">
                       <xsl:value-of select="@DFUWUcutoff"/>
                    </xsl:attribute>
                    <xsl:attribute name="interval">
                       <xsl:value-of select="@DFUWUinterval"/>
                    </xsl:attribute>
                    <xsl:attribute name="at">
                       <xsl:value-of select="@DFUWUat"/>
                    </xsl:attribute>
                    <xsl:attribute name="duration">
                       <xsl:value-of select="@DFUWUduration"/>
                    </xsl:attribute>
                    <xsl:attribute name="throttle">
                       <xsl:value-of select="@DFUWUthrottle"/>
                    </xsl:attribute>
                </xsl:element>
                
                <xsl:element name="CachedWorkUnits">
                    <xsl:attribute name="limit">
                       <xsl:value-of select="@cachedWUlimit"/>
                    </xsl:attribute>
                    <xsl:attribute name="interval">
                       <xsl:value-of select="@cachedWUinterval"/>
                    </xsl:attribute>
                    <xsl:attribute name="at">
                       <xsl:value-of select="@cachedWUat"/>
                    </xsl:attribute>
                </xsl:element>
                
            </xsl:element>
            
            <xsl:element name="Coalescer">
                <xsl:attribute name="interval">
                   <xsl:value-of select="@coalesceInterval"/>
                </xsl:attribute>
                <xsl:attribute name="at">
                   <xsl:value-of select="@coalesceAt"/>
                </xsl:attribute>
                <xsl:attribute name="minDeltaSize">
                   <xsl:value-of select="@minDeltaSize"/>
                </xsl:attribute>
                <xsl:attribute name="recoverFromIncErrors">
                   <xsl:value-of select="@recoverDeltaErrors"/>
                </xsl:attribute>
            </xsl:element>
            <xsl:element name="DfuXRef">
                <xsl:attribute name="interval">
                   <xsl:value-of select="@xrefInterval"/>
                </xsl:attribute>
                <xsl:attribute name="at">
                   <xsl:value-of select="@xrefAt"/>
                </xsl:attribute>
                <xsl:attribute name="clusterlist">
                   <xsl:value-of select="@xrefList"/>
                </xsl:attribute>
                <xsl:attribute name="cutoff">
                   <xsl:value-of select="@xrefCutoff"/>
                </xsl:attribute>
                <xsl:attribute name="eclwatchProvider">
                   <xsl:call-template name="outputBool">
                      <xsl:with-param name="val" select="@xrefEclWatchProvider"/>
                   </xsl:call-template>
                </xsl:attribute>
            </xsl:element>
            <xsl:element name="DfuExpiry">
                <xsl:attribute name="interval">
                   <xsl:value-of select="@ExpiryInterval"/>
                </xsl:attribute>
                <xsl:attribute name="at">
                   <xsl:value-of select="@ExpiryAt"/>
                </xsl:attribute>
            </xsl:element>
            <xsl:element name="ThorQMon">
                <xsl:attribute name="queues">
                   <xsl:value-of select="@thorQMonQueues"/>
                </xsl:attribute>
                <xsl:attribute name="interval">
                   <xsl:value-of select="@thorQMonInterval"/>
                </xsl:attribute>
                <xsl:attribute name="switchMinTime">
                   <xsl:value-of select="@thorQMonSwitchMinTime"/>
                </xsl:attribute>
            </xsl:element>
            <xsl:element name="DaFileSrvMonitor">
                <xsl:attribute name="interval">
                   <xsl:value-of select="@dafsmonInterval"/>
                </xsl:attribute>
                <xsl:attribute name="at">
                   <xsl:value-of select="@dafsmonAt"/>
                </xsl:attribute>
                <xsl:attribute name="clusterlist">
                   <xsl:value-of select="@dafsmonList"/>
                </xsl:attribute>
            </xsl:element>
            
        </SASHA>
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
