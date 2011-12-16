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

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xml:space="default"
 xmlns:seisint="http://seisint.com" exclude-result-prefixes="seisint">
    <xsl:output method="xml" indent="yes" omit-xml-declaration="no" encoding="UTF-8"/>
    <xsl:template match="text()"/>
    <xsl:param name="process" select="'unknown'"/>
    <xsl:param name="isLinuxInstance" select="0"/>
    <xsl:param name="outputFilePath"/>
    <xsl:param name="tempPath" select="'c:\temp\'"/>
    
    <xsl:variable name="oldPathSeparator">
        <xsl:choose>
            <xsl:when test="$isLinuxInstance = 1">\:</xsl:when>
            <xsl:otherwise>/$</xsl:otherwise>
        </xsl:choose>   
    </xsl:variable>
    
    <xsl:variable name="newPathSeparator">
        <xsl:choose>
            <xsl:when test="$isLinuxInstance = 1">/$</xsl:when>
            <xsl:otherwise>\:</xsl:otherwise>
        </xsl:choose>   
    </xsl:variable>

    <xsl:variable name="pathSep">
        <xsl:choose>
            <xsl:when test="$isLinuxInstance = 1">/</xsl:when>
            <xsl:otherwise>\</xsl:otherwise>
        </xsl:choose>   
    </xsl:variable>

   <xsl:variable name="roxieClusterNode" select="/Environment/Software/RoxieCluster[@name=$process]"/>
   
   <xsl:template match="/">
      <xsl:apply-templates select="$roxieClusterNode"/>
   </xsl:template>
   
    <xsl:template match="RoxieCluster">
        <xsl:element name="RoxieTopology">
        <xsl:copy-of select="@*[string(.)!='' and name()!='thorHost' and name()!='buildSet' and name()!='queryDir' and name()!='logDir' and name()!='siteCertificate' and name()!='pluginsPath']"/>     
            <xsl:if test="string(@siteCertificate) != ''">
                <xsl:if test="not(function-available('seisint:siteCertificate'))">
                    <xsl:message terminate="yes">This XSL transformation can only be run by the Seisint Deployment Tool!</xsl:message>               
                </xsl:if>
                <xsl:variable name="siteCertNode" select="/Environment/Software/SiteCertificate[@name=current()/@siteCertificate]"/>
                <xsl:if test="string($siteCertNode/@passphrase)=''">
                    <xsl:message terminate="yes">Site certificate component '<xsl:value-of select="@siteCertificate"/>' has missing passphrase!</xsl:message>
                </xsl:if>
                <xsl:copy-of select="$siteCertNode/@certificateFileName|$siteCertNode/@privateKeyFileName|$siteCertNode/@passphrase"/>
                <xsl:if test="string($siteCertNode/Instance[1]/@name) = ''">
                    <xsl:message terminate="yes">Site certificate component '<xsl:value-of select="@siteCertificate"/>' or its instance is not defined!</xsl:message>
                </xsl:if>
                <!--ask deployment tool to generate certificate, privateKey and CSR -->
                <!--format is: <processType>+<process name>+<instance name>+<output path>-->
                <xsl:variable name="parameter">
                    <xsl:text>SiteCertificate+</xsl:text>
                    <xsl:value-of select="@siteCertificate"/><xsl:text>+</xsl:text>
                    <xsl:value-of select="$siteCertNode/Instance[1]/@name"/><xsl:text>+</xsl:text>
                    <xsl:value-of select="$outputFilePath"/>
                </xsl:variable>
                <xsl:variable name="response" select="seisint:siteCertificate($parameter)"/>    
                <xsl:if test="starts-with($response, 'exception:')">
                    <xsl:message terminate="yes"><xsl:value-of select="substring-after($response, 'exception:')"/></xsl:message>
                </xsl:if>
            </xsl:if>
            <xsl:call-template name="validateChannels"/>
            <xsl:attribute name="daliServers">
                <xsl:call-template name="getDaliServers">
                    <xsl:with-param name="daliservers" select="@daliServers"/>
                </xsl:call-template>
            </xsl:attribute>
            <xsl:attribute name="pluginDirectory">
                <xsl:variable name="path" select="translate(@pluginsPath, '/$', '\:')"/>
                <xsl:variable name="path2">
                    <xsl:choose>
                        <xsl:when test="starts-with($path, '.\')"><!--skip .\ prefix -->
                            <xsl:value-of select="substring($path, 3)"/>                            
                        </xsl:when>
                        <xsl:otherwise>
                            <xsl:value-of select="$path"/>
                        </xsl:otherwise>
                    </xsl:choose>
                </xsl:variable>
                <xsl:variable name="len" select="string-length($path2)"/>
                <xsl:variable name="path3">
                    <xsl:choose>
                        <xsl:when test="$len > 0">
                            <xsl:value-of select="$path2"/>
                            <xsl:if test="not(substring($path2, number($len)-1, 1) = '\')">\</xsl:if>
                        </xsl:when>
                        <xsl:otherwise>plugins\</xsl:otherwise>
                    </xsl:choose>
                </xsl:variable>
                <xsl:value-of select="translate($path3, $oldPathSeparator, $newPathSeparator)"/>
            </xsl:attribute>
            <xsl:copy-of select="/Environment/Software/Directories"/>
            <xsl:for-each select="ACL">
                <xsl:copy>
                    <xsl:copy-of select="@name"/>
                    <xsl:for-each select="BaseList">
                        <Access base='{@name}'/>
                    </xsl:for-each>
                    <xsl:for-each select="Access">
                        <xsl:copy>
                            <xsl:if test="string(@mask)!='' and string(@ip)=''">
                                <xsl:message terminate="yes">Access rule for an ACL must specify an I.P. address if mask is specified!</xsl:message>
                            </xsl:if>
                            <xsl:attribute name="allow">
                                <xsl:choose>
                                    <xsl:when test="@allow='Yes'">1</xsl:when>
                                    <xsl:otherwise>0</xsl:otherwise>
                                </xsl:choose>
                            </xsl:attribute>
                            <xsl:copy-of select="@*[name()!='allow']"/>
                        </xsl:copy>
                    </xsl:for-each>
                </xsl:copy>
            </xsl:for-each>
            <xsl:for-each select="UserMetric">
                <xsl:copy>
                    <xsl:copy-of select="@name"/>
                    <xsl:copy-of select="@regex"/>
                </xsl:copy>
            </xsl:for-each>
            <xsl:for-each select="RoxieServerProcess">
                <xsl:variable name="computer" select="@computer"/>
                <xsl:element name="RoxieServerProcess">
                    <xsl:attribute name="netAddress"><xsl:value-of select="/Environment/Hardware/Computer[@name=$computer]/@netAddress"/></xsl:attribute>
                    <xsl:copy-of select="@port"/>
                <xsl:if test="string(@dataDirectory)=''">
                    <xsl:message terminate="yes">Data directory is not specified for Roxie server '<xsl:value-of select="@computer"/>'.</xsl:message>
                </xsl:if>
                <xsl:variable name="dataDir">
                            <xsl:call-template name="makeAbsolutePath">
                                <xsl:with-param name="path" select="@dataDirectory"/>
                            </xsl:call-template>                        
                </xsl:variable>
                    <xsl:attribute name="baseDataDirectory">
                        <xsl:value-of select="$dataDir"/>
                    </xsl:attribute>
                    <xsl:attribute name="dataDirectory">
                        <xsl:value-of select="concat($dataDir, $pathSep, $roxieClusterNode/@name)"/>
                    </xsl:attribute>
                    <xsl:copy-of select="@*[name()!='netAddress' and name()!='dataDirectory' and name()!='computer' and name()!='name' and name()!='port']"/>
                </xsl:element>
            </xsl:for-each>
            <xsl:for-each select="RoxieSlaveProcess">
                <xsl:sort select="@dataDirectory"/>
                <xsl:sort select="@channel" data-type="number"/>
                <xsl:element name="RoxieSlaveProcess">
                    <xsl:variable name="computer" select="@computer"/>
                    <xsl:attribute name="netAddress"><xsl:value-of select="/Environment/Hardware/Computer[@name=$computer]/@netAddress"/></xsl:attribute>
                    <xsl:attribute name="channel"><xsl:value-of select="@channel"/></xsl:attribute>
                <xsl:if test="string(@dataDirectory)=''">
                    <xsl:message terminate="yes">
                        <xsl:text>Data directory is not specified for Roxie slave '</xsl:text>
                        <xsl:value-of select="@computer"/>
                        <xsl:text>' for channel </xsl:text>
                        <xsl:value-of select="@channel"/>.</xsl:message>
                </xsl:if>                           

                                        <xsl:variable name="dataDir">
                            <xsl:call-template name="makeAbsolutePath">
                            <xsl:with-param name="path" select="@dataDirectory"/>
                        </xsl:call-template>                        
                                        </xsl:variable>
                    <xsl:attribute name="dataDirectory">
                        <xsl:value-of select="concat($dataDir, $pathSep, $roxieClusterNode/@name)"/>
                    </xsl:attribute>
                                  
                </xsl:element>
            </xsl:for-each>
            <xsl:for-each select="RoxieMonitorProcess">
                <xsl:variable name="computer" select="@computer"/>
                <xsl:element name="RoxieMonitorProcess">
                    <xsl:attribute name="netAddress"><xsl:value-of select="/Environment/Hardware/Computer[@name=$computer]/@netAddress"/></xsl:attribute>
                </xsl:element>
            </xsl:for-each>
        </xsl:element>
    </xsl:template>

    
    <xsl:template name="validateChannels">
        <xsl:variable name="numChannels" select="@numChannels"/>
        <xsl:variable name="numSets" select="count(RoxieSlaveProcess[@channel='1'])"/>
        
        <xsl:if test="$numChannels &lt; 1">
            <xsl:message terminate="yes">Number of channels must be at least 1.</xsl:message>
        </xsl:if>
        
        <xsl:if test="RoxieSlaveProcess[@channel &lt; '1' or @channel &gt; $numChannels]">
            <xsl:message terminate="yes">Roxie slaves must not have channel numbers outside the range 1 to <xsl:value-of select="$numChannels"/>.</xsl:message>
        </xsl:if>
        
        <!--make sure that there are exactly $numChannels sets for each channel-->
        <!--we don't have incremental looping in xslt so start the 'domino effect' with channel 1-->
        <xsl:if test="not(RoxieSlaveProcess[@channel='1'])">
            <xsl:message terminate="yes">No Roxie slaves defined for channel 1.</xsl:message>
        </xsl:if>
        <xsl:apply-templates select="RoxieSlaveProcess[@channel='1' and count(preceding-sibling::RoxieSlaveProcess[@channel=1])=0]" mode="validateChannels">
            <xsl:with-param name="channel" select="1"/>
            <xsl:with-param name="numChannels" select="$numChannels"/>
            <xsl:with-param name="numSets" select="$numSets"/>
        </xsl:apply-templates>
        
    </xsl:template>


    <xsl:template match="RoxieSlaveProcess" mode="validateChannels">
    <xsl:param name="channel"/>
    <xsl:param name="numChannels"/>
    <xsl:param name="numSets"/>
        <!--only process the first slave for each unique channel number to avoid duplicate validation-->
        <xsl:if test="not(preceding-sibling::RoxieSlaveProcess[@channel=$channel])">
            <xsl:variable name="setsize" select="count(../RoxieSlaveProcess[@channel=$channel])"/>
            <!--note that setsize cannot be 0 since the current node has that channel already-->
            <xsl:if test="$setsize != $numSets">
                <xsl:call-template name="message">
                    <xsl:with-param name="text">Number of Roxie slaves for channel <xsl:value-of select="$channel"/> are different than those for channel 1.</xsl:with-param>
                </xsl:call-template>                
            </xsl:if>
            <xsl:if test="$channel &lt; $numChannels">
                <xsl:variable name="nextChannel" select="$channel+1"/>
                <xsl:variable name="nextChannelSlaveSet" select="../RoxieSlaveProcess[@channel=$nextChannel]"/>
                <xsl:if test="not($nextChannelSlaveSet)">
                    <xsl:message terminate="yes">No Roxie slaves defined for channel <xsl:value-of select="$nextChannel"/>.</xsl:message>
                </xsl:if>
                <xsl:apply-templates select="$nextChannelSlaveSet" mode="validateChannels">
                    <xsl:with-param name="channel" select="$nextChannel"/>
                    <xsl:with-param name="numChannels" select="$numChannels"/>
                    <xsl:with-param name="numSets" select="$numSets"/>
                </xsl:apply-templates>
            </xsl:if>
        </xsl:if>
    </xsl:template>

    <xsl:template name="makeAbsolutePath">
        <xsl:param name="path"/>
        <xsl:variable name="newPath" select="translate($path, $oldPathSeparator, $newPathSeparator)"/>
        <xsl:choose>
            <xsl:when test="$isLinuxInstance=1">
                <xsl:if test="not(starts-with($newPath, '/'))">/</xsl:if>
               <xsl:value-of select="$newPath"/>
            </xsl:when>
            <xsl:when test="starts-with($newPath, '\')"><!--windows-->
               <xsl:value-of select="substring($newPath, 2)"/>
            </xsl:when>
            <xsl:otherwise><!--windows-->
               <xsl:value-of select="$newPath"/>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>

    <xsl:template name="message">
        <xsl:param name="text"/>
        <xsl:choose>
            <xsl:when test="function-available('seisint:message')">
            <xsl:variable name="dummy" select="seisint:message($text)"/>
            </xsl:when>
            <xsl:otherwise>
                <xsl:message terminate="no">
                    <xsl:value-of select="$text"/>
                </xsl:message>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>


<xsl:template name="getDaliServers">
   <xsl:param name="daliservers"/>
   <xsl:variable name="daliServerNode" select="/Environment/Software/DaliServerProcess[@name=$daliservers]"/>
   
   <xsl:for-each select="$daliServerNode/Instance">
     <xsl:value-of select="/Environment/Hardware/Computer[@name=current()/@computer]/@netAddress"/>
    <xsl:if test="string(@port) != ''">:<xsl:value-of select="@port"/>
    </xsl:if>
    <xsl:if test="position() != last()">, </xsl:if>
   </xsl:for-each>
</xsl:template>

<xsl:template name="GetPathName">
  <xsl:param name="path"/>
  <xsl:if test="contains($path, '\')">
    <xsl:variable name="prefix" select="substring-before($path, '\')"/>
    <xsl:value-of select="concat($prefix, '\')"/>
    <xsl:call-template name="GetPathName">
      <xsl:with-param name="path" select="substring-after($path, '\')"/>
    </xsl:call-template>
  </xsl:if>
</xsl:template>

</xsl:stylesheet>
