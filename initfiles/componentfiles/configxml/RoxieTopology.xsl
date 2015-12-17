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
            <xsl:attribute name="querySets">
                <xsl:call-template name="GetQueueNames">
                    <xsl:with-param name="roxie" select="@name"/>
                </xsl:call-template>
            </xsl:attribute>
            <xsl:attribute name="targetAliases">
                <xsl:call-template name="GetTargetAliases">
                    <xsl:with-param name="roxie" select="@name"/>
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
            <xsl:copy-of select="/Environment/Hardware/NAS"/>
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
            <xsl:for-each select="PreferredCluster">
                <xsl:copy>
                    <xsl:copy-of select="@name"/>
                    <xsl:copy-of select="@priority"/>
                </xsl:copy>
            </xsl:for-each>
            <xsl:for-each select="RoxieFarmProcess">
                <xsl:element name="RoxieFarmProcess">
                    <xsl:copy-of select="@*[name()!='name' and name()!='level']"/>
                </xsl:element>
            </xsl:for-each>
            <xsl:for-each select="RoxieServerProcess">
                <xsl:variable name="computer" select="@computer"/>
                <xsl:element name="RoxieServerProcess">
                    <xsl:attribute name="netAddress"><xsl:value-of select="/Environment/Hardware/Computer[@name=$computer]/@netAddress"/></xsl:attribute>
                    <xsl:copy-of select="@*[name()!='netAddress' and name()!='computer' and name()!='name' and name()!='port' and name()!='level' and name()!='listenQueue' and name()!='numThreads' and name()!='requestArrayThreads']"/>
                </xsl:element>
            </xsl:for-each>
        </xsl:element>
    </xsl:template>

    
    <xsl:template name="validateChannels">
        <xsl:variable name="numChannels" select="@numChannels"/>
        <xsl:if test="$numChannels &lt; 1">
            <xsl:message terminate="yes">Number of channels must be at least 1.</xsl:message>
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

<xsl:template name="GetQueueNames">
  <xsl:param name="roxie"/>
  <xsl:for-each select="/Environment/Software/Topology/Cluster/RoxieCluster[@process=$roxie]">
   <xsl:value-of select="../@name"/>
   <xsl:if test="position() != last()">,</xsl:if>
  </xsl:for-each>
</xsl:template>

<xsl:template name="GetTargetAliases">
  <xsl:param name="roxie"/>
  <xsl:for-each select="/Environment/Software/Topology/Cluster[@alias]/RoxieCluster[@process=$roxie]">
   <xsl:value-of select="../@alias"/>=<xsl:value-of select="../@name"/>
   <xsl:if test="position() != last()">,</xsl:if>
  </xsl:for-each>
</xsl:template>


</xsl:stylesheet>
