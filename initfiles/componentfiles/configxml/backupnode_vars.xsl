<?xml version="1.0" encoding="UTF-8"?>
<!--
################################################################################
#    HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.
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
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:fo="http://www.w3.org/1999/XSL/Format" xml:space="default">
    <xsl:output method="text" indent="no" omit-xml-declaration="yes"/>
    <xsl:strip-space elements="*"/>
    <xsl:param name="process" select="'unknown'"/>

    <xsl:template match="/">
        <xsl:apply-templates select="/Environment/Software"/>
    </xsl:template>

    <xsl:template match="BackupNodeProcess">
        <xsl:if test="@name = $process">
            <xsl:call-template name="printHeader">
                <xsl:with-param name="header" select="'default'"/>
            </xsl:call-template>
            <xsl:call-template name="printVariable">
                <xsl:with-param name="var" select="'thorlist'"/>
                <xsl:with-param name="val">
                    <xsl:call-template name="getNodeList"/>
                </xsl:with-param>
            </xsl:call-template>
            <xsl:call-template name="printClusterDetails"/>
        </xsl:if>
    </xsl:template>

    <!-- getNodeList 
        return NodeGroups in csv list
    -->
    <xsl:template name="getNodeList">
        <xsl:choose>
            <xsl:when test="NodeGroup">
                <xsl:for-each select="NodeGroup">
                    <xsl:variable name="nodeName" select="@name"/>
                    <xsl:for-each select="/Environment/Software/ThorCluster">
                        <xsl:if test="($nodeName = @name) and (((@localThor = 'false' or not(@localThor)) and not(@nodeGroup)) or (@nodeGroup = @name))">
                            <xsl:value-of select="@name"/><xsl:text>,</xsl:text>
                        </xsl:if>
                    </xsl:for-each>
                </xsl:for-each>
            </xsl:when>
            <xsl:otherwise>
                <xsl:for-each select="/Environment/Software/ThorCluster">
                    <xsl:if test="((@localThor = 'false' or not(@localThor)) and not(@nodeGroup)) or (@nodeGroup = @name)">
                        <xsl:value-of select="@name"/><xsl:text>,</xsl:text>
                    </xsl:if>
                </xsl:for-each>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>
    <!-- getNodeList -->

    <xsl:template name="printClusterDetails">
        <xsl:choose>
            <xsl:when test="NodeGroup">
                <xsl:for-each select="NodeGroup">
                    <xsl:variable name="nodeName" select="@name"/>
                    <xsl:variable name="interval" select="@interval"/>
                    <xsl:for-each select="/Environment/Software/ThorCluster[@name=$nodeName]">
                        <xsl:if test="((@localThor = 'false' or not(@localThor)) and not(@nodeGroup)) or (@nodeGroup = @name)">
                            <xsl:call-template name="getThorClusterDetails">
                                <xsl:with-param name="interval" select="$interval"/>
                            </xsl:call-template>
                        </xsl:if>
                    </xsl:for-each>
                </xsl:for-each>
            </xsl:when>
            <xsl:otherwise>
                <xsl:for-each select="/Environment/Software/ThorCluster">
                    <xsl:if test="((@localThor = 'false' or not(@localThor)) and not(@nodeGroup)) or (@nodeGroup = @name)">
                        <xsl:call-template name="getThorClusterDetails">
                        </xsl:call-template>
                    </xsl:if>
                </xsl:for-each>
            </xsl:otherwise>
        </xsl:choose>		
    </xsl:template>


    <!--getThorClusterDetails-->
    <!--
        [header]
        interval
        daliserver
        localthor
        thormaster
        thorprimary
        thorname
        SSHidentityfile
        SSHusername
        SSHpassword
        SSHtimeout
        SSHretries
    -->
    <xsl:template name="getThorClusterDetails">
        <xsl:param name="interval" select="12"/>
        <xsl:call-template name="printHeader">
            <xsl:with-param name="header" select="@name"/>
        </xsl:call-template>
        <xsl:call-template name="printVariable">
            <xsl:with-param name="var" select="'interval'"/>
            <xsl:with-param name="val" select="$interval"/>
        </xsl:call-template>
        <xsl:call-template name="printVariable">
            <xsl:with-param name="var" select="'daliserver'"/>
            <xsl:with-param name="val">
                <xsl:call-template name="getDaliServers">
                    <xsl:with-param name="daliServer" select="@daliServers"/>
                </xsl:call-template>
            </xsl:with-param>
        </xsl:call-template>
        <xsl:call-template name="printVariable">
            <xsl:with-param name="var" select="'localthor'"/>
            <xsl:with-param name="val" select="@localThor"/>
        </xsl:call-template>
        <xsl:call-template name="printVariable">
            <xsl:with-param name="var" select="'thormaster'"/>
            <xsl:with-param name="val">
                <xsl:call-template name="getNetAddress">
                    <xsl:with-param name="computer" select="ThorMasterProcess/@computer"/>
                </xsl:call-template>
            </xsl:with-param>
        </xsl:call-template>
        <xsl:call-template name="printVariable">
            <xsl:with-param name="var" select="'thorprimary'"/>
            <xsl:with-param name="val" select="@nodeGroup"/>
        </xsl:call-template>
        <xsl:call-template name="printVariable">
            <xsl:with-param name="var" select="'thorname'"/>
            <xsl:with-param name="val" select="@name"/>
        </xsl:call-template>
        <xsl:call-template name="printVariable">
            <xsl:with-param name="var" select="'SSHidentityfile'"/>
            <xsl:with-param name="val" select="SSH/@SSHidentityfile"/>
        </xsl:call-template>
        <xsl:call-template name="printVariable">
            <xsl:with-param name="var" select="'SSHusername'"/>
            <xsl:with-param name="val" select="SSH/@SSHusername"/>
        </xsl:call-template>
        <xsl:call-template name="printVariable">
            <xsl:with-param name="var" select="'SSHpassword'"/>
            <xsl:with-param name="val" select="SSH/@SSHpassword"/>
        </xsl:call-template>
        <xsl:call-template name="printVariable">
            <xsl:with-param name="var" select="'SSHtimeout'"/>
            <xsl:with-param name="val" select="SSH/@SSHtimeout"/>
        </xsl:call-template>
        <xsl:call-template name="printVariable">
            <xsl:with-param name="var" select="'SSHretries'"/>
            <xsl:with-param name="val" select="SSH/@SSHretries"/>
        </xsl:call-template>
    </xsl:template>
    <!--getThorClusterDetails-->

    <!-- printHeader -->
    <xsl:template name="printHeader">
        <xsl:param name="header"/>
        <xsl:text>[</xsl:text><xsl:value-of select="$header"/><xsl:text>]&#xa;</xsl:text>
    </xsl:template>
    <!-- printHeader -->

    <!-- printVariable -->
    <xsl:template name="printVariable">
        <xsl:param name="var"/>
        <xsl:param name="val"/>
        <xsl:value-of select="$var"/><xsl:text>=</xsl:text><xsl:value-of select="$val"/><xsl:text>&#xa;</xsl:text>
    </xsl:template>
    <!-- printVar -->

    <!--getDaliServers-->
    <xsl:template name="getDaliServers">
        <xsl:param name="daliServer"/>
        <xsl:for-each select="/Environment/Software/DaliServerProcess[@name=$daliServer]/Instance">
            <xsl:call-template name="getNetAddress">
                <xsl:with-param name="computer" select="@computer"/>
            </xsl:call-template>
            <xsl:if test="string(@port) != ''">:<xsl:value-of select="@port"/></xsl:if>
            <xsl:if test="position() != last()">, </xsl:if>
        </xsl:for-each>
    </xsl:template>
    <!--getDaliServers-->


    <!--getNetAddress-->
    <xsl:template name="getNetAddress">
        <xsl:param name="computer"/>
        <xsl:value-of select="/Environment/Hardware/Computer[@name=$computer]/@netAddress"/>
    </xsl:template>
    <!--getNetAddress-->

</xsl:stylesheet>
