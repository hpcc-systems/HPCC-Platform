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
            <xsl:text>&#xa;</xsl:text>
            <xsl:text>[default]</xsl:text>
            <xsl:text>&#xa;</xsl:text>
            <xsl:text>thorlist=</xsl:text>
            <xsl:call-template name="getThorClusterList"/>
            <xsl:call-template name="getThorClusterDetails"/>
        </xsl:if>
    </xsl:template>

    <!--getThorClusterList-->
    <xsl:template name="getThorClusterList">
        <xsl:for-each select="/Environment/Software/BackupNodeProcess/NodeGroup">
            <xsl:value-of select="@name"/>
            <xsl:if test="position() != last()">
                <xsl:text>,</xsl:text>
            </xsl:if>
        </xsl:for-each>
    </xsl:template>
    <!--getThorClusterList-->

    <!--getThorClusterDetails-->
    <xsl:template name="getThorClusterDetails">
        <xsl:for-each select="/Environment/Software/BackupNodeProcess/NodeGroup">
            <xsl:variable name="name" select="@name"/>
            <!--header-->
            <xsl:text>&#xa;</xsl:text>
            <xsl:text>[</xsl:text>
            <xsl:value-of select="@name"/>
            <xsl:text>]</xsl:text>
            <!--interval-->
            <xsl:text>&#xa;</xsl:text>
            <xsl:text>interval=</xsl:text>
            <xsl:value-of select="@interval"/>
            <!--daliserver-->
            <xsl:text>&#xa;</xsl:text>
            <xsl:text>daliserver=</xsl:text>
            <xsl:call-template name="getDaliServers">
                <xsl:with-param name="daliServer" select="/Environment/Software/ThorCluster[@name=$name]/@daliServers"/>
            </xsl:call-template>
            <!--localthor-->
            <xsl:text>&#xa;</xsl:text>
            <xsl:text>localthor=</xsl:text>
            <xsl:value-of select="/Environment/Software/ThorCluster[@name=$name]/@localThor"/>
            <!--thormaster-->
            <xsl:text>&#xa;</xsl:text>
            <xsl:text>thormaster=</xsl:text>
            <xsl:call-template name="getNetAddress">
                <xsl:with-param name="computer" select="/Environment/Software/ThorCluster[@name=$name]/ThorMasterProcess/@computer"/>
            </xsl:call-template>
            <!--thorprimary-->
            <xsl:text>&#xa;</xsl:text>
            <xsl:text>thorprimary=</xsl:text>
            <xsl:value-of select="/Environment/Software/ThorCluster[@name=$name]/@nodeGroup"/>
            <!--thorname-->
            <xsl:text>&#xa;</xsl:text>
            <xsl:text>thorname=</xsl:text>
            <xsl:value-of select="@name"/>
            <!--SSHidentityfile-->
            <xsl:text>&#xa;</xsl:text>
            <xsl:text>SSHidentityfile=</xsl:text>
            <xsl:value-of select="/Environment/Software/ThorCluster[@name=$name]/SSH/@SSHidentityfile"/>
            <!--SSHusername-->
            <xsl:text>&#xa;</xsl:text>
            <xsl:text>SSHusername=</xsl:text>
            <xsl:value-of select="/Environment/Software/ThorCluster[@name=$name]/SSH/@SSHusername"/>
            <!--SSHpassword-->
            <xsl:text>&#xa;</xsl:text>
            <xsl:text>SSHpassword=</xsl:text>
            <xsl:value-of select="/Environment/Software/ThorCluster[@name=$name]/SSH/@SSHpassword"/>
            <!--SSHtimeout-->
            <xsl:text>&#xa;</xsl:text>
            <xsl:text>SSHtimeout=</xsl:text>
            <xsl:value-of select="/Environment/Software/ThorCluster[@name=$name]/SSH/@SSHtimeout"/>
            <!--SSHretries-->
            <xsl:text>&#xa;</xsl:text>
            <xsl:text>SSHretries=</xsl:text>
            <xsl:value-of select="/Environment/Software/ThorCluster[@name=$name]/SSH/@SSHretries"/>
        </xsl:for-each>
    </xsl:template>
    <!--getThorClusterDetails-->

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
