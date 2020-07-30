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

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xml:space="default"
xmlns:set="http://exslt.org/sets">
    <xsl:import href="esp_logging_agent_basic.xsl"/>
    <xsl:import href="esp_logging_transid.xsl"/>
    <xsl:import href="decoupled_logging.xsl"/>

    <xsl:template name="WsLogServiceESPAgent" type="DefaultLoggingAgent">
        <xsl:param name="agentName"/>
        <xsl:param name="agentNode"/>
        <xsl:param name="disableFailSafe"/>
        <xsl:if test="not($agentNode)">
            <xsl:message terminate="yes">An WsLogService ESP Logging Agent <xsl:value-of select="$agentName"/> is undefined!</xsl:message>
        </xsl:if>
        <xsl:variable name="loggingServer" select="$agentNode/LoggingServer"/>
        <xsl:if test="not($loggingServer)">
            <xsl:message terminate="yes">ESP logging server is undefined for <xsl:value-of select="$agentName"/> </xsl:message>
        </xsl:if>
        <xsl:variable name="loggingServerUrl" select="$loggingServer/@Url"/>
        <xsl:if test="string($loggingServerUrl) = ''">
            <xsl:message terminate="yes">Logging Server URL is undefined for <xsl:value-of select="$agentName"/>!</xsl:message>
        </xsl:if>
        <xsl:if test="not($agentNode/LogDataItem[1]) and not($agentNode/LogInfo[1])">
            <xsl:message terminate="yes">Log Data XPath is undefined for <xsl:value-of select="$agentName"/> </xsl:message>
        </xsl:if>

        <xsl:variable name="Services">
            <xsl:choose>
                <xsl:when test="string($agentNode/@Services) != ''"><xsl:value-of select="$agentNode/@Services"/></xsl:when>
                <xsl:otherwise>GetTransactionSeed,UpdateLog,GetTransactionID</xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        <LogAgent name="{$agentName}" type="LogAgent" services="{$Services}" plugin="wslogserviceespagent">
            <LoggingServer url="{$loggingServerUrl}" user="{$loggingServer/@User}" password="{$loggingServer/@Password}"/>
            <xsl:call-template name="DecoupledLogging">
                <xsl:with-param name="agentNode" select="$agentNode"/>
            </xsl:call-template>
            <xsl:call-template name="EspLoggingAgentBasic">
                <xsl:with-param name="agentNode" select="$agentNode"/>
            </xsl:call-template>
            <xsl:if test="string($disableFailSafe) != ''">
                <DisableFailSafe><xsl:value-of select="$disableFailSafe"/></DisableFailSafe>
            </xsl:if>
            <xsl:if test="string($agentNode/@TransactionSeedType) != ''">
                <TransactionSeedType><xsl:value-of select="$agentNode/@TransactionSeedType"/></TransactionSeedType>
            </xsl:if>
            <xsl:if test="string($agentNode/@AlternativeTransactionSeedType) != ''">
                <AlternativeTransactionSeedType><xsl:value-of select="$agentNode/@AlternativeTransactionSeedType"/></AlternativeTransactionSeedType>
            </xsl:if>

            <xsl:call-template name="EspLoggingTransactionID">
                <xsl:with-param name="agentNode" select="$agentNode"/>
            </xsl:call-template>
                
            <LogDataXPath>
                <xsl:for-each select="$agentNode/LogDataItem">
                    <LogDataItem name="{current()/@name}" XPath="{current()/@xpath}" xsl="{current()/@xsl}" encode="{current()/@encode}" default="{current()/@default}"/>
                </xsl:for-each>
                <xsl:for-each select="$agentNode/LogInfo">
                    <LogInfo name="{current()/@name}" default="{current()/@default}" XPath="{current()/@xpath}" xsl="{current()/@xsl}" multiple="{current()/@multiple}" encode="{current()/@encode}" type="{current()/@type}"/>
                </xsl:for-each>
            </LogDataXPath>

            <xsl:for-each select="$agentNode/Variant">
                <Variant type="{current()/@type}" group="{current()/@group}"/>
            </xsl:for-each>
        </LogAgent>
    </xsl:template>

</xsl:stylesheet>
