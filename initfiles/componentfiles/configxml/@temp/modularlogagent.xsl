<?xml version="1.0" encoding="UTF-8"?>
<!--
################################################################################
#    HPCC SYSTEMS software Copyright (C) 2021 HPCC Systems®.
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

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xml:space="default" xmlns:seisint="http://seisint.com"  xmlns:set="http://exslt.org/sets" exclude-result-prefixes="seisint set">

    <xsl:template name="ModularLogAgent" type="DefaultLoggingAgent">
        <xsl:param name="agentName"/>
        <xsl:param name="agentNode"/>
        <xsl:param name="disableFailSafe"/>

        <xsl:if test="not($agentNode)">
            <xsl:message terminate="yes">ModularLogAgent '<xsl:value-of select="$agentName"/>' is undefined!</xsl:message>
        </xsl:if>
        <xsl:variable name="plugin">
            <xsl:choose>
                <xsl:when test="string($agentNode/@plugin) != ''"><xsl:value-of select="$agentNode/@plugin"/></xsl:when>
                <xsl:otherwise>modularlogagent</xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        <LogAgent name="{$agentName}" type="LogAgent" plugin="{$plugin}">
            <!-- Setup the FailSafe override flag based on template input. -->
            <xsl:if test="string($disableFailSafe) != ''">
                <xsl:attribute name="DisableFailSafe"><xsl:value-of select="$disableFailSafe"/></xsl:attribute>
            </xsl:if>

            <!-- Set attributes equivalent to the elements defined by DecoupledLogging (decoupled_logging.xsl).-->
            <xsl:if test="string($agentNode/@AckedFiles) != ''">
                <xsl:attribute name="AckedFiles"><xsl:value-of select="$agentNode/@AckedFiles"/></xsl:attribute>
            </xsl:if>
            <xsl:if test="string($agentNode/@AckedLogRequests) != ''">
                <xsl:attribute name="AckedLogRequests"><xsl:value-of select="$agentNode/@AckedLogRequests"/></xsl:attribute>
            </xsl:if>
            <xsl:if test="string($agentNode/@PendingLogBufferSize) != ''">
                <xsl:attribute name="PendingLogBufferSize"><xsl:value-of select="$agentNode/@PendingLogBufferSize"/></xsl:attribute>
            </xsl:if>
            <xsl:if test="string($agentNode/@ReadRequestWaitingSeconds) != ''">
                <xsl:attribute name="ReadRequestWaitingSeconds"><xsl:value-of select="$agentNode/@ReadRequestWaitingSeconds"/></xsl:attribute>
            </xsl:if>

            <!-- Set attributes equivalent to the elements defined by EspLoggingAgentBasic (esp_logging_agent_basic.xsl).-->
            <xsl:if test="string($agentNode/@FailSafe) != ''">
                <xsl:attribute name="FailSafe"><xsl:value-of select="$agentNode/@FailSafe"/></xsl:attribute>
            </xsl:if>
            <xsl:if test="string($agentNode/@FailSafeLogsDir) != ''">
                <xsl:attribute name="FailSafeLogsDir"><xsl:value-of select="$agentNode/@FailSafeLogsDir"/></xsl:attribute>
            </xsl:if>
            <xsl:if test="string($agentNode/@MaxLogQueueLength) != ''">
                <xsl:attribute name="MaxLogQueueLength"><xsl:value-of select="$agentNode/@MaxLogQueueLength"/></xsl:attribute>
            </xsl:if>
            <!-- Presented for completeness; value applies to GetTransactionSeed and is read by individual agents.
            <xsl:if test="string($agentNode/@MaxTriesGTS) != ''">
                <xsl:attribute name="MaxTriesGTS"><xsl:value-of select="$agentNode/@MaxTriesGTS"/></xsl:attribute>
            </xsl:if>
            -->
            <xsl:if test="string($agentNode/@MaxTriesRS) != ''">
                <xsl:attribute name="MaxTriesRS"><xsl:value-of select="$agentNode/@MaxTriesRS"/></xsl:attribute>
            </xsl:if>
            <xsl:if test="string($agentNode/@QueueSizeSignal) != ''">
                <xsl:attribute name="QueueSizeSignal"><xsl:value-of select="$agentNode/@QueueSizeSignal"/></xsl:attribute>
            </xsl:if>
            <xsl:if test="string($agentNode/@SafeRolloverThreshold) != ''">
                <xsl:attribute name="SafeRolloverThreshold"><xsl:value-of select="$agentNode/@SafeRolloverThreshold"/></xsl:attribute>
            </xsl:if>

            <!-- Define either split or inline configuration. -->
            <xsl:choose>
                <xsl:when test="string($agentNode/@configuration) != ''">
                    <xsl:attribute name="configuration"><xsl:value-of select="$agentNode/@configuration"/></xsl:attribute>
                </xsl:when>
                <xsl:otherwise>
                    <xsl:attribute name="trace-priority-limit"><xsl:value-of select="$agentNode/@trace-priority-limit"/></xsl:attribute>
                    <!-- All LogAgent attributes must be added before adding a child. -->
                    <UpdateLog/>
                </xsl:otherwise>
            </xsl:choose>

            <!-- Copy agent variant identification. -->
            <xsl:for-each select="$agentNode/Variant">
                <Variant type="{current()/@type}" group="{current()/@group}"/>
            </xsl:for-each>

            <!-- Copy content filters. -->
            <xsl:if test="count($agentNode/Filter) > 0">
                <Filters>
                <xsl:for-each select="$agentNode/Filter">
                    <Filter type="{current()/@type}" value="{current()/@filter}"/>
                </xsl:for-each>
                </Filters>
            </xsl:if>
        </LogAgent>
    </xsl:template>

</xsl:stylesheet>
