<?xml version="1.0" encoding="UTF-8"?>
<!--
################################################################################
#    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.
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
xmlns:set="http://exslt.org/sets" exclude-result-prefixes="set">

  <xsl:template name="EspLoggingAgentBasic">
    <xsl:param name="agentNode"/>
    <xsl:if test="string($agentNode/@FailSafe) != ''">
        <FailSafe><xsl:value-of select="$agentNode/@FailSafe"/></FailSafe>
    </xsl:if>
    <xsl:if test="string($agentNode/@FailSafeLogsDir) != ''">
        <FailSafeLogsDir><xsl:value-of select="$agentNode/@FailSafeLogsDir"/></FailSafeLogsDir>
    </xsl:if>
    <xsl:if test="string($agentNode/@MaxLogQueueLength) != ''">
        <MaxLogQueueLength><xsl:value-of select="$agentNode/@MaxLogQueueLength"/></MaxLogQueueLength>
    </xsl:if>
    <xsl:if test="string($agentNode/@MaxTriesGTS) != ''">
        <MaxTriesGTS><xsl:value-of select="$agentNode/@MaxTriesGTS"/></MaxTriesGTS>
    </xsl:if>
    <xsl:if test="string($agentNode/@MaxTriesRS) != ''">
        <MaxTriesRS><xsl:value-of select="$agentNode/@MaxTriesRS"/></MaxTriesRS>
    </xsl:if>
    <xsl:if test="string($agentNode/@QueueSizeSignal) != ''">
        <QueueSizeSignal><xsl:value-of select="$agentNode/@QueueSizeSignal"/></QueueSizeSignal>
    </xsl:if>
    <xsl:if test="string($agentNode/@SafeRolloverThreshold) != ''">
        <SafeRolloverThreshold><xsl:value-of select="$agentNode/@SafeRolloverThreshold"/></SafeRolloverThreshold>
    </xsl:if>
  </xsl:template>
</xsl:stylesheet>
