<?xml version="1.0" encoding="UTF-8"?>
<!--
################################################################################
#    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems®.
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
    <xsl:import href="esp_service.xsl"/>

    <xsl:template match="EspService">
        <xsl:param name="bindingNode"/>
        <xsl:param name="authNode"/>

        <xsl:variable name="serviceType" >
            <xsl:choose>
                <xsl:when test="Properties/@type='DynamicESDL'">
                    <xsl:value-of select="@name"/>
                </xsl:when>
                <xsl:otherwise>
                    <xsl:value-of select="Properties/@type"/>
                </xsl:otherwise>
            </xsl:choose>
        </xsl:variable>

        <xsl:variable name="namespaceBase" select="@namespaceBase"/>
        <xsl:variable name="serviceName" select="@name"/>
        <xsl:variable name="bindName" select=" $bindingNode/@name"/>
        <xsl:variable name="bindType">
            <xsl:variable name="protocolBasedBindingType" select="Properties/Binding[@protocol=$bindingNode/@protocol]/@type"/>
                <xsl:choose>
                    <xsl:when test="string($protocolBasedBindingType)!=''">
                        <xsl:value-of select="$protocolBasedBindingType"/>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:value-of select="Properties/@bindingType"/>
                    </xsl:otherwise>
                </xsl:choose>
        </xsl:variable>
        <xsl:variable name="servicePlugin">
            <xsl:call-template name="defineServicePlugin">
                <xsl:with-param name="plugin" select="Properties/@plugin"/>
            </xsl:call-template>
        </xsl:variable>
        <EspService name="{$serviceName}" type="{$serviceType}" plugin="{$servicePlugin}" namespaceBase="{$namespaceBase}">
            <xsl:if test="string(@LoggingManager) != ''">
                <xsl:variable name="managerName" select="@LoggingManager"/>
                <xsl:variable name="managerNode" select="/Environment/Software/LoggingManager[@name=$managerName]"/>

                <xsl:if test="not($managerNode)">
                    <xsl:message terminate="yes">Logging Manager is undefined!</xsl:message>
                </xsl:if>

                <xsl:if test="not($managerNode/ESPLoggingAgent/@ESPLoggingAgent[1])">
                     <xsl:message terminate="yes">ESP Logging Agent <xsl:value-of select="$managerNode/@ESPLoggingAgent"/> is undefined for <xsl:value-of select="$managerName"/> !</xsl:message>
                </xsl:if>

                <LoggingManager name="{$managerNode/@name}">

                <xsl:for-each select="$managerNode/ESPLoggingAgent">
                    <xsl:variable name="agentName" select="@ESPLoggingAgent"/>
                    <xsl:variable name="agentNode" select="/Environment/Software/ESPLoggingAgent[@name=$agentName]"/>

                    <xsl:if test="not($agentNode)">
                        <xsl:message terminate="yes">An ESP Logging Agent <xsl:value-of select="$agentName"/>  for <xsl:value-of select="$managerNode/@name"/> is undefined!</xsl:message>
                    </xsl:if>

                    <xsl:if test="string($agentNode/@ESPServer) = ''">
                        <xsl:message terminate="yes">ESP server is undefined for <xsl:value-of select="$agentName"/> </xsl:message>
                    </xsl:if>

                    <xsl:variable name="espServer" select="$agentNode/@ESPServer"/>
                    <xsl:variable name="espNode" select="/Environment/Software/EspProcess[@name=$espServer]"/>

                    <xsl:if test="not($espNode)">
                        <xsl:message terminate="yes">ESP process for <xsl:value-of select="$agentName"/> is undefined!</xsl:message>
                    </xsl:if>

                    <xsl:variable name="espPort" select="$espNode/EspBinding[@service='wslogging']/@port"/>

                    <xsl:if test="string($espPort) = ''">
                        <xsl:message terminate="yes">ESP server port is undefined for <xsl:value-of select="$espServer"/>!</xsl:message>
                    </xsl:if>

                    <xsl:variable name="espNetAddress" select="$espNode/Instance/@netAddress"/>

                    <xsl:if test="string($espNetAddress) = ''">
                        <xsl:message terminate="yes">ESP NetAddress is undefined!</xsl:message>
                    </xsl:if>
                    <xsl:variable name="wsloggingUrl"><xsl:text>http://</xsl:text><xsl:value-of select="$espNetAddress"/><xsl:text>:</xsl:text><xsl:value-of select="$espPort"/></xsl:variable>
                        <LogAgent name="{$agentName}" type="LogAgent" services="GetTransactionSeed,UpdateLog" plugin="espserverloggingagent">
                            <ESPServer url="{$wsloggingUrl}" user="{$agentNode/@User}" password="{$agentNode/@Password}"/>
                            <xsl:if test="string($agentNode/@MaxServerWaitingSeconds) != ''">
                                <MaxServerWaitingSeconds><xsl:value-of select="$agentNode/@MaxServerWaitingSeconds"/></MaxServerWaitingSeconds>
                            </xsl:if>
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
                            <Filters>
                                <xsl:for-each select="$agentNode/Filter">
                                    <Filter value="{current()/@filter}" type="{current()/@type}"/>
                                </xsl:for-each>
                            </Filters>
                        </LogAgent>
                </xsl:for-each>

                </LoggingManager>

            </xsl:if>
        </EspService>
        <EspBinding name="{$bindName}" service="{$serviceName}" protocol="{$bindingNode/@protocol}" type="{$bindType}" plugin="{$servicePlugin}" netAddress="0.0.0.0" port="{$bindingNode/@port}" defaultBinding="true">
            <xsl:call-template name="bindAuthentication">
                <xsl:with-param name="bindingNode" select="$bindingNode"/>
                <xsl:with-param name="authMethod" select="$authNode/@method"/>
                <xsl:with-param name="service" select="Properties/@type"/>
            </xsl:call-template>
        </EspBinding>
    </xsl:template>
</xsl:stylesheet>
