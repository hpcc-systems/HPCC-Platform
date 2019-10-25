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
    <xsl:import href="logging_agent.xsl"/>
    <xsl:import href="wslogserviceespagent.xsl"/>

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
        <xsl:variable name="namespaceScheme" select="@namespaceScheme"/>
        <xsl:variable name="serviceName" select="@name"/>
        <xsl:variable name="defaultFeatureAuth" select="@defaultFeatureAuth"/>
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
        <EspService name="{$serviceName}" type="{$serviceType}" plugin="{$servicePlugin}" namespaceBase="{$namespaceBase}" namespaceScheme="{$namespaceScheme}" defaultFeatureAuth="{$defaultFeatureAuth}">
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
                    <xsl:if test="string($managerNode/@FailSafe) != ''">
                        <FailSafe><xsl:value-of select="$managerNode/@FailSafe"/></FailSafe>
                    </xsl:if>
                    <xsl:if test="string($managerNode/@FailSafeLogsDir) != ''">
                        <FailSafeLogsDir><xsl:value-of select="$managerNode/@FailSafeLogsDir"/></FailSafeLogsDir>
                    </xsl:if>
                    <xsl:if test="string($managerNode/@SafeRolloverThreshold) != ''">
                        <SafeRolloverThreshold><xsl:value-of select="$managerNode/@SafeRolloverThreshold"/></SafeRolloverThreshold>
                    </xsl:if>
                    <Filters>
                        <xsl:for-each select="$managerNode/Filter">
                            <Filter value="{current()/@filter}" type="{current()/@type}"/>
                        </xsl:for-each>
                    </Filters>

                    <xsl:for-each select="$managerNode/ESPLoggingAgent">
                        <xsl:variable name="agentName" select="@ESPLoggingAgent"/>
                        <xsl:variable name="espLoggingAgentNode" select="/Environment/Software/ESPLoggingAgent[@name=$agentName]"/>
                        <xsl:variable name="wsLogServiceESPAgentNode" select="/Environment/Software/WsLogServiceESPAgent[@name=$agentName]"/>
                        <xsl:choose>
                            <xsl:when test="($espLoggingAgentNode)">
                                <xsl:call-template name="ESPLoggingAgent">
                                    <xsl:with-param name="agentName" select="$agentName"/>
                                    <xsl:with-param name="agentNode" select="$espLoggingAgentNode"/>
                                </xsl:call-template>
                            </xsl:when>
                            <xsl:when test="($wsLogServiceESPAgentNode)">
                                <xsl:call-template name="WsLogServiceESPAgent">
                                    <xsl:with-param name="agentName" select="$agentName"/>
                                    <xsl:with-param name="agentNode" select="$wsLogServiceESPAgentNode"/>
                                </xsl:call-template>
                            </xsl:when>
                            <xsl:otherwise>
                                <xsl:message terminate="yes">ESP Logging Agent is undefined for <xsl:value-of select="$managerName"/> !</xsl:message>
                            </xsl:otherwise>
                        </xsl:choose>
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
            <xsl:if test="$bindingNode/CustomBindingParameter">
                <CustomBindingParameters>
                    <xsl:for-each select="$bindingNode/CustomBindingParameter">
                        <xsl:copy-of select="."/>
                    </xsl:for-each>
                </CustomBindingParameters>
            </xsl:if>
        </EspBinding>
    </xsl:template>
</xsl:stylesheet>
