<?xml version="1.0" encoding="UTF-8"?>

<!--
################################################################################
#    HPCC SYSTEMS software Copyright (C) 2016 HPCC Systems®.
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

    <xsl:template name="LogSourceMap">
        <xsl:param name="agentNode"/>
        <LogSourceMap>
            <xsl:for-each select="$agentNode/LogSourceMap/LogSource">
                <xsl:if test="string(current()/@name) = ''">
                    <xsl:message terminate="yes">LogSource name is undefined!</xsl:message>
                </xsl:if>
                <xsl:if test="string(current()/@mapToDB) = ''">
                    <xsl:message terminate="yes">LogSource mapToDB is undefined for <xsl:value-of select="current()/@name"/>!</xsl:message>
                </xsl:if>
                <xsl:if test="string(current()/@mapToLogGroup) = ''">
                    <xsl:message terminate="yes">LogSource mapToLogGroup is undefined for <xsl:value-of select="current()/@name"/>!</xsl:message>
                </xsl:if>
                <LogSource name="{current()/@name}" maptodb="{current()/@mapToDB}" maptologgroup="{current()/@mapToLogGroup}"/>
            </xsl:for-each>
        </LogSourceMap>
    </xsl:template>

    <xsl:template name="LogGroup">
        <xsl:param name="agentNode"/>
        <xsl:for-each select="$agentNode/LogGroup">
            <LogGroup name="{current()/@name}">
                <xsl:if test="string(current()/@MaxTransIDLength) != ''">
                    <MaxTransIDLength><xsl:value-of select="current()/@MaxTransIDLength"/></MaxTransIDLength>
                </xsl:if>
                <xsl:if test="string(current()/@MaxTransIDSequenceNumber) != ''">
                    <MaxTransIDSequenceNumber><xsl:value-of select="current()/@MaxTransIDSequenceNumber"/></MaxTransIDSequenceNumber>
                </xsl:if>
                <xsl:if test="string(current()/@MaxTransSeedTimeoutMinutes) != ''">
                    <MaxTransSeedTimeoutMinutes><xsl:value-of select="current()/@MaxTransSeedTimeoutMinutes"/></MaxTransSeedTimeoutMinutes>
                </xsl:if>
                <xsl:for-each select="current()/Fieldmap">
                    <Fieldmap table="{current()/@table}">
                        <xsl:for-each select="current()/Field">
                            <xsl:if test="string(current()/@name) = ''">
                                <xsl:message terminate="yes">Field name is undefined!</xsl:message>
                            </xsl:if>
                            <xsl:if test="string(current()/@mapTo) = ''">
                                <xsl:message terminate="yes">Field mapTo is undefined for <xsl:value-of select="current()/@name"/>!</xsl:message>
                            </xsl:if>
                            <xsl:if test="string(current()/@type) = ''">
                                <xsl:message terminate="yes">Field type is undefined for <xsl:value-of select="current()/@name"/>!</xsl:message>
                            </xsl:if>
                            <xsl:choose>
                                <xsl:when test="string(current()/@default) = ''">
                                    <Field name="{current()/@name}" mapto="{current()/@mapTo}" type="{current()/@type}"/>
                                </xsl:when>
                                <xsl:otherwise>
                                    <Field name="{current()/@name}" mapto="{current()/@mapTo}" type="{current()/@type}" default="{current()/@default}"/>
                                </xsl:otherwise>
                            </xsl:choose>
                        </xsl:for-each>
                    </Fieldmap>
                </xsl:for-each>
            </LogGroup>
        </xsl:for-each>
    </xsl:template>

    <xsl:template name="TransactionSeed">
        <xsl:param name="agentNode"/>
        <xsl:if test="string($agentNode/@defaultTransaction) != ''">
            <defaultTransaction><xsl:value-of select="$agentNode/@defaultTransaction"/></defaultTransaction>
        </xsl:if>
        <xsl:if test="string($agentNode/@loggingTransaction) != ''">
            <LoggingTransaction><xsl:value-of select="$agentNode/@loggingTransaction"/></LoggingTransaction>
        </xsl:if>
    </xsl:template>

    <xsl:template name="CassandraLoggingAgent">
        <xsl:param name="agentName"/>
        <xsl:param name="agentNode"/>
        <xsl:if test="string($agentNode/@serverIP) = ''">
            <xsl:message terminate="yes">Cassandra server network address is undefined for <xsl:value-of select="$agentName"/>!</xsl:message>
        </xsl:if>
        <xsl:variable name="Services">
            <xsl:choose>
                <xsl:when test="string($agentNode/@Services) != ''"><xsl:value-of select="$agentNode/@Services"/></xsl:when>
                <xsl:otherwise>GetTransactionSeed,UpdateLog,GetTransactionID</xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        <LogAgent name="{$agentName}" type="LogAgent" services="{$Services}" plugin="cassandralogagent">
            <Cassandra server="{$agentNode/@serverIP}" dbUser="{$agentNode/@userName}" dbPassWord="{$agentNode/@userPassword}" dbName="{$agentNode/@ksName}"/>

            <xsl:call-template name="DecoupledLogging">
                <xsl:with-param name="agentNode" select="$agentNode"/>
            </xsl:call-template>
            <xsl:call-template name="EspLoggingAgentBasic">
                <xsl:with-param name="agentNode" select="$agentNode"/>
            </xsl:call-template>
            <xsl:call-template name="TransactionSeed">
                <xsl:with-param name="agentNode" select="$agentNode"/>
            </xsl:call-template>

            <xsl:if test="string($agentNode/@logSourcePath) != ''">
                <LogSourcePath><xsl:value-of select="$agentNode/@logSourcePath"/></LogSourcePath>
            </xsl:if>

            <xsl:call-template name="LogSourceMap">
                <xsl:with-param name="agentNode" select="$agentNode"/>
            </xsl:call-template>
            <xsl:call-template name="LogGroup">
                <xsl:with-param name="agentNode" select="$agentNode"/>
            </xsl:call-template>
        </LogAgent>
    </xsl:template>

    <xsl:template name="ESPLoggingAgent">
        <xsl:param name="agentName"/>
        <xsl:param name="agentNode"/>
        <xsl:variable name="ESPLoggingUrl">
          <xsl:choose>
            <xsl:when test="string($agentNode/@ESPLoggingUrl) != ''"><xsl:value-of select="$agentNode/@ESPLoggingUrl"/></xsl:when>
            <xsl:otherwise>
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

        <xsl:text>http://</xsl:text><xsl:value-of select="$espNetAddress"/><xsl:text>:</xsl:text><xsl:value-of select="$espPort"/>
            </xsl:otherwise>
          </xsl:choose>
        </xsl:variable>
        <xsl:variable name="Services">
            <xsl:choose>
                <xsl:when test="string($agentNode/@Services) != ''"><xsl:value-of select="$agentNode/@Services"/></xsl:when>
                <xsl:otherwise>GetTransactionSeed,UpdateLog,GetTransactionID</xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        <LogAgent name="{$agentName}" type="LogAgent" services="{$Services}" plugin="espserverloggingagent">
            <ESPServer url="{$ESPLoggingUrl}" user="{$agentNode/@User}" password="{$agentNode/@Password}"/>
            <xsl:if test="string($agentNode/@MaxServerWaitingSeconds) != ''">
                <MaxServerWaitingSeconds><xsl:value-of select="$agentNode/@MaxServerWaitingSeconds"/></MaxServerWaitingSeconds>
            </xsl:if>

            <xsl:call-template name="EspLoggingTransactionID">
                <xsl:with-param name="agentNode" select="$agentNode"/>
            </xsl:call-template>

            <xsl:call-template name="DecoupledLogging">
                <xsl:with-param name="agentNode" select="$agentNode"/>
            </xsl:call-template>
            <xsl:call-template name="EspLoggingAgentBasic">
                <xsl:with-param name="agentNode" select="$agentNode"/>
            </xsl:call-template>
            <xsl:call-template name="LogSourceMap">
                <xsl:with-param name="agentNode" select="$agentNode"/>
            </xsl:call-template>
            <xsl:call-template name="LogGroup">
                <xsl:with-param name="agentNode" select="$agentNode"/>
            </xsl:call-template>
            <Filters>
                <xsl:for-each select="$agentNode/Filter">
                    <Filter value="{current()/@filter}" type="{current()/@type}"/>
                </xsl:for-each>
            </Filters>
        </LogAgent>
    </xsl:template>

</xsl:stylesheet>
