<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xml:space="default"
xmlns:seisint="http://seisint.com"  xmlns:set="http://exslt.org/sets" exclude-result-prefixes="seisint set">
    <xsl:import href="esp_service.xsl"/>

    <xsl:template match="EspService">
        <xsl:param name="authNode"/>
        <xsl:param name="bindingNode"/>

        <xsl:variable name="serviceType" select="'WsLoggingService'"/>
        <xsl:variable name="bindType" select="'loggingservice_binding'"/>
        <xsl:variable name="servicePlugin">
            <xsl:choose>
                <xsl:when test="$isLinuxInstance">libws_loggingservice.so</xsl:when>
                <xsl:otherwise>ws_loggingservice.dll</xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        <xsl:variable name="serviceName" select="concat('WsLoggingService', '_', @name, '_', $process)"/>
        <xsl:variable name="bindName" select="concat('WsLoggingService', '_', $bindingNode/@name, '_', $process)"/>

        <EspService name="{$serviceName}" type="{$serviceType}" plugin="{$servicePlugin}">
            <xsl:if test="string(@LoggingManager) != ''">
                <xsl:variable name="managerName" select="@LoggingManager"/>
                <xsl:variable name="managerNode" select="/Environment/Software/LoggingManager[@name=$managerName]"/>

                <xsl:if test="not($managerNode)">
                    <xsl:message terminate="yes">Logging Manager is undefined!</xsl:message>
                </xsl:if>

                <xsl:if test="not($managerNode/ESPLoggingAgent[1]/@ESPLoggingAgent)">
                     <xsl:message terminate="yes">ESP Logging Agent <xsl:value-of select="$managerNode/@ESPLoggingAgent[1]"/>is undefined!</xsl:message>
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
                        <xsl:message terminate="yes">ESP process is undefined!</xsl:message>
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

        <EspBinding name="{$bindName}" service="{$serviceName}" protocol="{$bindingNode/@protocol}" type="{$bindType}" plugin="{$servicePlugin}" netAddress="0.0.0.0" port="{$bindingNode/@port}">
            <xsl:call-template name="bindAuthentication">
                <xsl:with-param name="bindingNode" select="$bindingNode"/>
                <xsl:with-param name="authMethod" select="$authNode/@method"/>
            </xsl:call-template>
        </EspBinding>
    </xsl:template>

    <!-- UTILITY templates-->
    <xsl:template name="bindAuthentication">
      <xsl:param name="authMethod"/>
      <xsl:param name="bindingNode"/>
      <xsl:choose>
         <xsl:when test="$authMethod='basic'">
            <Authenticate type="Basic" method="UserDefined">
               <xsl:for-each select="$bindingNode/Authenticate[string(@path) != '']">
                  <Location path="{@path}"/>
               </xsl:for-each>
            </Authenticate>
         </xsl:when>
         <xsl:when test="$authMethod='local'">
            <Authenticate method="Local">
               <xsl:for-each select="$bindingNode/Authenticate[string(@path) != '']">
                  <Location path="{@path}" resource="{@resource}" required="{@access}" description="{@description}"/>
               </xsl:for-each>
            </Authenticate>
         </xsl:when>
         <xsl:when test="$authMethod='ldap' or $authMethod='ldaps'">
            <Authenticate method="LdapSecurity" config="ldapserver">
            <xsl:copy-of select="$bindingNode/@resourcesBasedn"/> <!--if binding has an ldap resourcebasedn specified then copy it out -->

            <xsl:for-each select="$bindingNode/Authenticate">
               <Location path="{@path}" resource="{@resource}" access="{@access}"/>
            </xsl:for-each>

            <xsl:for-each select="$bindingNode/AuthenticateFeature[@authenticate='Yes']">
               <Feature name="{@name}" path="{@path}" resource="{@resource}" required="{@access}" description="{@description}"/>
            </xsl:for-each>
            </Authenticate>
         </xsl:when>
         <xsl:when test="$authMethod='secmgrPlugin'">
            <Authenticate>
            <xsl:attribute name="method">
              <xsl:value-of select="$bindingNode/@type"/>
            </xsl:attribute>
            <xsl:copy-of select="$bindingNode/@resourcesBasedn"/>

            <xsl:for-each select="$bindingNode/Authenticate">
               <Location path="{@path}" resource="{@resource}" access="{@access}"/>
            </xsl:for-each>

            <xsl:for-each select="$bindingNode/AuthenticateFeature[@authenticate='Yes']">
               <Feature name="{@name}" path="{@path}" resource="{@resource}" required="{@access}" description="{@description}"/>
            </xsl:for-each>
            </Authenticate>
         </xsl:when>
      </xsl:choose>
    </xsl:template>

</xsl:stylesheet>
