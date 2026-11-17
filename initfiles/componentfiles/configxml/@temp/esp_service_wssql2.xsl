<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xml:space="default"
xmlns:set="http://exslt.org/sets" exclude-result-prefixes="set">

<xsl:import href="esp_service_WsSMC.xsl"/>

    <xsl:template match="EspBinding">
        <xsl:param name="authNode"/>
        <xsl:variable name="serviceNode" select="/Environment/Software/EspService[@name=$espServiceName and @name=current()/@service and Properties/@type='ws_sql2']"/>
        <xsl:apply-templates select="$serviceNode" mode="WsSQL2">
            <xsl:with-param name="bindingNode" select="."/>
            <xsl:with-param name="authNode" select="$authNode"/>
        </xsl:apply-templates>
    </xsl:template>

    <xsl:template match="EspService" mode="WsSQL2">
        <xsl:param name="bindingNode"/>
        <xsl:param name="authNode"/>
        <xsl:variable name="serviceType" select="'ws_sql2'"/>
        <xsl:variable name="serviceName" select="concat($serviceType, '_', @name, '_', $process)"/>
        <xsl:variable name="bindName" select="concat($serviceType, '_', $bindingNode/@name, '_', $process)"/>
        <xsl:variable name="bindType" select="'ws_sql2SoapBinding'"/>
        <xsl:variable name="servicePlugin">
            <xsl:call-template name="defineServicePlugin">
                <xsl:with-param name="plugin" select="'ws_sql2'"/>
            </xsl:call-template>
        </xsl:variable>
        <EspService name="{$serviceName}" type="{$serviceType}" plugin="{$servicePlugin}">
            <xsl:copy-of select="@memoryLimit|@threads|@tempDirectory|@maxTempDirectorySize|@maxByteLimit|@defaultFetchSize|@maxFetchSize|@maxActiveCursors|@maxCursorsPerUser|@cursorIdleTimeoutSeconds|@cursorMaxLifetimeSeconds|@leakCheckThresholdBytes"/>
        </EspService>
        <EspBinding name="{$bindName}" service="{$serviceName}" protocol="{$bindingNode/@protocol}" type="{$bindType}"
            plugin="{$servicePlugin}" netAddress="0.0.0.0" port="{$bindingNode/@port}" defaultBinding="true">
            <xsl:call-template name="bindAuthentication">
                <xsl:with-param name="bindingNode" select="$bindingNode"/>
                <xsl:with-param name="authMethod" select="$authNode/@method"/>
                <xsl:with-param name="service" select="'ws_sql2'"/>
            </xsl:call-template>
        </EspBinding>
    </xsl:template>
</xsl:stylesheet>
