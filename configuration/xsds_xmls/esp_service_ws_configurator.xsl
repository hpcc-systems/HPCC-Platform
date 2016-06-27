<?xml version="1.0" encoding="UTF-8"?>
<!--
################################################################################
#    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems.
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

<xsl:import href="esp_service_WsSMC.xsl"/>

    <xsl:template match="EspBinding">
        <xsl:param name="authNode"/>

     <xsl:variable name="serviceNode" select="/Environment/Software/EspService[@name=$espServiceName and @name=current()/@service and Properties/@type='WsSMC']"/>
        <xsl:apply-templates select="$serviceNode" mode="ws_configurator">
            <xsl:with-param name="bindingNode" select="."/>
            <xsl:with-param name="authNode" select="$authNode"/>
        </xsl:apply-templates>
    </xsl:template>

    <!-- WS-configurator -->
    <xsl:template match="EspService" mode="ws_configurator">
        <xsl:param name="bindingNode"/>
        <xsl:param name="authNode"/>

        <xsl:variable name="serviceType" select="'ws_configurator'"/>
        <xsl:variable name="serviceName" select="concat($serviceType, '_', @name, '_', $process)"/>
        <xsl:variable name="bindName" select="concat($serviceType, '_', $bindingNode/@name, '_', $process)"/>
        <xsl:variable name="bindType" select="'ws_configuratorSoapBinding'"/>
        <xsl:variable name="servicePlugin">
            <xsl:call-template name="defineServicePlugin">
                <xsl:with-param name="plugin" select="'ws_configurator'"/>
            </xsl:call-template>
        </xsl:variable>
        <EspService name="{$serviceName}" type="{$serviceType}" plugin="{$servicePlugin}" folderName="configurator" widgetName="ConfiguratorWidget" shortName="Configurator" longName="Configurator Plugin"/>
        <EspBinding name="{$bindName}" service="{$serviceName}" protocol="{$bindingNode/@protocol}" type="{$bindType}"
             plugin="{$servicePlugin}" netAddress="0.0.0.0" port="{$bindingNode/@port}" defaultBinding="false">
            <xsl:call-template name="bindAuthentication">
                <xsl:with-param name="bindingNode" select="$bindingNode"/>
                <xsl:with-param name="authMethod" select="$authNode/@method"/>
                <xsl:with-param name="service" select="'ws_configurator'"/>
            </xsl:call-template>
        </EspBinding>
    </xsl:template>
</xsl:stylesheet>
