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
        <xsl:variable name="serviceName" select="concat($serviceType, '_', @name, '_', $process)"/>
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
        <EspService name="{$serviceName}" type="{$serviceType}" plugin="{$servicePlugin}">
            <!--xsl:call-template name="processServiceSpecifics">
                <xsl:with-param name="serviceType" select="$serviceType"/>
            </xsl:call-template-->
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
