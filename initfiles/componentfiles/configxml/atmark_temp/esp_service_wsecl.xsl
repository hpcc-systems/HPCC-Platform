<?xml version="1.0" encoding="UTF-8"?>
<!--
################################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
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

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xml:space="default">
    <xsl:output method="xml" indent="yes" omit-xml-declaration="no" encoding="UTF-8"/>
    <xsl:param name="process" select="'esp'"/>
    <xsl:param name="instance" select="'s1'"/>
    <xsl:param name="isLinuxInstance" select="0"/>
    <xsl:param name="espServiceName" select="'wsecl'"/>


    <xsl:template match="text()"/>

    <xsl:variable name="serviceModuleType" select="'wsecl'"/>

    <xsl:template match="/Environment">
        <Environment>
            <Software>
                <EspProcess>
                    <xsl:apply-templates select="/Environment/Software/EspProcess[@name=$process]"/>
                </EspProcess>
            </Software>
        </Environment>
    </xsl:template>

    <xsl:template match="EspProcess">
        <xsl:variable name="authNode" select="Authentication[1]"/>
        <xsl:for-each select="EspBinding">
            <xsl:variable name="serviceModuleName" select="@service"/>
            <xsl:apply-templates select="/Environment/Software/EspService[@name=$espServiceName and @name=$serviceModuleName and Properties/@type=$serviceModuleType]">
                <xsl:with-param name="bindingNode" select="."/>
                <xsl:with-param name="authNode" select="$authNode"/>
            </xsl:apply-templates>
        </xsl:for-each>
    </xsl:template>

   <!--create one of these service specific templates  for each type of service to be created in the final configuration file-->
   <xsl:template match="EspService">
   <xsl:param name="authNode"/>
   <xsl:param name="bindingNode"/>
      <xsl:variable name="serviceType" select="'ws_ecl_attribute'"/>
      <xsl:variable name="bindType" select="'scrubbed_binding'"/>
      <xsl:variable name="servicePlugin">
      <xsl:choose>
         <xsl:when test="$isLinuxInstance">libws_ecl_attribute.so</xsl:when>
         <xsl:otherwise>ws_ecl_attribute.dll</xsl:otherwise>
      </xsl:choose>
      </xsl:variable>

      <xsl:variable name="serviceName" select="concat('wsecl', '_', @name, '_', $process)"/>
      <xsl:variable name="bindName" select="concat('wsecl', '_', $bindingNode/@name, '_', $process)"/>
      <EspService name="{$serviceName}" type="{$serviceType}" plugin="{$servicePlugin}">
         <xsl:if test="string(@clusterName) != ''">
            <ClusterName><xsl:value-of select="@clusterName"/></ClusterName>
         </xsl:if>
         <!--Did you know that the eclServer prompt in wsecl actually meant to ask for eclserver queue and not just its name?
              There may be configurations with eclserver queue names specified as @eclServer so handle them as well-->
         <xsl:variable name="eclServerName" select="@eclServer"/>
         <xsl:variable name="eclServerNode" select="/Environment/Software/EclServerProcess[@name=$eclServerName]"/>
         <xsl:variable name="eclServerQueue">
            <xsl:choose>
               <xsl:when test="$eclServerNode"><!--if there is an eclserver with this name-->
                  <xsl:value-of select="$eclServerNode/@queue"/>
               </xsl:when>
               <xsl:when test="/Environment/Software/EclServerProcess[@queue=$eclServerName]"><!--if there is an eclserver queue with this name-->
                  <xsl:value-of select="$eclServerName"/>
               </xsl:when>
               <xsl:otherwise/>
            </xsl:choose>
         </xsl:variable>
         <xsl:choose>
             <xsl:when test="string(@roxieAddress) != '' and string(@attributeServer) != ''">
                <xsl:message terminate="yes">
                    <xsl:text>Both Roxie address and Attribute Server cannot be specified for '</xsl:text>
                    <xsl:value-of select="$espServiceName"/>
                    <xsl:text>' service at the same time!</xsl:text>
                </xsl:message>
             </xsl:when>
             <xsl:when test="string(@roxieAddress) != ''">
                <RoxieAddress><xsl:value-of select="@roxieAddress"/></RoxieAddress>
                <xsl:if test="@loadbalanced">
                    <LoadBalanced><xsl:value-of select="@loadbalanced"/></LoadBalanced>
                </xsl:if>
             </xsl:when>
             <xsl:when test="string(@attributeServer) != ''">
                        <AttributeServer>
                            <xsl:call-template name="GetEspBindingAddress">
                                <xsl:with-param name="espBindingInfo" select="@attributeServer"/><!--format is "esp_name/binding_name" -->
                            </xsl:call-template>
                        </AttributeServer>
                <xsl:if test="string($eclServerQueue)=''">
                    <xsl:message terminate="yes">An ECL server queue must be specified!</xsl:message>
                </xsl:if>
               <EclServer><xsl:value-of select="$eclServerQueue"/></EclServer>
               <EclServerQueue>
                <xsl:value-of select="$eclServerQueue"/>
               </EclServerQueue><!-- produce both EclServer and EclServerQueue and phase out EclServer tag eventually -->
             </xsl:when>
             <xsl:otherwise>
                <xsl:message terminate="yes">Either roxieAddress or ECL server attribute must be specified for '<xsl:value-of select="$espServiceName"/> service!</xsl:message>
             </xsl:otherwise>
         </xsl:choose>
            <xsl:if test="string(@eclWatch) != ''">
               <EclWatch>
                <xsl:call-template name="GetEspBindingAddress">
                    <xsl:with-param name="espBindingInfo" select="@eclWatch"/><!--format is "esp_name/binding_name" -->
                </xsl:call-template>
               </EclWatch>
            </xsl:if>
            <!--optional values-->
            <xsl:if test="(@userName)">
                <UserName><xsl:value-of select="@userName"/></UserName>
            </xsl:if>
            <xsl:if test="(@password)">
                <Password><xsl:value-of select="@password"/></Password>
            </xsl:if>
            <xsl:if test="string(@wuTimeout) != ''">
                <WuTimeout><xsl:value-of select="@wuTimeout"/></WuTimeout>
            </xsl:if>
            <xsl:if test="string(@roxieTimeout) != ''">
                <RoxieTimeout><xsl:value-of select="@roxieTimeout"/></RoxieTimeout>
            </xsl:if>
            <xsl:if test="string(@deleteWorkunits) != ''">
                <DeleteWorkUnits><xsl:value-of select="@deleteWorkunits"/></DeleteWorkUnits>
            </xsl:if>
            <xsl:if test="string(@roxyServer) != ''">
                <RoxieAddress><xsl:value-of select="@roxyServer"/></RoxieAddress>
            </xsl:if>
            <xsl:choose>
                <xsl:when test="string(@defaultStyle) != ''">
                    <DefaultStyle><xsl:value-of select="@defaultStyle"/></DefaultStyle>
                </xsl:when>
                <xsl:otherwise>
                    <DefaultStyle>wsecl/xslt</DefaultStyle>
                </xsl:otherwise>
            </xsl:choose>
            <xsl:choose>
                <xsl:when test="string(@encodeResultsXml) != ''">
                    <EncodeResultsXml><xsl:value-of select="@encodeResultsXml"/></EncodeResultsXml>
                </xsl:when>
                <xsl:otherwise>
                    <EncodeResultsXml>true</EncodeResultsXml>
                </xsl:otherwise>
            </xsl:choose>
            <xsl:if test="string(@loggingServer) != ''">
                <loggingserver>
                    <url><xsl:value-of select="@loggingServer"/></url>
                    <xsl:if test="@failSafe='true'">
                            <failsafe>true</failsafe>
                    </xsl:if>
                </loggingserver>
            </xsl:if>
            <!--hard coded values-->
            <FilesPath>files/</FilesPath>
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
               <xsl:for-each select="$bindingNode/Authenticate[string(@path) != '']">
                  <Location path="{@path}" resource="{@resource}" access="{@access}" description="{@description}"/>
               </xsl:for-each>
               <xsl:for-each select="$bindingNode/AuthenticateFeature[@authenticate='Yes']">
                  <Feature name="{@name}" path="{@path}" resource="{@resource}" required="{@access}" description="{@description}"/>
               </xsl:for-each>
            </Authenticate>
         </xsl:when>
         <xsl:when test="$authMethod='htpasswd'">
           <Authenticate method="htpasswd">
             <xsl:attribute name="htpasswdFile"> <xsl:value-of select="$bindingNode/../Authentication/@htpasswdFile"/> </xsl:attribute>
           </Authenticate>
         </xsl:when>
      </xsl:choose>
   </xsl:template>
    <xsl:template name="GetEspBindingAddress">
    <xsl:param name="espBindingInfo"/><!--format is "esp_name/binding_name" -->
    <xsl:param name="addProtocolPrefix" select="'true'"/>
      <xsl:variable name="espName" select="substring-before($espBindingInfo, '/')"/>
      <xsl:variable name="bindingName" select="substring-after($espBindingInfo, '/')"/>
      <xsl:variable name="espNode" select="/Environment/Software/EspProcess[@name=$espName]"/>
      <xsl:variable name="bindingNode" select="$espNode/EspBinding[@name=$bindingName]"/>
      <xsl:if test="not($espNode) or not($bindingNode)">
         <xsl:message terminate="yes">Invalid ESP process and/or ESP binding information in '<xsl:value-of select="$espBindingInfo"/>'.</xsl:message>
      </xsl:if>
      <xsl:variable name="espComputer" select="$espNode/Instance[1]/@computer"/>
      <xsl:variable name="espIP" select="/Environment/Hardware/Computer[@name=$espComputer]/@netAddress"/>
      <xsl:if test="string($espIP) = ''">
         <xsl:message terminate="yes">The ESP server defined in '<xsl:value-of select="$espBindingInfo"/>' has invalid instance!</xsl:message>
      </xsl:if>

      <xsl:if test="string($bindingNode/@port) = ''">
         <xsl:message terminate="yes">The ESP binding defined in '<xsl:value-of select="$espBindingInfo"/>' has invalid port!</xsl:message>
      </xsl:if>
      <xsl:if test="boolean($addProtocolPrefix)">
         <xsl:if test="string($bindingNode/@protocol) = ''">
            <xsl:message terminate="yes">The ESP binding defined in '<xsl:value-of select="$espBindingInfo"/>' has no protocol!</xsl:message>
         </xsl:if>
         <xsl:value-of select="concat($bindingNode/@protocol, '://')"/>
      </xsl:if>
      <xsl:value-of select="concat($espIP, ':', $bindingNode/@port)"/>
   </xsl:template>
</xsl:stylesheet>
