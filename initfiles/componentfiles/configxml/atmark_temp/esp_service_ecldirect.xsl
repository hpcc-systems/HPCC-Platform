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
    <xsl:param name="espServiceName" select="'ecldirect'"/>


    <xsl:template match="text()"/>

    <xsl:variable name="serviceModuleType" select="'ecldirect'"/>

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

        <xsl:variable name="serviceType" select="'EclDirect'"/>
        <xsl:variable name="bindType" select="'EclDirectSoapBinding'"/>
        <xsl:variable name="servicePlugin">
            <xsl:choose>
                <xsl:when test="$isLinuxInstance">libEclDirect.so</xsl:when>
                <xsl:otherwise>EclDirect.dll</xsl:otherwise>
            </xsl:choose>
        </xsl:variable>

        <xsl:variable name="serviceName" select="concat('ecldirect', '_', @name, '_', $process)"/>
        <xsl:variable name="bindName" select="concat('ecldirect', '_', $bindingNode/@name, '_', $process)"/>

        <EspService name="{$serviceName}" type="{$serviceType}" plugin="{$servicePlugin}">
            <ClusterName><xsl:value-of select="@clusterName"/></ClusterName>
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
        <xsl:when test="$authMethod='htpasswd'">
          <Authenticate method="htpasswd">
            <xsl:attribute name="htpasswdFile"> <xsl:value-of select="$bindingNode/../Authentication/@htpasswdFile"/> </xsl:attribute>
          </Authenticate>
        </xsl:when>
      </xsl:choose>
    </xsl:template>

</xsl:stylesheet>
