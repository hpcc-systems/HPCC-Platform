<?xml version="1.0" encoding="UTF-8"?>
<!--
################################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
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

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

  <xsl:template name="validateLdapVaultReferences">
    <xsl:for-each select="/Environment/Software/LDAPServerProcess">
      <xsl:variable name="ldapServerName" select="@name"/>
      <xsl:variable name="ldapAdminVaultId" select="normalize-space(@ldapAdminVaultId)"/>
      <xsl:variable name="hpccAdminVaultId" select="normalize-space(@hpccAdminVaultId)"/>

      <xsl:if test="$ldapAdminVaultId != '' and not(/Environment/Software/vaults/authn[@name=$ldapAdminVaultId])">
        <xsl:message terminate="yes">LDAPServerProcess '<xsl:value-of select="$ldapServerName"/>' references ldapAdminVaultId '<xsl:value-of select="$ldapAdminVaultId"/>' which does not match any /Environment/Software/vaults/authn/@name.</xsl:message>
      </xsl:if>

      <xsl:if test="$hpccAdminVaultId != '' and not(/Environment/Software/vaults/authn[@name=$hpccAdminVaultId])">
        <xsl:message terminate="yes">LDAPServerProcess '<xsl:value-of select="$ldapServerName"/>' references hpccAdminVaultId '<xsl:value-of select="$hpccAdminVaultId"/>' which does not match any /Environment/Software/vaults/authn/@name.</xsl:message>
      </xsl:if>
    </xsl:for-each>
  </xsl:template>

  <xsl:template name="validateVaultTypeKind">
    <xsl:for-each select="/Environment/Software/vaults/*">
      <xsl:variable name="vaultName" select="@name"/>
      <xsl:variable name="vaultType" select="translate(normalize-space(@type), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz')"/>
      <xsl:variable name="vaultKind" select="translate(normalize-space(@kind), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz')"/>
      <xsl:variable name="isAkeyless" select="$vaultType='akeyless' or ($vaultType='' and $vaultKind='akeyless')"/>

      <xsl:if test="$vaultType='' and $vaultKind=''">
        <xsl:message terminate="yes">Vault '<xsl:value-of select="$vaultName"/>' is invalid: either type or kind must be specified.</xsl:message>
      </xsl:if>

      <xsl:if test="normalize-space(@url) = ''">
        <xsl:message terminate="yes">Vault '<xsl:value-of select="$vaultName"/>' is missing required attribute url.</xsl:message>
      </xsl:if>

      <xsl:if test="$vaultType != '' and not($vaultType='akeyless' or $vaultType='hashicorp' or $vaultType='kv-v1' or $vaultType='kv-v2')">
        <xsl:message terminate="yes">Vault '<xsl:value-of select="$vaultName"/>' has invalid type '<xsl:value-of select="@type"/>'. Allowed values are: akeyless, hashicorp, kv-v1, kv-v2.</xsl:message>
      </xsl:if>

      <!-- Match runtime behavior in jsecrets.cpp: only akeyless is a distinct provider; any other/non-empty type is treated as hashicorp-like. -->
      <xsl:if test="$vaultType='akeyless' and $vaultKind != '' and $vaultKind != 'akeyless'">
        <xsl:message terminate="yes">Vault '<xsl:value-of select="$vaultName"/>' has invalid type/kind combination: type='<xsl:value-of select="@type"/>' kind='<xsl:value-of select="@kind"/>'.</xsl:message>
      </xsl:if>

      <xsl:if test="$vaultType != '' and $vaultType != 'akeyless' and $vaultKind = 'akeyless'">
        <xsl:message terminate="yes">Vault '<xsl:value-of select="$vaultName"/>' has invalid type/kind combination: type='<xsl:value-of select="@type"/>' kind='<xsl:value-of select="@kind"/>'.</xsl:message>
      </xsl:if>

      <xsl:if test="$isAkeyless and normalize-space(@accessId) = ''">
        <xsl:message terminate="yes">Vault '<xsl:value-of select="$vaultName"/>' is missing required akeyless attribute accessId.</xsl:message>
      </xsl:if>

      <xsl:if test="$isAkeyless and normalize-space(@accessKey) = '' and normalize-space(@client-secret) = ''">
        <xsl:message terminate="yes">Vault '<xsl:value-of select="$vaultName"/>' must specify one of akeyless attributes accessKey or client-secret.</xsl:message>
      </xsl:if>

      <xsl:if test="$isAkeyless and normalize-space(@accessKey) != '' and normalize-space(@client-secret) != ''">
        <xsl:message terminate="yes">Vault '<xsl:value-of select="$vaultName"/>' cannot specify both akeyless attributes accessKey and client-secret.</xsl:message>
      </xsl:if>
    </xsl:for-each>
  </xsl:template>

  <xsl:template name="copyVaultsConfig">
    <xsl:call-template name="validateVaultTypeKind"/>
    <xsl:if test="/Environment/Software/vaults">
      <vaults>
        <xsl:copy-of select="/Environment/Software/vaults/@*"/>
        <xsl:for-each select="/Environment/Software/vaults/*">
          <xsl:copy>
            <xsl:copy-of select="@*"/>
            <xsl:copy-of select="node()"/>
          </xsl:copy>
        </xsl:for-each>
      </vaults>
    </xsl:if>
  </xsl:template>

</xsl:stylesheet>
