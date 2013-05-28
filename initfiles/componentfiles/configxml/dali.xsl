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
  <xsl:template match="text()"/>
  <xsl:param name="process" select="'dali'"/>
  <xsl:param name="isLinuxInstance" select="0"/>


  <xsl:variable name="oldPathSeparator">
    <xsl:choose>
      <xsl:when test="$isLinuxInstance = 1">'\:'</xsl:when>
      <xsl:otherwise>'/$'</xsl:otherwise>
    </xsl:choose>
  </xsl:variable>


  <xsl:variable name="newPathSeparator">
    <xsl:choose>
      <xsl:when test="$isLinuxInstance = 1">'/$'</xsl:when>
      <xsl:otherwise>'\:'</xsl:otherwise>
    </xsl:choose>
  </xsl:variable>


  <xsl:variable name="oldPathSeparator2">
    <xsl:choose>
      <xsl:when test="$isLinuxInstance = 1">'\:'</xsl:when>
      <xsl:otherwise>'/:'</xsl:otherwise>
    </xsl:choose>
  </xsl:variable>


  <xsl:variable name="newPathSeparator2">
    <xsl:choose>
      <xsl:when test="$isLinuxInstance = 1">'/$'</xsl:when>
      <xsl:otherwise>'\$'</xsl:otherwise>
    </xsl:choose>
  </xsl:variable>


  <xsl:variable name="bslash">
    <xsl:choose>
      <xsl:when test="$isLinuxInstance">/</xsl:when>
      <xsl:otherwise>\</xsl:otherwise>
    </xsl:choose>
  </xsl:variable>


  <xsl:variable name="daliServerNode" select="/Environment/Software/DaliServerProcess[@name=$process]"/>


  <xsl:template match="/">
    <xsl:if test="not($daliServerNode)">
      <xsl:message terminate="yes">
        Dali server '<xsl:value-of select="$process"/>' is undefined!
      </xsl:message>
    </xsl:if>
    <xsl:apply-templates select="$daliServerNode"/>
  </xsl:template>


  <xsl:template match="DaliServerProcess">
    <DALI>
      <xsl:attribute name="name">
        <xsl:value-of select="@name"/>
      </xsl:attribute> 
      <xsl:if test="string(@LogDir)!=''">
        <xsl:attribute name="log_dir">
          <xsl:value-of select="@LogDir"/>
        </xsl:attribute>
      </xsl:if>
      <xsl:if test="string(@AuditLogDir)!=''">
        <xsl:attribute name="auditlog_dir">
          <xsl:value-of select="@AuditLogDir"/>
        </xsl:attribute>
      </xsl:if>
      <xsl:if test="string(@dataPath) != ''">
        <xsl:attribute name="dataPath">
          <xsl:call-template name="makeAbsolutePath">
            <xsl:with-param name="path" select="@dataPath"/>
          </xsl:call-template>
        </xsl:attribute>
      </xsl:if>
      <xsl:copy-of select="/Environment/Software/Directories"/>  
      <xsl:element name="SDS">
        <xsl:attribute name="store">dalisds.xml</xsl:attribute>
        <xsl:attribute name="caseInsensitive">0</xsl:attribute>
        <xsl:attribute name="enableSysLog">
          <xsl:call-template name="outputBool">
            <xsl:with-param name="val" select="@enableSysLog"/>
          </xsl:call-template>
        </xsl:attribute>
        <xsl:attribute name="enableSNMP">
          <xsl:call-template name="outputBool">
            <xsl:with-param name="val" select="@enableSNMP"/>
          </xsl:call-template>
        </xsl:attribute>
        <xsl:attribute name="snmpSendWarnings">
          <xsl:call-template name="outputBool">
            <xsl:with-param name="val" select="@snmpSendWarnings"/>
          </xsl:call-template>
        </xsl:attribute>
        <xsl:attribute name="recoverFromIncErrors">
          <xsl:call-template name="outputBool">
            <xsl:with-param name="val" select="@recoverFromIncErrors"/>
          </xsl:call-template>
        </xsl:attribute>
        <xsl:copy-of select="@snmpErrorMsgLevel | @msgLevel | @lightweightCoalesce | @keepStores"/>
        <xsl:attribute name="lCIdlePeriod">
          <xsl:value-of select="@IdlePeriod"/>
        </xsl:attribute>
        <xsl:attribute name="lCIdleRate">
          <xsl:value-of select="@IdleRate"/>
        </xsl:attribute>
        <xsl:attribute name="lCMinTime">
          <xsl:value-of select="@MinTime"/>
        </xsl:attribute>
        <xsl:attribute name="lCQuietStartTime">
          <xsl:value-of select="@StartTime"/>
        </xsl:attribute>
        <xsl:attribute name="lCQuietEndTime">
          <xsl:value-of select="@EndTime"/>
        </xsl:attribute>
        <xsl:if test="string(@environment)!=''">
          <xsl:attribute name="environment">
            <xsl:value-of select="@environment"/>
          </xsl:attribute>
        </xsl:if>
        <xsl:if test="string(@backupComputer) != ''">
          <xsl:if test="string(@remoteBackupLocation) != ''">
            <xsl:attribute name="remoteBackupLocation">
              <xsl:value-of select="$bslash"/>
              <xsl:value-of select="$bslash"/>
              <xsl:value-of select="/Environment/Hardware/Computer[@name=current()/@backupComputer]/@netAddress"/>
              <xsl:variable name="backupDir" select="translate(@backupDirectory, $oldPathSeparator2, $newPathSeparator2)"/>
              <xsl:if test="not(starts-with($backupDir, $bslash))">
                <xsl:value-of select="$bslash"/>
              </xsl:if>
              <xsl:value-of select="$backupDir"/>
            </xsl:attribute>
          </xsl:if>
          <xsl:attribute name="backupComputer">
            <xsl:value-of select="/Environment/Hardware/Computer[@name=current()/@backupComputer]/@netAddress"/>
          </xsl:attribute>
        </xsl:if>
        <xsl:attribute name="asyncBackup">
          <xsl:call-template name="outputBool">
            <xsl:with-param name="val" select="@asyncBackup"/>
          </xsl:call-template>
        </xsl:attribute>
        <xsl:attribute name="useNFSBackupMount">
          <xsl:call-template name="outputBool">
            <xsl:with-param name="val" select="@useNFSBackupMount"/>
          </xsl:call-template>
        </xsl:attribute>
      </xsl:element>
      <DFS>
      <xsl:copy-of select="@forceGroupUpdate | @numThreads"/>
      </DFS>
      <xsl:element name="Coven">
        <xsl:attribute name="store">dalicoven.xml</xsl:attribute>
        <xsl:element name="Alerts">
          <xsl:element name="SNMPalert">
            <xsl:attribute name="type">Processor</xsl:attribute>
            <xsl:attribute name="trigger">
              <xsl:value-of select="@cpuAlertTrigger"/>
            </xsl:attribute>
            <xsl:attribute name="warning">
              <xsl:value-of select="@cpuAlertWarning"/>
            </xsl:attribute>
            <xsl:attribute name="minor">
              <xsl:value-of select="@cpuAlertMinor"/>
            </xsl:attribute>
            <xsl:attribute name="major">
              <xsl:value-of select="@cpuAlertMajor"/>
            </xsl:attribute>
            <xsl:attribute name="critical">
              <xsl:value-of select="@cpuAlertCritical"/>
            </xsl:attribute>
          </xsl:element>
          <xsl:element name="SNMPalert">
            <xsl:attribute name="type">VirtualMemory</xsl:attribute>
            <xsl:attribute name="warning">
              <xsl:value-of select="@memoryAlertWarning"/>
            </xsl:attribute>
            <xsl:attribute name="minor">
              <xsl:value-of select="@memoryAlertMinor"/>
            </xsl:attribute>
            <xsl:attribute name="major">
              <xsl:value-of select="@memoryAlertMajor"/>
            </xsl:attribute>
            <xsl:attribute name="critical">
              <xsl:value-of select="@memoryAlertCritical"/>
            </xsl:attribute>
          </xsl:element>
          <xsl:element name="SNMPalert">
            <xsl:attribute name="type">Disk1</xsl:attribute>
            <xsl:attribute name="warning">
              <xsl:value-of select="@disk1AlertWarning"/>
            </xsl:attribute>
            <xsl:attribute name="minor">
              <xsl:value-of select="@disk1AlertMinor"/>
            </xsl:attribute>
            <xsl:attribute name="major">
              <xsl:value-of select="@disk1AlertMajor"/>
            </xsl:attribute>
            <xsl:attribute name="critical">
              <xsl:value-of select="@disk1AlertCritical"/>
            </xsl:attribute>
          </xsl:element>
          <xsl:element name="SNMPalert">
            <xsl:attribute name="type">Disk2</xsl:attribute>
            <xsl:attribute name="warning">
              <xsl:value-of select="@disk2AlertWarning"/>
            </xsl:attribute>
            <xsl:attribute name="minor">
              <xsl:value-of select="@disk2AlertMinor"/>
            </xsl:attribute>
            <xsl:attribute name="major">
              <xsl:value-of select="@disk2AlertMajor"/>
            </xsl:attribute>
            <xsl:attribute name="critical">
              <xsl:value-of select="@disk2AlertCritical"/>
            </xsl:attribute>
          </xsl:element>
          <xsl:element name="SNMPalert">
            <xsl:attribute name="type">Threads</xsl:attribute>
            <xsl:attribute name="warning">
              <xsl:value-of select="@threadAlertWarning"/>
            </xsl:attribute>
            <xsl:attribute name="minor">
              <xsl:value-of select="@threadAlertMinor"/>
            </xsl:attribute>
            <xsl:attribute name="major">
              <xsl:value-of select="@threadAlertMajor"/>
            </xsl:attribute>
            <xsl:attribute name="critical">
              <xsl:value-of select="@threadAlertCritical"/>
            </xsl:attribute>
          </xsl:element>
        </xsl:element>
        <xsl:if test="string(@ldapServer) != ''">
          <xsl:element name="ldapSecurity">
            <xsl:copy-of select="@ldapProtocol | @authMethod | @maxConnections | @workunitsBasedn | @filesDefaultUser | @filesDefaultPassword"/>
            <xsl:variable name="ldapServerName" select="@ldapServer"/>
            <xsl:attribute name="filesBasedn">
                <xsl:value-of select="/Environment/Software/LDAPServerProcess[@name=$ldapServerName]/@filesBasedn"/>
            </xsl:attribute>
            <xsl:variable name="ldapServerNode" select="/Environment/Software/LDAPServerProcess[@name=$ldapServerName]"/>
            <xsl:if test="not($ldapServerNode)">
              <xsl:message terminate="yes">
                Invalid LDAP server process '<xsl:value-of select="$ldapServerName"/>'.
              </xsl:message>
            </xsl:if>
            <xsl:attribute name="ldapAddress">
              <xsl:choose>
                <xsl:when test="$ldapServerNode/Instance[1]">
                  <xsl:for-each select="$ldapServerNode/Instance[@name]">
                    <xsl:variable name="netAddress" select="/Environment/Hardware/Computer[@name=current()/@computer]/@netAddress"/>
                    <xsl:if test="string($netAddress) = ''">
                      <xsl:message terminate="yes">
                        Invalid I.P. address for instance '<xsl:value-of select="@name"/>' of LDAP server '<xsl:value-of select="$ldapServerName"/>'.
                      </xsl:message>
                    </xsl:if>
                    <xsl:if test="position() > 1">|</xsl:if>
                    <xsl:value-of select="$netAddress"/>
                  </xsl:for-each>
                </xsl:when>
                <xsl:otherwise>
                  <xsl:value-of select="/Environment/Hardware/Computer[@name=$ldapServerNode/@computer]/@netAddress"/>
                </xsl:otherwise>
              </xsl:choose>
            </xsl:attribute>
            <xsl:attribute name="checkScopeScans">
              <xsl:value-of select="@checkScopeScans"/>
            </xsl:attribute>

            <xsl:for-each select="$ldapServerNode">
              <xsl:copy-of select="@ldapPort | @ldapSecurePort | @cacheTimeout | @workunitsBasedn | @modulesBasedn | @systemBasedn | @systemCommonName | @systemUser | @systemPassword | @usersBasedn | @groupsBasedn"/>
            </xsl:for-each>
          </xsl:element>
        </xsl:if>
      </xsl:element>
    </DALI>
  </xsl:template>


  <xsl:template name="outputBool">
    <xsl:param name="val"/>
    <xsl:param name="default" select="0"/>
    <xsl:choose>
      <xsl:when test="$val='true'">1</xsl:when>
      <xsl:when test="$val='false'">0</xsl:when>
      <xsl:otherwise>
        <xsl:value-of select='$default'/>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>

  <xsl:template name="makeAbsolutePath">
    <xsl:param name="path"/>
    <xsl:variable name="osPath" select="translate($path, $oldPathSeparator, $newPathSeparator)"/>
    <xsl:choose>

      <xsl:when test="$isLinuxInstance">
        <xsl:if test="not(starts-with($osPath, '/'))">/</xsl:if>
        <xsl:value-of select="$osPath"/>
      </xsl:when>

      <xsl:otherwise>
        <xsl:variable name="osPath2">
          <!--define a variable that skips leading \, if present-->
          <xsl:choose>
            <xsl:when test="starts-with($osPath, '\')">
              <xsl:value-of select="substring($osPath, 2)"/>
            </xsl:when>
            <xsl:otherwise>
              <xsl:value-of select="$osPath"/>
            </xsl:otherwise>
          </xsl:choose>
        </xsl:variable>
        <xsl:if test="not(string-length($osPath2) >= 2) or (substring($osPath2, 2, 1) != ':')">
          <xsl:message terminate="yes">
            The path '<xsl:value-of select="$path"/>' needs to be an absolute path on the system.
          </xsl:message>
        </xsl:if>
        <xsl:value-of select="$osPath2"/>
      </xsl:otherwise>

    </xsl:choose>
  </xsl:template>

</xsl:stylesheet>
