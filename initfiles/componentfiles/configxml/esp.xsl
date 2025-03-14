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

<?xml-stylesheet type="text/xsl" href="C:\Development\deployment\xmlenv\esp.xsl"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xml:space="default"
    xmlns:seisint="http://seisint.com" xmlns:exslt="http://exslt.org/common" exclude-result-prefixes="seisint exslt">

    <xsl:output method="xml" indent="yes" omit-xml-declaration="no" encoding="UTF-8"/>
    <!--xsl:template match="text()"/-->
    <xsl:param name="process" select="'esp'"/>
    <xsl:param name="instance" select="'2wd20'"/>
    <xsl:param name="isLinuxInstance" select="0"/>
    <xsl:param name="serviceFilesList" select="''"/>
    <xsl:param name="deployedFromDali" select="''"/>
    <xsl:param name="apos">'</xsl:param>


   <xsl:variable name="oldPathSeparator">
    <xsl:choose>
        <xsl:when test="$isLinuxInstance = 1">\:</xsl:when>
        <xsl:otherwise>/$</xsl:otherwise>
    </xsl:choose>
   </xsl:variable>


   <xsl:variable name="newPathSeparator">
    <xsl:choose>
        <xsl:when test="$isLinuxInstance = 1">/$</xsl:when>
        <xsl:otherwise>\:</xsl:otherwise>
    </xsl:choose>
   </xsl:variable>

    <xsl:variable name="break"><xsl:text>&#xa;</xsl:text></xsl:variable>
    <xsl:variable name="indent"><xsl:text>&#32;&#32;</xsl:text></xsl:variable>
    <xsl:variable name="espProcess" select="/Environment/Software/EspProcess[@name=$process]"/>
    <xsl:variable name="espBindingProtocol" select="/Environment/Software/EspProcess[@name=$process]/EspBinding/@protocol"/>
    <xsl:variable name="method" select="/Environment/Software/EspProcess[@name=$process]/EspBinding/@type"/>
    <xsl:variable name="controlPortSetting" select="/Environment/Software/EspProcess[@name=$process]/@controlPort"/>
    <xsl:variable name="controlPort">
        <xsl:choose>
            <xsl:when test="string($controlPortSetting) != ''"><xsl:value-of select="$controlPortSetting"/></xsl:when>
            <xsl:when test="string($espBindingProtocol) = 'https'">18010</xsl:when>
            <xsl:when test="string($espBindingProtocol) = 'http'">8010</xsl:when>
            <xsl:otherwise>0</xsl:otherwise><!-- the WSESPControl will not be in esp.xml if controlPort = 0-->
        </xsl:choose>
    </xsl:variable>

    <xsl:template match="/Environment">
        <xsl:copy>
            <Software>
                <xsl:apply-templates select="$espProcess"/>
                <xsl:copy-of select="/Environment/Software/Directories"/>
            </Software>
            <!--
            # Generated for configuration info. accessed by getGlobalConfig()
            -->
            <global>
                <expert>
                    <xsl:copy-of select="/Environment/Software/Globals/@* | /Environment/Software/Globals/*"/>
                </expert>
                <storage>
                    <xsl:copy-of select="/Environment/Software/RemoteStorage/*"/>
                    <xsl:copy-of select="/Environment/Software/Globals/storage/*"/>
                </storage>
                <xsl:copy-of select="/Environment/Hardware/cost"/>
                <xsl:copy-of select="/Environment/Software/tracing"/>
            </global>
        </xsl:copy>
    </xsl:template>

    <xsl:template match="/Environment/Software/EspProcess">
        <!-- note that this can only be @name=$process since it is guided by template
          for /Environment above -->
        <xsl:copy>
            <xsl:apply-templates select="@*[string(.) != '']"/>
            <xsl:variable name="computerName" select="Instance[@name=$instance]/@computer"/>
            <xsl:attribute name="computer"><xsl:value-of select="$computerName"/></xsl:attribute>
            <xsl:attribute name="directory">
               <xsl:value-of select="translate(Instance[@name=$instance]/@directory, $oldPathSeparator, $newPathSeparator)"/>
            </xsl:attribute>

            <xsl:call-template name="addEnvironmentInfo"/>

            <xsl:call-template name="addMetricsConfig"/>

            <xsl:if test="ESPCacheGroup[1]">
               <xsl:value-of disable-output-escaping="yes" select="$break" />
               <xsl:value-of disable-output-escaping="yes" select="$indent" />
               <xsl:value-of disable-output-escaping="yes" select="$indent" />
               <xsl:value-of disable-output-escaping="yes" select="$indent" />
               <ESPCache>
                  <xsl:for-each select="ESPCacheGroup">
                    <xsl:variable name="ecgid" select="current()/@id"/>
                    <xsl:variable name="ecgInitString" select="current()/@initString"/>
                    <xsl:value-of disable-output-escaping="yes" select="$break" />
                    <xsl:value-of disable-output-escaping="yes" select="$indent" />
                    <xsl:value-of disable-output-escaping="yes" select="$indent" />
                    <xsl:value-of disable-output-escaping="yes" select="$indent" />
                    <xsl:value-of disable-output-escaping="yes" select="$indent" />
                    <Group id="{$ecgid}" initString="{$ecgInitString}"/>
                  </xsl:for-each>
                  <xsl:value-of disable-output-escaping="yes" select="$break" />
                  <xsl:value-of disable-output-escaping="yes" select="$indent" />
                  <xsl:value-of disable-output-escaping="yes" select="$indent" />
                  <xsl:value-of disable-output-escaping="yes" select="$indent" />
               </ESPCache>
            </xsl:if>

            <xsl:if test="./Authentication/@method='ldap' or ./Authentication/@method='ldaps' or ./Authentication/@method='secmgrPlugin'">
              <xsl:value-of disable-output-escaping="yes" select="$break" />
              <xsl:value-of disable-output-escaping="yes" select="$indent" />
              <xsl:value-of disable-output-escaping="yes" select="$indent" />
              <xsl:value-of disable-output-escaping="yes" select="$indent" />
              <xsl:if test="AuthDomain[1]">
                <AuthDomains>
                  <xsl:for-each select="AuthDomain">
                    <xsl:value-of disable-output-escaping="yes" select="$break" />
                    <xsl:value-of disable-output-escaping="yes" select="$indent" />
                    <xsl:value-of disable-output-escaping="yes" select="$indent" />
                    <xsl:value-of disable-output-escaping="yes" select="$indent" />
                    <xsl:value-of disable-output-escaping="yes" select="$indent" />
                    <xsl:copy-of select="."/>
                  </xsl:for-each>
                  <xsl:value-of disable-output-escaping="yes" select="$break" />
                  <xsl:value-of disable-output-escaping="yes" select="$indent" />
                  <xsl:value-of disable-output-escaping="yes" select="$indent" />
                  <xsl:value-of disable-output-escaping="yes" select="$indent" />
                </AuthDomains>
              </xsl:if>
            </xsl:if>

            <xsl:for-each select="Authentication">
                <xsl:if test="@method='ldap' or @method='ldaps'">
                    <xsl:value-of disable-output-escaping="yes" select="$break" />
                    <xsl:value-of disable-output-escaping="yes" select="$indent" />
                    <xsl:value-of disable-output-escaping="yes" select="$indent" />
                    <xsl:value-of disable-output-escaping="yes" select="$indent" />
                    <xsl:call-template name="doLdapSecurity">
                        <xsl:with-param name="method" select="@method"/>
                        <xsl:with-param name="ldapServer" select="@ldapServer"/>
                        <xsl:with-param name="ldapConnections" select="@ldapConnections"/>
                        <xsl:with-param name="passwordExpirationWarningDays" select="@passwordExpirationWarningDays"/>
                        <xsl:with-param name="checkViewPermissions" select="@checkViewPermissions"/>
                        <xsl:with-param name="localDomain" select="/Environment/Hardware/Computer[@name=$computerName]/@domain"/>
                    </xsl:call-template>
                </xsl:if>
            </xsl:for-each>

            <xsl:if test="./Authentication/@method='secmgrPlugin'">
            <SecurityManagers>
                <xsl:for-each select="./EspBinding[@type != '']">
                    <xsl:if test="not(preceding-sibling::EspBinding/@type = current()/@type)">
                        <SecurityManager>
                            <xsl:variable name="instanceName" select="@type"/>
                            <xsl:if test="not(/Environment/Software/*[@name=$instanceName and @type='SecurityManager'])">
                                <xsl:message terminate="yes">Security Manager instance of name <xsl:value-of select="@type"/> is referenced in service <xsl:value-of select="@name"/> of ESP <xsl:value-of select="../@name"/> but does not exist"</xsl:message>
                            </xsl:if>
                            <xsl:attribute name="name">
                                <xsl:value-of select="/Environment/Software/*[@name=$instanceName and @type='SecurityManager']/@name"/>
                            </xsl:attribute>
                            <xsl:attribute name="instanceFactoryName">
                                <xsl:value-of select="/Environment/Software/*[@name=$instanceName and @type='SecurityManager']/@instanceFactoryName"/>
                            </xsl:attribute>
                            <xsl:attribute name="libName">
                                <xsl:value-of select="/Environment/Software/*[@name=$instanceName and @type='SecurityManager']/@libName"/>
                            </xsl:attribute>
                            <xsl:attribute name="type">
                                <xsl:value-of select="name(/Environment/Software/*[@name=$instanceName and @type='SecurityManager'])"/>
                            </xsl:attribute>
                            <xsl:copy-of select="/Environment/Software/*[@name=$instanceName and @type='SecurityManager']"/>
                        </SecurityManager>
                    </xsl:if>
                </xsl:for-each>
            </SecurityManagers>
           </xsl:if>

            <xsl:variable name="maxRequestEntityLength">
                <xsl:choose>
                    <xsl:when test="EspBinding[1]">
                        <xsl:apply-templates select="EspBinding[1]" mode="maxRequestEntityLength">
                            <xsl:with-param name="max" select="number(@maxRequestEntityLength)"/>
                        </xsl:apply-templates>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:value-of select="@maxRequestEntityLength"/>
                    </xsl:otherwise>
                </xsl:choose>
            </xsl:variable>
            <!-- insert http protocol, if being used for any binding and is not defined -->
            <xsl:if test="EspBinding[@protocol='http'] and not(EspProtocol[@name='http'])">
                <EspProtocol>
                    <xsl:attribute name="name">http</xsl:attribute>
                    <xsl:attribute name="type">http_protocol</xsl:attribute>
                    <xsl:attribute name="plugin">esphttp</xsl:attribute>
                    <xsl:attribute name="maxRequestEntityLength">
                        <xsl:value-of select="$maxRequestEntityLength"/>
                    </xsl:attribute>
                </EspProtocol>
            </xsl:if>
            <!-- insert https protocol, if a certificate has been specified for it-->
            <xsl:if test="EspBinding[@protocol='https']">
                <EspProtocol name="https" type="secure_http_protocol">
                    <xsl:attribute name="plugin">esphttp</xsl:attribute>
                    <xsl:attribute name="maxRequestEntityLength">
                        <xsl:value-of select="$maxRequestEntityLength"/>
                    </xsl:attribute>
                    <certificate>
                        <xsl:value-of select="HTTPS/@certificateFileName"/>
                    </certificate>
                    <privatekey>
                        <xsl:value-of select="HTTPS/@privateKeyFileName"/>
                    </privatekey>
                    <passphrase>
                        <xsl:value-of select="HTTPS/@passphrase"/>
                    </passphrase>
                    <cipherList>
                        <xsl:value-of select="HTTPS/@cipherList"/>
                    </cipherList>
                    <verify enable="{HTTPS/@enableVerification}" address_match="{HTTPS/@requireAddressMatch}" accept_selfsigned="{HTTPS/@acceptSelfSigned}">
                        <ca_certificates path="{HTTPS/@CA_Certificates_Path}"/>
                        <trusted_peers><xsl:value-of select="HTTPS/@trustedPeers"/></trusted_peers>
                    </verify>
                </EspProtocol>
            </xsl:if>
            <xsl:if test="string($controlPort) != '0'">
                <xsl:variable name="serviceName" select="concat('WSESPControl_', $process)"/>
                <xsl:variable name="servicePlugin">
                    <xsl:call-template name="makeServicePluginName">
                        <xsl:with-param name="plugin" select="'ws_espcontrol'"/>
                    </xsl:call-template>
                </xsl:variable>
                <xsl:variable name="bindName" select="concat('WSESPControl_Binding_', $process)"/>
                <xsl:value-of disable-output-escaping="yes" select="$break" />
                <xsl:value-of disable-output-escaping="yes" select="$indent" />
                <xsl:value-of disable-output-escaping="yes" select="$indent" />
                <xsl:value-of disable-output-escaping="yes" select="$indent" />
                <EspService name="{$serviceName}" type="WSESPControl" plugin="{$servicePlugin}"/>
                 <xsl:value-of disable-output-escaping="yes" select="$break" />
                <xsl:value-of disable-output-escaping="yes" select="$indent" />
                <xsl:value-of disable-output-escaping="yes" select="$indent" />
                <xsl:value-of disable-output-escaping="yes" select="$indent" />
                <EspBinding name="{$bindName}" service="{$serviceName}" protocol="{$espBindingProtocol}" type="ws_espcontrolSoapBinding" plugin="{$servicePlugin}" netAddress="0.0.0.0" port="{$controlPort}">
                    <xsl:if test="EspControlBinding">
                        <xsl:variable name="authNode" select="Authentication[1]"/>
                        <xsl:variable name="espControlBinding" select="EspControlBinding"/>
                        <xsl:call-template name="bindAuthentication">
                            <xsl:with-param name="bindingNode" select="$espControlBinding"/>
                            <xsl:with-param name="authMethod" select="$authNode/@method"/>
                        </xsl:call-template>
                    </xsl:if>
                </EspBinding>
            </xsl:if>
            <xsl:variable name="importedServiceDefinitionFiles">
                <xsl:call-template name="importServiceDefinitionFiles">
                    <xsl:with-param name="filesList" select="$serviceFilesList"/>
                </xsl:call-template>
            </xsl:variable>
            <xsl:apply-templates select="exslt:node-set($importedServiceDefinitionFiles)" mode="processImportedServiceDefinitions"/>
            <xsl:apply-templates select="node()"/>
        </xsl:copy>

    </xsl:template>

    <xsl:template match="/Environment/Software/EspProtocol">
        <xsl:variable name="protocolName" select="@name"/>
        <xsl:if test="../EspBinding[@protocol=$protocolName]">
            <xsl:apply-templates select="."/>
        </xsl:if>
    </xsl:template>

    <xsl:template name="importServiceDefinitionFiles">
        <xsl:param name="filesList"/>
        <xsl:if test="string($filesList) != ''">
            <xsl:variable name="fileName" select="substring-before($filesList, ';')"/>
            <xsl:call-template name="getServiceDefinition">
                <xsl:with-param name="serviceFileName" select="$fileName"/>
            </xsl:call-template>
            <xsl:call-template name="importServiceDefinitionFiles">
                <xsl:with-param name="filesList" select="substring-after($filesList, ';')"/>
            </xsl:call-template>
        </xsl:if>
    </xsl:template>

    <xsl:template name="getServiceDefinition">
        <xsl:param name="serviceFileName"/>
        <xsl:variable name="serviceFile" select="document(concat('file:///', translate($serviceFileName, '\', '/')))"/>
        <xsl:if test="not($serviceFile)">
            <xsl:message terminate="yes">ESP service definition file <xsl:value-of select="$serviceFileName"/> not found!</xsl:message>
        </xsl:if>
        <xsl:variable name="serviceFileNode" select="$serviceFile/Environment/Software/EspProcess/*"/>
        <xsl:if test="not($serviceFileNode)">
            <xsl:message terminate="yes">No ESP service definition found in <xsl:value-of select="$serviceFileName"/>!</xsl:message>
        </xsl:if>
        <xsl:copy-of select="$serviceFileNode"/>
    </xsl:template>


    <xsl:template match="EspBinding">
       <xsl:variable name="name" select="@name"/>
       <xsl:variable name="port" select="@port"/>
       <xsl:variable name="service" select="@service"/>
       <xsl:variable name="type" select="/Environment/Software/EspService[@name=$service]/Properties/@type"/>
       <xsl:variable name="defaultForPort" select="string(@defaultForPort)='true'"/>

       <xsl:if test="string($service)=''">
          <xsl:message terminate="yes">No service is specified for ESP binding '<xsl:value-of select="$name"/>'.</xsl:message>
       </xsl:if>
       <xsl:if test="string($port) = ''">
          <xsl:message terminate="yes">No port is specified for ESP binding '<xsl:value-of select="$name"/>'.</xsl:message>
       </xsl:if>
       <xsl:for-each select="preceding-sibling::EspBinding[@port=$port]">
           <xsl:variable name="type2" select="/Environment/Software/EspService[@name=current()/@service]/Properties/@type"/>
            <xsl:choose>
                <xsl:when test="$type2=$type">
                    <xsl:message terminate="yes">
                        <xsl:text>Port conflict in ESP binding '</xsl:text>
                        <xsl:value-of select="$name"/>
                        <xsl:text>' of '</xsl:text>
                        <xsl:value-of select="$process"/>
                        <xsl:text>'!  Port </xsl:text>
                        <xsl:value-of select="$port"/>
                        <xsl:text> is already bound to another service of the same type for binding '</xsl:text>
                        <xsl:value-of select="current()/@name"/>
                        <xsl:text>'.</xsl:text>
                    </xsl:message>
                </xsl:when>
                <xsl:when test="$defaultForPort and @defaultForPort!='false'">
                    <xsl:call-template name="message">
                        <xsl:with-param name="text">
                            <xsl:value-of select="concat('Multiple ESP bindings on port ', $port, ' of ESP ', $apos, $process, $apos)"/>
                            <xsl:text> are defined as default.  Last one would dictate root level access.</xsl:text>
                        </xsl:with-param>
                    </xsl:call-template>
                </xsl:when>
            </xsl:choose>
       </xsl:for-each>
    </xsl:template>


    <xsl:template match="EspBinding" mode="maxRequestEntityLength">
    <xsl:param name="max"/>
        <xsl:variable name="svcValue"
            select="number(/Environment/Software/EspService[@name=current()/@service]/@maxRequestEntityLength)"/>
        <xsl:variable name="newMax">
            <xsl:choose>
                <xsl:when test="$max &lt; $svcValue">
                    <xsl:value-of select="$svcValue"/>
                </xsl:when>
                <xsl:otherwise>
                    <xsl:value-of select="$max"/>
                </xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        <xsl:variable name="nextBinding" select="following-sibling::EspBinding[1]"/>
        <xsl:choose>
            <xsl:when test="$nextBinding">
                <xsl:apply-templates select="$nextBinding" mode="maxRequestEntityLength">
                    <xsl:with-param name="max" select="$newMax"/>
                </xsl:apply-templates>
            </xsl:when>
            <xsl:otherwise>
                <xsl:value-of select="$newMax"/>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>

    <!--don't produce in output -->
    <xsl:template match="EspProcess/Instance|EspProcess/HTTPS|ProtocolX"/>


    <!--don't produce in output -->
    <xsl:template match="@buildSet|@maxRequestEntityLength"/>

    <!--don't produce in output -->
    <xsl:template match="EspProcess/ESPCacheGroup"/>

    <!--don't produce in output -->
    <xsl:template match="EspProcess/AuthDomain"/>

    <!--don't produce in output -->
    <xsl:template match="EspProcess/EspControlBinding"/>

    <xsl:template match="/|@*|node()">
        <!--matches any attribute or child of any types-->
        <xsl:copy>
            <!--copy input to output-->
            <xsl:apply-templates select="@*|node()"/>
        </xsl:copy>
    </xsl:template>


    <xsl:template name="doLdapSecurity">
        <xsl:param name="method"/>
        <xsl:param name="ldapServer"/>
        <xsl:param name="ldapConnections"/>
        <xsl:param name="localDomain"/>
        <xsl:param name="passwordExpirationWarningDays"/>
        <xsl:param name="checkViewPermissions"/>
        <xsl:variable name="ldapServerNode" select="/Environment/Software/LDAPServerProcess[@name=$ldapServer]"/>
        <xsl:if test="not($ldapServerNode)">
           <xsl:message terminate="yes">LDAP server is either not specified or is invalid!</xsl:message>
        </xsl:if>
        <xsl:for-each select="$ldapServerNode">
            <xsl:element name="ldapSecurity">
                <xsl:attribute name="name">ldapserver</xsl:attribute>
                <xsl:attribute name="ldapProtocol"><xsl:value-of select="$method"/></xsl:attribute>
                <xsl:attribute name="localDomain"><xsl:value-of select="$localDomain"/></xsl:attribute>
                <xsl:attribute name="checkViewPermissions"><xsl:value-of select="$checkViewPermissions"/></xsl:attribute>
                <xsl:attribute name="maxConnections">
                   <xsl:choose>
                      <xsl:when test="string($ldapConnections) != ''"><xsl:value-of select="$ldapConnections"/></xsl:when>
                      <xsl:otherwise><xsl:value-of select="@maxConnections"/></xsl:otherwise>
                   </xsl:choose>
                </xsl:attribute>
                <xsl:attribute name="passwordExpirationWarningDays">
                    <xsl:choose>
                        <xsl:when test="string($passwordExpirationWarningDays) != ''">
                            <xsl:value-of select="$passwordExpirationWarningDays"/>
                        </xsl:when>
                        <xsl:otherwise>10</xsl:otherwise>
                    </xsl:choose>
                </xsl:attribute>
                <xsl:variable name="ldapAddress">
                   <xsl:for-each select="Instance[@name]">
                 <xsl:variable name="netAddress" select="/Environment/Hardware/Computer[@name=current()/@computer]/@netAddress"/>
                 <xsl:if test="string($netAddress) = ''">
                    <xsl:message terminate="yes">Invalid I.P. address for instance '<xsl:value-of select="@name"/>' of LDAP server '<xsl:value-of select="$ldapServer"/>'.</xsl:message>
                 </xsl:if>
                 <xsl:if test="position() > 1">|</xsl:if>
                 <xsl:value-of select="$netAddress"/>
                   </xsl:for-each>
                </xsl:variable>
                <xsl:if test="string($ldapAddress) != ''">
                    <xsl:attribute name="ldapAddress"><xsl:value-of select="$ldapAddress"/></xsl:attribute>
                </xsl:if>
                <xsl:for-each select="@*">
                    <xsl:choose>
                        <xsl:when test="name()='ldapConnections'"/>
                        <xsl:when test="name()='build'"/>
                        <xsl:when test="name()='buildSet'"/>
                        <xsl:when test="name()='name'"/>
                        <xsl:when test="name()='modulesBasedn' and string(.) != ''">
                            <xsl:attribute name="resourcesBasedn"><xsl:value-of select="."/></xsl:attribute>
                        </xsl:when>
                        <xsl:when test="name()='computer' and string(.) != '' and string($ldapAddress) = ''">
                            <xsl:attribute name="ldapAddress"><xsl:value-of select="/Environment/Hardware/Computer[@name=current()]/@netAddress"/></xsl:attribute>
                        </xsl:when>
                        <xsl:when test="string(.) != ''">
                            <xsl:copy/>
                        </xsl:when>
                        <xsl:otherwise/>
                    </xsl:choose>
                </xsl:for-each>
                <xsl:attribute name="useLegacyDefaultFileScopePermissionCache">
                    <xsl:value-of select="/Environment/Software/LDAPServerProcess[@name=$ldapServer]/@useLegacyDefaultFileScopePermissionCache"/>
                </xsl:attribute>
                <xsl:attribute name="ldapAdminSecretKey">
                    <xsl:value-of select="/Environment/Software/LDAPServerProcess[@name=$ldapServer]/@ldapAdminSecretKey"/>
                </xsl:attribute>
            </xsl:element>
        </xsl:for-each>
    </xsl:template>

    <xsl:template name="dohtpasswdSecurity">
        <xsl:param name="method"/>
        <xsl:param name="htpasswdFile"/>
        <xsl:element name="htpasswdSecurity">
            <xsl:attribute name="method"> <xsl:value-of select="$method"/> </xsl:attribute>
            <xsl:attribute name="htpasswdFile"> <xsl:value-of select="$htpasswdFile"/> </xsl:attribute>
        </xsl:element>
    </xsl:template>

    <xsl:template name="doAccurintSecurity">
        <xsl:param name="method"/>
        <xsl:param name="accurintSecurity"/>
        <xsl:param name="localDomain"/>

        <xsl:for-each select="/Environment/Software/AccurintServerProcess[@name=$accurintSecurity]">
            <xsl:element name="AccurintSecurity">
                <xsl:attribute name="name">accurintserver</xsl:attribute>
                <xsl:attribute name="localDomain"><xsl:value-of select="$localDomain"/></xsl:attribute>
                <xsl:for-each select="@*">
                    <xsl:choose>
                        <xsl:when test="name()='build'"/>
                        <xsl:when test="name()='buildSet'"/>
                        <xsl:when test="name()='name'"/>
                        <xsl:when test="string(.) != ''">
                            <xsl:copy/>
                        </xsl:when>
                        <xsl:otherwise/>
                    </xsl:choose>
                </xsl:for-each>
                <SafeIPList>
                     <xsl:for-each select="/Environment/Software/AccurintServerProcess[@name=$accurintSecurity]/SafeIPList">
                        <ip><xsl:value-of select="@ip"/></ip>
                     </xsl:for-each>
                 </SafeIPList>
                <xsl:for-each select="StoredProcedureMap">
                    <StoredProcedureMap>
                        <xsl:for-each select="@*">
                            <xsl:copy/>
                        </xsl:for-each>
                    </StoredProcedureMap>
                </xsl:for-each>
            </xsl:element>
        </xsl:for-each>
    </xsl:template>


    <xsl:template match="@daliServers">
        <xsl:variable name="daliServerName" select="."/>
        <xsl:attribute name="daliServers">
           <xsl:for-each select="/Environment/Software/DaliServerProcess[@name=$daliServerName]/Instance">
              <xsl:call-template name="getNetAddress">
                 <xsl:with-param name="computer" select="@computer"/>
              </xsl:call-template>
              <xsl:if test="string(@port) != ''">:<xsl:value-of select="@port"/>
              </xsl:if>
              <xsl:if test="position() != last()">, </xsl:if>
           </xsl:for-each>
        </xsl:attribute>
       <xsl:apply-templates select="@*|node()"/>
    </xsl:template>


    <xsl:template name="getNetAddress">
        <xsl:param name="computer"/>
        <xsl:value-of select="/Environment/Hardware/Computer[@name=$computer]/@netAddress"/>
    </xsl:template>


    <xsl:template name="makeServicePluginName">
        <xsl:param name="plugin"/>
        <xsl:choose>
            <xsl:when test="$isLinuxInstance"><xsl:value-of select="$plugin"/></xsl:when>
            <xsl:otherwise>
                <xsl:value-of select="$plugin"/>.dll</xsl:otherwise>
        </xsl:choose>
    </xsl:template>


     <xsl:template match="EspService" mode="processImportedServiceDefinitions">
         <xsl:variable name="name" select="@name"/>
         <xsl:if test="not(preceding-sibling::EspService[@name=$name])">
            <xsl:value-of disable-output-escaping="yes" select="$break" />
            <xsl:value-of disable-output-escaping="yes" select="$indent" />
            <xsl:value-of disable-output-escaping="yes" select="$indent" />
            <xsl:value-of disable-output-escaping="yes" select="$indent" />
            <xsl:copy>
               <xsl:apply-templates select="*|@*[string(.) != '']|text()" mode="processImportedServiceDefinitions"/>
            </xsl:copy>
         </xsl:if>
     </xsl:template>

     <xsl:template match="EspBinding" mode="processImportedServiceDefinitions">
         <xsl:if test="not(preceding-sibling::EspBinding[@name=current()/@name])">
            <xsl:value-of disable-output-escaping="yes" select="$break" />
            <xsl:value-of disable-output-escaping="yes" select="$indent" />
            <xsl:value-of disable-output-escaping="yes" select="$indent" />
            <xsl:value-of disable-output-escaping="yes" select="$indent" />
            <xsl:variable name="bindNode" select="."/>
            <!--note that the name of this generated binding is not same as the original binding name
              and is mangled in this format: "[service-name]_[binding-name]_[esp-name]".  Besides, the
              service name does not have to be same as @service in original esp binding, for instance,
              eclwatch is actually a collection of half a dozen services.  Find the matching binding
              that ends with mangled name "_[binding-name]_[esp-name]". -->
            <xsl:variable name="origBindName">
               <xsl:for-each select="$espProcess/EspBinding">
                  <xsl:variable name="mangledName" select="concat('_', @name, '_', $process)"/>
                  <xsl:if test="contains($bindNode/@name, $mangledName) and substring-after($bindNode/@name, $mangledName)=''">
                     <xsl:value-of select="@name"/>
                  </xsl:if>
               </xsl:for-each>
            </xsl:variable>
            <xsl:variable name="envBindNode" select="$espProcess/EspBinding[@name = $origBindName]"/>

            <xsl:copy>
               <xsl:apply-templates select="@*[string(.) != '']" mode="processImportedServiceDefinitions">
                <xsl:with-param name="envBindNode" select="$envBindNode"/>
               </xsl:apply-templates>

               <!--if the generated EspBinding failed to propagate @wsdlServiceAddress from the original binding then add it there -->
               <xsl:if test="$envBindNode"><!--found matching original binding node in the environment-->
                  <xsl:if test="string($bindNode/@wsdlServiceAddress) = ''">
                     <xsl:copy-of select="$envBindNode/@wsdlServiceAddress"/>
                  </xsl:if>
                  <xsl:if test="string($bindNode/@defaultServiceVersion) = ''">
                     <xsl:copy-of select="$envBindNode/@defaultServiceVersion"/>
                  </xsl:if>
               </xsl:if>

           <xsl:if test="$envBindNode/CustomBindingParameter">
                 <CustomBindingParameters>
                    <xsl:for-each select="$envBindNode/CustomBindingParameter">
                       <xsl:copy-of select="."/>
                    </xsl:for-each>
                 </CustomBindingParameters>
           </xsl:if>

               <!-- if the generated EspBinding/Authenticate is missing @workunitsBasedn then add it there -->
               <xsl:variable name="defaultWorkunitsBasedn">
                  <xsl:choose>
                     <xsl:when test="string($envBindNode/@workunitsBasedn) != ''"><xsl:value-of select="$envBindNode/@workunitsBasedn"/></xsl:when>
                     <xsl:otherwise>ou=workunits,ou=ecl</xsl:otherwise>
                  </xsl:choose>
               </xsl:variable>

               <xsl:for-each select="$bindNode/Authenticate">
                  <xsl:copy>
                     <xsl:apply-templates select="@*[string(.) != '']" mode="processImportedServiceDefinitions"/>
                     <xsl:if test="string(@workunitsBasedn)=''">
                        <xsl:attribute name="workunitsBasedn"><xsl:value-of select="$defaultWorkunitsBasedn"/></xsl:attribute>
                     </xsl:if>
                     <xsl:apply-templates select="* | text()" mode="processImportedServiceDefinitions"/>
                  </xsl:copy>
               </xsl:for-each>
               <xsl:apply-templates select="*[name() != 'Authenticate'] | text()" mode="processImportedServiceDefinitions"/>
            </xsl:copy>
         </xsl:if>
     </xsl:template>

        <xsl:template name="addEnvironmentInfo">
            <Environment>
                <xsl:if test="string($deployedFromDali)!=''">
                    <xsl:attribute name="daliAddress">
                        <xsl:value-of select="$deployedFromDali"/>
                    </xsl:attribute>
                </xsl:if>
                <!--get space delimited net addresses for all ECL watches in this environment-->
                <xsl:variable name="allEclWatches">
                    <xsl:apply-templates select="/Environment/Software/EspProcess" mode="EclWatch"/>
                </xsl:variable>
                <xsl:call-template name="printUniqueTokens">
                    <xsl:with-param name="s" select="$allEclWatches"/>
                    <xsl:with-param name="enclosingTagName" select="'EclWatch'"/>
                </xsl:call-template>
            </Environment>
        </xsl:template>

    <xsl:template name="addMetricsConfig">
        <xsl:copy-of select="/Environment/Software/metrics"/>
    </xsl:template>

     <xsl:template match="/Environment/Software/EspProcess" mode="EclWatch">
        <xsl:for-each select="EspBinding">
            <xsl:variable name="protocol" select="@protocol"/>
               <xsl:variable name="port" select="@port"/>
               <xsl:variable name="method" select="@type"/>
               <xsl:variable name="service" select="@service"/>
               <xsl:variable name="type" select="/Environment/Software/EspService[@name=$service]/Properties/@type"/>
               <xsl:if test="$type='WsSMC' and starts-with($protocol, 'http') and string($port)!=''">
                <xsl:for-each select="../Instance">
                    <xsl:value-of select="concat($protocol, '://')"/>
               <xsl:value-of select="/Environment/Hardware/Computer[@name=current()/@computer]/@netAddress"/>
               <xsl:text>:</xsl:text>
               <xsl:value-of select="$port"/>
               <xsl:text> </xsl:text>
                </xsl:for-each>
               </xsl:if>
        </xsl:for-each>
     </xsl:template>

     <xsl:template name="bindAuthentication">
        <xsl:param name="authMethod"/>
        <xsl:param name="bindingNode"/>
        <xsl:copy-of select="$bindingNode/cors"/>
        <xsl:choose>
           <xsl:when test="$authMethod='basic'">
              <Authenticate type="Basic" method="UserDefined">
                 <xsl:for-each select="$bindingNode/Authenticate[string(@path) != '']">
                    <Location path="{@path}"/>
                 </xsl:for-each>
              </Authenticate>
           </xsl:when>
           <xsl:when test="$authMethod='ldap' or $authMethod='ldaps'">
              <Authenticate method="LdapSecurity" config="ldapserver">
                 <xsl:copy-of select="$bindingNode/@resourcesBasedn"/>
                 <xsl:for-each select="$bindingNode/Authenticate[string(@path) != '']">
                    <Location path="{@path}" resource="{@resource}" access="{@access}" description="{@description}"/>
                 </xsl:for-each>
                 <xsl:for-each select="$bindingNode/AuthenticateFeature[@authenticate='Yes']">
                    <Feature name="{@name}" path="{@path}" resource="{@resource}" required="{@access}" description="{@description}"/>
                 </xsl:for-each>
                 <xsl:for-each select="$bindingNode/AuthenticateSetting[@include='Yes']">
                    <Setting path="{@path}" resource="{@resource}" description="{@description}"/>
                 </xsl:for-each>
              </Authenticate>
           </xsl:when>
           <xsl:when test="$authMethod='secmgrPlugin'">
              <Authenticate>
                 <xsl:attribute name="method">
                    <xsl:value-of select="$bindingNode/@type"/>
                 </xsl:attribute>
                 <xsl:copy-of select="$bindingNode/@resourcesBasedn"/>
                 <xsl:for-each select="$bindingNode/Authenticate[string(@path) != '']">
                    <Location path="{@path}" resource="{@resource}" access="{@access}" description="{@description}"/>
                 </xsl:for-each>

                 <xsl:for-each select="$bindingNode/AuthenticateFeature[@authenticate='Yes']">
                    <Feature name="{@name}" path="{@path}" resource="{@resource}" required="{@access}" description="{@description}"/>
                 </xsl:for-each>
                 <xsl:for-each select="$bindingNode/AuthenticateSetting[@include='Yes']">
                    <Setting path="{@path}" resource="{@resource}" description="{@description}"/>
                 </xsl:for-each>
              </Authenticate>
           </xsl:when>
        </xsl:choose>
     </xsl:template>

        <xsl:template name="printUniqueTokens">
            <xsl:param name="s"/><!--space delimited string of tokens with space as last char-->
            <xsl:param name="enclosingTagName"/>
            <xsl:if test="$s!=''">
                <xsl:variable name="token" select="substring-before($s, ' ')"/>
                <xsl:variable name="suffix" select="substring-after($s, ' ')"/>
                <xsl:choose>
                    <xsl:when test="$token=''"></xsl:when>
                    <xsl:when test="contains($suffix, $token)">
                        <xsl:call-template name="printUniqueTokens">
                            <xsl:with-param name="s" select="$suffix"/>
                            <xsl:with-param name="enclosingTagName" select="$enclosingTagName"/>
                        </xsl:call-template>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:element name="{$enclosingTagName}">
                            <xsl:value-of select="$token"/>
                        </xsl:element>
                    </xsl:otherwise>
                </xsl:choose>
            </xsl:if>
        </xsl:template>

        <xsl:template name="message">
            <xsl:param name="text"/>
            <xsl:choose>
                <xsl:when test="function-available('seisint:message')">
                <xsl:variable name="dummy" select="seisint:message($text)"/>
                </xsl:when>
                <xsl:otherwise>
                    <xsl:message terminate="no">
                        <xsl:value-of select="$text"/>
                    </xsl:message>
                </xsl:otherwise>
            </xsl:choose>
        </xsl:template>

     <xsl:template match="@defaultBinding" mode="processImportedServiceDefinitions">
        <xsl:param name="envBindNode"/>
            <xsl:if test="$envBindNode/@defaultForPort!='false'">
                <xsl:copy-of select="."/>
            </xsl:if>
     </xsl:template>
     <xsl:template match="*|@*|text()" mode="processImportedServiceDefinitions">
         <xsl:copy>
            <xsl:apply-templates select="@*|*|text()" mode="processImportedServiceDefinitions"/>
         </xsl:copy>
     </xsl:template>
</xsl:stylesheet>
