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

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xml:space="default" 
xmlns:seisint="http://seisint.com"  xmlns:set="http://exslt.org/sets" exclude-result-prefixes="seisint set">
    <xsl:output method="xml" indent="yes" omit-xml-declaration="no" encoding="UTF-8"/>
    <xsl:param name="process" select="'esp_dermot'"/>
    <xsl:param name="instance" select="'dermot'"/>
    <xsl:param name="isLinuxInstance" select="0"/>
    <xsl:param name="espServiceName" select="'wsaccurint'"/>
    <xsl:param name="outputFilePath" select="'c:\development\deployment\xmlenv\wsattributes.xml'"/>
    <xsl:template match="text()"/>
    <xsl:variable name="espNode" select="/Environment/Software/EspProcess[@name=$process]"/>
    <xsl:template match="/Environment">
        <Environment>
            <Software>
                <EspProcess>
                    <xsl:apply-templates select="$espNode"/>
                </EspProcess>
            </Software>
        </Environment>
    </xsl:template>


    <xsl:template match="EspProcess">
        <xsl:apply-templates select="EspBinding">
            <xsl:with-param name="authNode" select="Authentication[1]"/>
        </xsl:apply-templates>
    </xsl:template>


    <xsl:template match="EspBinding">
        <xsl:param name="authNode"/>
        <xsl:variable name="serviceModuleName" select="@service"/>
        <xsl:apply-templates select="/Environment/Software/EspService[@name=$serviceModuleName and @name=$espServiceName]">
            <xsl:with-param name="bindingNode" select="."/>
            <xsl:with-param name="authNode" select="$authNode"/>
        </xsl:apply-templates>
    </xsl:template>


    <xsl:template match="EspService">
        <xsl:param name="bindingNode"/>
        <xsl:param name="authNode"/>
        <xsl:variable name="serviceType" select="Properties/@type"/>
        <xsl:variable name="serviceName" select="concat($serviceType, '_', @name, '_', $process)"/>
        <xsl:variable name="bindName" select="concat($serviceType, '_', $bindingNode/@name, '_', $process)"/>
        <xsl:variable name="bindType">
            <xsl:variable name="protocolBasedBindingType" select="Properties/Binding[@protocol=$bindingNode/@protocol]/@type"/>
            <xsl:choose>
                <xsl:when test="Properties/@bindingType='WsRiskWiseXBinding'">
                    <xsl:choose>
                        <xsl:when test="$bindingNode/@protocol='protocolx'">
                            <xsl:text>WsRiskWiseXBinding</xsl:text>
                        </xsl:when>
                        <xsl:otherwise>
                            <xsl:text>WsRiskWiseSoapBinding</xsl:text>
                        </xsl:otherwise>
                    </xsl:choose>
                </xsl:when>
                <xsl:otherwise>
                    <xsl:choose>
                        <xsl:when test="string($protocolBasedBindingType)!=''">
                            <xsl:value-of select="$protocolBasedBindingType"/>
                        </xsl:when>
                        <xsl:otherwise>
                            <xsl:value-of select="Properties/@bindingType"/>
                        </xsl:otherwise>
                    </xsl:choose>
                </xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        <xsl:variable name="servicePlugin">
            <xsl:call-template name="defineServicePlugin">
                <xsl:with-param name="plugin" select="Properties/@plugin"/>
            </xsl:call-template>
        </xsl:variable>
        
        <EspService name="{$serviceName}" type="{$serviceType}" plugin="{$servicePlugin}">
            <xsl:call-template name="processServiceSpecifics">
                <xsl:with-param name="serviceType" select="$serviceType"/>
            </xsl:call-template>
        </EspService>
        <EspBinding name="{$bindName}" service="{$serviceName}" protocol="{$bindingNode/@protocol}" type="{$bindType}" plugin="{$servicePlugin}" netAddress="0.0.0.0" port="{$bindingNode/@port}" defaultBinding="true">
            <xsl:call-template name="bindAuthentication">
                <xsl:with-param name="bindingNode" select="$bindingNode"/>
                <xsl:with-param name="authMethod" select="$authNode/@method"/>
                <xsl:with-param name="service" select="Properties/@type"/>
            </xsl:call-template>
        </EspBinding>
    </xsl:template>


    <xsl:template match="WsLogListener">
        <listener name="{@name}" type="{@type}">
            <xsl:if test="string(@server) != ''">
                <server>
                    <xsl:value-of select="@server"/>
                </server>
            </xsl:if>
            <xsl:if test="string(@dbName) != ''">
                <dbName>
                    <xsl:value-of select="@dbName"/>
                </dbName>
            </xsl:if>
            <xsl:if test="string(@userID) != ''">
                <userID>
                    <xsl:value-of select="@userID"/>
                </userID>
            </xsl:if>
            <xsl:if test="string(@password) != ''">
                <passWord>
                    <xsl:value-of select="@password"/>
                </passWord>
            </xsl:if>
            <xsl:copy-of select="fieldmap"/>
        </listener>
    </xsl:template>
    <xsl:template name="WsArchiveDatabase">
        <archive-database>
            <xsl:if test="string(@server) != ''">
                <server>
                    <xsl:value-of select="@server"/>
                </server>
            </xsl:if>
            <xsl:if test="string(@dbName) != ''">
                <dbName>
                    <xsl:value-of select="@dbName"/>
                </dbName>
            </xsl:if>
            <xsl:if test="string(@userID) != ''">
                <userID>
                    <xsl:value-of select="@userID"/>
                </userID>
            </xsl:if>
            <xsl:if test="string(@password) != ''">
                <passWord>
                    <xsl:value-of select="@password"/>
                </passWord>
            </xsl:if>
        </archive-database>
    </xsl:template>


    <!-- Utility templates -->
    <xsl:template name="bindAuthentication">
        <xsl:param name="bindingNode"/>
        <xsl:param name="authMethod"/>
        <xsl:param name="service"/>
        <xsl:choose>
            <!--xsl:when test="$authMethod='basic' or $service='WsAttributes'"--> <!--#37316-->
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
                    <xsl:copy-of select="$bindingNode/@resourcesBasedn"/>
                    <!--if binding has an ldap resourcebasedn specified then copy it out -->
                    <xsl:copy-of select="$bindingNode/@workunitsBasedn"/>
                    <!--if binding has an ldap workunitsbasedn specified then copy it out -->
                    <xsl:for-each select="$bindingNode/Authenticate[@path='/']">
                        <Location path="/" resource="{@resource}" required="{@access}" description="{@description}"/>
                    </xsl:for-each>
                    <xsl:for-each select="$bindingNode/AuthenticateFeature[@authenticate='Yes']">
                        <xsl:if test="@service=$service">
                            <Feature name="{@name}" path="{@path}" resource="{@resource}" required="{@access}" description="{@description}"/>
                        </xsl:if>
                    </xsl:for-each>
                </Authenticate>
            </xsl:when>
            <xsl:when test="$authMethod='accurint'">
                <Authenticate method="AccurintSecurity" config="accurintserver">
                    <xsl:for-each select="$bindingNode/Authenticate[@path='/']">
                        <Location path="/" objectclass="{@objectclass}" resource="{@resource}" required="{@access}" description="{@description}"/>
                    </xsl:for-each>
                    <xsl:for-each select="$bindingNode/AuthenticateFeature[@authenticate='Yes']">
                        <xsl:if test="$service='ws_adl' or @service=$service">
                            <Feature name="{@name}" path="{@path}" objectclass="{@objectclass}" resource="{@resource}" required="{@access}" description="{@description}"/>
                        </xsl:if>
                    </xsl:for-each>
                    <xsl:for-each select="$bindingNode/AuthenticateSetting">
                        <xsl:if test="$service='ws_adl' or @service=$service">
                            <Setting path="{@path}" service="{@service}" objectclass="{@objectclass}" resource="{@resource}" mapping="{@mapping}" userprop="{@userprop}" description="{@description}"/>
                        </xsl:if>
                    </xsl:for-each>
                </Authenticate>
            </xsl:when>
            <xsl:when test="$authMethod='accurintaccess'">
                <Authenticate method="AccurintAccess" config="accurintserver">
                <xsl:for-each select="$bindingNode/Authenticate[@path='/']">
                    <Location path="/" objectclass="{@objectclass}"  resource="{@resource}" required="{@access}" description="{@description}"/>
                </xsl:for-each>
                <xsl:for-each select="$bindingNode/AuthenticateFeature[@authenticate='Yes']">
                    <xsl:if test="$service='ws_adl' or @service=$service">
                        <Feature name="{@name}" path="{@path}" objectclass="{@objectclass}" resource="{@resource}" required="{@access}" description="{@description}"/>
                    </xsl:if>
                </xsl:for-each>
                </Authenticate>
            </xsl:when>
            <xsl:when test="$authMethod='remotens'">
                <Authenticate method="RemoteNSSecurity" config="accurintserver">
                    <xsl:for-each select="$bindingNode/Authenticate[@path='/']">
                        <Location path="/" objectclass="{@objectclass}" resource="{@resource}" required="{@access}" description="{@description}"/>
                    </xsl:for-each>
                    <xsl:for-each select="$bindingNode/AuthenticateFeature[@authenticate='Yes']">
                        <xsl:if test="$service='ws_adl' or @service=$service">
                            <Feature name="{@name}" path="{@path}" objectclass="{@objectclass}" resource="{@resource}" required="{@access}" description="{@description}"/>
                        </xsl:if>
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


    <xsl:template name="processServiceSpecifics">
        <xsl:param name="serviceType"/>
        <xsl:choose>
            <xsl:when test="$serviceType='WsAccurint'">
                <xsl:attribute name="subservices">wsaccurint_subservices.xml</xsl:attribute>
            </xsl:when>
            <xsl:when test="$serviceType='WsIdentity'">
                <xsl:attribute name="subservices">wsidentity_subservices.xml</xsl:attribute>
            </xsl:when>
            <xsl:when test="$serviceType='WsPrism'">
                <xsl:copy-of select="@FCRA"/>
            </xsl:when>
        </xsl:choose>
        <xsl:copy-of select="@maxWaitSecondsAllowed[.!='']"/>
        <xsl:copy-of select="@DefaultDataRestrictionMask[.!='']"/>
        <xsl:copy-of select="@adlLogDirectory[.!='']"/>
        <xsl:copy-of select="@FeatureFlags[.!='']"/>
        <xsl:choose>
           <xsl:when test="$serviceType='DynamicESDL'">
                <xsl:element name="ESDL">
                    <xsl:attribute name="service">
                        <xsl:value-of select="@esdlservice" />
                    </xsl:attribute>

                    <xsl:element name="XMLFile">
                        <xsl:value-of select="@XMLFile" />
                    </xsl:element>

                    <xsl:element name="Methods">
                        <xsl:for-each select="./MethodConfiguration">
                            <Method name="{@name}" queryname="{@queryname}" querytype="{@querytype}" url="{@url}"  username="{@username}" password="{@password}" />
                        </xsl:for-each>
                    </xsl:element>
                </xsl:element>
                <Gateways>
                    <xsl:for-each select="Gateways">
						<Gateway name="{@name}" url="{@url}"  username="{@username}" password="{@password}" />
                    </xsl:for-each>
                </Gateways>
            </xsl:when>

            <xsl:when test="$serviceType='WsAutoUpdate'">
                <xsl:for-each select="@path|@downloadUrl|@successUrl|@errorUrl|@daysValid">
                    <xsl:if test="string(.) != ''">
                        <xsl:copy-of select="."/>
                    </xsl:if>
                </xsl:for-each>
                <xsl:copy-of select="data"/>
            </xsl:when>
            <xsl:when test="$serviceType='WsLogService'">
                <xsl:for-each select="listener">
                    <xsl:apply-templates select="../../WsLogListener[@name=current()/@name]"/>
                </xsl:for-each>
            </xsl:when>
            <xsl:when test="$serviceType='WsArchive'">
                <xsl:call-template name="WsArchiveDatabase"/>
            </xsl:when>
            <xsl:when test="$serviceType='WsFcicReport'">
                <xsl:copy-of select="@url[string(.) != '']"/>
                <xsl:copy-of select="@soap_url[string(.) != '']"/>
                <xsl:call-template name="processLoggingClient"/>
                <xsl:copy-of select="localCache"/>
                <CompReportType>
                    <xsl:value-of select="@CompReportType"/>
                </CompReportType>
                <secure>
                    <xsl:value-of select="@secure"/>
                </secure>
                <webuser>
                    <userid>
                        <xsl:value-of select="@webusername"/>
                    </userid>
                    <password>
                        <xsl:value-of select="@webpassword"/>
                    </password>
                </webuser>
            </xsl:when>
            <xsl:when test="$serviceType='WsZipResolver'">
                <xsl:copy-of select="data"/>
            </xsl:when>
            <xsl:when test="$serviceType='WsJabber'">
                <xsl:copy-of select="JabberServer"/>
            </xsl:when>
            <xsl:when test="$serviceType='WsRemoteNS'">
                <RemoteNSdb>
                    <xsl:copy-of select="database"/>
                </RemoteNSdb>
            </xsl:when>
            <xsl:when test="$serviceType='WsRemoteNSReplication'">
                <ReplicationInfo>
                    <Type>
                        <xsl:value-of select="@Type"/>
                    </Type>
                    <Path>
                        <xsl:value-of select="@Path"/>
                    </Path>
                    <Timer>
                        <xsl:value-of select="@Timer"/>
                    </Timer>
                    <RemoteNSdb>
                        <xsl:copy-of select="database"/>
                    </RemoteNSdb>
                </ReplicationInfo>
            </xsl:when>
            <xsl:when test="$serviceType='WsFcicQuery'">
                <xsl:call-template name="processLoggingClient"/>
                <moxies>
                    <xsl:copy-of select="moxie"/>
                </moxies>
            </xsl:when>
            <xsl:when test="$serviceType='WsFcicEclQuery'">
                <xsl:call-template name="processLoggingClient"/>
                <ECL>
                    <xsl:copy-of select="eclFunction"/>
                </ECL>
                <Roxie>
                    <xsl:copy-of select="roxieFunction"/>
                </Roxie>
            </xsl:when>
            <xsl:when test="$serviceType='WsSNM'">
                <xsl:call-template name="processLoggingClient"/>
                <secure>
                    <xsl:value-of select="@secure"/>
                </secure>
                <xsl:if test="not(MoxieServer[1])">
                    <xsl:message terminate="yes">No Moxie server specified for WsMoxie service.</xsl:message>
                </xsl:if>
                <xsl:call-template name="processWsMoxie">
                    <xsl:with-param name="serviceType" select="$serviceType"/>
                </xsl:call-template>
                <imageServers>
                    <xsl:copy-of select="imageServer"/>
                </imageServers>
            </xsl:when>
            <xsl:when test="$serviceType='WsReferenceTracker'">
                <db>
                    <xsl:if test="string(@server) != ''">
                        <server>
                            <xsl:value-of select="@server"/>
                        </server>
                    </xsl:if>
                    <xsl:if test="string(@dbname) != ''">
                        <dbname>
                            <xsl:value-of select="@dbname"/>
                        </dbname>
                    </xsl:if>
                    <xsl:if test="string(@username) != ''">
                        <username>
                            <xsl:value-of select="@username"/>
                        </username>
                    </xsl:if>
                    <xsl:if test="string(@password) != ''">
                        <password>
                            <xsl:value-of select="@password"/>
                        </password>
                    </xsl:if>
                </db>
                <fieldmap>
                    <fields>
                        <field name="date_added" type="command"/>
                        <field name="userid" type="string"/>
                        <field name="domain" type="string"/>
                        <field name="referencecode" type="string"/>
                        <field name="casename" type="string"/>
                        <field name="crimecategory" type="number"/>
                        <field name="comment" type="blob"/>
                    </fields>
                </fieldmap>
            </xsl:when>
            <xsl:when test="$serviceType='WsDataAccess'">
                <db>
                    <xsl:if test="@server">
                        <server>
                            <xsl:value-of select="@server"/>
                        </server>
                    </xsl:if>
                    <xsl:if test="@dbname">
                        <dbname>
                            <xsl:value-of select="@dbname"/>
                        </dbname>
                    </xsl:if>
                    <xsl:if test="@username">
                        <username>
                            <xsl:value-of select="@username"/>
                        </username>
                    </xsl:if>
                    <xsl:if test="@password">
                        <password>
                            <xsl:value-of select="@password"/>
                        </password>
                    </xsl:if>
                </db>
            </xsl:when>
            <xsl:when test="$serviceType='WsAccurintAuthentication'">
                <xsl:copy-of select="database"/>
                <resources>
                    <resourceList name="rl_root">
                        <resource name="fdle_criminal"/>
                        <resource name="Access DL Images"/>
                        <resource name="Access Criminal History"/>
                        <resource name="Access Relavint"/>
                        <resource name="access_socialnet"/>
                        <resource name="specialized"/>
                        <resource name="dl_images"/>
                        <resource name="access_socialnet"/>
                        <resource name="AllowAdvancedGIS"/>
                        <resource name="AllowAdvancedSexOffender"/>
                        <resource name="AllowPhonesPlus"/>
                        <resource name="AllowPictometryData"/>
                        <resource name="AllowPictometryGateway"/>
                        </resourceList>
                </resources>
            </xsl:when>
            <xsl:when test="$serviceType='ws_accurintAccess'">
                <xsl:if test="string(@reportServer) != ''">
                    <xsl:variable name="reportserver" select="@reportServer"/>
                    <xsl:for-each select="/Environment/Software/AccurintServerProcess[@name=$reportserver]">
                    <xsl:element name="ReportServer">
                        <xsl:attribute name="name"><xsl:value-of select="$reportserver"/></xsl:attribute>
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
                    </xsl:element>
                    </xsl:for-each>
                </xsl:if>
            </xsl:when>
            <xsl:when test=" $serviceType='wsadlengine'">
                <xsl:if test="@configuration='local'">
                    <!-- RemoteNScfg -->
                    <xsl:call-template name="processRemoteNScfg">
                        <xsl:with-param name="serviceType" select="$serviceType"/>
                    </xsl:call-template>
                </xsl:if>
                <xsl:if test="@configuration='database'">
                    <!-- RemoteNSdb-->
                    <xsl:call-template name="processRemoteNSdb">
                        <xsl:with-param name="serviceType" select="$serviceType"/>
                    </xsl:call-template>
                </xsl:if>
                <xsl:call-template name="processMySQLdbCfg">
                    <xsl:with-param name="dbconfig" select="ADLCachedb"/>
                </xsl:call-template>
            </xsl:when>
            <xsl:when test="$serviceType='ws_ias'">
                <xsl:if test="@configuration='local'">
                    <xsl:call-template name="processRemoteNScfg">
                        <xsl:with-param name="serviceType" select="$serviceType"/>
                    </xsl:call-template>
                </xsl:if>
                <xsl:if test="@configuration='database'">
                    <xsl:call-template name="processRemoteNSdb">
                        <xsl:with-param name="serviceType" select="$serviceType"/>
                    </xsl:call-template>
                </xsl:if>
                <xsl:call-template name="processLoggingClient"/>
                <xsl:if test="string(@roxieAddress) != ''">
                    <RoxieAddress>
                        <xsl:value-of select="@roxieAddress"/>
                    </RoxieAddress>
                </xsl:if>
                <TransferServers>
                    <xsl:call-template name="getImageTransferServers">
                        <xsl:with-param name="espName" select="@espImageTransfer"/>
                    </xsl:call-template>
                </TransferServers>
            </xsl:when>
            <xsl:when test="$serviceType='ws_its'">
                <xsl:if test="@configuration='local'">
                    <xsl:call-template name="processRemoteNScfg">
                        <xsl:with-param name="serviceType" select="$serviceType"/>
                    </xsl:call-template>
                </xsl:if>
                <xsl:if test="@configuration='database'">
                    <xsl:call-template name="processRemoteNSdb">
                        <xsl:with-param name="serviceType" select="$serviceType"/>
                    </xsl:call-template>
                </xsl:if>
                <xsl:if test="string(@espImageAccess) = ''">
                    <xsl:message terminate="yes">No image access server specified for image transfer service.</xsl:message>
                </xsl:if>
                <xsl:call-template name="processLoggingClient"/>
                <xsl:if test="string(@roxieAddress) != ''">
                    <RoxieAddress>
                        <xsl:value-of select="@roxieAddress"/>
                    </RoxieAddress>
                </xsl:if>
                <AccessServers>
                    <xsl:call-template name="getImageAccessServers">
                        <xsl:with-param name="espName" select="@espImageAccess"/>
                    </xsl:call-template>
                </AccessServers>
            </xsl:when>
            <xsl:when test="$serviceType='WsAttributes'">
                <!--Note that plugins.xsl already validated that @eclServer is specified-->
                <xsl:variable name="eclServerNode" select="/Environment/Software/EclServerProcess[@name=current()/@eclServer]"/>
                <xsl:variable name="mySqlServer" select="string($eclServerNode/@MySQL)"/>
                 <xsl:if test="$mySqlServer = ''">
                    <xsl:message terminate="yes">WsAttributes: No MySQL server is defined for the specified ECL server!</xsl:message>
                </xsl:if>
                <xsl:variable name="mySqlServerComputer" select="/Environment/Software/MySQLProcess[@name=$mySqlServer]/@computer"/>
                <xsl:variable name="mySqlServerIP" select="/Environment/Hardware/Computer[@name=$mySqlServerComputer]/@netAddress"/>
                <xsl:if test="string($mySqlServerComputer) = '' or string($mySqlServerIP) = ''">
                    <xsl:message terminate="yes">Invalid MySQL server specified for WsAttributes service</xsl:message>
                </xsl:if>
                <xsl:if test="string($eclServerNode/@dbUser) = '' or string($eclServerNode/@dbPassword) = ''">
                    <xsl:message terminate="yes">WsAttributes: MySQL userid or password not specified for the specified ECL server!</xsl:message>
                </xsl:if>
                <xsl:if test="string($eclServerNode/@repository) = ''">
                    <xsl:message terminate="yes">WsAttributes: No repository specified for the specified ECL server!</xsl:message>
                </xsl:if>
                <Mysql server="{$mySqlServerIP}" repository="{$eclServerNode/@repository}" user="{$eclServerNode/@dbUser}" password="{$eclServerNode/@dbPassword}" poolSize="{@poolSize}" waitTimeout="{@waitTimeout}"/>
                <xsl:if test="string(@viewTimeout) != ''">
                    <ViewTimeout>
                        <xsl:value-of select="@viewTimeout"/>
                    </ViewTimeout>
                </xsl:if>
                <xsl:if test="string(@useAttributeTypes) != ''">
                    <UseAttributeTypes>
                          <xsl:value-of select="@useAttributeTypes"/>
                    </UseAttributeTypes>
                </xsl:if>
                <xsl:variable name="outputPath">
                    <xsl:call-template name="GetPathName">
                        <xsl:with-param name="path" select="translate($outputFilePath, '/$', '\:')"/>
                    </xsl:call-template>
                </xsl:variable>
                <xsl:variable name="pluginsFilePath" select="concat('file:///', $outputPath, $espServiceName, '_plugins.xml')"/>
                <xsl:variable name="pluginsRoot" select="document($pluginsFilePath)"/>
                <xsl:if test="not($pluginsRoot)">
                    <xsl:message terminate="yes">The plugins file '<xsl:value-of select="$pluginsFilePath"/>' was either not generated or failed to open!</xsl:message>
                </xsl:if>
                <xsl:variable name="pluginsNodes" select="$pluginsRoot/Plugins/Plugin/@destName"/>
                <xsl:if test="not(function-available('set:distinct'))">
                    <xsl:message terminate="yes">This XSL transformation can only be run by an XSLT processor supporting exslt sets!</xsl:message>
                </xsl:if>
                <Plugins>
                    <xsl:attribute name="path"><xsl:value-of select="$eclServerNode/@pluginsPath"/></xsl:attribute>
                </Plugins>
                <!--BUG: 9141 - Special repository OUs should be set apart in display 
                Copy ECL server's ldap configuration verbatim here.  Note that plugins.xsl already validated 
                that an ECL server is specified-->
                <xsl:variable name="eclAuthNode" select="$eclServerNode/ldapSecurity"/>
                <xsl:variable name="espAuthNode" select="$espNode/Authentication"/>
                <xsl:for-each select="$eclAuthNode">
                    <xsl:element name="{local-name()}"><!--ignore namespace-->
                        <xsl:copy-of select="@*[name()!='server' and string(.) != '']"/>
                    </xsl:element>
                </xsl:for-each>
            </xsl:when>
            <xsl:when test="$serviceType='WsMoxie' or $serviceType='WsMatrix'">
                <xsl:variable name="serviceSubType" select="Properties/@subType"/>
                <xsl:if test="not(MoxieServer[1])">
                    <xsl:message terminate="yes">No Moxie server specified for WsMoxie service.</xsl:message>
                </xsl:if>
                <xsl:call-template name="processWsMoxie">
                    <xsl:with-param name="serviceType" select="$serviceType"/>
                </xsl:call-template>
                <Options>
                    <!-- matrix support -->
                    <xsl:if test="$serviceSubType='WsMatrix'">
                        <matrix-support>true</matrix-support>
                    </xsl:if>
                    <xsl:if test="$serviceSubType='WsMoxie'">
                        <matrix-support>false</matrix-support>
                    </xsl:if>
                </Options>
                <xsl:call-template name="processLoggingClient"/>
            </xsl:when>
            <xsl:when test=" $serviceType='WsDistrix' or $serviceType='WsAccurint'  or $serviceType='WsFacts' or $serviceType='WsArchive'  or $serviceType='WsIdentity' or $serviceType='WsGateway' or $serviceType='WsGIS' or $serviceType='WsRiskwiseGateway' or $serviceType='WsRiskView' or $serviceType='WsPrism' or $serviceType='WsOnDemand' or $serviceType='WsAnalytics'">
                <xsl:if test="@configuration='local'">
                    <!-- RemoteNScfg -->
                    <RemoteNScfg location="{@location}" adlserverid="{@adlserverid}">
                        <xsl:for-each select="RemoteNScfg">
                                <xsl:variable name="username" select="@username"/>
                                <xsl:variable name="password" select="@password"/>
                                <xsl:variable name="u_netAddress">
                               <xsl:choose>
                                    <xsl:when test="$serviceType!='WsGateway' and $serviceType!='WsOnDemand'">
                                        <xsl:value-of select="translate(@netAddress, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz')"/>
                                    </xsl:when>
                                    <xsl:otherwise>
                                        <xsl:value-of select="@netAddress"/>
                                    </xsl:otherwise>
                                </xsl:choose>
                            </xsl:variable> 
                                <xsl:variable name="netAddress">
                                    <xsl:choose>
                                        <xsl:when test="string(@espBinding)='' and string(@netAddress)=''">
                                            <xsl:message terminate="yes">WsGateway: Either a WsECL binding or a network address must be specified for RemoteNS Local.</xsl:message>
                                        </xsl:when>
                                        <xsl:when test="string(@espBinding)!='' and string(@netAddress)!=''">
                                            <xsl:message terminate="yes">WsGateway: Both WsECL binding and network address cannot be specified for RemoteNS Local.</xsl:message>
                                        </xsl:when>
                                        <xsl:when test="string(@espBinding)!=''">
                                            <xsl:call-template name="GetEspBindingAddress">
                                                <xsl:with-param name="espBindingInfo" select="@espBinding"/>
                                            </xsl:call-template>
                                        </xsl:when>
                                        <xsl:when test="starts-with($u_netAddress, 'http://') or starts-with($u_netAddress, 'https://')">
                                            <xsl:value-of select="$u_netAddress"/>
                                        </xsl:when>
                                        <xsl:otherwise>
                                            <xsl:value-of select="concat('http://', $u_netAddress)"/>
                                        </xsl:otherwise>
                                    </xsl:choose>
                                </xsl:variable>
                                <Location name="{../@location}">
                              <xsl:choose>
                                <xsl:when test="$serviceType='WsPrism'">
                                  <Service>
                                    <xsl:copy-of select="@name | @path | @queryname | @username | @password | @status | @optional1 | @optional2 | @roxieSince"/>
                                    <xsl:attribute name="url">
                                       <xsl:value-of select="$netAddress"/>
                                    </xsl:attribute>
                                    <xsl:choose>
                                    <xsl:when test="@name='LNBusinessInview'">
                                        <xsl:attribute name="roxieClient">true</xsl:attribute>
                                        <xsl:copy-of select="@cache[.!='']"/>
                                    </xsl:when>
                                    <xsl:otherwise>
                                        <xsl:attribute name="roxieClient">false</xsl:attribute>
                                        <xsl:copy-of select="@cache[.!='']"/>
                                    </xsl:otherwise>
                                    </xsl:choose>
                                    <xsl:if test="Option[1]">
                                         <Options>
                                             <xsl:for-each select="Option">
                                               <xsl:copy>
                                                   <xsl:copy-of select="@*[name()!='query']"/>
                                               </xsl:copy>
                                             </xsl:for-each>
                                           </Options>
                                    </xsl:if>                                       
                                  </Service>    
                                </xsl:when>
                                <xsl:otherwise>
                                <xsl:for-each select="Configuration">
                                    <xsl:variable name="svrPath">
                                        <xsl:choose>
                                            <xsl:when test="@path">
                                                <xsl:value-of select="@path"/>
                                            </xsl:when>
                                            <xsl:when test="$serviceType!='WsGateway' and $serviceType!='WsOnDemand'">doxie</xsl:when>
                                        </xsl:choose>
                                    </xsl:variable>
                                    <xsl:variable name="svcName">
                                        <xsl:value-of select="@name"/>
                                    </xsl:variable>
                                    <Service>
                                        <xsl:attribute name="name">
                                            <xsl:value-of select="@name"/>
                                            <xsl:choose>
                                                <xsl:when test="@mode='Boolean' or @mode='ESDLBoolean'">Bool</xsl:when>
                                                <xsl:when test="@mode='Focus' or @mode='ESDLFocus'">Focus</xsl:when>
                                            </xsl:choose>
                                        </xsl:attribute>
                                        <xsl:attribute name="path">
                                            <xsl:value-of select="$svrPath"/>
                                        </xsl:attribute>                                        
                                        <xsl:attribute name="url">
                                            <xsl:value-of select="$netAddress"/>
                                        </xsl:attribute>                                        
                                        <xsl:if test="@mode='Boolean' or @mode='Focus'">
                                            <xsl:variable name="normalQuery" select="../../RemoteNScfg/Configuration[@name=current()/@name and (@mode='Normal' or string(@mode)='')]"/>
                                            <xsl:if test="not($normalQuery)">
                                                <xsl:call-template name="message">
                                                    <xsl:with-param name="text">
                                                        <xsl:text>A boolean or focus search service requires a normal service with the same name to be configured.  Please configure the service '</xsl:text>
                                                        <xsl:value-of select="@name"/>
                                                        <xsl:text>' in 'Normal' mode.</xsl:text>
                                                    </xsl:with-param>
                                                </xsl:call-template>
                                            </xsl:if>
                                        </xsl:if>
                                        <xsl:if test="@mode='ESDLBoolean' or @mode='ESDLFocus'">
                                            <xsl:variable name="esdlQuery" select="../../RemoteNScfg/Configuration[@name=current()/@name and @mode='ESDL']"/>
                                            <xsl:if test="not($esdlQuery)">
                                                <xsl:call-template name="message">
                                                    <xsl:with-param name="text">
                                                        <xsl:text>An ESDL boolean or focus search service requires an ESDL service with the same name  to be configured.  Please configure the service '</xsl:text>
                                                        <xsl:value-of select="@name"/>
                                                        <xsl:text>' in 'ESDL' mode.</xsl:text>
                                                    </xsl:with-param>
                                                </xsl:call-template>
                                            </xsl:if>
                                        </xsl:if>
                                        <xsl:if test="@mode='Focus' or @mode='ESDLFocus'">
                                            <xsl:variable name="boolQuery" select="../../RemoteNScfg/Configuration[@name=current()/@name and (@mode='Boolean' or @mode='ESDLBoolean')]"/>
                                            <xsl:if test="$boolQuery">
                                                <xsl:call-template name="message">
                                                    <xsl:with-param name="text">
                                                        <xsl:text>A focus search service supersedes any boolean service with the same.  Please remove configuration for the service '</xsl:text>
                                                        <xsl:value-of select="@name"/>
                                                        <xsl:text>' in 'Boolean' or 'ESDLBoolean' mode.</xsl:text>
                                                    </xsl:with-param>
                                                </xsl:call-template>
                                            </xsl:if>
                                        </xsl:if>
                                        <xsl:if test="@mode='ESDL' or @mode='ESDLFocus'">
                                            <xsl:attribute name="mode">ESDL</xsl:attribute>
                                        </xsl:if>
                                        <xsl:copy-of select="@queryname | ../@username | ../@password | @status | @optional1 | @optional2 | @roxieSince"/>
                                    <xsl:choose>
                                            <xsl:when test="$serviceType='WsGateway' or $serviceType='WsOnDemand'">
                                                <xsl:attribute name="roxieClient">false</xsl:attribute>
                                                <xsl:copy-of select="@cache[.!='']"/>
                                            </xsl:when>
                                            <xsl:when test="$serviceType='WsRiskwiseGateway' and $svcName='ThindexAuto'">
                                                <xsl:attribute name="roxieClient">false</xsl:attribute>
                                            </xsl:when>
                                        </xsl:choose>
                                        <xsl:if test="../Option[@query=current()/@name][1]">
                                            <Options>
                                                <xsl:for-each select="../Option[@query=current()/@name]">
                                                    <xsl:copy>
                                                        <xsl:copy-of select="@*[name()!='query']"/>
                                                    </xsl:copy>
                                                </xsl:for-each>
                                            </Options>
                                        </xsl:if>
                                        <xsl:if test="../Verify[@query=current()/@name][1]">
                                          <xsl:for-each select="../Verify[@query=current()/@name]">
                                            <Verify address_match="{@address_match}" enable="{@enable}" accept_selfsigned="{@accept_selfsigned}">
                                            <ca_certificates path="{@path}"/>
                          <trusted_peers><xsl:value-of select="@trusted_peers"/></trusted_peers>
                                            </Verify>
                                            </xsl:for-each>
                                        </xsl:if>
                                    </Service>
                                </xsl:for-each>
                                </xsl:otherwise>
                              </xsl:choose>
                            </Location>
                            <Gateways>
                                <xsl:for-each select="Gateway">
                                    <Gateway name="{@name}" url="{@url}"  username="{@username}" password="{@password}" />
                                </xsl:for-each>
                            </Gateways>
                        </xsl:for-each>
                    </RemoteNScfg>
                </xsl:if>
                <!-- RemoteNS -->
                <xsl:if test="@configuration='database'">
                    <xsl:if test="string(RemoteNSdb/@username) = '' or string(RemoteNSdb/@password) = ''">
                        <xsl:message terminate="yes">Use name and password required to access the RemoteNS service.</xsl:message>
                    </xsl:if>
                    <RemoteNSdb location="{@location}" adlserverid="{@adlserverid}">
                        <database name="{RemoteNSdb/@name}" userdb="{RemoteNSdb/@userdb}" server="{RemoteNSdb/@server}" username="{RemoteNSdb/@username}" password="{RemoteNSdb/@password}">
                            <xsl:if test="$serviceType='WsIdentity' or $serviceType='WsRiskView'">
                                <xsl:attribute name="dbtype"><xsl:value-of select="RemoteNSdb/@dbtype"/></xsl:attribute>
                            </xsl:if>
                        </database>
                    </RemoteNSdb>
                </xsl:if>
                <xsl:call-template name="processLoggingClient"/>
                <secure>
                    <xsl:value-of select="@secure"/>
                </secure>
                <xsl:if test="(MoxieServer[1])">
                    <xsl:call-template name="processWsMoxie">
                        <xsl:with-param name="serviceType" select="$serviceType"/>
                    </xsl:call-template>
                </xsl:if>
                <xsl:if test="MemCache[1]">
                    <MemCache>
                      <xsl:for-each select="MemCache"> 
                            <xsl:if test="string(@netAddress)='' or string(@port)='' ">
                               <xsl:message terminate="yes">Netaddress and port are required to configure MemCache Server</xsl:message>
                            </xsl:if>
                             <server name="{@computer}" ip="{@netAddress}" port="{@port}" expiretime="{@ExpiresInSeconds}" waittime="{@MaxWaitTime}" status="{@status}"/>
                         </xsl:for-each>
                       </MemCache>
                </xsl:if>
            </xsl:when>
            <xsl:when test="$serviceType='WsDeploy'">
                <xsl:if test="string($espNode/@daliServers) = ''">
                    <xsl:message terminate="yes">No Dali server specified for ESP '<xsl:value-of select="$process"/>'.
               This is required by its binding with WsDeploy service '<xsl:value-of select="$espServiceName"/>'.</xsl:message>
                </xsl:if>
            </xsl:when>
            <xsl:when test="$serviceType='ws_ssn'">
                <xsl:if test="string(@databaseDir) != ''">
                    <xsl:element name="DatabaseDir">
                        <xsl:value-of select="@databaseDir"/>
                    </xsl:element>
                </xsl:if>
                <xsl:if test="string(@loggingServer) != ''">
                    <loggingserver>
                        <url>
                            <xsl:value-of select="@loggingServer"/>
                        </url>
                        <xsl:if test="@failSafe='true'">
                                <failsafe>true</failsafe>
                        </xsl:if>
                    </loggingserver> 
                </xsl:if>   
            </xsl:when>
            <xsl:when test="$serviceType='WsEDA' or $serviceType='WsRiskWise'">
                <!-- RemoteNScfg -->
                <RemoteNScfg location="{@location}" adlserverid="{@adlserverid}">
                        <Location name="{../@location}">
                            <xsl:for-each select="RemoteNSGatewayLocal">
                            <xsl:if test="not(@username) or not(@password)">
                                <xsl:message terminate="yes">Username and Password are required for service access.r.</xsl:message>
                            </xsl:if>
                            <Service name="{@name}" url="{@url}" username="{@username}" password="{@password}"/>
                            </xsl:for-each>
                        </Location>
                        <Gateways>
                            <xsl:for-each select="Gateway">
                                <Gateway name="{@name}" url="{@url}"  username="{@username}" password="{@password}" />
                            </xsl:for-each>
                        </Gateways>
                    </RemoteNScfg>
                <xsl:if test="string(@roxieAddress) = ''">
                    <xsl:message terminate="yes"><xsl:value-of select="$serviceType"/> service '<xsl:value-of select="$espServiceName"/>': No Roxie cluster was specified!</xsl:message>
                </xsl:if>
                <RoxieAddress>
                    <xsl:call-template name="addPortIfMissing">
                        <xsl:with-param name="netAddress" select="@roxieAddress"/>
                        <xsl:with-param name="defaultPort" select="'9876'"/>
                    </xsl:call-template>
                </RoxieAddress>
                <xsl:if test="string(@loadBalanced)!=''">
                    <LoadBalanced>
                        <xsl:value-of select="@loadBalanced"/>
                    </LoadBalanced>
                </xsl:if>
                <xsl:if test="$serviceType='WsRiskWise'">
                    <xsl:if test="string(@roxieModule)=''">
                        <xsl:message terminate="yes">WsRiskwise service '<xsl:value-of select="$espServiceName"/>': No Roxie module was specified!</xsl:message>
                    </xsl:if>
                    <xsl:if test="string(@FCRARoxieModule)=''">
                        <xsl:message terminate="yes">WsRiskwise service '<xsl:value-of select="$espServiceName"/>': No FCRA Roxie module was specified!</xsl:message>
                    </xsl:if>
                    <xsl:if test="string(@FCRARoxieAddress)=''">
                        <xsl:message terminate="yes">WsRiskwise service '<xsl:value-of select="$espServiceName"/>': No FCRA Roxie address was specified!</xsl:message>
                    </xsl:if>
                    <RoxieModule>
                        <xsl:value-of select="@roxieModule"/>
                    </RoxieModule>
                    <FCRARoxieAddress>
                        <xsl:value-of select="@FCRARoxieAddress"/>
                    </FCRARoxieAddress>
                    <FCRARoxieModule>
                        <xsl:value-of select="@FCRARoxieModule"/>
                    </FCRARoxieModule>
                    <seedData>
                        <xsl:value-of select="@seedData"/>
                    </seedData>
                    <resolveInterval>
                        <xsl:value-of select="@resolveInterval"/>
                    </resolveInterval>
                </xsl:if>
                <xsl:call-template name="processLoggingClient"/>
                <xsl:call-template name="processLoggingClientFCRA"/>
            </xsl:when>
            <xsl:when test="$serviceType='WsFCRAInquiry'">
              <xsl:if test="string(@dbName) and string(@dbPassWord) and string(@dbUserID) and string(@dbServer)">
                <inquiryDatabase>
                    <dbName>
                        <xsl:value-of select="@dbName"/>
                    </dbName>
                    <passWord>
                        <xsl:value-of select="@dbPassWord"/>
                    </passWord>
                    <userID>
                        <xsl:value-of select="@dbUserID"/>
                    </userID>
                    <server>
                        <xsl:value-of select="@dbServer"/>
                    </server>
                </inquiryDatabase>
              </xsl:if>
              <xsl:if test="string(@roxieAddress) and string(@roxieAttribute) and string(@instantIP)">
                <roxie>
                    <address>
                        <xsl:value-of select="@roxieAddress"/>
                    </address>
                    <attribute>
                        <xsl:value-of select="@roxieAttribute"/>
                    </attribute>
                    <instantIP>
                        <xsl:value-of select="@instantIP"/>
                    </instantIP>
                </roxie>
              </xsl:if>
            </xsl:when>
            <xsl:when test="$serviceType='ws_thing_finder'">
                <LanguageDirectory><xsl:value-of select="@LanguageDirectory"/></LanguageDirectory>
            </xsl:when>
            <xsl:when test="$serviceType='ws_ecl'">
                <VIPS>
                    <xsl:for-each select="ProcessCluster">
                        <xsl:if test="string(@roxie) != '' and string(@vip) != ''">
                            <xsl:variable name="roxie" select="@roxie"/>
                            <xsl:variable name="vip" select="@vip"/>
                            <ProcessCluster name="{$roxie}" vip="{$vip}"></ProcessCluster>
                        </xsl:if>
                    </xsl:for-each>
                </VIPS>
            </xsl:when>
        </xsl:choose>
    </xsl:template>


    <xsl:template name="processLoggingClient">
        <xsl:if test="string(@loggingServer) != ''">
            <loggingserver>
                <url>
                    <xsl:value-of select="@loggingServer"/>
                </url>
                <xsl:if test="@failSafe='true'">
                    <failsafe>true</failsafe>
                </xsl:if>
                <xsl:if test="@LogResponseXml='true'"> 
                    <LogResponseXml>true</LogResponseXml> 
                </xsl:if>
                <MaxLogQueueLength>
                    <xsl:value-of select="@MaxLogQueueLength"/>
                </MaxLogQueueLength>
                <QueueSizeSignal>
                    <xsl:value-of select="@QueueSizeSignal"/>
                </QueueSizeSignal>
                <Throttle>
                    <xsl:value-of select="@Throttle"/>
                </Throttle>
                <BurstWaitInterval>
                    <xsl:value-of select="@BurstWaitInterval"/>
                </BurstWaitInterval>
                <LinearWaitInterval>
                    <xsl:value-of select="@LinearWaitInterval"/>
                </LinearWaitInterval>
                <NiceLevel>
                    <xsl:value-of select="@NiceLevel"/>
                </NiceLevel>
                <MaxLoggingThreads>
                    <xsl:value-of select="@MaxLoggingThreads"/>
                </MaxLoggingThreads>
            </loggingserver>
        </xsl:if>
    </xsl:template>


    <xsl:template name="processRemoteNScfg">
        <xsl:param name="serviceType"/>
        <RemoteNScfg location="{@location}" adlserverid="{@adlserverid}">
            <xsl:for-each select="RemoteNScfg">
                <Location name="{@location}">
                    <xsl:if test="not(@espBinding)">
                        <xsl:message terminate="yes">Incomplete definition of WsECL binding was found for service.</xsl:message>
                    </xsl:if>
                    <xsl:if test="not(@username) or not(@password)">
                        <xsl:message terminate="yes">Username and Password are required for service access.r.</xsl:message>
                    </xsl:if>
                    <xsl:variable name="username" select="@username"/>
                    <xsl:variable name="password" select="@password"/>
                    <xsl:variable name="netAddress">
                        <xsl:call-template name="GetEspBindingAddress">
                            <xsl:with-param name="espBindingInfo" select="@espBinding"/>
                        </xsl:call-template>
                    </xsl:variable>
                    <xsl:for-each select="Configuration">
                        <xsl:variable name="svrPath">
                            <xsl:choose>
                                <xsl:when test="@path">
                                    <xsl:value-of select="@path"/>
                                </xsl:when>
                                <xsl:otherwise>doxie</xsl:otherwise>
                            </xsl:choose>
                        </xsl:variable>
                        <xsl:variable name="svcName">
                            <xsl:value-of select="@name"/>
                        </xsl:variable>
                        <Service name="{@name}" url="{$netAddress}" path="{$svrPath}" queryname="{@queryname}" username="{$username}" password="{$password}"/>
                    </xsl:for-each>
                </Location>
            </xsl:for-each>
        </RemoteNScfg>
    </xsl:template>


    <xsl:template name="processRemoteNSdb">
        <xsl:if test="string(RemoteNSdb/@username) = '' or string(RemoteNSdb/@password) = ''">
            <xsl:message terminate="yes">Use name and password required to access the RemoteNS service.</xsl:message>
        </xsl:if>
        <RemoteNSdb location="{@location}" adlserverid="{@adlserverid}">
            <database name="{RemoteNSdb/@name}" userdb="{RemoteNSdb/@userdb}" server="{RemoteNSdb/@server}" username="{RemoteNSdb/@username}" password="{RemoteNSdb/@password}">
            </database>
        </RemoteNSdb>
    </xsl:template>


    <xsl:template name="processMySQLdbCfg">
        <xsl:param name="dbconfig"/>
        <db>
            <xsl:if test="string($dbconfig/@server) != ''">
                <server>
                    <xsl:value-of select="$dbconfig/@server"/>
                </server>
            </xsl:if>
            <xsl:if test="string($dbconfig/@dbname) != ''">
                <dbname>
                    <xsl:value-of select="$dbconfig/@dbname"/>
                </dbname>
            </xsl:if>
            <xsl:if test="string($dbconfig/@username) != ''">
                <username>
                    <xsl:value-of select="$dbconfig/@username"/>
                </username>
            </xsl:if>
            <xsl:if test="string($dbconfig/@password) != ''">
                <password>
                    <xsl:value-of select="$dbconfig/@password"/>
                </password>
            </xsl:if>
        </db>
    </xsl:template>


    <xsl:template name="processLoggingClientFCRA">
        <xsl:if test="string(@loggingServerFCRA) != ''">
            <loggingserverFCRA>
                <url>
                    <xsl:value-of select="@loggingServerFCRA"/>
                </url>
                <xsl:if test="@failSafeFCRA='true'">
                    <failsafe>true</failsafe>
                </xsl:if>
                <MaxLogQueueLength>
                    <xsl:value-of select="@MaxLogQueueLengthFCRA"/>
                </MaxLogQueueLength>
                <QueueSizeSignal>
                    <xsl:value-of select="@QueueSizeSignalFCRA"/>
                </QueueSizeSignal>
                <Throttle>
                    <xsl:value-of select="@ThrottleFCRA"/>
                </Throttle>
                <BurstWaitInterval>
                    <xsl:value-of select="@BurstWaitIntervalFCRA"/>
                </BurstWaitInterval>
                <LinearWaitInterval>
                    <xsl:value-of select="@LinearWaitIntervalFCRA"/>
                </LinearWaitInterval>
                <NiceLevel>
                    <xsl:value-of select="@NiceLevelFCRA"/>
                </NiceLevel>
                <MaxLoggingThreads>
                    <xsl:value-of select="@MaxLoggingThreadsFCRA"/>
                </MaxLoggingThreads>
                <xsl:if test="string(@LogsDirFCRA) != ''">
                <LogsDir>
                    <xsl:value-of select="@LogsDirFCRA"/>
                </LogsDir>
                </xsl:if>
            </loggingserverFCRA>
        </xsl:if>
    </xsl:template>


    <xsl:template name="processWsMoxie">
        <moxie>
            <xsl:for-each select="MoxieServer">
                <xsl:if test="string(@name) = '' or string(@computer) = '' or string(@netAddress) = '' or string(@port) = ''">
                    <xsl:message terminate="yes">Incomplete definition of Moxie server was found for WsMoxie service.</xsl:message>
                </xsl:if>
                <!-- username and password are optional-->
                <xsl:variable name="hardCodeMoxieUser" select="string(@moxieUser) != '' and string(@moxiePassword) != ''"/>
                <xsl:if test="not($hardCodeMoxieUser)">
                    <server name="{@computer}" ip="{@netAddress}" port="{@port}" moxiepassthrough="{@moxiepassthrough}" passcredentials="{@passcredentials}"/>
                </xsl:if>
                <xsl:if test="$hardCodeMoxieUser">
                    <server name="{@computer}" ip="{@netAddress}" port="{@port}" userid="{@moxieUser}" password="{@moxiePassword}" moxiepassthrough="{@moxiepassthrough}" passcredentials="{@passcredentials}"/>
                </xsl:if>
            </xsl:for-each>
        </moxie>
    </xsl:template>


    <xsl:template name="defineServicePlugin">
        <xsl:param name="plugin"/>
        <xsl:value-of select="$plugin"/>
        <!--
        <xsl:choose>
            <xsl:when test="$isLinuxInstance">lib<xsl:value-of select="$plugin"/>.so</xsl:when>
            <xsl:otherwise>
                <xsl:value-of select="$plugin"/>.dll</xsl:otherwise>
        </xsl:choose>-->
    </xsl:template>


    <xsl:template name="getImageTransferServers">
        <xsl:param name="espName"/>
        <xsl:variable name="espNode" select="/Environment/Software/EspProcess[@name=$espName]"/>
        <xsl:if test="not($espNode)">
            <xsl:message terminate="yes">ESP process '<xsl:text>$espName</xsl:text>' not found!</xsl:message>
        </xsl:if>
        <xsl:for-each select="$espNode/EspBinding">
            <xsl:variable name="serviceName" select="@service"/>
            <xsl:variable name="servicePort" select="@port"/>
            <xsl:variable name="serviceProtocol" select="@protocol"/>
            <xsl:variable name="serviceType" select="/Environment/Software/EspService[@name=$serviceName]/Properties/@type"/>
            <xsl:if test="$serviceType = 'ws_its'">
                <xsl:for-each select="$espNode/Instance">
                    <TransferServer address="{@netAddress}" port="{$servicePort}" protocol="{$serviceProtocol}"/>
                </xsl:for-each>
            </xsl:if>
        </xsl:for-each>
    </xsl:template>


    <xsl:template name="getImageAccessServers">
        <xsl:param name="espName"/>
        <xsl:variable name="espNode" select="/Environment/Software/EspProcess[@name=$espName]"/>
        <xsl:if test="not($espNode)">
            <xsl:message terminate="yes">ESP process '<xsl:text>$espName</xsl:text>' not found!</xsl:message>
        </xsl:if>
        <xsl:for-each select="$espNode/EspBinding">
            <xsl:variable name="serviceName" select="@service"/>
            <xsl:variable name="servicePort" select="@port"/>
            <xsl:variable name="serviceProtocol" select="@protocol"/>
            <xsl:variable name="serviceType" select="/Environment/Software/EspService[@name=$serviceName]/Properties/@type"/>
            <xsl:if test="$serviceType = 'ws_ias'">
                <xsl:for-each select="$espNode/Instance">
                    <AccessServer address="{@netAddress}" port="{$servicePort}" protocol="{$serviceProtocol}"/>
                </xsl:for-each>
            </xsl:if>
        </xsl:for-each>
    </xsl:template>


    <xsl:template name="GetPathName">
        <xsl:param name="path"/>
        <xsl:if test="contains($path, '\')">
            <xsl:variable name="prefix" select="substring-before($path, '\')"/>
            <xsl:value-of select="concat($prefix, '/')"/>
            <xsl:call-template name="GetPathName">
                <xsl:with-param name="path" select="substring-after($path, '\')"/>
            </xsl:call-template>
        </xsl:if>
    </xsl:template>


    <xsl:template name="GetEspBindingAddress">
        <xsl:param name="espBindingInfo"/>
        <!--format is "esp_name/binding_name" -->
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


    <xsl:template name="addPortIfMissing">
        <xsl:param name="netAddress"/>
        <xsl:param name="defaultPort"/>
        <xsl:variable name="port" select="substring-after($netAddress, ':')"/>
        <xsl:value-of select="$netAddress"/>
        <xsl:if test="$port=''">:<xsl:value-of select="$defaultPort"/>
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
</xsl:stylesheet>
