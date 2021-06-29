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

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xml:space="default" 
xmlns:seisint="http://seisint.com"  xmlns:set="http://exslt.org/sets" exclude-result-prefixes="seisint set">
<!--xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xml:space="default"-->
    <xsl:output method="xml" indent="yes" omit-xml-declaration="no" encoding="UTF-8"/>
    <xsl:param name="process" select="'esp'"/>
    <xsl:param name="instance" select="'rmoondhra'"/>
    <xsl:param name="outputFilePath" select="'c:\development\deployment\xmlenv\dummy.xml'"/>
    <xsl:param name="isLinuxInstance" select="0"/>    
    <xsl:param name="espServiceName" select="'espsmc'"/>

    <xsl:template match="text()"/>

    <xsl:template match="/Environment">
        <Environment>
            <Software>
                <EspProcess>
                    <xsl:apply-templates select="Software/EspProcess[@name=$process]"/>
                </EspProcess>
            </Software>
        </Environment>
    </xsl:template>

    <xsl:template match="EspProcess">
        <xsl:if test="string(@daliServers) = ''">
            <xsl:message terminate="yes">No Dali server specified for ESP '<xsl:value-of select="$process"/>'.
This is required by its binding with ESP service '<xsl:value-of select="$espServiceName"/>'.</xsl:message>
        </xsl:if>
        <xsl:apply-templates select="EspBinding">
            <xsl:with-param name="authNode" select="Authentication[1]"/>
        </xsl:apply-templates>
    </xsl:template>

    <xsl:template match="EspBinding">
        <xsl:param name="authNode"/>
        
     <xsl:variable name="serviceNode" select="/Environment/Software/EspService[@name=$espServiceName and @name=current()/@service and Properties/@type='WsSMC']"/>
        <xsl:apply-templates select="$serviceNode">
            <xsl:with-param name="bindingNode" select="."/>
            <xsl:with-param name="authNode" select="$authNode"/>
        </xsl:apply-templates>
    </xsl:template>


    <xsl:template match="EspService">
        <xsl:param name="bindingNode"/>
        <xsl:param name="authNode"/>

        <!--call each of the service specific templates for each type of service to be created in the final configuration file-->
        <xsl:apply-templates select="." mode="WsSMC">
            <xsl:with-param name="bindingNode" select="$bindingNode"/>
            <xsl:with-param name="authNode" select="$authNode"/>
        </xsl:apply-templates>
        <xsl:apply-templates select="." mode="WsWorkunits">
            <xsl:with-param name="bindingNode" select="$bindingNode"/>
            <xsl:with-param name="authNode" select="$authNode"/>
        </xsl:apply-templates>
        <xsl:apply-templates select="." mode="WsTopology">
            <xsl:with-param name="bindingNode" select="$bindingNode"/>
            <xsl:with-param name="authNode" select="$authNode"/>
        </xsl:apply-templates>
        <xsl:apply-templates select="." mode="WsDfu">
            <xsl:with-param name="bindingNode" select="$bindingNode"/>
            <xsl:with-param name="authNode" select="$authNode"/>
        </xsl:apply-templates>
        <xsl:apply-templates select="." mode="WsDfuXRef">
            <xsl:with-param name="bindingNode" select="$bindingNode"/>
            <xsl:with-param name="authNode" select="$authNode"/>
        </xsl:apply-templates>
        <xsl:apply-templates select="." mode="FileSpray_Serv">
            <xsl:with-param name="bindingNode" select="$bindingNode"/>
            <xsl:with-param name="authNode" select="$authNode"/>
        </xsl:apply-templates>
        <xsl:apply-templates select="." mode="WsFileIO">
            <xsl:with-param name="bindingNode" select="$bindingNode"/>
            <xsl:with-param name="authNode" select="$authNode"/>
        </xsl:apply-templates>
        <xsl:apply-templates select="." mode="WsPackageProcess">
            <xsl:with-param name="bindingNode" select="$bindingNode"/>
            <xsl:with-param name="authNode" select="$authNode"/>
        </xsl:apply-templates>
        <xsl:apply-templates select="." mode="WsRoxieQuery">
            <xsl:with-param name="bindingNode" select="$bindingNode"/>
            <xsl:with-param name="authNode" select="$authNode"/>
        </xsl:apply-templates>
        <xsl:apply-templates select="." mode="ws_machine">
            <xsl:with-param name="bindingNode" select="$bindingNode"/>
            <xsl:with-param name="authNode" select="$authNode"/>
        </xsl:apply-templates>
        <xsl:apply-templates select="." mode="ws_account">
            <xsl:with-param name="bindingNode" select="$bindingNode"/>
            <xsl:with-param name="authNode" select="$authNode"/>
        </xsl:apply-templates>
        <xsl:apply-templates select="." mode="ws_access">
            <xsl:with-param name="bindingNode" select="$bindingNode"/>
            <xsl:with-param name="authNode" select="$authNode"/>
        </xsl:apply-templates>
        <xsl:apply-templates select="." mode="ws_config">
            <xsl:with-param name="bindingNode" select="$bindingNode"/>
            <xsl:with-param name="authNode" select="$authNode"/>
        </xsl:apply-templates>
        <xsl:apply-templates select="." mode="WsESDLConfig">
            <xsl:with-param name="bindingNode" select="$bindingNode"/>
            <xsl:with-param name="authNode" select="$authNode"/>
        </xsl:apply-templates>
        <xsl:apply-templates select="." mode="ws_elk">
            <xsl:with-param name="bindingNode" select="$bindingNode"/>
            <xsl:with-param name="authNode" select="$authNode"/>
        </xsl:apply-templates>
        <xsl:apply-templates select="." mode="ws_store">
            <xsl:with-param name="bindingNode" select="$bindingNode"/>
            <xsl:with-param name="authNode" select="$authNode"/>
        </xsl:apply-templates>
        <xsl:apply-templates select="." mode="ws_codesign">
            <xsl:with-param name="bindingNode" select="$bindingNode"/>
            <xsl:with-param name="authNode" select="$authNode"/>
        </xsl:apply-templates>
        <xsl:apply-templates select="." mode="ws_dali">
            <xsl:with-param name="bindingNode" select="$bindingNode"/>
            <xsl:with-param name="authNode" select="$authNode"/>
        </xsl:apply-templates>
        <xsl:apply-templates select="." mode="ws_logaccess">
            <xsl:with-param name="bindingNode" select="$bindingNode"/>
            <xsl:with-param name="authNode" select="$authNode"/>
        </xsl:apply-templates>
    </xsl:template>

    <!-- WS-SMC -->
    <xsl:template match="EspService" mode="WsSMC">
        <xsl:param name="bindingNode"/>
        <xsl:param name="authNode"/>

        <xsl:variable name="serviceType" select="'WsSMC'"/>
        <xsl:variable name="serviceName" select="concat($serviceType, '_', @name, '_', $process)"/>
        <xsl:variable name="bindName" select="concat($serviceType, '_', $bindingNode/@name, '_', $process)"/>
        <xsl:variable name="bindType" select="'ws_smcSoapBinding'"/>
        <xsl:variable name="servicePlugin">
            <xsl:call-template name="defineServicePlugin">
                <xsl:with-param name="plugin" select="'ws_smc'"/>
            </xsl:call-template>
        </xsl:variable>
        <EspService name="{$serviceName}" type="{$serviceType}" plugin="{$servicePlugin}">
            <xsl:if test="string(@ActivityInfoCacheSeconds) != ''">
                <ActivityInfoCacheSeconds><xsl:value-of select="@ActivityInfoCacheSeconds"/></ActivityInfoCacheSeconds>
            </xsl:if>
            <xsl:if test="string(@ActivityInfoCacheAutoRebuildSeconds) != ''">
                <ActivityInfoCacheAutoRebuildSeconds><xsl:value-of select="@ActivityInfoCacheAutoRebuildSeconds"/></ActivityInfoCacheAutoRebuildSeconds>
            </xsl:if>
            <xsl:if test="string(@enableLogDaliConnection) != ''">
                <LogDaliConnection><xsl:value-of select="@enableLogDaliConnection"/></LogDaliConnection>
            </xsl:if>
        </EspService>
        <EspBinding name="{$bindName}" service="{$serviceName}" protocol="{$bindingNode/@protocol}" type="{$bindType}" 
             plugin="{$servicePlugin}" netAddress="0.0.0.0" port="{$bindingNode/@port}" defaultBinding="true">
            <xsl:call-template name="bindAuthentication">
                <xsl:with-param name="bindingNode" select="$bindingNode"/>
                <xsl:with-param name="authMethod" select="$authNode/@method"/>
                <xsl:with-param name="service" select="'ws_smc'"/>
            </xsl:call-template>
        </EspBinding>
    </xsl:template>
    
    <!-- WS-TOPOLOGY -->
    <xsl:template match="EspService" mode="WsTopology">
        <xsl:param name="bindingNode"/>
        <xsl:param name="authNode"/>

        <xsl:variable name="serviceType" select="'WsTopology'"/>
        <xsl:variable name="serviceName" select="concat($serviceType, '_', @name, '_', $process)"/>
        <xsl:variable name="bindName" select="concat($serviceType, '_', $bindingNode/@name, '_', $process)"/>
        <xsl:variable name="bindType" select="'ws_topologySoapBinding'"/>
        <xsl:variable name="servicePlugin">
            <xsl:call-template name="defineServicePlugin">
                <xsl:with-param name="plugin" select="'ws_topology'"/>
            </xsl:call-template>
        </xsl:variable>    
        <EspService name="{$serviceName}" type="{$serviceType}" plugin="{$servicePlugin}">
            <xsl:for-each select="@warnIfCpuLoadOver|@warnIfFreeStorageUnder|@warnIfFreeMemoryUnder">
                <xsl:if test="string-length()!=0">
                    <xsl:copy-of select="."/>
                </xsl:if>
            </xsl:for-each>
            <xsl:if test="string(@enableRoxieOnDemand) != ''">
                <EnableRoxieOnDemand><xsl:value-of select="@enableRoxieOnDemand"/></EnableRoxieOnDemand>
            </xsl:if>
                <xsl:if test="string(@allowNewRoxieOnDemandQuery) != ''">
                <AllowNewRoxieOnDemandQuery><xsl:value-of select="@allowNewRoxieOnDemandQuery"/></AllowNewRoxieOnDemandQuery>
            </xsl:if>
            <xsl:if test="string(@defaultTargetCluster) != ''">
                      <xsl:variable name="targetClusterName" select="@defaultTargetCluster"/>
                      <xsl:variable name="targetClusterPrefix" select="/Environment/Software/Topology/Cluster[@name=$targetClusterName]/@prefix"/>
                      <DefaultTargetCluster  name="{$targetClusterName}" prefix="{$targetClusterPrefix}"/>
            </xsl:if>
            <xsl:if test="string(@enableSystemUseRewrite) != ''">
                <SystemUseRewrite><xsl:value-of select="@enableSystemUseRewrite"/></SystemUseRewrite>
            </xsl:if>
      <xsl:if test="string(@processesInPreflightCheck) != ''">
        <PreflightProcessFilter>
          <xsl:value-of select="@processesInPreflightCheck"/>
        </PreflightProcessFilter>
      </xsl:if>
        </EspService>
        <EspBinding name="{$bindName}" service="{$serviceName}" protocol="{$bindingNode/@protocol}" type="{$bindType}" plugin="{$servicePlugin}" netAddress="0.0.0.0" port="{$bindingNode/@port}">
            <xsl:call-template name="bindAuthentication">
                <xsl:with-param name="bindingNode" select="$bindingNode"/>
                <xsl:with-param name="authMethod" select="$authNode/@method"/>
                <xsl:with-param name="service" select="'ws_topology'"/>
            </xsl:call-template>
        </EspBinding>
    </xsl:template>

    <!-- WS-WORKUNITS -->
    <xsl:template match="EspService" mode="WsWorkunits">
        <xsl:param name="bindingNode"/>
        <xsl:param name="authNode"/>

        <xsl:variable name="serviceType" select="'WsWorkunits'"/>
        <xsl:variable name="serviceName" select="concat($serviceType, '_', @name, '_', $process)"/>
        <xsl:variable name="bindName" select="concat($serviceType, '_', $bindingNode/@name, '_', $process)"/>
        <xsl:variable name="bindType" select="'ws_workunitsSoapBinding'"/>
        <xsl:variable name="servicePlugin">
            <xsl:call-template name="defineServicePlugin">
                <xsl:with-param name="plugin" select="'ws_workunits'"/>
            </xsl:call-template>
        </xsl:variable>

        <EspService name="{$serviceName}" type="{$serviceType}" plugin="{$servicePlugin}">
            <xsl:if test="string(@viewTimeout) != ''">
                <ViewTimeout><xsl:value-of select="@viewTimeout"/></ViewTimeout>
            </xsl:if>
            <xsl:if test="string(@clusterQueryStateThreadPoolSize) != ''">
                <ClusterQueryStateThreadPoolSize><xsl:value-of select="@clusterQueryStateThreadPoolSize"/></ClusterQueryStateThreadPoolSize>
            </xsl:if>
            <xsl:if test="string(@ThorSlaveLogThreadPoolSize) != ''">
                <ThorSlaveLogThreadPoolSize><xsl:value-of select="@ThorSlaveLogThreadPoolSize"/></ThorSlaveLogThreadPoolSize>
            </xsl:if>
            <xsl:if test="string(@WUResultMaxSizeMB) != ''">
                <WUResultMaxSizeMB><xsl:value-of select="@WUResultMaxSizeMB"/></WUResultMaxSizeMB>
            </xsl:if>
            <xsl:if test="string(@WUResultDownloadFlushThreshold) != ''">
                <WUResultDownloadFlushThreshold><xsl:value-of select="@WUResultDownloadFlushThreshold"/></WUResultDownloadFlushThreshold>
            </xsl:if>
            <xsl:if test="string(@AWUsCacheTimeout) != ''">
                <AWUsCacheMinutes><xsl:value-of select="@AWUsCacheTimeout"/></AWUsCacheMinutes>
            </xsl:if>
            <xsl:if test="string(@serverForArchivedECLWU) != ''">
                <xsl:variable name="sashaServer" select="@serverForArchivedECLWU"/>
                <xsl:variable name="sashaServerIP" select="/Environment/Software/SashaServerProcess[@name=$sashaServer]/Instance/@netAddress"/>
                <xsl:variable name="sashaServerPort" select="/Environment/Software/SashaServerProcess[@name=$sashaServer]/Instance/@port"/>
                <serverForArchivedECLWU netAddress="{$sashaServerIP}" port="{$sashaServerPort}" />
            </xsl:if>
            <xsl:if test="string(@allowNewRoxieOnDemandQuery) != ''">
                <AllowNewRoxieOnDemandQuery><xsl:value-of select="@allowNewRoxieOnDemandQuery"/></AllowNewRoxieOnDemandQuery>
            </xsl:if>
            <LayoutProgram>dot/dot -Tsvg -Gordering=out</LayoutProgram>

            <xsl:if test="string(@syntaxCheckQueue) != ''">
                <SyntaxCheckQueue><xsl:value-of select="@syntaxCheckQueue"/></SyntaxCheckQueue>
            </xsl:if>
            
            <xsl:if test="string(@ZAPEmailServer) != ''">
                <xsl:variable name="emailServer" select="@ZAPEmailServer"/>
                <xsl:variable name="emailTo" select="@ZAPEmailTo"/>
                <xsl:variable name="emailFrom" select="@ZAPEmailFrom"/>
                <xsl:variable name="emailServerPort" select="@ZAPEmailServerPort"/>
                <xsl:variable name="emailMaxAttachment" select="@ZAPEmailMaxAttachmentSize"/>
                <ZAPEmail serverURL="{$emailServer}" serverPort="{$emailServerPort}" to="{$emailTo}" from="{$emailFrom}" maxAttachmentSize="{$emailMaxAttachment}"/>
            </xsl:if>
            <StyleSheets>
                <xslt name="atts">/esp/xslt/atts.xslt</xslt>
                <xslt name="dot_update">/esp/xslt/dot_update.xslt</xslt>
                <xslt name="dot">/esp/xslt/dot.xslt</xslt>
                <xslt name="graphStats">/esp/xslt/graphStats.xslt</xslt>
        <xslt name="graphupdate_gvc">/esp/xslt/graphupdate_gvc.xslt</xslt>
      </StyleSheets>
        </EspService>
        <EspBinding name="{$bindName}" service="{$serviceName}" protocol="{$bindingNode/@protocol}" type="{$bindType}" plugin="{$servicePlugin}" netAddress="0.0.0.0" port="{$bindingNode/@port}">
            <xsl:call-template name="bindAuthentication">
                <xsl:with-param name="bindingNode" select="$bindingNode"/>
                <xsl:with-param name="authMethod" select="$authNode/@method"/>
                <xsl:with-param name="service" select="'ws_workunits'"/>
            </xsl:call-template>
        </EspBinding>
    </xsl:template>

    <!-- WS-DFU -->
    <xsl:template match="EspService" mode="WsDfu">
        <xsl:param name="bindingNode"/>
        <xsl:param name="authNode"/>

        <xsl:variable name="serviceType" select="'WsDfu'"/>
        <xsl:variable name="serviceName" select="concat($serviceType, '_', @name, '_', $process)"/>
        <xsl:variable name="bindName" select="concat($serviceType, '_', $bindingNode/@name, '_', $process)"/>
        <xsl:variable name="bindType" select="'ws_dfuSoapBinding'"/>
        <xsl:variable name="servicePlugin">
            <xsl:call-template name="defineServicePlugin">
                <xsl:with-param name="plugin" select="'ws_dfu'"/>
            </xsl:call-template>
        </xsl:variable>

        <EspService name="{$serviceName}" type="{$serviceType}" plugin="{$servicePlugin}">
            <xsl:if test="string(@NodeGroupCacheMinutes) != ''">
                <NodeGroupCacheMinutes><xsl:value-of select="@NodeGroupCacheMinutes"/></NodeGroupCacheMinutes>
            </xsl:if>
            <xsl:if test="string(@disableUppercaseTranslation) != ''">
                <DisableUppercaseTranslation><xsl:value-of select="@disableUppercaseTranslation"/></DisableUppercaseTranslation>
            </xsl:if>
            <xsl:if test="string(@viewTimeout) != ''">
                <ViewTimeout><xsl:value-of select="@viewTimeout"/></ViewTimeout>
            </xsl:if>
            <xsl:if test="string(@clusterName) != ''">
            <ClusterName><xsl:value-of select="@clusterName"/></ClusterName>
         </xsl:if>
            <StyleSheets>
                <xslt name="def_file">./smc_xslt/def_file.xslt</xslt>
            </StyleSheets>
            <xsl:variable name="outputPath">
                <xsl:call-template name="GetPathName">
                    <xsl:with-param name="path" select="translate($outputFilePath, '/$', '\:')"/>
                </xsl:call-template>
            </xsl:variable>
          <xsl:variable name="outputPath1" select="translate($outputPath, '\','/')"/>
            
            <xsl:variable name="pluginsFilePath" select="concat('file:///', $outputPath1, $espServiceName, '_plugins.xml')"/>
            <xsl:variable name="pluginsRoot" select="document($pluginsFilePath)"/>
            <xsl:if test="not($pluginsRoot)">
                <xsl:message terminate="yes">The plugins file '<xsl:value-of select="$pluginsFilePath"/>' was either not generated or failed to open!</xsl:message>
            </xsl:if>
            <xsl:variable name="pluginsNodes" select="$pluginsRoot/Plugins/Plugin/@destName"/>
            <xsl:if test="not(function-available('set:distinct'))">
                <xsl:message terminate="yes">This XSL transformation can only be run by an XSLT processor supporting exslt sets!</xsl:message>
            </xsl:if>
            <Plugins>
               <xsl:attribute name="path"><xsl:value-of select="@pluginsPath"/></xsl:attribute>
            </Plugins>
        </EspService>
        <EspBinding name="{$bindName}" service="{$serviceName}" protocol="{$bindingNode/@protocol}" type="{$bindType}" plugin="{$servicePlugin}" netAddress="0.0.0.0" port="{$bindingNode/@port}">
            <xsl:call-template name="bindAuthentication">
                <xsl:with-param name="bindingNode" select="$bindingNode"/>
                <xsl:with-param name="authMethod" select="$authNode/@method"/>
                <xsl:with-param name="service" select="'ws_dfu'"/>
            </xsl:call-template>
        </EspBinding>
    </xsl:template>

    <!-- WS-DFUXREF -->
    <xsl:template match="EspService" mode="WsDfuXRef">
        <xsl:param name="bindingNode"/>
        <xsl:param name="authNode"/>

        <xsl:variable name="serviceType" select="'WsDfuXRef'"/>
        <xsl:variable name="serviceName" select="concat($serviceType, '_', @name, '_', $process)"/>
        <xsl:variable name="bindName" select="concat($serviceType, '_', $bindingNode/@name, '_', $process)"/>
        <xsl:variable name="bindType" select="'ws_dfuxrefSoapBinding'"/>
        <xsl:variable name="servicePlugin">
            <xsl:call-template name="defineServicePlugin">
                <xsl:with-param name="plugin" select="'ws_dfu'"/>
            </xsl:call-template>
        </xsl:variable>

        <EspService name="{$serviceName}" type="{$serviceType}" plugin="{$servicePlugin}">
            <xsl:if test="string(@viewTimeout) != ''">
                <ViewTimeout><xsl:value-of select="@viewTimeout"/></ViewTimeout>
            </xsl:if>
            <LayoutProgram>dot/dot -Tsvg -Gordering=out</LayoutProgram>
            <StyleSheets>
                <xslt name="def_file">./smc_xslt/def_file.xslt</xslt>
            </StyleSheets>
        </EspService>
        <EspBinding name="{$bindName}" service="{$serviceName}" protocol="{$bindingNode/@protocol}" type="{$bindType}" plugin="{$servicePlugin}" netAddress="0.0.0.0" port="{$bindingNode/@port}">
            <xsl:call-template name="bindAuthentication">
                <xsl:with-param name="bindingNode" select="$bindingNode"/>
                <xsl:with-param name="authMethod" select="$authNode/@method"/>
                <xsl:with-param name="service" select="'ws_dfuxref'"/>
            </xsl:call-template>
        </EspBinding>
    </xsl:template>

    <!-- WS-FILESPRAY -->
    <xsl:template match="EspService" mode="FileSpray_Serv">
        <xsl:param name="bindingNode"/>
        <xsl:param name="authNode"/>

        <xsl:variable name="serviceType" select="'FileSpray_Serv'"/>
        <xsl:variable name="serviceName" select="concat($serviceType, '_', @name, '_', $process)"/>
        <xsl:variable name="bindName" select="concat($serviceType, '_', $bindingNode/@name, '_', $process)"/>
        <xsl:variable name="bindType" select="'FileSpray_Bind'"/>
        <xsl:variable name="servicePlugin">
            <xsl:call-template name="defineServicePlugin">
                <xsl:with-param name="plugin" select="'ws_fs'"/>
            </xsl:call-template>
        </xsl:variable>


        <EspService name="{$serviceName}" type="{$serviceType}" plugin="{$servicePlugin}">
            <QueueLabel>dfuserver_queue</QueueLabel>
            <MonitorQueueLabel>dfuserver_monitor_queue</MonitorQueueLabel>
        </EspService>
        <EspBinding name="{$bindName}" service="{$serviceName}" protocol="{$bindingNode/@protocol}" type="{$bindType}" plugin="{$servicePlugin}" netAddress="0.0.0.0" port="{$bindingNode/@port}">
            <xsl:call-template name="bindAuthentication">
                <xsl:with-param name="bindingNode" select="$bindingNode"/>
                <xsl:with-param name="authMethod" select="$authNode/@method"/>
                <xsl:with-param name="service" select="'ws_fs'"/>
            </xsl:call-template>
        </EspBinding>
    </xsl:template>

    <!-- WS-FILEIO -->
    <xsl:template match="EspService" mode="WsFileIO">
        <xsl:param name="bindingNode"/>
        <xsl:param name="authNode"/>

        <xsl:variable name="serviceType" select="'WsFileIO'"/>
        <xsl:variable name="serviceName" select="concat($serviceType, '_', @name, '_', $process)"/>
        <xsl:variable name="bindName" select="concat($serviceType, '_', $bindingNode/@name, '_', $process)"/>
        <xsl:variable name="bindType" select="'WsFileIO'"/>
        <xsl:variable name="servicePlugin">
            <xsl:call-template name="defineServicePlugin">
                <xsl:with-param name="plugin" select="'ws_fileio'"/>
            </xsl:call-template>
        </xsl:variable>


        <EspService name="{$serviceName}" type="{$serviceType}" plugin="{$servicePlugin}"/>
        <EspBinding name="{$bindName}" service="{$serviceName}" protocol="{$bindingNode/@protocol}" type="{$bindType}" plugin="{$servicePlugin}" netAddress="0.0.0.0" port="{$bindingNode/@port}">
            <xsl:call-template name="bindAuthentication">
                <xsl:with-param name="bindingNode" select="$bindingNode"/>
                <xsl:with-param name="authMethod" select="$authNode/@method"/>
                <xsl:with-param name="service" select="'ws_fileio'"/>
            </xsl:call-template>
        </EspBinding>
    </xsl:template>

    <!-- WS-PACKAGEPROCESS -->
    <xsl:template match="EspService" mode="WsPackageProcess">
        <xsl:param name="bindingNode"/>
        <xsl:param name="authNode"/>

        <xsl:variable name="serviceType" select="'WsPackageProcess'"/>
        <xsl:variable name="serviceName" select="concat($serviceType, '_', @name, '_', $process)"/>
        <xsl:variable name="bindName" select="concat($serviceType, '_', $bindingNode/@name, '_', $process)"/>
        <xsl:variable name="bindType" select="'WsPackageProcessSoapBinding'"/>
        <xsl:variable name="servicePlugin">
            <xsl:call-template name="defineServicePlugin">
                <xsl:with-param name="plugin" select="'ws_packageprocess'"/>
            </xsl:call-template>
        </xsl:variable>


        <EspService name="{$serviceName}" type="{$serviceType}" plugin="{$servicePlugin}"/>
        <EspBinding name="{$bindName}" service="{$serviceName}" protocol="{$bindingNode/@protocol}" type="{$bindType}" plugin="{$servicePlugin}" netAddress="0.0.0.0" port="{$bindingNode/@port}">
            <xsl:call-template name="bindAuthentication">
                <xsl:with-param name="bindingNode" select="$bindingNode"/>
                <xsl:with-param name="authMethod" select="$authNode/@method"/>
                <xsl:with-param name="service" select="'ws_packageprocess'"/>
            </xsl:call-template>
        </EspBinding>
    </xsl:template>
    
    <!-- ws_machine-->
    <xsl:template match="EspService" mode="ws_machine">
        <xsl:param name="bindingNode"/>
        <xsl:param name="authNode"/>

        <xsl:variable name="serviceType" select="'ws_machine'"/>
        <xsl:variable name="serviceName" select="concat($serviceType, '_', @name, '_', $process)"/>
        <xsl:variable name="bindName" select="concat($serviceType, '_', $bindingNode/@name, '_', $process)"/>
        <xsl:variable name="bindType" select="'ws_machineSoapBinding'"/>
        <xsl:variable name="servicePlugin">
            <xsl:call-template name="defineServicePlugin">
                <xsl:with-param name="plugin" select="'ws_machine'"/>
            </xsl:call-template>
        </xsl:variable>

        <EspService name="{$serviceName}" type="{$serviceType}" plugin="{$servicePlugin}">
            <xsl:copy-of select="@monitorDaliFileServer|@excludePartitions"/>
            <xsl:apply-templates select="Properties/ProcessFilters" mode="copy"/>
            <xsl:if test="string(@MachineUsageCacheMinutes) != ''">
                <MachineUsageCacheMinutes><xsl:value-of select="@MachineUsageCacheMinutes"/></MachineUsageCacheMinutes>
            </xsl:if>
            <xsl:if test="string(@MachineUsageCacheAutoRebuildMinutes) != ''">
                <MachineUsageCacheAutoRebuildMinutes><xsl:value-of select="@MachineUsageCacheAutoRebuildMinutes"/></MachineUsageCacheAutoRebuildMinutes>
            </xsl:if>
        </EspService>
        
        <EspBinding name="{$bindName}" service="{$serviceName}" protocol="{$bindingNode/@protocol}" type="{$bindType}" plugin="{$servicePlugin}" netAddress="0.0.0.0" port="{$bindingNode/@port}">
            <xsl:call-template name="bindAuthentication">
                <xsl:with-param name="bindingNode" select="$bindingNode"/>
                <xsl:with-param name="authMethod" select="$authNode/@method"/>
                <xsl:with-param name="service" select="'ws_machine'"/>
            </xsl:call-template>
        </EspBinding>
    </xsl:template>

    <!-- ws_account-->
    <xsl:template match="EspService" mode="ws_account">
        <xsl:param name="bindingNode"/>
        <xsl:param name="authNode"/>

        <xsl:variable name="serviceType" select="'ws_account'"/>
        <xsl:variable name="serviceName" select="concat($serviceType, '_', @name, '_', $process)"/>
        <xsl:variable name="bindName" select="concat($serviceType, '_', $bindingNode/@name, '_', $process)"/>
        <xsl:variable name="bindType" select="'ws_accountSoapBinding'"/>
        <xsl:variable name="servicePlugin">
            <xsl:call-template name="defineServicePlugin">
                <xsl:with-param name="plugin" select="'ws_account'"/>
            </xsl:call-template>
        </xsl:variable>
        
        <EspService name="{$serviceName}" type="{$serviceType}" plugin="{$servicePlugin}"/>     
        <EspBinding name="{$bindName}" service="{$serviceName}" protocol="{$bindingNode/@protocol}" type="{$bindType}" plugin="{$servicePlugin}" netAddress="0.0.0.0" port="{$bindingNode/@port}">
            <xsl:call-template name="bindAuthentication">
                <xsl:with-param name="bindingNode" select="$bindingNode"/>
                <xsl:with-param name="authMethod" select="$authNode/@method"/>
                <xsl:with-param name="service" select="'ws_account'"/>
            </xsl:call-template>
        </EspBinding>
    </xsl:template>

    <!-- ws_config-->
    <xsl:template match="EspService" mode="ws_config">
        <xsl:param name="bindingNode"/>
        <xsl:param name="authNode"/>

        <xsl:variable name="serviceType" select="'ws_config'"/>
        <xsl:variable name="serviceName" select="concat($serviceType, '_', @name, '_', $process)"/>
        <xsl:variable name="bindName" select="concat($serviceType, '_', $bindingNode/@name, '_', $process)"/>
        <xsl:variable name="bindType" select="'ws_configSoapBinding'"/>
        <xsl:variable name="servicePlugin">
            <xsl:call-template name="defineServicePlugin">
                <xsl:with-param name="plugin" select="'ws_config'"/>
            </xsl:call-template>
        </xsl:variable>
        
        <EspService name="{$serviceName}" type="{$serviceType}" plugin="{$servicePlugin}"/>     
        <EspBinding name="{$bindName}" service="{$serviceName}" protocol="{$bindingNode/@protocol}" type="{$bindType}" plugin="{$servicePlugin}" netAddress="0.0.0.0" port="{$bindingNode/@port}">
            <xsl:call-template name="bindAuthentication">
                <xsl:with-param name="bindingNode" select="$bindingNode"/>
                <xsl:with-param name="authMethod" select="$authNode/@method"/>
                <xsl:with-param name="service" select="'ws_config'"/>
            </xsl:call-template>
        </EspBinding>
    </xsl:template>

<!-- WS-ESDLCONFIG -->
    <xsl:template match="EspService" mode="WsESDLConfig">
     <xsl:param name="bindingNode"/>
     <xsl:param name="authNode"/>
     <xsl:variable name="serviceType" select="'ws_esdlconfig'"/>
     <xsl:variable name="serviceName" select="concat($serviceType, '_', @name, '_', $process)"/>
     <xsl:variable name="bindName" select="concat($serviceType, '_', $bindingNode/@name, '_', $process)"/>
     <xsl:variable name="bindType" select="'ws_esdlconfigSoapBinding'"/>
     <xsl:variable name="servicePlugin">
     <xsl:call-template name="defineServicePlugin">
      <xsl:with-param name="plugin" select="'ws_esdlconfig'"/>
</xsl:call-template>
</xsl:variable>
<EspService name="{$serviceName}" type="{$serviceType}" plugin="{$servicePlugin}"/>
<EspBinding name="{$bindName}" service="{$serviceName}" protocol="{$bindingNode/@protocol}" type="{$bindType}" plugin="{$servicePlugin}" netAddress="0.0.0.0" port="{$bindingNode/@port}">
<xsl:call-template name="bindAuthentication">
<xsl:with-param name="bindingNode" select="$bindingNode"/>
<xsl:with-param name="authMethod" select="$authNode/@method"/>
<xsl:with-param name="service" select="'ws_esdlconfig'"/>
</xsl:call-template>
</EspBinding>
</xsl:template>
    <!-- ws_access-->
    <xsl:template match="EspService" mode="ws_access">
        <xsl:param name="bindingNode"/>
        <xsl:param name="authNode"/>

        <xsl:variable name="serviceType" select="'ws_access'"/>
        <xsl:variable name="serviceName" select="concat($serviceType, '_', @name, '_', $process)"/>
        <xsl:variable name="bindName" select="concat($serviceType, '_', $bindingNode/@name, '_', $process)"/>
        <xsl:variable name="bindType" select="'ws_accessSoapBinding'"/>
        <xsl:variable name="servicePlugin">
            <xsl:call-template name="defineServicePlugin">
                <xsl:with-param name="plugin" select="'ws_access'"/>
            </xsl:call-template>
        </xsl:variable>
        
        <EspService name="{$serviceName}" type="{$serviceType}" plugin="{$servicePlugin}">
            <xsl:variable name="ldapservername" select="$bindingNode/../Authentication/@ldapServer"/>
            <xsl:choose>
                <xsl:when test="$ldapservername != ''">
                <xsl:variable name="filesbasedn" select="/Environment/Software/LDAPServerProcess[@name=$ldapservername]/@filesBasedn"/>
                <Files basedn="{$filesbasedn}"/>
                </xsl:when>
            </xsl:choose>
            <Resources>
                <xsl:for-each select="../EspProcess[Authentication/@ldapServer=$ldapservername]/EspBinding">
                    <Binding name="{@name}" service="{@service}" port="{@port}" basedn="{@resourcesBasedn}" workunitsBasedn="{@workunitsBasedn}"/>
                </xsl:for-each>
            </Resources>
        </EspService>
        
        <EspBinding name="{$bindName}" service="{$serviceName}" protocol="{$bindingNode/@protocol}" type="{$bindType}" plugin="{$servicePlugin}" netAddress="0.0.0.0" port="{$bindingNode/@port}">
            <xsl:call-template name="bindAuthentication">
                <xsl:with-param name="bindingNode" select="$bindingNode"/>
                <xsl:with-param name="authMethod" select="$authNode/@method"/>
                <xsl:with-param name="service" select="'ws_access'"/>
            </xsl:call-template>
        </EspBinding>
    </xsl:template>

    <!-- ws_elk-->
    <xsl:template match="EspService" mode="ws_elk">
        <xsl:param name="bindingNode"/>
        <xsl:param name="authNode"/>

        <xsl:variable name="serviceType" select="'ws_elk'"/>
        <xsl:variable name="serviceName" select="concat($serviceType, '_', @name, '_', $process)"/>
        <xsl:variable name="bindName" select="concat($serviceType, '_', $bindingNode/@name, '_', $process)"/>
        <xsl:variable name="bindType" select="'ws_elkSoapBinding'"/>
        <xsl:variable name="servicePlugin">
            <xsl:call-template name="defineServicePlugin">
                <xsl:with-param name="plugin" select="'ws_elk'"/>
            </xsl:call-template>
        </xsl:variable>

        <EspService name="{$serviceName}" type="{$serviceType}" plugin="{$servicePlugin}">
            <ELKIntegration>
                <Kibana>
                    <xsl:copy-of select="@integrateKibana|@kibanaAddress|@kibanaPort|@kibanaEntryPointURI"/>
                </Kibana>
                <ElasticSearch>
                    <xsl:copy-of select="@reportElasticHealth|@elasticSearchAddresses|@elasticSearchPort"/>
                </ElasticSearch>
                <LogStash>
                    <xsl:copy-of select="@reportLogStashHealth|@logStashAddresses|@logStashPort"/>
                </LogStash>
            </ELKIntegration>
        </EspService>

        <EspBinding name="{$bindName}" service="{$serviceName}" protocol="{$bindingNode/@protocol}" type="{$bindType}" plugin="{$servicePlugin}" netAddress="0.0.0.0" port="{$bindingNode/@port}">
            <xsl:call-template name="bindAuthentication">
                <xsl:with-param name="bindingNode" select="$bindingNode"/>
                <xsl:with-param name="authMethod" select="$authNode/@method"/>
                <xsl:with-param name="service" select="'ws_account'"/>
            </xsl:call-template>
        </EspBinding>
    </xsl:template>

    <!-- WS-Store -->
    <xsl:template match="EspService" mode="ws_store">
        <xsl:param name="bindingNode"/>
        <xsl:param name="authNode"/>

        <xsl:variable name="serviceType" select="'ws_store'"/>
        <xsl:variable name="serviceName" select="concat($serviceType, '_', @name, '_', $process)"/>
        <xsl:variable name="bindName" select="concat($serviceType, '_', $bindingNode/@name, '_', $process)"/>
        <xsl:variable name="bindType" select="'ws_storeSoapBinding'"/>
        <xsl:variable name="servicePlugin">
            <xsl:call-template name="defineServicePlugin">
                <xsl:with-param name="plugin" select="'ws_store'"/>
            </xsl:call-template>
        </xsl:variable>
        <EspService name="{$serviceName}" type="{$serviceType}" plugin="{$servicePlugin}">
            <xsl:element name="StoreProvider">
                <xsl:attribute name="lib">dalistorelib</xsl:attribute>
            </xsl:element>
            <xsl:element name="Stores">
                <xsl:element name="Store">
                    <xsl:attribute name="description">Generic KeyVal store for HPCC Applications</xsl:attribute>
                    <xsl:attribute name="name">HPCCApps</xsl:attribute>
                    <xsl:attribute name="default">true</xsl:attribute>
                </xsl:element>
                <xsl:element name="Store">
                    <xsl:attribute name="description">JWT token cache</xsl:attribute>
                    <xsl:attribute name="name">JWTAuth</xsl:attribute>
                    <xsl:attribute name="default">false</xsl:attribute>
                    <xsl:attribute name="maxValSize">32768</xsl:attribute>
                </xsl:element>
            </xsl:element>
        </EspService>
        <EspBinding name="{$bindName}" service="{$serviceName}" protocol="{$bindingNode/@protocol}" type="{$bindType}"
            plugin="{$servicePlugin}" netAddress="0.0.0.0" port="{$bindingNode/@port}">
            <xsl:call-template name="bindAuthentication">
                <xsl:with-param name="bindingNode" select="$bindingNode"/>
                <xsl:with-param name="authMethod" select="$authNode/@method"/>
                <xsl:with-param name="service" select="'ws_store'"/>
            </xsl:call-template>
        </EspBinding>
    </xsl:template>
   <xsl:template match="*" mode="copy">
      <xsl:copy>
         <xsl:apply-templates select="@*[string(.) != '']|node()" mode="copy"/>
      </xsl:copy>
   </xsl:template>

    <!-- ws_codesign -->
    <xsl:template match="EspService" mode="ws_codesign">
        <xsl:param name="bindingNode"/>
        <xsl:param name="authNode"/>

        <xsl:variable name="serviceType" select="'ws_codesign'"/>
        <xsl:variable name="serviceName" select="concat($serviceType, '_', @name, '_', $process)"/>
        <xsl:variable name="bindName" select="concat($serviceType, '_', $bindingNode/@name, '_', $process)"/>
        <xsl:variable name="bindType" select="'ws_codesignSoapBinding'"/>
        <xsl:variable name="servicePlugin">
            <xsl:call-template name="defineServicePlugin">
                <xsl:with-param name="plugin" select="'ws_codesign'"/>
            </xsl:call-template>
        </xsl:variable>
        <EspService name="{$serviceName}" type="{$serviceType}" plugin="{$servicePlugin}">
        </EspService>
        <EspBinding name="{$bindName}" service="{$serviceName}" protocol="{$bindingNode/@protocol}" type="{$bindType}"
            plugin="{$servicePlugin}" netAddress="0.0.0.0" port="{$bindingNode/@port}">
            <xsl:call-template name="bindAuthentication">
                <xsl:with-param name="bindingNode" select="$bindingNode"/>
                <xsl:with-param name="authMethod" select="$authNode/@method"/>
                <xsl:with-param name="service" select="'ws_codesign'"/>
            </xsl:call-template>
        </EspBinding>
    </xsl:template>
   <xsl:template match="*" mode="copy">
      <xsl:copy>
         <xsl:apply-templates select="@*[string(.) != '']|node()" mode="copy"/>
      </xsl:copy>
   </xsl:template>

    <!-- ws_logAccess -->
    <xsl:template match="EspService" mode="ws_logaccess">
        <xsl:param name="bindingNode"/>
        <xsl:param name="authNode"/>

        <xsl:variable name="serviceType" select="'ws_logaccess'"/>
        <xsl:variable name="serviceName" select="concat($serviceType, '_', @name, '_', $process)"/>
        <xsl:variable name="bindName" select="concat($serviceType, '_', $bindingNode/@name, '_', $process)"/>
        <xsl:variable name="bindType" select="'ws_logaccessSoapBinding'"/>
        <xsl:variable name="servicePlugin">
            <xsl:call-template name="defineServicePlugin">
                <xsl:with-param name="plugin" select="'ws_logaccess'"/>
            </xsl:call-template>
        </xsl:variable>
        <EspService name="{$serviceName}" type="{$serviceType}" plugin="{$servicePlugin}">
        </EspService>
        <EspBinding name="{$bindName}" service="{$serviceName}" protocol="{$bindingNode/@protocol}" type="{$bindType}"
            plugin="{$servicePlugin}" netAddress="0.0.0.0" port="{$bindingNode/@port}">
            <xsl:call-template name="bindAuthentication">
                <xsl:with-param name="bindingNode" select="$bindingNode"/>
                <xsl:with-param name="authMethod" select="$authNode/@method"/>
                <xsl:with-param name="service" select="'ws_logaccess'"/>
            </xsl:call-template>
        </EspBinding>
    </xsl:template>
   <xsl:template match="*" mode="copy">
      <xsl:copy>
         <xsl:apply-templates select="@*[string(.) != '']|node()" mode="copy"/>
      </xsl:copy>
   </xsl:template>
   
   <xsl:template match="@*" mode="copy">
      <xsl:if test="string(.) != ''">
         <xsl:copy/>
      </xsl:if>
   </xsl:template>
   
   <xsl:template match="Process" mode="copy">
      <xsl:variable name="processNameLowerCase" select="translate(@name, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz')"/>
      <xsl:variable name="processName">
         <xsl:choose>
            <xsl:when test="contains($processNameLowerCase, '.exe')">
               <xsl:value-of select="substring-before($processNameLowerCase, '.exe')"/>
            </xsl:when>
            <xsl:otherwise><xsl:value-of select="$processNameLowerCase"/></xsl:otherwise>
         </xsl:choose>
      </xsl:variable>
      <xsl:copy>
         <xsl:attribute name="name"><xsl:value-of select="$processName"/></xsl:attribute>
        <xsl:copy-of select="@remove"/>
      </xsl:copy>
   </xsl:template>
   
    <!-- Utility templates -->
    <xsl:template name="bindAuthentication">
        <xsl:param name="bindingNode"/>
        <xsl:param name="authMethod"/>
        <xsl:param name="service"/>

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
            <xsl:copy-of select="$bindingNode/@resourcesBasedn"/> <!--if binding has an ldap resourcebasedn specified then copy it out -->
            <xsl:copy-of select="$bindingNode/@workunitsBasedn"/> <!--if binding has an ldap workunitsbasedn specified then copy it out -->
            <xsl:for-each select="$bindingNode/Authenticate[@path='/']">
               <Location path="/" resource="{@resource}" required="{@access}" description="{@description}"/>
            </xsl:for-each>

            <xsl:for-each select="$bindingNode/AuthenticateFeature[@authenticate='Yes']">
               <xsl:if test="$service='ws_smc' or @service=$service">
                  <Feature name="{@name}" path="{@path}" resource="{@resource}" required="{@access}" description="{@description}"/>
               </xsl:if>
            </xsl:for-each>

            <xsl:if test="$service = 'ws_topology'"><!--also add MachineInfoAccess stuff for topology-->
               <xsl:for-each select="$bindingNode/AuthenticateFeature[@authenticate='Yes']">
                  <xsl:if test="starts-with(@path, 'MachineInfoAccess')">
                     <Feature path="{@path}" resource="{@resource}" required="{@access}" description="{@description}">
                        <xsl:copy-of select="*"/>
                     </Feature>
                  </xsl:if>
               </xsl:for-each>
            </xsl:if>

            <xsl:for-each select="$bindingNode/AuthenticateSetting[@include='Yes']">
               <xsl:if test="$service='ws_smc' or @service=$service">
                  <Setting path="{@path}" resource="{@resource}" description="{@description}"/>
               </xsl:if>
            </xsl:for-each>

            <xsl:if test="$service = 'ws_topology'"><!--also add MachineInfoAccess stuff for topology-->
               <xsl:for-each select="$bindingNode/AuthenticateSetting[@include='Yes']">
                  <xsl:if test="starts-with(@path, 'MachineInfoAccess')">
                     <Setting path="{@path}" resource="{@resource}" description="{@description}">
                        <xsl:copy-of select="*"/>
                     </Setting>
                  </xsl:if>
               </xsl:for-each>
            </xsl:if>         
         </Authenticate>
      </xsl:when>
      <xsl:when test="$authMethod='secmgrPlugin'">
         <Authenticate>
            <xsl:attribute name="method">
               <xsl:value-of select="$bindingNode/@type"/>
            </xsl:attribute>
            <xsl:copy-of select="$bindingNode/@resourcesBasedn"/>
            <xsl:copy-of select="$bindingNode/@workunitsBasedn"/>

            <xsl:for-each select="$bindingNode/Authenticate[@path='/']">
              <Location path="/" resource="{@resource}" required="{@access}" description="{@description}"/>
            </xsl:for-each>

            <xsl:for-each select="$bindingNode/AuthenticateFeature[@authenticate='Yes']">
               <xsl:if test="$service='ws_smc' or @service=$service">
                  <Feature name="{@name}" path="{@path}" resource="{@resource}" required="{@access}" description="{@description}"/>
               </xsl:if>
            </xsl:for-each>

            <xsl:if test="$service = 'ws_topology'">
               <xsl:for-each select="$bindingNode/AuthenticateFeature[@authenticate='Yes']">
                  <xsl:if test="starts-with(@path, 'MachineInfoAccess')">
                     <Feature path="{@path}" resource="{@resource}" required="{@access}" description="{@description}">
                        <xsl:copy-of select="*"/>
                     </Feature>
                  </xsl:if>
               </xsl:for-each>
            </xsl:if>

            <xsl:for-each select="$bindingNode/AuthenticateSetting[@include='Yes']">
               <xsl:if test="$service='ws_smc' or @service=$service">
                  <Setting path="{@path}" resource="{@resource}" description="{@description}"/>
               </xsl:if>
            </xsl:for-each>

            <xsl:if test="$service = 'ws_topology'">
               <xsl:for-each select="$bindingNode/AuthenticateSetting[@include='Yes']">
                  <xsl:if test="starts-with(@path, 'MachineInfoAccess')">
                     <Setting path="{@path}" resource="{@resource}" description="{@description}">
                        <xsl:copy-of select="*"/>
                     </Setting>
                  </xsl:if>
               </xsl:for-each>
            </xsl:if>
         </Authenticate>
      </xsl:when>
        </xsl:choose>
    </xsl:template>

    <xsl:template name="defineServicePlugin">
        <xsl:param name="plugin"/>            
        <xsl:choose>
            <xsl:when test="$isLinuxInstance"><xsl:value-of select="$plugin"/></xsl:when>
            <xsl:otherwise><xsl:value-of select="$plugin"/>.dll</xsl:otherwise>
        </xsl:choose>
    </xsl:template>

    <xsl:template name="GetPathName">
        <xsl:param name="path"/>
    <xsl:if test="contains($path, '\')">
        <xsl:variable name="prefix" select="substring-before($path, '\')"/>
            <xsl:value-of select="concat($prefix, '\')"/>
            <xsl:call-template name="GetPathName">
                <xsl:with-param name="path" select="substring-after($path, '\')"/>
            </xsl:call-template>
        </xsl:if>
    </xsl:template>
</xsl:stylesheet>
