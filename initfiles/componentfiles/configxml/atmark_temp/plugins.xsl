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
  xmlns:seisint="http://seisint.com" exclude-result-prefixes="seisint">
    <xsl:output method="xml" indent="yes"/>
    <xsl:param name="process" select="'eclserver'"/>
    <xsl:param name="instance" select="'2wd20'"/>
    <xsl:param name="isLinuxInstance" select="0"/>
    <xsl:param name="espServiceName" select="''"/>
  <xsl:variable name="pluginsPath">
     <xsl:variable name="path1">
        <xsl:choose>
           <xsl:when test="$espServiceNode">
              <xsl:value-of select="$espServiceNode/@pluginsPath"/>
           </xsl:when>
           <xsl:otherwise>
               <xsl:message terminate="yes">
                   ESP service '<xsl:value-of select="$espServiceName"/>' is undefined!
               </xsl:message>
           </xsl:otherwise>
        </xsl:choose>
     </xsl:variable>
     <xsl:variable name="path2" select="translate($path1, '/$', '\:')"/>
     <xsl:variable name="path">
        <xsl:choose>
            <xsl:when test="starts-with($path2, '.\')">
                    <xsl:value-of select="substring($path2, 3)"/><!--skip .\ prefix -->
            </xsl:when>
            <xsl:otherwise>
               <xsl:value-of select="$path2"/>
            </xsl:otherwise>
         </xsl:choose>
     </xsl:variable>
     <xsl:variable name="len" select="string-length($path)"/>
     <xsl:choose>
       <xsl:when test="$len > 0">
         <xsl:if test="$isLinuxInstance = 0">
           <xsl:if test="starts-with($path, '\') or ($len>1 and substring($path, 2, 1) = ':')">
             <xsl:message terminate="yes">
               Plugins path for '<xsl:value-of select="$process"/>' must be a relative path!
             </xsl:message>
           </xsl:if>
         </xsl:if>
           <xsl:value-of select="$path"/>
           <xsl:if test="not(substring($path, number($len)-1, 1) = '\')">\</xsl:if>
       </xsl:when>
       <xsl:otherwise>plugins\</xsl:otherwise>
     </xsl:choose>
  </xsl:variable>
    <xsl:template match="PluginRef">
       <xsl:variable name="pluginName" select="@process"/>
       <xsl:variable name="pluginNode" select="/Environment/Software/PluginProcess[@name=$pluginName]"/>
       <xsl:if test="not($pluginNode)">
          <xsl:message terminate="yes">The plugin '<xsl:value-of select="$pluginName"/>' referenced by process '<xsl:value-of select="$process"/>' does not exist!</xsl:message>
       </xsl:if>
       <xsl:call-template name="generatePluginsPath">
          <xsl:with-param name="node" select="$pluginNode"/>
       </xsl:call-template>
       <!--handle custom plugins-->
       <xsl:if test="$pluginNode/InstallSet/File[1]"><!--custom plugin-->
          <xsl:variable name="basePath" select="translate($pluginNode/@basePath, '/', '\')"/>
          <xsl:variable name="len" select="string-length($basePath)"/>
          <xsl:if test="$len=0">
             <xsl:message terminate="yes">Base path is not specified for custom plugin '<xsl:value-of select="$pluginNode/@name"/></xsl:message>
          </xsl:if>
          <xsl:variable name="sourcePath">
             <xsl:value-of select="$basePath"/>
             <xsl:if test="not(substring($basePath, $len, 1)='\')">\</xsl:if>
          </xsl:variable>
       <xsl:apply-templates select="$pluginNode/InstallSet">
             <xsl:with-param name="sourcePath" select="$sourcePath"/>
       </xsl:apply-templates>
       </xsl:if>
    </xsl:template>
    <xsl:template name="generatePluginsPath">
       <xsl:param name="node"/>
       <xsl:variable name="buildSetPath">
          <xsl:call-template name="getBuildSetPath">
             <xsl:with-param name="build" select="$node/@build"/>
            <xsl:with-param name="buildSet" select="$node/@buildSet"/>
          </xsl:call-template>
       </xsl:variable>
     <xsl:variable name="buildSetFile">
       <xsl:call-template name="getBuildSetFile">
         <xsl:with-param name="build" select="$node/@build"/>
         <xsl:with-param name="buildSet" select="$node/@buildSet"/>
       </xsl:call-template>
     </xsl:variable>
     <xsl:variable name="installSetPath">
       <xsl:value-of select="concat('file:///', $buildSetPath, $buildSetFile)"/>
     </xsl:variable>
       <xsl:variable name="pluginInstallSet" select="document($installSetPath)"/>
       <xsl:if test="not($pluginInstallSet)">
          <xsl:message terminate="yes">Failed to open plugin buildset '<xsl:value-of select="$installSetPath"/>'!</xsl:message>
       </xsl:if>
       <xsl:apply-templates select="$pluginInstallSet/InstallSet">
         <xsl:with-param name="sourcePath" select="$buildSetPath"/>
       </xsl:apply-templates>
    </xsl:template>

   <xsl:template match="InstallSet">
   <xsl:param name="sourcePath"/>
      <xsl:variable name="getDefaultPlugins" select="@processName != 'PluginProcess'"/>
      <xsl:choose>
         <xsl:when test="$getDefaultPlugins">
            <xsl:apply-templates select="File" mode="DefaultPlugins">
               <xsl:with-param name="sourcePath" select="$sourcePath"/>
            </xsl:apply-templates>
         </xsl:when>
         <xsl:otherwise>
            <xsl:apply-templates select="File">
               <xsl:with-param name="sourcePath" select="$sourcePath"/>
            </xsl:apply-templates>
         </xsl:otherwise>
      </xsl:choose>
   </xsl:template>

    <xsl:template match="File" mode="DefaultPlugins">
       <xsl:param name="sourcePath"/>
       <xsl:variable name="name" select="translate(@name, '/', '\')"/>
     <xsl:variable name="srcPath">
        <xsl:value-of select="$sourcePath"/>
        <xsl:if test="string(@srcPath) != ''">
           <xsl:variable name="srcPathAttr"    select="translate(@srcPath, '/', '\')"/>
           <xsl:if test="starts-with($srcPathAttr, '\')">   <xsl:value-of select="substring(@srcPathAttr, 2)"/></xsl:if>
           <xsl:value-of select="$srcPathAttr"/>
           <xsl:if test="not(substring($srcPathAttr, string-length($srcPathAttr))='\')">\</xsl:if>
        </xsl:if>
     </xsl:variable>
       <xsl:variable name="destName">
          <xsl:choose>
         <xsl:when test="string(@destName) != ''"><xsl:value-of select="translate(@destName, '/', '\')"/></xsl:when>
         <xsl:otherwise><xsl:value-of select="translate(@name, '/', '\')"/></xsl:otherwise>
        </xsl:choose>
       </xsl:variable>
       <xsl:variable name="extension" select="substring-after($destName, '.')"/>
       <xsl:variable name="destPath">
        <xsl:if test="$extension != 'hql' and $extension != 'hqllib'">
           <xsl:value-of select="$pluginsPath"/>
        </xsl:if>
       </xsl:variable>
     <xsl:if test="$extension='hql' or $extension='hqllib' or (starts-with($name, 'plugins\') and ($extension='dll' or $extension='so' or $extension='lib'))">
           <xsl:call-template name="outputPluginInfo">
           <xsl:with-param name="name" select="$name"/>
              <xsl:with-param name="type"  select="$extension"/>
           <xsl:with-param name="srcPath" select="$srcPath"/>
           <xsl:with-param name="destName" select="@destName"/>
           <xsl:with-param name="destPath" select="$destPath"/>
            <xsl:with-param name="method" select="@method"/>
        </xsl:call-template>
      </xsl:if>
    </xsl:template>
    <xsl:template match="File">
       <xsl:param name="sourcePath"/>
     <xsl:variable name="srcPath">
        <xsl:value-of select="$sourcePath"/>
        <xsl:if test="string(@srcPath) != ''">
           <xsl:variable name="srcPathAttr"    select="translate(@srcPath, '/', '\')"/>
           <xsl:if test="starts-with($srcPathAttr, '\')">   <xsl:value-of select="substring(@srcPathAttr, 2)"/></xsl:if>
           <xsl:value-of select="$srcPathAttr"/>
           <xsl:if test="not(substring($srcPathAttr,  string-length($srcPathAttr))='\')">\</xsl:if>
        </xsl:if>
     </xsl:variable>
       <xsl:variable name="extension" select="substring-after(@name, '.')"/>
       <xsl:if test="$extension='dll' or $extension='so' or $extension='lib'">
          <xsl:call-template name="outputPluginInfo">
             <xsl:with-param name="name" select="@name"/>
             <xsl:with-param name="type"  select="$extension"/>
             <xsl:with-param name="srcPath" select="$srcPath"/>
             <xsl:with-param name="destName" select="@destName"/>
             <xsl:with-param name="destPath" select="$pluginsPath"/>
             <xsl:with-param name="method" select="@method"/>
          </xsl:call-template>
     </xsl:if>
    </xsl:template>


   <xsl:template name="outputPluginInfo">
   <xsl:param name="name"/><!--this can have a directory prefix like plugin\xyz.dll but any / converted to \ -->
   <xsl:param name="type"/>
   <xsl:param name="srcPath"/>
   <xsl:param name="destName"/>
   <xsl:param name="destPath"/>
   <xsl:param name="method"/>
      <Plugin>
         <xsl:variable name="method2">
            <xsl:choose>
               <xsl:when test="string($method) !=''">
                  <xsl:value-of select="$method"/>
               </xsl:when>
               <xsl:otherwise>copy</xsl:otherwise>
            </xsl:choose>
         </xsl:variable>
       <xsl:variable name="srcName">
          <xsl:choose>
                 <xsl:when test="contains($name, '\')"><xsl:value-of select="substring-after($name, '\')"/></xsl:when>
                 <xsl:otherwise><xsl:value-of select="$name"/></xsl:otherwise>
              </xsl:choose>
       </xsl:variable>
       <xsl:variable name="destName2">
          <xsl:choose>
              <xsl:when test="string($destName) != ''"><xsl:value-of select="translate($destName, '/', '\')"/></xsl:when>
              <xsl:otherwise><xsl:value-of select="$srcName"/></xsl:otherwise>
           </xsl:choose>
       </xsl:variable>
         <xsl:attribute name="name"><xsl:value-of select="$srcName"/></xsl:attribute>
         <xsl:attribute name="type"><xsl:value-of select="$type"/></xsl:attribute>
         <xsl:attribute name="method"><xsl:value-of select="$method2"/></xsl:attribute>
         <xsl:attribute name="destName"><xsl:value-of select="$destName2"/></xsl:attribute>
         <xsl:if test="string($srcPath) != ''">
            <xsl:variable name="srcPath2">
               <xsl:value-of select="$srcPath"/>
               <xsl:if test="contains($name, '\')">
                  <xsl:variable name="pathName">
                     <xsl:call-template name="GetPathName">
                        <xsl:with-param name="path" select="$name"/>
                     </xsl:call-template>
                  </xsl:variable>
                  <xsl:value-of select="$pathName"/>
                  <xsl:if test="not(substring($pathName, string-length($pathName))='\')">\</xsl:if>
               </xsl:if>
            </xsl:variable>
            <xsl:attribute name="srcPath"><xsl:value-of select="$srcPath2"/></xsl:attribute>
            <xsl:if test="not(function-available('seisint:addDeploymentFile'))">
               <xsl:message terminate="yes">This XSL transformation can only be run by the Seisint Deployment Tool!</xsl:message>
            </xsl:if>
            <!--ask deployment tool to add this plugin file to be copied if copying files-->
            <!--format is: method+name+sourcePath+destName[+destPath]-->
            <xsl:variable name="parameter">
               <xsl:value-of select="$method2"/><xsl:text>+</xsl:text>
               <xsl:value-of select="$srcName"/><xsl:text>+</xsl:text>
               <xsl:value-of select="$srcPath2"/><xsl:text>+</xsl:text>
               <xsl:value-of select="$destName2"/><xsl:text>+</xsl:text>
               <xsl:value-of select="$destPath"/><xsl:text>+</xsl:text>
               <xsl:value-of select="$eclServerName"/>
            </xsl:variable>
            <xsl:variable name="dummy" select="seisint:addDeploymentFile($parameter)"/>
         </xsl:if>
         <xsl:if test="string($destPath) != ''">
            <xsl:attribute name="destPath"><xsl:value-of select="$destPath"/></xsl:attribute>
         </xsl:if>
      </Plugin>
   </xsl:template>
   <xsl:template name="getBuildSetPath">
     <xsl:param name="build"/>
     <xsl:param name="buildSet"/>
       <xsl:variable name="buildNode" select="/Environment/Programs/Build[@name=$build]"/>
       <xsl:if test="not($buildNode)">
          <xsl:message terminate="yes">Invalid build '<xsl:value-of select="$build"/>'!</xsl:message>
       </xsl:if>
       <xsl:variable name="buildSetNode" select="$buildNode/BuildSet[@name=$buildSet]"/>
       <xsl:if test="not($buildSetNode)">
          <xsl:message terminate="yes">The build '<xsl:value-of select="$build"/>' does not have build set '<xsl:value-of select="$buildSet"/>'!</xsl:message>
       </xsl:if>
     <xsl:variable name="url" select="translate($buildNode/@url, '/', '\')"/>
     <xsl:value-of select="$url"/>
     <xsl:variable name="len" select="string-length($url)"/>
     <xsl:if test="$len > 0 and not(substring($url, $len, 1) = '\')">\</xsl:if>
     <xsl:variable name="path" select="translate($buildSetNode/@path, '/', '\')"/>
     <xsl:if test="$path">
        <xsl:value-of select="$path"/>
        <xsl:variable name="len2" select="string-length($path)"/>
        <xsl:if test="$len2 > 0 and not(substring($path, $len2, 1) = '\')">\</xsl:if>
     </xsl:if>
   </xsl:template>

  <xsl:template name="getBuildSetFile">
    <xsl:param name="build"/>
    <xsl:param name="buildSet"/>
    <xsl:variable name="buildNode" select="/Environment/Programs/Build[@name=$build]"/>
    <xsl:if test="not($buildNode)">
      <xsl:message terminate="yes">
        Invalid build '<xsl:value-of select="$build"/>'!
      </xsl:message>
    </xsl:if>
    <xsl:variable name="buildSetNode" select="$buildNode/BuildSet[@name=$buildSet]"/>
    <xsl:if test="not($buildSetNode)">
      <xsl:message terminate="yes">
        The build '<xsl:value-of select="$build"/>' does not have build set '<xsl:value-of select="$buildSet"/>'!
      </xsl:message>
    </xsl:if>
    <xsl:value-of select="$buildSetNode/@installSet"/>
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
    <xsl:template match="node()|@*"/>
</xsl:stylesheet>
