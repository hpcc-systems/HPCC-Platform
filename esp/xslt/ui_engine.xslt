<?xml version="1.0" encoding="UTF-8"?>
<!--
##############################################################################
#    Copyright (C) 2011 HPCC Systems.
#
#    All rights reserved. This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
##############################################################################
-->

<!DOCTYPE xsl:stylesheet [
    <!--define the HTML non-breaking space:-->
    <!ENTITY nbsp "<xsl:text disable-output-escaping='yes'>&amp;nbsp;</xsl:text>">
]>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <xsl:output method="html" encoding="utf-8"/>
  
  <xsl:variable name="debugMode" select="0"/>
<xsl:variable name="filePath">
    <xsl:choose>
        <xsl:when test="$debugMode">c:/development/bin/debug/files</xsl:when>
        <xsl:otherwise>/esp/files_</xsl:otherwise>
    </xsl:choose>
</xsl:variable>

<xsl:variable name="Component" select="/*[1]/Component/text()"/>
<xsl:variable name="Command" select="/*[1]/Command/text()"/>
<!--path for componentns.xml must be relative file path oblivious to esp-->
<xsl:variable name="ComponentsDoc" select="document('../files/components.xml')"/>
<xsl:variable name="componentNode" select="$ComponentsDoc/Components/*[name()=$Component]"/>
<xsl:variable name="commandNode" select="$componentNode/Commands/*[name()=$Command]"/>
<xsl:variable name="apos">'</xsl:variable>

<xsl:include href="./ui_overrides.xslt"/>

<xsl:variable name="argsNodeName">
    <xsl:choose>
        <xsl:when test="$commandNode/@argsNode">
            <xsl:value-of select="string($commandNode/@argsNode)"/>
        </xsl:when>
        <xsl:otherwise>Arguments</xsl:otherwise>
    </xsl:choose>
</xsl:variable>

<xsl:variable name="schemaRootNode" select="$commandNode[$argsNodeName='.'] | $commandNode/*[$argsNodeName!='.' and name()=$argsNodeName]"/>
<xsl:variable name="objRootNode" select="/NavMenuEventResponse/XmlArgument | /*[name() != 'NavMenuEventResponse']"/>

<xsl:template match="/">
    <xsl:if test="not($ComponentsDoc)">
        <xsl:message terminate="yes">Failed to load file components.xml.</xsl:message>
    </xsl:if>
    <html>
        <head>
            <title>
                <xsl:value-of select="$Command"/>
            </title>
      <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
      <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
      <link type="text/css" rel="StyleSheet" 
                href="{$filePath}/css/sortabletable.css"/>
            <link type="text/css" rel="StyleSheet" href="{$filePath}/css/tabs.css"/>
            <style type="text/css" media="screen">
                @import url( <xsl:value-of select="$filePath"/>/css/headerFooter.css );
            </style>
      <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
      <!--style type="text/css"> - not used any more but save for later
                    .row-table table {
                        table-layout:fixed;
                        font: 10pt arial, helvetica, sans-serif;
                        border-collapse:collapse;
                        padding:0;
                        spacing:0;
                        margin:0;
                    }
                    .row-table td {
                        padding:0;
                        spacing:0;
                        margin:0;
                        //border:1px solid gray;
                        border-collapse:collapse;
                        border:0;
                    }
                    .row-table tr {
                        padding:0;
                        spacing:0;
                        margin:0;
                        //border:0;
                        //border-collapse:collapse;
                    }
         </style-->
            <script type="text/javascript" src="{$filePath}/scripts/CMultiSelect.js">
            </script>
            <script type="text/javascript" src="{$filePath}/scripts/tooltip.js">
       </script>
            <script type="text/javascript" src="{$filePath}/popup.js">
            </script>
            <script type="text/javascript" src="{$filePath}/scripts/ui_engine.js">
       </script>
            <script type="text/javascript">
                <xsl:attribute name="src">
                    <xsl:value-of select="concat($filePath, '/scripts/')"/>
                    <xsl:choose>
                        <xsl:when test="not($commandNode/@nosort='true')">
                            <xsl:text>sortabletable.js</xsl:text>
                        </xsl:when>
                        <xsl:otherwise>
                            <xsl:text>fixedTables.js</xsl:text>
                        </xsl:otherwise>
                    </xsl:choose>
                </xsl:attribute>
            </script>
        </head>
    <body class="yui-skin-sam" onload="nof5();loadCommandInfo()" onmousemove="overhere()" bgcolor="white">
            <xsl:copy-of select="$commandNode/script"/>
            <xsl:if test="string($commandNode/@selectedTab)">
                <xsl:apply-templates select="$componentNode/TabContainer">
                    <xsl:with-param name="selectedTab" select="$commandNode/@selectedTab"/>
                </xsl:apply-templates>
            </xsl:if>
            <xsl:if test="string($commandNode/@schemaIsland)!='false'">
                <xsl:call-template name="makeSchemaDataIsland"/>
            </xsl:if>
            <xsl:if test="not($commandNode/@objIsland='')">
                <xsl:call-template name="makeArgsDataIsland">
                    <xsl:with-param name="elementName" select="string($commandNode/@objIsland)"/>
                </xsl:call-template>
            </xsl:if>
            <form method="post" onsubmit="return onSubmit()">
                <xsl:copy-of select="$schemaRootNode/@enctype"/>
                <xsl:attribute name="action">
                    <xsl:choose>
                        <xsl:when test="$commandNode/@action">
                            <xsl:value-of select="$commandNode/@action"/>
                        </xsl:when>
                        <xsl:otherwise>
                            <xsl:value-of select="concat('/', $Component, '/', $Command)"/>
                        </xsl:otherwise>
                    </xsl:choose>
                </xsl:attribute>
                <div id="pageBody">
                    <div id="ToolTip" style="position:absolute;left:0;top:0;visibility:hidden"/>                 
                    <xsl:apply-templates select="$componentNode">
                        <xsl:with-param name="ObjNode" select="$objRootNode"/>
                    </xsl:apply-templates>
                </div>
                <xsl:for-each select="$commandNode/Buttons[1]">
                    <div id="pageFooter">
                        <xsl:call-template name="createButtons">
                            <xsl:with-param name="btnsNode" select="."/>
                        </xsl:call-template>
                    </div>
                </xsl:for-each>
            </form>
        </body>
    </html>
</xsl:template>


<xsl:template match="TabContainer">
    <xsl:param name="selectedTab"/>
    <div id="pageHeader">
        <div id="tabContainer" width="100%">
            <ul id="tabNavigator">
                <xsl:for-each select="Tab">
                    <li>
                        <a href="{@url}">
                            <xsl:if test="@name=$selectedTab">
                                <xsl:attribute name="class">active</xsl:attribute>
                            </xsl:if>
                            <xsl:value-of select="@name"/>
                        </a>
                    </li>
                </xsl:for-each>
            </ul>
        </div>
    </div>
</xsl:template>


<xsl:template name="makeSchemaDataIsland">
  <xml id="xmlSchema" style="display:none;">
        <xsl:element name="Components">
            <xsl:element name="{$Component}">
                <xsl:apply-templates select="$componentNode/Defaults" mode="copy"/>
                <xsl:element name="Commands">
                    <xsl:for-each select="$commandNode">
                        <xsl:copy>
              <xsl:copy-of select="@*"/>
              <xsl:choose>
                                <xsl:when test="$argsNodeName='.'">
                  <xsl:copy-of select="*[name()!='Component' and name() != 'Command' and name()!='script']"/>
                                </xsl:when>
                                <xsl:otherwise>
                  <xsl:copy-of select="*[name()!='script' and name()!='Buttons']" />
                </xsl:otherwise>
                            </xsl:choose>
                        </xsl:copy>
                    </xsl:for-each>
                </xsl:element>
            </xsl:element>
        </xsl:element>
    </xml>
    <input type="hidden" name="xmlArgs" value=""/>
</xsl:template>

  <xsl:template name="makeArgsDataIsland">
<xsl:param name="elementName"/>
    <div style="display:none;">
      <xml id="xmlPrevArgsDoc" >
        <PrevArgsDoc>
          <xsl:for-each select="$objRootNode">
            <xsl:variable name="inputFragment" select="*[$argsNodeName!='.' and name() = $argsNodeName][1]"/>
            <xsl:choose>
              <xsl:when test="$elementName!=''">
                <xsl:copy-of select="*[name()=$elementName]"/>
              </xsl:when>
              <xsl:when test="$inputFragment">
                <xsl:copy-of select="$inputFragment"/>
              </xsl:when>
              <xsl:when test="$argsNodeName='.'">
                <xsl:element name="{$Command}">
                  <xsl:copy-of select="@*|*[name() != 'Component' and name() != 'Command']"/>
                </xsl:element>
              </xsl:when>
              <xsl:otherwise>
                <xsl:element name="{$argsNodeName}"/>
              </xsl:otherwise>
            </xsl:choose>
          </xsl:for-each>
        </PrevArgsDoc>
      </xml>
    </div>
</xsl:template>


<xsl:template match="Components/*">
    <xsl:param name="ObjNode"/>
    <xsl:if test="not($commandNode)">
        <xsl:message terminate="yes">Command '<xsl:value-of select="$Command"/>' is not defined in components.xml file!"/></xsl:message>
    </xsl:if>
    
    <input type="hidden" id="component" name="comp" value="{$Component}"/>
    <input type="hidden" id="command" name="command" value="{$Command}"/>
    
    <br/>
    <!--define a table that is dynamically filled up using DOM / DHTML using javascript --> 
    <xsl:if test="$commandNode/@caption">
        <h3 id="pageCaption">
            <xsl:value-of select="$commandNode/@caption"/>
        </h3>
    </xsl:if>
    <xsl:if test="string($commandNode/@subcaption)!=''">
        <h4 id="pageSubCaption">
            <xsl:value-of select="$commandNode/@subcaption"/>
        </h4>
    </xsl:if>               

   <xsl:for-each select="$commandNode">
        <xsl:if test="$schemaRootNode/@maxOccurs='unbounded'">
            <xsl:call-template name="createColumnHeaders">
                <xsl:with-param name="tableId" select="'ArgsTable'"/>
                <xsl:with-param name="rowId" select="'ArgsTable.header'"/>
                <xsl:with-param name="targetNode"   select="$objRootNode"/>
                <xsl:with-param name="schemaNode"   select="$schemaRootNode"/>
                <xsl:with-param name="xpath" select="''"/>
            </xsl:call-template>
        </xsl:if>
      <xsl:call-template name="populateTable">
            <xsl:with-param name="tableId" select="'ArgsTable'"/>
         <xsl:with-param name="schemaNode" select="$schemaRootNode"/>
         <xsl:with-param name="targetNode" select="$objRootNode"/>
         <xsl:with-param name="xPath" select="''"/>
      </xsl:call-template>
   </xsl:for-each>
</xsl:template>


<xsl:template name="populateTable">
<xsl:param name="tableId"/>
<xsl:param name="schemaNode"/>
<xsl:param name="targetNode"/>
<xsl:param name="xpath"/>
    <xsl:variable name="columnCount">
        <xsl:call-template name="countColumns">
            <xsl:with-param name="schemaNode" select="$schemaNode"/>
        </xsl:call-template>
    </xsl:variable>
    
    <!--display "- none -" if $targetNode is empty-->
    <xsl:variable name="name" select="name($schemaNode)"/>
    <xsl:choose>
        <xsl:when test="not($targetNode/*[name()=$name][1]) and $schemaNode/@maxOccurs='unbounded'">
            <tr>
                <td colspan="{$columnCount}">- none -</td>
            </tr>
        </xsl:when>
        <xsl:otherwise>
            <xsl:call-template name="addTableRows">
                <xsl:with-param name="tableId" select="$tableId"/>
                <xsl:with-param name="objNode" select="$targetNode"/>
                <xsl:with-param name="schemaNode" select="$schemaNode"/>
                <xsl:with-param name="xpath" select="$xpath"/>
                <xsl:with-param name="columnCount" select="$columnCount"/>
            </xsl:call-template>    
        </xsl:otherwise>
    </xsl:choose>
</xsl:template>

<xsl:template name="makeNextXPath">
<xsl:param name="schemaNode"/>
<xsl:param name="xpath"/>
    <xsl:if test="string($xpath)!=''">
        <xsl:value-of select="$xpath"/>
    </xsl:if>
    <xsl:if test="not($schemaNode/@nodeName='')"><!--if not(exists and is '')-->
        <xsl:if test="string($xpath)!=''">
            <xsl:text>.</xsl:text>
        </xsl:if>
        <xsl:choose>
            <xsl:when test="$schemaNode/@nodeName">
                <xsl:value-of select="$schemaNode/@nodeName"/>
            </xsl:when>
            <xsl:otherwise>
                <xsl:value-of select="name($schemaNode)"/>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:if>
</xsl:template>


<xsl:template name="createColumnHeaders">
<xsl:param name="tableId"/>
<xsl:param name="rowId"/>
<xsl:param name="targetNode"/>
<xsl:param name="schemaNode"/>
<xsl:param name="xpath"/>
<xsl:param name="nChildElements"/>

    <xsl:variable name="xpath2">
        <xsl:if test="name($schemaNode[1])!=name($schemaRootNode[1])">
            <xsl:call-template name="makeNextXPath">
                <xsl:with-param name="schemaNode" select="$schemaNode"/>
                <xsl:with-param name="xpath" select="$xpath"/>
            </xsl:call-template>
        </xsl:if>
    </xsl:variable>
    
    <xsl:variable name="maxOccurs" select="$schemaNode/@maxOccurs"/>
    <xsl:if test="$maxOccurs">      
        <xsl:variable name="numCheckColumn">
            <xsl:choose>
                <xsl:when test="string($maxOccurs)='unbounded' and string($schemaNode/@checkboxes)!='false'">1</xsl:when>
                <xsl:otherwise>0</xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
    
        <xsl:if test="$numCheckColumn=1">
            <th width="35">
                <xsl:choose>
                    <xsl:when test="string($schemaNode/@multiselect)='true'">
                        <xsl:attribute name="id">
                            <xsl:value-of select="concat($tableId, '_ms0_T')"/>
                        </xsl:attribute>
                        <input type="checkbox" value="1" title="Select or unselect all items">
                            <xsl:if test="$schemaNode/@checked">
                                <xsl:attribute name="checked">true</xsl:attribute>
                            </xsl:if>
                            <xsl:attribute name="onclick">
                                <xsl:value-of select="concat('ms_setAll(this, ', $apos, $tableId, $apos, ',0)')"/>
                            </xsl:attribute>
                            <xsl:if test="not($targetNode/*[name()=name($schemaNode)][2])">
                                <xsl:attribute name="disabled">true</xsl:attribute>
                            </xsl:if>                           
                        </input>
                    </xsl:when>
                    <xsl:otherwise>&nbsp;</xsl:otherwise>
                </xsl:choose>
                <script type="text/javascript">
                    var ms = ms_create('<xsl:value-of select="$tableId"/>', onRowCheck );
                    <xsl:if test="string($schemaNode/@multiselect)!='true'">
                        ms.b_singleSelect = true;
                    </xsl:if>
                </script>
            </th>
        </xsl:if>
                        
        
        <xsl:if test="string($maxOccurs)='unbounded' or string($xpath)!=''">
            <xsl:variable name="childObjNode" select="$targetNode/*[name()=name($schemaNode)][1]"/>
            <xsl:for-each select="$schemaNode/*[name()!='Buttons' and name()!='script']">
                <xsl:variable name="name" select="name()"/>
                <!--any of our siblings which is either a visible attribute or a node with the same holds a column so count them-->
                <xsl:choose>
                    <xsl:when test="string(@maxOccurs)='1'">
                        <xsl:choose>
                            <xsl:when test="@table">
                                <xsl:if test="@caption and not($argsNodeName='.' and name()=name($schemaRootNode))">
                                    <th>
                                        <xsl:copy-of select="@width"/>
                                        <xsl:value-of select="@caption"/>
                                    </th>
                                </xsl:if>
                            </xsl:when>
                            <xsl:otherwise>
                                <xsl:call-template name="createColumnHeaders">
                                    <xsl:with-param name="tableId" select="$tableId"/>
                                    <xsl:with-param name="rowId" select="$rowId"/>
                                    <xsl:with-param name="targetNode" select="$childObjNode"/>
                                    <xsl:with-param name="schemaNode" select="."/>
                                    <xsl:with-param name="xpath" select="$xpath2"/>
                                    <xsl:with-param name="nChildElements" select="$nChildElements"/>
                                </xsl:call-template>
                            </xsl:otherwise>
                        </xsl:choose>
                    </xsl:when>
                    <xsl:when test="not(@maxOccurs)"><!--attribute definition-->
                        <xsl:variable name="viewType" select="@viewType"/>
                        <xsl:if test="string($viewType)!='hidden'">
                            <xsl:variable name="caption">
                                <xsl:choose>
                                    <xsl:when test="@caption">
                                        <xsl:value-of select="@caption"/>
                                    </xsl:when>
                                    <xsl:otherwise>
                                        <xsl:value-of select="$name"/>
                                    </xsl:otherwise>
                                </xsl:choose>
                            </xsl:variable>
                            
                            <th>
                                <xsl:copy-of select="@width"/>
                                <xsl:variable name="nodeName" select="name()"/>
                                <xsl:variable name="dataNode" select="$childObjNode/*[name()=$nodeName][1]"/>
                                <xsl:variable name="nRows" select="count($childObjNode/*[name()=current()/@name])"/>
                                <xsl:variable name="popupPrefix" select="concat('return showColumnPopup(', $apos, $tableId, $apos, ', this.cellIndex')"/>
                                <xsl:choose>
                                    <xsl:when test="($nRows=0 or $nRows>1) and string(@multiselect)='true'">
                                        <xsl:variable name="msId" select="concat($tableId, '_ms', string(@column), '_T')"/>
                                        <xsl:attribute name="id">
                                            <xsl:value-of select="$msId"/>
                                        </xsl:attribute>
                                        <xsl:attribute name="oncontextmenu">
                                            <xsl:value-of select="concat($popupPrefix, ', toggleMultiSelect)')"/>
                                        </xsl:attribute>
                                        <xsl:value-of select="$caption"/>
                                        <xsl:call-template name="createInputControlForNode">
                                            <xsl:with-param name="tableId" select="$tableId"/>
                                            <xsl:with-param name="rowId" select="$rowId"/>
                                            <xsl:with-param name="idPrefix" select="$xpath"/>
                                            <xsl:with-param name="node" select="$dataNode"/>
                                            <xsl:with-param name="schemaNode" select="."/>
                                            <xsl:with-param name="dataType" select="@dataType"/>
                                            <xsl:with-param name="viewType" select="@viewType"/>
                                            <xsl:with-param name="multiselect" select="1"/>
                                            <xsl:with-param name="columnHeader" select="1"/>
                                        </xsl:call-template>
                                    </xsl:when>
                                    <xsl:otherwise>
                                        <xsl:attribute name="oncontextmenu"><xsl:value-of select="$popupPrefix"/>)</xsl:attribute>
                                        <xsl:value-of select="$caption"/>
                                    </xsl:otherwise>
                                </xsl:choose>
                            </th>
                        </xsl:if>
                    </xsl:when><!--attribute definition-->
                </xsl:choose>
            </xsl:for-each>
        </xsl:if>
    </xsl:if><!--maxOccurs is defined-->
</xsl:template>


<xsl:template name="tallyColumns">
<xsl:param name="schemaNode"/>
    <xsl:choose>
        <xsl:when test="not($schemaNode/@maxOccurs)">
            <xsl:if test="string($schemaNode/@viewType)!='hidden'">1</xsl:if>
        </xsl:when>
        <xsl:when test="string($schemaNode/@maxOccurs)='1'">
            <xsl:for-each select="$schemaNode/*[name()!='Buttons' and name()!='script']">
                <xsl:call-template  name="tallyColumns">
                    <xsl:with-param name="schemaNode" select="."/>
                </xsl:call-template>
            </xsl:for-each>
        </xsl:when>
        <xsl:otherwise><!--maxOccurs='unbounded'-->
            <xsl:if test="string($schemaNode/@checkboxes)!='false'">1</xsl:if>
        </xsl:otherwise>
    </xsl:choose>
</xsl:template>

<xsl:template name="countColumns">
<xsl:param name="schemaNode"/>
    <xsl:variable name="columnTally">
        <xsl:if test="string($schemaNode/@maxOccurs)='unbounded' and string($schemaNode/@checkboxes)!='false'">
            <xsl:text>1</xsl:text>
        </xsl:if>
        <xsl:for-each select="$schemaNode/*[name()!='Buttons' and name()!='script' and not(@break)]">
            <xsl:call-template name="tallyColumns">
                <xsl:with-param name="schemaNode" select="."/>
            </xsl:call-template>
        </xsl:for-each>
    </xsl:variable>
    <xsl:choose>
        <xsl:when test="$columnTally=''">1</xsl:when>
        <xsl:otherwise>
            <xsl:value-of select="string-length($columnTally)"/>
        </xsl:otherwise>
    </xsl:choose>       
</xsl:template>

<xsl:template name="createButtons">
<xsl:param name="btnsNode"/>
    <center>
        <xsl:for-each select="$btnsNode/*">
            <xsl:if test="position()!=1">&nbsp;</xsl:if>
            <xsl:copy>
                <xsl:copy-of select="@*[name()!='override']"/>
                <xsl:if test="@override">
                    <xsl:apply-templates select="." mode="override"/>
                </xsl:if>
                <xsl:apply-templates select="*|text()" mode="copy"/>
            </xsl:copy>
        </xsl:for-each>
    </center>
</xsl:template>

<xsl:template name="createFixedTableWithHeader">
<xsl:param name="schemaNode"/>
<xsl:param name="targetNode"/>
<xsl:param name="tableId"/>
<xsl:param name="xpath"/>
<xsl:param name="width"/>
    <xsl:choose>
        <xsl:when test="not($schemaNode/@splitTable='false')">
            <xsl:if test="$schemaNode/@maxOccurs='unbounded' or $xpath!=''">
                <div class="TableHeaderDiv" id="DH.{$tableId}">
                    <script type="text/javascript">
                       a_fixedTableNames.push('<xsl:value-of select="$tableId"/>');
                    </script>
                    <table id="H.{$tableId}">
                        <xsl:if test="$width and string($width)!='100%'">
                            <xsl:attribute name="style">table-layout:fixed</xsl:attribute>
                            <xsl:attribute name="width"><xsl:value-of select="$width"/></xsl:attribute>
                        </xsl:if>
                        <thead>
                            <tr>
                                <xsl:call-template name="createColumnHeaders">
                                    <xsl:with-param name="tableId" select="$tableId"/>
                                    <xsl:with-param name="rowId" select="concat($tableId, '.header')"/>
                                    <xsl:with-param name="targetNode" select="$targetNode"/>
                                    <xsl:with-param name="schemaNode" select="$schemaNode"/>
                                    <xsl:with-param name="xpath" select="$xpath"/>
                                </xsl:call-template>
                            </tr>
                        </thead>
                    </table>
                </div>
            </xsl:if>
            <div class="TableBodyDiv" id="DB.{$tableId}">
                <table id="{$tableId}">
                    <xsl:if test="$width and string($width)!='100%'">
                        <xsl:attribute name="style">table-layout:fixed</xsl:attribute>
                        <xsl:attribute name="width"><xsl:value-of select="$width"/></xsl:attribute>
                    </xsl:if>
                    <tbody>
                        <xsl:call-template name="populateTable">
                            <xsl:with-param name="tableId" select="$tableId"/>
                            <xsl:with-param name="targetNode" select="$targetNode"/>
                            <xsl:with-param name="schemaNode" select="$schemaNode"/>
                            <xsl:with-param name="xpath" select="$xpath"/>
                        </xsl:call-template>
                    </tbody>
                </table>
            </div>
        </xsl:when>
        <xsl:otherwise>
            <table id="{$tableId}" class="sort-table">
                <xsl:if test="$width and string($width)!='100%'">
                    <xsl:attribute name="style">table-layout:fixed</xsl:attribute>
                    <xsl:attribute name="width"><xsl:value-of select="$width"/></xsl:attribute>
                </xsl:if>
                <thead>
                    <xsl:if test="$schemaNode/@maxOccurs='unbounded' or $xpath!=''">
                        <tr style="background:lightgrey">
                            <xsl:call-template name="createColumnHeaders">
                                <xsl:with-param name="tableId" select="$tableId"/>
                                <xsl:with-param name="rowId" select="concat($tableId, '.header')"/>
                                <xsl:with-param name="targetNode" select="$targetNode"/>
                                <xsl:with-param name="schemaNode" select="$schemaNode"/>
                                <xsl:with-param name="xpath" select="$xpath"/>
                            </xsl:call-template>
                        </tr>
                    </xsl:if>
                </thead>
                <tbody>
                    <xsl:call-template name="populateTable">
                        <xsl:with-param name="tableId" select="$tableId"/>
                        <xsl:with-param name="targetNode" select="$targetNode"/>
                        <xsl:with-param name="schemaNode" select="$schemaNode"/>
                        <xsl:with-param name="xpath" select="$xpath"/>
                    </xsl:call-template>
                </tbody>
            </table>
        </xsl:otherwise>
    </xsl:choose>
</xsl:template>

<xsl:template name="createFixedTable">
<xsl:param name="schemaNode"/>
<xsl:param name="targetNode"/>
<xsl:param name="id"/>
<xsl:param name="tableId"/>
<xsl:param name="xpath"/>
<xsl:param name="columnCount"/>
    <xsl:if test="not($schemaNode/@splitTable='false')">
        <xsl:text disable-output-escaping="yes">&lt;div class="TableBodyDiv" id="DB.</xsl:text>
        <xsl:value-of select="$id"/>
        <xsl:text disable-output-escaping="yes">"&gt;</xsl:text>
        <script type="text/javascript">
           a_fixedTableNames.push('<xsl:value-of select="$id"/>');
        </script>
    </xsl:if>
    <table id="{$id}">
        <xsl:copy-of select="$schemaNode/@width|$schemaNode/@style"/>
        <tbody>
            <xsl:for-each select="$schemaNode/*[name()!='Buttons' and name()!='script']">
                <xsl:call-template name="addTableRows">
                    <xsl:with-param name="tableId" select="$tableId"/>
                    <xsl:with-param name="objNode" select="$targetNode"/>
                    <xsl:with-param name="schemaNode" select="."/>
                    <xsl:with-param name="xpath" select="$xpath"/>
                    <xsl:with-param name="columnCount" select="$columnCount"/>
                </xsl:call-template>
            </xsl:for-each>
        </tbody>
    </table>
    <xsl:if test="not($schemaNode/@splitTable='false')">
        <xsl:text disable-output-escaping="yes">&lt;/div&gt;</xsl:text>
    </xsl:if>
</xsl:template>

<xsl:template name="addTableRows">
<xsl:param name="tableId"/>
<xsl:param name="objNode"/>
<xsl:param name="schemaNode"/>
<xsl:param name="xpath"/>
<xsl:param name="columnCount"/>
<xsl:param name="showTableCaption" select="1"/>
    <xsl:variable name="name" select="name($schemaNode)"/>
    <xsl:variable name="maxOccurs" select="$schemaNode/@maxOccurs"/>
    <xsl:variable name="xpath2">
        <xsl:if test="name($schemaNode)!=name($schemaRootNode) or $argsNodeName!='Arguments'">
            <xsl:call-template name="makeNextXPath">
                <xsl:with-param name="schemaNode" select="$schemaNode"/>
                <xsl:with-param name="xpath" select="$xpath"/>
            </xsl:call-template>
        </xsl:if>
    </xsl:variable>
    
    <xsl:choose>
        <xsl:when test="string($maxOccurs)='unbounded'">
            <xsl:choose>
                <xsl:when test="string($tableId)!=string($xpath2)">
                    <xsl:if test="$schemaNode/@colspan">
                        <xsl:text disable-output-escaping="yes">&lt;tr&gt;&lt;td colspan="</xsl:text>
                        <xsl:value-of select="$schemaNode/@colspan"/>
                        <xsl:text disable-output-escaping="yes">" style="text-align:left"&gt;</xsl:text>
                    </xsl:if>
                    <!-- create another table that would occupy entire cell of the parent table's row-->
                    <xsl:if test="$schemaNode/@break='true'">
                        <br/>
                    </xsl:if>
                    <xsl:if test="$schemaNode/@caption and not($argsNodeName='.' and name($schemaNode)=name($schemaRootNode))">
                        <b align="left"><xsl:value-of select="$schemaNode/@caption"/></b>
                    </xsl:if>
                    <xsl:variable name="tableWidth">
                        <xsl:choose>
                            <xsl:when test="$schemaNode/@width and string($schemaNode/@width)!='100%'">
                                <xsl:choose>
                                    <xsl:when test="not($schemaNode/@checkboxes='false')">
                                        <xsl:value-of select="number($schemaNode/@width)+35"/>
                                    </xsl:when>
                                    <xsl:otherwise>
                                        <xsl:value-of select="number($schemaNode/@width)"/>
                                    </xsl:otherwise>
                                </xsl:choose>
                            </xsl:when>
                            <xsl:otherwise>
                                <xsl:attribute name="width">100%</xsl:attribute>        
                            </xsl:otherwise>
                        </xsl:choose>
                    </xsl:variable>
                    <xsl:call-template name="createFixedTableWithHeader">
                        <xsl:with-param name="schemaNode" select="$schemaNode"/>
                        <xsl:with-param name="targetNode" select="$objNode"/>
                        <xsl:with-param name="tableId" select="$xpath2"/>
                        <xsl:with-param name="xpath" select="$xpath"/>
                        <xsl:with-param name="width" select="$tableWidth"/>
                    </xsl:call-template>
                    <xsl:if test="$schemaNode/../@table">
                        <xsl:text disable-output-escaping="yes">&lt;/td&gt;&lt;/tr&gt;</xsl:text>
                    </xsl:if>
                    <!--xsl:call-template name="createButtons">
                        <xsl:with-param name="schemaNode" select="$schemaNode"/>
                        <xsl:with-param name="targetNode" select="$objNode"/>
                    </xsl:call-template-->
                </xsl:when>
                <xsl:otherwise><!--tableId == xpath2-->
                    <xsl:variable name="rowCheckbox" select="string($schemaNode/@checkboxes)!='false'"/>
                    <xsl:variable name="checked" select="string($schemaNode/@checked)='true'"/>
                    <xsl:variable name="itemListValue">
                        <xsl:text>+</xsl:text>
                        <xsl:for-each select="$objNode/*[name()=$name]">
                            <xsl:value-of select="position()-1"/>
                            <xsl:text>+</xsl:text>
                        </xsl:for-each>
                    </xsl:variable>
                    <input type="hidden" id="{$tableId}.itemlist" 
                        name="{$tableId}.itemlist" value="{$itemListValue}"/>
                    
                    <xsl:for-each select="$objNode/*[name()=$name]">
                        <xsl:variable name="rowId" select="concat($xpath2, '.', string(position()-1))"/>
                        <tr id="{$rowId}">
                            <xsl:if test="string($schemaNode/@hover)!='false'">
                                <xsl:attribute name="onmouseenter">this.bgColor='#F0F0FF'</xsl:attribute>
                                <xsl:choose>
                                    <xsl:when test="position() mod 2">
                                        <xsl:attribute name="bgColor">#FFFFFF</xsl:attribute>
                                        <xsl:attribute name="onmouseleave">this.bgColor = '#FFFFFF'</xsl:attribute>
                                    </xsl:when>
                                    <xsl:otherwise>
                                        <xsl:attribute name="bgColor">#F0F0F0</xsl:attribute>
                                        <xsl:attribute name="onmouseleave">this.bgColor = '#F0F0F0'</xsl:attribute>
                                    </xsl:otherwise>
                                </xsl:choose>
                            </xsl:if>
                            <!--xsl:choose> - not used any more but save for future
                                <xsl:when test="$schemaNode/@tableRows">
                                    <td colspan="{$columnCount}" class="row-table">
                                        <table border="0" class="row-table" width="100%">
                                            <tbody>
                                                <tr>
                                                    <xsl:call-template name="populateTableRow">
                                                        <xsl:with-param name="tableId" select="$tableId"/>
                                                        <xsl:with-param name="rowId" select="$rowId"/>
                                                        <xsl:with-param name="domNode" select="."/>
                                                        <xsl:with-param name="schemaNode" select="$schemaNode"/>
                                                        <xsl:with-param name="xpath" select="$xpath2"/>
                                                        <xsl:with-param name="bRowCheckbox" select="$rowCheckbox"/>
                                                        <xsl:with-param name="bChecked" select="$checked"/>
                                                    </xsl:call-template>
                                                </tr>
                                            </tbody>
                                        </table>
                                        <xsl:variable name="objNode2" select="."/>
                                        <xsl:for-each select="$schemaNode/*[@break]">
                                            <br/>
                                            <xsl:variable name="columnCount2">
                                                <xsl:call-template name="countColumns">
                                                    <xsl:with-param name="schemaNode" select="."/>
                                                </xsl:call-template>
                                            </xsl:variable>
                                            <xsl:call-template name="addTableRows">
                                                <xsl:with-param name="tableId" select="$tableId"/>
                                                <xsl:with-param name="objNode" select="$objNode2"/>
                                                <xsl:with-param name="schemaNode" select="."/>
                                                <xsl:with-param name="xpath" select="$rowId"/>
                                                <xsl:with-param name="columnCount" select="$columnCount2"/>
                                            </xsl:call-template>
                                        </xsl:for-each>
                                    </td>
                                </xsl:when>
                                <xsl:otherwise-->
                                    <xsl:call-template name="populateTableRow">
                                        <xsl:with-param name="tableId" select="$tableId"/>
                                        <xsl:with-param name="rowId" select="$rowId"/>
                                        <xsl:with-param name="domNode" select="."/>
                                        <xsl:with-param name="schemaNode" select="$schemaNode"/>
                                        <xsl:with-param name="xpath" select="$xpath2"/>
                                        <xsl:with-param name="bRowCheckbox" select="$rowCheckbox"/>
                                        <xsl:with-param name="bChecked" select="$checked"/>
                                    </xsl:call-template>
                                <!--/xsl:otherwise>
                            </xsl:choose-->
                        </tr>
                    </xsl:for-each>                 
                </xsl:otherwise>
            </xsl:choose>
        </xsl:when>
        
        <xsl:when test="string($maxOccurs)='1' or name($schemaNode)=name($schemaRootNode)">
            <xsl:variable name="rootName" select="name($schemaRootNode)"/>
            <xsl:variable name="targetNode" 
            select="$objNode/*[name()=$name and ($argsNodeName!='.' or $name!=$rootName)][1]|$objNode[$argsNodeName='.' and $name=$rootName]"/>
            <xsl:if test="not($targetNode)">
                <!--TODO-->
            </xsl:if>
            <xsl:choose>
                <xsl:when test="$schemaNode/@table">
                    <!--create another table that would occupy entire cell of parent table's row    -->
                    <xsl:variable name="nestedTableWidth">
                        <xsl:choose>
                            <xsl:when test="@width">
                                <xsl:value-of select="@width"/>
                            </xsl:when>
                            <xsl:otherwise>100%</xsl:otherwise>
                        </xsl:choose>
                    </xsl:variable>
                    <!--tr-->
                        <xsl:choose>
                            <xsl:when test="$schemaNode/../@maxOccurs='unbounded'">
                                <xsl:text disable-output-escaping="yes">&lt;td style="padding:0" width="</xsl:text>
                                <xsl:value-of select="$nestedTableWidth"/>
                                <xsl:text disable-output-escaping="yes">"&gt;</xsl:text>
                            </xsl:when>
                            <xsl:when test="$schemaNode/@break">
                                <br/>
                            </xsl:when>
                        </xsl:choose>
                        <xsl:variable name="tableId2">
                            <!--xsl:if test="not($schemaNode/@id='')"--><!--not(exists and is '')-->
                                <xsl:choose>
                                    <xsl:when test="$schemaNode/@id">
                                        <xsl:value-of select="$schemaNode/@id"/>
                                    </xsl:when>
                                    <xsl:otherwise>
                                        <xsl:value-of select="$xpath2"/>
                                    </xsl:otherwise>
                                </xsl:choose>
                            <!--/xsl:if-->
                        </xsl:variable>
                        <xsl:variable name="caption" select="$schemaNode/@caption"/>
                        <xsl:if test="$showTableCaption and $caption and not($argsNodeName='.' and name($schemaNode)=name($schemaRootNode))">
                            <b align="left"><xsl:value-of select="$caption"/></b>
                        </xsl:if>
                        <!--border="0" style="font: 10pt arial, helvetica, sans-serif;"-->
                        <xsl:call-template name="createFixedTable">
                            <xsl:with-param name="schemaNode" select="$schemaNode"/>
                            <xsl:with-param name="targetNode" select="$targetNode"/>
                            <xsl:with-param name="id" select="$tableId2"/>
                            <xsl:with-param name="tableId" select="$tableId"/>
                            <xsl:with-param name="xpath" select="$xpath2"/>
                            <xsl:with-param name="columnCount" select="$columnCount"/>
                        </xsl:call-template>
                        <xsl:if test="$schemaNode/@overrideCell='after'">
                            <xsl:apply-templates select="$schemaNode" mode="overrideCell">
                                <xsl:with-param name="objNode" select="$targetNode"/>
                                <xsl:with-param name="rowId" select="$xpath2"/>
                                <xsl:with-param name="columnHeader" select="0"/>
                            </xsl:apply-templates>
                        </xsl:if>                           
                        <xsl:if test="$schemaNode/../@maxOccurs='unbounded'">
                            <xsl:text disable-output-escaping="yes">&lt;/td&gt;</xsl:text>
                        </xsl:if>
                    <!--/tr-->
                </xsl:when>
                <xsl:otherwise>
                    <xsl:for-each select="$schemaNode/*[name()!='Buttons' and name()!='script']">
                        <xsl:call-template name="addTableRows">
                            <xsl:with-param name="tableId" select="$tableId"/>
                            <xsl:with-param name="objNode" select="$targetNode"/>
                            <xsl:with-param name="schemaNode" select="."/>
                            <xsl:with-param name="xpath" select="$xpath2"/>
                            <xsl:with-param name="columnCount" select="$columnCount"/>
                        </xsl:call-template>
                    </xsl:for-each>
                </xsl:otherwise>
            </xsl:choose>
        </xsl:when>
        
        <xsl:otherwise><!--an attribute-->
            <xsl:variable name="targetNode" select="$objNode/*[name()=$name][1]"/>
            <xsl:if test="not($targetNode)">
                <!--TODO-->
            </xsl:if>
            <xsl:variable name="rowId" select="concat($tableId, '.', name($schemaNode))"/>
            <xsl:choose>
                <xsl:when test="string($schemaNode/@viewType)!='hidden'">
                    <xsl:variable name="caption">
                        <xsl:choose>
                            <xsl:when test="$schemaNode/@caption">
                                <xsl:value-of select="$schemaNode/@caption"/>
                            </xsl:when>
                            <xsl:otherwise>
                                <xsl:value-of select="name($schemaNode)"/>
                            </xsl:otherwise>
                        </xsl:choose>
                    </xsl:variable>
                    <tr><!--id="{$rowId}.row" is this needed?-->
                        <th width="1%" style="border:0;text-align:left">
                            <xsl:copy-of select="$schemaNode/@noWrap"/>
                            <xsl:value-of select="$caption"/>
                        </th>
                        <td width="1%"><b>:</b></td>
                        <td>
                            <xsl:call-template name="insertAttribNodeInRow">
                                <xsl:with-param name="tableId" select="$tableId"/>
                                <xsl:with-param name="rowId" select="$rowId"/>
                                <xsl:with-param name="targetNode" select="$targetNode"/>
                                <xsl:with-param name="schemaNode" select="$schemaNode"/>
                                <xsl:with-param name="xpath" select="$xpath"/>
                                <xsl:with-param name="alignment" select="'left'"/>
                            </xsl:call-template>
                        </td>               
                    </tr>
                </xsl:when>
                <xsl:otherwise>
                    <xsl:call-template name="insertAttribNodeInRow">
                        <xsl:with-param name="tableId" select="$tableId"/>
                        <xsl:with-param name="rowId" select="$rowId"/>
                        <xsl:with-param name="targetNode" select="$targetNode"/>
                        <xsl:with-param name="schemaNode" select="$schemaNode"/>
                        <xsl:with-param name="xpath" select="$xpath"/>
                        <xsl:with-param name="alignment" select="'left'"/>
                    </xsl:call-template>
                </xsl:otherwise>
            </xsl:choose>
        </xsl:otherwise>
    </xsl:choose>
</xsl:template>


<xsl:template name="populateTableRow">
<xsl:param name="tableId"/>
<xsl:param name="rowId"/>
<xsl:param name="domNode"/>
<xsl:param name="schemaNode"/>
<xsl:param name="xpath"/>
<xsl:param name="bRowCheckbox"/>
<xsl:param name="bChecked"/>
    <xsl:variable name="nodeName" select="name($domNode)"/>
    <xsl:if test="$bRowCheckbox">
        <td width="35">
            <xsl:variable name="cbName" select="concat($xpath, '.checkbox')"/>
            <input type="checkbox" name="{$cbName}" value="1">
                <xsl:if test="$bChecked">
                    <xsl:attribute name="checked">true</xsl:attribute>
                </xsl:if>
                <xsl:attribute name="onclick">
                    <xsl:value-of select="concat('ms_onChange(this, ', $apos, $tableId, $apos, ', 0)')"/>
                </xsl:attribute>
            </input>
        </td>
    </xsl:if>
    
    <xsl:for-each select="$schemaNode/*[name()!='Buttons' and name()!='script' and not(@break)]">
        <xsl:choose>
            <xsl:when test="@table">
                <xsl:call-template name="addTableRows">
                    <xsl:with-param name="tableId" select="concat($rowId, '.', name())"/>
                    <xsl:with-param name="objNode" select="$domNode"/>
                    <xsl:with-param name="schemaNode" select="."/>
                    <xsl:with-param name="xpath" select="$rowId"/>
                    <xsl:with-param name="columnCount" select="1"/>
                    <xsl:with-param name="showTableCaption" select="0"/>
                </xsl:call-template>
            </xsl:when>
            <xsl:otherwise>
                <xsl:call-template name="insertCellInRow">
                    <xsl:with-param name="tableId" select="$tableId"/>
                    <xsl:with-param name="rowId" select="$rowId"/>
                    <xsl:with-param name="targetNode" select="$domNode"/>
                    <xsl:with-param name="schemaNode" select="."/>
                    <xsl:with-param name="xpath" select="$rowId"/>
                </xsl:call-template>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:for-each>
    <xsl:if test="$schemaNode/@overrideRow='after'">
        <xsl:apply-templates select="$schemaNode" mode="overrideRow">
            <xsl:with-param name="objNode" select="$domNode"/>
            <xsl:with-param name="rowId" select="$rowId"/>
            <xsl:with-param name="columnHeader" select="0"/>
        </xsl:apply-templates>
    </xsl:if>
</xsl:template>


<xsl:template name="insertCellInRow">
<xsl:param name="tableId"/>
<xsl:param name="rowId"/>
<xsl:param name="targetNode"/>
<xsl:param name="schemaNode"/>
<xsl:param name="xpath"/>
    <xsl:variable name="maxOccurs" select="$schemaNode/@maxOccurs"/>
    <xsl:choose>
        <xsl:when test="string($maxOccurs)='unbounded'">
            <!--create another table that would occupy entire cell of parent table's row-->
            <xsl:variable name="xpath2">
                <xsl:if test="string($xpath)!=''">
                    <xsl:value-of select="$xpath"/>
                    <xsl:text>.</xsl:text>
                </xsl:if>
                <xsl:value-of select="name($schemaNode)"/>
            </xsl:variable>
            <td style="padding:0;margin:0">
                <xsl:call-template name="createFixedTableWithHeader">
                    <xsl:with-param name="schemaNode" select="$schemaNode"/>
                    <xsl:with-param name="targetNode" select="$targetNode"/>
                    <xsl:with-param name="tableId" select="$xpath2"/>
                    <xsl:with-param name="xpath" select="$xpath"/>
                    <xsl:with-param name="width" select="'100%'"/>
                </xsl:call-template>
                <!--border="0" cellspacing="0"-->
                <!--xsl:call-template name="createButtons">
                    <xsl:with-param name="schemaNode" select="$schemaNode"/>
                    <xsl:with-param name="targetNode" select="$targetNode"/>
                </xsl:call-template-->
            </td>
        </xsl:when><!--unbounded-->
        
        <xsl:when test="string($maxOccurs)='1'">
            <xsl:variable name="name" select="name($schemaNode)"/>
            <xsl:variable name="childNode" select="$targetNode/*[name()=$name][1]"/>
            <xsl:if test="not($childNode)">
                <!--TODO-->
            </xsl:if>
            <xsl:variable name="xpath3">
                <xsl:if test="string($xpath)!=''">
                    <xsl:value-of select="$xpath"/>
                    <xsl:text>.</xsl:text>
                </xsl:if>
                <xsl:value-of select="$name"/>
            </xsl:variable>
            <xsl:for-each select="$schemaNode/*[name()!='Buttons' and name()!='script']">               
                <xsl:call-template name="insertCellInRow">
                    <xsl:with-param name="tableId" select="$tableId"/>
                    <xsl:with-param name="rowId" select="$rowId"/>
                    <xsl:with-param name="targetNode" select="$childNode"/>
                    <xsl:with-param name="schemaNode" select="."/>
                    <xsl:with-param name="xpath" select="$xpath3"/>
                </xsl:call-template>
            </xsl:for-each>
        </xsl:when>
        
        <xsl:otherwise><!--attribute or innerText-->
            <xsl:variable name="name" select="name($schemaNode)"/>
            <xsl:variable name="inner" select="$schemaNode/@dataType='innerText'"/>
            <xsl:variable name="childNode" 
                            select="$targetNode[$inner]|$targetNode/*[not($inner) and name()=$name]"/>
            <xsl:choose>
                <xsl:when test="string($schemaNode/@viewType)!='hidden'">
                    <td>
                        <xsl:copy-of select="$schemaNode/@width"/>
                        <xsl:call-template name="insertAttribNodeInRow">
                            <xsl:with-param name="tableId" select="$tableId"/>
                            <xsl:with-param name="rowId" select="$rowId"/>
                            <xsl:with-param name="targetNode" select="$childNode"/>
                            <xsl:with-param name="schemaNode" select="$schemaNode"/>
                            <xsl:with-param name="xpath" select="$xpath"/>
                        </xsl:call-template>
                    </td>
                </xsl:when>
                <xsl:otherwise>
                    <xsl:call-template name="insertAttribNodeInRow">
                        <xsl:with-param name="tableId" select="$tableId"/>
                        <xsl:with-param name="rowId" select="$rowId"/>
                        <xsl:with-param name="targetNode" select="$childNode"/>
                        <xsl:with-param name="schemaNode" select="$schemaNode"/>
                        <xsl:with-param name="xpath" select="$xpath"/>
                    </xsl:call-template>
                </xsl:otherwise>
            </xsl:choose>
        </xsl:otherwise>
    </xsl:choose>
</xsl:template>


<xsl:template name="insertAttribNodeInRow">
    <xsl:param name="tableId"/>
    <xsl:param name="rowId"/>
    <xsl:param name="targetNode"/>
    <xsl:param name="schemaNode"/>
    <xsl:param name="xpath"/>
    <xsl:param name="alignment"/>
        
    <xsl:for-each select="$schemaNode">
        <xsl:if test="string(@viewType)!='hidden'">
            <xsl:if test="@overrideCell='before'">
                <xsl:apply-templates select="." mode="overrideCell">
                    <xsl:with-param name="objNode" select="$targetNode"/>
                    <xsl:with-param name="rowId" select="$rowId"/>
                    <xsl:with-param name="columnHeader" select="0"/>
                </xsl:apply-templates>
            </xsl:if>                           
            <xsl:if test="string(@align)!='' or string($alignment)!=''">
                <xsl:attribute name="style"><!--we need to define style since @align cannot override sort-table's style-->
                    <xsl:text>text-align:</xsl:text>
                    <xsl:choose>
                        <xsl:when test="string(@align)!=''">
                            <xsl:value-of select="@align"/>
                        </xsl:when>
                        <xsl:otherwise>
                            <xsl:value-of select="$alignment"/>
                        </xsl:otherwise>
                    </xsl:choose>
                </xsl:attribute>
            </xsl:if>
            <xsl:copy-of select="@valign"/>
            <xsl:if test="@dataType!='boolean' and @dataType!='file'">
                <xsl:copy-of select="@nowrap|@width"/>              
            </xsl:if>
            <xsl:variable  name="isStatic" 
                                    select="starts-with(@viewType, 'static') or @viewType='tooltip'"/>
            <xsl:if test="$isStatic">
                <xsl:if test="@span">
                    <xsl:text disable-output-escaping="yes">&lt;span id="</xsl:text>
                    <xsl:value-of select="concat($xpath, '.', name(), '.span')"/>
                    <xsl:text disable-output-escaping="yes">"&gt;</xsl:text>
                </xsl:if>           
                <xsl:choose>
                    <xsl:when test="@viewType='tooltip'">
                        <xsl:variable name="len" select="string-length($targetNode/text())"/>
                        <xsl:choose>
                            <xsl:when test="$len&lt;8">
                                <xsl:value-of select="$targetNode/text()"/>
                            </xsl:when>
                            <xsl:otherwise>
                                <xsl:attribute name="onmouseover">
                                    <xsl:text disable-output-escaping="yes">EnterContent('ToolTip', null, '</xsl:text>
                                    <xsl:value-of select="$targetNode/text()"/>
                                    <xsl:text disable-output-escaping="yes">', true; Activate();</xsl:text>
                                </xsl:attribute>
                                <xsl:attribute name="onmouseout">deActivate()</xsl:attribute>
                                <xsl:value-of select="substring(string($targetNode), 1, 4)"/>
                                <xsl:text>...</xsl:text>
                            </xsl:otherwise>
                        </xsl:choose>
                    </xsl:when><!--tooltip-->
                    <xsl:when test="@dataType='boolean'">
                        <xsl:value-of select="$targetNode/text()"/>
                    </xsl:when>
                    <xsl:when test="string($targetNode)!=''">
                        <xsl:choose>
                            <xsl:when test="contains(@viewType, ':')">
                                <xsl:variable name="options" select="substring-after(@viewType, ':')"/>
                                <xsl:call-template name="getIndexedOption">
                                    <xsl:with-param name="stringOptions" select="$options"/>
                                    <xsl:with-param name="index" select="number($targetNode)"/>
                                </xsl:call-template>
                            </xsl:when>
                            <xsl:otherwise>
                                <xsl:value-of select="$targetNode/text()"/>
                            </xsl:otherwise>
                        </xsl:choose>
                    </xsl:when>
                    <xsl:otherwise>&nbsp;</xsl:otherwise>
                </xsl:choose>
                <xsl:if test="@span">
                    <xsl:text>&lt;/span&gt;</xsl:text>
                </xsl:if>
            </xsl:if><!--$isStatic-->
        </xsl:if>
    
        <xsl:if test="string(@dataType) != 'none'">
            <xsl:call-template name="createInputControlForNode">
                <xsl:with-param name="tableId" select="$tableId"/>
                <xsl:with-param name="rowId" select="$rowId"/>
                <xsl:with-param name="idPrefix" select="$xpath"/>
                <xsl:with-param name="node" select="$targetNode"/>
                <xsl:with-param name="schemaNode" select="."/>
                <xsl:with-param name="dataType" select="@dataType"/>
                <xsl:with-param name="viewType" select="@viewType"/>
                <xsl:with-param name="columnHeader" select="0"/>
                <xsl:with-param name="multiselect" select="@multiselect='true'"/>
            </xsl:call-template>
        </xsl:if>   
        
        <xsl:if test="@overrideCell='after'">
            <xsl:apply-templates select="." mode="overrideCell">
                <xsl:with-param name="objNode" select="$targetNode"/>
                <xsl:with-param name="rowId" select="$rowId"/>
                <xsl:with-param name="columnHeader" select="0"/>
            </xsl:apply-templates>
        </xsl:if>                           
    </xsl:for-each>
</xsl:template>


<xsl:template name="getIndexedOption">
<xsl:param name="stringOptions"/>
<xsl:param name="index"/>
    <xsl:choose>
        <xsl:when test="$index=0">
            <xsl:choose>
                <xsl:when test="contains($stringOptions, '|')">
                    <xsl:value-of select="substring-before($stringOptions, '|')"/>              
                </xsl:when>
                <xsl:otherwise>
                    <xsl:value-of select="$stringOptions"/>
                </xsl:otherwise>
            </xsl:choose>           
        </xsl:when>
        <xsl:when test="$index=0">
            <xsl:value-of select="substring-before($stringOptions, '|')"/>
        </xsl:when>
        <xsl:otherwise>
            <xsl:call-template name="getIndexedOption">
                <xsl:with-param name="stringOptions" select="substring-after($stringOptions, '|')"/>
                <xsl:with-param name="index" select="number($index)-1"/>
            </xsl:call-template>
        </xsl:otherwise>
    </xsl:choose>

</xsl:template>


<xsl:template name="createInputControlForNode">
    <xsl:param name="tableId"/>
    <xsl:param name="rowId"/>
    <xsl:param name="idPrefix"/>
    <xsl:param name="node"/>
    <xsl:param name="schemaNode"/>
    <xsl:param name="dataType"/>
    <xsl:param name="viewType"/>
    <xsl:param name="multiselect"/>
    <xsl:param name="columnHeader"/>
    
    <xsl:variable name="id">
        <xsl:if test="string($idPrefix)!=''">
            <xsl:value-of select="$idPrefix"/>
        </xsl:if>
        <xsl:if test="name($schemaNode)!='innerText'">
            <xsl:if test="string($idPrefix)!=''">
                <xsl:text>.</xsl:text>
            </xsl:if>
            <xsl:value-of select="name($schemaNode)"/>
        </xsl:if>
    </xsl:variable>
    
    <xsl:variable name="value">
        <xsl:if test="not($columnHeader)">
            <xsl:choose>
                <xsl:when test="string($node/text())!=''">
                    <xsl:value-of select="$node/text()"/>
                </xsl:when>
                <xsl:otherwise>
                    <xsl:value-of select="$schemaNode/@default"/>
                </xsl:otherwise>
            </xsl:choose>
        </xsl:if>
    </xsl:variable>
    
    <xsl:variable name="type">
        <xsl:choose>
            <xsl:when test="$viewType='hidden'">hidden</xsl:when> 
            <xsl:when test="starts-with($viewType, 'static')">hidden</xsl:when>
            <xsl:when test="$viewType='tooltip'">hidden</xsl:when>
            <xsl:when test="$viewType='select'">select-one</xsl:when>
            <xsl:when test="$dataType='boolean'">checkbox</xsl:when>
            <xsl:when test="$viewType='showHideRowBtn'">hidden</xsl:when>
            <xsl:when test="$viewType='url'">hidden</xsl:when>
            <xsl:when test="$dataType='file'">file</xsl:when>
            <xsl:otherwise>text</xsl:otherwise>
        </xsl:choose>       
    </xsl:variable>
    
    <xsl:if test="$columnHeader and $type='hidden'">
        <xsl:message terminate="yes">
            <xsl:text>Multiple selection is not supported for columns with type </xsl:text>
            <xsl:value-of select="$viewType"/>
        </xsl:message>
    </xsl:if>
    
    <xsl:variable name="inputName">
        <xsl:choose>
            <xsl:when test="$viewType='select'">select</xsl:when>
            <xsl:otherwise>input</xsl:otherwise>
        </xsl:choose>
    </xsl:variable>
    
    <xsl:variable name="modifiedHandlerName">
        <xsl:choose>
            <xsl:when test="$type='checkbox' or $type='file'">onclick</xsl:when>
            <xsl:when test="$type='hidden'"></xsl:when>
            <xsl:otherwise>onchange</xsl:otherwise>
        </xsl:choose>
    </xsl:variable>
    
    <xsl:variable name="disabled">
        <xsl:if test="$type!='hidden'">
            <xsl:choose>
                <xsl:when test="string($schemaNode/@disabled)='1'">1</xsl:when>
                <xsl:when test="string($node/@disabled)='1'">1</xsl:when>
                <xsl:otherwise>0</xsl:otherwise>
            </xsl:choose>
        </xsl:if>
    </xsl:variable>
    
    <xsl:if test="$schemaNode/@override">
        <xsl:apply-templates select="$schemaNode" mode="override">
            <xsl:with-param name="objNode" select="$node"/>
            <xsl:with-param name="rowId" select="$rowId"/>
            <xsl:with-param name="columnHeader" select="$columnHeader"/>
        </xsl:apply-templates>
    </xsl:if>
    
    <xsl:element name="{$inputName}">
        <xsl:if test="string($type)!=''">
            <xsl:attribute name="type">
                <xsl:value-of select="$type"/>
            </xsl:attribute>
        </xsl:if>
        
        <xsl:choose>
            <xsl:when test="$columnHeader">
                <xsl:attribute name="id">
                    <xsl:value-of select="concat($tableId, '_ms', $schemaNode/@column, '_T_I')"/>
                </xsl:attribute>
                <!--TODO set inputTag and inputType of multiselect-->
                <xsl:attribute name="style">display:none</xsl:attribute>
                <xsl:attribute name="title">
                    <xsl:choose>
                        <xsl:when test="$type='checkbox'">
                            <xsl:text>Check or uncheck all items in this column</xsl:text>
                        </xsl:when>
                        <xsl:otherwise>
                            <xsl:text>Update all items in this column with this value</xsl:text>
                        </xsl:otherwise>
                    </xsl:choose>
                </xsl:attribute>
            </xsl:when>         
            <xsl:otherwise>
                <xsl:attribute name="id">
                    <xsl:value-of select="$id"/>
                </xsl:attribute>
                <xsl:attribute name="name">
                    <xsl:value-of select="$id"/>
                </xsl:attribute>
                <!--xsl:if test="string(@viewType)!='showHideRowBtn' or string(@dataType)!='none'">
                    <xsl:attribute name="name">
                        <xsl:value-of select="$id"/>
                    </xsl:attribute>
                </xsl:if-->
                <xsl:if test="string($viewType)!='select'">
                    <xsl:attribute name="value">
                        <xsl:choose>
                            <xsl:when test="string($type)='checkbox'">1</xsl:when>
                            <xsl:otherwise>
                                <xsl:value-of select="$value"/>
                            </xsl:otherwise>
                        </xsl:choose>                       
                    </xsl:attribute>
        </xsl:if>
            </xsl:otherwise>
        </xsl:choose>
                    
        <xsl:if test="$disabled=1">
            <xsl:attribute name="disabled">true</xsl:attribute>
        </xsl:if>
        
        <xsl:choose>
            <xsl:when test="$type='checkbox'">
                <xsl:if test="$value='1'">
                    <xsl:attribute name="checked">true</xsl:attribute>
                </xsl:if>
            </xsl:when>
            <xsl:when test="$type='text'">
                <xsl:copy-of select="$schemaNode/@size"/>
            </xsl:when>
        </xsl:choose>
            
        <xsl:if test="string($modifiedHandlerName)!=''">
            <xsl:attribute name="{$modifiedHandlerName}">
                <xsl:text>setModified(); </xsl:text>
                <xsl:if test="$multiselect">
                    <xsl:choose>
                        <xsl:when test="$columnHeader">ms_setAll</xsl:when>
                        <xsl:otherwise>ms_onChange</xsl:otherwise>
                    </xsl:choose>
                    <xsl:value-of select="concat('(this, ', $apos, $tableId, $apos, ', ', $schemaNode/@column, ');')"/>
                    <xsl:if test="$columnHeader"> <!--ms column > 0 -->
                        <xsl:text disable-output-escaping="yes">; this.style.display='none';</xsl:text>
                    </xsl:if>
                </xsl:if>
                <xsl:variable name="handlerAttrib" 
                                        select="$schemaNode/@*[name()=$modifiedHandlerName]"/>
                <xsl:if test="not($columnHeader) and $handlerAttrib">
                    <xsl:choose>
                        <xsl:when test="$node">
                            <xsl:call-template name="expand_embedded_xpaths">
                                <xsl:with-param name="str" select="$handlerAttrib"/>
                                <xsl:with-param name="schemaNode" select="$schemaNode"/>
                                <xsl:with-param name="node" select="$node"/>
                            </xsl:call-template>
                        </xsl:when>
                        <xsl:otherwise>
                            <xsl:value-of select="$handlerAttrib"/>
                        </xsl:otherwise>
                    </xsl:choose>
                </xsl:if>
            </xsl:attribute>
        </xsl:if><!--$modifiedHandlerName-->

        <xsl:if test="$type='select-one'">
      <xsl:call-template name="addSelectControlOptions">
                <xsl:with-param name="schemaNode" select="$schemaNode"/>
                <xsl:with-param name="node" select="$node[not($schemaNode/@xpath)] | $objRootNode[$schemaNode/@xpath='/']/*[name()=name($schemaNode)]"/>
                <xsl:with-param name="value" select="$value"/>
            </xsl:call-template>
        </xsl:if>       
    </xsl:element>
    
    <xsl:choose>
        <xsl:when test="$columnHeader">
            <xsl:if test="$multiselect">
                <xsl:variable name="ms_onchange_item" select="string($schemaNode/@ms_onchange_item)"/>
                <xsl:variable name="ms_onchange_item_handler">
                    <xsl:choose>
                        <xsl:when test="$ms_onchange_item!=''"><xsl:value-of select="$ms_onchange_item"/></xsl:when>
                        <xsl:otherwise>null</xsl:otherwise>
                    </xsl:choose>
                </xsl:variable>
                <script type="text/javascript">
                    <xsl:variable name="msId" select="concat($tableId, '_ms', string(@column), '_T')"/>
                    <xsl:value-of select="concat('var input = document.getElementById(', $apos, $msId, '_I', $apos, ');')"/>
                    <xsl:value-of select="concat('ms_create(', $apos, $tableId, $apos, ', onColumnCheck, ', $ms_onchange_item_handler, ', ', @column, ', input.tagName, input.type);')"/>
                </script>                                                               
            </xsl:if>
        </xsl:when>
        <xsl:otherwise>
            <xsl:choose>
                <xsl:when test="string(@viewType)='showHideRowBtn'">
                    <input type="button" id="{$id}.btn" value="Show">
                        <xsl:choose>
                            <xsl:when test="string($node/text())!=''">
                                <xsl:attribute name="onclick">
                                    <xsl:value-of select="concat('onShowHideRow(', $apos, $tableId, $apos, ',', $apos, $rowId, $apos, ',', 'this)')"/>
                                </xsl:attribute>
                            </xsl:when>
                            <xsl:otherwise>
                                <xsl:attribute name="disabled">true</xsl:attribute>
                            </xsl:otherwise>
                        </xsl:choose>
                    </input>
                </xsl:when>
                <xsl:when test="$viewType='url'">
                    <a id="{$id}.url">
                        <xsl:copy-of select="$schemaNode/@onclick|$schemaNode/@href"/>
                        <xsl:value-of select="$value"/>
                    </a>
                </xsl:when>
            </xsl:choose>
        </xsl:otherwise>
    </xsl:choose>
</xsl:template>


<xsl:template name="addSelectControlOptions">
<xsl:param name="schemaNode"/>
<xsl:param name="node"/>
<xsl:param name="value"/>

    <xsl:if test="$schemaNode/@source='object'  and not($node)">
        <xsl:attribute name="disabled">true</xsl:attribute>
    </xsl:if>
    <xsl:if test="$schemaNode/@addEmpty='true'">
        <option value="">
            <xsl:if test="$value and string($value)=''">
                <xsl:attribute name="selected">true</xsl:attribute>
            </xsl:if>
        </option>
    </xsl:if>
    <xsl:choose>
        <xsl:when test="$schemaNode/@source='object'">
            <!--optionTag defines tag name of child that defines an option-->
            <xsl:variable name="optionTag">
                <xsl:choose>
                    <xsl:when test="$schemaNode/@option">
                        <xsl:value-of select="$schemaNode/@option"/>
                    </xsl:when>
                    <xsl:otherwise>option</xsl:otherwise>
                </xsl:choose>
            </xsl:variable>
            <!--
            '@text', '@value' and '@selected' attributes of schemaNode 
                hold the needed tag names which are used to populate the drop down list
                these are supposed to be child nodes of option node in object hierarchy
            
                for instance, the following schema node definition creates a select object
                from the object node below and selects second item:
                <NameServices caption="Name Service" viewType="select" source="object"
                 option="NameService" text="Name" value="Value" selected="Selected"/>
                
                object node:
                <NameServices>
                    <NameService>
                        <Name>ns1</Name>
                        <Value>val1</Value>
                    </NameService>
                    <NameService>
                        <Name>ns2</Name>
                        <Value>val2</Value>
                        <Selected>true</Selected>
                    </NameService>
                </NameServices>
            -->
            <xsl:variable name="textTag" select="$schemaNode/@text"/>
            <xsl:variable name="valueTag" select="$schemaNode/@value"/>
            <xsl:variable name="selectedTag" select="$schemaNode/@selected"/>
            <xsl:choose>
                <xsl:when test="$node">
                    <xsl:for-each select="$node/*[name()=$optionTag]">
                        <xsl:variable name="option" select="."/>
                        <xsl:variable name="val" select="*[name()=$valueTag][1]/text()"/>
                        <option value="{$val}">
                            <xsl:variable name="selected" select="*[name()=$selectedTag][1]"/>
                            <xsl:if test="$selected=1 or $val=string($value)">
                                <xsl:attribute name="selected">true</xsl:attribute>
                            </xsl:if>
                            <xsl:value-of select="*[name()=$textTag][1]/text()"/>
                        </option>
                    </xsl:for-each>
                </xsl:when><!--$node-->
                <xsl:otherwise><!--$node is null-->
                    <option value="">-undefined-</option>
                </xsl:otherwise>
            </xsl:choose> 
        </xsl:when><!--$schemaNode/@source='object'-->
        <xsl:otherwise><!--$schemaNode/@source!='object'-->
            <xsl:for-each select="$schemaNode/option">
                <option value="{@value}">
                    <xsl:if test="$value and string($value)=string(@value)">
                        <xsl:attribute name="selected">true</xsl:attribute>
                    </xsl:if>
                    <xsl:value-of select="text()"/>
                </option>
            </xsl:for-each>
        </xsl:otherwise>
    </xsl:choose>
</xsl:template>

<xsl:template name="expand_embedded_xpaths">
<xsl:param name="str" select="$handlerAttrib"/>
<xsl:param name="schemaNode" select="$schemaNode"/>
<xsl:param name="node" select="$node"/>
    <xsl:choose>
        <xsl:when test="contains($str, '{')">
            <xsl:variable name="prefix" select="substring-before($str, '{')"/>
            <xsl:variable name="suffix" select="substring-after($str, '{')"/>
            <xsl:variable name="embraced" select="substring-before($suffix, '}')"/>
            <xsl:variable name="replacement">
                <xsl:choose>
                    <xsl:when test="$pattern='.'"><xsl:value-of select="$node/text()"/></xsl:when>
                    <xsl:otherwise><xsl:value-of select="$node/*[name()=$embraced]"/></xsl:otherwise>
                </xsl:choose>
            </xsl:variable>
            
            <xsl:value-of select="$prefix"/>
            <xsl:value-of select="$replacement"/>
            <xsl:call-template name="expand_embedded_xpaths">
                <xsl:with-param name="str" select="substring-after($suffix, '}')"/>
                <xsl:with-param name="schemaNode" select="$schemaNode"/>
                <xsl:with-param name="node" select="$node"/>
            </xsl:call-template>
        </xsl:when>
        <xsl:otherwise>
            <xsl:value-of select="$str"/>
        </xsl:otherwise>
    </xsl:choose>
</xsl:template>


<xsl:template match="*|@*" mode="copy">
    <xsl:copy>
        <xsl:apply-templates select="*|@*|text()" mode="copy"/>
    </xsl:copy>
</xsl:template>


<xsl:template match="text()" mode="copy">
    <xsl:copy-of select="normalize-space(.)"/>
</xsl:template>

</xsl:stylesheet>
