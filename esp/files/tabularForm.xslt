<?xml version="1.0" encoding="utf-8"?>
<!--

## Copyright © 2011 HPCC Systems.  All rights reserved.
-->

<!DOCTYPE xsl:stylesheet [
   <!ENTITY endl "&#xd;&#xa;">
   <!ENTITY nbsp "&#160;"><!--define the HTML non-breaking space:-->
]>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"    
   xmlns:external="http://www.seisint.com" exclude-result-prefixes="external">

   <xsl:param name="Component" select="'ws_roxieconfig'"/>
   <xsl:param name="Command" select="'DeployECLAttributes'"/>
   <xsl:param name="ArgsNodeName" select="'Arguments'"/>
      
   <xsl:output method="html" indent="yes" encoding="utf-8"/>       


   
   <xsl:template match="/">
      <html>
         <head>
            <title>
               <xsl:value-of select="$Command"/>
            </title>
            <link type="text/css" rel="StyleSheet" href="/esp/files_/css/sortabletable.css"/>
            <style type="text/css">
               .sort-table td {
                  text-align:center;
               }
            </style>
            <script type="text/javascript" src="/esp/files_/scripts/tabularForm.js">
            </script>
         </head>
         <body onload="onLoad()">
            <xml id="xmlPrevArgsDoc">
              <xsl:copy-of select="."/>
            </xml>
            <form method="post" action="/{$Component}/{$Command}" onsubmit="return onSubmit()">
               <!--xsl:attribute name="action">/<xsl:value-of select="$Component"/>/<xsl:value-of select="$Command"/></xsl:attribute-->
               <input type="hidden" id="component" name="comp" value="{$Component}"/>
               <input type="hidden" id="command" name="command" value="{$Command}"/>
               <xsl:variable name="componentsDoc" select="document('files/components.xml')"/>
               <xsl:if test="not($componentsDoc)">
                  <xsl:message terminate="yes">Failed to load components.xml.</xsl:message>
               </xsl:if>
               <xsl:apply-templates select="$componentsDoc/Components/*[name()=$Component]"/>
            </form>
         </body>
      </html>
   </xsl:template>
   
   <xsl:template match="Components/*">
      <xsl:choose>
         <xsl:when test="@caption">
            <xsl:if test="string-length(@caption) > 0">
               <h1>
                  <xsl:value-of select="@caption"/>
               </h1>                        
            </xsl:if>
         </xsl:when>
         <xsl:otherwise>
            <h1>
               <xsl:value-of select="name(.)"/>
            </h1>
         </xsl:otherwise>
      </xsl:choose>
      <xsl:variable name="CommandNode" select="Commands/*[name(.)=$Command]"/>
      <xsl:if test="not($CommandNode)">
         <xsl:message terminate="yes">Command '<xsl:value-of select="$Command"/>' is not defined in components.xml file!"/></xsl:message>
      </xsl:if>
      <h2>
         <xsl:choose>
            <xsl:when test="$CommandNode/@caption"><xsl:value-of select="$CommandNode/@caption"/></xsl:when>
            <xsl:otherwise><xsl:value-of select="$CommandNode"/></xsl:otherwise>
         </xsl:choose>               
      </h2>
      <xsl:call-template name="createXmlFragment">
         <xsl:with-param name="CompNode" select="."/>
      </xsl:call-template>
      <xsl:for-each select="$CommandNode">
         <xsl:call-template name="PopulateFormForCommand">
            <xsl:with-param name="ArgsNode" select=".//*[name(.)=$ArgsNodeName]"/>
            <xsl:with-param name="CreateCancelBtn" select="0"/>
         </xsl:call-template>
      </xsl:for-each>
   </xsl:template>


   
   <xsl:template name="CreateHtmlForm">
      <xsl:param name="Component"/>
      <xsl:param name="Command"/>
      <xsl:param name="FormActionURL"/>
      <html>
         <head>
            <title>
               <xsl:value-of select="concat($Command, ' [', $Component, ']')"/>
            </title>
         </head>
         <body>
            <h1>
               <xsl:value-of select="$Component"/>
            </h1>
            <h2>
               <xsl:value-of select="$Command"/>
            </h2>
            <form>
               <xsl:if test="$FormActionURL">
                  <attribute name="action">
                     <xsl:value-of select="$FormActionURL"/>
                  </attribute>
               </xsl:if>
               <xsl:apply-templates/>
            </form>
         </body>
      </html>
   </xsl:template>

   
   <xsl:template name="DisplayValue">
      <xsl:param name="var"/>
      <p>
         <b>
            <xsl:value-of select="name($var)"/>
            <xsl:text>: </xsl:text>
            <xsl:value-of select="$var"/>
         </b>
      </p>
   </xsl:template>

   
   <xsl:template name="MakeTableHeader">
      <xsl:param name="Id" select="table1"/>
      <xsl:param name="Columns"/>
      <xsl:param name="Border"/>
      <xsl:param name="CheckBox" select="0"/>
      <xsl:text disable-output-escaping="yes">&lt;table id="</xsl:text>
      <xsl:value-of select="$Id"/>
      <xsl:text disable-output-escaping="yes">" class="sort-table" border="</xsl:text>
      <xsl:value-of select="$Border"/>
      <xsl:text disable-output-escaping="yes">"&gt;</xsl:text>
      <tr>
         <xsl:if test="$CheckBox">
            <th/>
         </xsl:if>
         <xsl:for-each select="$Columns/*">
            <xsl:call-template name="MakeColumnHeader">
               <xsl:with-param name="Column" select="."/>
            </xsl:call-template>
         </xsl:for-each>
      </tr>
   </xsl:template>

   <xsl:template name="MakeColumnHeader">
      <xsl:param name="Column"/>
         <xsl:choose>
            <!--if the current node has only one child and only occurs once then
                 its role is simply to group info so use its children instead -->
            <xsl:when test="$Column/@maxOccurs='1'">
               <xsl:for-each select="$Column/*">
                  <xsl:call-template name="MakeColumnHeader">
                     <xsl:with-param name="Column" select="."/>
                  </xsl:call-template>
               </xsl:for-each>
            </xsl:when>
            <xsl:otherwise>
               <th>
                  <xsl:choose>
                     <xsl:when test="@caption">
                        <xsl:value-of select="@caption"/>
                     </xsl:when>
                     <xsl:otherwise>
                        <xsl:value-of select="name(.)"/>
                     </xsl:otherwise>
                  </xsl:choose>
               </th>
            </xsl:otherwise>
         </xsl:choose>
   </xsl:template>

   
   <xsl:template name="MakeVTableRow">
      <xsl:param name="Columns"/>
      <tr>
         <xsl:for-each select="$Columns/*">
            <td>
               <xsl:choose>
                  <xsl:when test="*">
                     <xsl:apply-templates select="."/>
                  </xsl:when>
                  <xsl:otherwise>
                     <xsl:value-of select="."/>
                  </xsl:otherwise>
               </xsl:choose>
               <xsl:apply-templates/>
            </td>
         </xsl:for-each>
      </tr>
   </xsl:template>

    
   <xsl:template name="MakeHTableRows">
      <xsl:param name="Rows"/>
      <xsl:for-each select="$Rows/*">
         <tr>
            <th align="left" valign="top">
               <xsl:choose>
                  <xsl:when test="@caption">
                     <xsl:value-of select="@caption"/>
                  </xsl:when>
                  <xsl:otherwise>
                     <xsl:value-of select="name(.)"/>
                  </xsl:otherwise>
               </xsl:choose>
            </th>
            <th align="left" valign="top">
                    :
                </th>
            <td>
               <xsl:choose>
                  <xsl:when test="*">
                     <table border="0">
                        <xsl:call-template name="MakeHTableRows">
                           <xsl:with-param name="Rows" select="."/>
                        </xsl:call-template>
                     </table>
                  </xsl:when>
                  <xsl:otherwise>
                     <xsl:value-of select="."/>
                  </xsl:otherwise>
               </xsl:choose>
            </td>
         </tr>
      </xsl:for-each>
   </xsl:template>
   
   
   <xsl:template name="MakeVerticalTable">
      <xsl:param name="Id" select="table1"/>
      <xsl:param name="Rows"/>
      <xsl:param name="CheckBox" select="0"/>
      <xsl:param name="HeaderOnly" select="0"/>
      <xsl:param name="AddDelButtons" select="0"/>
      
      <xsl:call-template name="MakeTableHeader">
         <xsl:with-param name="Id" select="$Id"/>
         <xsl:with-param name="Columns" select="$Rows"/>
         <xsl:with-param name="Border" select="1"/>
         <xsl:with-param name="CheckBox" select="$CheckBox"/>
      </xsl:call-template>
      <xsl:if test="($HeaderOnly=0 and $Rows/*)">
         <tr>
            <xsl:variable name="CheckBoxId" select="concat($Id, 'checkbox')"/>
            <td>
               <xsl:if test="$CheckBox and normalize-space($Rows/text())">
                  <input type="checkbox" id="$CheckBoxId" name="$CheckBoxId"/>
               </xsl:if>
            </td>
            <xsl:for-each select="$Rows/*">
               <td>
                  <xsl:choose>
                     <xsl:when test="*">
                        <xsl:call-template name="MakeVerticalTable">
                           <xsl:with-param name="Rows" select="."/>
                           <xsl:with-param name="CheckBox" select="$CheckBox"/>
                           <xsl:with-param name="HeaderOnly" select="$HeaderOnly"/>
                        </xsl:call-template>
                     </xsl:when>
                     <xsl:otherwise>
                        <xsl:if test="$HeaderOnly=0">
                           <xsl:value-of select="."/>
                        </xsl:if>
                     </xsl:otherwise>
                  </xsl:choose>
               </td>
            </xsl:for-each>
         </tr>
      </xsl:if>
      <xsl:text disable-output-escaping="yes">&lt;/table&gt;</xsl:text>
      <xsl:if test="$AddDelButtons">
         <xsl:variable name="ArgsNodeName" select="name($Rows)"/>
         <input type="button" id="{$Id}.Add" name="Add" value="Add" onclick="onAdd('{$Id}')"/>
         &nbsp;
         <input type="button" id="{$Id}.Delete" name="Delete" value="Delete" disabled="true" onclick="onDelete('{$Id}')"/>
      </xsl:if>
   </xsl:template>
   
   
   <xsl:template name="MakeCheckbox">
      <xsl:param name="CheckboxName"/>
      <xsl:param name="Id"/>
      <xsl:param name="Checked"/>
      <input type="checkbox" id="{$Id}" name="{$CheckboxName}" onclick="this.value=1-this.value;">
         <xsl:choose>
            <xsl:when test="$Checked=1">
               <xsl:attribute name="checked">true</xsl:attribute>
               <xsl:attribute name="value">1</xsl:attribute>
            </xsl:when>
            <xsl:otherwise>
               <xsl:attribute name="value">0</xsl:attribute>
            </xsl:otherwise>
         </xsl:choose>
      </input>
   </xsl:template>
   
   
   <xsl:template name="MakeDropDownFileList">
      <xsl:param name="Name"/>
      <xsl:param name="Id"/>
      <xsl:param name="PathName"/>
      <xsl:param name="FileExtension"/>
      <xsl:param name="Selected"/>
      <xsl:param name="onchange"/>
      <xsl:variable name="Directory" select="external:Base64Encryption('directory', $PathName, $FileExtension)"/>
      <select id="$Id" name="$Name" onchange="{$onchange}">
         <xsl:for-each select="$Directory/*">
            <xsl:variable name="FilePathName" select="concat($PathName, '/', .)"/>
            <option value="$FilePathName">
               <xsl:value-of select="$FilePathName"/>
            </option>
         </xsl:for-each>
      </select>
   </xsl:template>
   
   
   <xsl:template name="MakeEditable">
      <xsl:param name="InputName"/>
      <xsl:param name="Id"/>
      <xsl:param name="InputValue"/>
      <xsl:param name="dataType"/>
      <xsl:choose>
         <xsl:when test="not($dataType)">
            <INPUT type="text" size="30" id="{$Id}" name="{$InputName}" value="{$InputValue}"/>
         </xsl:when>
         <xsl:when test="$dataType='boolean'">
            <xsl:call-template name="MakeCheckbox">
               <xsl:with-param name="CheckboxName" select="$InputName"/>
               <xsl:with-param name="Checked" select="$InputValue"/>
               <xsl:with-param name="Id" select="checkBox"/>
            </xsl:call-template>
         </xsl:when>
         <xsl:when test="$dataType='text'">
            <textarea rows="30" cols="80" id="{$Id}" name="{$InputName}">
               <xsl:value-of select="$InputValue"/>
            </textarea>
         </xsl:when>
         <xsl:when test="$dataType='file'">
            <INPUT type="file" size="30" id="{$Id}" name="{$InputName}" value="{$InputValue}"/>
         </xsl:when>
         <xsl:when test="$dataType='editablefile'">
            <xsl:variable name="DropDownListName" select="concat('select', '{$InputName}')"/>
            <xsl:variable name="Quote">'</xsl:variable>
            <xsl:call-template name="MakeDropDownFileList">
               <xsl:with-param name="Name" select="$DropDownListName"/>
               <xsl:with-param name="Id" select="$DropDownListName"/>
               <xsl:with-param name="PathName" select="'DMS/Blobs'"/>
               <xsl:with-param name="FileExtension" select="'*.xml'"/>
               <xsl:with-param name="Selected" select="none"/>
               <xsl:with-param name="onchange" select="concat('loadXmlInTextArea(options[selectedIndex].text, ', $Quote, $InputName, $Quote, ')')"/>
            </xsl:call-template>
            <br/>
            <textarea rows="15" cols="60" id="{$Id}" name="{$InputName}" wrap="off">
               <xsl:copy-of select="document($InputValue)"/>
            </textarea>
         </xsl:when>
         <xsl:when test="$dataType='password'">
            <INPUT type="password" size="30" id="{$Id}" name="{$InputName}" value="{$InputValue}"/>
         </xsl:when>
         <xsl:otherwise>
            </xsl:otherwise>
      </xsl:choose>
   </xsl:template>
   
   
   <xsl:template name="MakeEditableTable">
      <xsl:param name="argNode"/>
      <xsl:param name="border" select="0"/>
      <xsl:param name="OnClickOKBtn" select="''"/>
      <xsl:param name="CreateCancelBtn"/>
      <table id="ArgumentsTable" name="ArgumentsTable" border="{$border}">
         <xsl:variable name="numArgs">
            <xsl:choose>
               <xsl:when test="count($argNode/*) = 1 and $argNode/*[1]/@maxOccurs='1'">
                  <xsl:value-of select="count($argNode/*[1]/*)"/>
               </xsl:when>
               <xsl:otherwise><xsl:value-of select="count($argNode/*)"/></xsl:otherwise>
            </xsl:choose>
         </xsl:variable>
         <xsl:for-each select="$argNode/*">
            <xsl:variable name="InputName">
               <xsl:value-of select="name(.)"/>
            </xsl:variable>
            <xsl:variable name="InputValue">
               <xsl:value-of select="@default"/>
            </xsl:variable>
            <tr>
               <xsl:if test="$numArgs > 1">
                  <th align="left" valign="top">
                     <xsl:value-of select="name(.)"/>
                  </th>
                  <th valign="top">
                     <xsl:text>: </xsl:text>
                  </th>
               </xsl:if>
               <td>
                  <xsl:choose>
                     <xsl:when test="@maxOccurs='unbounded'">
                        <xsl:call-template name="MakeVerticalTable">
                           <xsl:with-param name="Id" select="name(.)"/>
                           <xsl:with-param name="Rows" select="."/>
                           <xsl:with-param name="CheckBox" select="1"/>
                           <xsl:with-param name="HeaderOnly" select="1"/>
                           <xsl:with-param name="AddDelButtons" select="1"/>
                        </xsl:call-template>
                     </xsl:when>
                     <xsl:otherwise>
                        <xsl:call-template name="MakeEditable">
                           <xsl:with-param name="InputName" select="$InputName"/>
                           <xsl:with-param name="Id" select="$InputName"/>
                           <xsl:with-param name="InputValue" select="$InputValue"/>
                           <xsl:with-param name="dataType" select="@dataType"/>
                        </xsl:call-template>
                     </xsl:otherwise>
                  </xsl:choose>
               </td>
            </tr>
         </xsl:for-each>
         <!--create vertical space before reset/ok/cancel buttons-->
         <tr>
            <td style="height:40px">
               <xsl:if test="$numArgs > 1">
                  <xsl:attribute name="colspan">3</xsl:attribute>
               </xsl:if>
            </td>
         </tr>
         <!-- create the buttons now-->
         <tr>
            <td align="center">
               <xsl:if test="$numArgs > 1">
                  <xsl:attribute name="colspan">3</xsl:attribute>
               </xsl:if>
               <input type="reset" value="Reset"/>&nbsp;&nbsp;&nbsp;
               <xsl:call-template name="CreateOKButton">
                  <xsl:with-param name="OnClickOKBtn" select="$OnClickOKBtn"/>
               </xsl:call-template>
               <xsl:if test="$CreateCancelBtn">
                  &nbsp;
                  <input type="button" id="Cancel" name="Cancel" value="Cancel" onclick="OnCancel()"/>
               </xsl:if>
            </td>
         </tr>
      </table>
   </xsl:template>
   
   
   <xsl:template name="PopulateFormForCommand">
      <xsl:param name="ArgsNode"/>
      <xsl:param name="OnClickOKBtn" select="''"/>
      <xsl:param name="CreateCancelBtn" select="1"/>
      <xsl:choose>
         <xsl:when test="$ArgsNode/*">
            <xsl:call-template name="MakeEditableTable">
               <xsl:with-param name="argNode" select="$ArgsNode"/>
               <xsl:with-param name="OnClickOKBtn" select="$OnClickOKBtn"/>
               <xsl:with-param name="CreateCancelBtn" select="$CreateCancelBtn"/>
            </xsl:call-template>
         </xsl:when>
         <xsl:otherwise>
            <xsl:call-template name="CreateOKButton">
               <xsl:with-param name="OnClickOKBtn" select="$OnClickOKBtn"/>
            </xsl:call-template>
         </xsl:otherwise>
      </xsl:choose>
   </xsl:template>
   
   
   <xsl:template name="CreateOKButton">
      <xsl:param name="OnClickOKBtn" select="''"/>
      <xsl:choose>
         <xsl:when test="not($OnClickOKBtn='')">
            <input type="button" id="OK" name="OK" value="Submit" onclick="{$OnClickOKBtn}"/>
         </xsl:when>
         <xsl:otherwise>
            <input type="submit" id="OK" name="OK" value="Submit"/>
         </xsl:otherwise>
      </xsl:choose>
   </xsl:template>
   
   
   <xsl:template name="createXmlFragment">
      <xsl:param name="CompNode"/>
      <xsl:variable name="XmlFragment">
         <xsl:element name="Components">
            <xsl:element name="{name($CompNode)}">
               <xsl:apply-templates select="$CompNode/Defaults" mode="copy"/>
               <xsl:element name="Commands">
                  <xsl:apply-templates select="$CompNode/Commands/*[name(.)=$Command]" mode="copy"/>
               </xsl:element>
            </xsl:element>
         </xsl:element>
      </xsl:variable>
      <xml id="xmlSchema">
         <xsl:copy-of select="$XmlFragment"/>
      </xml>
      <input type="hidden" name="xmlArgs" value=""/>
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
