<?xml version="1.0" encoding="UTF-8"?>
<!--

## HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.  All rights reserved.
-->

<!DOCTYPE xsl:stylesheet [
    <!ENTITY nbsp "&#160;">
    <!ENTITY apos "&#39;">
]>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xml:space="default" xmlns:xsd="http://www.w3.org/2001/XMLSchema" exclude-result-prefixes="xsd">
    <xsl:strip-space elements="*"/>
    <!-- TODO: change indent="no" for performance -->
    <xsl:output method="html" indent="yes" omit-xml-declaration="yes" version="4.01" doctype-public="-//W3C//DTD HTML 4.01 Transitional//EN" doctype-system="http://www.w3.org/TR/html4/loose.dtd"/>
    <!-- ===============================================================================
  parameters
  ================================================================================ -->
    <xsl:param name="queryParams" select="''"/>
    <xsl:param name="formOptionsAccess" select="1"/>
    <xsl:param name="noDefaultValue" select="0"/>
    <xsl:param name="includeSoapTest" select="1"/>
    <xsl:param name="schemaRoot" select="/FormInfo/xsd:schema"/>
    <xsl:param name="esdl_links" select="0"/>
    <xsl:param name="useTextareaForStringArray" select="0"/>

    <xsl:variable name="queryPath" select="/FormInfo/QuerySet"/>
    <xsl:variable name="methodName" select="/FormInfo/QueryName"/>
    <xsl:variable name="wuid" select="/FormInfo/WUID"/>
    <xsl:variable name="methodHelp" select="/FormInfo/Help"/>
    <xsl:variable name="methodDesc" select="/FormInfo/Info"/>
    <xsl:variable name="serviceVersion" select="/FormInfo/Version"/>
    <xsl:variable name="requestLabel" select="/FormInfo/RequestElement"/>
    <xsl:variable name="requestElement" select="/FormInfo/RequestElement"/>

    <!-- ===============================================================================
  global settings
  ================================================================================ -->
    <!-- debug -->
    <xsl:variable name="show_ctrl_name" select="0"/>
    <xsl:variable name="set_ctrl_value" select="0"/>
    <xsl:variable name="verbose" select="0"/>
    <!-- config -->
    <xsl:variable name="useTableBorder" select="1"/>
    <!-- ================================================================================
   The main template: produce the html (and call GenerateRequestForm for request input)
  ================================================================================ -->

    <xsl:template match="FormInfo">
        <xsl:apply-templates select="xsd:schema"/>
    </xsl:template>
    <!--xsl:variable name="" select="ViewFormResponse/xsd/xsd:schema"/ -->
    <xsl:template match="xsd:schema">
          <xsl:if test="$verbose">
              noDefaultValue: <xsl:value-of select="$noDefaultValue"/>
          </xsl:if>

        <!-- prepare -->
        <xsl:variable name="array-types-all">
            <xsl:variable name="core">
                <xsl:call-template name="GetArrayTypes">
                    <xsl:with-param name="node" select="$schemaRoot/xsd:element[@name=$requestElement]"/>
                </xsl:call-template>
            </xsl:variable>
            <xsl:value-of select="string($core)"/>
        </xsl:variable>
        <xsl:variable name="array-types">
            <xsl:call-template name="RemoveDuplicates">
                <xsl:with-param name="types" select="concat($requestElement,':',$array-types-all)"/>
            </xsl:call-template>
        </xsl:variable>

        <xsl:variable name="enum-types-all">
            <xsl:variable name="core">
                <xsl:call-template name="GetEnumTypes">
                    <xsl:with-param name="node" select="$schemaRoot/xsd:element[@name=$requestElement]"/>
                </xsl:call-template>
            </xsl:variable>
            <xsl:value-of select="string($core)"/>
        </xsl:variable>
        <xsl:variable name="enum-types">
            <xsl:call-template name="RemoveDuplicates">
                <xsl:with-param name="types" select="$enum-types-all"/>
            </xsl:call-template>
        </xsl:variable>

        <html>
            <head>
                <title>WsECL Service formx</title>
                <link rel="shortcut icon" href="/esp/files/img/affinity_favicon_1.ico" />
                <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
                <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
                <link rel="stylesheet" type="text/css" href="/esp/files/gen_form.css"/>
                <script type="text/javascript" src="/esp/files/req_array.js"/>
                <script type="text/javascript" src="/esp/files/hashtable.js"/>
                <script type="text/javascript" src="/esp/files/gen_form_wsecl.js"/>
                <script type="text/javascript"><xsl:text disable-output-escaping="yes">
                <![CDATA[
  var isIE = (navigator.appName == "Microsoft Internet Explorer");

  function getRequestFormHtml()
  {
        var html =  get_]]></xsl:text><xsl:value-of select="translate($requestElement, '.-~@#%^:;,/?=+`|&amp;', '_________________')"/><xsl:text disable-output-escaping="yes"><![CDATA[_Item();
        return html.replace(new RegExp('\\$\\$\\.', 'g'), '');
    }
  function get_Array_Input(parentId,typeName,itemName)
  {
    var newId = parentId + "." + itemName;
    return "<span id='$C."+parentId+"'>"
    ]]></xsl:text>
    <xsl:if test="$useTableBorder">
                        <xsl:text disable-output-escaping="yes"><![CDATA[+ "<table id='"+newId+"' class='struct'> </table>"]]></xsl:text>
    </xsl:if>
                    <xsl:if test="not($useTableBorder)">
                        <xsl:text disable-output-escaping="yes"><![CDATA[ + "<table id='"+newId+"'> </table></hr>"]]></xsl:text>
                    </xsl:if>
                    <xsl:text disable-output-escaping="yes"><![CDATA[
       + "<input type='hidden' id='"+newId+"_ItemCt' name='"+newId+".itemcount!' value='0' />"
          + "&nbsp;<input type='button' id='"+newId+"_AddBtn' onclick='appendRow(\""+newId+"\",\""+itemName+"\",get_"+typeName+"_Item)' value='Add' /> "
          + "<input type='button' id='"+newId+"_RvBtn' onclick='removeRow(\""+newId+"\",-1)' value='Delete' disabled='true' />" ]]></xsl:text>
                    <xsl:if test="not($useTableBorder)">
                        <xsl:text disable-output-escaping="yes"><![CDATA[ + "</hr>"]]></xsl:text>
                    </xsl:if>
                    <xsl:text disable-output-escaping="yes"><![CDATA[ + "</span>"; ]]>
  }</xsl:text>
                    <xsl:if test="not($useTextareaForStringArray)"><xsl:text disable-output-escaping="yes"><![CDATA[
  function get_XsdArray_Item(parentId,itemName) {
     return "<span id='$$.Span'><table id='$$'> <tr> <td><input type='text' name='$$.Item' size='50' /></td></tr></table> </span>";
  }
      ]]></xsl:text></xsl:if>

    <xsl:call-template name="GenerateJSFuncs">
        <xsl:with-param name="types" select="$array-types"/>
    </xsl:call-template>
    <xsl:call-template name="GenerateEnumFuncs">
        <xsl:with-param name="types" select="$enum-types"/>
    </xsl:call-template>
<xsl:text disable-output-escaping="yes"><![CDATA[

function setESPFormAction()  // reqType: 0: regular form, 1: soap, 2: form param passing, 3: roxiexml
{
    var form = document.forms['esp_form'];
    if (!form)  return false;

    var actval = document.getElementById('submit_type');
    var actionpath = "/jserror";
    if (actval && actval.value)
    {
        if (actval.value=="esp_soap")
            actionpath = "]]></xsl:text><xsl:value-of disable-output-escaping="yes" select="concat('/WsEcl/forms/soap/query/', $queryPath, '/', $methodName)"/><xsl:text disable-output-escaping="yes"><![CDATA[";
        else if (actval.value=="esp_json")
            actionpath = "]]></xsl:text><xsl:value-of disable-output-escaping="yes"  select="concat('/WsEcl/forms/json/query/', $queryPath, '/', $methodName)"/><xsl:text disable-output-escaping="yes"><![CDATA[";
        else if (actval.value=="run_xslt")
            actionpath = "]]></xsl:text><xsl:value-of disable-output-escaping="yes"  select="concat('/WsEcl/xslt/query/', $queryPath, '/', $methodName)"/><xsl:text disable-output-escaping="yes"><![CDATA[";
        else if (actval.value=="proxy_xml")
            actionpath = "]]></xsl:text><xsl:value-of disable-output-escaping="yes"  select="concat('/WsEcl/proxy/query/', $queryPath, '/', $methodName, '/xml?view=xml&amp;display')"/><xsl:text disable-output-escaping="yes"><![CDATA[";
        else if (actval.value=="xml")
            actionpath = "]]></xsl:text><xsl:value-of disable-output-escaping="yes"  select="concat('/WsEcl/submit/query/', $queryPath, '/', $methodName, '/xml?view=xml&amp;display')"/><xsl:text disable-output-escaping="yes"><![CDATA[";
        else if (actval.value=="json")
            actionpath = "]]></xsl:text><xsl:value-of disable-output-escaping="yes"  select="concat('/WsEcl/submit/query/', $queryPath, '/', $methodName, '/json')"/><xsl:text disable-output-escaping="yes"><![CDATA[";
        else
            actionpath= "]]></xsl:text><xsl:value-of disable-output-escaping="yes"  select="concat('/WsEcl/xslt/query/', $queryPath, '/', $methodName, '?view=')"/><xsl:text disable-output-escaping="yes"><![CDATA["+actval.value;
    }
    //alert("actionpath = " + actionpath);
    var dest = document.getElementById('esp_dest');
    if (actionpath=="bookmark")
         doBookmark(form);
    else if (dest && dest.checked)
         form.action = document.getElementById('dest_url').value;
    else
        form.action = actionpath;
    var methodval = document.getElementById('method_type').value;
    form.method = methodval;
    if (methodval == "GET")
        form.target = "_blank";
    else
        form.target = "_self";

    // firefox now save input values (version 1.5)
    saveInputValues(form);

    return true;
}
function switchInputForm()
{
    var inputform = document.getElementById('SelectForm');
    if (inputform.value!="InputBox")
       return false;
    document.location.href = "]]></xsl:text><xsl:value-of disable-output-escaping="yes" select="concat('/WsEcl/forms/box/query/', $queryPath, '/', $methodName)"/><xsl:text disable-output-escaping="yes"><![CDATA[";
    return true;
}

]]>
</xsl:text>
<xsl:call-template name="GetHtmlHeadAddon"/>
</script>

      </head>
      <body class="yui-skin-sam" onload="onPageLoad()">
                <!-- internal: workaround browser's inability to cache DHTML -->
                <input type="hidden" id="esp_html_"/>
                <input type="hidden" id="esp_vals_"/>
                <xsl:if test="$show_ctrl_name">
                    <br/>Array Types: <xsl:value-of select="$array-types-all"/>
                    <br/>Array Types Deduped: <xsl:value-of select="$array-types"/>
                    <br/>Enum Types: <xsl:value-of select="$enum-types-all"/>
                    <br/>Enum Types Deduped: <xsl:value-of select="$enum-types"/>
                </xsl:if>
                <p align="center"/>
                <table cellSpacing="0" cellPadding="1" width="100%" bgColor="#4775FF" border="0">
                    <tr align="left" class="service">
                        <td height="23">
                            <font color="#efefef">
                                <b>
                                    <xsl:value-of select="/FormInfo/QuerySet"/>
                                    <xsl:if test="number($serviceVersion)>0">
                                        <xsl:value-of select="concat(' [Version ', $serviceVersion, ']')"/>
                                    </xsl:if>
                                </b>
                            </font>
                        </td>
                    </tr>
                    <tr class='method'>
                        <td height="23" align="left">
                            <xsl:variable name="params">
                                <xsl:if test="$queryParams">
                                    <xsl:value-of select="concat('&amp;', substring($queryParams,2))"/>
                                </xsl:if>
                            </xsl:variable>
                            <b><xsl:value-of select="$methodName"/>
                            </b>&nbsp;<a>
                                <xsl:attribute name="href"><xsl:call-template name="build_link"><xsl:with-param name="type" select="'wsdl'"/></xsl:call-template></xsl:attribute>
                                <xsl:attribute name="target">_blank</xsl:attribute>
                                <img src="/esp/files/img/wsdl.gif" title="WSDL" border="0" align="bottom"/>
                            </a>&nbsp;<a>
                                <xsl:attribute name="href"><xsl:call-template name="build_link"><xsl:with-param name="type" select="'xsd'"/></xsl:call-template></xsl:attribute>
                                <xsl:attribute name="target">_blank</xsl:attribute>
                                <img src="/esp/files/img/xsd.gif" title="Schema" border="0" align="bottom"/>
                            </a>&nbsp;<a>
                                <xsl:attribute name="href"><xsl:call-template name="build_link"><xsl:with-param name="type" select="'reqxml'"/></xsl:call-template></xsl:attribute>
                                <img src="/esp/files/img/reqxml.gif" title="Sample Request XML" border="0" align="bottom"/>
                            </a>&nbsp;<a>
                                <xsl:attribute name="href"><xsl:call-template name="build_link"><xsl:with-param name="type" select="'respxml'"/></xsl:call-template></xsl:attribute>
                                <img src="/esp/files/img/respxml.gif" title="Sample Response XML" border="0" align="bottom"/>
                            </a>&nbsp;&nbsp;
                            <select id="SelectForm" name="select_form" onChange="switchInputForm()">
                               <option value="DynamicForm" selected="selected">Dynamic Form</option>
                              <option value="InputBox">Input Box</option>
                            </select>&nbsp;<br/>
                        </td>
                    </tr>
                    <xsl:if test="$methodDesc and $methodDesc!=''">
                    <tr>
                        <td class="desc">
                            <table cellSpacing="0" border="0">
                                <tr>
                                    <td valign="middle" align="left">
                                        <br/>
                                        <xsl:value-of disable-output-escaping="yes" select="$methodDesc"/>
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>
                    </xsl:if>
                    <xsl:if test="$methodHelp and $methodHelp!=''">
                    <tr>
                        <td class="help">
                            <table cellSpacing="0" border="0">
                                <tr>
                                    <td valign="middle" align="left">
                                        <xsl:value-of disable-output-escaping="yes" select="$methodHelp"/>
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>
                    </xsl:if>
                    <tr bgColor="#efefef">
                        <td>
                            <p align="left"/>
                            <xsl:variable name="action">
                                <xsl:call-template name="build_link"><xsl:with-param name="type" select="'action'"/></xsl:call-template>
                            </xsl:variable>
         <!-- TODO: different enctype when form as attachment ?? -->
            <form id="esp_form" method="POST" enctype="application/x-www-form-urlencoded" action="/jserror">
               <xsl:attribute name="onSubmit">return setESPFormAction()</xsl:attribute>
               <table cellSpacing='0' width='100%' border='0'>
                 <tr><td>
                   <span class='request'>
                    <xsl:value-of select="translate($requestElement, '.-~@#%^:;,/?=+`|&amp;', '_________________')"/>
                    <input type='checkbox' checked='checked' id='V_' onclick='disableAllInputs(this)'/>
                  </span>
                 </td></tr>

            <tr><td bgcolor="#030303" height="1"></td></tr>
                <tr><td height="6"></td></tr>

            <tr>
                   <td class='input' align='left'>
                       <span id="esp_dyn">
                       <script type="text/javascript">document.write(getRequestFormHtml());</script>
                     </span>
              </td>
            </tr>

            <tr><td bgcolor="#030303" height="1"></td></tr>
                <tr><td height="6"></td></tr>

                <tr class='commands'>
                  <td align='left'>
                    <select id="submit_type">
                        <xsl:for-each select="/FormInfo/CustomViews/Result">
                            <option><xsl:attribute name="value"><xsl:value-of select="."/></xsl:attribute><xsl:value-of select="."/></option>
                        </xsl:for-each>
                        <option value="run_xslt">Output Tables</option>
                        <option value="proxy_xml">Output XML (Proxy mode)</option>
                        <option value="xml">Output XML</option>
                        <option value="json">Output JSON</option>
                        <option value="esp_soap">SOAP Test</option>
                        <option value="esp_json">JSON Test</option>
                    </select>&nbsp;
                    <select id="method_type">
                         <option value="POST">FORM POST</option>
                        <option value="GET">REST URL</option>
                    </select>&nbsp;
                   <input type='submit' value='Submit'/>

          &nbsp;<input type='button' value='Clear All' onclick='onClearAll()'  title='Reset the form, and remove all arrays you added'/>
           </td>
                 </tr>
                      </table>
                     </form>
                      </td>
                    </tr>
                 </table>
              </body>
          </html>
    </xsl:template>
    <!-- ********************************************************************************************************** -->
    <!-- get array type used in an element node -->
    <xsl:template name="GetHtmlHeadAddon">
        <xsl:variable name="items" select="//xsd:annotation/xsd:appinfo/form/@html_head"/>
        <xsl:variable name="s" select="string($items)"/>
        <xsl:value-of select="$s" disable-output-escaping="yes"/>
    </xsl:template>
    <!-- ================================================================================
          generate javascript functions
  ================================================================================ -->
    <xsl:template name="GenerateJSFuncs">
        <xsl:param name="types"/>
        <xsl:if test="$types">
            <xsl:variable name="first" select="substring-before($types,':')"/>
            <xsl:choose>
                <xsl:when test="$first">
                    <xsl:call-template name="GenerateFunc">
                        <xsl:with-param name="type" select="$first"/>
                    </xsl:call-template>
                    <xsl:call-template name="GenerateJSFuncs">
                        <xsl:with-param name="types" select="substring-after($types,':')"/>
                    </xsl:call-template>
                </xsl:when>
                <xsl:otherwise>
                    <xsl:call-template name="GenerateFunc">
                        <xsl:with-param name="type" select="$types"/>
                    </xsl:call-template>
                </xsl:otherwise>
            </xsl:choose>
        </xsl:if>
    </xsl:template>

    <xsl:template name="GenerateItemFuncFromCxType">
        <xsl:param name="cxtype"/>
        <xsl:choose>
            <xsl:when test="$cxtype/xsd:sequence">
                <xsl:call-template name="GenEspStructHtmlTable">
                    <xsl:with-param name="nodes" select="($cxtype/xsd:sequence/xsd:element) | ($cxtype/xsd:attribute)"/>
                    <xsl:with-param name="parentId" select="'$$'"/>
                </xsl:call-template>
            </xsl:when>
            <xsl:when test="$cxtype/xsd:all">
                <xsl:call-template name="GenEspStructHtmlTable">
                    <xsl:with-param name="nodes" select="($cxtype/xsd:all/xsd:element) | ($cxtype/xsd:attribute)"/>
                    <xsl:with-param name="parentId" select="'$$'"/>
                </xsl:call-template>
            </xsl:when>
            <xsl:when test="$cxtype/xsd:simpleContent">
                <xsl:call-template name="GenEspStructHtmlTable">
                    <xsl:with-param name="nodes" select="$cxtype/xsd:simpleContent/xsd:extension/xsd:attribute"/>
                    <xsl:with-param name="parentId" select="'$$'"/>
                    <xsl:with-param name="simpleContent" select="true()"/>
                </xsl:call-template>
            </xsl:when>
            <xsl:when test="$cxtype/xsd:all/xsd:element">
                <xsl:call-template name="GenEspStructHtmlTable">
                    <xsl:with-param name="nodes" select="$cxtype/xsd:all/xsd:element"/>
                    <xsl:with-param name="parentId" select="'$$'"/>
                </xsl:call-template>
            </xsl:when>
        </xsl:choose>
  </xsl:template>
    <xsl:template name="GenerateFunc">
        <xsl:param name="type"/>
        <xsl:variable name="html">
            <xsl:choose>
                <xsl:when test="$schemaRoot/xsd:element[@name=$type]/xsd:complexType">
                            <xsl:call-template name="GenerateItemFuncFromCxType">
                                <xsl:with-param name="cxtype" select="$schemaRoot/xsd:element[@name=$type]/xsd:complexType"/>
                            </xsl:call-template>
                </xsl:when>
                <xsl:when test="$schemaRoot/xsd:complexType[@name=$type]">
                            <xsl:call-template name="GenerateItemFuncFromCxType">
                                <xsl:with-param name="cxtype" select="$schemaRoot/xsd:complexType[@name=$type]"/>
                            </xsl:call-template>
                </xsl:when>
                <xsl:when test="/xsd:schema/xsd:simpleType[@name=$type]/xsd:restriction">
                    <xsl:call-template name="GenEspEnumHtmlTable">
                        <xsl:with-param name="type" select="$type"/>
                        <xsl:with-param name="parentId" select="'$$'"/>
                    </xsl:call-template>
                </xsl:when>
                <xsl:otherwise>
                    <xsl:variable name="found_cxtype" select="$schemaRoot//xsd:element[@name=$type]/xsd:complexType"/>
                    <xsl:if test="$found_cxtype" >
                        <xsl:call-template name="GenerateItemFuncFromCxType">
                            <xsl:with-param name="cxtype" select="$found_cxtype"/>
                        </xsl:call-template>
                    </xsl:if>
                </xsl:otherwise>
            </xsl:choose>
        </xsl:variable>



  function get_<xsl:value-of select="translate($type, '.-~@#%^:;,/?=+`|&amp;', '_________________')"/>_Item(parentId,itemName) {
    return &quot;<xsl:value-of select="$html" disable-output-escaping="yes"/>&quot;;
  }
  </xsl:template>
    <!-- ================================================================================
          generate javascript functions for enum types
  ================================================================================ -->
    <xsl:template name="GenerateEnumFuncs">
        <xsl:param name="types"/>
        <xsl:if test="$types">
            <xsl:variable name="first" select="substring-before($types,':')"/>
            <xsl:choose>
                <xsl:when test="$first">
                    <xsl:call-template name="GenerateEnumFunc">
                        <xsl:with-param name="type" select="$first"/>
                    </xsl:call-template>
                    <xsl:call-template name="GenerateEnumFuncs">
                        <xsl:with-param name="types" select="substring-after($types,':')"/>
                    </xsl:call-template>
                </xsl:when>
                <xsl:otherwise>
                    <xsl:call-template name="GenerateEnumFunc">
                        <xsl:with-param name="type" select="$types"/>
                    </xsl:call-template>
                </xsl:otherwise>
            </xsl:choose>
        </xsl:if>
    </xsl:template>
    <xsl:template name="GenerateEnumFunc">
        <xsl:param name="type"/>
        <xsl:variable name="html">
            <xsl:variable name="typeDef" select="$schemaRoot/xsd:simpleType[@name=$type]"/>
            <xsl:call-template name="GenEnumHtmls">
             <xsl:with-param name="nodes" select="$typeDef/xsd:restriction/xsd:enumeration"/>
             <xsl:with-param name="appinfoNode" select="$typeDef/xsd:annotation/xsd:appinfo"/>
            </xsl:call-template>
        </xsl:variable>
  function get_<xsl:value-of select="translate($type, '.-~@#%^:;,/?=+`|&amp;', '_________________')"/>_Enum(ctrl_id, selVal) {
    return  <xsl:text disable-output-escaping="yes"><![CDATA["<select name='" + ctrl_id + "' id='" + ctrl_id +"'>]]></xsl:text><xsl:value-of select="$html"/><xsl:text disable-output-escaping="yes"><![CDATA[</select>";
    }
 ]]></xsl:text>
  </xsl:template>

 <xsl:template name="GetEnumItemDescription">
   <xsl:param name="name"/>
   <xsl:param name="appinfoNode"/>
   <xsl:value-of select="$appinfoNode/item[@name=$name]/@description"/>
 </xsl:template>

<xsl:template name="GenEnumHtmls">
    <xsl:param name="nodes"/>
    <xsl:param name="appinfoNode"/>
    <xsl:if test="$nodes">
            <xsl:variable name="first">
               <xsl:variable name="value" select="$nodes[1]/@value"/>
               <xsl:variable name="desc">
                 <xsl:call-template name="GetEnumItemDescription">
                    <xsl:with-param name="name"     select="$value"/>
                    <xsl:with-param name="appinfoNode" select="$appinfoNode"/>
                 </xsl:call-template>
               </xsl:variable>
                 <xsl:if test="$value">
                   <xsl:text disable-output-escaping="yes"><![CDATA[<option value=']]></xsl:text>
                   <xsl:value-of select="$value"/>
                   <xsl:text disable-output-escaping="yes"><![CDATA['" + ((selVal=="]]></xsl:text>
                   <xsl:value-of select="$value"/>
                <xsl:text disable-output-escaping="yes"><![CDATA[")?" selected='1'":"") + ">]]></xsl:text>
                   <xsl:value-of select="$value"/>
                   <xsl:if test="$desc != '' ">
                       <xsl:text disable-output-escaping="yes"> - </xsl:text>
                       <xsl:value-of select="$desc"/>
                   </xsl:if>
                   <xsl:text disable-output-escaping="yes"><![CDATA[</option>]]></xsl:text>
               </xsl:if>
            </xsl:variable>
            <xsl:variable name="rest">
                <xsl:call-template name="GenEnumHtmls">
                   <xsl:with-param name="nodes" select="$nodes[position()!=1]"/>
                     <xsl:with-param name="appinfoNode" select="$appinfoNode"/>
                </xsl:call-template>
            </xsl:variable>
            <xsl:value-of select="concat($first,$rest)"/>
        </xsl:if>
      </xsl:template>

    <!-- =======================================================================================
  * check a node is collapsable
  ========================================================================================= -->
    <xsl:template name="IsNodeCollapsable">
        <xsl:param name="node"/>
        <xsl:variable name="type" select="$node/@type"/>
        <xsl:variable name="cpxType" select="$node/xsd:complexType"/>
        <xsl:choose>
            <xsl:when test="starts-with($type, 'xsd:') or starts-with($type, 'xsd:')">
                <xsl:value-of select="0"/>
            </xsl:when>
            <xsl:when test="starts-with($type, 'tns:ArrayOf')">
                <xsl:value-of select="1"/>
            </xsl:when>
            <xsl:when test="$type = 'tns:EspStringArray' and $useTextareaForStringArray">
                <xsl:value-of select="0"/>
            </xsl:when>
            <xsl:when test="starts-with($type, 'tns:')">
                <xsl:variable name="typeDef" select="$schemaRoot/xsd:complexType[@name=substring($type,5)]"/>
                <xsl:choose>
                    <xsl:when test="$typeDef">
                       <xsl:value-of select="1"/>
                         <!-- // check the number of fields: if 0 or 1 field: don't collapse
                          <xsl:if test="count($typeDef/xsd:sequence/xsd:element)>1">
                            <xsl:value-of select="1"/>
                        </xsl:if>
                          <xsl:if test="count($typeDef/xsd:all/xsd:element)>1">
                            <xsl:value-of select="1"/>
                        </xsl:if>
                        -->
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:value-of select="0"/> <!-- for ESPEnum -->
                    </xsl:otherwise>
                </xsl:choose>
            </xsl:when>
            <xsl:when test="$cpxType/xsd:sequence">
                <xsl:variable name="ems" select="$cpxType/xsd:sequence/xsd:element"/>
                <xsl:choose>
                    <xsl:when test="count($ems)=1 and ($cpxType/xsd:sequence/@maxOccurs='unbounded' or $ems[1]/@maxOccurs='unbounded')">
                        <xsl:choose>
                            <xsl:when test="$useTextareaForStringArray and $ems[1]/@type = 'xsd:string'">
                                <xsl:value-of select="0"/>
                            </xsl:when>
                            <xsl:when test="$useTextareaForStringArray and $ems[1]/@type = 'xs:string'">
                                <xsl:value-of select="0"/>
                            </xsl:when>
                            <xsl:otherwise>
                                <xsl:value-of select="1"/>
                            </xsl:otherwise>
                        </xsl:choose>
                    </xsl:when>
                    <xsl:otherwise>
                        <!-- TODO: check the number of fields: if 0 or 1 field: don't collapse -->
                        <xsl:value-of select="1"/>
                    </xsl:otherwise>
                </xsl:choose>
            </xsl:when>
            <xsl:when test="$cpxType/xsd:all">
                <xsl:value-of select="1"/>
            </xsl:when>
            <xsl:otherwise>
                <xsl:value-of select="0"/>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>

    <!-- =======================================================================================
   Output the form sniplet to input control only (no tag, no enable checkbox etc)
   ======================================================================================= -->
    <xsl:template name="GetInputCtrlHtml">
        <xsl:param name="node"/>
        <xsl:param name="fieldId"/>
        <xsl:param name="collapsed"/>
        <xsl:param name="cpxType" select="$node/xsd:complexType"/>
        <xsl:param name="type" select="$node/@type"/>
        <xsl:if test="$verbose">
            <xsl:variable name="translated_name" select="translate($node/@name,'.-~@#%^:;,/?=+`|&amp;', '_________________')"/>
            <xsl:value-of select="concat('GetInputCtrlHtml(node=', $translated_name, ',fieldId=', $fieldId, ',collapsed=',  $collapsed, ')&lt;br/&gt;') "/>
        </xsl:if>
        <xsl:choose>
            <xsl:when test="not($type) and not($cpxType)">
                <xsl:text disable-output-escaping="yes"><![CDATA[<input type='checkbox' name=']]></xsl:text><xsl:value-of select="$fieldId"/><xsl:text disable-output-escaping="yes"><![CDATA['/>]]></xsl:text>
            </xsl:when>
            <xsl:when test="starts-with($type, 'xsd:') or starts-with($type, 'xs:')">
                <xsl:call-template name="GenXsdTypeHtml">
                    <xsl:with-param name="typeName" select="substring-after($type,':')"/>
                    <xsl:with-param name="fieldId" select="$fieldId"/>
                    <xsl:with-param name="value" select="$node/@default"/>
                    <xsl:with-param name="annot" select="$node/xsd:annotation/xsd:appinfo/form"/>
                    <xsl:with-param name="maxoccurs" select="$node/@maxOccurs"/>
                </xsl:call-template>
            </xsl:when>
            <xsl:when test="starts-with($type, 'tns:ArrayOf')">
                <xsl:variable name="stype" select="substring($type,12)"/>
                <xsl:text disable-output-escaping="yes"><![CDATA["+get_Array_Input("]]></xsl:text>
                <xsl:value-of select="$fieldId"/>
                <xsl:text disable-output-escaping="yes"><![CDATA[","]]></xsl:text>
                <xsl:value-of select="$stype"/>
                <xsl:text disable-output-escaping="yes"><![CDATA[","]]></xsl:text>
                <xsl:value-of select="$stype"/>
                <xsl:text disable-output-escaping="yes"><![CDATA[")+"]]></xsl:text>
            </xsl:when>
            <xsl:when test=" starts-with($type, 'tns:Esp') and substring($type, string-length($type)-4) = 'Array' and $useTextareaForStringArray">
                <xsl:variable name="xsdType">
                    <xsl:value-of select="translate(substring($type,8, string-length($type)-12), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz')"/>
                </xsl:variable>
                <xsl:variable name="formNode" select="$node/xsd:annotation/xsd:appinfo/form"/>
                <xsl:variable name="inputCols">
                    <xsl:choose>
                        <xsl:when test="$formNode/@formCols">
                            <xsl:value-of select="$formNode/@formCols"/>
                        </xsl:when>
                        <xsl:when test="$xsdType='string'">75</xsl:when>
                        <xsl:otherwise>75</xsl:otherwise>
                    </xsl:choose>
                </xsl:variable>
                <xsl:variable name="inputRows">
                    <xsl:choose>
                        <xsl:when test="$formNode/@formRows">
                            <xsl:value-of select="$formNode/@formRows"/>
                        </xsl:when>
                        <xsl:otherwise>20</xsl:otherwise>
                    </xsl:choose>
                </xsl:variable>
                <xsl:text disable-output-escaping="yes"><![CDATA[[Enter one item per line]<br/>]]></xsl:text>
                <xsl:text disable-output-escaping="yes"><![CDATA[<textarea name=']]></xsl:text>
                <xsl:value-of select="$fieldId"/>
                <xsl:text disable-output-escaping="yes"><![CDATA[.Item$' id=']]></xsl:text><!--Note $ at end of name not id marks field as array input via textarea-->
                <xsl:value-of select="$fieldId"/>
                <xsl:text disable-output-escaping="yes"><![CDATA[' cols=']]></xsl:text>
                <xsl:value-of select="$inputCols"/>
                <xsl:text disable-output-escaping="yes"><![CDATA[' rows=']]></xsl:text>
                <xsl:value-of select="$inputRows"/>
                <xsl:text disable-output-escaping="yes"><![CDATA[' >]]></xsl:text>
                <xsl:if test="$set_ctrl_value">
                    <xsl:choose>
                        <xsl:when test="$xsdType = 'string'">
                            <xsl:text disable-output-escaping="yes">esp string array value 1\nesp string array value 2</xsl:text>
                        </xsl:when>
                        <xsl:when test="$xsdType = 'int'">1234\n3456</xsl:when>
                        <xsl:otherwise>Unknown type: <xsl:value-of select="$xsdType"/>
                        </xsl:otherwise>
                    </xsl:choose>
                </xsl:if>
                <xsl:text disable-output-escaping="yes"><![CDATA[</textarea>]]></xsl:text>
            </xsl:when>
            <xsl:when test="$cpxType/xsd:sequence">
                <xsl:variable name="ems" select="$cpxType/xsd:sequence/xsd:element"/>
                <xsl:variable name="emseq" select="$cpxType/xsd:sequence"/>
                <xsl:choose>
                    <xsl:when test="count($ems)=1 and ($cpxType/xsd:sequence/@maxOccurs='unbounded' or $ems[1]/@maxOccurs='unbounded')">
                        <xsl:variable name="stype" select="$ems[1]/@type"/>
                        <xsl:variable name="itemname" select="$ems[1]/@name"/>
                        <xsl:choose>
                            <xsl:when test="$ems/xsd:complexType">
                                <xsl:variable name="btype" select="$ems/@name"/>
                                <xsl:value-of select="concat('&quot;+get_Array_Input(&quot;',$fieldId,'&quot;,&quot;',$btype,'&quot;,&quot;',translate($ems[1]/@name, '.-~@#%^:;,/?=+`|&amp;', '_________________'),'&quot;)+&quot;')"/>
                            </xsl:when>
                            <xsl:when test="starts-with($stype,'tns:')">
                                <xsl:variable name="btype" select="substring($stype,5)"/>
                                <xsl:value-of select="concat('&quot;+get_Array_Input(&quot;',$fieldId,'&quot;,&quot;',$btype,'&quot;,&quot;',translate($ems[1]/@name, '.-~@#%^:;,/?=+`|&amp;', '_________________'),'&quot;)+&quot;')"/>
                            </xsl:when>
                            <xsl:when test="starts-with($stype,'xsd:') or starts-with($stype,'xs:')">
                                <!-- TODO: should we use Add/Delete too? -->
                                <xsl:variable name="xsdType" select="substring-after($stype,':')"/>
                                <!--<xsl:text disable-output-escaping="yes"><xsl:value-of select="$xsdType"/></xsl:text>-->
                                <xsl:if test="$useTextareaForStringArray">
                                    <xsl:variable name="formNode" select="$ems[1]/xsd:annotation/xsd:appinfo/form"/>
                                    <xsl:variable name="inputCols">
                                        <xsl:choose>
                                            <xsl:when test="$formNode/@formCols">
                                                <xsl:value-of select="$formNode/@formCols"/>
                                            </xsl:when>
                                            <xsl:when test="$xsdType='string'">75</xsl:when>
                                            <xsl:otherwise>75</xsl:otherwise>
                                        </xsl:choose>
                                    </xsl:variable>
                                    <xsl:variable name="inputRows">
                                        <xsl:choose>
                                            <xsl:when test="$formNode/@formRows">
                                                <xsl:value-of select="$formNode/@formRows"/>
                                            </xsl:when>
                                            <xsl:otherwise>20</xsl:otherwise>
                                        </xsl:choose>
                                    </xsl:variable>
                                    <xsl:text disable-output-escaping="yes"><![CDATA[[Enter one item per line]<br/>]]></xsl:text>
                                    <xsl:text disable-output-escaping="yes"><![CDATA[<textarea name=']]></xsl:text>
                                    <xsl:value-of select="$fieldId"/>
                                    <xsl:text disable-output-escaping="yes"><![CDATA[.]]></xsl:text>
                                    <xsl:value-of select="$itemname"/>
                                    <xsl:text disable-output-escaping="yes"><![CDATA[$' id=']]></xsl:text><!--Note $ at end of name not id marks field as array input via textarea-->
                                    <xsl:value-of select="$fieldId"/>
                                    <xsl:text disable-output-escaping="yes"><![CDATA[' cols=']]></xsl:text>
                                    <xsl:value-of select="$inputCols"/>
                                    <xsl:text disable-output-escaping="yes"><![CDATA[' rows=']]></xsl:text>
                                    <xsl:value-of select="$inputRows"/>
                                    <xsl:text disable-output-escaping="yes"><![CDATA[' >]]></xsl:text>
                                    <xsl:if test="$set_ctrl_value">
                                        <xsl:choose>
                                            <xsl:when test="$xsdType='string'">
                                                <xsl:text disable-output-escaping="yes">esp string array value 1\nesp string array value 2</xsl:text>
                                            </xsl:when>
                                            <xsl:otherwise>1234\n5678</xsl:otherwise>
                                        </xsl:choose>
                                    </xsl:if>
                                    <xsl:text disable-output-escaping="yes"><![CDATA[</textarea>]]></xsl:text>
                                </xsl:if>
                                <xsl:if test="not($useTextareaForStringArray)">
                                    <!-- new way  -->
                                    <xsl:text disable-output-escaping="yes"><![CDATA["+get_Array_Input("]]></xsl:text>
                                    <!-- <xsl:value-of select="$fieldId"/>   <xsl:text disable-output-escaping="yes"><![CDATA[","EspStringArray","]]></xsl:text> -->
                                    <xsl:value-of select="$fieldId"/>
                                    <xsl:text disable-output-escaping="yes"><![CDATA[","]]></xsl:text>
                                    <xsl:value-of select="'XsdArray'"/>
                                    <xsl:text disable-output-escaping="yes"><![CDATA[","]]></xsl:text>
                                    <xsl:value-of select="translate($ems[1]/@name, '.-~@#%^:;,/?=+`|&amp;', '_________________')"/>
                                    <xsl:text disable-output-escaping="yes"><![CDATA[")+"]]></xsl:text>
                                </xsl:if>
                            </xsl:when>
                        </xsl:choose>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:call-template name="GenEspStructHtmlTable">
                            <xsl:with-param name="nodes" select="($cpxType/*/xsd:element) | ($cpxType/xsd:attribute)"/>
                            <xsl:with-param name="parentId" select="$fieldId"/>
                            <xsl:with-param name="collapsed" select="$collapsed"/>
                        </xsl:call-template>
                    </xsl:otherwise>
                </xsl:choose>
            </xsl:when>
            <xsl:when test="$cpxType/xsd:all">
                <xsl:call-template name="GenEspStructHtmlTable">
                    <xsl:with-param name="nodes" select="$cpxType/xsd:all/xsd:element | $cpxType/xsd:attribute"/>
                    <xsl:with-param name="parentId" select="$fieldId"/>
                    <xsl:with-param name="collapsed" select="$collapsed"/>
                </xsl:call-template>
            </xsl:when>
            <xsl:when test="starts-with($type, 'tns:')">
                  <xsl:variable name="bareType" select="substring($type,5)"/>
                  <xsl:choose>
                    <xsl:when test="$schemaRoot/xsd:complexType[@name=$bareType]/*/xsd:element">
                        <xsl:call-template name="GetInputCtrlHtml">
                            <xsl:with-param name="node" select="$node"/>
                            <xsl:with-param name="fieldId" select="$fieldId"/>
                            <xsl:with-param name="collapsed" select="$collapsed"/>
                            <xsl:with-param name="cpxType" select="$schemaRoot/xsd:complexType[@name=$bareType]"/>
                            <xsl:with-param name="type" select="$type"/>
                        </xsl:call-template>
                        <!--xsl:call-template name="GenEspStructHtmlTable">
                            <xsl:with-param name="nodes" select="($schemaRoot/xsd:complexType[@name=$bareType]/*/xsd:element) | ($schemaRoot/xsd:complexType[@name=$bareType]/xsd:attribute)"/>
                            <xsl:with-param name="parentId" select="$fieldId"/>
                            <xsl:with-param name="collapsed" select="$collapsed"/>
                        </xsl:call-template-->
                    </xsl:when>
                    <xsl:when test="$schemaRoot/xsd:complexType[@name=$bareType]/xsd:simpleContent">
                        <xsl:variable name="nodes" select="$schemaRoot/xsd:complexType[@name=$bareType]/xsd:simpleContent/xsd:extension/xsd:attribute"/>
                        <xsl:if test="$verbose">
                            <xsl:value-of select="concat('  *** simpleContent: baseType=', $bareType, ', attributes=', count($nodes), ')&lt;br/&gt;') "/>
                        </xsl:if>
                        <xsl:call-template name="GenEspStructHtmlTable">
                            <xsl:with-param name="nodes" select="$nodes"/>
                            <xsl:with-param name="parentId" select="$fieldId"/>
                            <xsl:with-param name="collapsed" select="$collapsed"/>
                            <xsl:with-param name="simpleContent" select="true()"/>
                        </xsl:call-template>
                    </xsl:when>
                    <xsl:when test="$schemaRoot/xsd:simpleType[@name=$bareType]/xsd:restriction/xsd:enumeration">
                        <xsl:text disable-output-escaping="yes">" + get_</xsl:text><xsl:value-of select="$bareType"/><xsl:text disable-output-escaping="yes">_Enum("</xsl:text>
                        <xsl:value-of select="$fieldId"/>
                        <xsl:text disable-output-escaping="yes">","</xsl:text>
                        <xsl:value-of select="$node/@default"/>
                        <xsl:text disable-output-escaping="yes">") + "</xsl:text>
                    </xsl:when>
                    <xsl:when test="$schemaRoot/xsd:simpleType[@name=$bareType]/xsd:restriction">
                        <xsl:variable name="restrict" select="$schemaRoot/xsd:simpleType[@name=$bareType]/xsd:restriction"/>
                        <xsl:call-template name="GenXsdTypeHtml">
                                <xsl:with-param name="typeName" select="substring-after($restrict/@base, ':')"/>
                                    <xsl:with-param name="fieldId" select="$fieldId"/>
                                    <xsl:with-param name="value" select="$node/@default"/>
                                    <xsl:with-param name="annot" select="$node/xsd:annotation/xsd:appinfo/form"/>
                                    <xsl:with-param name="field_len" select="$restrict/xsd:maxLength/@value"/>
                            </xsl:call-template>
                    </xsl:when>
                    <xsl:when test="$schemaRoot/xsd:complexType[@name=$bareType]/xsd:attribute"> <!-- attributes only -->
                        <xsl:call-template name="GenEspStructHtmlTable">
                            <xsl:with-param name="nodes" select="$schemaRoot/xsd:complexType[@name=$bareType]/xsd:attribute"/>
                            <xsl:with-param name="parentId" select="$fieldId"/>
                            <xsl:with-param name="collapsed" select="$collapsed"/>
                        </xsl:call-template>
                    </xsl:when>
                    <xsl:when test="$schemaRoot/xsd:complexType[@name=$bareType]/xsd:all">
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:value-of select="concat('WARNING[1]: unknown type: ', $type, ',fieldId=',$fieldId)"/>
                    </xsl:otherwise>
                </xsl:choose>
            </xsl:when>
            <xsl:otherwise>
                <xsl:if test="$type">
                    <xsl:value-of select="concat('WARNING[1]: unknown type: ', $type, ',fieldId=',$fieldId)"/>
                </xsl:if>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>
    <!-- =======================================================================================
   Output the form sniplet to input ESP struct WITHOUT <table> </table>
   ======================================================================================= -->

    <xsl:template name="GenOneInputCtrlHtml">
        <xsl:param name="node"/>
        <xsl:param name="parentId"/>

        <xsl:if test="$verbose">
            <xsl:variable name="translated_name" select="translate($node/@name, '.-~@#%^:;,/?=+`|&amp;', '_________________')"/>
            <xsl:value-of select="concat('GenOneInputCtrlHtml(node=', $translated_name, ',parentId=', $parentId, ')&lt;br/&gt;') "/>
        </xsl:if>

        <xsl:variable name="fieldName" select="translate($node/@name, '.-~@#%^:;,/?=+`|&amp;', '_________________')"/>
        <xsl:variable name="fieldId">
            <xsl:call-template name="MakeId">
                <xsl:with-param name="parentId" select="$parentId"/>
                <xsl:with-param name="fieldName" select="$fieldName"/>
                <xsl:with-param name="isattr" select="name($node)='xsd:attribute'"/>
            </xsl:call-template>
        </xsl:variable>
        <xsl:variable name="ui" select="$node/xsd:annotation/xsd:appinfo/form/@ui"/>
        <xsl:variable name="collapsed" select="boolean($node/xsd:annotation/xsd:appinfo/form/@collapsed)"/>
        <xsl:variable name="tmp_result">
            <xsl:call-template name="IsNodeCollapsable">
                <xsl:with-param name="node" select="$node"/>
            </xsl:call-template>
        </xsl:variable>
        <xsl:variable name="collapsable" select="number($tmp_result)"/>
        <xsl:variable name="inputCtrlHtml">
            <xsl:call-template name="GetInputCtrlHtml">
                <xsl:with-param name="node" select="$node"/>
                <xsl:with-param name="fieldId" select="$fieldId"/>
                <xsl:with-param name="collapsed" select="$collapsed"/>
            </xsl:call-template>
        </xsl:variable>

        <xsl:call-template name="GenOneInputCtrlHtmlRaw">
            <xsl:with-param name="parentId" select="$parentId"/>
            <xsl:with-param name="fieldName" select="$fieldName"/>
            <xsl:with-param name="fieldId" select="$fieldId"/>
            <xsl:with-param name="ui" select="$ui"/>
            <xsl:with-param name="collapsable" select="$collapsable"/>
            <xsl:with-param name="collapsed" select="$collapsed"/>
            <xsl:with-param name="inputCtrlHtml" select="$inputCtrlHtml"/>
            <xsl:with-param name="isAttr" select="name($node)='xsd:attribute'"/>
            <xsl:with-param name="isBool" select="$node/@type='xsd:boolean'"/>
        </xsl:call-template>
    </xsl:template >

    <xsl:template name="GenOneInputCtrlHtmlRaw">
        <xsl:param name="parentId"/>
        <xsl:param name="fieldName"/>
        <xsl:param name="fieldId"/>
        <xsl:param name="collapsable" select="false()"/>
        <xsl:param name="collapsed" select="false()"/>
        <xsl:param name="ui" select="''"/>
        <xsl:param name="inputCtrlHtml" select="''"/>
        <xsl:param name="isAttr" select="false()"/>
        <xsl:param name="isBool" select="false()"/>

        <!--    <xsl:if test="$verbose"><xsl:value-of select="concat('GenOneInputCtrlHtmlRaw(parentId=', $parentId, ', fieldId=', $fieldId, ', ui=', $ui, ', inputCtrlHtml=', $inputCtrlHtml, ')&lt;br/&gt;') "/> </xsl:if> -->

        <xsl:variable name="ctrlId">
            <xsl:if test="$show_ctrl_name">
                <xsl:value-of select="concat(' {',$fieldId,'}')"/>
            </xsl:if>
        </xsl:variable>
        <!-- output the html that render the input for the $fieldId -->
        <!-- collapse/expand image -->
        <xsl:text disable-output-escaping="yes"><![CDATA[<td]]></xsl:text>
        <xsl:text disable-output-escaping="yes"><![CDATA[>]]></xsl:text>

        <xsl:if test="$collapsable">
            <xsl:text disable-output-escaping="yes"><![CDATA[<img id='$I.]]></xsl:text>
            <xsl:value-of select="$fieldId"/>
            <xsl:if test="$collapsed">
                <xsl:text disable-output-escaping="yes"><![CDATA[' src='/esp/files/img/form_plus.gif' onclick='hideIt(\"]]></xsl:text>
            </xsl:if>
            <xsl:if test="not($collapsed)">
                <xsl:text disable-output-escaping="yes"><![CDATA[' src='/esp/files/img/form_minus.gif' onclick='hideIt(\"]]></xsl:text>
              </xsl:if>
            <xsl:value-of select="$fieldId"/>
            <xsl:text disable-output-escaping="yes"><![CDATA[\")' alt='+'/>]]></xsl:text>
        </xsl:if>

        <xsl:text disable-output-escaping="yes"><![CDATA[</td>]]></xsl:text>

        <!-- label -->
        <xsl:choose>
            <xsl:when test="$collapsable">
                <xsl:text disable-output-escaping="yes"><![CDATA[<td><span]]></xsl:text>
                                <xsl:text disable-output-escaping="yes"><![CDATA[ id='$L.]]></xsl:text>
                                <xsl:value-of select="$fieldId"/>
                                <xsl:text disable-output-escaping="yes"><![CDATA['> <b>]]></xsl:text>
                <xsl:value-of select="$fieldName"/>
                <xsl:text disable-output-escaping="yes"><![CDATA[</b>]]></xsl:text>
                <xsl:value-of select="$ctrlId"/>
                <xsl:choose>
                    <xsl:when test="$isBool">
                        <xsl:text disable-output-escaping="yes"><![CDATA[?]]></xsl:text>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:text disable-output-escaping="yes"><![CDATA[:]]></xsl:text>
                    </xsl:otherwise>
                </xsl:choose>
                <xsl:value-of select="$ui"/>
                <xsl:text disable-output-escaping="yes"><![CDATA[</td><td>]]></xsl:text>
            </xsl:when>
            <xsl:otherwise>
                <xsl:text disable-output-escaping="yes"><![CDATA[<td><span id='$L.]]></xsl:text>
                <xsl:value-of select="$fieldId"/>
                <xsl:text disable-output-escaping="yes"><![CDATA['><b>]]></xsl:text>
                <xsl:if test="$isAttr"><xsl:text disable-output-escaping="yes"><![CDATA[<i>@]]></xsl:text></xsl:if>
                <xsl:value-of select="$fieldName"/>
                <xsl:if test="$isAttr"><xsl:text disable-output-escaping="yes"><![CDATA[</i>]]></xsl:text></xsl:if>
                <xsl:text disable-output-escaping="yes"><![CDATA[</b></span>]]></xsl:text>
                <xsl:value-of select="$ctrlId"/>
                <xsl:choose>
                    <xsl:when test="$isBool">
                        <xsl:text disable-output-escaping="yes"><![CDATA[?]]></xsl:text>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:text disable-output-escaping="yes"><![CDATA[:]]></xsl:text>
                    </xsl:otherwise>
                </xsl:choose>
                <xsl:text disable-output-escaping="yes"><![CDATA[</td><td>]]></xsl:text>
            </xsl:otherwise>
        </xsl:choose>
        <!-- more -->
        <xsl:if test="$collapsable">
              <xsl:text disable-output-escaping="yes"><![CDATA[<span id='$M.]]></xsl:text>
              <xsl:value-of select="$fieldId"/>
                        <xsl:if test="not($collapsed)">
                <xsl:text disable-output-escaping="yes"><![CDATA[' style='display:none]]></xsl:text>
            </xsl:if>
            <xsl:text disable-output-escaping="yes"><![CDATA['><img src='/esp/files/img/form_more.gif' onclick='onMore(\"]]></xsl:text>
            <xsl:value-of select="$fieldId"/>
                               <xsl:text disable-output-escaping="yes"><![CDATA[\")' alt='More'/></span>]]></xsl:text>
        </xsl:if>
        <!-- control -->
        <xsl:value-of select="$inputCtrlHtml"/>
        <xsl:text disable-output-escaping="yes"><![CDATA[</td>]]></xsl:text>
    </xsl:template>

    <xsl:template name="GenEspStructHtmlBare">
        <xsl:param name="nodes"/>
        <xsl:param name="parentId"/>
        <xsl:if test="$verbose">
            <xsl:value-of select="concat('GenEspStructHtmlBare(parent=', $parentId, ', nodes=', count($nodes), ')&lt;br/&gt;')"/>
        </xsl:if>
        <xsl:choose>
            <xsl:when test="$nodes">
                <xsl:variable name="first">
                    <xsl:call-template name="GenOneInputCtrlHtml">
                        <xsl:with-param name="node" select="$nodes[1]"/>
                        <xsl:with-param name="parentId" select="$parentId"/>
                    </xsl:call-template>
                </xsl:variable>
                <xsl:variable name="rest">
                    <xsl:if test="count($nodes) &gt; 1">
                        <xsl:call-template name="GenEspStructHtmlBare">
                            <xsl:with-param name="nodes" select="$nodes[position()!=1]"/>
                            <xsl:with-param name="parentId" select="$parentId"/>
                        </xsl:call-template>
                    </xsl:if>
                </xsl:variable>
                <xsl:value-of select="concat('&lt;tr&gt;',$first,$rest,'&lt;/tr&gt;')"/>
            </xsl:when>
        </xsl:choose>
    </xsl:template>
    <!-- =======================================================================================
  Output the form sniplet to input ESP struct with <table> </table>
   ======================================================================================= -->
    <xsl:template name="ConstructHtmlTable">
           <xsl:param name="core"/>
        <xsl:param name="parentId"/>
        <xsl:param name="collapsed"/>
        <!-- control -->
        <xsl:text disable-output-escaping="yes"><![CDATA[<span id='$C.]]></xsl:text>
        <xsl:value-of select="$parentId"/>
        <xsl:if test="$collapsed">
            <xsl:text disable-output-escaping="yes"><![CDATA[' style='display:none]]></xsl:text>
        </xsl:if>
        <xsl:text disable-output-escaping="yes"><![CDATA['><table]]></xsl:text>
        <xsl:if test="$useTableBorder">
            <xsl:text disable-output-escaping="yes"><![CDATA[ class='struct'> ]]></xsl:text>
            <xsl:value-of select="$core"/>
            <xsl:text disable-output-escaping="yes"><![CDATA[</table> </span>]]></xsl:text>
        </xsl:if>
        <xsl:if test="not($useTableBorder)">
            <xsl:text disable-output-escaping="yes"><![CDATA[> <tr> <td colspan='3'> <hr/> </td> </tr>]]></xsl:text>
            <xsl:value-of select="$core"/>
            <xsl:text disable-output-escaping="yes"><![CDATA[<tr> <td colspan='3'> <hr/> </td> </tr> </table> </span>]]></xsl:text>
        </xsl:if>
    </xsl:template>
      <xsl:template name="GenEspStructHtmlTable">
        <xsl:param name="nodes"/>
        <xsl:param name="parentId"/>
        <xsl:param name="collapsed" select="false()"/>
        <xsl:param name="simpleContent" select="false()"/>

        <xsl:if test="$verbose">
            <xsl:value-of select="concat('GenEspStructHtmlTable(nodes=', count(nodes),  ',parentId=', $parentId, ',collapsed=', $collapsed, ', simpleContent=', string($simpleContent),  ')&lt;br/&gt;')"/>
        </xsl:if>
        <xsl:variable name="core">
            <xsl:if test="$simpleContent">
                <xsl:text disable-output-escaping="yes"><![CDATA[<tr><td colspan='3'><b>*</b>]]></xsl:text>
                <xsl:if test="$show_ctrl_name">
                    <xsl:value-of select="concat(' {', $parentId, '}')"/>
                </xsl:if>
                    <xsl:text disable-output-escaping="yes"><![CDATA[<input type='text' size='50' id=']]></xsl:text>
                    <xsl:value-of select="$parentId"/>
                    <xsl:text disable-output-escaping="yes"><![CDATA[' name=']]></xsl:text>
                    <xsl:value-of select="$parentId"/>
                    <xsl:text disable-output-escaping="yes"><![CDATA['/></td></tr>]]></xsl:text>
            </xsl:if>
            <xsl:call-template name="GenEspStructHtmlBare">
                <xsl:with-param name="parentId" select="$parentId"/>
                <xsl:with-param name="nodes" select="$nodes[name()='xsd:attribute']"/>
            </xsl:call-template>
            <xsl:call-template name="GenEspStructHtmlBare">
                <xsl:with-param name="parentId" select="$parentId"/>
                <xsl:with-param name="nodes" select="$nodes[name()!='xsd:attribute']"/>
            </xsl:call-template>
        </xsl:variable>
             <xsl:call-template name="ConstructHtmlTable">
                  <xsl:with-param name="core" select="$core"/>
                  <xsl:with-param name="parentId" select="$parentId"/>
                  <xsl:with-param name="collapsed" select="$collapsed"/>
             </xsl:call-template>
      </xsl:template>
      <xsl:template name="GenEspEnumHtmlTable">
        <xsl:param name="type"/>
        <xsl:param name="parentId"/>
        <xsl:param name="collapsed" select="false()"/>
        <xsl:if test="$verbose">
            <xsl:value-of select="concat('GenEspEnumHtmlTable(type=', $type,  ',parentId=', $parentId, ',collapsed=', $collapsed, ')&lt;br/&gt;')"/>
        </xsl:if>
        <xsl:variable name="core">
            <xsl:text disable-output-escaping="yes"><![CDATA[<tr><td>"+ get_]]></xsl:text>
            <xsl:value-of select="translate($type, '.-~@#%^:;,/?=+`|&amp;', '_________________')"/>
            <xsl:text disable-output-escaping="yes"><![CDATA[_Enum('$$','') + "</td></tr>]]></xsl:text>
        </xsl:variable>
             <xsl:call-template name="ConstructHtmlTable">
                  <xsl:with-param name="core" select="$core"/>
                  <xsl:with-param name="parentId" select="'$$'"/>
                  <xsl:with-param name="collapsed" select="$collapsed"/>
             </xsl:call-template>
      </xsl:template>
    <!-- html for xsd build-in types -->
    <xsl:template name="GenXsdTypeHtml">
        <xsl:param name="typeName"/>
        <xsl:param name="fieldId"/>
        <xsl:param name="value"/>
        <xsl:param name="annot"/>
        <xsl:param name="field_len"/>
        <xsl:param name="maxoccurs"/>
        <xsl:choose>
            <xsl:when test="$annot/select/option">
                <xsl:text disable-output-escaping="yes"><![CDATA[<select ]]></xsl:text>
                    <xsl:text disable-output-escaping="yes"><![CDATA[name=']]></xsl:text>
                    <xsl:value-of select="$fieldId"/>
                    <xsl:text disable-output-escaping="yes"><![CDATA[' id=']]></xsl:text>
                    <xsl:value-of select="$fieldId"/>
                    <xsl:text disable-output-escaping="yes"><![CDATA['>]]></xsl:text>
                    <xsl:for-each select="$annot/select/option">
                        <xsl:text disable-output-escaping="yes"><![CDATA[<option value=']]></xsl:text>
                        <xsl:value-of select="@value"/><xsl:if test="@selected"><xsl:text disable-output-escaping="yes"><![CDATA[' selected='true]]></xsl:text></xsl:if>
                        <xsl:text disable-output-escaping="yes"><![CDATA['>]]></xsl:text><xsl:value-of select="@name"/><xsl:text disable-output-escaping="yes"><![CDATA[</option>]]></xsl:text>
                    </xsl:for-each>
                <xsl:text disable-output-escaping="yes"><![CDATA[</select>]]></xsl:text>
            </xsl:when>
            <!-- string -->
            <xsl:when test="$typeName='string'">
                <xsl:variable name="inputRows">
                    <xsl:choose>
                        <xsl:when test="$annot/@formRows">
                            <xsl:value-of select="$annot/@formRows"/>
                        </xsl:when>
                        <xsl:when test="$maxoccurs='unbounded'">
                            <xsl:number value="4"/>
                        </xsl:when>
                        <xsl:otherwise>0</xsl:otherwise>
                    </xsl:choose>
                </xsl:variable>
                <xsl:variable name="inputCols">
                    <xsl:choose>
                        <xsl:when test="$annot/@formCols">
                            <xsl:value-of select="$annot/@formCols"/>
                        </xsl:when>
                        <xsl:when test="number($field_len)>0">
                            <xsl:value-of select="$field_len"/>
                        </xsl:when>
                        <xsl:otherwise>50</xsl:otherwise>
                    </xsl:choose>
                </xsl:variable>
                <xsl:choose>
                    <!-- use text area for string type -->
                    <xsl:when test="number($inputRows)">
                        <xsl:if test="$maxoccurs='unbounded'">
                            <xsl:text disable-output-escaping="yes"><![CDATA[[Enter one item per line]<br/>]]></xsl:text>
                        </xsl:if>
                        <xsl:text disable-output-escaping="yes"><![CDATA[<textarea rows=']]></xsl:text>
                        <xsl:value-of select="$inputRows"/>
                        <xsl:text disable-output-escaping="yes"><![CDATA[' cols=']]></xsl:text>
                        <xsl:value-of select="$inputCols"/>
                        <xsl:text disable-output-escaping="yes"><![CDATA[' name=']]></xsl:text>
                        <xsl:value-of select="$fieldId"/>
                        <xsl:if test="$maxoccurs='unbounded'">$</xsl:if>
                        <xsl:text disable-output-escaping="yes"><![CDATA[' id=']]></xsl:text>
                        <xsl:value-of select="$fieldId"/>
                        <xsl:text disable-output-escaping="yes"><![CDATA['>]]></xsl:text>
                        <xsl:choose>
                            <xsl:when test="not($noDefaultValue) and $value">
                                <xsl:value-of select="$value"/>
                            </xsl:when>
                            <xsl:when test="$set_ctrl_value">
                                <xsl:value-of select="$fieldId"/>
                            </xsl:when>
                        </xsl:choose>
                        <xsl:text disable-output-escaping="yes"><![CDATA[</textarea>]]></xsl:text>
                    </xsl:when>
                    <!-- use input for string type -->
                    <!-- -->
                    <xsl:otherwise>
                        <xsl:variable name="inputType">
                            <xsl:choose>
                                <xsl:when test="$annot/@password">password</xsl:when>
                                <xsl:otherwise>text</xsl:otherwise>
                            </xsl:choose>
                        </xsl:variable>
                        <xsl:text disable-output-escaping="yes"><![CDATA[<input type=']]></xsl:text>
                        <xsl:value-of select="$inputType"/>
                        <xsl:text disable-output-escaping="yes"><![CDATA[' name=']]></xsl:text>
                        <xsl:value-of select="$fieldId"/>
                        <xsl:text disable-output-escaping="yes"><![CDATA[' id=']]></xsl:text>
                        <xsl:value-of select="$fieldId"/>
                        <xsl:choose>
                            <xsl:when test="$value and not($noDefaultValue)">
                                <xsl:text disable-output-escaping="yes"><![CDATA[' value=']]></xsl:text>
                                <xsl:value-of select="$value"/>
                            </xsl:when>
                            <xsl:when test="$set_ctrl_value">
                                <xsl:text disable-output-escaping="yes"><![CDATA[' value=']]></xsl:text>
                                <xsl:value-of select="$fieldId"/>
                            </xsl:when>
                        </xsl:choose>
                        <xsl:text disable-output-escaping="yes"><![CDATA[' size=']]></xsl:text>
                        <xsl:value-of select="$inputCols"/>
                        <xsl:text disable-output-escaping="yes"><![CDATA[' ></input>]]></xsl:text>
                    </xsl:otherwise>
                    <!-- -->
                </xsl:choose>
            </xsl:when>
            <!-- numbers -->
            <xsl:when test="$typeName='int' or $typeName='integer' or $typeName='short' or $typeName = 'long' or $typeName='unsignedInt' or $typeName='nonPositiveInteger' or $typeName='negative' or  $typeName='nonNegativeInteger' or $typeName='positiveInteger' or $typeName='imsignedShort' or $typeName='unsignedLong' or $typeName = 'long' or $typeName='byte' or $typeName='unsignedByte' or $typeName='double' or $typeName='float' ">
                <xsl:variable name="inputCols">
                    <xsl:choose>
                        <xsl:when test="$annot/@formCols">
                            <xsl:value-of select="$annot/@formCols"/>
                        </xsl:when>
                        <xsl:otherwise>20</xsl:otherwise>
                    </xsl:choose>
                </xsl:variable>
                <xsl:variable name="inputRows">
                    <xsl:choose>
                        <xsl:when test="$annot/@formRows">
                            <xsl:value-of select="$annot/@formRows"/>
                        </xsl:when>
                        <xsl:when test="$maxoccurs='unbounded'">
                            <xsl:number value="4"/>
                        </xsl:when>
                        <xsl:otherwise>0</xsl:otherwise>
                    </xsl:choose>
                </xsl:variable>
                <xsl:choose>
                    <!-- use text area for string type -->
                    <xsl:when test="number($inputRows)">
                        <xsl:if test="$maxoccurs='unbounded'">
                            <xsl:text disable-output-escaping="yes"><![CDATA[[Enter one item per line]<br/>]]></xsl:text>
                        </xsl:if>
                        <xsl:text disable-output-escaping="yes"><![CDATA[<textarea rows=']]></xsl:text>
                        <xsl:value-of select="$inputRows"/>
                        <xsl:text disable-output-escaping="yes"><![CDATA[' cols=']]></xsl:text>
                        <xsl:value-of select="$inputCols"/>
                        <xsl:text disable-output-escaping="yes"><![CDATA[' name=']]></xsl:text>
                        <xsl:value-of select="$fieldId"/>
                        <xsl:if test="$maxoccurs='unbounded'">$</xsl:if>
                        <xsl:text disable-output-escaping="yes"><![CDATA[' id=']]></xsl:text>
                        <xsl:value-of select="$fieldId"/>
                        <xsl:text disable-output-escaping="yes"><![CDATA['>]]></xsl:text>
                        <xsl:choose>
                            <xsl:when test="not($noDefaultValue) and $value">
                                <xsl:value-of select="$value"/>
                            </xsl:when>
                            <xsl:when test="$set_ctrl_value">
                                <xsl:value-of select="$fieldId"/>
                            </xsl:when>
                        </xsl:choose>
                        <xsl:text disable-output-escaping="yes"><![CDATA[</textarea>]]></xsl:text>
                    </xsl:when>
                    <!-- use input for string type -->
                    <!-- -->
                    <xsl:otherwise>
                        <xsl:text disable-output-escaping="yes"><![CDATA[<input type='text' name=']]></xsl:text>
                        <xsl:value-of select="$fieldId"/>
                        <xsl:text disable-output-escaping="yes"><![CDATA[' id=']]></xsl:text>
                        <xsl:value-of select="$fieldId"/>
                        <xsl:choose>
                            <xsl:when test="not($noDefaultValue) and $value">
                                <xsl:text disable-output-escaping="yes"><![CDATA[' value=']]></xsl:text>
                                <xsl:value-of select="$value"/>
                            </xsl:when>
                            <xsl:when test="$set_ctrl_value">
                                <xsl:text disable-output-escaping="yes"><![CDATA[' value='12]]></xsl:text>
                            </xsl:when>
                        </xsl:choose>
                        <xsl:text disable-output-escaping="yes"><![CDATA[' size=']]></xsl:text>
                        <xsl:value-of select="$inputCols"/>
                        <xsl:text disable-output-escaping="yes"><![CDATA[' ></input>]]></xsl:text>
                    </xsl:otherwise>
                </xsl:choose>
            </xsl:when>
            <!-- boolean -->
            <xsl:when test="$typeName='boolean'">
             <xsl:variable name="checkval">
               <xsl:choose>
                <xsl:when test="$noDefaultValue">default</xsl:when>
                <xsl:when test="$value='true' or $value='1'">true</xsl:when>
                <xsl:when test="$value='false' or $value='0'">false</xsl:when>
                <xsl:otherwise>default</xsl:otherwise>
             </xsl:choose>
            </xsl:variable>
            <!-- use tristate true/false/default -->

            <xsl:text disable-output-escaping="yes"><![CDATA[<input class='tributton' type='text' readonly='1' size='6' onkeypress='onTriButtonKeyPress(this)' onClick='onClickTriButton(this, 1)']]></xsl:text>
            <xsl:if test="$checkval!='default'">
              <xsl:text disable-output-escaping="yes"><![CDATA[ name=']]></xsl:text>
              <xsl:value-of select="$fieldId"/>
            </xsl:if>
            <xsl:text disable-output-escaping="yes"><![CDATA[' id=']]></xsl:text>
            <xsl:value-of select="$fieldId"/>
            <xsl:text disable-output-escaping="yes"><![CDATA[' value=']]></xsl:text>
            <xsl:value-of select="$checkval"/>
            <xsl:text disable-output-escaping="yes"><![CDATA['></input>]]></xsl:text>
            </xsl:when>
            <!-- other native schema types: treat as string -->
            <xsl:otherwise>
                <!--  <xsl:value-of select="concat('WARNING[2]: Unhandled XSD type:', $typeName, ', id=',$fieldId)" /> -->
                <xsl:text disable-output-escaping="yes"><![CDATA[<input type='text' name=']]></xsl:text>
                <xsl:value-of select="$fieldId"/>
                <xsl:text disable-output-escaping="yes"><![CDATA[' id=']]></xsl:text>
                <xsl:value-of select="$fieldId"/>
                <xsl:if test="$set_ctrl_value">
                    <xsl:text disable-output-escaping="yes"><![CDATA[' value=']]></xsl:text>
                    <xsl:value-of select="$fieldId"/>
                </xsl:if>
                <xsl:text disable-output-escaping="yes"><![CDATA[' size='50' ></input>]]></xsl:text>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>
    <!-- ================================================================================
          get all types used in an ESP array
  ================================================================================ -->
    <!-- remove type duplicates in : separated string -->
    <xsl:template name="RemoveDuplicates">
        <xsl:param name="types"/>
        <xsl:if test="$types">
            <xsl:variable name="first" select="substring-before($types,':')"/>
            <xsl:variable name="rest" select="substring-after($types,':')"/>
            <xsl:choose>
                <xsl:when test="$first">
                    <xsl:variable name="restclean">
                        <xsl:call-template name="RemoveDuplicates">
                            <xsl:with-param name="types" select="$rest"/>
                        </xsl:call-template>
                    </xsl:variable>
                    <xsl:choose>
                        <xsl:when test="contains(concat(':',$rest,':'),concat(':',$first,':'))">
                            <xsl:value-of select="$restclean"/>
                        </xsl:when>
                        <xsl:otherwise>
                            <xsl:value-of select="concat($first,':',$restclean)"/>
                        </xsl:otherwise>
                    </xsl:choose>
                </xsl:when>
                <xsl:otherwise>
                    <xsl:value-of select="$types"/>
                </xsl:otherwise>
            </xsl:choose>
        </xsl:if>
    </xsl:template>

    <!-- get enum type used in an element node -->
    <xsl:template name="GetEnumTypes">
        <xsl:param name="node"/>
        <xsl:if test="$node">
            <xsl:choose>
                <xsl:when test="$node/@type">
                    <xsl:variable name="type" select="$node/@type"/>
                    <xsl:if test="starts-with($type, 'tns:')">
                          <xsl:variable name="bareType" select="substring($type,5)"/>
                        <xsl:choose>
                            <xsl:when test="$schemaRoot/xsd:simpleType[@name=$bareType]/xsd:restriction/xsd:enumeration">
                               <xsl:value-of select="substring($type,5)"/>
                            </xsl:when>
                            <xsl:when test="$schemaRoot/xsd:complexType[@name=$bareType]">
                                <xsl:call-template name="GetEnumTypesOfComplexType">
                                    <xsl:with-param name="complexNode" select="$schemaRoot/xsd:complexType[@name=$bareType]"/>
                                </xsl:call-template>
                            </xsl:when>
                        </xsl:choose>
                    </xsl:if>
                </xsl:when>
                <xsl:when test="$node/xsd:complexType">
                   <xsl:call-template name="GetEnumTypesOfComplexType">
                      <xsl:with-param name="complexNode" select="$node/xsd:complexType"/>
                   </xsl:call-template>
                </xsl:when>
            </xsl:choose>
        </xsl:if>
    </xsl:template>

    <!-- get enum types used in a complex type node -->
    <xsl:template name="GetEnumTypesOfComplexType">
        <xsl:param name="complexNode"/>
        <xsl:if test="$complexNode">
            <xsl:choose>
                <xsl:when test="$complexNode/xsd:sequence">
                    <xsl:call-template name="GetEnumTypesOfElements">
                        <xsl:with-param name="nodes" select="$complexNode/xsd:sequence/xsd:element"/>
                    </xsl:call-template>
                </xsl:when>
                <xsl:when test="$complexNode/xsd:all">
                    <xsl:call-template name="GetEnumTypesOfElements">
                        <xsl:with-param name="nodes" select="$complexNode/xsd:all/xsd:element"/>
                    </xsl:call-template>
                </xsl:when>
            </xsl:choose>
        </xsl:if>
    </xsl:template>
    <!-- get array type used in element nodes -->
    <xsl:template name="GetEnumTypesOfElements">
        <xsl:param name="nodes"/>
        <xsl:variable name="first">
            <xsl:call-template name="GetEnumTypes">
                <xsl:with-param name="node" select="$nodes[1]"/>
            </xsl:call-template>
        </xsl:variable>
        <xsl:variable name="rest">
            <xsl:if test="count($nodes) &gt; 1">
                <xsl:call-template name="GetEnumTypesOfElements">
                    <xsl:with-param name="nodes" select="$nodes[position()!=1]"/>
                </xsl:call-template>
            </xsl:if>
        </xsl:variable>
        <xsl:choose>
            <xsl:when test="string($first) and string($rest)">
                <!-- $rest is a tree fragment =: always true! -->
                <xsl:value-of select="concat($first, ':', $rest)"/>
            </xsl:when>
            <xsl:when test="string($first)">
                <xsl:value-of select="$first"/>
            </xsl:when>
            <xsl:otherwise>
                <xsl:value-of select="$rest"/>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>

    <!-- get array type used in an element node -->
    <xsl:template name="GetArrayTypes">
        <xsl:param name="node"/>
        <xsl:choose>
            <xsl:when test="not($node)">
                <!-- <xsl:value-of select="concat('Warning[3]: Fail to find type definition for ', $node/@name)" /> -->
            </xsl:when>
            <xsl:when test="$node/@type">
                <xsl:variable name="type" select="$node/@type"/>
                <xsl:choose>
                    <xsl:when test="$type = 'tns:EspStringArray'">
                        <!--<xsl:value-of select="'EspStringArray'"/>-->
                    </xsl:when>
                    <xsl:when test="starts-with($type, 'tns:ArrayOf')">
                        <xsl:variable name="more">
                            <xsl:call-template name="GetArrayTypesOfComplexType">
                                <xsl:with-param name="complexNode" select="$schemaRoot/xsd:complexType[@name=substring($type,12)]"/>
                            </xsl:call-template>
                        </xsl:variable>
                        <xsl:choose>
                            <xsl:when test="string($more)">
                                <xsl:value-of select="concat(substring($type,12),':',$more)"/>
                            </xsl:when>
                            <xsl:otherwise>
                                <xsl:value-of select="substring($type,12)"/>
                            </xsl:otherwise>
                        </xsl:choose>
                    </xsl:when>
                    <xsl:when test="$node/@maxOccurs='unbounded' and $type='xsd:string'">
                        <!-- <xsl:value-of select="'EspStringArray'"/>-->
                    </xsl:when>
                    <xsl:when test="($node/@maxOccurs='unbounded' or $node/../@maxOccurs='unbounded') and starts-with($type,'tns:')"> <!--here-->
                        <xsl:variable name="more">
                            <xsl:call-template name="GetArrayTypesOfComplexType">
                                <xsl:with-param name="complexNode" select="$schemaRoot/xsd:complexType[@name=substring($type,5)]"/>
                            </xsl:call-template>
                        </xsl:variable>
                        <xsl:choose>
                            <xsl:when test="string($more)">
                                <xsl:value-of select="concat(substring($type,5),':',$more)"/>
                            </xsl:when>
                            <xsl:otherwise>
                                <xsl:value-of select="substring($type,5)"/>
                            </xsl:otherwise>
                        </xsl:choose>
                    </xsl:when>
                    <xsl:when test="starts-with($type,'tns:')">
                        <xsl:call-template name="GetArrayTypesOfComplexType">
                            <xsl:with-param name="complexNode" select="$schemaRoot/xsd:complexType[@name=substring($type,5)]"/>
                        </xsl:call-template>
                    </xsl:when>
                </xsl:choose>
            </xsl:when>
            <xsl:when test="$node/xsd:complexType">
                <xsl:variable name="parent" select="$node/.."/>
                <xsl:choose>
                    <xsl:when test="$node/@maxOccurs='unbounded' or $parent/@maxOccurs='unbounded'">
                        <xsl:variable name="more">
                            <xsl:call-template name="GetArrayTypesOfComplexType">
                                <xsl:with-param name="complexNode" select="$node/xsd:complexType"/>
                            </xsl:call-template>
                        </xsl:variable>
                        <xsl:choose>
                            <xsl:when test="string($more)">
                                <xsl:value-of select="concat($node/@name,':',$more)"/>
                            </xsl:when>
                            <xsl:otherwise>
                                <xsl:value-of select="$node/@name"/>
                            </xsl:otherwise>
                        </xsl:choose>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:call-template name="GetArrayTypesOfComplexType">
                            <xsl:with-param name="complexNode" select="$node/xsd:complexType"/>
                        </xsl:call-template>
                    </xsl:otherwise>
                </xsl:choose>
            </xsl:when>
        </xsl:choose>
    </xsl:template>
    <!-- get array types used in a complex type node -->
    <!--xsl:template name="GetArrayTypesOfComplexType">
        <xsl:param name="complexNode"/>
        <xsl:choose>
            <xsl:when test="$complexNode/xsd:sequence">
                <xsl:variable name="more">
                    <xsl:call-template name="GetArrayTypesOfElements">
                        <xsl:with-param name="nodes" select="$complexNode/xsd:sequence/xsd:element"/>
                    </xsl:call-template>
                </xsl:variable>
                <xsl:choose>
                    <xsl:when test="$complexNode/xsd:sequence/@maxOccurs='unbounded'">
                            <xsl:value-of select="concat($complexNode/@name,':',$more)"/>
                    </xsl:when>
                    <xsl:otherwise>
                            <xsl:value-of select="$more"/>
                    </xsl:otherwise>
                </xsl:choose>
            </xsl:when>
            <xsl:when test="$complexNode/xsd:all">
                <xsl:variable name="more">
                    <xsl:call-template name="GetArrayTypesOfElements">
                        <xsl:with-param name="nodes" select="$complexNode/xsd:all/xsd:element"/>
                    </xsl:call-template>
                </xsl:variable>
                <xsl:choose>
                    <xsl:when test="$complexNode/xsd:all/@maxOccurs='unbounded'">
                            <xsl:value-of select="concat($complexNode/@name,':',$more)"/>
                    </xsl:when>
                    <xsl:otherwise>
                            <xsl:value-of select="$more"/>
                    </xsl:otherwise>
                </xsl:choose>
            </xsl:when>
        </xsl:choose>
    </xsl:template-->
    <!-- get array types used in a complex type node -->
    <xsl:template name="GetArrayTypesOfComplexType">
        <xsl:param name="complexNode"/>
        <xsl:choose>
            <xsl:when test="$complexNode/xsd:sequence">
                    <xsl:call-template name="GetArrayTypesOfElements">
                        <xsl:with-param name="nodes" select="$complexNode/xsd:sequence/xsd:element"/>
                    </xsl:call-template>
            </xsl:when>
            <xsl:when test="$complexNode/xsd:all">
                    <xsl:call-template name="GetArrayTypesOfElements">
                        <xsl:with-param name="nodes" select="$complexNode/xsd:all/xsd:element"/>
                    </xsl:call-template>
            </xsl:when>
        </xsl:choose>
    </xsl:template>
    <!-- get array type used in element nodes -->
    <xsl:template name="GetArrayTypesOfElements">
        <xsl:param name="nodes"/>
        <xsl:variable name="first">
            <xsl:call-template name="GetArrayTypes">
                <xsl:with-param name="node" select="$nodes[1]"/>
            </xsl:call-template>
        </xsl:variable>
        <xsl:variable name="rest">
            <xsl:if test="count($nodes) &gt; 1">
                <xsl:call-template name="GetArrayTypesOfElements">
                    <xsl:with-param name="nodes" select="$nodes[position()!=1]"/>
                </xsl:call-template>
            </xsl:if>
        </xsl:variable>
        <xsl:choose>
            <xsl:when test="string($first) and string($rest)">
                <!-- $rest is a tree fragment =: always true! -->
                <xsl:value-of select="concat($first, ':', $rest)"/>
            </xsl:when>
            <xsl:when test="string($first)">
                <xsl:value-of select="$first"/>
            </xsl:when>
            <xsl:otherwise>
                <xsl:value-of select="$rest"/>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>
    <!-- make a field id -->
    <xsl:template name="MakeId">
        <xsl:param name="parentId"/>
        <xsl:param name="fieldName"/>
        <xsl:param name="isattr"/>
        <xsl:variable name="prefix">
        <xsl:if test="$isattr">@</xsl:if>
        </xsl:variable>
        <xsl:choose>
            <xsl:when test="$parentId">
                <xsl:value-of select="concat($parentId,'.',$prefix,$fieldName)"/>
            </xsl:when>
            <xsl:otherwise>
                <xsl:value-of select="concat($prefix,$fieldName)"/>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>
    <xsl:template name="concat_link">
        <xsl:param name="begin" select="0"/>
        <xsl:param name="base" select="'unknown'"/>
        <xsl:if test="$queryParams">
            <xsl:choose>
                <xsl:when test="$begin"><xsl:value-of select="concat($base,'?', substring($queryParams,2))"/></xsl:when>
                <xsl:otherwise><xsl:value-of select="concat($base,'&amp;', substring($queryParams,2))"/></xsl:otherwise>
            </xsl:choose>
        </xsl:if>
    </xsl:template>
    <xsl:template name="build_link">
        <xsl:param name="type" select="'unkown'"/>
        <xsl:choose>
            <xsl:when test="$esdl_links=1">
                    <xsl:choose>
                        <xsl:when test="$type='reqxml'"><xsl:call-template name="concat_link"><xsl:with-param name="begin" select="0"/><xsl:with-param name="base" select="'SampleMessage?Type=request'"/></xsl:call-template></xsl:when>
                        <xsl:when test="$type='respxml'"><xsl:call-template name="concat_link"><xsl:with-param name="begin" select="0"/><xsl:with-param name="base" select="'SampleMessage?Type=response'"/></xsl:call-template></xsl:when>
                        <xsl:when test="$type='xsd'"><xsl:call-template name="concat_link"><xsl:with-param name="begin" select="1"/><xsl:with-param name="base" select="'GetMethodSchema'"/></xsl:call-template></xsl:when>
                        <xsl:when test="$type='wsdl'"><xsl:call-template name="concat_link"><xsl:with-param name="begin" select="1"/><xsl:with-param name="base" select="'GetWsdl'"/></xsl:call-template></xsl:when>
                        <xsl:when test="$type='action'"><xsl:call-template name="concat_link"><xsl:with-param name="begin" select="1"/><xsl:with-param name="base" select="'Invoke'"/></xsl:call-template></xsl:when>
                    </xsl:choose>
            </xsl:when>
            <xsl:otherwise>
                    <xsl:variable name="params">
                        <xsl:if test="$queryParams">
                            <xsl:value-of select="concat('&amp;', substring($queryParams,2))"/>
                        </xsl:if>
                    </xsl:variable>
                    <xsl:choose>
                        <xsl:when test="$type='reqxml'"><xsl:value-of select="concat('/WsEcl/example/request/query/', $queryPath, '/', $methodName,'?display')"/></xsl:when>
                        <xsl:when test="$type='respxml'"><xsl:value-of select="concat('/WsEcl/example/response/query/', $queryPath, '/', $methodName,'?display')"/></xsl:when>
                        <xsl:when test="$type='xsd'"><xsl:value-of select="concat('/WsEcl/definitions/query/', $queryPath, '/', $methodName,'/main/',$methodName,'.xsd')"/></xsl:when>
                        <xsl:when test="$type='wsdl'"><xsl:value-of select="concat('/WsEcl/definitions/query/', $queryPath, '/', $methodName,'/main/',$methodName,'.wsdl')"/></xsl:when>
                        <xsl:when test="$type='action'"><xsl:value-of select="concat('/WsEcl/submit/query/', $queryPath, '/', $methodName,$queryParams)"/></xsl:when>
                    </xsl:choose>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>
</xsl:stylesheet>



<!-- ********************************************************************************************************** -->
