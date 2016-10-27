<?xml version="1.0" encoding="UTF-8"?>
<!--

## Copyright (c) 2012 HPCC Systems®.  All rights reserved.
-->
<!DOCTYPE xsl:stylesheet [
<!ENTITY nbsp "&#160;">
<!ENTITY apos "&#39;">
]>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:xsd="http://www.w3.org/2001/XMLSchema" version="1.0" xml:space="default" exclude-result-prefixes="xsd">
    <xsl:strip-space elements="*"/>
    <xsl:output method="html" indent="yes" omit-xml-declaration="yes" version="4.01" doctype-public="-//W3C//DTD HTML 4.01 Transitional//EN" doctype-system="http://www.w3.org/TR/html4/loose.dtd"/>

    <xsl:param name="queryParams" select="''"/>
    <xsl:variable name="QueryInput" select="/FormInfo/Request"/>
    <xsl:variable name="queryPath" select="/FormInfo/QuerySet"/>
    <xsl:variable name="methodName" select="/FormInfo/QueryName"/>
    <xsl:variable name="methodHelp" select="/FormInfo/Help"/>
    <xsl:variable name="methodDesc" select="/FormInfo/Info"/>
    <xsl:variable name="serviceVersion" select="/FormInfo/Version"/>

    <!-- ===============================================================================
  global settings
  ================================================================================ -->
   <!-- config -->
   <xsl:template match="FormInfo">
      <html>
          <head>
              <title>WsECL Service form</title>
              <link rel="shortcut icon" href="/esp/files/img/affinity_favicon_1.ico"/>
              <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css"/>
              <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css"/>
              <link rel="stylesheet" type="text/css" href="/esp/files/gen_form.css"/>
              <script type="text/javascript" src="/esp/files/hashtable.js"/>
              <script type="text/javascript" src="/esp/files/gen_form_wsecl.js"/>
              <script type="text/javascript">

      <xsl:text disable-output-escaping="yes">
<![CDATA[
function setESPFormAction()
{
    var form = document.forms['esp_form'];
    if (!form)  return false;

    var actval = document.getElementById('submit_type');
    var actionpath = "/jserror";
    if (actval && actval.value)
    {
        if (actval.value=="esp_soap")
            actionpath = "]]></xsl:text><xsl:value-of disable-output-escaping="yes" select="concat('/WsEcl/forms/soap/query/', $queryPath, '/', $methodName)"/><xsl:text disable-output-escaping="yes"><![CDATA[";
        else if (actval.value=="run_xslt")
            actionpath = "]]></xsl:text><xsl:value-of disable-output-escaping="yes" select="concat('/WsEcl/xslt/query/', $queryPath, '/', $methodName)"/><xsl:text disable-output-escaping="yes"><![CDATA[";
        else if (actval.value=="xml")
            actionpath = "]]></xsl:text><xsl:value-of disable-output-escaping="yes" select="concat('/WsEcl/submit/query/', $queryPath, '/', $methodName, '?view=xml&amp;display')"/><xsl:text disable-output-escaping="yes"><![CDATA[";
        else
            actionpath= "]]></xsl:text><xsl:value-of disable-output-escaping="yes" select="concat('/WsEcl/xslt/query/', $queryPath, '/', $methodName, '?view=')"/><xsl:text disable-output-escaping="yes"><![CDATA["+actval.value;
    }

    form.action = actionpath;
    saveInputValues(form);
    return true;
}

function switchInputForm()
{
    var inputform = document.getElementById('SelectForm');
    if (inputform.value!="DynamicForm")
       return false;
    document.location.href = "]]></xsl:text><xsl:value-of disable-output-escaping="yes" select="concat('/WsEcl/forms/ecl/query/', $queryPath, '/', $methodName)"/><xsl:text disable-output-escaping="yes"><![CDATA[";
    return true;
}
]]></xsl:text>
<xsl:call-template name="GetHtmlHeadAddon"/>
</script>
</head>
      <body class="yui-skin-sam" onload="onPageLoad()">
                <input type="hidden" id="esp_html_"/>
                <input type="hidden" id="esp_vals_"/>
                <p align="center"/>
                <table cellSpacing="0" cellPadding="1" width="100%" bgColor="#4775FF" border="0">
                    <tr align="left" class="service">
                        <td height="23">
                            <font color="#efefef">
                                <b>
                                    <xsl:value-of select="/FormInfo/QuerySet"/>
                                    <xsl:if test="number($serviceVersion)&gt;0">
                                        <xsl:value-of select="concat(' [Version ', $serviceVersion, ']')"/>
                                    </xsl:if>
                                </b>
                            </font>
                        </td>
                    </tr>
                    <tr class="method">
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
                               <option value="InputBox" selected="selected">Input Box</option>
                               <option value="DynamicForm">Dynamic Form</option>
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
            <form id="esp_form" method="POST" enctype="application/x-www-form-urlencoded" action="/jserror">
               <xsl:attribute name="onSubmit">return setESPFormAction()</xsl:attribute>
               <table cellSpacing="0" width="100%" border="0">
                 <tr>
                  <td>Query Input:</td>
                </tr>
                <tr><td bgcolor="#030303" height="1"/></tr>
                <tr><td height="4"/></tr>
                <tr>
                  <td>
                   <textarea name="_boxFormInput" rows="40" cols="120"><xsl:value-of select="$QueryInput"/>
                   </textarea>
                  </td>
                </tr>
                <tr><td height="4"/></tr>
                <tr><td bgcolor="#030303" height="1"/></tr>
                <tr><td height="6"/></tr>
                <tr class="commands">
                  <td align="left">
                    <select id="submit_type" name="submit_type_">
                        <xsl:for-each select="/FormInfo/CustomViews/Result">
                            <option value="{.}"><xsl:value-of select="."/></option>
                        </xsl:for-each>
                        <option value="run_xslt">Output Tables</option>
                        <option value="xml">Output XML</option>
                        <option value="esp_soap">SOAP Test</option>
                    </select>&nbsp;
                    <input type="submit" value="Submit" name="S1"/>
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
    <xsl:template name="GetHtmlHeadAddon">
        <xsl:variable name="items" select="//xsd:annotation/xsd:appinfo/form/@html_head"/>
        <xsl:variable name="s" select="string($items)"/>
        <xsl:value-of select="$s" disable-output-escaping="yes"/>
    </xsl:template>
    <xsl:template name="build_link">
        <xsl:param name="type" select="'unkown'"/>
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
    </xsl:template>
</xsl:stylesheet>
<!-- ********************************************************************************************************** -->
