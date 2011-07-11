<?xml version="1.0" encoding="UTF-8"?>
<!--

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
-->

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:param name="showColumns" select="/DFUGetDataColumnsResponse/ShowColumns"/>
    <xsl:param name="oldFile" select="/DFUGetDataColumnsResponse/LogicalName"/>
    <xsl:param name="chooseFile" select="/DFUGetDataColumnsResponse/ChooseFile"/>
    <xsl:output method="html"/>
    <xsl:template match="DFUGetDataColumnsResponse">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
            <head>
        <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/menu/assets/skins/sam/menu.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/button/assets/skins/sam/button.css" />
        <link type="text/css" rel="StyleSheet" href="files_/css/list.css"/>
        <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
        <title>EclWatch</title>
                <script language="JavaScript1.2">
                    var showcolumns='<xsl:value-of select="$showColumns"/>';
                    var oldfile='<xsl:value-of select="$oldFile"/>';
                    var choosefile='<xsl:value-of select="$chooseFile"/>';
                    var filterBy = null;
                    var max_name_len = 3;
                    var max_value_len = 4;

                    <xsl:if test="ChooseFile = '1'">
                    var checkup = window.setInterval("checkChange(event);", 100); 

                    function checkChange(e) 
                    { 
                      CheckFileName(e); 
                    } 
                    </xsl:if>

                    <xsl:text disable-output-escaping="yes"><![CDATA[
                    function onOpenClick()
                    {

                        var name = document.getElementById("OpenLogicalName").value;
                        if(!name)
                        {
                            alert('No file name is defined.');
                            return;
                        }

                        document.location.href="/WsDfu/DFUSearchData?RoxieSelections=0&OpenLogicalName=" + name;
                        return;
                    }       

                    function trimAll(sString) 
                    {
                        while (sString.substring(0,1) == ' ')
                        {
                            sString = sString.substring(1, sString.length);
                        }
                        while (sString.substring(sString.length-1, sString.length) == ' ')
                        {
                            sString = sString.substring(0,sString.length-1);
                        }
                        return sString;
                    }

                    function toSpecialString(v, l)
                    {
                        var v_ret = "";
                        var v_len = v.length;
                        var v_str = v_len.toString();
                        var v_len1 = v_str.length;
                        while (v_len1 < l)
                        {
                            v_ret = v_ret.concat("0");
                            v_len1++;
                        }

                        v_ret = v_ret.concat(v_str);
                        return v_ret;
                    }

                    function checkTextField(o)
                    {
                        if (o.tagName=='INPUT' && o.type=='text' && o.value !='')
                        {
                            var oname = trimAll(o.name);
                            var ovalue = trimAll(o.value);
                            var len_name = toSpecialString(oname, max_name_len);
                            var len_value = toSpecialString(ovalue, max_value_len);

                            if (filterBy)
                                filterBy = filterBy + len_name + len_value + oname + ovalue; 
                            else
                                filterBy = len_name + len_value + oname + ovalue; 
                        }

                        var ch=o.children;
                        if (ch)
                            for (var i in ch)
                                checkTextField(ch[i]);
                         return;
                    }
                    function onFindClick()
                    {
                        var startIndex = document.getElementById("StartIndex").value;
                        var endIndex = document.getElementById("EndIndex").value;
                        var start = startIndex - 1;
                        var count = endIndex - start;
                        var name = document.getElementById("LogicalName").value;
                        if(!name)
                        {
                            alert('No file name is defined.');
                            return;
                        }

                        checkTextField(document.forms['dfucolumnsform']);

                        var url = "/WsDfu/DFUSearchData?RoxieSelections=0&ChooseFile=" + choosefile + "&OpenLogicalName=" + name + "&LogicalName=" + name + "&Start=" + start + "&Count=" + count
                             + "&StartForGoback=" + start + "&CountForGoback=" + count;
                        if (showcolumns.length > 0)
                            url = url + "&ShowColumns=" + showcolumns;
                    
                        if (filterBy)
                        {
                            var newFilterBy = max_name_len.toString() + max_value_len.toString() + filterBy;
                            url = url + "&FilterBy=" + escape(newFilterBy);
                        }
                        //url = url + "&FilterBy=" + max_name_len.toString() + max_value_len.toString() + filterBy;
                    
                        document.location.href=url;
                        return;
                    }       

                    function CheckFileName(e)
                    {
            if (!e) 
              { e = window.event; }

            var o = document.getElementById('OpenLogicalName');
                        if (o.value !='')
                        {
                            if (oldfile != '')
                            {
                                var ovalue0 = trimAll(oldfile);
                                var ovalue = trimAll(o.value);
                                var v_len0 = ovalue0.length;
                                var v_len = ovalue.length;
                                if ((v_len0 != v_len) || (ovalue0 != ovalue))
                                {
                                    document.getElementById("GetColumns").disabled = false;
                                }
                                else
                                {
                                    document.getElementById("GetColumns").disabled = true;
                                }
                            }
                            else
                            {
                                var ovalue = trimAll(o.value);
                                var v_len = ovalue.length;
                                if (v_len > 0)
                                {
                                    document.getElementById("GetColumns").disabled = false;
                                }
                                else
                                {
                                    document.getElementById("GetColumns").disabled = true;
                                }
                            }
                        }
                        else
                        {
                            document.getElementById("GetColumns").disabled = true;
                        }
            if (e && e.keyCode == 13)
            {
              onOpenClick();
            }

                    }

          function toggleElement(ElementId, ClassName)
          {
              obj = document.getElementById(ElementId);
              explink = document.getElementById('explink' + ElementId);
              if (obj) 
              {
                if (obj.style.visibility == 'visible')
                {
                  obj.style.display = 'none';
                  obj.style.visibility = 'hidden';
                  if (explink)
                  {
                    explink.className = ClassName + 'expand';
                  }
                }
                else
                {
                  obj.style.display = 'inline';
                  obj.style.visibility = 'visible';
                  if (explink)
                  {
                    explink.className = ClassName + 'contract';
                  }
                  else
                  {
                    alert('could not find: explink' + ElementId);
                  }
                }
              }
          }

          function checkFilterField(InputField, e) {
              if (InputField.value.length > 0) {
                 InputField.style.background = 'lightyellow';
              } 
              else {
                 InputField.style.background = '';
              }
              checkEnter(InputField, e);
          }

          function checkEnter(InputField, e) {
            if (!e) 
              { e = window.event; }
            if (e && e.keyCode == 13)
            {
              onFindClick(InputField);
            }
          }


                    ]]></xsl:text>
                </script>
            </head>
      <body class="yui-skin-sam" onload="nof5();">
        <xsl:choose>
          <xsl:when test="string-length(LogicalName)">
            <h3>
              View Data File: <xsl:value-of select="LogicalName"/>
            </h3>
          </xsl:when>
          <xsl:otherwise>
            <h3>View Data File</h3>
          </xsl:otherwise>
        </xsl:choose>
        <xsl:if test="ChooseFile != '1'">
           <xsl:if test="string-length(LogicalName)">
             <input type="hidden" name="LogicalName" id="LogicalName" value="{LogicalName}"/>
           </xsl:if>
          <form id="dfucolumnsform">
            <div id="DataFields">
              <xsl:if test="DFUDataKeyedColumns1/DFUDataColumn[1]">
                <table>
                  <tr>
                    <td valign="top" width="160">
                      <A href="javascript:void(0)" onclick="toggleElement('Keyed', 'viewkey');" id="explinkKeyed" class="viewkeycontract">
                        Keyed Columns:
                      </A>
                    </td>
                  </tr>
                </table>
                <div id="Keyed" style="visibility:visible;">
                  <xsl:for-each select="*[contains(name(), 'DFUDataKeyedColumns')]">
                    <xsl:apply-templates select="DFUDataColumn" />
                  </xsl:for-each>
                </div>
              </xsl:if>
              <xsl:if test="DFUDataNonKeyedColumns1/DFUDataColumn[1]">
                <xsl:variable name="nonkeyedstate">
                  <xsl:choose>
                    <xsl:when test="count(//DFUDataColumn[string-length(ColumnValue)&gt;0 and contains(name(..), 'DFUDataNonKeyedColumns')])">
                      <xsl:text>viewkeycontract</xsl:text>
                    </xsl:when>
                    <xsl:otherwise>
                      <xsl:text>viewkeyexpand</xsl:text>
                    </xsl:otherwise>
                  </xsl:choose>
                </xsl:variable>
                <table>
                  <tr>
                    <td valign="top" width="160">
                      <A href="javascript:void(0)" onclick="toggleElement('NonKeyed', 'viewkey');" id="explinkNonKeyed" class="{$nonkeyedstate}">
                        Non-Keyed Columns:
                      </A>
                    </td>
                  </tr>
                </table>

                <xsl:variable name="nonkeyedstyle">
                  <xsl:choose>
                    <xsl:when test="count(//DFUDataColumn[string-length(ColumnValue)&gt;0 and contains(name(..), 'DFUDataNonKeyedColumns')])">
                      <xsl:text></xsl:text>
                    </xsl:when>
                    <xsl:otherwise>
                      <xsl:text>display:none; visibility:hidden;</xsl:text>
                    </xsl:otherwise>
                  </xsl:choose>
                </xsl:variable>

                <div id="NonKeyed" style="{$nonkeyedstyle}">
                  <xsl:for-each select="*[contains(name(), 'DFUDataNonKeyedColumns')]">
                    <xsl:apply-templates select="DFUDataColumn" />
                  </xsl:for-each>
                </div>
              </xsl:if>
            </div>
          </form>
      </xsl:if>
    
      <xsl:choose>
          <xsl:when test="number(RowCount)>0">
            <xsl:if test="DFUDataKeyedColumns1/DFUDataColumn[1] or DFUDataNonKeyedColumns1/DFUDataColumn[1]">
              <table>
                <tr>
                  <td>If the first key column is not specified, it may take a long time to retrieve the data.</td>
                </tr>
              </table>
              <br/>
              <table>
                <tr>
                  <td>
                    Records from:
                  </td>
                  <td>
                    <input type="text" value="{StartIndex}" name="StartIndex" size="5"/>
                  </td>
                  <td>
                    to:
                  </td>
                  <td>
                    <input type="text" value="{EndIndex}" name="EndIndex" size="5"/>
                  </td>
                  <td>
                    <input type="button" value="Retrieve data" class="sbutton" onclick="onFindClick()"/>
                  </td>
                </tr>
              </table>
              <br/>
              <br/>
            </xsl:if>
          </xsl:when>
          <xsl:when test="string-length(LogicalName)">
            <table>
              <tr>
                <td>There is no data in this file.</td>
              </tr>
            </table>
            <br/>
            <br/>
          </xsl:when>
          <xsl:otherwise>
          </xsl:otherwise>
        </xsl:choose>
        <xsl:if test="ChooseFile = '1'">
          <table>
            <tr>
              <td>File Name:</td>
              <td>
                <input id="OpenLogicalName" name="OpenLogicalName" size="80" type="text" value="{LogicalName}" onkeyup="CheckFileName(event)"/>
              </td>
              <td>
                <input type="button" id="GetColumns" value="Get columns" class="sbutton"  disabled="true" onclick="onOpenClick()"/>
              </td>
            </tr>
          </table>
        </xsl:if>
      </body>
        </html>
    </xsl:template>
  
  <xsl:template match="DFUDataColumn">
    <xsl:if test="position() = 1">
      <xsl:text disable-output-escaping="yes"><![CDATA[        
   <table border="0" cellspacing="0" cellpadding="0">
    <tr>
    ]]></xsl:text>
    </xsl:if>
    <td xmlns="http://www.w3.org/1999/xhtml" nowrap="nowrap">
      <span class="searchprompt">
        <xsl:value-of select="ColumnLabel"/>
      </span>
      <br/>
      <xsl:variable name="ColumnStyle">
        <xsl:choose>
          <xsl:when test="string-length(ColumnValue) &gt; 0">
            <xsl:text>background-color:lightyellow</xsl:text>
          </xsl:when>
        </xsl:choose>
      </xsl:variable>
      <xsl:choose>
        <xsl:when test="ColumnSize &gt; 50">
          <input name="{ColumnLabel}" style="{$ColumnStyle}" size="50" maxlength="{MaxSize}" type="text" value="{ColumnValue}" onKeyUp="checkFilterField(this, event)" />
        </xsl:when>
        <xsl:when test="ColumnSize &gt; 0 and ColumnSize &lt; 50">
          <input name="{ColumnLabel}" style="{$ColumnStyle}" size="{ColumnSize}" maxlength="{MaxSize}" type="text" value="{ColumnValue}" onKeyUp="checkFilterField(this, event)" />
        </xsl:when>
        <xsl:otherwise>
          <input name="{ColumnLabel}" style="{$ColumnStyle}" size="10" type="text" value="{ColumnValue}" onKeyUp="checkFilterField(this, event)" />
        </xsl:otherwise>
      </xsl:choose>
    </td>
    <xsl:if test="position() = count(../*)">
      <xsl:text disable-output-escaping="yes">      
      <![CDATA[        
    </tr>
   </table>
    ]]></xsl:text>
    </xsl:if>
  </xsl:template>

</xsl:stylesheet>
