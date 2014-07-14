<?xml version="1.0" encoding="UTF-8"?>
<!--

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
-->

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:output method="html"/>
    <xsl:variable name="subfiles" select="/AddtoSuperfileResponse/Subfiles"/>
  <xsl:variable name="backtopage" select="/AddtoSuperfileResponse/BackToPage"/>
    <xsl:template match="/AddtoSuperfileResponse">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
            <head>
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
        <link type="text/css" rel="StyleSheet" href="files_/css/list.css"/>
        <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
                <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
                <title>EclWatch</title>
                <link type="text/css" rel="StyleSheet" href="files_/css/sortabletable.css"/>
                <script language="JavaScript1.2" src="files_/scripts/multiselect.js">
                    <xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
                </script>
                <script language="JavaScript1.2">
          var backToPageLink='<xsl:value-of select="$backtopage"/>';
          var subfiles = '<xsl:value-of select="$subfiles"/>';
                    <xsl:text disable-output-escaping="yes"><![CDATA[
          function go_back()
                  {
                      document.location.href = backToPageLink;
                  }

                    function getAllChecked(o)
                    {
                        var loc = -1;
                        if (o.tagName =='INPUT' && o.type == 'checkbox')
                        {
                            loc = o.name.indexOf("name");
                        }

                        if (o.tagName=='INPUT' && o.type == 'checkbox' && (o.value != 'on' || loc != 0))
                            return o.checked ? '\n'+o.name : '';

                        var s='';
                        var ch=o.childNodes;
                        if (ch)
                            for (var i in ch)
                                s=s+getAllChecked(ch[i]);
                        return s;
                    }

                    function validateInput()
                    {
                        var resources = 1;
                        var sel = getAllChecked(document.getElementById('resultsTable'));
                        if (sel == '')
                            resources = 0;
                        else if (document.getElementById('Superfile').value == '')
                            resources = 0;
                        
                        //alert(resources);

                        if (resources > 0)
                            document.getElementById('OK').disabled = false;
                        else
                            document.getElementById('OK').disabled = true;
                    }

                    function onRowCheck(checked)
                    {
                        validateInput();
                    }  

                    function toggleElement()
                    {
                     obj = document.getElementById('NameArea');
                     obj1 = document.getElementById('NameAreaHdr');
                     if (obj) 
                     {
                        if (obj.style.visibility == 'visible')
                        {
                          obj.style.display = 'none';
                          obj.style.visibility = 'hidden';
                          if (obj1)
                                obj1.innerHTML = 'Click here to view subfiles.';
                        }
                        else
                        {
                          obj.style.display = 'inline';
                          obj.style.visibility = 'visible';
                          if (obj1)
                                obj1.innerHTML = 'Click here to hide this section.';
                        }
                     }
                    }

                    function onLoad()
                    {
                        initSelection('resultsTable');

                        selectAll(true);

                        obj = document.getElementById('NameArea');
                        if (obj) 
                        {
                            obj1 = document.getElementById('NameAreaHdr');
                            obj.style.display = 'inline';
                            obj.style.visibility = 'visible';
                            if (obj1)
                                obj1.innerHTML = 'Click here to hide this section.';
                        }
                    }

                    function onCancel()
                    {
                        document.location.href='/WsDfu/DFUQuery';
                        return true;
                    }

                    function preSubmit()
                    {
                        if (document.getElementById("AddtoFile").checked == true)
                        {
                            document.getElementById("ExistingFile").value = 1;
                        }

                        /*var ref='/WsDfu/AddtoSuperfile?Superfile=' + escape(filename);
                        if (subfiles.length > 0)
                        {
                            ref += '&Subfiles=' + escape(subfiles);
                        }
                        document.forms["form1"].action=ref;*/
                        return true;
                    }
                    ]]></xsl:text>
                </script>
            </head>
      <body class="yui-skin-sam" onload="nof5();onLoad()">
            <h4>Add to Superfile</h4>
                <form id="form1" method="POST" action="/WsDfu/AddtoSuperfile">
                    <input type="hidden" id="ExistingFile" name="ExistingFile" value="0"/>
                    <table>
                        <tr>
                            <td>Superfile Name:</td>
                            <td>
                                <input type="text" id="Superfile" name="Superfile" size="25" onkeyup="validateInput()" oninput="validateInput()" onmouseout="validateInput()"/>
                            </td>
                        </tr>
                        <tr>
                            <td height="10"></td>
                        </tr>
                        <tr>
                            <td colspan="2">
                                <div>
                                    <div>
                                        <A href="javascript:void(0)" onclick="toggleElement();" class="wusectionexpand">
                                            <div id="NameAreaHdr">
                                                Click here to hide this section.
                                            </div>
                                        </A>
                                    </div>
                                    <div id="NameArea">
                                        <table class="sort-table" id="resultsTable">
                                            <colgroup>
                                              <col width="5"/>
                                              <col/>
                                            </colgroup>
                                            <thead>
                                              <tr class="grey">
                                                 <th id="selectAll1">
                                                    <xsl:if test="SubfileNames/SubfileName[2]">
                                                        <input type="checkbox" id="SelectAllBtn" title="Select or deselect all" onclick="selectAll(this.checked)"/>
                                                    </xsl:if>
                                                 </th>
                                                 <th align="left">Subfile</th>
                                              </tr>
                                            </thead>
                                            <tbody>
                                                <xsl:apply-templates select="SubfileNames/SubfileName">
                                                    <xsl:sort select="SubfileName"/>
                                                </xsl:apply-templates>
                                            </tbody>
                                        </table>
                                    </div>
                                </div>
                            </td>
                        </tr>
                        <tr>
                            <td height="10"></td>
                        </tr>
                        <tr>
                            <td colspan="2">
                                <input type="radio" name="Option" id="CreateNewFile">
                                      <xsl:attribute name="checked">true</xsl:attribute>
                                </input>Create a new superfile<br/>
                                <input type="radio" name="Option" id="AddtoFile"/>Add to an existing superfile<br/>
                            </td>
                        </tr>
                        <tr>
                            <td colspan="2">
                                <input type="submit" id="OK" value="OK" class="sbutton" onclick="return preSubmit(this)"/>
                            </td>
                        </tr>
                    </table>
                </form>
        <xsl:choose>
          <xsl:when test="$backtopage!=''">
            <input type="button" class="sbutton" value="Go Back" onclick="go_back()"/>
          </xsl:when>
          <xsl:otherwise>
            <input id="backBtn" type="button" class="sbutton" value="Go Back" onclick="history.go(-1)"> </input>
          </xsl:otherwise>
        </xsl:choose>
      </body>
        </html>
    </xsl:template>

  <xsl:template match="SubfileName">
            <tr onmouseenter="this.bgColor = '#F0F0FF'">
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
        <td>
        <input type="checkbox" name="names_i{position()}" value="{.}" onclick="return clicked(this, event)"/>
        </td>
        <td align="left">
                <xsl:value-of select="."/>
        </td>
        </tr>
  </xsl:template>

</xsl:stylesheet>
