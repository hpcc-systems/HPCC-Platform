<?xml version="1.0" encoding="UTF-8"?>
<!--

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

    This program is free software: you can redistribute it and/or modify
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
  <xsl:variable name="netAddress0" select="/DropZoneFilesResponse/NetAddress"/>
  <xsl:variable name="path0" select="/DropZoneFilesResponse/Path"/>
  <xsl:variable name="os0" select="/DropZoneFilesResponse/OS"/>

  <xsl:template match="DropZoneFilesResponse">
    <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
      <head>
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
        <link type="text/css" rel="styleSheet" href="/esp/files/css/sortabletable.css"/>
        <link type="text/css" rel="StyleSheet" href="/esp/files/css/list.css"/>
        <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
        <script language="javascript" src="/esp/files/scripts/multiselect.js">
          <xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
        </script>
        <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
        <title>EclWatch</title>
        <script language="JavaScript1.2">
          var initialPath = '<xsl:value-of select="/DropZoneFilesResponse/Path"/>';
          <xsl:text disable-output-escaping="yes"><![CDATA[
            var intervalId = 0;
            var hideLoading = 1;
            var url0 = '';
            function onChangeMachine(resetPath)
            {
              machineDropDown = document.forms['DropZoneForm'].machine;
              if (machineDropDown.selectedIndex >=0)
              {
                selected=machineDropDown.options[machineDropDown.selectedIndex];               
                document.forms['DropZoneForm'].NetAddress.value=selected.value;
                pos = selected.title.indexOf(';');
                directory = selected.title.substring(0, pos);
                linux = selected.title.substring(pos+1);
                sourceOS = linux == 'true' ? 1: 0;

                if (resetPath)
                {
                  var val = document.forms['DropZoneForm'].Directory.value;
                  if ((val = 'undefined') || (document.forms[0].Directory.value.length <= 1) )
                  {
                    document.forms['DropZoneForm'].Directory.value = directory;
                  }
                }

                document.location.href = "/FileSpray/DropZoneFiles?NetAddress=" + selected.value + "&OS=" + sourceOS+ "&Path=" + directory;
              }
            }         

            function doBlink() 
            {
              var obj = document.getElementById('loadingMsg');
              if (obj)
              {
                obj.style.visibility = obj.style.visibility == "" ? "hidden" : "";

                if (hideLoading > 0 && intervalId && obj.style.visibility == "hidden")
                {              
                  clearInterval (intervalId);
                  intervalId = 0;
                }   
              }
            }

            function loadPageTimeOut() 
            {
              hideLoading = 1;

              var obj = document.getElementById('loadingMsg');
              if (obj)
                obj.style.display = "none";

              var obj1 = document.getElementById('loadingTimeOut');
              if (obj1)
                obj1.style.visibility = "";
            }

            function uploadNow()
            {
              var file = document.forms['DropZoneForm'].FilesToUpload.value;
              if (file == '')              
              {
                alert("You must choose a file first.");
                return false;
              }

              var sectionDiv = document.getElementById("DropzoneFileData");
              if (sectionDiv)
              {
                var inner = "<span id=\"loadingMsg\">";
                inner += "<h3>Uploading, please wait ...</h3>";
                inner += "<h3>If you navigate away from this page, your upload may be aborted.</h3>";
                inner += "</span>";
                inner += "<span id=\"loadingTimeOut\" style=\"visibility:hidden\">";
                inner += "<h3>Browser timed out due to a long time delay.</h3>";
                inner += "</span>";
                sectionDiv.innerHTML = inner;
              }

              hideLoading = 0;
              var obj = document.getElementById('loadingMsg');
              if (obj)
              {
                obj.style.display = "inline";
                obj.style.visibility = "visible";
              }

              var obj = document.getElementById('UploadBtn');
              if (obj)
              {
                obj.disabled = true;
              }

              var obj1 = document.getElementById('loadingTimeOut');
              if (obj1)
              {
                obj1.style.display = "none";
                obj1.style.visibility = "hidden";
              }
              intervalId = setInterval("doBlink()",1000);

              document.forms['DropZoneForm'].action= url0;
              document.forms['DropZoneForm'].submit();
              return false;
            }   

            function checkSelected(o)
            {
              updateChecked(o.checked);
              var checkAllBox = document.getElementById("All");
              if (checkAllBox)
                checkAllBox.checked = checkedCount == totalItems;
              document.getElementById("DeleteFileBtn").disabled = checkedCount == 0;
              return;
            }

            function selectAll0(checked)
            {
              selectAllItems(checked, 1, multiSelectTable.rows.length-1);
              checkedCount = checked ? totalItems : 0;
              document.getElementById("DeleteFileBtn").disabled = checked ? false : true;;
            }

            function getSelected(o)
            {
              if (o.tagName=='INPUT' && o.type == 'checkbox' && o.value != 'on')
              {
                var val = o.value;
                var pt = val.indexOf("@");
                if (pt > 0)
                  val = val.substring(0, pt);
                return o.checked ? '\n'+val : '';
              }

              var s='';
              var ch=o.childNodes;
              if (ch)
                for (var i in ch)
                  s=s+getSelected(ch[i]);
              return s;
            }

            function getSelectedDropZone()
            {
              machineDropDown = document.forms['DropZoneForm'].machine;
              if (machineDropDown.selectedIndex >=0)
              {
                selected=machineDropDown.options[machineDropDown.selectedIndex];               
                document.forms['DropZoneForm'].NetAddress.value=selected.value;
                pos = selected.title.indexOf(';');
                directory = selected.title.substring(0, pos);
                linux = selected.title.substring(pos+1);
                sourceOS = linux == 'true' ? 1: 0;

                document.forms['DropZoneForm'].Directory.value = directory;

                document.forms['DropZoneFileForm'].NetAddress.value=selected.value;
                document.forms['DropZoneFileForm'].Path.value=directory;
                document.forms['DropZoneFileForm'].OS.value=sourceOS;

                url0 = "/FileSpray/UploadFile?upload_&NetAddress=" + selected.value + "&OS=" + sourceOS + "&Path=" + directory;
              }
            }

            function onLoad()
            {
              initSelection('resultsTable');

              getSelectedDropZone();
              document.forms['DropZoneForm'].Directory.value = initialPath;
              document.forms['DropZoneFileForm'].Path.value = initialPath;
              url0 = "/FileSpray/UploadFile?upload_&NetAddress=" + selected.value + "&OS=" + sourceOS + "&Path=" + initialPath;
            }   
          ]]></xsl:text>
        </script>
      </head>
      <body class="yui-skin-sam" onload="nof5();onLoad();">
        <h4>Dropzones and Files</h4>
        <xsl:choose>
          <xsl:when test="not(DropZones/DropZone[1])">
            <p>
              <xsl:value-of select="Result"/>
            </p>
          </xsl:when>
          <xsl:otherwise>
            <table name="table0" id="table0">
              <tr>
                <td>
                  <form id="DropZoneForm" name="DropZoneForm" enctype="multipart/form-data" method="post">
                    <table name="table1" id="table1">
                      <tr>
                        <td>Machine/dropzone:</td>
                        <td>
                          <select name="machine" id="machine" onchange="onChangeMachine(true)" onblur="onChangeMachine(true)">
                            <xsl:for-each select="DropZones/DropZone">
                              <option>
                                <xsl:variable name="curip" select="NetAddress"/>
                                <xsl:attribute name="value">
                                  <xsl:value-of select="NetAddress"/>
                                </xsl:attribute>
                                <xsl:attribute name="title">
                                  <xsl:value-of select="Path"/>;<xsl:value-of select="Linux"/>
                                </xsl:attribute>
                                <xsl:if test="$netAddress0=NetAddress and ($path0=Path or $path0=concat(Path,'/') or $path0=concat(Path,'\\'))">
                                  <xsl:attribute name="selected">selected</xsl:attribute>
                                </xsl:if>
                                <xsl:value-of select="Computer"/>/<xsl:value-of select="Name"/>
                              </option>
                            </xsl:for-each>
                          </select>
                        </td>
                      </tr>
                      <tr>
                        <td>IP Address:</td>
                        <td>
                          <input type="text" id="NetAddress" name="NetAddress" value="" disabled="disabled" size="22"/>
                        </td>
                      </tr>
                      <tr>
                        <td>Local Path:</td>
                        <td colspan="3">
                          <input type="text" name="Directory" id="Directory" value="/" disabled="disabled" size="87"
                            onchange="onChangeMachine(false)" onblur="onChangeMachine(false)"/>
                        </td>
                      </tr>
                      <tr>
                        <td>Select a file to upload:</td>
                        <td>
                          <input type="file" id="FilesToUpload" name="FilesToUpload" size="65"/>
                        </td>
                        <td>
                          <input type="button" class="sbutton" id="UploadBtn" value="Upload Now" onclick="return uploadNow();"/>
                        </td>
                      </tr>
                    </table>
                  </form>
                </td>
              </tr>
              <tr>
                <td>
                  <div id="DropzoneFileData">
                  </div>
                </td>
              </tr>
              <tr>
                <td>
                  <form id="DropZoneFileForm" action="/FileSpray/DeleteDropZoneFiles" method="post">
                    <input type="hidden" name="NetAddress" value="{$netAddress0}"/>
                    <input type="hidden" name="Path" value="{$path0}"/>
                    <input type="hidden" name="OS" value="{$os0}"/>
                    <xsl:choose>
                      <xsl:when test="not(Files/PhysicalFileStruct[1])">
                        <br/><br/>No file found in this dropzone.<br/><br/>
                      </xsl:when>
                      <xsl:otherwise>
                        <xsl:apply-templates select="Files"/>
                      </xsl:otherwise>
                    </xsl:choose>
                  </form>
                </td>
              </tr>
            </table>
          </xsl:otherwise>
        </xsl:choose>
      </body>
    </html>
  </xsl:template>

  <xsl:template match="Files">
    <table id="table2" name="table2">
      <tr>
        <td>
          <table class="sort-table" id="resultsTable" width="800">
            <colgroup>
              <col width="5"/>
              <col/>
              <col style="text-align:right" class="number"/>
              <col style="text-align:right"/>
            </colgroup>
            <thead>
              <tr class="grey">
                <th>
                  <xsl:if test="PhysicalFileStruct[2]">
                    <input id ="All" type="checkbox" title="Select or deselect all files" onclick="selectAll0(this.checked)"/>
                  </xsl:if>
                </th>
                <th style="cursor:pointer" >Name</th>
                <th style="cursor:pointer" >Size</th>
                <th style="cursor:pointer" width="180">Date</th>
              </tr>
            </thead>
            <tbody>
              <xsl:variable name="directories" select="PhysicalFileStruct[isDir=1]"/>
              <xsl:apply-templates select="$directories">
                <xsl:sort select="name"/>
              </xsl:apply-templates>
              <xsl:apply-templates select="PhysicalFileStruct[isDir=0]">
                <xsl:sort select="name"/>
                <xsl:with-param name="dirs" select="count($directories)"/>
              </xsl:apply-templates>
            </tbody>
          </table>
        </td>
      </tr>
      <tr>
        <td>
          <input type="submit" class="sbutton" id="DeleteFileBtn" value="Delete" disabled="true" onclick="return confirm('Are you sure you want to delete the following file(s) ?\n\n'+getSelected(document.forms['DropZoneFileForm']).substring(1,1000))"/>
        </td>
      </tr>
    </table>
  </xsl:template>

  <xsl:template match="PhysicalFileStruct">
    <xsl:param name="dirs" select="1"/>
    <xsl:variable name="size" select="number(filesize)"/>
    <xsl:variable name="dir" select="number(isDir)"/>
    <xsl:choose>
      <xsl:when test="$dir">
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
            <input type="checkbox" name="Names_i{position()}" value="{name}" onclick="return checkSelected(this)"/>
          </td>
          <td align="left">
            <a title="Open folder..." href="javascript:go('/FileSpray/DropZoneFiles?Subfolder={name}&amp;NetAddress={../../NetAddress}&amp;Path={../../Path}&amp;OS={../../OS}')">
              <img src="/esp/files_/img/folder.gif" width="19" height="16" border="0" alt="Open folder..." style="vertical-align:bottom"/>
            </a>
            <xsl:value-of select="name"/>
          </td>
          <td>
          </td>
          <td align="center">
            <xsl:value-of select="modifiedtime"/>
          </td>
        </tr>
      </xsl:when>
      <xsl:otherwise>
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
            <input type="checkbox" name="Names_i{position()}" value="{name}" onclick="return checkSelected(this)"/>
          </td>
          <td align="left">

            <a title="Download file..." href="javascript:go('/FileSpray/DownloadFile?Name={name}&amp;NetAddress={../../NetAddress}&amp;Path={../../Path}&amp;OS={../../OS}')">
              <xsl:value-of select="name"/>
            </a>
          </td>
          <td align="right">
            <xsl:value-of select="filesize"/>
          </td>
          <td align="center">
            <xsl:value-of select="modifiedtime"/>
          </td>
        </tr>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>
</xsl:stylesheet>
