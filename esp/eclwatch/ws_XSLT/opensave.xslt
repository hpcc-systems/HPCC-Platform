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

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:fo="http://www.w3.org/1999/XSL/Format">
    <xsl:output method="html"/>
  <xsl:variable name="viewable" select="/OpenSaveResponse/Viewable"/>
    <xsl:template match="/">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
            <head>
                <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
                <title>Open/save File</title>
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
        <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
                <style type="text/css">
                    .selectedRow { background-color: #0af;}
                </style>
                <script language="JavaScript1.2">
          var viewable0=<xsl:value-of select="$viewable"/>;
          <xsl:text disable-output-escaping="yes"><![CDATA[
                    var rbid = 0;
          function onRBChanged(id)
                    {
                        rbid = id;
                        document.getElementById('DownloadOption').disabled = false;
                        if (rbid != 1)
                        {
                            document.getElementById('DownloadOption').disabled = true;
                        }
          }

                    function onOK()
                    {
                        var opener = window.opener;
                        if (opener)
                        {                           
                            if (!opener.closed)
                            {
                if (rbid < 1)
                                  opener.setOpenSave( 0 );
                else
                  opener.setOpenSave( document.getElementById("DownloadOption").selectedIndex + 1 );
                            }
                            window.close();
                        }
                        else
                        {
                            alert('Lost reference to parent window.  Please traverse the path again!');
                            unselect();
                        }
                    }                               

                    function onCancel()
                    {
                        var opener = window.opener;
                        if (opener)
                        {                           
                            window.close();
                        }
                    }       

          function onLoad()
          {
            if (viewable0 < 1)
            {
              rbid = 1;
              document.form1.Option[0].disabled = true;
              document.form1.Option[1].checked="checked";
              document.getElementById('DownloadOption').disabled = false;
            }
          }
                        
                ]]></xsl:text>
                </script>
            </head>
      <body class="yui-skin-sam" onload="onLoad()">
        <h4>Do you want to open or save this file?</h4>
                <xsl:apply-templates/>
            </body>
        </html>
    </xsl:template>
    
    
    <xsl:template match="OpenSaveResponse">
    <form method="POST" name="form1">
      <table>
        <tr>
          <td>
            <img src="/esp/files_/img/page.gif" width="40" height="32"/>
          </td>
          <td>
            <table>
              <xsl:if test="string-length(Name)">
                <tr>
                  <td>Name: </td>
                  <td>
                    <xsl:value-of select="Name"/>
                  </td>
                </tr>
              </xsl:if>
              <xsl:if test="string-length(Path)">
                <tr>
                  <td>Path: </td>
                  <td>
                    <xsl:value-of select="Path"/>
                  </td>
                </tr>
              </xsl:if>
              <xsl:if test="string-length(Location)">
                <tr>
                  <td>Location: </td>
                  <td>
                    <xsl:value-of select="Location"/>
                  </td>
                </tr>
              </xsl:if>
              <xsl:if test="string-length(Type)">
                <tr>
                  <td>Type: </td>
                  <td>
                    <xsl:value-of select="Type"/>
                  </td>
                </tr>
              </xsl:if>
              <xsl:if test="string-length(DatwTime)">
                <tr>
                  <td>Datw/Time: </td>
                  <td>
                    <xsl:value-of select="DatwTime"/>
                  </td>
                </tr>
              </xsl:if>
            </table>
          </td>
        </tr>
      </table>
      <table style="margin-top:20px;margin-left:50px;">
        <tr>
          <td>
            <input type="radio" name="Option" id="Option" value="ViewRB" checked="checked" onclick="onRBChanged(0)"/>
          </td>
          <td>Open this file.</td>
        </tr>
        <tr>
          <td>
            <input type="radio" name="Option" id="Option" value="DownloadRB" onclick="onRBChanged(1)"/>
          </td>
          <td>Download this file:</td>
        </tr>
        <tr>
          <td></td>
          <td>
            <select id="DownloadOption" name="DownloadOption" disabled="true">
              <option value="original">Original File</option>
              <option value="gz">WINZIP Format</option>
              <option value="gz">GZIP Format</option>
            </select>
          </td>
        </tr>
      </table>
      <table style="margin-top:20px;margin-left:50px;">
        <tr>
          <td>
            <input type="submit" id="OK" value="OK" class="sbutton" onclick="return onOK()"/>
          </td>
          <td>
                <input type="button" id="Cancel" value="Cancel" onclick="return onCancel()"/>
          </td>
        </tr>
      </table>
    </form>
    </xsl:template>
    
</xsl:stylesheet>
