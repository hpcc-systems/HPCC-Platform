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
   <xsl:output method="html"/>
   <xsl:param name="fullHtml" select="1"/>
   <xsl:param name="includeFormTag" select="1"/>
   <xsl:param name="method" select="'SprayVariable'"/>
   <xsl:param name="submethod" select="'csv'"/>
   
   <xsl:template match="/Environment">
     <xsl:choose>
       <xsl:when test="ErrorMessage">
         <h4>
           Spray:
           <xsl:value-of select="ErrorMessage"/>
         </h4>
       </xsl:when>
       <xsl:otherwise>
      <xsl:choose>
         <xsl:when test="$fullHtml">
            <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
               <head>
                  <title>Spray / Despray result</title>
                 <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
                 <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
                 <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
                 <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
               </head>
               <body class="yui-skin-sam" onload="nof5();onChangeMachine(true)">
                  <form method="POST" action="/FileSpray/{$method}">
                     <xsl:call-template name="generateForm"/>
                  </form>
               </body>
            </html>
         </xsl:when>
         <xsl:otherwise>
            <xsl:choose>
               <xsl:when test="$includeFormTag">
                  <form method="POST" action="/FileSpray/{$method}">
                     <xsl:call-template name="generateForm"/>
                  </form>
               </xsl:when>
               <xsl:otherwise>
                  <xsl:call-template name="generateForm"/>
               </xsl:otherwise>
            </xsl:choose>
         </xsl:otherwise>
      </xsl:choose>
       </xsl:otherwise>
     </xsl:choose>
   </xsl:template>
   <xsl:template name="generateForm">
         <script type="text/javascript" language="javascript">
           method = '<xsl:value-of select="$method"/>';
           smethod = '<xsl:value-of select="$submethod"/>';
           dir0 = '<xsl:value-of select="Software/DfuWorkunit/Source/@directory"/>';
           partmask0 = '<xsl:value-of select="Software/DfuWorkunit/Source/@partmask"/>';
           <![CDATA[
            var firsttime = true;
            var sourceOS = 0;
            var pathSep;
            var prefix;
            var dropzoneDirectory;
            function onChangeMachine(resetPath)
            {
              machineDropDown = document.forms[0].machine;
              if (machineDropDown.selectedIndex >=0)
              {
               selected=machineDropDown.options[machineDropDown.selectedIndex];               
               document.forms[0].sourceIP.value=selected.value;
               pos = selected.title.indexOf(';');
               directory = selected.title.substring(0, pos);
               linux = selected.title.substring(pos+1);
               
               sourceOS = linux == 'true' ? 1: 0;
         
               dropzoneDirectory = directory;
               if ((dir0 != '') && (partmask0 != ''))
               {
                  if (sourceOS != 0)
                    directory = dir0 + '/' + partmask0;
                  else
                    directory = dir0 + '\\' + partmask0;
               }
               
               if (resetPath)
               {
                var val = document.forms[0].sourcePath.value;
                if (!firsttime  || (val = 'undefined') || (document.forms[0].sourcePath.value.length <= 1) )
                {
                  document.forms[0].sourcePath.value = directory;
                  if (document.forms[0].sourcePathAndFile.value)
                    document.forms[0].sourcePath.value = document.forms[0].sourcePathAndFile.value;
                }
                firsttime = false;
               }
                   
               path = document.forms[0].sourcePath.value;               
               pathSep = linux == '' ? '\\' : '/';
               prefix = path.length > 0 && (path.substring(0,1) == pathSep) ? '' : pathSep;
               document.getElementById('sourceN').innerHTML = pathSep + pathSep + selected.value + prefix + path;
               handleSubmitBtn();

               if (document.forms[0].label.value.length > 0)
                onChangeLabel(document.forms[0].label)
              }
            }         
                     
            function onChangeLabel(o)
            {   
               document.forms[0].destLogicalName.value = o.value;
               document.getElementById('maskV').innerHTML = o.value+'._$P$_of_$N$';
               handleSubmitBtn();
            }
            /*function onChangeFormat()
            {   
               if (method == 'SprayVariable' && smethod == 'csv')
               {
                  if (document.forms[0].sourceFormat.value != "1")
                     document.forms[0].compress.disabled = true;
                  else
                     document.forms[0].compress.disabled = false;
               }
            }*/
            function handleSubmitBtn()
            {
               //disable = label == '';
               if (document.getElementById('label').value.length < 1)
                  disable = true;
               else
                  disable = false;
               if (!disable)
                  if (method == 'SprayFixed')
                  {
                     disable = document.getElementById('sourceRecordSize').value == 0;
                  }
                  else
                     if (method == 'SprayVariable')
                     {
                        disable = (document.getElementById('sourceMaxRecordSize') != null && document.getElementById('sourceMaxRecordSize').value == 0) || 
                                      (document.getElementById('sourceCsvSeparate') != null && document.getElementById('sourceCsvSeparate').value  == '') ||
                                      (document.getElementById('sourceCsvTerminate') != null && document.getElementById('sourceCsvTerminate').value == '') ||
                                      (document.getElementById('sourceCsvQuote') != null && document.getElementById('sourceCsvQuote').value == '') ||
                                      (document.getElementById('sourceRowTag') != null && document.getElementById('sourceRowTag').value == '');
                     }
               document.getElementById('submitBtn').readonly = disable;
            }

            function popup(ip, path)
            {
               mywindow = window.open ("/FileSpray/FileList?Netaddr="+ip+"&OS="+sourceOS+"&Path="+path, "mywindow", 
                                                    "location=0,status=1,scrollbars=1,resizable=1,width=500,height=600");
               if (mywindow.opener == null)
                  mywindow.opener = window;
               mywindow.focus();
            } 

            //note that the following function gets invoked from the file selection window
            //
            function setSelectedPath(path)
            {
              machineDropDown = document.forms[0].machine;
              if (machineDropDown.selectedIndex >=0)
              {
                selected=machineDropDown.options[machineDropDown.selectedIndex];   
                pos = selected.title.indexOf(';');
                linux = selected.title.substring(pos+1);

                if (linux != '')
                  document.forms[0].sourcePath.value = path;
                else
                {
                  var s='';
                  for (i=0; i<path.length; i++)
                  {
                    if (path.charAt(i) == '/')
                      s += '\\';
                    else
                      s += path.charAt(i);
                    }
                  document.forms[0].sourcePath.value = s;
                }
                document.forms[0].sourcePathAndFile.value = document.forms[0].sourcePath.value;
                document.getElementById('sourceN').innerHTML = pathSep + pathSep + selected.value + prefix + path;
                handleSubmitBtn();
              }
            }

            function beforeSubmit()
            {
              if ((document.getElementById("sourcePath").value == '')
                || (document.getElementById("sourcePath").value == dropzoneDirectory))
              {
                alert("Please specify a file in the Local Path field.");
                return false;
              }
              return true;
            }
      ]]></script>
      <table name="table1" id="table1">
        <xsl:variable name="srcip" select="Software/DfuWorkunit/Source/Part/@node"/>
        <xsl:variable name="srcpath" select="Software/DfuWorkunit/Source/@directory"/>
        <xsl:variable name="srcpartmask" select="Software/DfuWorkunit/Source/@partmask"/>
        <tr>
            <th colspan="2" style="text-align:left;">
               <h3>Spray <xsl:choose>
                     <xsl:when test="$method='SprayFixed'">Fixed</xsl:when>
                     <xsl:otherwise>
                         <xsl:choose><xsl:when test="$submethod='csv'"> CSV</xsl:when><xsl:otherwise> XML</xsl:otherwise></xsl:choose>
                    </xsl:otherwise>
                  </xsl:choose>
               </h3>
            </th>
         </tr>
         <tr>
            <td height="10"/>
         </tr>
         <tr>
            <td colspan="2">
               <b>Source</b>
            </td>
         </tr>
         <tr>
            <td>Machine/dropzone:</td>
            <td>
               <select name="machine" id="machine" onchange="onChangeMachine(true)" onblur="onChangeMachine(true)">
                  <xsl:for-each select="Software/DropZone">
                     <option>
                       <xsl:variable name="curip" select="@netAddress"/>
                        <xsl:attribute name="value"><xsl:value-of select="@netAddress"/></xsl:attribute>
                        <xsl:attribute name="title"><xsl:value-of select="@directory"/>;<xsl:value-of select="@linux"/></xsl:attribute>
                        <xsl:if test="@sourceNode = '1'">
                             <xsl:attribute name="selected"/>
                        </xsl:if>
                        <xsl:value-of select="@computer"/>/<xsl:value-of select="@name"/>
                     </option>
                  </xsl:for-each>
               </select>
            </td>
         </tr>
         <tr>
            <td>IP Address:</td>
            <td>
               <input type="text" id="sourceIP" name="sourceIP" value="{$srcip}" size="30"/>
            </td>
         </tr>
         <tr>
            <td>Local Path:</td>
            <td>
              <input type="hidden" name="sourcePathAndFile" id="sourcePathAndFile" value=""/>
              <xsl:choose>
                <xsl:when test="string-length($srcpath)">
                  <input type="text" name="sourcePath" id="sourcePath" value="{$srcpath}/{$srcpartmask}" size="70"
                    onchange="onChangeMachine(false)" onblur="onChangeMachine(false)"/>
                </xsl:when>
                <xsl:otherwise>
                  <input type="text" name="sourcePath" id="sourcePath" value="/" size="70"
                    onchange="onChangeMachine(false)" onblur="onChangeMachine(false)"/>
                </xsl:otherwise>
              </xsl:choose>
              <input type="button" name="Choose File" value="Choose File" onclick="popup(sourceIP.value, escape(sourcePath.value))"/>
            </td>
         </tr>
        <tr>
          <td>Network Path:</td>
          <td id="sourceN" name="sourceN"/>
        </tr>
         <xsl:choose>
            <xsl:when test="$method='SprayFixed'">
               <tr>
                  <td>Record Length:</td>
                  <td>
                     <input type="text" name="sourceRecordSize" id="sourceRecordSize" value="{Software/DfuWorkunit/Source/Attr/@recordSize}" size="4" 
                        onchange="handleSubmitBtn()" onblur="handleSubmitBtn()"/>
                  </td>
               </tr>
            </xsl:when>
            <xsl:otherwise>
               <tr>
                  <td>Format:</td>
                  <td>
                     <xsl:variable name="fmt" select="Software/DfuWorkunit/Source/Attr/@format"/>
                     <!-- generate a drop down list mostly in custom order that corresponds to the following values:
                           except for fixed format since that is not shown in SprayVariable form.
                        enum DFUfileformat
                        {
                            DFUff_fixed, //0
                            DFUff_csv,   //1
                            DFUff_utf8,  //2
                            DFUff_utf8n, //3
                            DFUff_utf16, //4
                            DFUff_utf16le, //5
                            DFUff_utf16be, //6
                            DFUff_utf32, //7
                            DFUff_utf32le, //8
                            DFUff_utf32be, //9
                            DFUff_variable, //10
                            DFUff_recfmvb, //11
                            DFUff_recfmv //12
                        };
                     <select id="sourceFormat" name="sourceFormat" onchange="onChangeFormat()" onblur="onChangeFormat()">
                     -->
                     <select id="sourceFormat" name="sourceFormat">
                           <xsl:if test="$submethod='csv'">
                           <option value="1">ASCII</option>
                           </xsl:if>
                           <xsl:choose>
                           <xsl:when test="$fmt='utf8'">
                           <option value="2" selected="1">Unicode (UTF-8)</option>
                           </xsl:when>
                           <xsl:otherwise>
                           <option value="2">Unicode (UTF-8)</option>
                           </xsl:otherwise>
                           </xsl:choose>

                           <xsl:choose>
                           <xsl:when test="$fmt='utf8n'">
                           <option value="3"  selected="1">Unicode (UTF-8 N)</option>
                           </xsl:when>
                           <xsl:otherwise>
                           <option value="3">Unicode (UTF-8 N)</option>
                           </xsl:otherwise>
                           </xsl:choose>


                           <xsl:choose>
                           <xsl:when test="$fmt='utf16'">
                           <option value="4"  selected="1">Unicode (UTF-16)</option>
                           </xsl:when>
                           <xsl:otherwise>
                           <option value="4">Unicode (UTF-16)</option>
                           </xsl:otherwise>
                           </xsl:choose>

                           <xsl:choose>
                           <xsl:when test="$fmt='utf16le'">
                           <option value="5"  selected="1">Unicode (UTF-16 LE)</option>
                           </xsl:when>
                           <xsl:otherwise>
                           <option value="5">Unicode (UTF-16 LE)</option>
                           </xsl:otherwise>
                           </xsl:choose>

                           <xsl:choose>
                           <xsl:when test="$fmt='utf16be'">
                           <option value="6" selected="1">Unicode (UTF-16 BE)</option>
                           </xsl:when>
                           <xsl:otherwise>
                           <option value="6">Unicode (UTF-16 BE)</option>
                           </xsl:otherwise>
                           </xsl:choose>

                           <xsl:choose>
                           <xsl:when test="$fmt='utf32'">
                           <option value="7" selected="1">Unicode (UTF-32)</option>
                           </xsl:when>
                           <xsl:otherwise>
                           <option value="7">Unicode (UTF-32)</option>
                           </xsl:otherwise>
                           </xsl:choose>

                           <xsl:choose>
                           <xsl:when test="$fmt='utf32le'">
                           <option value="8" selected="1">Unicode (UTF-32 LE)</option>
                           </xsl:when>
                           <xsl:otherwise>
                           <option value="8">Unicode (UTF-32 LE)</option>
                           </xsl:otherwise>
                           </xsl:choose>

                           <xsl:choose>
                           <xsl:when test="$fmt='utf32be'">
                           <option value="9" selected="1">Unicode (UTF-32 BE)</option>
                           </xsl:when>
                           <xsl:otherwise>
                           <option value="9">Unicode (UTF-32 BE)</option>
                           </xsl:otherwise>
                           </xsl:choose>

                     </select>               
                  </td>
               </tr>
               <xsl:variable name="rsz">
                  <xsl:choose>
                    <xsl:when test="Software/DfuWorkunit/Source/Attr/@maxRecordSize"><xsl:value-of select="Software/DfuWorkunit/Source/Attr/@maxRecordSize"/></xsl:when>
                    <xsl:otherwise>8192</xsl:otherwise>
                  </xsl:choose>
               </xsl:variable>
               <xsl:variable name="sep">
                  <xsl:choose>
                    <xsl:when test="Software/DfuWorkunit/Source/Attr/@csvSeparate"><xsl:value-of select="Software/DfuWorkunit/Source/Attr/@csvSeparate"/></xsl:when>
                    <xsl:otherwise>\,</xsl:otherwise>
                  </xsl:choose>
               </xsl:variable>
               <xsl:variable name="term">
                  <xsl:choose>
                    <xsl:when test="Software/DfuWorkunit/Source/Attr/@csvTerminate"><xsl:value-of select="Software/DfuWorkunit/Source/Attr/@csvTerminate"/></xsl:when>
                    <xsl:otherwise>\n,\r\n</xsl:otherwise>
                  </xsl:choose>
               </xsl:variable>
               <xsl:variable name="quote">
                  <xsl:choose>
                    <xsl:when test="Software/DfuWorkunit/Source/Attr/@csvQuote"><xsl:value-of select="Software/DfuWorkunit/Source/Attr/@csvQuote"/></xsl:when>
                    <xsl:otherwise>'</xsl:otherwise>
                  </xsl:choose>
               </xsl:variable>
               <tr>
                  <td>Max Record Length:</td>
                  <td>
                     <input type="text" id="sourceMaxRecordSize" name="sourceMaxRecordSize" value="{$rsz}" size="4" onchange="handleSubmitBtn()" onblur="handleSubmitBtn()"/>
                  </td>
               </tr>
              <xsl:choose><xsl:when test="$submethod='csv'">
               <tr>
                  <td>Separator:</td>
                  <td>
                     <input type="text" id="sourceCsvSeparate" name="sourceCsvSeparate" size="4" value="{$sep}" onchange="handleSubmitBtn()" onblur="handleSubmitBtn()"/>
                  </td>
               </tr>
               <tr>
                  <td>Line Terminator:</td>
                  <td>
                     <input type="text" id="sourceCsvTerminate" name="sourceCsvTerminate" size="4" value="{$term}" onchange="handleSubmitBtn()" onblur="handleSubmitBtn()"/>
                  </td>
               </tr>
               <tr>
                  <td>Quote:</td>
                  <td>
                     <input type="text" id="sourceCsvQuote" name="sourceCsvQuote" size="4" value="{$quote}"  maxlength="1" onchange="handleSubmitBtn()" onblur="handleSubmitBtn()"/>
                  </td>
               </tr>
               </xsl:when>
               <xsl:otherwise>
               <tr>
                  <td>Row Tag:</td>
                  <td>
                    <input type="text" id="sourceRowTag" name="sourceRowTag" size="30" value="{Software/DfuWorkunit/Source/Attr/@rowTag}" onchange="handleSubmitBtn()" onblur="handleSubmitBtn()"/>
                  </td>
               </tr>
               </xsl:otherwise>
             </xsl:choose>
            </xsl:otherwise>
         </xsl:choose>
         <tr>
            <td height="10"/>
         </tr>
         <tr>
            <td colspan="2">
               <b>Destination</b>
            </td>
         </tr>
         <tr>
            <td>Group:</td>
            <td>
               <select name="destGroup" id="destGroup">
                  <xsl:variable name="grp" select="Software/DfuWorkunit/Destination/@group"/>
                  <xsl:for-each select="Software/ThorCluster">
                     <option>
                        <xsl:attribute name="value"><xsl:value-of select="@name"/></xsl:attribute>
                        <xsl:variable name="curgrp" select="@name"/>
                        <xsl:if test="$grp = $curgrp">
                           <xsl:attribute name="selected"/>
                        </xsl:if>
                        <xsl:value-of select="@name"/>
                     </option>
                  </xsl:for-each>
                  <xsl:for-each select="Software/EclAgentProcess/Instance">
                     <option>
                        <!--xsl:attribute name="value"><xsl:value-of select="@netAddress"/></xsl:attribute>
                        <xsl:value-of select="../@name"/><xsl:text> </xsl:text><xsl:value-of select="@netAddress"/-->
                        <xsl:attribute name="value"><xsl:value-of select="@gname"/><xsl:text> </xsl:text><xsl:value-of select="@netAddress"/></xsl:attribute>
                        <xsl:value-of select="@gname"/><xsl:text> </xsl:text><xsl:value-of select="@netAddress"/>
                     </option>
                  </xsl:for-each>
               </select>
            </td>
         </tr>
         <xsl:variable name="origname" select="Software/DfuWorkunit/Destination/OrigName"/>
         <tr>
            <td>Label:</td>
            <td>
               <input type="text" id="label" name="label" size="70" value="{$origname}" onchange="onChangeLabel(this)" onblur="onChangeLabel(this)"/>
               <input type="hidden" name="destLogicalName" id="destLogicalName" size="30" value="{$origname}" readonly="true"/>
            </td>
         </tr>
         <tr>
            <td>Mask:</td>
            <td id="maskV" name="maskV"><xsl:value-of select="$origname"/>._$P$_of_$N$</td>
         </tr>
         <tr>
            <td height="10"/>
         </tr>
         <tr>
            <td>Prefix:</td>
            <td>
              <input type="text" name="prefix" size="30" value=""/>
            </td>
         </tr>
         <tr>
            <td colspan="2">
               <b>Options</b>
            </td>
         </tr>
         <tr>
            <td>Overwrite:</td>
            <td>
               <input type="checkbox" name="overwrite" value="1"/>
            </td>
         </tr>
         <tr>
            <td>Replicate:</td>
            <td>
               <input type="checkbox" name="replicate" value="1" checked="1"/>
            </td>
         </tr>
         <tr>
            <td>No Split:</td>
            <td>
               <input type="checkbox" name="nosplit" value="1"/>
            </td>
         </tr>
         <xsl:if test="$method='SprayFixed' or $submethod='csv'">
            <tr>
               <td>Compress:</td>
               <td>
                  <input type="checkbox" id="compress" name="compress" value="1"/>
               </td>
            </tr>
         </xsl:if>
         <xsl:if test="$fullHtml='1'">
            <tr>
               <td/>
               <td>
                  <input type="submit" id="submitBtn" name="submitBtn" value="Submit" onclick="return beforeSubmit();" readonly="true"/>
               </td>
            </tr>
         </xsl:if>
      </table>
   </xsl:template>
</xsl:stylesheet>
