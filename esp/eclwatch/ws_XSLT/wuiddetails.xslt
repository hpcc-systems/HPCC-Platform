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
  <xsl:variable name="wuid" select="WUInfoResponse/Workunit/Wuid"/>
  <xsl:variable name="wuid0" select="WUGraphTimingResponse/Workunit/Wuid"/>
  <xsl:variable name="state" select="WUInfoResponse/Workunit/StateID"/>
  <xsl:variable name="compile" select="WUInfoResponse/CanCompile"/>
  <xsl:variable name="autoRefresh" select="WUInfoResponse/AutoRefresh"/>
  <xsl:variable name="isArchived" select="WUInfoResponse/Workunit/Archived"/>
  <xsl:variable name="havesubgraphtimings" select="WUInfoResponse/Workunit/HaveSubGraphTimings"/>
  <xsl:variable name="thorSlaveIP" select="WUInfoResponse/ThorSlaveIP"/>
  <xsl:include href="/esp/xslt/lib.xslt"/>
  <xsl:include href="/esp/xslt/wuidcommon.xslt"/>


  <xsl:template match="WUInfoResponse">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
            <head>
                <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
                <title><xsl:value-of select="$wuid"/></title>
                <script type="text/javascript" src="files_/scripts/tooltip.js">
                    <xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
                </script>
                <link type="text/css" rel="StyleSheet" href="/esp/files_/css/sortabletable.css"/>
        <link type="text/css" rel="stylesheet" href="/esp/files_/default.css"/>
        <link type="text/css" rel="stylesheet" href="/esp/files_/css/espdefault.css"/>
        <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
        <script type="text/javascript">
                var autoRefreshVal=<xsl:value-of select="$autoRefresh"/>;
                var isarchived=<xsl:value-of select="$isArchived"/>;
                var wid='<xsl:value-of select="$wuid"/>';
                var dorefresh = true;
                <xsl:text disable-output-escaping="yes"><![CDATA[
         
                     // This function gets called when the window has completely loaded.
                     // It starts the reload timer with a default time value.
         
                   function onLoad()
                   {
              // Find the sections that are available and move them across.
              parent.autoRefreshVal = autoRefreshVal;
              updateSection('wudetails'); // Update main form details.
              updateSection('wufooter'); // Update main form details.
              updateSections();
              parent.UpdateAutoRefresh();
              //parent.saveState();

                   }               

            function updateSection(Section)
            {
              // Check if there's actual content.
              var sectionContentCheck = document.getElementById(Section + 'Content');
              if (sectionContentCheck)
              {
                // Move the Section content into the parent section.                
                var sectionContent = document.getElementById(Section);
                var sectionDiv = document.getElementById(Section);
                if (sectionDiv)
                {
                  var parentSectionDiv = parent.document.getElementById(Section);
                  if (parentSectionDiv)
                  {
                    parentSectionDiv.innerHTML = sectionDiv.innerHTML; 
                  }
                }
              }
              else
              {
                var parentSectionDiv = parent.document.getElementById(Section);
                if (parentSectionDiv)
                {
                  parentSectionDiv.innerHTML = '<span class="loading">&nbsp;&nbsp;Workunit has no ' + Section + '.</span>';
                  if (Section == 'Exceptions' || Section == 'Warnings' || Section == 'Info')
                  {
                    parentSectionDiv.style.display = 'none';
                    parentSectionDiv.style.visibility = 'hidden';
                    var sectionobj = parent.document.getElementById('Hdr' + Section);
                    if (sectionobj)
                    {
                      sectionobj.style.display = 'none';
                      sectionobj.style.visibility = 'hidden';
                    }
                  }
                }
              }

            }
      
            function updateSections()
            {
              for(i=0; i < parent.window.sections.length;i++)
              {
                if (location.toString().indexOf('Include' + parent.window.sections[i] + '=1') > -1)
                {
                  updateSection(parent.window.sections[i]);
                  if (parent.window.sections[i] == 'Exceptions')
                  {
                    updateSection('Warnings');
                    updateSection('Info');
                  }
                }
              }
            }

            function toggleElement(ElementId)
            {
                obj = document.getElementById(ElementId);
                hdrobj = document.getElementById('HdrExp' + ElementId);
                if (obj) 
                {
                  if (obj.style.visibility == 'visible')
                  {
                    obj.style.display = 'none';
                    obj.style.visibility = 'hidden';
                      if (hdrobj)
                    {
                       hdrobj.innerText = 'Click header to expand this section';
                    }
                  }
                  else
                  {
                    obj.style.display = 'inline';
                    obj.style.visibility = 'visible';
                        if (hdrobj)
                    {
                       hdrobj.innerText = 'Click header to contract this section';
                    }
                  }
                }
            }

               ]]></xsl:text>
          </script>
            </head>
      <body class="yui-skin-sam" onload="nof5();onLoad()">
                <table id="wudetailheader" class="workunit0">
          <xsl:text disable-output-escaping="yes"><![CDATA[
                    <colgroup>
                        <col width="30%"/>
                        <col width="10%"/>
                        <col width="60%"/>
                    </colgroup>
          ]]></xsl:text>
          
                    <tr>
            <td>
              <strong>Workunit Details</strong>
            </td>
                        <xsl:if test="number(Workunit/Archived) &lt; 1">
                            <td onmouseover="EnterContent('ToolTip','','Turn on/off Auto Refresh'); Activate();" onmouseout="deActivate();">
                                <input type="image" id="refresh" value="refresh" onclick="TurnRefresh()"/>
                            </td>
                            <td style="visibility:hidden">
                            <div id="ToolTip"/>
                            </td>
                        </xsl:if>
                    </tr>
                </table>

        <xsl:apply-templates select="Workunit" />

      </body>
        </html>
    </xsl:template>

</xsl:stylesheet>
