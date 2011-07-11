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
   <xsl:template match="/DFUXRefListResponse">
      <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
         <head>
            <title>XRef</title>
           <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
           <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
           <link rel="stylesheet" type="text/css" href="/esp/files/css/list.css" />
           <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
         </head>
        <body class="yui-skin-sam" onload="nof5();">
            <h3>
              XRef Clusters
            </h3>
            <xsl:choose>
               <xsl:when test="DFUXRefListResult/XRefNodes/XRefNode[1]">
                  <xsl:apply-templates/>
               </xsl:when>
               <xsl:otherwise>No clusters defined.</xsl:otherwise>
            </xsl:choose>
         </body>
      </html>
   </xsl:template>
   <xsl:template match="XRefNodes">
      <table class="list">
         <colgroup>
            <col width="400"/>
            <col width="400"/>
            <col width="400"/>
            <col width="400"/>
            <col width="200"/>
         </colgroup>
         <tr class="grey">
            <th colspan="1">Name</th>
          <th>Last Run</th>
            <th>Last Message</th>
            <th>Available Reports</th>
            <th>Action</th></tr>
         <xsl:apply-templates select="XRefNode">
            <xsl:sort select="Name"/>
         </xsl:apply-templates>
         <tr>
            <th colspan="4"/>
            <th>
               <input type="button" class="sbutton" name="DFUXRefCancel" value="Cancel All" onclick="var x =  window.confirm('This will attempt to stop any running jobs and clear the job queue. Do you wish to continue?'); if(x) document.location='/WsDFUXRef/DFUXRefBuildCancel' "/>
            </th>
         </tr>
      </table>
      <table>
         <colgroup>
            <col width="150"/>
            <col width="1600"/>
         </colgroup>
            <tr>
               <td colspan="2"><b>GLOSSARY</b></td>
            </tr>
            <tr>
               <th colspan="2"><hr/></th>
            </tr>
            <tr>
               <th align="left" valign="top">Found File</th>
               <td>A found file has all of its parts on disk that are not referenced in the Dali server.  All the file parts are accounted for so they can be added back to the Dali server. 
They can also be deleted from the cluster, if required.</td>
            </tr>            
            <tr>
               <th align="left" valign="top">Orphan File</th>
               <td>An orphan file has partial file parts on disk.  However, a full set of parts is not available to construct a 
complete logical file. These partial file parts, therefore, do not have a reference in the Dali server.</td>
            </tr>
            <tr>
               <th align="left" valign="top">Lost File</th>
               <td>A logical file that is missing at least one file part on both the primary and replicated locations in storage.  The logical file is still referenced in the Dali server. Deleting the file removes the 
reference from the Dali server and any remaining parts on disk.</td>                              
            </tr>
      </table>
   </xsl:template>
   
   
   <xsl:template match="XRefNode">
      <tr>
         <th>
            <xsl:value-of select="Name"/>
         </th>
         <td align="center">
            <xsl:value-of select="Modified"/>
         </td>
         <td align="center">
            <xsl:value-of select="Status"/>
         </td>
         <td align="center">
         <xsl:choose>
            <xsl:when test="string-length(Modified) and starts-with(Status,'Generated')">
               <xsl:choose>
                  <xsl:when test="string-length(Modified) and not(starts-with(Name,'SuperFiles'))">
                     <a href="/WsDFUXRef/DFUXRefFoundFiles?Cluster={Name}">Found Files </a>
                     <br/>
                     <a href="/WsDFUXRef/DFUXRefOrphanFiles?Cluster={Name}">Orphan Files</a>
                     <br/>
                     <a href="/WsDFUXRef/DFUXRefLostFiles?Cluster={Name}">Lost Files</a>
                     <br/>
                     <a href="/WsDFUXRef/DFUXRefDirectories?Cluster={Name}">Directories</a>
                     <br/>
                  </xsl:when>
               </xsl:choose>
               <a href="/WsDFUXRef/DFUXRefMessages?Cluster={Name}">Errors/Warnings</a>
            </xsl:when>
            <xsl:otherwise>Not generated yet.</xsl:otherwise>
         </xsl:choose>
         </td>
         <td align="center">
            <input type="button" class="sbutton" name="DFUXRefBuild" value="Generate" onclick="var x =  window.confirm('Running this Process may take a long time and will put a very heavy strain on the servers. Do you wish to continue?'); if(x) document.location='/WsDFUXRef/DFUXRefBuild?Cluster={Name}' "/>
         </td>
      </tr>
   </xsl:template>
</xsl:stylesheet>
