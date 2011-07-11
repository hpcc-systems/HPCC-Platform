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

  <xsl:template match="/QueryFileListResponse">
    <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
      <head>
      <xsl:text disable-output-escaping="yes"><![CDATA[
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/menu/assets/skins/sam/menu.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/button/assets/skins/sam/button.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
        <link type="text/css" rel="styleSheet" href="/esp/files/css/sortabletable.css"/>
        <script type="text/javascript" src="/esp/files/scripts/espdefault.js"></script>
        <script type="text/javascript" src="/esp/files/yui/build/yahoo-dom-event/yahoo-dom-event.js"></script>
        <script type="text/javascript" src="/esp/files/yui/build/container/container_core-min.js"></script>
        <script type="text/javascript" src="/esp/files/yui/build/menu/menu-min.js"></script>
        <script language="JavaScript1.2" src="/esp/files/scripts/multiselect.js"></script>
      ]]></xsl:text>

      <script language="JavaScript1.2" id="menuhandlers">
        var gobackURL = '<xsl:value-of select="/QueryFileListResponse/GoBackURL"/>';;
        var clusterName0 = '<xsl:value-of select="/QueryFileListResponse/Cluster"/>';;
        var fileType = '<xsl:value-of select="/QueryFileListResponse/FileType"/>';;
        var sortBy = '<xsl:value-of select="/QueryFileListResponse/Sortby"/>';;
        var descending0 = '<xsl:value-of select="/QueryFileListResponse/Descending"/>';;
        var parametersForSorting = '<xsl:value-of select="/QueryFileListResponse/ParametersForSorting"/>';;

        <xsl:text disable-output-escaping="yes"><![CDATA[
          var oMenu;
          function QueryFilePopup(id, cluster, query, type, browsedata, PosId)
          {
            function details()
            {
              if (type)
                document.location.href='/WsRoxieQuery/QueryFileDetails?LogicalName='+ id + '&Cluster=' + cluster
                  + '&Query=' + query + '&Type=' + type;
              else
                document.location.href='/WsRoxieQuery/QueryFileDetails?LogicalName='+ id + '&Cluster=' + cluster
                  + '&Query=' + query;
            }
            function showQueris()
            {
              document.location.href='/WsRoxieQuery/RoxieQueryList?LogicalName='+ id + '&Cluster=' + cluster;
            }
            function browseDFUData()
            {
              document.location.href='/WsDfu/DFUGetDataColumns?OpenLogicalName='+id;
            }
            function browseNewDFUData()
            {
              document.location.href='/WsDfu/DFUSearchData?RoxieSelections=0&Cluster=' + cluster + '&OpenLogicalName='+id;
            }

            var xypos = YAHOO.util.Dom.getXY('mn' + PosId);
            if (oMenu)
            {
              oMenu.destroy();
            }
            oMenu = new YAHOO.widget.Menu("logicalfilecontextmenu", {position: "dynamic", xy: xypos} );
            oMenu.clearContent();

            if (fileType == 'Super Files')
            {
              if (browsedata != 0)
              {
                if (query != '')
                {
                  oMenu.addItems([
                    { text: "Details", onclick: { fn: details } },
                    { text: "View Data File", onclick: { fn: browseNewDFUData } },
                    { text: "ShowQueries", onclick: { fn: showQueris } }
                    ]);
                }
                else
                {
                  oMenu.addItems([
                    { text: "View Data File", onclick: { fn: browseNewDFUData } },
                    { text: "ShowQueries", onclick: { fn: showQueris } }
                    ]);
                }
              }
              else
              {
                if (query != '')
                {
                  oMenu.addItems([
                    { text: "Details", onclick: { fn: details } },
                    { text: "ShowQueries", onclick: { fn: showQueris } }
                    ]);
                }
                else
                {
                  oMenu.addItems([
                    { text: "ShowQueries", onclick: { fn: showQueris } }
                    ]);
                }
              }
            }
            else
            {
              oMenu.addItems([
                { text: "Details", onclick: { fn: details } },
                { text: "View Data File", onclick: { fn: browseNewDFUData } },
                { text: "ShowQueries", onclick: { fn: showQueris } }
                ]);
            }

            //showPopup(menu,(window.event ? window.event.screenX : 0),  (window.event ? window.event.screenY : 0));
            oMenu.render("roxiequerylistmenu");
            oMenu.show();
            return false;
          }
          var totalChecked = 0;

          function checkSelected(o)
          {
            if (o.tagName=='INPUT' && o.id!='All'  && o.id!='All1' && o.checked)
            {
              totalChecked++;
              return;
            }

            var ch=o.children;
            if (ch)
              for (var i in ch)
                checkSelected(ch[i]);
            return;
          }

          function onRowCheck(checked)
          {
            totalChecked = 0;
            checkSelected(document.forms['RoxieFileForm']);

            document.getElementById("queriesBtn").disabled = totalChecked == 0;
          }     

          function headerClicked(headername, descending)
          {
            if (parametersForSorting)
            {
              var para = parametersForSorting.replace(/&amp;/g, "&");
              document.location.href='/WsRoxieQuery/QueryFileList?'+para+'&Sortby='+headername+'&Descending='+descending;
            }
            else
              document.location.href='/WsRoxieQuery/QueryFileList?Sortby='+headername+'&Descending='+descending;
          }                
             
          function getNewPage()
          {
            var startFrom = document.getElementById("PageStartFrom").value;
            var pageEndAt = document.getElementById("PageEndAt").value;
            if (!startFrom || startFrom < 1)
              startFrom = 1;
            if (!pageEndAt || pageEndAt < startFrom)
              pageEndAt = 100;
            var size = pageEndAt - startFrom + 1;

            if (basicQuery.length > 0)
              document.location.href = '/WsRoxieQuery/QueryFileList?PageSize='+size+'&'+basicQuery+'&PageStartFrom='+startFrom;
            else
              document.location.href = '/WsRoxieQuery/QueryFileList?PageStartFrom='+startFrom+'&PageSize='+size;

            return false;
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

          function startBlink() 
          {
            if (document.all)
            {
              hideLoading = 0;
              intervalId = setInterval("doBlink()",500);
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

            if (gobackURL != '')
              document.getElementById('backBtn').disabled = false;
          }

          function go_back()
          {
            if (gobackURL != '')
              go(gobackURL);

            return;
          }

          function onLoad()
          {
            if (gobackURL != '')
              document.getElementById('backBtn').disabled = true;

            if (clusterName0)
            {
              startBlink();

              var dataFrame = document.getElementById('DataFrame');
              if (dataFrame)
              {
                if (parametersForSorting != '')
                {
                  var para = parametersForSorting.replace(/&amp;/g, "&");
                  url = "/WsRoxieQuery/QueryFileListDone?"+ para + "&ReadList=1";
                }
                else
                  url = "/WsRoxieQuery/QueryFileListDone?ReadList=1&Cluster=" + clusterName0 + "&FileType=" + fileType;

                if (sortBy != '')
                  url += ("&Sortby=" + sortBy);
                if (descending0 != '')
                  url += ("&Descending=" + descending0);

                if (gobackURL != '')
                  url += ("&GoBackURL=" + gobackURL);

                dataFrame.src = url;
              }
            } 
          }      

          function go(url)
          {
            document.location.href=url;
          }                           
          ]]></xsl:text>
        </script>
      </head>
      <body class="yui-skin-sam" onload="nof5();onLoad()">
        <form name="RoxieFileForm" action="/WsRoxieQuery/QueryFilesAction" method="post">
          <div id="RoxieFileData">
            <span id="loadingMsg">
              <h3>Loading, please wait ...</h3>
            </span>
            <span id="loadingTimeOut" style="visibility:hidden">
              <h3>Browser timed out due to a long time delay.</h3>
            </span>
          </div>
          <tr>
            <td>
              <input type="submit" class="sbutton" id="queriesBtn" name="Type" value="ListQueries" disabled="true"/>
            </td>
          </tr>
        </form>
        <xsl:if test="string-length(/QueryFileListResponse/GoBackURL)">
          <input id="backBtn" type="button" value="Go Back" onclick="go_back()"/>
        </xsl:if>
        <div id="roxiequerylistmenu" />
        <iframe id="DataFrame" name="DataFrame" style="display:none; visibility:hidden;"/>
      </body>
    </html>
  </xsl:template>
    
  <xsl:template match="*|@*|text()"/>
    
</xsl:stylesheet>
