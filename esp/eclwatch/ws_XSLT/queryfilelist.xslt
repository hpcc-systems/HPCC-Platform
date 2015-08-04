<?xml version="1.0" encoding="UTF-8"?>
<!--

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

            var ch=o.childNodes;
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
