<?xml version="1.0" encoding="utf-8"?>
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
   
  <xsl:variable name="autoRefresh" select="/MetricsResponse/AutoRefresh"/>
  <xsl:variable name="autoupdatechecked" select="/MetricsResponse/AutoUpdate"/>
  <xsl:variable name="clusterName" select="/MetricsResponse/Cluster"/>
  <xsl:variable name="selectAllChecked" select="/MetricsResponse/SelectAllChecked"/>
  <xsl:variable name="acceptLanguage" select="/MetricsResponse/AcceptLanguage"/>
  <xsl:variable name="localiseFile"><xsl:value-of select="concat('../nls/', $acceptLanguage, '/hpcc.xml')"/></xsl:variable>
  <xsl:variable name="hpccStrings" select="document($localiseFile)/hpcc/strings"/>

  <!--define acceptable percentage deviation from mean-->
   
   <xsl:template match="text()"/>
   <xsl:decimal-format NaN="##########0.##"/>
   
   <!--each MetricInfo node may have different set of name/value pairs in arbitrary order so -->
   <!--collect all names from first instance of MetricsInfo node-->            
   <xsl:template match="/MetricsResponse">
      <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
         <head>
            <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
            <title>Computers</title>
           <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
           <style type="text/css">
               .menu1 {
                MARGIN: 0px 5px 0px 0px; WIDTH: 12px; HEIGHT: 14px
               }
               .sort-table {
                  border-top: #777 2px solid; 
                  border-bottom: #777 2px solid; 
                  font: 9pt verdana, arial, helvetica, sans-serif;
                  padding:1px;
                  text-align:center;
               }
               .sort-table thead {
                  background:lightgrey;
               };
               .sort-table th {
                  border: #c0c0c0 1px solid; 
               }
               .sort-table td {
                  border: #e0e0e0 1px solid; 
                  border-collapse:collapse;
               }
            </style>
            <script type="text/javascript" src="/esp/files_/scripts/multiselect.js">0</script>
            <script type="text/javascript">
              var autoRefreshVal=<xsl:value-of select="$autoRefresh"/>;
              var autoUpdateChecked=<xsl:value-of select="$autoupdatechecked"/>;
            <xsl:text disable-output-escaping="yes"><![CDATA[
         //############ begin auto reload script ######################
         var reloadTimer = null;
         var reloadTimeout = 0;
         var idCount = 0;
         var checkboxIDs = new Array();
         var viewColumnsStr = '<xsl:value-of select="$hpccStrings/st[@id='ViewColumns']"/>';

         // This function gets called when the window has completely loaded.
         // It starts the reload timer with a default time value.
         
         function onLoad()
         {
            initSelection('viewTable', true, onChecked);
            //onRowCheck(false);
          
            var autoRefreshEdit = document.getElementById('AutoRefresh');
            autoRefreshVal = autoRefreshEdit.value;
            if (autoRefreshVal > 0)            
               setReloadTimeout(autoRefreshVal); // Pass a default value
            enableAutoRefresh(autoRefreshVal > 0);

            if (!autoUpdateChecked)
            {
              document.getElementById("autoUpdateChk").checked = false;
              document.getElementById("updateMetriceBtn").disabled = false;
            }
            else
            {
              document.getElementById("autoUpdateChk").checked = true;
              document.getElementById("updateMetriceBtn").disabled = true;
            }
         }               
                             
         function setReloadTimeout(mins) {
             if (reloadTimeout != mins) {
                if (reloadTimer) {              
                   clearTimeout(reloadTimer);
                   reloadTimer = null;
                }               
                if (mins > 0)
                    reloadTimer = setTimeout("reloadPage()", Math.ceil(parseFloat(mins) * 60 * 1000));
                reloadTimeout = mins;
             }
         }

         function enableAutoRefresh(enable) {
            var autoRefreshEdit = document.getElementById('AutoRefresh');
            if (enable) {
               if (autoRefreshVal == 0)
                  autoRefreshVal = 1;
               autoRefreshEdit.value = autoRefreshVal;
               autoRefreshEdit.disabled = false;
               setReloadTimeout(autoRefreshVal);
            }
            else {
               autoRefreshVal = autoRefreshEdit.value;
               autoRefreshEdit.value = 0;
               autoRefreshEdit.disabled = true
               setReloadTimeout(0);
            }
         }
         
         function reloadPage() {
             document.forms[0].submit();
         }
                  
         //############ end of auto reload script ######################               
               function getElementByTagName(node, tag)
               {
                  for (var child = node.firstChild; child; child=child.nextSibling)
                     if (child.tagName == tag)
                        return child;
                  return null;
               }
                        
               function hideCell(o, str, show)
               {
                  if (o.tagName=='TH' || o.tagName=='TD')
                                  {
                    var sp = o.id.lastIndexOf('_');
                    var str0 = o.id.substring(0, sp);
                    if (str0 == str)
                    {
                      o.style.display = show ? 'block' : 'none'; 
                    }
                    return;
                                  }

                  var ch=o.children;
                                if (ch)
                                    for (var i in ch)
                                            hideCell(ch[i], str, show);
                                    return;
               }
      
               function checkBoxUpdated(cmd, skipCheckBoxUpdate)
               {
                  var index = cmd.substring(5); //skip past "_col_"
                  var show = false; 
                  var id = 'ShowColumns_i' + (index - 1); //no checkbox exists for column 1 (I.P. Address)
                  var checkbox = document.getElementById(id);    
                  if (checkbox.checked == true)   
                    show = true;               

                  if (!show)
                  {
                    document.getElementById("SelectAllChecked").value = false;
                    document.getElementById("SelectAllCheckBox").checked = false;
                  }

                  var table     = document.getElementById('resultsTable');
                  var cellid = 'cell_'+ index;
                  if (autoUpdateChecked)
                    hideCell(table, cellid, show);
                  else if (idCount < 1)
                  {                 
                    checkboxIDs[0] = index;
                    idCount++;
                  }
                  else
                  {                 
                    var bExist = false;
                    for (i = 0; i < idCount; i++)
                    {
                      if (checkboxIDs[i] == index)
                      {
                          bExist = true;
                          break;
                      }
                    }

                    if (!bExist)
                    {
                      checkboxIDs[idCount] = index;
                      idCount++;
                    }
                  }

                  if (skipCheckBoxUpdate == null)
                  {
                     checkbox.checked = show;
                     clicked(checkbox);
                  }
                  else
                  {
                    var bFF=/Firefox[\/\s](\d+\.\d+)/.test(navigator.userAgent)?1:0;
                    var isChrome=/Chrome[\/\s](\d+\.\d+)/.test(navigator.userAgent)?1:0;
                    if (show && (bFF || isChrome) && autoUpdateChecked)        
                      document.forms[0].submit();
                      //reloadPage();
                  }
               }
            
               function headClicked(cmd)
               {
                  var index = cmd.substring(5); //skip past "_col_"
                  var show = false; 
                  var id = 'ShowColumns_i' + (index - 1); //no checkbox exists for column 1 (I.P. Address)
                  var checkbox = document.getElementById(id);    
                  var table     = document.getElementById('resultsTable');
                  var cellid = 'cell_'+ index;
                  hideCell(table, cellid, false);

                  checkbox.checked = false;
                  clicked(checkbox);
               }
            
               function menuHandler(cmd, skipCheckBoxUpdate)
               {
                  var index = cmd.substring(5); //skip past "_col_"
                  var table     = document.forms[0].all['resultsTable'];
                  var colGroup  = getElementByTagName(table, 'COLGROUP');
                  var col = colGroup.children[index];
                  var show = col.style && col.style.display && col.style.display == 'none' ; //show if hidden at present
                  col.style.display = show ? 'block' : 'none';

                  if (skipCheckBoxUpdate == null)
                  {
                     var id = 'ShowColumns_i' + (index - 1); //no checkbox exists for column 1 (I.P. Address)
                     var checkbox = document.forms[0].all[id];                     
                     checkbox.checked = show;
                     clicked(checkbox);
                  }
               }

                function queuePopup(table)
                {
                   /* define a menu as follows
                   var menu=[["Column_1", "Caption 1", "menuHandler", true], //true for checked item
                             null, //separator
                              ["Column_2", "Caption 2", null, false]];//false for unchecked item
                  */
                   var colGroup  = getElementByTagName(table, 'COLGROUP');
                   var columns = colGroup.children;
                   var nColumns = columns.length;
                   var captions = table.rows[0].cells;
            
                   var menu = new Array;
                   for (var i=1; i < nColumns; i++)//skip first "IP address" column
                   {
                      var menuItem = new Array;
                      menuItem[0] = columns[i].id;
                      menuItem[1] = captions[i].innerText;
                      menuItem[2] = "menuHandler";
                      menuItem[3] = columns[i].style.display != 'none';
                      
                      menu[i] = menuItem;
                   }
                   
                   contextMenu = showPopup(menu);
                   return false;
                } 

               function showPopup(menu)
               {
                  contextMenu=window.createPopup();
                  var popupBody=contextMenu.document.body;
                  popupBody.style.backgroundColor = "menu";
                  popupBody.style.border = "outset white 2px";
                  
                  var s1='<table style="font:menu;" id="tab" width="10" oncontextmenu="return false" onselectstart="return false" >' ;
                  var s  ='<tr>';
                  var i = 0;
                  for(var item in menu)
                     if(menu[item] && menu[item].length==4)
                     {
                        var menuItem= menu[item];
                        var id      = menuItem[0];
                        var caption = menuItem[1];
                        var handler = menuItem[2];
                        var checked = menuItem[3];
                        var disabled=!menuItem[2];
               
                        if ((i++ % 5 == 0) && i>0)
                          s += '</tr><tr>';

                        s+='<td nowrap style="padding:0">'+ 
                                   '<input type="checkbox"' + (checked ? 'checked="true" ' : ' ') + (disabled?'':'onclick="return parent.'+handler+'(\''+id+'\')"') + 
                                   '      style="background-color:transparent;cursor:default" valign="center" '+
                            '      onmouseover=\''+ 'this.runtimeStyle.cssText="background-color:highlight;color:'+(disabled?'graytext;':'highlighttext;')+'" \' '+
                            '      onmouseout=\'this.runtimeStyle.cssText="" \'>'+caption+ 
                            ' </input>' +
                              '</td>';
                     }
                     else
                         s +='<tr><td><hr size="2"></td></tr>';

                  var numRows = i > 5 ? 5 : i;
                  s1+= '<tr><th align="center" colspan="' + numRows + '">' + viewColumnsStr + ':<hr/></th></tr>';
                  s1 += s;
                  s1 += '<tr><th align="center" colspan="' + numRows + '"><hr/>' + 
                          '<input type="button" value="Close" onclick="parent.contextMenu.hide()"/>' +
                          '</th></tr>'
                  s1 +='</tr></table>';
                  
                  document.all.menu.innerHTML = s1;
                  var w=document.all.tab.clientWidth+5;
                  var h=document.all.tab.clientHeight+5;
               
                  popupBody.innerHTML=s1;
               
                  contextMenu.show(window.event.screenX, window.event.screenY, w, h, null);
                  if(window.event) 
                     window.event.cancelBubble=true;
               
                  return contextMenu;
               }

               function onRowCheck(checked)
               {
               }              
      
               function selectAll0(show)
               {
                  document.getElementById("SelectAllChecked").value = show;
                  selectAll(show);
                  var bFF=/Firefox[\/\s](\d+\.\d+)/.test(navigator.userAgent)?1:0;
                  if (show && bFF)
                  {
                      document.forms[0].submit();
                  }
               }

               function onChecked(checkbox)
               {
                  var id = checkbox.id;
                  var index = Number(id.substring(13)); //skip past "ShowColumns_i"
                  var col_id = "_col_" + (index+1);
                  checkBoxUpdated(col_id);
               }
            
               function AutoUpdateMetrics(checked)
               {
                 if (checked && (idCount > 0))
                   UpdateMetricesNow(false);
                 autoUpdateChecked = checked;
                 document.getElementById("AutoUpdate").value = checked;
                 document.getElementById("updateMetriceBtn").disabled = checked;
               } 

               function UpdateMetricesNow(fromUpdateMetriceBtn)
               {
                  if (idCount > 0)
                  {
                    var table = document.getElementById('resultsTable');
                    for (i=0;i<idCount;i++)
                    {
                      var show = false;
                      var oneId = checkboxIDs[i];
                      var chkid = 'ShowColumns_i' + (oneId - 1);
                      var checkbox = document.getElementById(chkid);    
                      if (checkbox.checked == true)   
                        show = true;               
            
                      var cellid = 'cell_'+ oneId;
                      hideCell(table, cellid, show);
                    }

                    var bFF=/Firefox[\/\s](\d+\.\d+)/.test(navigator.userAgent)?1:0;
                    if (bFF)
                    {
                      document.forms[0].submit();
/*
                      if (fromUpdateMetriceBtn)
                      {                      
                        document.getElementById("autoUpdateChk").checked = false;
                        document.getElementById("updateMetriceBtn").disabled = false;
                      }
*/
                    }
                    idCount = 0;
                  }
               } 
               var contextMenu = null;
               ]]></xsl:text>                
            </script>
         </head>
         
         <body onload="setReloadFunction('reloadPage');onLoad()">
            <h2><xsl:value-of select="$hpccStrings/st[@id='Metrics']"/></h2>
            <form id="listitems" action="/ws_machine/GetMetrics" method="post">
              <input type="hidden" name="Cluster" value="{$clusterName}"/>
              <input type="hidden" id="SelectAllChecked" name="SelectAllChecked" value="{$selectAllChecked}"/>
              <input type="hidden" id="AutoUpdate" name="AutoUpdate" value="{$autoupdatechecked}"/>
              <table id='resultsTable' class="sort-table" oncontextmenu="return queuePopup(this)" cellspacing="0">
              <!--colgroup>
                   <col/>
                   <xsl:for-each select="FieldInformation/FieldInfo">
                      <col id="_col_{position()}">
                        <xsl:if test="Hide=1">
                           <xsl:attribute name="style">display:none</xsl:attribute>
                        </xsl:if>
                      </col>
                   </xsl:for-each>
                </colgroup-->
               <thead>
               <tr>
                  <th id="cell_0_0"><xsl:value-of select="$hpccStrings/st[@id='IPAddress']"/></th>
                  <xsl:apply-templates select="FieldInformation/FieldInfo"/>
               </tr>
               </thead>
               <tbody>
               <xsl:apply-templates select="Metrics/MetricsInfo"/>
               <xsl:variable name="nRows" select="count(Metrics/MetricsInfo)"/>
               <xsl:if test="$nRows>1">
                  <tr border="2px solid"/>
                  <tr onmouseenter="bgColor='#F0F0FF'">
                     <xsl:choose>
                        <xsl:when test="$nRows mod 2">
                           <xsl:attribute name="bgColor">#F0F0F0</xsl:attribute>
                           <xsl:attribute name="onmouseleave">bgColor='#F0F0F0'</xsl:attribute>
                        </xsl:when>
                        <xsl:otherwise>
                           <xsl:attribute name="bgColor">#FFFFFF</xsl:attribute>
                           <xsl:attribute name="onmouseleave">bgColor='#FFFFFF'</xsl:attribute>
                        </xsl:otherwise>
                     </xsl:choose>
                     <td><b><xsl:value-of select="$hpccStrings/st[@id='Mean']"/></b></td>
                     <xsl:for-each select="FieldInformation/FieldInfo">
                        <td id="cell_{position()}_mean" >
                          <xsl:if test="Hide=1">
                            <xsl:attribute name="style">display:none</xsl:attribute>
                          </xsl:if>
                          <xsl:value-of select="number(Mean)"/>
                        </td>
                     </xsl:for-each>                  
                  </tr>
                  <tr onmouseenter="bgColor='#F0F0FF'">
                     <xsl:choose>
                        <xsl:when test="$nRows mod 2">
                           <xsl:attribute name="bgColor">#FFFFFF</xsl:attribute>
                           <xsl:attribute name="onmouseleave">bgColor='#FFFFFF'</xsl:attribute>
                        </xsl:when>
                        <xsl:otherwise>
                           <xsl:attribute name="bgColor">#F0F0F0</xsl:attribute>
                           <xsl:attribute name="onmouseleave">bgColor='#F0F0F0'</xsl:attribute>
                        </xsl:otherwise>
                     </xsl:choose>
                     <td><b>Std Dev</b></td>
                     <xsl:for-each select="FieldInformation/FieldInfo">
                        <td id="cell_{position()}_stdd" >
                          <xsl:if test="Hide=1">
                            <xsl:attribute name="style">display:none</xsl:attribute>
                          </xsl:if>
                          <xsl:value-of select="number(StandardDeviation)"/>
                        </td>
                     </xsl:for-each>
                  </tr>
               </xsl:if>
               </tbody>
            </table>
            <table>
              <tr>
                <td>
                  <input type="checkbox" id="autoUpdateChk" onclick="AutoUpdateMetrics(this.checked)">
                    <!--xsl:choose>
                      <xsl:when test="$autoUpdate = true">
                        <xsl:attribute name="checked">true</xsl:attribute>a
                      </xsl:when>
                      <xsl:otherwise>
                        <xsl:attribute name="checked">false</xsl:attribute>b
                      </xsl:otherwise>
                    </xsl:choose-->
                    <xsl:value-of select="$hpccStrings/st[@id='AutoUpdateMetricsWhenViewColumnsChanging']"/>
                  </input>
                </td>
                <td>
                  <input type="button" id="updateMetriceBtn" value="{$hpccStrings/st[@id='UpdateMetricsNow']}" onclick="UpdateMetricesNow(true)">
                    <!--xsl:choose>
                      <xsl:when test="$autoUpdate = true">
                        <xsl:attribute name="disabled">true</xsl:attribute>
                      </xsl:when>
                      <xsl:otherwise>
                        <xsl:attribute name="disabled">false</xsl:attribute>
                      </xsl:otherwise>
                    </xsl:choose-->
                  </input>
                </td>
              </tr>
            </table>
            <br/>
            <br/>
            <b><xsl:value-of select="$hpccStrings/st[@id='ViewColumns']"/>:</b>
            <table id="viewTable" width="100%">
               <tr/>
               <tr>
                  <xsl:apply-templates select="FieldInformation/FieldInfo" mode="createCheckboxes"/>
               </tr>
            </table>
            <xsl:if test="count(FieldInformation/FieldInfo) > 2">
               <table>
                  <tr>
                     <th id="selectAll1" colspan="5">
                       <input type="checkbox" id="SelectAllCheckBox" title="{$hpccStrings/st[@id='SelectDeselectAll']}" onclick="selectAll0(this.checked)">
                         <xsl:if test="$selectAllChecked &gt; 0">
                           <xsl:attribute name="checked">true</xsl:attribute>
                         </xsl:if>
                       <xsl:value-of select="$hpccStrings/st[@id='SelectAllOrNone']"/></input>
                     </th>
                  </tr>
               </table>
            </xsl:if>
            <br/>
            <br/>
            <input type="checkbox" name="cbAutoRefresh" onclick="enableAutoRefresh(this.checked)">
            <xsl:if test="$autoRefresh &gt; 0">
               <xsl:attribute name="checked">true</xsl:attribute>
            </xsl:if>
            </input><xsl:value-of select="$hpccStrings/st[@id='AutoRefreshEvery']"/> <input type="text" id="AutoRefresh" name="AutoRefresh" size="2" value="{$autoRefresh}" onblur="setReloadTimeout(this.value)">
            <xsl:if test="$autoRefresh = 0">
               <xsl:attribute name="disabled">true</xsl:attribute>
            </xsl:if>
            </input> <xsl:value-of select="$hpccStrings/st[@id='Mins']"/>
            <br/><input type="submit" value="{$hpccStrings/st[@id='Refresh']}" id="submitBtn"/>
            </form>
            <br/>
            <br/>
            <a href="/WsTopology/TpClusterQuery?Type=ROOT"><xsl:value-of select="$hpccStrings/st[@id='ClusterTopology']"/></a><br/>
            <a href="/WsSMC/Activity"><xsl:value-of select="$hpccStrings/st[@id='Home']"/></a>
            <DIV id="menu" style="LEFT: 0px; TOP: 0px; VISIBILITY: hidden; POSITION: absolute"></DIV>
         </body>
      </html>
   </xsl:template>
   
   <xsl:template match="FieldInfo">
      <th id="cell_{position()}_0" onclick="headClicked('_col_{position()}')" padding="0">
        <xsl:if test="Hide=1">
          <xsl:attribute name="style">display:none</xsl:attribute>
        </xsl:if>
        <xsl:value-of select="Caption"/>
      </th>
   </xsl:template>
      
   <xsl:template match="FieldInfo" mode="createCheckboxes">
      <xsl:variable name="index" select="position()-1"/>
      <xsl:if test="$index mod 5 = 0 and $index > 0"><!--start new line after every 5 checkboxes -->
         <xsl:text disable-output-escaping="yes">&lt;/tr&gt;&lt;tr&gt;</xsl:text>
      </xsl:if>
      <td>
         <input type="checkbox" id="ShowColumns_i{$index}" name="ShowColumns_i{$index}" 
           value="{Name}" onclick="checkBoxUpdated('_col_{$index+1}', true); clicked(this);">
         <xsl:if test="not(Hide=1)">
            <xsl:attribute name="checked">true</xsl:attribute>
         </xsl:if>
         </input>
         <xsl:value-of select="Caption"/>
      </td>
   </xsl:template>
   
   <xsl:template match="node()|@*" mode="createCheckboxes"/>
   
   <xsl:template match="MetricsInfo">
      <tr onmouseenter="bgColor='#F0F0FF'">
            <xsl:choose>
                <xsl:when test="position() mod 2">
                    <xsl:attribute name="bgColor">#FFFFFF</xsl:attribute>
                    <xsl:attribute name="onmouseleave">bgColor='#FFFFFF'</xsl:attribute>
                </xsl:when>
                <xsl:otherwise>
                    <xsl:attribute name="bgColor">#F0F0F0</xsl:attribute>
                    <xsl:attribute name="onmouseleave">bgColor='#F0F0F0'</xsl:attribute>
                </xsl:otherwise>
            </xsl:choose>
         <td><xsl:value-of select="Address"/><input type="hidden" name="Addresses_i{position()}" value="{Address}"/></td>
         <xsl:variable name="row_id" select="position()"/>
        <xsl:variable name="FieldsNode" select="Fields"/>
         <xsl:variable name="FieldInformation" select="/MetricsResponse/FieldInformation"/>
         <xsl:for-each select="Fields/Field">
            <td id="cell_{position()}_{$row_id}" >
              <xsl:if test="Hide=1">
                <xsl:attribute name="style">display:none</xsl:attribute>
              </xsl:if>
              <xsl:choose>
                  <xsl:when test="Undefined = 1">-</xsl:when>
                  <xsl:otherwise>
                     <xsl:if test="Warn = 1">
                        <xsl:attribute name="bgcolor">FF8800</xsl:attribute>
                     </xsl:if>
                     <xsl:value-of select="number(Value)"/>
                  </xsl:otherwise>
               </xsl:choose>
            </td>
         </xsl:for-each>
      </tr>
   </xsl:template>

</xsl:stylesheet>
