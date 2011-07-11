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
    <xsl:variable name="namevalue" select="TpLogFileResponse/Name"/>
    <xsl:variable name="typevalue" select="TpLogFileResponse/Type"/>
    <xsl:variable name="filtertype" select="TpLogFileResponse/FilterType"/>
    <xsl:variable name="reversely" select="TpLogFileResponse/Reversely"/>
    <xsl:variable name="hasdate" select="TpLogFileResponse/HasDate"/>
    <xsl:variable name="prevpage" select="TpLogFileResponse/PrevPage"/>
    <xsl:variable name="nextpage" select="TpLogFileResponse/NextPage"/>

    <xsl:variable name="totalpages" select="TpLogFileResponse/TotalPages"/>
    <xsl:variable name="pagenumber" select="TpLogFileResponse/PageNumber"/>
    <xsl:variable name="startdate" select="TpLogFileResponse/StartDate"/>
    <xsl:variable name="enddate" select="TpLogFileResponse/EndDate"/>
    <xsl:variable name="lasthours" select="TpLogFileResponse/LastHours"/>
    
    <xsl:variable name="firstrows" select="TpLogFileResponse/FirstRows"/>
    <xsl:variable name="lastrows" select="TpLogFileResponse/LastRows"/>

    <xsl:template match="TpLogFileResponse">
    <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
      <head>
        <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
        <link type="text/css" rel="StyleSheet" href="/esp/files_/css/sortabletable.css"/>
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
        <link type="text/css" rel="StyleSheet" href="/esp/files_/css/sortabletable.css"/>
        <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
        <script type="text/javascript" src="/esp/files_/joblist.js">0</script>
        <script type="text/javascript">
          var nameValue = '<xsl:value-of select="$namevalue"/>';
          var typeValue = '<xsl:value-of select="$typevalue"/>';
          var rbid = <xsl:value-of select="$filtertype"/>;
          var reversely0 = <xsl:value-of select="$reversely"/>;
          var pageNumber = <xsl:value-of select="$pagenumber"/>;
          var startDate = '<xsl:value-of select="$startdate"/>';
          var endDate = '<xsl:value-of select="$enddate"/>';
          var lastHours = '<xsl:value-of select="$lasthours"/>';
          var firstRows = <xsl:value-of select="$firstrows"/>;
          var lastRows = <xsl:value-of select="$lastrows"/>;
          var hasDate = <xsl:value-of select="$hasdate"/>;
          var prevPage = <xsl:value-of select="$prevpage"/>;
          var nextPage = <xsl:value-of select="$nextpage"/>;

          <xsl:text disable-output-escaping="yes"><![CDATA[
            // This function gets called when the window has completely loaded.
            var intervalId = 0;
            var hideLoading = 1;
            var rbid0 = rbid;

            function onRBChanged(id)
            {
              rbid = id;
              document.getElementById('First').disabled = false;
              document.getElementById('Last').disabled = false;
              document.getElementById('Hour').disabled = false;
              document.getElementById('From').disabled = false;
              document.getElementById('To').disabled = false;
              document.getElementById('GetLogR').disabled = false;
              document.getElementById('PageNumber').disabled = false;

              if (rbid != 1)
              {
                document.getElementById('First').value = '';
                document.getElementById('First').disabled = true;
              }
              if (rbid != 2)
              {
                document.getElementById('Hour').value = '';
                document.getElementById('Hour').disabled = true;
              }
              if (rbid != 4)
              {
                document.getElementById('PageNumber').value = '';
                document.getElementById('PageNumber').disabled = true;
              }
              if (rbid != 5)
              {
                document.getElementById('Last').value = '';
                document.getElementById('Last').disabled = true;
              }
              if (rbid != 6)
              {
                document.getElementById('From').value = '';
                document.getElementById('From').disabled = true;
                document.getElementById('To').value = '';
                document.getElementById('To').disabled = true;
              }

              if (rbid == 0 || rbid == 4 || rbid == 3)
              {
                document.getElementById('GetLogR').disabled = true;
              }
              else
              {
                document.getElementById('PrevPage').disabled = true;
                document.getElementById('NextPage').disabled = true;
              }
            }

            function onEditKeyUp()
            {
              if (window.event && window.event.keyCode == 13)
              {
                onGetLog();
              }
            }

            function formatDT(d)
            {
              var dt=new Date(d);
              if(isNaN(dt)) return null;
              return (dt.getFullYear()+'-'+format2(dt.getMonth()+1)
                +'-'+format2(dt.getDate())+' '+format2(dt.getHours())
                +':'+format2(dt.getMinutes())+':'+format2(dt.getSeconds()));
            }

            function onGetLog(id)
            {
              var firstRows='', lastRows='';
              var startDate='', endDate='';
              var reverse = false;
              var nameValue0 = nameValue;
                            
              if (id < 0)
              {
                reverse = true;
              }
              var ref = '/WsTopology/TpLogFile/' + nameValue0 + '?Name='+nameValue+'&Type='+typeValue;
              ref += '&Reversely='+reverse+'&HasDate='+hasDate;
              if (id > 0)
              {
                ref += '&FilterType=0';
              }
              else
              {
                ref += '&FilterType='+rbid;
              }

              if (id > 1)
              {
                ref += '&PageNumber='+nextPage;
                document.location.href = ref;
              }
              else if (id > 0)
              {
                ref += '&PageNumber='+prevPage;
                document.location.href = ref;
              }
              else if (rbid == 4)
              {
                var nb = document.getElementById('PageNumber').value
                if(!nb)
                {
                  alert("'PageNumber' field should be defined.");
                  return false;
                }

                var d=parseInt(nb);
                if(isNaN(d))
                {
                  alert("Invalid data in 'PageNumber' field");
                  return false;
                }

                d = d - 1;
                ref += '&PageNumber='+d;
                document.location.href = ref;
              }
              else if (rbid == 1)
              {
                if(!document.getElementById('First').value)
                {
                  alert("'First rows' field should be defined.");
                  return false;
                }

                firstRows = document.getElementById('First').value
                var d=parseInt(firstRows);
                if(isNaN(d))
                {
                  alert("Invalid data in 'First rows' field");
                  return false;
                }

                document.location.href=ref +'&FirstRows='+firstRows;
              }
              else if (rbid == 2)
              {
                if(!document.getElementById('Hour').value)
                {
                  alert("'Last hours' field should be defined.");
                  return false;
                }

                var hours = document.getElementById('Hour').value
                var d=parseInt(hours);
                if(isNaN(d))
                {
                  alert("Invalid data in 'Last hours' field");
                  return false;
                }

                var df=new Date();
                var dt=new Date();
                df.setHours(df.getHours()-d);

                startDate=formatDT(df);
                endDate=formatDT(dt);

                document.location.href = ref + '&StartDate='+startDate + '&EndDate='+endDate;
              }
              else if (rbid == 5)
              {
                if(!document.getElementById('Last').value)
                {
                  alert("'Last rows' field should be defined.");
                  return false;
                }

                lastRows = document.getElementById('Last').value
                var d=parseInt(lastRows);
                if(isNaN(d))
                {
                  alert("Invalid data in a 'Last rows' field");
                  return false;
                }

                document.location.href= ref + '&LastRows='+lastRows;
              }
              else if (rbid == 6)
              {
                var df=document.getElementById('From').value;
                var dt=document.getElementById('To').value;
                if(!df && !dt)
                {
                  alert("Either 'Date from' or 'to' field should be defined.");
                  return false;
                }

                if (df)
                {
                  var d=Date.parse(df);
                  if(isNaN(d))
                  {
                      alert("Invalid data in a 'Date' field");
                      return false;
                  }

                  startDate=formatDT(df);
                }
                if (dt)
                {
                  var d=Date.parse(dt);
                  if(isNaN(d))
                  {
                      alert("Invalid data in a 'Date' field");
                      return false;
                  }

                  endDate=formatDT(dt);
                }
                document.location.href = ref + '&StartDate='+startDate + '&EndDate='+endDate;
              }
              else
              {
                document.location.href = ref;
              }

              return true;
            }

            function onGetLogData()
            {
              var nameValue0 = nameValue;                                                   
              var ref = '/WsTopology/TpLogFileDisplay/' + nameValue0 + '?LoadData=true&Name='+nameValue+'&Type='+typeValue;
              ref += '&FilterType='+rbid+'&PageNumber='+pageNumber;
              if (rbid == 1)
              {
                if(firstRows == '')
                {
                  alert("'First rows' field should be defined.");
                  return false;
                }

                ref=ref +'&FirstRows='+firstRows+'&Reversely='+reversely0;
              }
              else if (rbid == 5)
              {
                if(lastRows == '')
                {
                  alert("'Last rows' field should be defined.");
                  return false;
                }

                ref=ref +'&LastRows='+lastRows+'&Reversely='+reversely0;
              }
              else if (rbid == 2)
              {
                if(lastHours == '')
                {
                  return false;
                }

                var d=parseInt(lastHours);
                if(isNaN(d))
                {
                  alert("Invalid data in 'Last hours' field");
                  return false;
                }

                var df=new Date();
                var dt=new Date();
                df.setHours(df.getHours()-d);

                startDate=formatDT(df);
                endDate=formatDT(dt);

                ref = ref + '&StartDate='+startDate + '&EndDate='+endDate+'&Reversely='+reversely0;
              }
              else if (rbid == 6)
              {
                if(startDate == '' && endDate == '')
                {
                  alert("Either 'Date from' or 'to' field should be defined.");
                  return false;
                }

                if(startDate != '')
                {
                  var d=Date.parse(startDate);
                  if(isNaN(d))
                  {
                    return false;
                  }

                  startDate=formatDT(startDate);
                }

                if(endDate != '')
                {
                  var d=Date.parse(endDate);
                  if(isNaN(d))
                  {
                    return false;
                  }

                  endDate=formatDT(endDate);
                }

                ref = ref + '&StartDate='+startDate + '&EndDate='+endDate+'&Reversely='+reversely0;
              }

              //document.location.href = ref;

              var dataFrame = document.getElementById('DataFrame');
              if (dataFrame)
              {
                dataFrame.src = ref;
              }

              return true;
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

              if (prevPage > -1)
                document.getElementById('PrevPage').disabled = false;
              if (nextPage > -1)
                document.getElementById('NextPage').disabled = false;

              document.getElementById('GetLog').disabled = false;
              document.getElementById("GetLog").focus();
            }

            var url0 = '';
            function getOptions(name)
            {
              var link0 = "/FileSpray/OpenSave?Name="+name + "&BinaryFile=true";
              mywindow = window.open (link0, "name", "location=0,titlebar=no,scrollbars=0,resizable=0,width=400,height=260");
              if (mywindow.opener == null)
                mywindow.opener = window;
              mywindow.focus();
            }

            function setOpenSave(type)
            {
              //document.getElementById('OpenSaveOp').value = type;
              document.location.href=url0 + '&Zip=' + type;
            }

            function onDownload(zip)
            {
              //document.location.href = '/WsTopology/SystemLog?Name='+nameValue+'&Type='+typeValue+'&Zip='+zip;
              url0 = '/WsTopology/SystemLog?Name='+nameValue+'&Type='+typeValue;
              getOptions(nameValue); 
            }

            function onLoad()
            {
              onRBChanged(rbid0);
              for (i= 0; i < 7; i++)
              {
                if (document.ViewLogForm.FilterRB[i].value == rbid0)
                  document.ViewLogForm.FilterRB[i].checked = true;
              }
              //document.getElementById('Reversely').checked = reversely0;

              document.getElementById('PrevPage').disabled = true;
              document.getElementById('NextPage').disabled = true;
              document.getElementById('GetLog').disabled = true;
              if ((rbid0 == 1) || (rbid0 == 2) || (rbid0 == 5) || (rbid0 == 6))
                document.getElementById('GetLogR').disabled = false;
              else
                document.getElementById('GetLogR').disabled = true;

              startBlink();
              reloadTimer = setTimeout("loadPageTimeOut()", 300000);

              onGetLogData();

              return;
            }              

          ]]></xsl:text>
        </script>
      </head>
      <body class="yui-skin-sam" onload="nof5();onLoad()">
        <form name="ViewLogForm" >
          <table>
            <tr>
              <td>
                <table>
                  <tr>
                    <td><input type="radio" name="FilterRB" value="0" onclick="onRBChanged(0)"/>First page</td>
                  </tr>
                  <tr>
                    <td><input type="radio" name="FilterRB" value="3" onclick="onRBChanged(3)"/>or last page (page <xsl:value-of select="$totalpages"/>)</td>
                  </tr>
                  <tr>
                    <td><input type="radio" name="FilterRB" value="4" onclick="onRBChanged(4)"/>or go to page:<input id="PageNumber" size="5" type="text" value="{PageNumber+1}" style="text-align:right;" onKeyUp = "onEditKeyUp()"/> </td>
                  </tr>
                </table>
              </td>
              <td>
                <table>
                  <tr>
                    <td><input type="radio" name="FilterRB" value="1" onclick="onRBChanged(1)"/></td>
                    <td>or first:<input id="First" size="5" type="text" value="{FirstRows}" style="text-align:right;" onKeyUp = "onEditKeyUp()"/> rows</td>
                  </tr>
                  <tr>
                    <td><input type="radio" name="FilterRB" value="5" onclick="onRBChanged(5)"/></td>
                    <td colspan="2">or last:<input id="Last" size="5" type="text" value="{LastRows}" style="text-align:right;" onKeyUp = "onEditKeyUp()"/> rows</td>
                  </tr>
                </table>
              </td>
              <td>
                <table>
                  <tr>
                    <xsl:choose>
                      <xsl:when test="HasDate=1">
                        <td><input type="radio" name="FilterRB" value="6"  onclick="onRBChanged(6)"/>
                        or date from:<input id="From" size="15" type="text" value="{StartDate}" onKeyUp = "onEditKeyUp()"/></td>
                        <td colspan="4">to: <input id="To" size="15" type="text" value="{EndDate}" onKeyUp = "onEditKeyUp()"/> (mm/dd/yyyy hh:mm:ss)</td>
                      </xsl:when>
                      <xsl:otherwise>
                        <td><input type="radio" name="FilterRB" value="6"  onclick="onRBChanged(6)" disabled="true"/>
                        or date from:<input id="From" size="15" type="text" value="{StartDate}" onKeyUp = "onEditKeyUp()" disabled="true"/></td>
                        <td colspan="4">to: <input id="To" size="15" type="text" value="{EndDate}" onKeyUp = "onEditKeyUp()" disabled="true"/> (mm/dd/yyyy hh:mm:ss)</td>
                      </xsl:otherwise>
                    </xsl:choose>
                  </tr>
                  <tr>
                    <xsl:choose>
                      <xsl:when test="HasDate=1">
                        <td><input type="radio" name="FilterRB" value="2" onclick="onRBChanged(2)"/> or last:<input id="Hour" size="5" type="text" value="{LastHours}"  onKeyUp = "onEditKeyUp()" style="text-align:right;"/> hours</td>
                      </xsl:when>
                      <xsl:otherwise>
                        <td><input type="radio" name="FilterRB" value="2" onclick="onRBChanged(2)" disabled="true"/> or last:<input id="Hour" size="5" type="text" value="{LastHours}"  onKeyUp = "onEditKeyUp()" style="text-align:right;" disabled="true"/> hours</td>
                      </xsl:otherwise>
                    </xsl:choose>
                  </tr>
                  <tr>
                  </tr>
                </table>
              </td>
            </tr>
            <tr>
            </tr>
            <tr>
            </tr>
            <tr>
              <td colspan="4">
                <input type="button" id="GetLog" class="sbutton"  size="40" value="Submit" onClick="return onGetLog(0)"/>
                <input type="button" id="GetLogR" class="sbutton"  size="40" value="Reversely" onClick="return onGetLog(-1)"/>
                <input type="button" id="PrevPage" class="sbutton"  size="40" value="PrevPage" onClick="return onGetLog(1)"/>
                <input type="button" id="NextPage" class="sbutton"  size="40" value="NextPage" onClick="return onGetLog(2)"/>
                <input type="button" id="Download" class="sbutton"  size="40" value="Download" onClick="return onDownload(0)"/>
              </td>
              <xsl:if test="FilterType=0 or FilterType=3 or FilterType=4">
                <td>Viewing page <xsl:value-of select="1+$pagenumber"/> of <xsl:value-of select="$totalpages"/></td>
              </xsl:if>
            </tr>
          </table>
          <div id="ViewLogData">
            <span id="loadingMsg"><h3>Loading, please wait...</h3></span>
            <span id="loadingTimeOut" style="visibility:hidden"><h3>Browser timed out due to a long time delay.</h3></span>
          </div>
        </form>
        <iframe id="DataFrame" name="DataFrame" style="display:none; visibility:hidden;"></iframe>
      </body>
    </html>
  </xsl:template>

</xsl:stylesheet>
