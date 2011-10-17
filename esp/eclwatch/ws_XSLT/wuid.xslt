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
  <xsl:variable name="havesubgraphtimings" select="WUInfoResponse/Workunit/HaveSubGraphTimings"/>
  <xsl:variable name="compile" select="WUInfoResponse/CanCompile"/>
  <xsl:variable name="autoRefresh" select="WUInfoResponse/AutoRefresh"/>
  <xsl:variable name="thorSlaveIP" select="WUInfoResponse/ThorSlaveIP"/>
  <xsl:variable name="isArchived" select="WUInfoResponse/Workunit/Archived"/>
  <xsl:variable name="debugTargetClusterType" select="WUInfoResponse/Workunit/targetclustertype" />
  <xsl:variable name="jobName" select="WUInfoResponse/Workunit/Jobname" />
  <xsl:include href="/esp/xslt/lib.xslt"/>
  <xsl:include href="/esp/xslt/wuidcommon.xslt"/>

  <xsl:template match="WUInfoResponse">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
            <head>
                <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
                <title><xsl:value-of select="$wuid"/></title>
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
        <link type="text/css" rel="StyleSheet" href="/esp/files_/css/sortabletable.css"/>
        <link type="text/css" rel="stylesheet" href="/esp/files_/default.css"/>
        <link type="text/css" rel="stylesheet" href="/esp/files_/css/espdefault.css"/>
        <xsl:text disable-output-escaping="yes"><![CDATA[
        <script type="text/javascript" src="/esp/files/yui/build/yahoo-dom-event/yahoo-dom-event.js"></script>
        <script type="text/javascript" src="/esp/files/yui/build/yahoo/yahoo-min.js"></script>
        <script type="text/javascript" src="/esp/files/yui/build/event/event-min.js"></script>
        <script type="text/javascript" src="/esp/files/yui/build/connection/connection-min.js"></script>
        <script type="text/javascript" src="/esp/files/scripts/espdefault.js">&#160;</script>
        ]]></xsl:text>
        <script type="text/javascript">
                var autoRefreshVal=<xsl:value-of select="$autoRefresh"/>;
                var isarchived=<xsl:value-of select="$isArchived"/>;
                    var wid='<xsl:value-of select="$wuid"/>';
            var dorefresh = true;
            var Jobname = '<xsl:value-of select="$jobName"/>';

          <xsl:text disable-output-escaping="yes"><![CDATA[
           var url0 = '';
           var reloadTimer = null;
                     var reloadTimeout = 0;
           var sections = new Array("Exceptions","Graphs","SourceFiles","Results","Variables","Timers","DebugValues","ApplicationValues","Workflows");
           var activeSections = new Array();
         
                     // This function gets called when the window has completely loaded.
                     // It starts the reload timer with a default time value.

     function onLoad()
     {
        /*
        initialize();
            
          if (isarchived)
        {
          return;
        }
        UpdateAutoRefresh();
        //reloadSection('Exceptions');
        checkPreloadedSections();
        */
        return;
     } 

   function onUnload()
   {
      for(var i=0;i<downloadWnds.length;i++)
      {
        if (downloadWnds[i] && downloadWnds[i].Wnd && !downloadWnds[i].Wnd.closed)
        {
          downloadWnds[i].Wnd.close();
        }
      }
   }

   function loadUrl(Url)
   {
      saveState();
      document.location.href = Url;
      return false;
   }

   function UpdateAutoRefresh()
   {
      if (autoRefreshVal > 0)            
      {
        reloadTimeout = 0;
        var refreshImg = document.getElementById('refresh');
        if (refreshImg)
        {
          refreshImg.src = '/esp/files_/img/refreshenabled.png';
        }
        setReloadTimeout(autoRefreshVal); // Pass a default value
      }
      else
      {
        var refreshImg = document.getElementById('refresh');
        if (refreshImg)
        {
          refreshImg.src = '/esp/files_/img/refreshdisabled.png';
        }
        dorefresh = false;
      }               
   }
              
   function initialize() {
     if (sessionExists() != false) {
       var doc = getIFrameDocument();
       if (doc.innerHTML != '')
       {
          document.body.innerHTML = doc.body.innerText;
          saveState();
       }
     }
   }

  function getOptions(name, url, viewable)
  {
    url0 = url;
    var link0 = "/FileSpray/OpenSave?Name="+name;
    if (!viewable)
      link0 += "&BinaryFile=true";
    mywindow = window.open (link0, "name", "location=0,titlebar=no,scrollbars=0,resizable=0,width=400,height=260");
    if (mywindow.opener == null)
      mywindow.opener = window;
    mywindow.focus();
  }

  function setOpenSave(type)
    {
    //document.getElementById('OpenSaveOp').value = type;
    document.location.href=url0 + '&Option=' + type;
  }

  function go(url)
  {
    document.location.href=url + '&Option=' + document.getElementById('OpenSaveOp').value;
  }

   function getIFrameDocument() {
    var historyFrame = document.getElementById('historyFrame');
    var doc = historyFrame.contentDocument;
    if (doc == undefined) // Internet Explorer
      doc = historyFrame.contentWindow.document;

    return doc;
   }

   function sessionExists() {
     var doc = getIFrameDocument();
     try {
       if (doc.body.innerText == '')
         return false;
       else
         return true;
     }
     catch (exp) {
       // sometimes an exception is thrown if a value is already in the iframe
       return true;
     }
   }

   function saveState() {
    // get our template that we will write into the history iframe
    
    // now write out the new contents
    var encodeDiv = document.getElementById('encodeDiv');
    if (encodeDiv)
    {
       encodeDiv.innerText = document.body.innerHTML;
       var doc = getIFrameDocument();    
       doc.open();
       doc.write('<html><body>' + encodeDiv.innerHTML + '</body></html>');
       doc.close();
       encodeDiv.innerText = '';
    }
   }

                   function setReloadTimeout(mins) 
                   {
                       if (reloadTimeout != mins) 
                       {
                          if (reloadTimer) 
                          {              
                             clearTimeout(reloadTimer);
                             reloadTimer = null;
                          }               
                          if (mins > 0)
              {
                              reloadTimer = setTimeout('reloadPage()', Math.ceil(parseFloat(mins) * 60 * 1000));
              }
                          reloadTimeout = mins;
                       }
                   }

                   function reloadPage() 
                   {
              var globalframe = document.getElementById('GlobalFrame');
              if (globalframe)
              {
                          globalframe.src = '/WsWorkunits/WUInfoDetails?Wuid=' + wid + '&' + getReloadUrl();
              }
                   }

                   function TurnRefresh() 
                   {
                       if (autoRefreshVal > 0)
                       {
                           if (dorefresh)
                           {
                              document.getElementById('refresh').src='/esp/files_/img/refreshdisabled.png';
                              dorefresh = false;
                              reloadTimeout = 0;
                              if (reloadTimer) 
                              {              
                                 clearTimeout(reloadTimer);
                                 reloadTimer = null;
                              }               
                           }
                           else
                           {
                              document.getElementById('refresh').src='/esp/files_/img/refreshenabled.png';
                              dorefresh = true;
                              setReloadTimeout(autoRefreshVal); // Pass a default value
                           }
                       }
                   }

                   function setProtected(obj)
                   {
                      var url='/WsWorkunits/WUUpdate?Wuid='+wid;
                      if (obj.checked)
                      {
                          url=url + "&ProtectedOrig=false&Protected=true";
                      }
                      else
                      {
                          url=url + "&ProtectedOrig=true&Protected=false";
                      }
                      document.location.href= url;
                   }

                   function CheckIPInput() 
                   {
                        var logBtn = document.getElementById('getthorslavelog')
                        if(document.getElementById('ThorSlaveIP').value)
                            logBtn.disabled = false;
                        else
                            logBtn.disabled = true;
                   }

                   function GetThorSlaveLog() 
                   {
                      //document.location.href='/WsWorkunits/WUFile?Wuid='+wid+'&Type=ThorSlaveLog&SlaveIP='+document.getElementById('ThorSlaveIP').value;
            getOptions('ThorSlave.log', '/WsWorkunits/WUFile?Wuid='+wid+'&Type=ThorSlaveLog&SlaveIP='+document.getElementById('ThorSlaveIP').value, true); 
                   }

           var downloadWnds = new Array();
         
           function download(ParentElement, Link)
           {
              ParentElement.disabled = true;
              for(var i=0;i<downloadWnds.length;i++)
              {
                if (downloadWnds[i] && downloadWnds[i].Element && downloadWnds[i].Element == ParentElement)
                {
                  downloadWnds[i].Wnd.focus();
                  return;
                }
              }
              var openHidden = false;
              if (Link.indexOf('xls')>0)
              {
                openHidden = true;
              }

                        var downloadLink = window.open(Link, "_graphStats_", 
                                              "toolbar=0,location=0,directories=0,status=1,menubar=0," + 
                                              "scrollbars=1, resizable=1", true);
              if (downloadLink)
              {
                            downloadLink.focus();

                //new Object({ Element:ParentElement, Wnd:window.open(Link, "", openHidden ? "left=1000,top=1000": "") });
                downloadWnds[downloadWnds.length+1] = new Object({Element:ParentElement, Wnd:downloadLink});
              }
              setTimeout(checkDownload, 500);
              return false;
           }   

           function launch(Link)
           {
              document.location.href = Link;
           }

           function checkDownload()
           {
              var continueCheck = false;
              if (downloadWnds)
              {
                  for(var i=0;i<downloadWnds.length;i++)
                  {
                    if (downloadWnds[i] && downloadWnds[i].Wnd)
                    {
                      if (downloadWnds[i].Wnd.closed)
                      {
                        downloadWnds[i].Element.disabled = false;
                        downloadWnds[i] = null;
                      }
                      else
                      {
                        continueCheck = true;
                      }
                    }
                  }
              }
              if (continueCheck)
              {
                setTimeout(checkDownload, 1000);
              }
              return true;
           }

            function toggleElement(ElementId)
            {
                var obj = document.getElementById(ElementId);
                explink = document.getElementById('explink' + ElementId.toLowerCase());
                if (obj) 
                {
                  if (obj.style.visibility == 'visible')
                  {
                    obj.style.display = 'none';
                    obj.style.visibility = 'hidden';
                    if (explink)
                    {
                      explink.className = 'wusectionexpand';
                    }
                  }
                  else
                  {
                    obj.style.display = 'inline';
                    obj.style.visibility = 'visible';
                    if (explink)
                    {
                      explink.className = 'wusectioncontract';
                    }
                    reloadSection(ElementId);

                  }
                  //saveState();

                }
            }

            function toggleFileElement(ElementId)
            {
                var obj = document.getElementById(ElementId);
                explink = document.getElementById('explink' + ElementId.toLowerCase());
                if (obj) 
                {
                  if (obj.style.visibility == 'visible')
                  {
                    obj.style.display = 'none';
                    obj.style.visibility = 'hidden';
                    if (explink)
                    {
                      explink.className = 'wufileexpand';
                    }
                  }
                  else
                  {
                    obj.style.display = 'inline';
                    obj.style.visibility = 'visible';
                    if (explink)
                    {
                      explink.className = 'wufilecontract';
                    }
                    else
                    {
                      alert('could not find: explink' + ElementId);
                    }
                    reloadSection(ElementId);

                  }
                  //saveState();

                }
            }


            function reloadSection(ElementId)
            {
                var thisSection = getSectionName(ElementId);

                if (hasSection(activeSections, thisSection))
                {
                }
                else
                {
                  var frameobj = document.getElementById(thisSection + 'Frame');
                  if (frameobj)
                  {
                    frameobj.src = '/WsWorkunits/WUInfoDetails?Wuid=' + wid + '&' + getSectionLoadUrl(thisSection);
                  }                      
                  activeSections[activeSections.length] = thisSection;
                  
                }
            }

            function checkPreloadedSections()
            {
              for(var i=0; i < sections.length;i++)
              {
                if (location.toString().indexOf('Include' + sections[i] + '=0') > -1)
                {
                }
                else
                {
                  activeSections[activeSections.length] = sections[i];
                  if (sections[i] == 'Exceptions')
                  {
                    activeSections[activeSections.length] = 'Warnings';
                    activeSections[activeSections.length] = 'Info';
                  }
                }
              }
              for(var i=0; i < activeSections.length;i++)
              {
                  toggleElement(activeSections[i]);
              }
              
            }

            function getReloadUrl()
            {
              var sectionUrl = '';
              for(i=0; i < activeSections.length;i++)
              {
                sectionUrl += '&Include' + activeSections[i] + '=1';
              }
              for(i=0; i < sections.length;i++)
              {
                if (sectionUrl.indexOf(sections[i])== -1)
                {
                  sectionUrl += '&Include' + sections[i] + '=0';
                }
              }
              return sectionUrl;
            }

            function getSectionName(Section)
            {
              if ('WarningsInfo'.indexOf(Section)>-1)
              {
                return 'Exceptions';
              }
              return Section;
            }

            function getSectionLoadUrl(Section)
            {
              var sectionUrl = '';
              if (Section=="Results")
                sectionUrl += '&IncludeResultsViewNames=1';
              for(i=0; i < sections.length;i++)
              {
                sectionUrl += '&Include' + sections[i];
                if (sections[i] == Section)
                {
                  sectionUrl += '=1';
                }
                else
                {
                  sectionUrl += '=0';
                }
              }
              return sectionUrl;
            }

            function hasSection(SectionList, Section)
            {
              for(i=0; i < SectionList.length;i++)
              {
                if (SectionList[i] == Section)
                {
                  return true;
                }
              }
              return false;
              
            }

                      function selectSubGraph(GraphName, SubGraphId)
                      {
                // Load the Graph page directly.
                var urlBase = '/WsWorkunits/GVCAjaxGraph?Name=' + wid + '&GraphName=' + GraphName + '&SubGraphId=' + SubGraphId;
                document.location.href = urlBase;
                      } 

            var updateSuccess = function(o){
                //o.responseText !== undefined
                alert('Workunit Updated');
            };
             
            var updateFailure = function(o){
                alert('Workunit Update Failed');
            };
             
            var callback =
            {
              success:updateSuccess,
              failure:updateFailure,
              argument:['']
            };

            function updateWorkunit(wuid) {
              var wuformObject = document.getElementById('protect');
              Jobname = document.getElementById('Jobname').value;
              YAHOO.util.Connect.setForm(wuformObject);
              var cObj = YAHOO.util.Connect.asyncRequest('POST', '/WsWorkunits/WUUpdate?Wuid=' + wuid, callback);
            }

            var publishSuccess = function(o){
                var i = o.responseText.indexOf('"MessageText">');
                if (i > -1) {
                   var j = o.responseText.indexOf('<', i+14);
                   alert('Workunit not Published. ' + o.responseText.substring(i+14, j));
                } else {
                   alert('Workunit Published.');
                }
            };
             
            var publishFailure = function(o){
                alert('Publish Failed');
            };
             
            var publishCallback =
            {
              success:publishSuccess,
              failure:publishFailure,
              argument:['']
            };

            function publishWorkunit(wuid) {
              Jobname = document.getElementById('Jobname').value;
              if (Jobname.length==0) {
                alert('The workunit must have a jobname to be published. \r\nEnter a jobname, then publish.');
                return;
              }
              var cObj = YAHOO.util.Connect.asyncRequest('POST', '/WsWorkunits/WUPublishWorkunit?Wuid=' + wuid + '&JobName=' + Jobname + '&Activate=1', publishCallback);
            }

        /*
        <?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope" xmlns:SOAP-ENC="http://schemas.xmlsoap.org/soap/encoding" xmlns="http://webservices.seisint.com/WsWorkunits">
 <soap:Body>
  <WUPublishWorkunitRequest>
   <Wuid>W20110125-150953</Wuid>
   <Activate>1</Activate>
  </WUPublishWorkunitRequest>
 </soap:Body>
</soap:Envelope>
        */

               ]]></xsl:text>
          </script>
            </head>
      <body class="yui-skin-sam" onload="nof5();onLoad()" onunload="onUnload();">
        <input type="hidden" id="OpenSaveOp"/>
        <xsl:text disable-output-escaping="yes"><![CDATA[
      <iframe name="historyFrame" id="historyFrame"  style="display:none; visibility:hidden;"></iframe>
        ]]></xsl:text>
        
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
              <h3>Workunit Details</h3>
            </td>
                        <xsl:if test="number(Workunit/Archived) &lt; 1">
                            <td>
                                <img id="refresh" src="/esp/files/img/refresh.png" onclick="TurnRefresh()" title="Turn on/off Auto Refresh" />
                            </td>
                            <td style="visibility:hidden">
                            </td>
                        </xsl:if>
                    </tr>
                </table>

        <xsl:apply-templates select="Workunit" />

        <xsl:text disable-output-escaping="yes"><![CDATA[
          <iframe id="GlobalFrame" name="GlobalFrame" style="display:none; visibility:hidden;"></iframe>
          <iframe id="ExceptionsFrame" name="ExceptionsFrame" style="display:none; visibility:hidden;"></iframe>
          <iframe id="SourceFilesFrame" name="SourceFilesFrame" style="display:none; visibility:hidden;"></iframe>
          <iframe id="ResultsFrame" name="ResultsFrame" style="display:none; visibility:hidden;"></iframe>
          <iframe id="VariablesFrame" name="VariablesFrame" style="display:none; visibility:hidden;"></iframe>
          <iframe id="DebugValuesFrame" name="DebugValuesFrame" style="display:none; visibility:hidden;"></iframe>
          <iframe id="WorkflowsFrame" name="WorkflowsFrame" style="display:none; visibility:hidden;"></iframe>
          <iframe id="TimersFrame" name="TimersFrame" style="display:none; visibility:hidden;"></iframe>
          <iframe id="GraphsFrame" name="GraphsFrame" style="display:none; visibility:hidden;"></iframe>
          <div id="encodeDiv" name="encodeDiv" style="display:none; visibility:hidden;"></div>
        ]]></xsl:text>


      </body>
        </html>
    </xsl:template>

</xsl:stylesheet>
