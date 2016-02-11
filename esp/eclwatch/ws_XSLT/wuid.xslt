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
  <xsl:variable name="SecMethod" select="WUInfoResponse/SecMethod"/>
  <xsl:include href="/esp/xslt/lib.xslt"/>
  <xsl:include href="/esp/xslt/wuidcommon.xslt"/>

  <xsl:template match="WUInfoResponse">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
            <head>
                <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
                <title><xsl:value-of select="$wuid"/></title>
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/button/assets/skins/sam/button.css" />
        <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/container/assets/skins/sam/container.css" />
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
        <script type="text/javascript" src="/esp/files/yui/build/element/element-min.js"></script>
        <script type="text/javascript" src="/esp/files/yui/build/button/button-min.js"></script>
        <script type="text/javascript" src="/esp/files/yui/build/container/container-min.js"></script>
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
            var thorProcess;
            var thorGroup;
            var thorLogDate;
            var numberOfSlaves;
         
            function CheckSlaveLogInput(e, allows)
            {
                var key, keychar;
                if (window.event)
                {
                    key = window.event.keyCode;
                    keychar = String.fromCharCode(key);
                }
                else if (e)
                {
                    key = e.keyCode;
                    keychar = String.fromCharCode(e.which);
                }
                else
                   return true;

                if (key == 13) //for 'enter' key
                {
                    GetThorSlaveLog();
                    return true;
                }

                if (key ==  8 || key == 37 || key == 39) //8/37/39: backspace/left/right
                   return true;

                if (((allows).indexOf(keychar) > -1))
                   return true;
                else
                   return false;
            }

            function CheckSlaveNum(e)
            {
                if (document.getElementById('NumberSlaves').disabled == 'true')
                    return false;

                return CheckSlaveLogInput(e, '0123456789');
            }

            function CheckSlaveAddress(e)
            {
                if (document.getElementById('SlaveAddress').disabled == 'true')
                    return false;

                return CheckSlaveLogInput(e, '0123456789_.');
            }

            function thorProcessChanged(value)
            {
                pos = value.indexOf('@');
                thorLogDate = value.substring(pos+1);
                numberOfSlaves = parseInt(value.substring(0, pos));
                pos1 = thorLogDate.indexOf('@');
                thorProcess = thorLogDate.substring(pos1+1);
                thorLogDate = thorLogDate.substring(0, pos1);
                pos2 = thorProcess.indexOf('@');
                thorGroup = thorProcess.substring(pos2+1);
                thorProcess = thorProcess.substring(0, pos2);

                var el = document.getElementById('NumberSlaves');
                if (el == undefined)
                    return;

                if (numberOfSlaves == 1)
                {
                    el.innerText = '';
                    document.getElementById('SlaveNum').disabled=true;
                }
                else
                {
                    el.innerText = ' (from 1 to ' + numberOfSlaves + ')';
                    document.getElementById('SlaveNum').disabled = false;
                }
                document.getElementById('SlaveNum').value = '1';
            }

            function GetThorSlaveLog()
            {
                if (document.getElementById('NumberSlaves') != undefined)
                {
                    var slaveNum = parseInt(document.getElementById('SlaveNum').value);
                    if (slaveNum > numberOfSlaves)
                    {
                        alert('Slave Number cannot be greater than ' + numberOfSlaves);
                        return;
                    }

                    getOptions('ThorSlave.log', '/WsWorkunits/WUFile?Wuid='+wid+'&Type=ThorSlaveLog&Process='
                    +thorProcess+'&ClusterGroup='+thorGroup+'&LogDate='+thorLogDate+'&SlaveNumber='+slaveNum, true);
                }
                else
                {
                    var el = document.getElementById('SlaveAddress');
                    if (el.value == '')
                    {
                        alert('Slave address not specified');
                        return;
                    }

                    getOptions('ThorSlave.log', '/WsWorkunits/WUFile?Wuid='+wid+'&Type=ThorSlaveLog&SlaveNumber=0&Process='
                    +document.getElementById('ProcessName').value+'&IPAddress='+el.value+'&LogDate='
                    +document.getElementById('LogDate').value, true);
                }
            }

            // This function gets called when the window has completely loaded.
            // It starts the reload timer with a default time value.
            function onLoad()
            {
                var thorProcessDropDown = document.getElementById('ThorProcess');
                if (thorProcessDropDown == undefined)
                    return;

                thorProcessChanged(thorProcessDropDown.options[thorProcessDropDown.selectedIndex].value);

                UpdateAutoRefresh();
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

           var downloadWnds = new Array();
         
           function getLink(ParentElement, Link)
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
                    activeSections[activeSections.length] = 'Alert';
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
              if ('WarningsInfoAlert'.indexOf(Section)>-1)
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
                var handleOk = function() {
                     this.hide();
                };

                var exceptionHtml = "Workunit successfully published.";
                var headerText = "Workunit Published";
                var hasException = 0;
                var i = o.responseText.indexOf('<Exception>');
                if (i > -1) {
                    hasException = 1;
                    headerText = "Error Publishing Workunit";
                    var j = o.responseText.indexOf('<Message>');
                    if (j > -1) {
                        var k = o.responseText.indexOf('</Message>');
                        if (k > -1) {
                            exceptionHtml = o.responseText.substring(j+9, k);
                        }
                    }
                } else {
                  i = o.responseText.indexOf('<ReloadFailed>1');
                  if (i > -1)
                  {
                    headerText = "Error reloading cluster";
                    exceptionHtml = "Published to Queryset.<br/>But request to update cluster failed.";
                  }
                  else {
                    i = o.responseText.indexOf('<Suspended>1');
                    if (i > -1) {
                        headerText = "Query is suspended";

                        var error = "";
                        var j = o.responseText.indexOf('<ErrorMessage>');
                        if (j > -1) {
                            var k = o.responseText.indexOf('</ErrorMessage>');
                            if (k > j+14) {
                                error = o.responseText.substring(j+14, k);
                            }
                        }

                        if (error.length > 0)
                            exceptionHtml = "Published to Queryset.<br/>Error: " + error;
                        else
                            exceptionHtml = "Published to Queryset.<br/>No error message.";
                    }
                  }
                }
                var publishDialog =
                     new YAHOO.widget.SimpleDialog("publishDialog",
                      { width: hasException ? null : "300px",
                        fixedcenter: true,
                        visible: false,
                        draggable: false,
                        close: true,
                        text: exceptionHtml,
                        icon: (i > -1) ? YAHOO.widget.SimpleDialog.ICON_ALARM : YAHOO.widget.SimpleDialog.ICON_INFO,
                        constraintoviewport: true,
                        buttons: [ { text:"Ok", handler:handleOk, isDefault:true } ]
                      } );

                publishDialog.setHeader(headerText);
                publishDialog.render("publishContainer");
                publishDialog.show();
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
              var cObj = YAHOO.util.Connect.asyncRequest('GET', '/WsWorkunits/WUPublishWorkunit?rawxml_&Wuid=' + wuid + '&JobName=' + Jobname + '&Activate=1&UpdateWorkUnitName=1&Wait=5000', publishCallback);
            }

        /*
        <?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:SOAP-ENC="http://schemas.xmlsoap.org/soap/encoding/" xmlns="http://webservices.seisint.com/WsWorkunits">
 <soap:Body>
  <WUPublishWorkunitRequest>
   <Wuid>W20110125-150953</Wuid>
   <Activate>1</Activate>
  </WUPublishWorkunitRequest>
 </soap:Body>
</soap:Envelope>
        */

                    function popupZAPInfoForm()
                    {
                        mywindow = window.open ("/WsWorkunits/WUGetZAPInfo?WUID="+wid,
                            "mywindow", "location=0,status=1,scrollbars=1,resizable=1,width=800,height=760");
                        if (mywindow.opener == null)
                            mywindow.opener = window;
                        mywindow.focus();
                        return false;
                    }
                    function createZAPInfo(wuid, espIP, thorIP, ESPBuildVersion, problemDesciption, history, timingInfo, password, thorSlaveLog)
                    {
                        document.getElementById("ESPIPAddress").value=espIP;
                        if (thorIP != '')
                            document.getElementById("ThorIPAddress").value=thorIP;
                        document.getElementById("BuildVersion").value=ESPBuildVersion;
                        document.getElementById("IncludeThorSlaveLog").value=thorSlaveLog;
                        if (problemDesciption != '')
                            document.getElementById("ProblemDescription").value=problemDesciption;
                        else
                            document.getElementById("ProblemDescription").value = "";
                        if (history != '')
                            document.getElementById("WhatChanged").value=history;
                        else
                            document.getElementById("WhatChanged").value = "";
                        if (timingInfo != '')
                            document.getElementById("WhereSlow").value=timingInfo;
                        else
                            document.getElementById("WhereSlow").value = "";
                        if (password != '')
                            document.getElementById("Password").value=password;
                        else
                            document.getElementById("Password").value = "";

                        document.forms['protect'].action = "/WsWorkunits/WUCreateZAPInfo";
                        document.forms['protect'].encType="application/x-www-form-urlencoded";
                        document.forms['protect'].submit();
                    }
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

        <div id="publishContainer" />

      </body>
        </html>
    </xsl:template>

</xsl:stylesheet>
