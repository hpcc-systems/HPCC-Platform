/*##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
############################################################################## */

/*global YAHOO*/
(function() {
  var loader = new YAHOO.util.YUILoader();

  loader.insert({
    require: ["reset-fonts-grids", "base", "datatable", "selector", "progressbar"],
    base: '/esp/files/yui/build/',
    loadOptional: true,
    filter: "MIN",
    allowRollup: true,
    onSuccess: function() {
      var invokeWiz = true;

      YAHOO.util.Event.onContentReady("envctrls", function() {
        function fnce() {
          var oPushButton3 = new YAHOO.widget.Button("validatebutton", { onclick: { fn: validateEnvironment} });
          var oPushButton2 = new YAHOO.widget.Button("savebutton", { onclick: { fn: saveEnvironment} });
          var oPushButton6 = new YAHOO.widget.Button("saveasbutton", { onclick: { fn: saveEnvironmentAs} });
          var oPushButton4 = new YAHOO.widget.Button("openbutton", { onclick: { fn: displayOpenEnvDialog} });
          var oPushButton5 = new YAHOO.widget.Button("wizardbutton", { onclick: { fn: invokeWizard} });

          var form = document.forms['treeForm'];
          updateEnvCtrls(form.isLocked.value === "true");
        }

        versionOperation(fnce, fnce);
      });

      top.document.startWait = function(doc) { top.document.body.style.cursor = "wait"; if (doc) doc.body.style.cursor = "wait"; }
      top.document.stopWait = function(doc) { top.document.body.style.cursor = "auto"; if (doc) doc.body.style.cursor = "auto"; }

      if (top.window.location.search.length > 0) {
        var filename = top.window.location.search.split(/=/g);
        document.forms['treeForm'].sourcefile.value = decodeURI(filename[1]);
        invokeWiz = false;
      }
      document.getElementById('top1').style.display = 'none';

      if (typeof(addEventListener) != 'undefined')
      {
        document.getElementById('top1').addEventListener("click", function() {
          if (top.document.ContextMenuCenter != null)
            top.document.ContextMenuCenter.clearContent();
          if (top.document.ContextMenuLeft != null)
            top.document.ContextMenuLeft.clearContent() } );
      }
      else
      {
        document.getElementById('top1').attachEvent('onclick',function() {
          if (top.document.ContextMenuCenter != null)
            top.document.ContextMenuCenter.clearContent();
          if (top.document.ContextMenuLeft != null)
            top.document.ContextMenuLeft.clearContent() } );
      }

      getWaitDlg().show();
      var params = "queryType=customType::params=environment,laststarted,defenvfile,username,wizops";

      if (!invokeWiz)
        params += ",lastsaved";

      YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/GetValue', {
        success: function(o) {
          getWaitDlg().hide();
          document.getElementById('top1').style.display = 'none';
          if (o.responseText.indexOf("<?xml") === 0) {
            var xml = o.responseText.split(/<ReqValue>/g);
            var xml1 = xml[1].split(/<\/ReqValue>/g);
            var form = document.forms['treeForm'];

            if (xml1.length > 0) {
              var arrayXml = xml1[0].split(",");
              for (var j = 0; j < arrayXml.length; j++) {
                var keyValue = arrayXml[j].split("=");
                var key = keyValue[0];
                var value = keyValue[1];
                if (key === 'environment') {
                  if (value === 'true')
                    form.foundEnvironment.value = "true";
                }
                else if (key == "laststarted")
                  form.lastStarted.value = value;
                else if (key == "lastSaved")
                  form.lastSaved.value = value;
                else if (key == "defenvfile")
                  form.defenvfile.value = value;
                else if (key == "username")
                  form.userid.value = value;
                else if (key == "wizops")
                  form.wizops.value = value;
              }

              if (invokeWiz)
                invokeWizard();
              else
                handleAdvance();
            }
          }
          else if (o.responseText.indexOf("<html") === 0) {
            var temp = o.responseText.split(/td align=\"left\">/g);
            var temp1 = temp[1].split(/<\/td>/g);
            alert(temp1[0]);
            var loc = window.location.href.split(/\?/g);
            var newwin = top.open(loc[0], "_self");
          }
        },
        failure: function(o) {
          getWaitDlg().hide();
          alert(o.statusText);
        },
        scope: this
      },
      getFileName(true) + 'Params=' + params);
    }
  });
})();

function getModeSelected() {
if(document.getElementById('advButton').checked)
   return '2';
 else if(document.getElementById('wizButton').checked)
   return '1';
 else
   return '0'; 
} 

function invokeWizard() {
  var handleCancelForDisplayScreen = function() {
    document.forms['treeForm'].mode.value = prevMode;
    top.document.displayModeDialog1.hide();
    if(!top.document.navDT && document.forms['treeForm'].sumparams.value !== '1'){
      top.document.layout.render();
      if (document.getElementById('ReadWrite') && !top.document.navDT)
      {
        document.getElementById('ReadWrite').disabled = true;
        document.getElementById('savebutton').disabled = true;
        document.getElementById('validatebutton').disabled = true;
      }
      document.getElementById('top1').style.display = 'block';
      if( document.forms['treeForm'].wizops != '3')
        getMessagePanel("Please select wizard to create an environment or select open environment to view environment summary.").show();
      else
        getMessagePanel("Please select wizard or open an environment to create/edit environment.").show();
    }
  }
  var prevMode = document.forms['treeForm'].mode.value;
  document.forms['treeForm'].mode.value = getModeSelected();

  var handleNext = function() {
    top.document.keepAliveInt = setInterval(keepAlive, 10000);
    var radioButtons = document.getElementsByName("radiobutton");
    for (var x = 0; x < radioButtons.length; x++) {
      if (radioButtons[x].checked) {
        if (document.forms['treeForm'].mode.value !== radioButtons[x].value)
          document.forms['treeForm'].mode.value = radioButtons[x].value;
      }
    }

    if (document.forms['treeForm'].mode.value === '1' || document.forms['treeForm'].mode.value === '0') {
      loadAndCheckFileNames(document.forms['treeForm'].mode.value);
    }
    else if (document.forms['treeForm'].mode.value === '2') {
      var pattern = /Select/;
      var val = document.getElementById('fileDropDownMenu').value;
      if (val.length == 0 || val.match(pattern) != null)
        alert("Please select the file to be opened");
      else {
        document.forms['treeForm'].sourcefile.value = val;
        var loc = window.location.href.split(/\?/g);
        var newwin = top.open(loc[0] + "?sourcefile=" + val, "_self");
      }
    }
    else if (document.forms['treeForm'].mode.value === '4') {
      var pattern = /Select/;
      var val = document.getElementById('sumDropDownMenu').value;
      if (val.length == 0 || val.match(pattern) != null)
        alert("Please select the file to be opened");
      else {
        document.forms['treeForm'].wizfile.value = val;
        this.hide();
        getSummaryPage();
      }
    }
  };

  if (!top.document.displayModeDialog1) {
    top.document.displayModeDialog1 = new YAHOO.widget.Dialog('displayModeDialog',
                                       {
                                         width: '500px',
                                         resizable: true,
                                         fixedcenter: true,
                                         visible: false,
                                         constraintoviewport: true,
                                         draggable: true,
                                         modal: true,
                                         close: false,
                                         zindex: 9999,
                                         buttons: [{ text: "Cancel", handler: handleCancelForDisplayScreen },
                                                    { text: "Next", handler: handleNext, isDefault: true}]
                                       });

    document.getElementById('displayModeDialog').style.display = 'block';
    top.document.displayModeDialog1.renderEvent.subscribe(function(){
         loadAndCheckFileNames('4');
         enableCurrentOption(4);
    });
    top.document.displayModeDialog1.render();
    top.document.displayModeDialog1.center();
    updateWizCtrls();
    top.document.displayModeDialog1.show();
    top.document.displayModeDialog1.cancelEvent.subscribe(function() {
      document.getElementById('top1').style.display = 'block';
    });
  }
  else {
    updateWizCtrls();
    loadAndCheckFileNames('4');
    top.document.displayModeDialog1.show();
  }
}

function handleAdvance(createFile) {
  if(document.forms['treeForm'].wizops.value !='3') {
     getSummaryPage();
     return;
  }
  
  if (top.document.displayModeDialog1)
    top.document.displayModeDialog1.hide();
  
 var xmlArgs = 'reloadEnv=true::lockEnv=false';
  if (createFile)
    xmlArgs += '::createFile=true';

  YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/GetNavTreeDefn', {
    success: function(o) {
      if (o.responseText.indexOf("<?xml") === 0) {
        resetHiddenVars();
        var form = document.forms['treeForm'];
        form.mode.value = '2';
        var lSaved = o.responseText.split(/<LastSaved>/g);
        if (lSaved.length > 1) {
          var lSaved1 = lSaved[1].split(/<\/LastSaved>/g);
          if (lSaved1[0].charAt(0) != '<')
            form.lastSaved.value = lSaved1[0];
        }

        var lStarted = o.responseText.split(/<LastStarted>/g);
        if (lStarted.length > 1) {
          var lStarted1 = lStarted[1].split(/<\/LastStarted>/g);
          if (lStarted1[0].charAt(0) != '<')
            form.lastStarted.value = lStarted1[0];
        }

        var temp = o.responseText.split(/<CompDefn>/g);
        var temp1 = temp[1].split(/<\/CompDefn>/g);

        eval(temp1[0]);
        var treeData = getNavTreeData();
        createNavigationTree(treeData);
        top.document.stopWait();
        top.document.title = 'HPCC Systems Configuration Manager - ' + form.sourcefile.value;

        if (top.document.navDT.keepAliveInt)
          clearInterval(top.document.navDT.keepAliveInt);
        top.document.navDT.keepAliveInt = setInterval(keepAlive, 10000);
      }
      else if (o.responseText.indexOf("<html") === 0) {
        var temp = o.responseText.split(/td align=\"left\">/g);
        var temp1 = temp[1].split(/<\/td>/g);
        alert(temp1[0]);
      }
    },
    failure: function(o) {
      top.document.stopWait();
      alert(o.statusText);
    },
    scope: this
  },
  getFileName(true) + 'XmlArgs=' + xmlArgs);
}

function getFileName(flag, wiz) {
  var str;
  if (wiz)
    str = "ReqInfo.FileName=" + document.forms['treeForm'].wizfile.value;
  else
    str = "ReqInfo.FileName=" + document.forms['treeForm'].sourcefile.value;

  str += '&ReqInfo.UserId=' + document.forms['treeForm'].userid.value;
  
  if (flag)
    str += "&";

  return str;
}

function keepAlive() {
  YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/ClientAlive',
  {
    success: function(o) {
      var RefreshClient = o.responseText.split(/<RefreshClient>/g);
      if (RefreshClient.length <= 1)
        return;
      var RefreshClient1 = RefreshClient[1].split(/<\/RefreshClient>/g);
      var LastSaved = o.responseText.split(/<LastSaved>/g);
      var LastSaved1 = LastSaved[1].split(/<\/LastSaved>/g);
      var LastStarted = o.responseText.split(/<LastStarted>/g);
      var LastStarted1 = LastStarted[1].split(/<\/LastStarted>/g);

      var form = document.forms['treeForm'];
      var msg = "";

      if ((LastStarted1[0].charAt(0) != '<' && form.lastStarted.value.length && (form.lastStarted.value != LastStarted1[0]))) {
        msg = "ConfigMgr has been restarted. Press Ok to reload the Environment.";
        var prevmode = document.forms['treeForm'].mode.value;
        if (prevmode !== '2')
          displayWizardFirstScreen();
        updateEnvCtrls(false);
        resetHiddenVars();
        document.forms['treeForm'].mode.value = prevmode;
      }
      else if ( document.getElementById('ReadWrite').checked == false &&
                form.saveInProgress.value !== "true" &&
                (RefreshClient1[0] === 'true' || (LastSaved1[0].charAt(0) != '<' && form.lastSaved.value.length && (form.lastSaved.value != LastSaved1[0]))))
      {
        if (document.forms['treeForm'].mode.value === '2')
          msg = "Environment has been updated by another user. Press Ok to reload the Environment.";
        form.lastSaved.value = LastSaved1[0];
      }
      if (msg.length > 0  && form.isChanged.value === "false")
      {
        refresh(msg);
      }
    },
    failure: function(o) {
    },
    scope: this
  },
  getFileName(false));
}

function createNavigationTree(navTreeData) {
  top.document.layout.render();
  document.getElementById('top1').style.display = 'block';
  document.getElementById('center1').style.display = 'block';
  document.getElementById('left1').style.display = 'block';

  var Dom = YAHOO.util.Dom,
            Lang = YAHOO.lang;

  var hasChildren = {};
  navTreeData[0].DisplayName += ' - ' + document.forms['treeForm'].sourcefile.value;
  var navDS = new YAHOO.util.DataSource(navTreeData);
  navDS.responseType = YAHOO.util.DataSource.TYPE_JSARRAY;
  navDS.responseSchema = { fields: ["Name", "DisplayName", "Build", "BuildSet", "parent", "id", "depth", "menu", "Params", "CompType"] };

  var navDT = new YAHOO.widget.ScrollingDataTable(
            'pageBody',
            [
                { key: "DisplayName", label: "Name", width: 325, maxAutoWidth: 325, formatter: function(el, oRecord, oColumn, oData) {
                  el.innerHTML = "<div id='depth" + oRecord.getData('depth') + "'>" + oData + "</div>";
                  Dom.addClass(el, 'yui-dt-liner depth' + oRecord.getData('depth'));
                }, scrollable: true, resizeable: true
                },
                { key: "BuildSet", width: 125, resizeable: true, hidden: true}
            ],
      navDS,
            {
              formatRow: function(row, record) {
                var prnt = record.getData('parent');
                if (prnt !== -1) {
                  Dom.addClass(row, 'hidden');
                }
                else //reset if we are starting or refreshing just the table
                  hasChildren = {};
                hasChildren[prnt] = true;
                return true;
              }, width: "100%"
            }
        );

    top.document.navDT = navDT;

    top.document.navDT.expandRecord = function(id) {
        var recSet = top.document.navDT.getRecordSet();

      if (typeof (id) === 'undefined') {
        var tdEl = top.document.navDT.getFirstTdEl(recSet.getRecord(0));
        var children = Dom.getChildren(tdEl);
        if (Dom.hasClass(children[0].children[0].children[0], 'yui-button')) {
          children[0].children[0].children[0].click();
          return;
        }
      }

      var recSetLen = recSet.getLength();
      for (var i = 0; i < recSetLen; i++) {
        var r = recSet.getRecord(i);
        if (r.getData('id') === id) {
          var tdEl = top.document.navDT.getFirstTdEl(r);
          var children = Dom.getChildren(tdEl);
          if (Dom.hasClass(children[0].children[0].children[0], 'yui-button') &&
                Dom.hasClass(children[0].children[0].children[0], 'buttoncollapsed')) {
            children[0].children[0].children[0].click();
          }
          else {
            top.document.navDT.unselectAllRows();
            top.document.navDT.selectRow(r);
          }
          break;
        }
        }
      }

    navDT.ds = navDS;
  navDT.navTreeData = navTreeData;
  navDT.subscribe("rowMouseoverEvent", navDT.onEventHighlightRow);
  navDT.subscribe("rowMouseoutEvent", navDT.onEventUnhighlightRow);
  navDT.subscribe("rowClickEvent", navDT.onEventSelectRow);
  navDT.selectionToXML = function(targetRec, selectedRows, parentRec, tagName) {
    var xmlStr;

    if (typeof (tagName) !== 'undefined')
      xmlStr = "<" + tagName + ">";
    else
      xmlStr = "<XmlArgs>";
    var parentName;

    if (typeof (targetRec) !== 'undefined') {
      xmlStr += "<Component name=\"" + targetRec.getData('Name');
      xmlStr += "\" compType=\"" + targetRec.getData('CompType');
      xmlStr += "\" build=\"" + targetRec.getData('Build');

      //if (targetRec.getData('parent') > 3)
      var parentName = 'Environment';

      if (targetRec.getData('parent') >= 0)
        parentName = navDT.navTreeData[targetRec.getData('parent')]['Name'];

      if (parentName !== 'Software' && parentName !== 'Environment') {
        //if (parentName  !== 'Environment')
        parentName = navDT.navTreeData[navDT.navTreeData[targetRec.getData('parent')]['parent']]['Name'];
        //else
        //parentName = navDT.navTreeData[targetRec.getData('parent')]['Name'];
      }

      xmlStr += "\" parent=\"" + parentName + "\"/>";
    }

    if (typeof (selectedRows) !== 'undefined') {
      for (var i = 0; i < selectedRows.length; i++) {
        if (navDT.getRecord(selectedRows[i]).getData('Name') == "")
          continue;
        if (navDT.getRecord(selectedRows[i]).getData('CompType') == 'Directories')
          continue;

        xmlStr += "<Component name=\"" + navDT.getRecord(selectedRows[i]).getData('Name');
        xmlStr += "\" compType=\"" + navDT.getRecord(selectedRows[i]).getData('CompType');
        xmlStr += "\" build=\"" + navDT.getRecord(selectedRows[i]).getData('Build');

        if (navDT.getRecord(selectedRows[i]).getData('parent') > 2)
          parentName = navDT.navTreeData[navDT.navTreeData[navDT.getRecord(selectedRows[i]).getData('parent')]['parent']]['Name'];
        else
          parentName = navDT.navTreeData[navDT.getRecord(selectedRows[i]).getData('parent')]['Name'];

        xmlStr += "\" parent=\"" + parentName + "\"/>";
      }
    }

    if (typeof (tagName) !== 'undefined')
      xmlStr += "</" + tagName + ">";
    else
      xmlStr += "</XmlArgs>";

    return xmlStr;
  };

  navDT.subscribe('renderEvent', function() {
    var recSet = this.getRecordSet();
    var recSetLen = recSet.getLength();
    for (var i = 0; i < recSetLen; i++) {
      var r = recSet.getRecord(i);
      if (hasChildren[r.getData('id')]) {
        var tdEl = this.getFirstTdEl(r);
        var divEl = Dom.getChildren(tdEl);
        var children = Dom.getChildren(divEl[0]);
        children[0].id = "depth" + (r.getData('depth') - 1);
        var inner = children[0].innerHTML;
        children[0].innerHTML = "<button type='button' class='yui-button buttoncollapsed' id='pushbutton' name='button'></button>" + inner;
        Dom.addClass(this.getTrEl(r), 'collapsed');
      }
    }

    var prevSelRec = top.document.lastSelectedRow;
    var prevTab = top.document.activeTab;

    expandRecord(this, "Name", "Environment");
    expandRecord(this, "Name", "Software");

    if (top.document.ResetFocus == true)
    {
      top.document.ResetFocus = false;
      top.document.navDT.clickCurrentSelOrNameAndCompType(top.document.navDT,top.document.ResetFocusValueName, top.document.ResetFocusCompType);
    }

    top.document.activeTab = prevTab;
    if (typeof (prevSelRec) !== "undefined")
      clickCurrentSelOrName(this, prevSelRec);
    else {
      var firstSWRec = getFirstNodeName(this, "Software",'');
      if (typeof (firstSWRec) !== 'undefined')
        clickCurrentSelOrName(this, firstSWRec.getData('Name'));
    }

    top.document.stopWait();

    var lastCounter2 = 0;

    if (top.document.navDT.doJumpToChoice == true)
    {
      for (counter = top.document.navDT.choice.length-1; counter >= 0; counter--)
      {
        for (counter2 = lastCounter2; true; counter2++)
        {
          if (top.document.navDT.getRecord(counter2).getData('CompType') == top.document.navDT.choice[counter] ||
              top.document.navDT.getRecord(counter2).getData('BuildSet') == top.document.navDT.choice[counter])
          {
             top.document.navDT.expandRecord(counter2);
             lastCounter2 = counter2;
             break;
          }
        }
      }

      window.scrollTo(0, document.body.scrollHeight);
      top.document.navDT.doJumpToChoice = false;
    }
  });

  var clickFn = function(oArgs) {
    setFocusToNavTable();
    var target = oArgs.target;
    var record = navDT.getRecord(target);
    var compName = record.getData('Name');
    if (oArgs.event.button === 2 && (compName === 'Environment' || compName === 'Software'))
      return;

    var curSel = navDT.getSelectedRows();
    for (i = 0; i < curSel.length; i++) {
      var rec = navDT.getRecord(curSel[i]);
      if (YAHOO.util.Dom.hasClass(navDT.getTrEl(rec), 'outoffocus'))
        YAHOO.util.Dom.removeClass(navDT.getTrEl(rec), 'outoffocus');
    }

    if (curSel.length > 0) {
      if (record.getData('id') !== navDT.getRecord(curSel[0]).getData('id')) {
        top.document.activeTab = 0;
        top.document.navDT.lastSelIndex = 0;
      }
      else if (hasChildren[record.getData('id')] !== true)
        return;
    }

    navDT.clearTextSelection();
    if (oArgs.event.button === 2 && navDT.getSelectedRows().length == 1)
      navDT.onEventSelectRow(oArgs);

    var selectedRows;

    if (oArgs.event.ctrlKey || oArgs.event.shiftKey) {
      selectedRows = navDT.getSelectedRows();
      if (record.getData('id') <= 2)
        return false;

      if (typeof (selectedRows) !== 'undefined') {
        if (selectedRows.length > 0) {
          if (navDT.getRecord(selectedRows[0]).getData('id') <= 2)
            return false;
        }
      }
    }

   var eventTarget = YAHOO.util.Event.getTarget(oArgs.event);
    if (eventTarget.id.indexOf("pushbutton") === -1) {
      top.document.lastSelectedRow = record.getData('Name');
      if(record.getData('Name') === ''){
        top.document.keyEventOccur = "false";
        if(hasChildren[record.getData('id')]){
          if(record.getData('expanded'))
            return;
        }
      }
      else{
        top.document.startWait();

        if (compName === "Environment" && document.forms['treeForm'].wizops.value === '3')
          getWaitDlg().show();
          
        if (oArgs.event.shiftKey == false && oArgs.event.ctrlKey == false)
          document.getElementById('center1frame').src = '/WsDeploy/DisplaySettings?Cmd=Select&' + getFileName(true) + 'XmlArgs=' + navDT.selectionToXML(record, selectedRows);
        if (top.document.lastSelectedRow !== 'Hardware' && record.getData('BuildSet') === '')
          top.document.stopWait();
        return;
    }
   }

    var parentId = record.getData('id');
    var recSet = navDT.getRecordSet();
    var recSetLen = recSet.getLength();
    var self = navDT;
    var visibility = function(parentId, visible) {
      var count = 0;
      for (var i = 0; i < recSetLen; i++) {
        var r = recSet.getRecord(i);
        if (r.getData('parent') === parentId) {
          if (visible) {
            Dom.removeClass(self.getTrEl(r), 'hidden');
          } else {
            Dom.addClass(self.getTrEl(r), 'hidden');
          }
          count += visibility(r.getData('id'), visible && r.getData('expanded'));
          count++;
        }
      }
      return count;
    };
    record.setData('expanded', !record.getData('expanded'));
    if (visibility(parentId, record.getData('expanded'))) {
      var tdEl = navDT.getFirstTdEl(record);
      var divEl = Dom.getChildren(tdEl);
      var children = Dom.getChildren(divEl[0]);
      var innerChild = children[0].children[0];
      if (record.getData('expanded')) {
        Dom.addClass(navDT.getTrEl(record), 'expanded');
        Dom.removeClass(navDT.getTrEl(record), 'collapsed');
        Dom.addClass(innerChild, 'buttonexpanded');
        Dom.removeClass(innerChild, 'buttoncollapsed');
      } else {
        Dom.addClass(navDT.getTrEl(record), 'collapsed');
        Dom.removeClass(navDT.getTrEl(record), 'expanded');
        Dom.addClass(innerChild, 'buttoncollapsed');
        Dom.removeClass(innerChild, 'buttonexpanded');
      }
    }

    top.document.RightTabView = null;
    top.document.lastSelectedRow = record.getData('Name');
    top.document.startWait();
    document.getElementById('center1frame').src = '/WsDeploy/DisplaySettings?Cmd=Select&' + getFileName(true) + 'XmlArgs=' + navDT.selectionToXML(record, selectedRows);
  };

  var selFn = function(oArgs) {
    var record = oArgs.record;
    var compName = record.getData('Name');

    var curSel = navDT.getSelectedRows();
    if (curSel.length > 0) {
      if (compName !== top.document.lastSelectedRow) {
        top.document.activeTab = 0;
        top.document.navDT.lastSelIndex = 0;
      }
      else if (hasChildren[record.getData('id')] !== true)
        return;
    }
    if( (compName === '' || compName === 'Software' || compName === 'Environment') && top.document.keyEventOccur === "false"){
      top.document.keyEventOccur = "true";
      if(record.getData('expanded')){
        if(hasChildren[record.getData('id')]){
          expandRecord(this,'DisplayName',record.getData("DisplayName"));
        }
        var firstRec = getFirstNodeName(this,record.getData("DisplayName"),'DisplayName');
        if (typeof (firstRec) !== 'undefined')
         clickCurrentSelOrName(this, firstRec.getData('Name'));
      }
    }
    else{   
      navDT.clearTextSelection();
      var selectedRows;
      top.document.lastSelectedRow = record.getData('Name');
      top.document.RightTabView = null;
      top.document.startWait();
      document.getElementById('center1frame').src = '/WsDeploy/DisplaySettings?Cmd=Select&' + getFileName(true) + 'XmlArgs=' + navDT.selectionToXML(record, selectedRows);
      if (top.document.lastSelectedRow !== 'Hardware' && record.getData('BuildSet') === '')
        top.document.stopWait();
    }
    return;
  };

  navDT.subscribe("cellClickEvent", clickFn);
  navDT.subscribe("rowSelectEvent", selFn);
  navDT.subscribe("cellMousedownEvent", function(oArgs) {
    //on left click, the cellClickEvent is fired, taking care of the behaviour
    //on right click, we use cellMousedownEvent to select the row, as well as cause the page to display on the right
    if (oArgs.event.button === 1 || oArgs.event.button === 0)
      return;
    else
      clickFn(oArgs)
  });

  navDT.subscribe("tableFocusEvent", function(oArgs) {
    var Dom = YAHOO.util.Dom;
    var selRows = this.getSelectedRows();
    for (var idx = 0; idx < selRows.length; idx++) {
      Dom.removeClass(this.getTrEl(selRows[idx]), 'outoffocus');
    }
  });
  navDT.subscribe("tableBlurEvent", function(oArgs) {
    var Dom = YAHOO.util.Dom;
    var selRows = this.getSelectedRows();
    for (var idx = 0; idx < selRows.length; idx++) {
      Dom.addClass(this.getTrEl(selRows[idx]), 'outoffocus');
    }
  });


  Dom.removeClass(navDT.getTrEl(0), 'hidden');
  Dom.removeClass(navDT.getTrEl(0), 'collapsed');
  Dom.addClass(navDT.getTrEl(0), 'expanded');


  //Add context menu's
  function onMenuSWClick(p_sType, p_aArgs, p_oValue) {
    var menuItemName = this.cfg.getProperty("text");

    if (menuItemName === 'New Esp Services')
      return;

    getWaitDlg().show();

    if (menuItemName === "Delete Component/Service") {
      var targetRec; //for undefined
      var xmlStr = navDT.selectionToXML(targetRec, navDT.getSelectedRows(), targetRec, "Components");
      YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/HandleComponent', {
        success: function(o) {
          if (o.status === 200) {
            top.document.navDT.choice = new Array();
            top.document.navDT.choice[0] = top.document.navDT.getRecordIndex(top.document.navDT.getSelectedRows()[0]);

            var recDepth = top.document.navDT.getRecord(top.document.navDT.choice[0])._oData.depth;
            var index = 0;

            for (counter = top.document.navDT.choice[0]; counter >= 0; counter--)
            {
              if (top.document.navDT.getRecord(counter)._oData.depth < recDepth)
              {
                top.document.navDT.choice[index] = top.document.navDT.getRecord(counter).getData('CompType');
                recDepth = top.document.navDT.getRecord(counter)._oData.depth;
                index++;
               }
             }

            top.document.navDT.doJumpToChoice = true;

            if (o.responseText.indexOf("<?xml") === 0) {
              var form = document.forms['treeForm'];
              var status = o.responseText.split(/<Status>/g);
              var status1 = status[1].split(/<\/Status>/g);
              getWaitDlg().hide();
              if (status1[0] !== 'true') {
                alert(status1[0]);

                var temp = o.responseText.split(/<CompName>/g);
                var temp1 = temp[1].split(/<\/CompName>/g);
                navDS.flushCache();
                form.isChanged.value = "true";
                refreshNavTree(navDS, navDT, temp1[0])

                return;
              }
              else {
                form.isChanged.value = "true";

                var temp = o.responseText.split(/<CompName>/g);
                var temp1 = temp[1].split(/<\/CompName>/g);
                top.document.lastSelectedRow = temp1[0];
                getWaitDlg().hide();
                navDS.flushCache();
                refreshNavTree(navDS, navDT, temp1[0])
              }
            }
            else if (o.responseText.indexOf("<html") === 0) {
              var temp = o.responseText.split(/td align=\"left\">/g);
              var temp1 = temp[1].split(/<\/td>/g);
              getWaitDlg().hide();
              alert(temp1[0]);
            }
          }
        },
        failure: function(o) {
          getWaitDlg().hide();
          alert(o.statusText);
        },
        scope: this
      },
        getFileName(true) + 'Operation=Delete&XmlArgs=' + xmlStr);
    }
    else if (menuItemName ==="Duplicate Component/Service")
    {
      var targetRec;
      xmlStr = navDT.selectionToXML(targetRec, navDT.getSelectedRows(), targetRec, "Components");

      YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/HandleComponent', {
        success: function(o) {
          if (o.status === 200) {
            if (o.responseText.indexOf("<?xml") === 0) {
              var form = document.forms['treeForm'];
              form.isChanged.value = "true";
              var temp = o.responseText.split(/<CompName>/g);
              var temp1 = temp[1].split(/<\/CompName>/g);
              top.document.lastSelectRow = top.document.navDT.getRecord(top.document.navDT.getRecordIndex(top.document.navDT.getSelectedRows()[0])).getData('CompType');
              getWaitDlg().hide();
              navDS.flushCache();
              refreshNavTree(navDS, navDT);

            }
            else if (o.responseText.indexOf("<html") === 0) {
              var temp = o.responseText.split(/td align=\"left\">/g);
              var temp1 = temp[1].split(/<\/td>/g);
              getWaitDlg().hide();
              alert(temp1[0]);
            }
          }
        },
        failure: function(o) {
          getWaitDlg().hide();
          alert(o.statusText);
        },
        scope: this
      },
        getFileName(true) + 'Operation=Duplicate&XmlArgs=' + xmlStr);
    }
    else if (p_oValue.parent.id ==="HWCopy" || p_oValue.parent.id === "SWCopy")
    {
      copyHWSWTo(menuItemName, (p_oValue.parent.id === "HWCopy" ? true : false) );
    }
    else {
      var xmlStr = "<Components><Component buildSet='" + menuItemName + "'/></Components>";
      YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/HandleComponent', {
        success: function(o) {
          if (o.status === 200) {
            if (o.responseText.indexOf("<?xml") === 0) {
              var form = document.forms['treeForm'];
              form.isChanged.value = "true";
              var temp = o.responseText.split(/<CompName>/g);
              var temp1 = temp[1].split(/<\/CompName>/g);
              top.document.lastSelectedRow = temp1[0];
              getWaitDlg().hide();
              navDS.flushCache();
              refreshNavTree(navDS, navDT);
            }
            else if (o.responseText.indexOf("<html") === 0) {
              var temp = o.responseText.split(/td align=\"left\">/g);
              var temp1 = temp[1].split(/<\/td>/g);
              getWaitDlg().hide();
              alert(temp1[0]);
            }
          }
        },
        failure: function(o) {
          getWaitDlg().hide();
          alert(o.statusText);
        },
        scope: this
      },
         getFileName(true) + 'Operation=Add&XmlArgs=' + xmlStr);
    }
  }

  function onMenuItemClick(p_sType, p_aArgs, p_oValue) {
    var menuItemName = this.cfg.getProperty("text");
    getWaitDlg().show();
    if (menuItemName === 'Lock Environment')
      lockEnvironment();
    else if (menuItemName === 'Unlock Environment')
      saveAndUnlockEnv();
    else if (menuItemName === 'Save Environment')
      saveEnvironment();
    else if (menuItemName === 'Save Environment As') {
      saveEnvironmentAs();
    }
    else if (menuItemName == 'Copy Hardware To')
      copyHWSWTo(menuItemName, true);
    else if (menuItemName === 'Validate Environment')
      validateEnvironment();
    else if (menuItemName === 'Deploy...') {
      if (!navDT.deployPanel) {
        navDT.deployPanel = new YAHOO.widget.Dialog("deployPanel", { width: "700px",
          height: "400px",
          resizable: true,
          fixedcenter: true,
          close: true,
          draggable: true,
          zindex: 9999,
          modal: true,
          visible: false,
          constraintoviewport: true
        }
                                              );

        document.getElementById('selectDeployFrame').src = '/WsDeploy/GetDeployableComps?Cmd=Deploy&XmlArgs=';

        var handleStartDeploy = function() {
          if (!navDT.progressPanel) {
            navDT.progressPanel = new YAHOO.widget.Panel("progressBarPanel", {
              width: "240px",
              fixedcenter: true,
              close: false,
              draggable: false,
              zindex: 4,
              modal: true,
              visible: false
            });

            navDT.progressBar = new YAHOO.widget.ProgressBar({ value: 1 }).render("progressBarDiv");
          }

          document.getElementById('progressBarPanel').style.display = 'block';
          navDT.progressPanel.render(document.body);
          navDT.progressPanel.show();
          var val = 2;
          var intervalId = window.setInterval(function() {
            if (val < 0) {
              val++;
            }
            else {
              if (val == 0) {
                navDT.progressPanel.show();
                val = 1;
              } else if (val == 100) {
                val = -4;
                navDT.progressPanel.hide();
              } else {
                val += 2 * Math.random();
                val = Math.min(val, 100);
              }
              navDT.progressBar.set('value', val);
            }
          }, 500);

          var selComps = top.document.forms['treeForm'].compsToBeDeployed.value;
          var tmp1 = document.getElementById('compareRadio').checked ? "Cmp" : "Dep";
          var options = "Options.Compare=" + document.getElementById('compareRadio').checked + "&" +
                  "Options.ConfigFiles=" + document.getElementById("configFiles" + tmp1).checked + "&" +
                  "Options.BuildFiles=" + document.getElementById("buildFiles" + tmp1).checked + "&" +
                  "Options.UpgradeBuildFiles=" + document.getElementById("buildFilesIfChanged").checked + "&" +
                  "Options.Start=" + document.getElementById("startComponents").checked + "&" +
                  "Options.Stop=" + document.getElementById("stopComponents").checked + "&" +
                  "Options.BackupRename=" + document.getElementById("renameBkupRadio").checked + "&" +
                  "Options.BackupCopy=" + document.getElementById("copyBkupRadio").checked + "&" +
                  "Options.ArchiveEnv=" + document.getElementById("archiveEnv").checked + "&" +
                  "Options.Log=" + document.getElementById("genLogFile").checked + "&" +
                  "Options.ArchivePath=" + document.getElementById("archivePath").value;

          YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/StartDeployment', {
            success: function(o) {
              navDT.progressPanel.hide();
              window.clearInterval(intervalId);
              if (o.status === 200) {
                alert(o.responseText);
                if (o.responseText.indexOf("<?xml") === 0) {
                  var temp = o.responseText.split(/<SelectedComponents>/g);
                  if (temp[0] !== o.responseText)
                    alert('Deployment succesful');
                  else
                    alert('Deployment failed');
                }
                else if (o.responseText.indexOf("<html") === 0) {
                  var temp = o.responseText.split(/td align=\"left\">/g);
                  var temp1 = temp[1].split(/<\/td>/g);
                  alert(temp1[0]);
                }
              } else {
                alert(r.replyText);
              }
            },
            failure: function(o) {
              navDT.progressPanel.hide();
              alert(o.statusText);
            },
            scope: this
          },
                getFileName(true) + 'SelComps=' + selComps + '&' + options);
        }

        var handleBackForSubmitDeploy = function() {
          this.hide();
          navDT.optionsPanel.show();
        }

        var handleSubmitForOptions = function() {
          this.hide();

          if (!navDT.submitPanel) {
            navDT.submitPanel = new YAHOO.widget.Dialog("submitDeployPanel",
                                    { width: "700px",
                                      height: "400px",
                                      resizable: true,
                                      fixedcenter: true,
                                      close: true,
                                      draggable: true,
                                      zindex: 9999,
                                      modal: true,
                                      visible: false,
                                      constraintoviewport: true
                                    }
                                );
          }

          var mySubmitButtons = [{ text: "Back", handler: handleBackForSubmitDeploy },
                                        { text: "Start", handler: handleStartDeploy, isDefault: true },
                                        { text: "Finish", handler: handleCancel}];
          navDT.submitPanel.setHeader("Deploy");
          document.getElementById('submitDeployPanel').style.display = 'block';
          navDT.submitPanel.cfg.queueProperty("buttons", mySubmitButtons);
          navDT.submitPanel.render();
          navDT.submitPanel.show();
        }

        var handleCancel = function() {
          this.cancel();
        }

        var handleBackForOptions = function() {
          this.hide();
          navDT.deployPanel.show();
        }

        var handleSubmit = function() {
          this.hide();

          if (!navDT.optionsPanel) {
            navDT.optionsPanel = new YAHOO.widget.Dialog("optionsPanel",
                                  { width: "700px",
                                    height: "420px",
                                    resizable: true,
                                    fixedcenter: true,
                                    close: true,
                                    draggable: true,
                                    zindex: 9999,
                                    modal: true,
                                    visible: false,
                                    constraintoviewport: true
                                  }
                              );
          }

          var myoptionsButtons = [{ text: "Back", handler: handleBackForOptions },
                                         { text: "Next", handler: handleSubmitForOptions, isDefault: true },
                                         { text: "Cancel", handler: handleCancel}];
          navDT.optionsPanel.setHeader("Deploy Options");
          document.getElementById('optionsPanel').style.display = 'block';
          navDT.optionsPanel.cfg.queueProperty("buttons", myoptionsButtons);
          navDT.optionsPanel.render();
          navDT.optionsPanel.show();
        }

        var myButtons = [{ text: "Next", handler: handleSubmit, isDefault: true },
                                { text: "Cancel", handler: handleCancel}];
        navDT.deployPanel.cfg.queueProperty("buttons", myButtons);
        navDT.deployPanel.setHeader("Deploy");

        var resize = new YAHOO.util.Resize("deployPanel", {
          autoRatio: false,
          minWidth: 300,
          minHeight: 100,
          status: false
        });

        resize.on("startResize", function(args) {

          if (this.cfg.getProperty("constraintoviewport")) {
            var D = YAHOO.util.Dom;
            var clientRegion = D.getClientRegion();
            var elRegion = D.getRegion(this.element);

            resize.set("maxWidth", clientRegion.right - elRegion.left - YAHOO.widget.Overlay.VIEWPORT_OFFSET);
            resize.set("maxHeight", clientRegion.bottom - elRegion.top - YAHOO.widget.Overlay.VIEWPORT_OFFSET);
          }
          else {
            resize.set("maxWidth", null);
            resize.set("maxHeight", null);
          }
        }, navDT.deployPanel, true);

        resize.on("resize", function(args) {
          if (args.height > 0) {
            var panelHeight = args.height;
            this.cfg.setProperty("height", panelHeight + "px");
          }
        }, navDT.deployPanel, true);
      }

      document.getElementById('deployPanel').style.display = 'block';
      getWaitDlg().hide();
      navDT.deployPanel.render(document.body);
      navDT.deployPanel.show();
    }
    else if (menuItemName === 'Import Build...') {
      getWaitDlg().hide();
      if (!navDT.importBuildPanel) {
        var handleSubmit = function() {
          var selRow = left2.buildServerDirsTable.getSelectedRows();
          var bldName = left2.buildServerDirsTable.getRecord(selRow[0]).getData('name');
          var selComps = "<BuildSets name=\"" + bldName + "\" path=\"" + document.getElementById("buildServer").value + bldName + "\">";
          this.hide();
          var recSet = left2.selComponentsTable.getRecordSet();
          var recSetLen = recSet.getLength();
          var self = left2.selComponentsTable;
          for (var i = 0; i < recSetLen; i++) {
            var r = recSet.getRecord(i);
            var elem = self.getTrEl(r);
            var child = elem.children[0];
            if (child.children[0].children[0].checked)
              selComps += "<BuildSet name=\"" + r.getData('name') + "\" path=\"" + r.getData('path') + "\"/>";
          }

          selComps += "</BuildSets>";

          var url = '/WsDeploy/ImportBuild?BuildSets=' + selComps;
          url.replace("\\", "\\\\");

          YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/ImportBuild', {
            success: function(o) {
              alert(o.responseText);
              clickCurrentSelOrName(navDT, "Name");
            },
            failure: function(o) {
              alert(o.statusText);
            },
            scope: this
          },
             getFileName(true) + 'BuildSets=' + selComps);
        }

        var handleCancel = function() {
          this.hide();
        }

        navDT.importBuildPanel = new YAHOO.widget.Dialog("importBuildPanel",
                                              { width: "700px",
                                                height: "400px",
                                                resizable: true,
                                                fixedcenter: true,
                                                close: true,
                                                draggable: true,
                                                //zindex:9999,
                                                modal: true,
                                                visible: false,
                                                underlay: 'none',
                                                constraintoviewport: true
                                              }
                                          );
        var myButtons = [{ text: "Ok", handler: handleSubmit, isDefault: true },
                               { text: "Cancel", handler: handleCancel}];
        navDT.importBuildPanel.cfg.queueProperty("buttons", myButtons);

        navDT.importBuildPanel.setHeader("Import Build");
        navDT.importBuildPanel.renderEvent.subscribe(function() {
          navDT.importBuildPanel.layout = new YAHOO.widget.Layout('buildLayout', {
            height: (navDT.importBuildPanel.body.offsetHeight - 20),
            units: [{ position: 'top', height: 50, body: 'top2' },
                          { position: 'left', /*header: 'Directories',*/width: 450, resize: true, body: 'left2', gutter: '2px', /*collapse: true, collapseSize: 20,*/scroll: true },
                          { position: 'center', /*header: 'Components',*/body: 'center2', gutter: '2px', resize: true, scroll: true }
                         ]
          });
          navDT.importBuildPanel.layout.render();
          document.getElementById('refreshButton').click();
        });
      }

      document.getElementById('importBuildPanel').style.display = 'block';
      navDT.importBuildPanel.render(document.body);
      navDT.importBuildPanel.show();

    }
  }

  top.document.copyCompMenu = new Array();
  top.document.copyCompMenu2 = new Array();
  var fnDeleteComps = function(){
  var params = "queryType=sourceEnvironments";

  YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/GetValue', {
    success: function(o) {
        if (o.responseText.indexOf("<?xml") === 0) {
      var tmp = o.responseText.split(/<ReqValue>/g);
      var tmp1;
      if (tmp.length > 1) {
        tmp1 = tmp[1].split(/<\/ReqValue>/g);
        if (tmp1.length > 1)
          result = tmp1[0];
        else
          result = '';
        }
        var files = result.split(/;/g);
        for (var i = 0; i < files.length; i++) {
          if( files[i]  ==  "<StagedConfiguration>" || files[i] == "</StagedConfiguration>" || files[i] == "")
            {
               continue;
            }
           top.document.copyCompMenu[i] = { text: files[i], onclick: { fn: onMenuSWClick} };
           top.document.copyCompMenu2[i] = { text: files[i], onclick: { fn: onMenuSWClick} };
          }
        }
      },
      failure: function(o) {
      },
      scope: this
    },
  getFileName(true) + 'Params=' + params);
  }
  fnDeleteComps();

  var compMenu = new Array();
  for (i = 0; i < navDT.navTreeData[0]["menuComps"].length; i++)
    compMenu[i] = { text: navDT.navTreeData[0]["menuComps"][i], onclick: { fn: onMenuSWClick} };

  var espServiceMenu = new Array();
  for (i = 0; i < navDT.navTreeData[0]["menuEspServices"].length; i++)
    espServiceMenu[i] = { text: navDT.navTreeData[0]["menuEspServices"][i], onclick: { fn: onMenuSWClick} };

  var oContextMenuItems = {
    "Environment": [{text: "Save Environment", onclick: { fn: onMenuItemClick } },
                    {text: "Save Environment As", onclick: { fn: onMenuItemClick } },
                    {text: "Validate Environment", onclick: { fn: onMenuItemClick } },
                    {text: "Copy Hardware To",
                      submenu: {
                           id: "HWCopy",
                           lazyload: true,
                           itemdata: top.document.copyCompMenu,
                           onclick: { fn: onMenuItemClick }
                          } }
                          ],
    "Hardware": [
                              "New",
                              "New Range..."
                          ],
    "Programs": [
                              {
                                text: "New",
                                submenu: {
                                  id: "ProgramsNew",
                                  lazyload: true,
                                  itemdata: [
                                                  "New Build",
                                                  "New BuildSet"
                                              ]
                                }
                              },
                              { text: "Import Build...", onclick: { fn: onMenuItemClick} },
                              "Reimport Build...",
                              "Purge Build..."
                          ],

    "Software": [
                              { text: "New Components",
                                submenu: {
                                  id: "SWNewComps",
                                  lazyload: true,
                                  itemdata: compMenu
                                }

                              },
                              { text: "New Esp Services",
                                submenu: {
                                  id: "SWNewEspServices",
                                  lazyload: true,
                                  itemdata: espServiceMenu
                                },
                                onclick: { fn: onMenuSWClick }
                              },
                              { text: "Delete Component/Service", onclick: { fn: onMenuSWClick} },
                              { text: "Duplicate Component/Service", onclick: { fn: onMenuSWClick} },
                              { text: "Copy Component/Service To",
                                submenu: {
                                  id: "SWCopy",
                                  lazyload: true,
                                  itemdata: top.document.copyCompMenu
                                 }
                              }
                          ],
    "Columns": [
                              {
                                text: "View/Hide Columns",
                                submenu: {
                                  id: "ColumnsShowHide",
                                  lazyload: true,
                                  itemdata: [
                                                  { text: "Name", disabled: true, checked: true },
                                                  { text: "Build", checked: true, onclick: { fn: onShowHideCol, obj: "Build"} },
                                                  { text: "BuildSet", checked: ((!navDT.getColumn("BuildSet").hidden)), onclick: { fn: function() {
                                                    if (navDT.getColumn("BuildSet").hidden)
                                                      navDT.showColumn("BuildSet");
                                                    else
                                                      navDT.hideColumn("BuildSet");
                                                  } }
                                                  }
                              ]
                                }
                              }
                          ]
  };


  var oSelectedTR;
  function onShowHideCol(p_sType, p_aArgs, key) {
    var itemIndex = (key === 'Build') ? 1 : 2;
    if (navDT.getColumn(key).hidden)
      navDT.showColumn(key);
    else
      navDT.hideColumn(key);
  }

  function onContextMenuBeforeShow(p_sType, p_aArgs) {
    top.document.navDT._elBdContainer.scrollTop = top.document.navigatorScrollOffset;
    var oTarget = this.contextEventTarget,
          aMenuItems,
            aClasses;


    if (this.getRoot() === this) {
      oSelectedTR = oTarget.nodeName.toUpperCase() === "TR" ?
                                    oTarget : Dom.getAncestorByTagName(oTarget, "TR");
      var recSet = navDT.getRecordSet();
      var record = recSet.getRecord(oSelectedTR.id);
      var parentName;

      if (record.getData('id') === 0)
          parentName = "Environment";
      else {
        if (record.getData('Name') === 'Software')
          parentName = "Software";
        else if (record.getData('parent') > 2)
          parentName = navDT.navTreeData[navDT.navTreeData[record.getData('parent')]['parent']]['Name'];
        else
        //parentName = record.getData('Name');
          parentName = navDT.navTreeData[record.getData('parent')]['Name'];
      }

      aMenuItems = oContextMenuItems[parentName];
      this.clearContent();

      if (typeof (aMenuItems) === 'undefined' || aMenuItems.length === 0)
        return false;

      this.addItems(aMenuItems);
      top.document.ContextMenuLeft = oContextMenu;

      if (record.getData('Name') === 'Software' || record.getData('Name') === 'Directories'){
        this.getItem(2).cfg.setProperty("disabled", true);
        this.getItem(3).cfg.setProperty("disabled", true);
        this.getItem(4).cfg.setProperty("disabled", true);
      }

      if (record.getData('id') === 0) {
        this.getItem(1).cfg.setProperty("disabled", false);
        var form = document.forms['treeForm'];
        if (form.isLocked.value === 'true') {
          this.getItem(0).cfg.setProperty("disabled", false);
          this.getItem(2).cfg.setProperty("disabled", false);
        }
        else {
          this.getItem(0).cfg.setProperty("disabled", true);
          this.getItem(2).cfg.setProperty("disabled", true);
        }
      }
      else if (top.document.forms['treeForm'].isLocked.value === 'false') {
        var groups = this.getItemGroups();
        for (iGroup = 0; iGroup < groups.length; iGroup++) {
          if (typeof (groups[iGroup]) !== 'undefined')
            for (i = 0; i < groups[iGroup].length; i++)
            if (groups[iGroup][i].element.innerText != "Save Environment As")
              groups[iGroup][i].cfg.setProperty("disabled", true);
        }
      }

      //Do not delete
      /*this.addItems(oContextMenuItems["Columns"], 2);
      var menuItems = this.getItems();
      var subMenuItems = menuItems[menuItems.length-1].cfg.getProperty("submenu").itemData;
      subMenuItems[1].checked = (!navDT.getColumn("Build").hidden);
      subMenuItems[2].checked = (!navDT.getColumn("BuildSet").hidden);*/
      // Render the ContextMenu instance with the new content
      this.render();
      if (top.document.ContextMenuCenter != null)
        top.document.ContextMenuCenter.clearContent();
    }
  }

  var oContextMenu = new YAHOO.widget.ContextMenu("contextmenu", { trigger: "pageBody", lazyload: true });
  oContextMenu.subscribe("beforeShow", onContextMenuBeforeShow);
  navDT.displayRoxieClusterAddFarm = displayAddFarmDlg;
  navDT.displayRoxieClusterReplaceServer = displayReplaceServerDlg;
  navDT.doPageRefresh = refresh;
  navDT.clickCurrentSelOrName = clickCurrentSelOrName;
  navDT.clickCurrentSelOrNameAndCompType = clickCurrentSelOrNameAndCompType;
  navDT.displayAddInstance = displayAddInstanceDlg;
  navDT.promptVerifyPwd = promptVerifyPwd;
  navDT.promptNewRange = promptNewRange;
  navDT.updateRecordName = updateRecordName;
  navDT.promptSlaveConfig = promptSlaveConfig;
  navDT.selectRecordAndClick = selectRecordAndClick;
  navDT.promptThorTopology = promptThorTopology;
  navDT.createDDRows = createDDRows;
  navDT.getWaitDlg = getWaitDlg;
  navDT.getFileName = getFileName;
  navDT.tt = new YAHOO.widget.Tooltip("navTabletooltip");
  navDT.subscribe("cellMouseoverEvent", function(oArgs) {
    var rec = navDT.getRecord(oArgs.target);
    var xy = [parseInt(oArgs.event.clientX, 10) + 10, parseInt(oArgs.event.clientY, 10) + 10];
    var params = "queryType=multiple::category=Software::compName=" + rec.getData('Name') + "::subType=Instance::attrName=netAddress";

    navDT.tt.doNotShow = false;
    YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/GetValue', {
      success: function(o) {
        if (o.responseText.indexOf("<?xml") === 0) {
          var tmp = o.responseText.split(/<ReqValue>/g);
          var tmp1, instances;
          
          if (tmp.length > 1) {
            tmp1 = tmp[1].split(/<\/ReqValue>/g);
            if (tmp1.length > 1)
              instances = tmp1[0];
            else
              instances = '';

            if (navDT.showTimer) {
              window.clearTimeout(navDT.showTimer);
              navDT.showTimer = 0;
            }

            if (instances.length) {
              navDT.showTimer = window.setTimeout(function() {
                navDT.tt.setBody("Instance(s): " + (instances.length > 100 ? (instances.substring(0,instances.substring(100,instances.length).indexOf(',')+100) + ",...") : instances));
                navDT.tt.cfg.setProperty('xy', xy);
                if (navDT.tt.doNotShow != true)
                  navDT.tt.show();
                navDT.hideTimer = window.setTimeout(function() {
                  navDT.tt.hide();
                }, 5000);
              }, 500);
            }
          }
        }
      },
      failure: function(o) {
      },
      scope: this
    },
      getFileName(true) + 'Params=' + params);

  });

  navDT.subscribe("cellMouseoutEvent", function(oArgs) {
    navDT.tt.doNotShow = true;
    if (navDT.showTimer) {
      window.clearTimeout(navDT.showTimer);
      navDT.showTimer = 0;
    }
    if (navDT.hideTimer) {
      window.clearTimeout(navDT.hideTimer);
      navDT.hideTimer = 0;
    }
    navDT.tt.hide();
  });
  
  navDT.subscribe("tableKeyEvent", function(oArgs) {
    if (oArgs.event.keyCode === 9) {
      var tabView = top.document.RightTabView;
      if (tabView) {
        var actTab = tabView.get("activeTab");
        actTab.dt.focus();
        this.fireEvent("tableBlurEvent");
      }
    }
    else if (oArgs.event.keyCode === 39 || oArgs.event.keyCode === 37) {
      var rec = navDT.getRecord(navDT.getSelectedRows()[0]);
      if (rec)
        expandRecord(navDT, "DisplayName", rec.getData("DisplayName"));
    }
    else
      handleComplexTableKeyDown(oArgs, navDT);
  });


  var handleWindowMouseDown = function(e) {
    if (top.document.ContextMenuCenter != null && (!YAHOO.env.ua.ie || top.document.ContextMenuCenter.itemData != undefined))
      top.document.ContextMenuCenter.clearContent();
    var tabView = top.document.RightTabView;
    if (tabView) {
      var actTab = tabView.get("activeTab");
      saveOpenEditors(actTab.dt);
    }
  }

  document.onmousedown = handleWindowMouseDown;

  var handleKeyDown = function(event) {

    if (!event)
      event = window.event;

    if (event.keyCode === 192 && event.ctrlKey === true)
      setFocusToNavTable();

  }

  document.onkeydown = handleKeyDown;

}

function enableOptions(isCompare) {
  document.getElementById("configFilesCmp").disabled = !isCompare;
  document.getElementById("buildFilesCmp").disabled = !isCompare;
  document.getElementById("buildFilesDep").disabled = isCompare;
  document.getElementById("configFilesDep").disabled = isCompare;
  document.getElementById("stopComponents").disabled = isCompare;
  document.getElementById("buildFilesIfChanged").disabled = isCompare;
  document.getElementById("startComponents").disabled = isCompare;
  document.getElementById("archiveEnv").disabled = isCompare;
  document.getElementById("genLogFile").disabled = isCompare;
  document.getElementById("backupDirs").disabled = isCompare;

  if (isCompare) {
    document.getElementById("renameBkupRadio").disabled = isCompare;
    document.getElementById("copyBkupRadio").disabled = isCompare;
  }
  else {
    document.getElementById("renameBkupRadio").disabled = !document.getElementById("backupDirs").checked;
    document.getElementById("copyBkupRadio").disabled = !document.getElementById("backupDirs").checked;
  }
}

function getDirectories() {
  var dir = document.getElementById("buildServer").value;

  YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/GetBuildServerDirs',
      {
        success: function(o) {
          if (!left2.buildServerDirsDataSource) {
            var myBSColumnDefs = [{ key: "name", label: "Build" },
                                  { key: "modified", label: "Modified"}];
            var xmlStr = '<?xml version="1.0" encoding="UTF-8"?><Directory name="" modified=""/>';
            var myBSDataSource = new YAHOO.util.DataSource(xmlStr);
            myBSDataSource.responseType = YAHOO.util.DataSource.TYPE_XML;
            myBSDataSource.responseSchema = { resultNode: "Directory", fields: ["name", "modified"] };
            var myBSDataTable = new YAHOO.widget.DataTable("left2", myBSColumnDefs,
                                                            myBSDataSource,
                                                            { width: "100%", initialLoad: false, resize: true });

            myBSDataTable.subscribe("rowClickEvent", myBSDataTable.onEventSelectRow);
            myBSDataTable.subscribe("cellDblclickEvent", function(oArgs) {
              myBSDataTable.clearTextSelection();
              var target = oArgs.target;
              var record = this.getRecord(target);
              var dir = document.getElementById("buildServer").value;
              dir += '\\' + record.getData('name');
              document.getElementById("buildServer").value = dir;
              document.getElementById('refreshButton').click();
            });
            myBSDataTable.subscribe("cellClickEvent", function(oArgs) {
              myBSDataTable.clearTextSelection();
              var target = oArgs.target;
              var record = this.getRecord(target);
              var selectedRows;
              var recSet = this.getRecordSet();

              YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/GetBuildServerDirs', {
                success: function(o) {
                  var myCompsColumnDefs = [{ key: "name", label: "Components", formatter: function(el, oRecord, oColumn, oData) {
                    el.innerHTML = "<input type=\"checkbox\" class=\"yui-checkbox\" id=\"" + oData + "\" name=\"button\" checked></input>" + oData;
                  } }];
                    var xmlStr = '';
                    var myCompsDataSource = new YAHOO.util.DataSource(xmlStr);
                    myCompsDataSource.responseType = YAHOO.util.DataSource.TYPE_XML;
                    myCompsDataSource.responseSchema = { resultNode: "Comp", fields: ["name", "path"] };
                    var myCompsDataTable = new YAHOO.widget.DataTable("center2", myCompsColumnDefs,
                                                                    myCompsDataSource,
                                                                    { width: "100%", initialLoad: false, resize: true, scrollable: true });

                    myCompsDataSource.handleResponse("", o, { success: myCompsDataTable.onDataReturnInitializeTable,
                      scope: myCompsDataTable
                    }, this, 999);
                    left2.selComponentsTable = myCompsDataTable;
                  },
                  failure: function(o) {
                    alert(o.statusText);
                  },
                  scope: this
                },
            getFileName(true) + 'Cmd=Release&XmlArgs=' + dir + '\\' + record.getData('name'));
              });

              left2.buildServerDirsTable = myBSDataTable;
              left2.buildServerDirsDataSource = myBSDataSource;
            }

            left2.buildServerDirsDataSource.handleResponse("", o, { success: left2.buildServerDirsTable.onDataReturnInitializeTable,
              scope: left2.buildServerDirsTable
            }, this, 999);

          },
          failure: function(o) {
            alert(o.statusText);
          },
          scope: this
        },
    getFileName(true) + 'Cmd=SubDirs&XmlArgs=' + dir);
}

function askUserToSave(navtable, dounlock) {
  var handleNo = function() {
    if (dounlock)
      unlockEnvironment(navtable, false);


    this.hide();
  }

  var handleCancel = function() {
    updateEnvCtrls(true);
    this.hide();
  }

  var handleSave = function() {
    if (dounlock)
      unlockEnvironment(navtable, true);
    else
      saveEnvironment();


    this.hide();
  }

  var msg = dounlock ? "The environment has been changed. Do you want to save the changes?" : "Do you want to save the changes?";

  //var r=confirm(msg);
  //if (r==true)
  //  handleSave();
  //else
  //  handleCancel();

  if (!navtable.promptSavePanel) {
    //    var handleCancel = function() {
    //    unlockEnvironment(navDT, false);
    //    clickCurrentSelOrName(navDT);
    //                            this.hide();
    //    }
    //    
    //    var handleSave = function() {
    //      unlockEnvironment(navDT, true);
    //      clickCurrentSelOrName(navDT);
    //      this.hide();
    //    }

    navtable.promptSavePanel = new YAHOO.widget.Dialog("promptSavePanel",
                            { width: "300px",
                              height: "125px",
                              resizable: true,
                              fixedcenter: true,
                              close: true,
                              draggable: true,
                              //zindex:9999,
                              modal: true,
                              visible: false,
                              underlay: 'none',
                              constraintoviewport: true
                            }
                        );
    var myButtons = [{ text: "Yes", handler: handleSave, isDefault: true },
                        { text: "No", handler: handleNo },
                        { text: "Cancel", handler: handleCancel}];
    navtable.promptSavePanel.cfg.queueProperty("buttons", myButtons);
    navtable.promptSavePanel.setHeader("ConfigMgr");
  }

  navtable.promptSavePanel.setBody(msg);
  document.getElementById('promptSavePanel').style.display = 'block';
  navtable.promptSavePanel.render(document.body);
  navtable.promptSavePanel.show();
}

function lockEnvironment() {
  YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/NavMenuEvent', {
    success: function(o) {
      if (o.status === 200) {
        if (o.responseText.indexOf("<?xml") === 0) {
          var temp = o.responseText.split(/<XmlArgs>/g);
          var isErr = false;
          if (temp.length > 0) {
            var temp1 = temp[1].split(/<\/XmlArgs>/g);
            if (temp1.length > 0 && temp1[0].length > 0 && temp1[0].charAt(0) != '<') {
              isErr = true;
              updateEnvCtrls(false);
              alert(temp1[0]);
            }
          }

          if (!isErr) {
            YAHOO.util.Dom.addClass(top.document.navDT.getTrEl(0), 'envlocked');
            var form = document.forms['treeForm'];
            form.isLocked.value = "true";
            updateEnvCtrls(true);

            var LastSaved = o.responseText.split(/<LastSaved>/g);
            var LastSaved1 = LastSaved[1].split(/<\/LastSaved>/g);

            if (LastSaved1[0].charAt(0) != '<' && form.lastSaved.value !== LastSaved1[0]) {
              form.lastSaved.value = LastSaved1[0];
              refresh();
            }
          }

          getWaitDlg().hide();
        }
        else if (o.responseText.indexOf("<html") === 0) {
          updateEnvCtrls(false);
          var temp = o.responseText.split(/td align=\"left\">/g);
          var temp1 = temp[1].split(/<\/td>/g);
          getWaitDlg().hide();
          alert(temp1[0]);
        }
      }
      else {
        updateEnvCtrls(false);
        getWaitDlg().hide();
        alert(r.replyText);
      }
    },
    failure: function(o) {
      updateEnvCtrls(false);
      getWaitDlg().hide();
      alert(o.statusText);
    },
    scope: this
  },
  getFileName(true) + 'Cmd=LockEnvironment');
}

function unlockEnvironment(navtable, saveEnv) {
  var Dom = YAHOO.util.Dom;
  var xmlArgs = '';
  if (saveEnv)
    xmlArgs = "<XmlArgs><SaveEnv%20flag=%27true%27/></XmlArgs>";

  YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/NavMenuEvent', {
    success: function(o) {
      getWaitDlg().hide();
      if (o.status === 200) {
        if (o.responseText.indexOf("<?xml") === 0) {
          var temp = o.responseText.split(/<XmlArgs>/g);
          var isErr = false;
          if (temp.length > 0) {
            var temp1 = temp[1].split(/<\/XmlArgs>/g);
            if (temp1.length > 0 && temp1[0].length > 0) {
              if (temp1[0].indexOf("<Warning>") === 0) {
                var warning = o.responseText.split(/<Warning>/g);
                if (warning.length > 0) {
                  var warning1 = warning[1].split(/<\/Warning>/g);
                  if (warning1.length > 0 && warning1[0].length > 0 && warning1[0].charAt(0) != '<')
                  {
                    alert(warning1[0]);
                    var err = o.responseText.split(/<\/Warning>/g);
                    if (err.length > 1) {
                      var err1 = err[1].split(/<\/XmlArgs>/g);
                      if (err1.length > 0 && err1[0].length > 0 && err1[0].charAt(0) != '<') {
                        isErr = true;
                        updateEnvCtrls(true);
                        alert(err1[0]);
                      }
                    }
                  }
                }
              }
              else if (temp1[0].charAt(0) != '<')
              {
                isErr = true;
                updateEnvCtrls(true);
                alert(temp1[0]);
              }
            }
          }

          if (!isErr) {
            var form = document.forms['treeForm'];
            var LastSaved = o.responseText.split(/<LastSaved>/g);
            var LastSaved1 = LastSaved[1].split(/<\/LastSaved>/g);
            var Refresh = "false";

            if (LastSaved1[0].charAt(0) != '<' && form.lastSaved.value !== LastSaved1[0])
            {
              Refresh = "true";
              form.lastSaved.value = LastSaved1[0];
            }

            Dom.removeClass(navtable.getTrEl(0), 'envlocked');
            form.isLocked.value = "false";
            var changed = form.isChanged.value;
            form.isChanged.value = "false";
            updateEnvCtrls(false);

            if (saveEnv || changed === "true" || Refresh === "true")
            {
              refresh();
            }
          }
        }
        else if (o.responseText.indexOf("<html") === 0) {
          updateEnvCtrls(true);
          var temp = o.responseText.split(/td align=\"left\">/g);
          var temp1 = temp[1].split(/<\/td>/g);
          alert(temp1[0]);
        }

      } else {
        updateEnvCtrls(true);
        alert(r.replyText);
      }
    },
    failure: function(o) {
      updateEnvCtrls(true);
      getWaitDlg().hide();
      alert(o.statusText);
    },
    scope: navtable
  },
  getFileName(true) + 'Cmd=UnlockEnvironment&XmlArgs=' + xmlArgs);
}

function roxieSelectionToXML(table, type, parentName, selectedRows, compName) {
  var xmlStr = "<RoxieData type=\"" + type + "\" parentName=\"" + parentName + "\" roxieName=\"" + compName + "\" >";

  if (typeof (selectedRows) !== 'undefined') {
    for (var i = 0; i < selectedRows.length; i++) {
      xmlStr += "<Component name=\"" + table.getRecord(selectedRows[i]).getData('name') + "\"/>";
    }
  }

  xmlStr += "</RoxieData>";

  return xmlStr;
}

function displayAddFarmDlg(self, farmName) {
  var tmpdt = self;
  var handleSubmit = function() {
    top.document.startWait();
    var compName = tmpdt.getRecord(tmpdt.getSelectedRows()[0]).getData('Name');
    var xmlStr = "<RoxieData roxieName='" + compName + "'></RoxieData>";

    YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/HandleRoxieOperation', {
      success: function(o) {
        var form = top.window.document.forms['treeForm'];
        form.isChanged.value = "true";
        top.document.stopWait();
        clickCurrentSelOrName(tmpdt);
      },
      failure: function(o) {
        top.document.stopWait();
        alert(o.statusText);
      },
      scope: this
    },
          getFileName(true) + 'Cmd=AddRoxieFarm&XmlArgs=' + xmlStr);
  }

  initSelectComputersPanel(tmpdt, handleSubmit);

  tmpdt.selectComputersPanel.currentFarmName = farmName;
  handleSubmit();
}

function displayReplaceServerDlg() {
  initReplaceRoxieNodesPanel();
  document.getElementById('roxieReplaceNodePanel').style.display = 'block';
  top.document.navDT.roxieNodesReplacePanel.render(document.body);
  top.document.navDT.roxieNodesReplacePanel.center();
  top.document.navDT.roxieNodesReplacePanel.show();
}

function setSingleSelectionModeForInstances() {
  if (selectComputersDTDiv.selectComputersTable)
    selectComputersDTDiv.selectComputersTable.set("selectionMode", "single");
  else
    setTimeout("setSingleSelectionModeForInstances()", 250);
}

function promptSaveChanges() {
  var form = document.forms['treeForm'];
  if (form.isChanged.value === "true")
    return "You will lose your changes if you press OK.\nTo save your changes, press Cancel and select 'Save Environment' command.";
  else
    return "";
  //  else if (form.isLocked.value === "true")
  //  {
  //    alert("No changes have been made. Unlocking the environment...");
  //    unlockEnvironment(top.window.document.body.dt, false);
  //  }
}


function saveEnvironment(saveas) {
  getWaitDlg().show();
  var form = document.forms['treeForm'];
  form.saveInProgress.value = "true";
  var args = 'Cmd=SaveEnvironment';
  if (typeof(saveas) === 'string') {
    args = 'Cmd=SaveEnvironmentAs&XmlArgs=<XmlArgs envSaveAs="' + saveas + '"/>';
  }
  YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/NavMenuEvent', {
    success: function(o) {
      getWaitDlg().hide();
      if (o.status === 200) {
        if (o.responseText.indexOf("<?xml") === 0) {
          var temp = o.responseText.split(/<Command>/g);
          var temp1 = temp[1].split(/<\/Command>/g);
          if (temp1[0] === 'SaveEnvironment') {
            YAHOO.util.Dom.addClass(top.document.navDT.getTrEl(0), 'envlocked');
            form.isLocked.value = "true";
            form.isChanged.value = "false";
          }
          else if (temp1[0] === 'SaveEnvironmentAs') {
            form.isChanged.value = "false";
            var loc = window.location.href.split(/\?/g);
            var newwin = top.open(loc[0] + "?sourcefile=" + document.getElementById('saveAsFileName').value, "_self");
          }
          var LastSaved = o.responseText.split(/<LastSaved>/g);
          var LastSaved1 = LastSaved[1].split(/<\/LastSaved>/g);

          if (LastSaved1[0].charAt(0) != '<')
            form.lastSaved.value = LastSaved1[0];

          var xmlargs = o.responseText.split(/<XmlArgs>/g);
          if (xmlargs.length > 0) {
            var xmlargs1 = xmlargs[1].split(/<\/XmlArgs>/g);
            if (xmlargs1.length > 0 && xmlargs1[0].length > 0 && xmlargs1[0].charAt(0) != '<') {
              alert(xmlargs1[0]);
            }
          }

          refresh();
          form.saveInProgress.value = "false";
        }
        else if (o.responseText.indexOf("<html") === 0) {
          var temp = o.responseText.split(/td align=\"left\">/g);
          var temp1 = temp[1].split(/<\/td>/g);
          alert(temp1[0]);
        }
      }
      else {
        getWaitDlg().hide();
        alert(r.replyText);
      }
    },
    failure: function(o) {
      getWaitDlg().hide();
      alert(o.statusText);
    },
    scope: this
  },
  getFileName(true) + args);
}

copyHWSWTo = function (menuItemName, IsHW, HW_XPath)
{
  var targetRec;
  var xmlStr = "";
  var xmlStr2 = "";
  var idx;

  menuItemName = menuItemName.replace(/\n/g,"");

  if (IsHW === true)
  {
    if (HW_XPath == "" || typeof(HW_XPath) == 'undefined')
     xmlStr2 = "<Components> <Component name=\"Hardware\" target=\"" + menuItemName + "\"/> " + "</Components>";
    else
     xmlStr2 = "<Components> <Component name=\"Hardware\" target=\"" + menuItemName + "\" hwxpath=\"" + HW_XPath + "\"/> " + "</Components>";
  }
  else // software
  {
    xmlStr = top.document.navDT.selectionToXML(targetRec, top.document.navDT.getSelectedRows(), targetRec, "Components");
    idx = xmlStr.indexOf("build");
    xmlStr2 = xmlStr.substr(0,idx-1) + " target=\"" + menuItemName + "\" " + xmlStr.substr(idx,xmlStr.length);
  }

  YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/HandleComponent', {
    success: function(o)
    {
      if (o.status === 200)
      {
        if (o.responseText.indexOf("<?xml") === 0)
        {
          var form = document.forms['treeForm'];
          form.isChanged.value = "true";
          var temp = o.responseText.split(/<CompName>/g);
          var temp1 = temp[1].split(/<\/CompName>/g);
          top.document.lastSelectedRow = temp1[0];
          getWaitDlg().hide();
        }
        else if (o.responseText.indexOf("<html") === 0)
        {
          var temp = o.responseText.split(/td align=\"left\">/g);
          var temp1 = temp[1].split(/<\/td>/g);
          getWaitDlg().hide();
          alert(temp1[0]);
        }
      }
    },
    failure: function(o) {
      getWaitDlg().hide();
      alert(o.statusText);
    },
    scope: this
  },
  getFileName(true) + 'Operation=Copy' + (IsHW ? 'HW' : 'SW') + '&XmlArgs='  + xmlStr2);
}

top.document.copyHWSWTo = copyHWSWTo;

function saveEnvironmentAs() {
  getWaitDlg().show();

  var handleCancel = function() {
    getWaitDlg().hide();
    top.document.envSaveAsDialog.hide();
  }

  var handleOk = function() {
    getWaitDlg().hide();
    loadAndCheckFileNames('3');
  };

  var clickSave = function() {
    top.document.envSaveAsDialog.cfg.queueProperty("keylisteners", null);
    var btns = top.document.envSaveAsDialog.cfg.getProperty("buttons");
    for (var bIdx = 0; bIdx < btns.length; bIdx++) {
      if (btns[bIdx].text === 'Ok') {
        YAHOO.util.UserAction.click(btns[bIdx].htmlButton);
        return false;
      }
    }
  }

  if (!top.document.envSaveAsDialog) {
    top.document.envSaveAsDialog = new YAHOO.widget.Dialog('envSaveAsDialog',
                                       {
                                         resizable: true,
                                         fixedcenter: true,
                                         visible: false,
                                         constraintoviewport: true,
                                         draggable: true,
                                         modal: true,
                                         close: false,
                                         zindex: 9999,
                                         buttons: [{ text: "Cancel", handler: handleCancel },
                                                   { text: "Ok", handler: handleOk, isDefault: true}]
                                       });

    var kl = new YAHOO.util.KeyListener(document, { keys: 13 }, { fn: clickSave,
                                                                  scope: top.document.envSaveAsDialog,
                                                                  correctScope: true});
    top.document.envSaveAsDialog.cfg.queueProperty("keylisteners", kl);
    
    document.getElementById('envSaveAsDialog').style.display = 'block';
    top.document.envSaveAsDialog.render();
    top.document.envSaveAsDialog.center();
    top.document.envSaveAsDialog.show();
  }
  else
    top.document.envSaveAsDialog.show();

}

function validateEnvironment() {
  getWaitDlg().show();
  YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/NavMenuEvent', {
    success: function(o) {
    getWaitDlg().hide();
      if (o.status === 200) {
        if (o.responseText.indexOf("<html") === 0) {
          var temp = o.responseText.split(/td align=\"left\">/g);
          var temp1 = temp[1].split(/<\/td>/g);
          promptValidationErrs(temp1[0]);
        }
        else
          alert("No issues detected.");
      }
      else {
        getWaitDlg().hide();
        alert(r.replyText);
      }
    },
    failure: function(o) {
      getWaitDlg().hide();
      alert(o.statusText);
    },
    scope: this
  },
  getFileName(true) + 'Cmd=ValidateEnvironment');
}

function initOpenEnvPanel(fnSubmit) {
  var tmpdt = top.document;
  if (!tmpdt.openEnvPanel) {
    tmpdt.openEnvPanel = new YAHOO.widget.Dialog("openEnvPanel",
                            { width: "300px",
                              height: "400px",
                              resizable: true,
                              fixedcenter: true,
                              close: true,
                              draggable: true,
                              //zindex:9999,
                              modal: true,
                              visible: false,
                              underlay: 'none',
                              constraintoviewport: true
                            }
                        );

                            tmpdt.openEnvPanel.renderEvent.subscribe(function() {

                            if (!tmpdt.openEnvPanel.layout) {
                              tmpdt.openEnvPanel.layout = new YAHOO.widget.Layout('openEnvLayout', {
                              height: (tmpdt.openEnvPanel.body.offsetHeight - 20),
          units: [
                                      { position: 'center', header: 'Available environments', width: 300, resize: true, body: 'openEnvTableDiv', gutter: '2px', /*collapse: true, collapseSize: 20,*/scroll: true}//,
                                  ]
        });
        tmpdt.openEnvPanel.layout.render();
      }

      populateOpenEnvTable();
    });

    tmpdt.openEnvPanel.setHeader("Select environment and click ok");
  }

  var fnCancel = function() {
    this.hide();
  }

  var myButtons = [{ text: "Open in new window", handler: fnSubmit, obj: true },
                   { text: "Ok", handler: fnSubmit, obj: false, isDefault: true },
                   { text: "Cancel", handler: fnCancel}];
  tmpdt.openEnvPanel.cfg.queueProperty("buttons", myButtons);
}

function populateOpenEnvTable() {

  top.document.startWait();
  var params = "queryType=sourceEnvironments";
  YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/GetValue', {
    success: function(o) {
      if (o.responseText.indexOf("<?xml") === 0) {
        var tmp = o.responseText.split(/<ReqValue>/g);
        var tmp1;
        if (tmp.length > 1) {
          tmp1 = tmp[1].split(/<\/ReqValue>/g);
          if (tmp1.length > 1)
            result = tmp1[0];
          else
            result = '';

          var files = result.split(/;/g);
          var objs = new Array();
          var staged_configuration = new Array();
          for (var i = 0, j = 0, k = 0; i < files.length; i++) {
            if (files[i] == "<StagedConfiguration>")
            {
              i++;
              staged_configuration[k] = files[i];
              k++;
            }
            else if (files[i] == "</StagedConfiguration>")
            {
              if (i+1 >= files.length)
              {
                break;
              }
              else
              {
                continue;
              }
            }
            objs[j] = {};
            objs[j].name = files[i];
            j++;
          }

          if (!Array.prototype.indexof) {
            Array.prototype.indexOf = function(obj, start) {
              for (var i = (start || 0), j = this.length; i < j; i++) {
                if (this[i] === obj) { return i; }
              }
              return -1;
            }
          }

          YAHOO.widget.DataTable.formatName = function(elLiner, oRecord, oColumn, oData) {
          if (staged_configuration.indexOf(oData) != -1)
           elLiner.innerHTML = "<font style=\"background-color:#004ADE;color:white\" title=\"Staged Configuration\">" + oData + "</font>";
          else
           elLiner.innerHTML = oData; };

          var openEnvColumnDefs = [{ key: "name", label: "Name", width: 280, formatter:YAHOO.widget.DataTable.formatName}];
          var openEnvDataSource = new YAHOO.util.DataSource(objs);
          openEnvDataSource.responseType = YAHOO.util.DataSource.TYPE_JSARRAY;
          var openEnvDataTable = new YAHOO.widget.DataTable("openEnvTableDiv", openEnvColumnDefs,
                                                            openEnvDataSource,
                                                            { initialLoad: false, resize: true, selectionMode: "single" });

          openEnvDataTable.subscribe("rowClickEvent", openEnvDataTable.onEventSelectRow);
          openEnvDataTable.subscribe("cellClickEvent", function() { openEnvDataTable.clearTextSelection() });
          openEnvDataTable.subscribe("cellDblclickEvent", function(oArgs) {
            var tmpdt = top.document;
            var btns = tmpdt.openEnvPanel.cfg.getProperty("buttons");
            for (var bIdx = 0; bIdx < btns.length; bIdx++) {
              if (btns[bIdx].text === 'Ok') {
                YAHOO.util.UserAction.click(btns[bIdx].htmlButton);
                return false;
              }
            }
          });
          openEnvTableDiv.openEnvTable = openEnvDataTable;
          openEnvTableDiv.openEnvDS = openEnvDataSource;
          openEnvTableDiv.openEnvDS.handleResponse("", objs, { success: openEnvTableDiv.openEnvTable.onDataReturnInitializeTable,
            scope: openEnvTableDiv.openEnvTable
          }, this, 999);
          top.document.stopWait();
        }
      }
    },
    failure: function(o) {
      top.document.stopWait();
    },
    scope: this
  },
  getFileName(true) + 'Params=' + params);
}

function displayOpenEnvDialog() {
  var tmpdt = top.document;
  var handleSubmit = function(evt) {
    var selRows = openEnvTableDiv.openEnvTable.getSelectedRows();
    if (selRows.length === 0) {
      alert("Please select the file to be opened");
      return;
    }

    this.hide();
    var fileName = openEnvTableDiv.openEnvTable.getRecord(selRows[0]).getData('name');
    openEnvTableDiv.openEnvTable.unselectAllRows();
    var envfileopened = window.location.href.split(/\?/g);

    if (envfileopened.length > 1 && fileName === document.forms['treeForm'].sourcefile.value)
      return;

    var txt = "";
    if (evt.srcElement) {
      if (evt.srcElement.textContent)
        txt = evt.srcElement.textContent;
      else
        txt = evt.srcElement.innerText;
    }
    else if (evt.target)
      txt = evt.target.textContent;

    var loc = window.location.href.split(/\?/g);
    var newwin = top.open(loc[0] + "?sourcefile=" + fileName, txt === "Ok" ? "_self" : "_blank");
  }
  
  initOpenEnvPanel(handleSubmit);

  document.getElementById('openEnvPanel').style.display = 'block';
  tmpdt.openEnvPanel.render(document.body);
  tmpdt.openEnvPanel.center();
  tmpdt.openEnvPanel.show();
}

function refresh(msg) {
  if (typeof (msg) !== 'undefined')
    alert(msg);

  if(top.document.navDT)
    refreshNavTree(top.document.navDT.ds, top.document.navDT, top.document.lastSelectedRow);
}

function expandRecord(dataTable, fldType, fldValue) {
  var Dom = YAHOO.util.Dom;
  var recSet = dataTable.getRecordSet();
  var recSetLen = recSet.getLength();
  for (var i = 0; i < recSetLen; i++) {
    var r = recSet.getRecord(i);
    if (r.getData(fldType) === fldValue) {
      var tdEl = dataTable.getFirstTdEl(r);
      var children = Dom.getChildren(tdEl);
      if (Dom.hasClass(children[0].children[0].children[0], 'yui-button')) {
        children[0].children[0].children[0].click();
        break;
      }
    }
  }
}

function getWaitDlg() {
  if (!top.document.wait) {
    top.document.wait = new YAHOO.widget.Dialog("wait",
                                  { width: "240px",
                                    fixedcenter: true,
                                    close: false,
                                    draggable: false,
                                    zindex: 4,
                                    modal: true,
                                    visible: false
                                  }
                              );

    top.document.wait.setHeader("Processing, please wait...");
    top.document.wait.setBody("<img src='/esp/files/img/loading.gif'/>");
    top.document.wait.render(document.body);
  }

  return top.document.wait;
}

function instanceSelectionToXML(table, selectedRows, navTable) {
  var rec = getCurrentSelRec(navTable);
  var xmlStr = "<Instances buildSet='" + rec.getData("BuildSet") + "' compName='" + rec.getData("Name") + "' >";

  if (typeof (selectedRows) !== 'undefined')
    for (var i = 0; i < selectedRows.length; i++)
    xmlStr += "<Instance name=\"" + table.getRecord(selectedRows[i]).getData('name') + "\"/>";

  xmlStr += "</Instances>";

  return xmlStr;
}

function displayAddInstanceDlg() {
  var tmpdt = top.document.navDT;
  var handleSubmit = function() {
    this.hide();
    top.document.startWait();
    var selRows = selectComputersDTDiv.selectComputersTable.getUserSelectedRows();
    var compName = tmpdt.getRecord(tmpdt.getSelectedRows()[0]).getData('Name');
    var xmlStr = instanceSelectionToXML(selectComputersDTDiv.selectComputersTable, selRows, tmpdt);
    clearSelectComputersTable();
    YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/HandleInstance', {
      success: function(o) {
        var dup = o.responseText.split(/<Duplicates>/g);
        if (dup.length > 1) {
          var dup1 = dup[1].split(/<\/Duplicates>/g);
          if (dup1.length > 1 && dup1[0].length > 0) {
            alert("Cannot add instances for the following computers as there can be only one instance of a component per computer.\n" + dup1[0]);
          }
        }

        var reqdcomps = o.responseText.split(/<AddReqdComps>/g);
        if (reqdcomps.length > 1) {
          var reqdcomps1 = reqdcomps[1].split(/<\/AddReqdComps>/g);
          if (reqdcomps1.length > 1 && reqdcomps1[0].length > 0) {
            var reqNames = o.responseText.split(/<ReqdCompNames>/g);
            if (reqNames.length > 1) {
              var reqNames1 = reqNames[1].split(/<\/ReqdCompNames>/g);
              if (reqNames1.length > 1 && reqNames1[0].length > 0) {
                var msg = "Following required component(s) '" + reqNames1[0] + "' do not have instance(s) on the following computer(s) '" + reqdcomps1[0] + "'. ";
                msg += "Would you like to add instances of the required components on these computers?";
                var fnyes = function() {
                  this.hide();
                  var xmlStr1 = "<ReqdComps>";
                  var ips = reqdcomps1[0].split(/\n/g);

                  for (var i = 0; i < ips.length; i++)
                    xmlStr1 += "<Computer netAddress=\"" + ips[i] + "\"/>";

                  xmlStr1 += "</ReqdComps>";

                  YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/AddReqdComps', {
                    success: function(o) {
                      var failed = o.responseText.split(/<Failures>/g);

                      if (failed.length > 1) {
                        var failed1 = failed[1].split(/<\/Failures>/g);
                        if (failed1.length > 1 && failed1[0].length > 0) {
                          var msg1 = "Failed to add required components for the following addresses.\n" + reqNames1[0];
                          msg1 += "\nPlease add instances for the following components manually\n" + reqNames1[0];
                          alert(msg1);
                        }
                      }
                    },
                    failure: function(o) {
                      top.document.stopWait();
                      alert(o.statusText);
                    },
                    scope: this
                  },
                  getFileName(true) + 'XmlArgs=' + xmlStr1);
                }

                var fnno = function() {this.hide();}
                promptYesNoCancel(msg, fnyes, fnno, null);
              }
            }
          }
        }

        var form = top.window.document.forms['treeForm'];
        form.isChanged.value = "true";
        top.document.stopWait();
        clickCurrentSelOrName(tmpdt);
        var temp = o.responseText.split(/<NewName>/g);

        if (temp.length > 1) {
          var temp1 = temp[1].split(/<\/NewName>/g);
          if (temp1.length > 1) {
            top.document.selectRecord = temp1[0];
            top.document.selectRecordField = "computer";
            top.document.selectRecordClick = true;
          }
        }
      },
      failure: function(o) {
        top.document.stopWait();
        alert(o.statusText);
      },
      scope: this
    },
        getFileName(true) + 'Operation=Add&XmlArgs=' + xmlStr);
  }

  initSelectComputersPanel(tmpdt, handleSubmit);

  document.getElementById('selectComputersPanel').style.display = 'block';
  tmpdt.selectComputersPanel.render(document.body);
  tmpdt.selectComputersPanel.center();
  tmpdt.selectComputersPanel.show();
}

function expandRecordWithId(dataTable, id) {
  var Dom = YAHOO.util.Dom;
  var recSet = dataTable.getRecordSet();
  var recSetLen = recSet.getLength();
  for (var i = 0; i < recSetLen; i++) {
    var r = recSet.getRecord(i);
    if (r.getData('id') === id) {
      var tdEl = dataTable.getFirstTdEl(r);
      var children = Dom.getChildren(tdEl);
      if (Dom.hasClass(children[0].children[0].children[0], 'buttoncollapsed'))
        if (Dom.hasClass(children[0].children[0].children[0], 'yui-button')) {
        children[0].children[0].children[0].click();
        break;
      }
      break;
    }
  }
}

function refreshNavTree(paramds, paramdt, selRec) {
  YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/GetNavTreeDefn', {
    success: function(o) {
      if (o.responseText.indexOf("<?xml") === 0) {
        var form = document.forms['treeForm'];
        var lSaved = o.responseText.split(/<LastSaved>/g);
        if (lSaved.length > 1) {
          var lSaved1 = lSaved[1].split(/<\/LastSaved>/g);
          if (lSaved1[0].charAt(0) != '<')
            form.lastSaved.value = lSaved1[0];
        }

        var lStarted = o.responseText.split(/<LastStarted>/g);
        if (lStarted.length > 1) {
          var lStarted1 = lStarted[1].split(/<\/LastStarted>/g);
          if (lStarted1[0].charAt(0) != '<')
            form.lastStarted.value = lStarted1[0];
        }

        var temp = o.responseText.split(/<CompDefn>/g);
        var temp1 = temp[1].split(/<\/CompDefn>/g);

        eval(temp1[0]);
        var treeData = getNavTreeData();

        paramdt.navTreeData = treeData;
        treeData[0].DisplayName += ' - ' + document.forms['treeForm'].sourcefile.value;
        paramds.handleResponse("", treeData, { success: paramdt.onDataReturnInitializeTable,
          scope: paramdt
        }, this, 999);

        clickCurrentSelOrName(paramdt, selRec);

        if (top.document.navDT.keepAliveInt) {
          clearInterval(top.document.navDT.keepAliveInt);
          top.document.navDT.keepAliveInt = setInterval(keepAlive, 10000);
        }

        updateEnvCtrls(form.isLocked.value === "true");

        top.document.stopWait();
        top.document.title = 'HPCC Systems Configuration Manager - ' + form.sourcefile.value;
      }
      else if (o.responseText.indexOf("<html") === 0) {
        document.forms['treeForm'].wizops.value = "1";
        var temp = o.responseText.split(/td align=\"left\">/g);
        var temp1 = temp[1].split(/<\/td>/g);
        alert(temp1[0]);
      }
    },
    failure: function(o) {
      top.document.stopWait();
      alert(o.statusText);
    },
    scope: this
  },
      getFileName(false));
}

function promptVerifyPwd(category, params, attrName, oldValue, newValue, recordIndex, callback) {
  var caller = this;
  this.focus();
  var handleCancel = function() {
    this.hide();
    caller.promptPwdPanel.cfg.queueProperty("keylisteners", null);
  }

  var clickSave = function() {
    caller.promptPwdPanel.cfg.queueProperty("keylisteners", null);
    var btns = caller.promptPwdPanel.cfg.getProperty("buttons");
    for (var bIdx = 0; bIdx < btns.length; bIdx++) {
      if (btns[bIdx].text === 'ok') {
        YAHOO.util.UserAction.click(btns[bIdx].htmlButton);
        return false;
      }
    }
  }

  var handleSave = function() {
    this.hide();
    caller.promptPwdPanel.cfg.queueProperty("keylisteners", null);
    var pwd = document.getElementById('pwd').value;
    if (pwd !== newValue) {
      alert("Passwords don't match");
      return false;
    }
    else {
      var xmlArgs = "<XmlArgs><Setting category=\"" + category;
      xmlArgs += "\" params=\"" + params;
      xmlArgs += "\" attrName=\"" + attrName;
      xmlArgs += "\" viewType=\"password";
      xmlArgs += "\" rowIndex=\"" + recordIndex;
      xmlArgs += "\" oldValue=\"" + oldValue;
      xmlArgs += "\" newValue=\"" + pwd + "\"/></XmlArgs>";
      xmlArgs = encodeURIComponent(xmlArgs);

      YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/SaveSetting', {
        success: function(o) {
          if (o.status === 200) {
            var form = top.window.document.forms['treeForm'];
            form.isChanged.value = "true";
            top.document.needsRefresh = true;
            var encPwd = o.responseText.split(/<UpdateValue>/g);
            var encPwd1 = encPwd[1].split(/<\/UpdateValue>/g);
            callback(true, encPwd1[0]);
          } else {
            alert(r.replyText);
            callback();
          }
        },
        failure: function(o) {
          alert(o.statusText);
          callback();
        },
        scope: this
      },
      getFileName(true) + 'XmlArgs=' + xmlArgs);

    }
  }
  if (!caller.promptPwdPanel) {
    caller.promptPwdPanel = new YAHOO.widget.Dialog("PwdPanel",
                              { width: "200px",
                                height: "125px",
                                resizable: true,
                                fixedcenter: true,
                                close: true,
                                draggable: true,
                                zindex: 9998,
                                modal: true,
                                visible: false,
                                underlay: 'none',
                                constraintoviewport: true
                              }
                          );
    var kl = new YAHOO.util.KeyListener(document, { keys: 13 }, { fn: clickSave,
      scope: caller.promptPwdPanel,
      correctScope: true
    });
    caller.promptPwdPanel.cfg.queueProperty("keylisteners", kl);
  }

  var myButtons = [{ text: "ok", handler: handleSave, isDefault: true },
                          { text: "Cancel", handler: handleCancel}];
  caller.promptPwdPanel.cfg.queueProperty("buttons", myButtons);

  document.getElementById('PwdPanel').style.display = 'block';
  caller.promptPwdPanel.setBody("<input type='password' id='pwd'/>");
  document.getElementById('pwd').value = '';
  caller.promptPwdPanel.render(document.body);
  caller.promptPwdPanel.firstFormElement = caller.promptPwdPanel.body.children[0];
  caller.promptPwdPanel.focusFirst();
  caller.promptPwdPanel.show();
}

function promptNewRange(domains, computerTypes, type) {
  if (!top.document.navDT.panelCfgAddComputers) {
    function onAddComputersButtonClick(p_oEvent) {
      switch (this.get("id")) {
        case "cfgAddComputersCancel":
          top.document.navDT.panelCfgAddComputers.hide();
          break;
        case "cfgAddComputersOk":
          {
            var domainsDropDown = document.getElementById('cfgAddComputersDomain');
            var cTypesDropDown = document.getElementById('cfgAddComputersType');
            var prefix = document.getElementById('cfgAddComputersNamePrefix');
            var startIP = document.getElementById('cfgAddComputersStartIP');
            var endIP = document.getElementById('cfgAddComputersStopIP');
            var isRange = document.getElementById('isRange');
            document.forms['treeForm'].computerRangeEnd.value = endIP.value;

            if (prefix.value === "") {
              alert("Prefix cannot be empty.");
              prefix.focus();
              return;
            }

            var errMsg = "";
            errMsg = isValidIPAddress(startIP.value, "Start IP Address", false, false);

            if (errMsg.length > 0) {
              alert(errMsg);
              startIP.select();
              startIP.focus();
              return;
            }

            if (isRange.checked) {
              errMsg = "";
              errMsg = isValidIPAddress(endIP.value, "Stop IP Address", false, false);

              if (errMsg.length > 0) {
                alert(errMsg);
                endIP.select();
                endIP.focus();
                return;
              }
            }
            else
              endIP.value = startIP.value;

            var xmlArgs = '<XmlArgs domain="' + domainsDropDown.value +
                            '" computerType="' + cTypesDropDown.value +
                            '" prefix="' + prefix.value +
                            '" startIP="' + startIP.value +
                            '" endIP="' + endIP.value + '" />';
            top.document.navDT.panelCfgAddComputers.hide();
            getWaitDlg().show();
            YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/HandleComputer', {
              success: function(o) {
                if (o.responseText.indexOf("<html") === 0) {
                  var temp = o.responseText.split(/td align=\"left\">/g);
                  var temp1 = temp[1].split(/<\/td>/g);
                  top.document.stopWait();
                  alert(temp1[0]);
                }
                else {
                  var form = top.window.document.forms['treeForm'];
                  form.isChanged.value = "true";
                  top.document.stopWait();
                  if (top.document.navDT.panelCfgAddComputers.populateType === 1)
                    populateSelectComputersPanel(top.document.navDT);
                  else if (top.document.navDT.panelCfgAddComputers.populateType === 2)
                    populateReplaceRoxieServers();
                  else {
                    top.document.navDT.clickCurrentSelOrName(top.document.navDT);
                    var temp = o.responseText.split(/<CompName>/g);
                    if (temp.length > 1) {
                      var temp1 = temp[1].split(/<\/CompName>/g);
                      if (temp1.length > 1) {
                        top.document.selectRecord = temp1[0];
                        top.document.selectRecordClick = true;
                      }
                    }
                  }
                }
                getWaitDlg().hide();
              },
              failure: function(o) {
                top.document.stopWait();
                alert(o.statusText);
                getWaitDlg().hide();
              },
              scope: this
            },
            getFileName(true) + 'Operation=NewRange&XmlArgs=' + xmlArgs);

            break;
          }
      }
    }

    top.document.navDT.panelCfgAddComputers = new YAHOO.widget.Panel("cfgAddComputers", { fixedcenter: true, visible: false, modal: true, constraintoviewport: true });
    var btncfgAddComputersOk = new YAHOO.widget.Button("cfgAddComputersOk");
    btncfgAddComputersOk.on("click", onAddComputersButtonClick);

    var btncfgAddComputersCancel = new YAHOO.widget.Button("cfgAddComputersCancel");
    btncfgAddComputersCancel.on("click", onAddComputersButtonClick);
    isRangeClicked(true);
  }

  var isRange = document.getElementById('isRange');
  if (type === 0 || type === 1) {
    isRange.checked = true;
    isRange.disabled=false;
    isRangeClicked(true);
  }
  else if (type === 3)
  {
     isRange.checked = false;
     isRange.disabled=true;
     isRangeClicked(false);
  }
  top.document.navDT.panelCfgAddComputers.populateType = type;
  var domainsDropDown = document.getElementById('cfgAddComputersDomain');
  var children = domainsDropDown.getElementsByTagName("option");
  for (i = 0; i < children.length; )
    domainsDropDown.removeChild(children.item(i));

  var inner = '';
  for (i = 0; i < domains.length; i++)
    domainsDropDown.options[domainsDropDown.options.length] = new Option(domains[i], domains[i]);

  inner = '';
  var cTypesDropDown = document.getElementById('cfgAddComputersType');

  children = cTypesDropDown.getElementsByTagName("option");
  for (i = 0; i < children.length; )
    cTypesDropDown.removeChild(children.item(i));

  for (i = 0; i < computerTypes.length; i++)
    cTypesDropDown.options[cTypesDropDown.options.length] = new Option(computerTypes[i], computerTypes[i]);

  document.getElementById('cfgAddComputers').style.display = 'block';
  top.document.navDT.panelCfgAddComputers.render(document.body);
  top.document.navDT.panelCfgAddComputers.focusFirst();
  top.document.navDT.panelCfgAddComputers.show();
}

function promptSlaveConfig(self) {
  var tmpdt = self;
  var handleSubmit = function() {
    var selRows = selectComputersDTDiv.selectComputersTable.getUserSelectedRows();
    if (selRows.length === 0) {
      alert("please make a selection or press cancel");
      return;
    }

    this.hide();
    top.document.startWait();
    var compName = tmpdt.getRecord(tmpdt.getSelectedRows()[0]).getData('Name');
    var xmlStr = instanceSelectionToXML(selectComputersDTDiv.selectComputersTable, selRows, tmpdt);

        YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/HandleRoxieOperation', {
          success: function(o) {
            clearSelectComputersTable();
            if (o.responseText.indexOf("<html") === 0) {
              var temp = o.responseText.split(/td align=\"left\">/g);
              var temp1 = temp[1].split(/<\/td>/g);
              top.document.stopWait();
              alert(temp1[0]);
            }
            else {
              var form = top.window.document.forms['treeForm'];
              form.isChanged.value = "true";
              top.document.stopWait();
              clickCurrentSelOrName(tmpdt);
            }
          },
          failure: function(o) {
            top.document.stopWait();
            alert(o.statusText);
          },
          scope: this
        },
            getFileName(true) + 'Cmd=RoxieSlaveConfig&XmlArgs=' + '<RoxieData ' + 'roxieName=\'' + compName + '\'>' + xmlStr + '</RoxieData>');
  }
  initSelectComputersPanel(tmpdt, handleSubmit);
  document.getElementById('selectComputersPanel').style.display = 'block';
  tmpdt.selectComputersPanel.render(document.body);
  tmpdt.selectComputersPanel.center();
  tmpdt.selectComputersPanel.show();
}

function slaveConfigToXml(table, type, val1, val2) {
  var compName = top.document.navDT.getRecord(top.document.navDT.getSelectedRows()[0]).getData('Name');
  var xmlStr = "<RoxieData type=\"" + type + "\" val1=\"" + val1 + "\" val2= \"" + val2 + "\" roxieName=\"" + compName + "\" >";
  var selectedRows = table.getUserSelectedRows();
  if (typeof (selectedRows) !== 'undefined') {
    for (var i = 0; i < selectedRows.length; i++) {
      xmlStr += "<Computer name=\"" + table.getRecord(selectedRows[i]).getData('name') + "\"/>";
    }
  }

  xmlStr += "</RoxieData>";

  return xmlStr;
}

function displaySlaveConfigDlg(paramdt, width) {
  if (width === 0)
    return;

  if (!paramdt.cfgRoxieSlave) {
    paramdt.cfgRoxieSlave = new YAHOO.widget.Dialog("cfgRoxieSlave",
                            { resizable: true,
                              fixedcenter: true,
                              close: true,
                              draggable: true,
                              //zindex:9999,
                              modal: true,
                              visible: false,
                              underlay: 'none',
                              constraintoviewport: true
                            }
                        );
  }

  var fnCancel = function() {
    this.hide();
  }

  var fnSave = function() {
    var conf, val1, val2, dirs;

    if (document.getElementById('cfgRoxieSlaveRedundancyFull').checked) {
      conf = "Full";
      val1 = document.getElementById('cfgRoxieSlaveFullChannel').value;
    }
    else if (document.getElementById('cfgRoxieSlaveRedundancyCir').checked) {
      conf = "Circular";
      val1 = document.getElementById('cfgRoxieSlaveCircularChannel').value;
      val2 = document.getElementById('cfgRoxieSlaveCircularOffset').value;
    }
    else if (document.getElementById('cfgRoxieSlaveRedundancyNone').checked)
      conf = "None";
    else if (document.getElementById('cfgRoxieSlaveRedundancyOver').checked) {
      val1 = document.getElementById('cfgRoxieSlaveChannelsPerHost').value;
      conf = "Overloaded";
    }

    if (conf !== "None" && val1 <= 0 || val1 > 3) {
      alert("Channel redundancy for " + conf + " redundancy cannot be <= 0 and > 3");
      return;
    }


        this.hide();
        var xmlStr = slaveConfigToXml(selectComputersDTDiv.selectComputersTable, conf, val1, val2);
        YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/HandleRoxieOperation', {
          success: function(o) {
            clearSelectComputersTable();
            if (o.responseText.indexOf("<html") === 0) {
              var temp = o.responseText.split(/td align=\"left\">/g);
              var temp1 = temp[1].split(/<\/td>/g);
              top.document.stopWait();
              alert(temp1[0]);
            }
            else {
              var form = top.window.document.forms['treeForm'];
              form.isChanged.value = "true";
              top.document.stopWait();
              clickCurrentSelOrName(paramdt);
            }
          },
          failure: function(o) {
            top.document.stopWait();
            alert(o.statusText);
          },
          scope: this
        },
            getFileName(true) + 'Cmd=RoxieSlaveConfig&XmlArgs=' + xmlStr);

  }

  var myButtons = [{ text: "Ok", handler: fnSave, isDefault: true },
                    { text: "Cancel", handler: fnCancel}];
                    paramdt.cfgRoxieSlave.cfg.queueProperty("buttons", myButtons);
  document.getElementById('cfgRoxieSlave').style.display = 'block';

  document.getElementById('cfgRoxieSlaveCircularChannel').value = 2;
  document.getElementById('cfgRoxieSlaveCircularOffset').value = 1;
  document.getElementById('cfgRoxieSlaveFullChannel').value = 2;
  document.getElementById('cfgRoxieSlaveChannelsPerHost').value = 1;

  paramdt.cfgRoxieSlave.render(document.body);

  if (width === 1) {
    document.getElementById("cfgRoxieSlaveRedundancyFull").disabled = true;
    document.getElementById("cfgRoxieSlaveRedundancyCir").disabled = true;
    document.getElementById("cfgRoxieSlaveRedundancyNone").checked = true;
    enableRoxieConfigOptions('None');
  }
  else {
    document.getElementById("cfgRoxieSlaveRedundancyFull").disabled = false;
    document.getElementById("cfgRoxieSlaveRedundancyCir").disabled = false;
    document.getElementById("cfgRoxieSlaveRedundancyFull").checked = true;
    enableRoxieConfigOptions('Full');
  }

  paramdt.cfgRoxieSlave.center();
  paramdt.cfgRoxieSlave.show();
  top.document.stopWait();
}

function populateSelectComputersPanel(paramdt)
{
  top.document.startWait();
  YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/GetComputersForRoxie', {
    success: function(o) {
      if (!selectComputersDTDiv.selectComputersDataSource) {
        var roxieComputersColumnDefs = [{ key: "check", label: "<input type='checkbox' id='SelectAllComputers'> Select <br/> All", formatter: "checkbox" },
                                    { key: "name", label: "Computer", width: 120 },
                                    { key: "netAddress", label: "Net Address", width: 100 },
                                    { key: "usage", label: "Usage", width: 180}];
        var xmlStr = '<?xml version="1.0" encoding="UTF-8"?><Machine name="" netAddress="" usage=""/>';
        var selectComputersDataSource = new YAHOO.util.DataSource(xmlStr);
        selectComputersDataSource.responseType = YAHOO.util.DataSource.TYPE_XML;
        selectComputersDataSource.responseSchema = { resultNode: "Machine", fields: ["name", "netAddress", "usage"] };
        var selectComputersDataTable = new YAHOO.widget.DataTable("selectComputersDTDiv", roxieComputersColumnDefs,
                                                              selectComputersDataSource,
                                                              { width: "490", initialLoad: false, resize: true });

        selectComputersDataTable.subscribe("checkboxClickEvent", function(oArgs) {
          var elem = oArgs.target;
          var oRecord = this.getRecord(elem);
          oRecord.setData("check", elem.checked);
          if (elem.checked !== true)
            top.document.getElementById('SelectAllComputers').checked = false;
        });

        selectComputersDataTable.getUserSelectedRows = function() {
          var selectedRows = new Array();
          var recSet = this.getRecordSet();
          var recSetLen = recSet.getLength();
          for (var i = 0; i < recSetLen; i++) {
            var r = recSet.getRecord(i);
            if (r.getData('check') === true)
              selectedRows[selectedRows.length] = r.getId();
          }

          return selectedRows;
        }

        selectComputersDataTable.on('theadCellClickEvent', function(oArgs) {
          var target = oArgs.target,
              column = this.getColumn(target),
              actualTarget = YAHOO.util.Event.getTarget(oArgs.event),
              check = actualTarget.checked;

          if (column.key == 'check') {
            var recordSet = this.getRecordSet();
            var len = recordSet.getLength();
            for (var i = 0; i < len; i++) {
              var rec = recordSet.getRecord(i);
              rec.setData('check', check);
            }
            this.render();
          }
          this.unselectAllRows();
        });

        selectComputersDataTable.subscribe("rowClickEvent", selectComputersDataTable.onEventSelectRow);
        selectComputersDataTable.subscribe("cellClickEvent", function() { selectComputersDataTable.clearTextSelection() });
        selectComputersDataTable.subscribe("cellDblclickEvent", function(oArgs) {
          var oRecord = this.getRecord(oArgs.target);
          oRecord.setData("check", true);
          var btns = paramdt.selectComputersPanel.cfg.getProperty("buttons");
          for (var bIdx = 0; bIdx < btns.length; bIdx++) {
            if (btns[bIdx].text === 'Ok') {
              YAHOO.util.UserAction.click(btns[bIdx].htmlButton);
              return false;
            }
          }
        });

        var oContextMenu = new YAHOO.widget.ContextMenu("selectcomputersmenu", { trigger: "selectComputersDTDiv", lazyload: true, container: "selectComputersDTDiv" });
        oContextMenu.subscribe("beforeShow", onSelectComputersContextMenuBeforeShow);
        oContextMenu.dt = selectComputersDataTable;

        selectComputersDTDiv.selectComputersTable = selectComputersDataTable;
        selectComputersDTDiv.selectComputersDataSource = selectComputersDataSource;
      }

      selectComputersDTDiv.selectComputersDataSource.handleResponse("", o, { success: selectComputersDTDiv.selectComputersTable.onDataReturnInitializeTable,
        scope: selectComputersDTDiv.selectComputersTable
      }, this, 999);

      top.document.getElementById('SelectAllComputers').checked = false;
      top.document.stopWait();
    },
    failure: function(o) {
      top.document.stopWait();
      alert(o.statusText);
    },
    scope: this
  },
  getFileName(true) + 'Cmd=Farms');
}

function initSelectComputersPanel(paramdt, fnSave, enableNumNodes, slavesPresent, slavesPerNode) {
  if (!paramdt.selectComputersPanel) {
    paramdt.selectComputersPanel = new YAHOO.widget.Dialog("selectComputersPanel",
                            { width: "520px",
                              height: "500px",
                              resizable: true,
                              fixedcenter: true,
                              close: true,
                              draggable: true,
                              modal: true,
                              visible: false,
                              underlay: 'none',
                              constraintoviewport: true
                            }
                        );

                            paramdt.selectComputersPanel.renderEvent.subscribe(function() {

                            if (!paramdt.selectComputersPanel.layout) {
                              paramdt.selectComputersPanel.layout = new YAHOO.widget.Layout('selectComputersLayout', {
                              height: (paramdt.selectComputersPanel.body.offsetHeight - 40),
                                  units: [
                                      { position: 'center', header: 'Computers', width: 490, resize: true, body: 'selectComputersDTDiv', gutter: '2px', scroll: true }
                                  ]
                                });
                                paramdt.selectComputersPanel.layout.render();
                              }

                              populateSelectComputersPanel(paramdt);
                            });

                            paramdt.selectComputersPanel.setHeader("Select computers");
  }

  var fnCancel = function() {
    this.hide();
  }
  
  var fnAddComputers = function(){
    var params = "queryType=DomainsAndComputerTypes";
    YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/GetValue', {
      success: function(o) {
        if (o.responseText.indexOf("<?xml") === 0) {
          var tmp = o.responseText.split(/<ReqValue>/g);
          var tmp1;
          if (tmp.length > 1) {
            tmp1 = tmp[1].split(/<\/ReqValue>/g);
            if (tmp1.length > 1)
              result = tmp1[0];
            else
              result = '';
             
            var domains = result.split(/<Domains>/g);
            var domainArr = new Array();
            if (domains.length > 0) {
              domains1 = domains[domains.length-1].split(/<\/Domains>/g);
              if (domains1.length > 0)
                domainArr = domains1[0].split(/,/g);
            }
              
            var cTypes = result.split(/<ComputerTypes>/g);
            var cTypeArr = new Array();
            if (cTypes.length > 0) {
              cTypes1 = cTypes[1].split(/<\/ComputerTypes>/g);
              if (cTypes1.length > 0)
                cTypeArr = cTypes1[0].split(/,/g);
            }

            promptNewRange(domainArr, cTypeArr, 1);
          }
        }
      },
      failure: function(o) {
      },
      scope: this
    },
  getFileName(true) + 'Params=' + params);
  }

  var myButtons = [{ text: "Add Hardware", handler: fnAddComputers },
                   { text: "Ok", handler: fnSave, isDefault: true },
                   { text: "Cancel", handler: fnCancel}];
  paramdt.selectComputersPanel.cfg.queueProperty("buttons", myButtons);
  if (enableNumNodes) {
    if (!document.getElementById('slavesPerNodeDiv')) {
      var newdiv = document.createElement("div");
      newdiv.id = "slavesPerNodeDiv";
      var newtext = document.createElement("LABEL"); 
      newtext.innerHTML = "Number of thor slaves per node(default 1): ";
      var aTextBox = document.createElement('input');
      aTextBox.type = 'text';
      aTextBox.value = slavesPerNode;
      aTextBox.id = 'slavesPerNode';
      aTextBox.style.width = "50";
      aTextBox.disabled = slavesPresent;
      newdiv.appendChild(newtext);
      newdiv.appendChild(aTextBox);
      var nodes = document.getElementById("selectComputersPanel").childNodes;
      for (var i = 0; i < nodes.length; i++) {
        if (nodes[i].className == "bd") {
          nodes[i].appendChild(newdiv);
          break;
        }
      }
    }
    
    document.getElementById('slavesPerNodeDiv').style.display = 'block';
    document.getElementById('slavesPerNodeDiv').style.styleFloat = "left";
    document.getElementById('slavesPerNodeDiv').style.cssFloat = "left";
    document.getElementById('slavesPerNodeDiv').style.paddingTop = "5";
  }
  else {
    if (document.getElementById('slavesPerNodeDiv'))
      document.getElementById('slavesPerNodeDiv').style.display = 'none';
  }

  if (document.getElementById('slavesPerNode')) {
    document.getElementById('slavesPerNode').disabled = slavesPresent;
    document.getElementById('slavesPerNode').value = slavesPerNode;
  }
}

function initReplaceRoxieNodesPanel() {
  var Dom = YAHOO.util.Dom;
  var Event = YAHOO.util.Event;
  var DDM = YAHOO.util.DragDropMgr;
  if (!top.document.navDT.roxieNodesReplacePanel) {
    top.document.navDT.roxieNodesReplacePanel = new YAHOO.widget.Dialog("roxieReplaceNodePanel",
                            { width: "450px",
                              height: "400px",
                              resizable: true,
                              fixedcenter: true,
                              close: true,
                              draggable: true,
                              //zindex:9999,
                              modal: true,
                              visible: false,
                              underlay: 'none',
                              constraintoviewport: true
                            });

    top.document.navDT.roxieNodesReplacePanel.renderEvent.subscribe(function() {
      if (!top.document.navDT.roxieNodesReplacePanel.layout) {
        top.document.navDT.roxieNodesReplacePanel.layout = new YAHOO.widget.Layout('roxieReplaceNodeLayout', {
          height: (top.document.navDT.roxieNodesReplacePanel.body.offsetHeight - 20),
          units: [{ position: 'center', header: 'Computers', width: 400, resize: true, body: 'left4', gutter: '2px', scroll: true }]
        });
        top.document.navDT.roxieNodesReplacePanel.layout.render();
      }

      top.document.startWait();
      populateReplaceRoxieServers();
    });

    top.document.navDT.roxieNodesReplacePanel.setHeader("Drag and Drop Computers onto Roxie Servers");
  }

  var fnClose = function() {
    this.hide();
  }

  var fnAddComputers = function() {
    var params = "queryType=DomainsAndComputerTypes";
    YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/GetValue', {
      success: function(o) {
        if (o.responseText.indexOf("<?xml") === 0) {
          var tmp = o.responseText.split(/<ReqValue>/g);
          var tmp1;
          if (tmp.length > 1) {
            tmp1 = tmp[1].split(/<\/ReqValue>/g);
            if (tmp1.length > 1)
              result = tmp1[0];
            else
              result = '';

            var domains = result.split(/<Domains>/g);
            var domainArr = new Array();
            if (domains.length > 0) {
              domains1 = domains[domains.length - 1].split(/<\/Domains>/g);
              if (domains1.length > 0)
                domainArr = domains1[0].split(/,/g);
            }

            var cTypes = result.split(/<ComputerTypes>/g);
            var cTypeArr = new Array();
            if (cTypes.length > 0) {
              cTypes1 = cTypes[1].split(/<\/ComputerTypes>/g);
              if (cTypes1.length > 0)
                cTypeArr = cTypes1[0].split(/,/g);
            }

            promptNewRange(domainArr, cTypeArr, 2);
          }
        }
      },
      failure: function(o) {
      },
      scope: this
    },
  getFileName(true) + 'Params=' + params);
  }

  var myButtons = [{ text: "Add Hardware", handler: fnAddComputers },
                   { text: "Close", handler: fnClose, isDefault: true }];
  top.document.navDT.roxieNodesReplacePanel.cfg.queueProperty("buttons", myButtons);
}

function populateReplaceRoxieServers() {
  YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/GetComputersForRoxie', {
    success: function(o) {
      if (!left4.roxieComputersDataSource) {
        var roxieComputersColumnDefs = [{ key: "name", label: "Computer", width: 120 },
                                        { key: "netAddress", label: "Net Address", width: 100 },
                                        { key: "usage", label: "Usage", width: 180}];
        var xmlStr = '<?xml version="1.0" encoding="UTF-8"?><Machine name="" netAddress="" usage=""/>';
        var roxieComputersDataSource = new YAHOO.util.DataSource(xmlStr);
        roxieComputersDataSource.responseType = YAHOO.util.DataSource.TYPE_XML;
        roxieComputersDataSource.responseSchema = { resultNode: "Machine", fields: ["name", "netAddress", "usage"] };
        var roxieComputersDataTable = new YAHOO.widget.DataTable("left4", roxieComputersColumnDefs,
                                      roxieComputersDataSource,
                                      { width: "400", initialLoad: false, resize: true });

        roxieComputersDataTable.subscribe("rowClickEvent", roxieComputersDataTable.onEventSelectRow);
        roxieComputersDataTable.subscribe("cellClickEvent", function() { roxieComputersDataTable.clearTextSelection() });
        roxieComputersDataTable.subscribe("cellDblclickEvent", function(oArgs) {
          var btns = top.document.navDT.roxieNodesReplacePanel.cfg.getProperty("buttons");
          for (var bIdx = 0; bIdx < btns.length; bIdx++) {
            if (btns[bIdx].text === 'Ok') {
              YAHOO.util.UserAction.click(btns[bIdx].htmlButton);
              return false;
            }
          }
        });
        
        left4.roxieComputersTable = roxieComputersDataTable;
        left4.roxieComputersDataSource = roxieComputersDataSource;
        myDTDrags = {};

        roxieComputersDataTable.subscribe("initEvent", function() {
          var i, id,
          allRows = this.getTbodyEl().rows;

          for (i = 0; i < allRows.length; i++) {
            id = allRows[i].id;
            if (myDTDrags[id]) {
              myDTDrags[id].unreg();
              delete myDTDrags[id];
            }

            myDTDrags[id] = new CONFIGMGR.DDRows(id, "default", { isTarget: false }, roxieComputersDataTable);
          }
        });
      }

      var i, id, allRows = left4.roxieComputersTable.getTbodyEl().rows;

      for (i = 0; i < allRows.length; i++) {
        id = allRows[i].id;
        if (myDTDrags[id]) {
          myDTDrags[id].unreg();
          delete myDTDrags[id];
        }
      }

      left4.roxieComputersDataSource.handleResponse("", o, { success: left4.roxieComputersTable.onDataReturnInitializeTable,
        scope: left4.roxieComputersTable
      }, this, 999);

      top.document.stopWait();
    },
    failure: function(o) {
      top.document.stopWait();
      alert(o.statusText);
    },
    scope: this
  },
  getFileName(true) + 'Cmd=Farms');
}

function updateRecordName(oldValue, newValue) {
  var sel = top.document.navDT.getSelectedRows()[0];
  var rec = top.document.navDT.getRecord(sel);
  var dispName = rec.getData('DisplayName');
  var newName = dispName.split(oldValue); //, newValue);

  rec.setData('DisplayName', newName[0] + newValue);
  rec.setData('Name', newValue);
  top.document.lastSelectedRow = newValue;
  top.document.navDT.selectRow(sel);
  clickCurrentSelOrName(top.document.navDT, newValue, true);
  expandRecord(this, "Name", "Environment");
  expandRecord(this, "Name", "Software");
  top.document.lastSelectedRow = newValue;
  top.document.navDT.render();
  //clickCurrentSelOrName(top.document.navDT, newValue, true);
}

function enableRoxieConfigOptions(type) {
  if (type === 'Cir') {
    document.getElementById('cfgRoxieSlaveChannelsPerHost').disabled = true;
    document.getElementById('cfgRoxieSlaveFullChannel').disabled = true;
    document.getElementById('cfgRoxieSlaveCircularChannel').disabled = false;
    document.getElementById('cfgRoxieSlaveCircularOffset').disabled = false;
  }
  else if (type === 'Full') {
    document.getElementById('cfgRoxieSlaveChannelsPerHost').disabled = true;
    document.getElementById('cfgRoxieSlaveFullChannel').disabled = false;
    document.getElementById('cfgRoxieSlaveCircularChannel').disabled = true;
    document.getElementById('cfgRoxieSlaveCircularOffset').disabled = true;
  }
  else if (type === 'None') {
    document.getElementById('cfgRoxieSlaveChannelsPerHost').disabled = true;
    document.getElementById('cfgRoxieSlaveFullChannel').disabled = true;
    document.getElementById('cfgRoxieSlaveCircularChannel').disabled = true;
    document.getElementById('cfgRoxieSlaveCircularOffset').disabled = true;
  }
  else if (type === 'Over') {
    document.getElementById('cfgRoxieSlaveChannelsPerHost').disabled = false;
    document.getElementById('cfgRoxieSlaveFullChannel').disabled = true;
    document.getElementById('cfgRoxieSlaveCircularChannel').disabled = true;
    document.getElementById('cfgRoxieSlaveCircularOffset').disabled = true;
  }
}

function clearSelectComputersTable() {
  if (selectComputersDTDiv.selectComputersDataSource) {
    selectComputersDTDiv.selectComputersTable.unselectAllRows();
    top.document.getElementById('SelectAllComputers').checked = false;
  }
}

function thorInstSelToXML(self, selectedRows, paramdt, type, validateComputers, skip, slavesPerNode) {
  var compName = top.document.navDT.getRecord(top.document.navDT.getSelectedRows()[0]).getData('Name');
  var xmlStr = "<ThorData type=\"" + type + "\" name=\"" + compName +
               "\" validateComputers=\"" + validateComputers +
               "\" slavesPerNode=\"" + slavesPerNode + 
               "\" skipExisting = \"" + skip + "\">";
  //var selectedRows = table.getSelectedRows();
  if (typeof (selectedRows) !== 'undefined') {
    for (var i = 0; i < selectedRows.length; i++) {
      xmlStr += "<Computer name=\"" + self.getRecord(selectedRows[i]).getData('name') + "\"/>";
    }
  }

  xmlStr += "</ThorData>";

  return xmlStr;
}

function promptThorTopology(self, type, slavesPresent, slavesPerNode) {
  var tmpdt = self;
  var handleSubmit = function() {
    var selRows = selectComputersDTDiv.selectComputersTable.getUserSelectedRows();
    if (selRows.length === 0) {
      alert("please make a selection or press cancel");
      return;
    }

    var numslaves = parseInt(document.getElementById("slavesPerNode").value, 10);
    if (!(isInteger(document.getElementById('slavesPerNode').value)) || numslaves < 1) {
      alert("Number of thor slaves per node must be a number greater than 0");
      return;
    }
    this.hide();
    top.document.startWait();
    var compName = tmpdt.getRecord(tmpdt.getSelectedRows()[0]).getData('Name');
    var xmlStr = thorInstSelToXML(selectComputersDTDiv.selectComputersTable, selRows, tmpdt, type, true, false, document.getElementById("slavesPerNode").value);

    YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/HandleThorTopology', {
      success: function(o) {
        if (o.responseText.indexOf("<html") === 0) {
          var temp = o.responseText.split(/td align=\"left\">/g);
          var temp1 = temp[1].split(/<\/td>/g);
          top.document.stopWait();
          alert(temp1[0]);
        }
        else {
          var form = document.forms['treeForm'];
          var status = o.responseText.split(/<Status>/g);
          var status1 = status[1].split(/<\/Status>/g);

          if (status1[0] !== 'true') {

            var fnYes = function() {
              this.hide();
              xmlStr = thorInstSelToXML(selectComputersDTDiv.selectComputersTable, selRows, tmpdt, type, false, false, document.getElementById("slavesPerNode").value);
              YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/HandleThorTopology', {
                success: function(o) {
                  clearSelectComputersTable();
                  if (o.responseText.indexOf("<html") === 0) {
                    var temp = o.responseText.split(/td align=\"left\">/g);
                    var temp1 = temp[1].split(/<\/td>/g);
                    top.document.stopWait();
                    alert(temp1[0]);
                  }
                  else {
                    var form = top.window.document.forms['treeForm'];
                    form.isChanged.value = "true";
                    top.document.stopWait();
                    clickCurrentSelOrName(tmpdt);
                  }
                },
                failure: function(o) {
                  top.document.stopWait();
                  alert(o.statusText);
                },
                scope: this
              },
              getFileName(true) + 'Operation=Add&Type=' + type + '&XmlArgs=' + xmlStr);
            }

            var fnNo = function() {
              this.hide();
              xmlStr = thorInstSelToXML(selectComputersDTDiv.selectComputersTable, selRows, tmpdt, type, false, true, document.getElementById("slavesPerNode").value);
              YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/HandleThorTopology', {
                success: function(o) {
                  clearSelectComputersTable();
                  if (o.responseText.indexOf("<html") === 0) {
                    var temp = o.responseText.split(/td align=\"left\">/g);
                    var temp1 = temp[1].split(/<\/td>/g);
                    top.document.stopWait();
                    alert(temp1[0]);
                  }
                  else {
                    var form = top.window.document.forms['treeForm'];
                    form.isChanged.value = "true";
                    setCurEnv(form, o.responseText)
                    top.document.stopWait();
                    clickCurrentSelOrName(tmpdt);
                  }
                },
                failure: function(o) {
                  top.document.stopWait();
                  alert(o.statusText);
                },
                scope: this
              },
              getFileName(true) + 'Operation=Add&Type=' + type + '&XmlArgs=' + xmlStr);
            }

            var fnCancel = function() { this.hide(); }
            promptYesNoCancel(status1[0], fnYes, fnNo, fnCancel);

          }
          else {
            var form = top.window.document.forms['treeForm'];
            form.isChanged.value = "true";
            top.document.stopWait();
            clickCurrentSelOrName(tmpdt);
          }
        }
      },
      failure: function(o) {
        top.document.stopWait();
        alert(o.statusText);
      },
      scope: this
    },
      getFileName(true) + 'Operation=Add&Type=' + type + '&XmlArgs=' + xmlStr);
  }
  initSelectComputersPanel(tmpdt, handleSubmit, type==="Slave" ? true : false, slavesPresent, slavesPerNode);
  document.getElementById('selectComputersPanel').style.display = 'block';
  tmpdt.selectComputersPanel.render(document.body);
  tmpdt.selectComputersPanel.center();
  tmpdt.selectComputersPanel.show();
}

function promptYesNoCancel(msg, fnYes, fnNo, fnCancel) {
  var tmpdt = top.document.navDT;
  if (!tmpdt.YNCancelPanel) {
    tmpdt.YNCancelPanel = new YAHOO.widget.Dialog("YesNoCancelPanel",
                            { width: "450px",
                              resizable: true,
                              fixedcenter: true,
                              close: true,
                              draggable: true,
                              //zindex:9999,
                              modal: true,
                              visible: false,
                              underlay: 'none',
                              constraintoviewport: true
                            }
                        );
  }

  var myButtons = [{ text: "Yes", handler: fnYes, isDefault: true },
                      { text: "No", handler: fnNo }];

  if (fnCancel !== null)
    myButtons[myButtons.length] = { text: "Cancel", handler: fnCancel};

  tmpdt.YNCancelPanel.cfg.queueProperty("buttons", myButtons);
  document.getElementById('YesNoCancelPanel').style.display = 'block';
  tmpdt.YNCancelPanel.setBody(msg);
  tmpdt.YNCancelPanel.render(document.body);
  tmpdt.YNCancelPanel.show();
}

function promptValidationErrs(msg) {
 if(!top.document.ValidationErrPanel){
    var fn = function () { this.hide(); }
    top.document.ValidationErrPanel = new YAHOO.widget.Dialog('validationErrPage',
    {
      width:500,
      height :550,
      visible : false,
      draggable : true,
      modal : true,
      close : false,
      constraintoviewport: true,
      underlay: 'none',
      buttons :[ { text:"Ok", handler:fn, isDefault:true}]
    });

    top.document.ValidationErrPanel.renderEvent.subscribe(function(){
     if (!top.document.ValidationErrPanel.layout) {
        top.document.ValidationErrPanel.layout = new YAHOO.widget.Layout('validationErrLayout', {
        height: (top.document.ValidationErrPanel.body.offsetHeight - 25),
        units: [
           { position: 'center' , body: 'validationErrLayoutTextArea'} ]
        });
        top.document.ValidationErrPanel.layout.render();
      }
    });
  }

  resize = new YAHOO.util.Resize('validationErrPage', {
    handles: ['br'],
    autoRatio: true,
    status: false,
    minWidth: 280,
    minHeight: 350
  });

  resize.on('resize', function(args) {
     var panelHeight = args.height,
     padding = 20;
     YAHOO.util.Dom.setStyle('validationErrLayout', 'display', 'none');
     this.cfg.setProperty("height", panelHeight + 'px');
     top.document.ValidationErrPanel.layout.set('height', this.body.offsetHeight - padding);
     top.document.ValidationErrPanel.layout.set('width', this.body.offsetWidth - padding);
     YAHOO.util.Dom.setStyle('validationErrs', 'height', this.body.offsetHeight - padding - 10);
     YAHOO.util.Dom.setStyle('validationErrs', 'width', this.body.offsetWidth - padding);
     YAHOO.util.Dom.setStyle('validationErrLayout', 'display', 'block');
     top.document.ValidationErrPanel.layout.resize();

  }, top.document.ValidationErrPanel, true);

  document.getElementById('validationErrPage').style.display = 'block';
  document.getElementById('validationErrs').value = msg;
  top.document.ValidationErrPanel.render();
  top.document.ValidationErrPanel.show();
  top.document.ValidationErrPanel.center();
}

function unlockUser() {
  //onbeforeunload handles unsaved changes. At this point, user
  //doesn't care about unsaved changes
  if (top.document.navDT && top.document.navDT.keepAliveInt) {
    clearInterval(top.document.navDT.keepAliveInt);
    top.document.navDT.keepAliveInt = 0;
  }
  
  var form = document.forms['treeForm'];
  if (form.isLocked.value === "true")
    unlockEnvironment(top.document.navDT, false);

  if (form.isWizLocked.value === "true")
    unlockEnvForWizard();

  resetHiddenVars();
}

function setReadWrite(flag) {
  if (!checkForEE()) {
    updateEnvCtrls(false);
    return;
  }

  getWaitDlg().show();
  if (flag)
    lockEnvironment();
  else
    saveAndUnlockEnv();
}

function saveAndUnlockEnv() {
  var form = document.forms['treeForm'];
  if (form.isChanged.value === "true")
    askUserToSave(top.document.navDT, true, true);
  else
    unlockEnvironment(top.document.navDT, false);
  getWaitDlg().hide();
}

function updateEnvCtrls(flag) {
 var Dom = YAHOO.util.Dom;
  var sbtn = Dom.get("savebutton");
  var vbtn = Dom.get("validatebutton");
  var fileopened = window.location.href.split(/\?/g);
  if (document.forms['treeForm'].wizops.value != '3' || fileopened.length <= 1)
    document.getElementById('ReadWrite').disabled = true;
  
  if (flag) {
    Dom.removeClass(sbtn, "yui-button-disabled");
    Dom.removeClass(vbtn, "yui-button-disabled");
    document.getElementById('savebutton').disabled = false;
    document.getElementById('validatebutton').disabled = false;
    document.getElementById('ReadWrite').checked = true;
  }
  else {
    Dom.addClass(sbtn, "yui-button-disabled");
    Dom.addClass(vbtn, "yui-button-disabled");
    document.getElementById('savebutton').disabled = true;
    document.getElementById('validatebutton').disabled = true;
    document.getElementById('ReadWrite').checked = false;
  }
}

function updateWizCtrls() {
  var form = document.forms['treeForm'];
  var Dom = YAHOO.util.Dom;
  var bbtn = Dom.get("blankEnv");
  var abtn = Dom.get("advButton");
  if (form.wizops.value === "3") {
    Dom.removeClass(bbtn, "yui-button-disabled");
    Dom.removeClass(abtn, "yui-button-disabled");
    document.getElementById('blankEnv').disabled = false;
    document.getElementById('advButton').disabled = false;
  }
  else {
    Dom.addClass(bbtn, "yui-button-disabled");
    Dom.addClass(abtn, "yui-button-disabled");
    document.getElementById('blankEnv').disabled = true;
    document.getElementById('advButton').disabled = true;
  }
}

function resetHiddenVars()
{
  //Firefox does not reset hidden form values
  var form = document.forms['treeForm'];
  form.isLocked.value = "false";
  form.isWizLocked.value = "false";
  form.isChanged.value = "false";
  form.lastSaved.value = "";
  form.lastStarted.value = "";
  form.compsToBeDeployed.value = "";
  form.configFiles.value = "0";
  form.saveInProgress.value = "false";
  form.displayMode.value = "0";
  form.mode.value = "0";
  form.foundEnvironment.value = "false";
  form.ipMode.value = "1";
  form.ip.value = "";
  form.textClear.value = "false";
  form.userName.value = "";
  form.computerRangeEnd.value = "";
  form.wizfile.value = "";
  //do not reset form.sourcefile.value
  //do not reset form.userid.value
  //do not reset form.wizops.value
}

if (typeof CONFIGMGR == "undefined" || !CONFIGMGR)
  CONFIGMGR = {};

CONFIGMGR.DDRows = function(id, sGroup, config, srcDt, rightViewDom) {
  CONFIGMGR.DDRows.superclass.constructor.call(this, id, sGroup, config);
  YAHOO.util.Dom.addClass(this.getDragEl(), "custom-class");
  this.goingUp = false;
  this.lastY = 0;
  this.dt = srcDt;
  this.rightViewDom = rightViewDom;
};

YAHOO.extend(CONFIGMGR.DDRows, YAHOO.util.DDProxy, {
  proxyEl: null,
  srcEl: null,
  srcData: null,
  dt: null,
  rightViewDom: null,

  startDrag: function(x, y) {
    var proxyEl = this.proxyEl = this.getDragEl(),
                    srcEl = this.srcEl = this.getEl();

    this.srcData = this.dt.getRecord(this.srcEl).getData();
    YAHOO.util.Dom.setStyle(srcEl, "visibility", "hidden");
    proxyEl.innerHTML = "<table><tbody>" + srcEl.innerHTML + "</tbody></table>";
  },

  endDrag: function(x, y) {
    var position, srcEl = this.srcEl;

    YAHOO.util.Dom.setStyle(this.proxyEl, "visibility", "hidden");
    YAHOO.util.Dom.setStyle(srcEl, "visibility", "");
  },

  onDragEnter: function(e, id) {
    var Dom = this.rightViewDom ? this.rightViewDom : YAHOO.util.Dom;
    var destEl = Dom.get(id);

    if (destEl && destEl.nodeName.toLowerCase() === "tr") {

      roxieDT = top.document.RightTabView.get("activeTab").dt;
      var tr = roxieDT.getTrEl(destEl.id);
      Dom.setStyle(tr, "outline-style", "solid");
      top.document.body.style.cursor = "blink";
    }
  },

  onDragOut: function(e, id) {
    var Dom = this.rightViewDom ? this.rightViewDom : YAHOO.util.Dom;
    var destEl = Dom.get(id);

    if (destEl && destEl.nodeName.toLowerCase() === "tr") {
      roxieDT = top.document.RightTabView.get("activeTab").dt;
      var tr = roxieDT.getTrEl(destEl.id);
      Dom.setStyle(tr, "outline-style", "none");
      top.document.body.style.cursor = "auto";
    }
  },

  getEl: function() {
    if (!this._domRef) {
      if (this.rightViewDom)
        this._domRef = this.rightViewDom.get(this.id);
      else
        this._domRef = YAHOO.util.Dom.get(this.id);
    }

    return this._domRef;
  },

  onDragDrop: function(e, id) {
    var destEl = YAHOO.util.Dom.get(id);
    if (!destEl || !destEl.id)
      return;
    top.document.startWait();
    roxieDT = top.document.RightTabView.get("activeTab").dt;
    var compName = top.document.navDT.getRecord(top.document.navDT.getSelectedRows()[0]).getData('Name');
    var xmlStr = "<RoxieData type='ReplaceRoxieServer' roxieName='" + compName + "'><Nodes>";
    var rec = roxieDT.getRecord(destEl.id);
    var parent = getRecord(roxieDT, rec.getData('parent'));
    xmlStr += "<Node name='" + rec.getData('name') + "' farm='" + parent.getData('name') + "' newComputer='" + this.srcData.name + "'/>";
    xmlStr += "</Nodes></RoxieData>";

    YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/HandleRoxieOperation', {
      success: function(o) {
        var form = top.window.document.forms['treeForm'];
        form.isChanged.value = "true";
        top.document.stopWait();
        clickCurrentSelOrName(top.document.navDT);
        populateReplaceRoxieServers();
      },
      failure: function(o) {
        top.document.stopWait();
        alert(o.statusText);
      },
      scope: this
    },
          getFileName(true) + 'Cmd=ReplaceRoxieServer&XmlArgs=' + xmlStr);
    YAHOO.util.DDM.refreshCache();
  }
});

function createDDRows(id, sGroup, config, srcDT, rightViewDom) {
  return new CONFIGMGR.DDRows(id, sGroup, config, srcDT, rightViewDom);
}

function wizardPanel(){
if(top.document.displayModeDialog1)
  top.document.displayModeDialog1.hide();
  
 if(!top.document.wizardpanel)
 {
    top.document.wizardpanel = new YAHOO.widget.Panel('wizardPanel', {
      modal:true,
      draggable: true,
      close: false,
      autofillheight: "body",
      constraintoviewport:true,
      underlay: 'none',
      width: '500',
      height: '540px'
    });
    
    top.document.wizardpanel.renderEvent.subscribe(function() {
      if(!top.document.wizardpanel.layout){
        top.document.wizardpanel.layout = new YAHOO.widget.Layout('wizardLayout', {
          height : 520,
          width: 475,
          units: [
           { position: 'top', height: 55, resize: false, body: 'wizardTop'},
           { position: 'center', body: 'wizardCenter'}
          ]
        });
      }
      
      top.document.wizardpanel.layout.on('render',handleWizardScreen);
      top.document.wizardpanel.layout.render();
    });
 }
  document.getElementById('wizardPanel').style.display='block';
  top.document.wizardpanel.render(document.body);
  top.document.wizardpanel.show();
  top.document.wizardpanel.center();
}

function handleWizardScreen() {
  var handleWizardDialog1Next = function() {
    top.document.displayModeDialog1.hide();
    validateWizardDialog1(document.getElementById('ipListText').value);
  };//WizardDialog1Next

  var handleWizardDialog1Back = function() {
    var gotoWizBack = function() {
      top.document.wizardDialog1.hide();
      top.document.wizardpanel.hide();
      top.document.displayModeDialog1.show();
      if (document.getElementById('ip').value === ''){
       document.getElementById('ipListText').value="Sample : X.X.X.X; X.X.X.X-XXX;";
       document.getElementById('ipListText').style.color="Gray";
       document.forms['treeForm'].textClear.value = "false"; 
      }
    }
     unlockEnvForWizard(gotoWizBack, false);
  };
  
  var handleWizardDialog1Cancel = function() {
    var gotoWizCancel = function() {
      top.document.wizardDialog1.hide();
      top.document.wizardpanel.hide();
      if(!top.document.navDT && document.forms['treeForm'].sumparams.value !== '1')
        top.document.displayModeDialog1.show();
      else
        top.document.displayModeDialog1.hide();
    }
    
    unlockEnvForWizard(gotoWizCancel, false);
  }
       
  if(!top.document.wizardDialog1)
  {
    var centers=top.document.wizardpanel.layout.getSizes();
    top.document.wizardDialog1 = new YAHOO.widget.Dialog('wizardIPAddressScreen', 
    {   
      width:((top.document.wizardpanel.layout.getUnitByPosition('center').get('width')) - 2),
      height:((top.document.wizardpanel.layout.getUnitByPosition('center').get('height')) - 2),
      visible : true, 
      draggable : false,
      modal : false,
      close : false,
      zindex : 9999,
      container: 'wizardCenter',
      xy:(YAHOO.util.Dom.getXY(top.document.wizardpanel.layout.getUnitByPosition('center'))),
      buttons : [ { text:"Cancel", handler:handleWizardDialog1Cancel},
        { text:"Back", handler:handleWizardDialog1Back},
        { text:"Next", handler:handleWizardDialog1Next, isDefault:true }]
    });
  }
  if(document.forms['treeForm'].wizops.value != '3')
     document.getElementById('autoIP').disabled=true;
  document.getElementById('wizardIPAddressScreen').style.display='block';
  top.document.wizardDialog1.render();    
  top.document.wizardDialog1.show(); 
  
}

function validateWizardDialog1(ipList){
//Before forming finalIPList remove the duplicate.
  var errorString = "";
  var finalIPList = '';
  ipList=removeNL(ipList);
  ipList =getUniqueIP(ipList);
  ipList=removeSpaces(ipList);
  
  if (document.forms['treeForm'].ipMode.value !== '2'){
    var theName ="IPAddress";
    document.getElementById('ipListText').value=ipList;
    var errorString = isValidIPAddress(ipList, theName, true, true)
    
    if( errorString === '') {
     saveIPList();
     finalIPList = ipList ;
    }
    else {
     indentIP();
     alert(errorString);
     top.document.wizardDialog1.show();
    }
  }
  else {
    finalIPList = document.getElementById('ipListText').value;
  }
  
  if(finalIPList !== '' && errorString === '')
  {
     handleNumNodesScreen();
  }
}

function lockEnvForWizard(){
getWaitDlg().show();
YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/NavMenuEvent', {
  success: function(o) {
    getWaitDlg().hide();
    if (o.status === 200) {
      if (o.responseText.indexOf("<?xml") === 0) {
        var form = document.forms['treeForm'];
        var temp = o.responseText.split(/<XmlArgs>/g);
        var isErr = false;
        if (temp.length > 0) {
          var temp1 = temp[1].split(/<\/XmlArgs>/g);
          if (temp1.length > 0 && temp1[0].length > 0 && temp1[0].charAt(0) != '<') {
            isErr = true;
            //alert(temp1[0]);
            if( temp1[0].match(/Write/) != null ){
              form.isWizLocked.value = "false";
              alert(temp1[0]);
              top.document.displayModeDialog1.show();
            }
            else if( temp1[0].match(/Cannot/) != null || temp1[0].match(/Another/) != null ){ 
              alert(temp1[0]);
              top.document.displayModeDialog1.show();         
            }
          }
        }
        
        if (!isErr) {
          var form = document.forms['treeForm'];
          form.isWizLocked.value = "true";
          getWaitDlg().hide();
          wizardPanel();
        }
          
      }
      else if (o.responseText.indexOf("<html") === 0) {
        var temp = o.responseText.split(/td align=\"left\">/g);
        var temp1 = temp[1].split(/<\/td>/g);
        getWaitDlg().hide();
        alert(temp1[0]);
      }
    }
    else {
      getWaitDlg().hide();
      alert(r.replyText);
    }
  },
  failure: function(o) {
    getWaitDlg().hide();
    alert(o.statusText);
  },
  scope: this
  },
    getFileName(true, true) + 'Cmd=LockEnvironment&XmlArgs=' + '');
}

function unlockEnvForWizard(fnCallback, saveEnv){
  getWaitDlg().show();
  var xmlArgs='';
  if (saveEnv)
    xmlArgs = "<XmlArgs><SaveEnv%20flag=%27true%27/></XmlArgs>";

  if(document.forms['treeForm'].isWizLocked.value === "true"){
    YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/NavMenuEvent', {
      success: function(o) {
        getWaitDlg().hide();
        var isErr = false;
        if (o.status === 200) {
          if (o.responseText.indexOf("<?xml") === 0) {
            var form = document.forms['treeForm'];
            form.isWizLocked.value = "false";
            var temp = o.responseText.split(/<XmlArgs>/g);
            if (temp.length > 0) {
              var temp1 = temp[1].split(/<\/XmlArgs>/g);
              if (temp1.length > 0 && temp1[0].length > 0) {
                if (temp1[0].indexOf("<Warning>") === 0) {
                  var warning = o.responseText.split(/<Warning>/g);
                  if (warning.length > 0) {
                    var warning1 = warning[1].split(/<\/Warning>/g);
                    if (warning1.length > 0 && warning1[0].length > 0 && warning1[0].charAt(0) != '<')
                      alert(warning1[0]);
                  }
                }
              }
            }
          }
          else if (o.responseText.indexOf("<html") === 0) {
            var temp = o.responseText.split(/td align=\"left\">/g);
            var temp1 = temp[1].split(/<\/td>/g);
            getWaitDlg().hide();
            alert(temp1[0]);
            isErr = true;
          }

          if (fnCallback && !isErr)
            fnCallback();
        } else {
          getWaitDlg().hide();
          alert(r.replyText);
        }
      },
      failure: function(o) {
        getWaitDlg().hide();
        alert(o.statusText);
      },
      scope: this
    },
      getFileName(true, true) + 'Cmd=UnlockEnvironment&XmlArgs=' + xmlArgs);
  }
  else{
    getWaitDlg().hide();
    fnCallback();
  }
}

function formXMLStringFromIPList(ipList)
{
  var xmlStr='';
  var finalStr='';
  ipList=removeNL(ipList);
  ipList = ipList.replace(/;$/, "");
  var pattern=/-/;
  var ipArr= new Array();
  ipArr=ipList.split(";");
  var hasrange = "false";
  for (k = 0; k < ipArr.length; k++) {
    var IPvalue = ipArr[k];
    if (IPvalue.match(pattern) != null) {
      if(hasrange === "false") {
        tempStr= xmlStr;
        xmlStr = "<Computerlist hasrange =\"true\">" + tempStr;
        hasrange = "true";
      }
      xmlStr = xmlStr + "<ComputerRange netAddress=\"" + IPvalue + "\"/>"   
    }
    else {
     xmlStr = xmlStr + "<Computer netAddress=\"" + IPvalue + "\"/>" ;
    }
  }
  if(hasrange === "true")
    finalxmlStr = xmlStr + "</Computerlist>";
  else
    finalxmlStr = "<Computerlist>" + xmlStr + "</Computerlist>";
  return finalxmlStr;
}

function enableTextArea(flag)
{
  document.getElementById("ipListText").disabled=!flag;
}

function clearTextArea()
{
  document.getElementById('ipListText').style.color='Black';
  if(document.forms['treeForm'].ipMode.value !== '2' && document.forms['treeForm'].textClear.value !== "true")
  {
    var pattern=/Sample/;
    if( (document.getElementById('ipListText').value).match(pattern) != null)
     document.getElementById("ipListText").value ='';
    else if(document.forms['treeForm'].ip.value !== ''){
     document.getElementById("ipListText").value = document.forms['treeForm'].ip.value;
   }
    else
     document.getElementById("ipListText").value='';
  }
  else if(document.forms['treeForm'].ipMode.value !== '2' && document.forms['treeForm'].ip.value !== ''){
    document.getElementById("ipListText").value = document.forms['treeForm'].ip.value;
    indentIP();
  }
  document.forms['treeForm'].textClear.value = "true"; 
}

function saveIPList()
{
  if(document.forms['treeForm'].ipMode.value !== '2')
    document.forms['treeForm'].ip.value = document.getElementById("ipListText").value;
}

function addNewLine(event)
{
  var evtobj= window.event? event : event.which; //distinguish between IE's explicit event object (window.event) and Firefox's implicit.
  var unicode = evtobj.charCode? evtobj.charCode : evtobj.keyCode
  if(unicode == 186 || unicode == 188)
    document.getElementById('ipListText').value += "\n";
}

function indentIP() {
  var arrIPs = document.getElementById('ipListText').value;
  arrIPs=removeNL(arrIPs);
  arrIPs = arrIPs.replace(/;/g,';\n');
  document.getElementById('ipListText').value = arrIPs;
}

function removeNL(s) {
  /*
  ** Remove NewLine, CarriageReturn and Tab characters from a String
  **   s  string to be processed
  ** returns new string also before removing \n check for the delimited (;) if not add it.
  */
  var ipArr = new Array();
  ipArr = s.split('\n');
  var ipList ='';
  for( i = 0; i < ipArr.length; i++) {
   var ips = ipArr[i];
   var pattern=/-/;
   if( ips.match(pattern) != null){
      ips=ips.replace(" -","-");
      ips=ips.replace("- ","-");
   }
   if ( ips !== "" && typeof(ips) !== 'undefined')
   {
       var ipsOnSingleLine = ips.split(" ");
       r = "";
       for (y=0; y < ipsOnSingleLine.length; y++) {
         var ip=ipsOnSingleLine[y];
         for (k=0; k < ip.length; k++) {
          if (ip.charAt(k) != '\n' &&
              ip.charAt(k) != '\r' &&
              ip.charAt(k) != '\t' &&
              ip.charAt(k) != ',' &&
              ip.charAt(k) != '\s'
             ) {
            r += ip.charAt(k);
          }
         }
         if(r.charAt((r.length)-1) !== ';' && r !== "")
             r+=';';
         ipList += r;
       }
    }
 }
  return ipList;
}


function getIPAddrThrAutoGenScript() {
getWaitDlg().show();
document.forms['treeForm'].textClear.value = "false";
var value=document.getElementById("ipListText").value;
var pattern=/Sample/;
if(value.match(pattern) != null)
{
    document.getElementById("ipListText").value='';
}
YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/GetSubnetIPAddr', {
    success: function(o) {
      if (o.responseText.indexOf("<?xml") === 0) {
        var temp = o.responseText.split(/<Message>/g);
        var isErr = false;
        if (temp.length > 0) {
        var temp1 = temp[1].split(/<\/Message>/g);
        if (temp1.length > 0 && temp1[0].length > 0 && temp1[0].charAt(0) != '<') {
          isErr = true;
          var errorString = temp1[0] + "Please use the manual entry for entering IP List";
          document.getElementById("manualIP").checked = true;
          document.getElementById("ipListText").disabled = false;
          alert(errorString);
        }
        if(!isErr)
        {
          document.getElementById("ipListText").disable=true;
          var ipList = o.responseText.split(/<IPList>/g);
          var ipList1 = ipList[1].split(/<\/IPList>/g);
          var form=document.forms['treeForm'];
          if (ipList1[0].charAt(0) != '<') {
            document.getElementById('ipListText').value=ipList1[0];
          }
            indentIP();
         }
        }
        getWaitDlg().hide();
        }
        else if (o.responseText.indexOf("<html") === 0) {
          var temp = o.responseText.split(/td align=\"left\">/g);
          var temp1 = temp[1].split(/<\/td>/g);
          getWaitDlg().hide();
          alert(temp1[0]);
        }
    },
    failure: function(o) {
      getWaitDlg().hide();
      alert(o.statusText);
    },
    scope: this
  },
  getFileName(true, true) + 'Cmd=GetSubnetIPAddr');
}

function getUniqueIP(ipList) {
  var arrIPs = ipList.split(";");
  var arrNewIPs = [];
  var seenIPs = {};
  for(var i=0;i<arrIPs.length;i++) {
    if (!seenIPs[arrIPs[i]]) {
      seenIPs[arrIPs[i]]=true;
      arrNewIPs.push(arrIPs[i]);
    }
  }
  return arrNewIPs.join(";");
}

function handleNumNodesScreen() {
getWaitDlg().hide();
populateNumberOfNode();
  var handleNumNodesDialogNext = function() {
    //Validation of number nodes against the number of IPs
    validateNumNodesDialog();
  };
                           
  var handleNumNodesDialogBack = function() {
    top.document.numNodesDialog.hide();
    top.document.wizardDialog1.show();
    indentIP();
  };
  
  var handleNumNodesDialogCancel = function() {
    var gotoWizCancel = function() {
     top.document.numNodesDialog.hide();
     top.document.wizardDialog1.hide();
     top.document.wizardpanel.hide();
      if(!top.document.navDT && document.forms['treeForm'].sumparams.value !== '1')
        top.document.displayModeDialog1.show();
    }
    
    unlockEnvForWizard(gotoWizCancel, false);
  }
                  
  if(!top.document.numNodesDialog)  {
    top.document.numNodesDialog = new YAHOO.widget.Dialog('wizardNumNodesPage', 
    {   
      width:((top.document.wizardpanel.layout.getUnitByPosition('center').get('width')) - 2),
      height:((top.document.wizardpanel.layout.getUnitByPosition('center').get('height')) - 2),
      visible : true, 
      draggable : false,
      modal : false,
      close : false,
      zindex : 9999,
      container: 'wizardCenter',
      xy:(YAHOO.util.Dom.getXY(top.document.wizardpanel.layout.getUnitByPosition('center'))),
      buttons : [ { text:"Cancel", handler: handleNumNodesDialogCancel},
          { text:"Back", handler:handleNumNodesDialogBack},
          { text:"Next", handler:handleNumNodesDialogNext, isDefault:true }]
    });
    
    top.document.numNodesDialog.cancelEvent.subscribe(function(){
      document.getElementById('top1').style.display='block';
      document.getElementById('mybutton').style.display='none';
    });
    
    top.document.numNodesDialog.renderEvent.subscribe(function(){
      if(document.forms['treeForm'].wizops.value != '3'){
         document.getElementById('slavesPerNode').disabled=true;
      }
    });
  }
  document.getElementById('wizardNumNodesPage').style.display='block';  
  top.document.numNodesDialog.render();
  top.document.numNodesDialog.show();  
  
}

function populateNumberOfNode(){
  var numberIPs = ipCount();
  var defaultNodes = parseInt(numberIPs);
  
  if( parseInt(defaultNodes) <= 1){
    document.getElementById('node4Thor').value = "1";
    document.getElementById('node4Support').value = "0";
  }
  else{
    var supportNodes = 7;

    if ( defaultNodes > 1 && defaultNodes <= 10 )
      supportNodes = 1;
    else if ( defaultNodes > 10 && defaultNodes <= 20 )
      supportNodes = 2;
    else if ( defaultNodes > 20 && defaultNodes <= 50 )
      supportNodes = 3;
    else if ( defaultNodes > 50 && defaultNodes <= 100 )
      supportNodes = 4;

    document.getElementById('node4Support').value = supportNodes;
    document.getElementById('node4Thor').value = defaultNodes - supportNodes == 1?1:defaultNodes - supportNodes - 1;
  }

  document.getElementById('node4RoxieServ').value = "0";
}   

function validateNumNodesDialog() {
  if (!(isInteger(document.getElementById('node4Thor').value)) ||
      !(isInteger(document.getElementById('node4RoxieServ').value)) ||
      !(isInteger(document.getElementById('slavesPerNode').value)) ||
      !(isInteger(document.getElementById('node4Support').value)))
  {
    alert("Only Numeric entries allowed for number of nodes");
  }
  else
  {
    var numberIPs = parseInt(ipCount());
    var roxieNodes = parseInt(document.getElementById('node4RoxieServ').value);
    var thorNodes = parseInt(document.getElementById('node4Thor').value);
    var supportNodes = parseInt(document.getElementById('node4Support').value);

    if (roxieNodes <= numberIPs && thorNodes <= numberIPs && supportNodes <= numberIPs) {
      if (thorNodes == numberIPs && numberIPs > 10) {
        if (!confirm("As the number of Thor slave nodes requested is equal to the number \
of ip addresses given, there will be an overlap of Thor master node and a Thor \
slave node. This is not recommended in an environment with more than \
10 nodes.\n\nDo you want to continue?"))
          return;
      }
      else if (supportNodes == numberIPs && thorNodes + roxieNodes > 0){
        alert("Number of support nodes cannot be equal to the number of IPs provided as nodes are required for thor/roxie.");
        return;
      }

      submitInformation();
    }
    else{
      alert("Number of nodes should be less then number of IPs provided.");
    }   
  }
}

function ipCount(){
  var ipList=document.getElementById('ipListText').value;
  ipList = removeNL(ipList);
  ipList = ipList.replace(/;$/,"");
  var ipArr= new Array();
  ipArr = ipList.split(";");
  var cnt = 0;
  var ipPattern =/^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/;
  
  for(var k=0; k < ipArr.length ;k++ )
  {

    if (ipArr[k].match("-") != null )
    {
      var ip=new Array();
      ip = ipArr[k].split("-");
      if ( ip.length > 1)
      {
        var ipArray = ip[0].match(ipPattern);
        var startAddr = parseInt(ipArray[4]);
        var endAddr = parseInt(ip[1]);
        if(startAddr > endAddr)
        {
           var tempAddr = startAddr;
           startAddr= endAddr;
           endAddr=tempAddr;
        }
        while(startAddr != endAddr)
        { 
          cnt++;
          startAddr++;
        }
      }
    }
    cnt++;
  }
  return cnt;   
}

function submitInformation() {
  //Before submitting collect all the information in XML format
  var roxieSrvNode = document.getElementById('node4RoxieServ').value ;
  var thorNode = document.getElementById('node4Thor').value ;
  var supportNodes = document.getElementById('node4Support').value ;
  var iplist = document.getElementById('ipListText').value;
  var slavesPerNode = document.getElementById('slavesPerNode').value;
  var roxieOnDemand = document.getElementById('roxieOnDemand').checked;
  
  var xmlStr = "<XmlArgs username=\"" ;
  xmlStr += "\" password=\"";
  xmlStr += "\" roxieNodes=\"" + roxieSrvNode;
  xmlStr += "\" thorNodes=\"" + thorNode;
  xmlStr += "\" supportNodes=\"" + supportNodes;
  xmlStr += "\" slavesPerNode=\"" + slavesPerNode;
  xmlStr += "\" roxieOnDemand=\"" + roxieOnDemand;
  xmlStr += "\" ipList=\"" + iplist;
  xmlStr += "\"/>";

  var button = top.document.numNodesDialog.getButtons();
  button[0].set("disabled",true);
  button[1].set("disabled",true);
  
getStaticProgressBar().show();
YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/BuildEnvironment', {
  success: function(o) {
    if (o.status === 200) {
      if (o.responseText.indexOf("<?xml") === 0) {
        var temp = o.responseText.split(/<Message>/g);
        var isErr = false;
        if (temp.length > 0) {
          var temp1 = temp[1].split(/<\/Message>/g);
          if (temp1.length > 0 && temp1[0].length > 0 && temp1[0].charAt(0) != '<') {
            isErr = true;
            getStaticProgressBar().hide();
            alert(temp1[0]);
            button[0].set("disabled", false);
            button[1].set("disabled", false);
          }
          else {
            var stat = o.responseText.split(/<Status>/g);
            var stat1 = stat[1].split(/<\/Status>/g);
            if (stat1[0] === "false") {
              getStaticProgressBar().hide();
              isErr = true;
              alert("Unknown error in generating Environment");
            }
            button[0].set("disabled", false);
            button[1].set("disabled", false);
          }
        }

        if (!isErr) {
          summaryPageForWizard();
          getStaticProgressBar().hide();
        }
      }
      else if (o.responseText.indexOf("<html") === 0) {
        var temp = o.responseText.split(/td align=\"left\">/g);
        var temp1 = temp[1].split(/<\/td>/g);
        getStaticProgressBar().hide();
        button[0].set("disabled", false);
        button[1].set("disabled", false);
        alert(temp1[0]);
      }
    }
    else {
      getStaticProgressBar().hide();
      button[0].set("disabled", false);
      button[1].set("disabled", false);
      alert(r.replyText);
    }
  },
  failure: function(o) {
    getStaticProgressBar().hide();
    button[0].set("disabled", false);
    button[1].set("disabled", false);
    alert(o.statusText);
  },
  scope: this
},
    getFileName(true, true) + 'XmlArgs=' + xmlStr);
}//submitInformation
 
function getStaticProgressBar(){

  var xx=400, yy=400 ;
  if(top.document.wizardpanel) {
     xx=((YAHOO.util.Dom.getX(top.document.wizardpanel.layout.getUnitByPosition('center'))) + 60);
     yy=((YAHOO.util.Dom.getY(top.document.wizardpanel.layout.getUnitByPosition('center'))) + 200);                        
  }

  if( !top.document.staticProgress1 )
  {
    top.document.staticProgress1 =  new YAHOO.widget.Panel("staticProgress",  
      { width:"250px",
        x:xx,
        y:yy,
        close:false, 
        draggable:false, 
        zindex:9999,
        modal:false,
        visible:true,
        underlay: 'none',
        container:'wizardcenter'
      } 
    );
  }
  document.getElementById('staticProgress').style.display='block';
  top.document.staticProgress1.render();
  top.document.staticProgress1.bringToTop();
  top.document.staticProgress1.center();
  return top.document.staticProgress1;
}

function summaryPageForWizard()
{
  top.document.numNodesDialog.hide();
  top.document.wizardDialog1.hide();
  top.document.wizardpanel.hide();
     
  var myButtons;
  var headerText = "Environment summary for " + document.forms['treeForm'].wizfile.value;
  getStaticProgressBar().show();
  var handleSummaryBack = function(o){
    top.document.sumPanel.hide();
    top.document.wizardpanel.show();
    top.document.wizardpanel.center();
    top.document.numNodesDialog.show();
  };

  var handleSummaryFinish = function(o) {
    var gotoFin = function() {
       callHtmlSummaryPage();
    }
    unlockEnvForWizard(gotoFin, true);
  };

  var handleSummaryGotoAdvance = function(o) {
    var gotoAdv = function() {
      top.document.sumPanel.hide();
      top.document.wizardpanel.hide();
      var loc = window.location.href.split(/\?/g);
      var newwin = top.open(loc[0] + "?sourcefile=" + document.forms['treeForm'].wizfile.value, "_self");  
    }
    unlockEnvForWizard(gotoAdv, true);
  };
  
  var handleSummaryPageCancel = function(o) {
    var gotoWizCancel = function() {
       top.document.sumPanel.hide();
       top.document.wizardDialog1.hide();
       top.document.numNodesDialog.hide();
       top.document.wizardpanel.hide();
      if(!top.document.navDT && document.forms['treeForm'].sumparams.value !== '1')
          top.document.displayModeDialog1.show();
     }
     unlockEnvForWizard(gotoWizCancel, false);
  };
  
 if(!top.document.sumPanel){
    top.document.sumPanel = new YAHOO.widget.Dialog('summaryPage',
    {
    width:500,
    height :550,
    visible : false, 
    draggable : true,
    modal : true,
    close : false,
    constraintoviewport: true,
    underlay: 'none',
    buttons :[ { text:"Cancel", handler:handleSummaryPageCancel},
                { text:"Back", handler:handleSummaryBack },
                { text:"Finish", handler:handleSummaryFinish, isDefault:true},
                { text:"Advanced View", handler:handleSummaryGotoAdvance }]
    });
    
    top.document.sumPanel.renderEvent.subscribe(function(){
     if (!top.document.sumPanel.layout) {
        top.document.sumPanel.layout = new YAHOO.widget.Layout('summaryPageLayout', {
        height: (top.document.sumPanel.body.offsetHeight - 25),
        units: [
           { position: 'top', height: 55, resize: false, body: 'summaryPageHeader'},
           { position: 'center' , header:headerText, body: 'summaryPageTable', scroll:true} ]
        });
        top.document.sumPanel.layout.render();
      }
      populateSummaryDetails(false, 'summaryPageTable');
         
      if(document.forms['treeForm'].wizops.value != '3'){
        var btn = top.document.sumPanel.cfg.getProperty("buttons");
        (top.document.sumPanel.getButtons())[3].set("disabled", true);
        btn[3].htmlButton.title = "This operation is only supported in Enterprise and above editions. Please contact HPCC Systems at http://www.hpccsystems.com/contactus";
      }
     
    });
  }
  
  resize = new YAHOO.util.Resize('summaryPage', { 
    handles: ['br'], 
    autoRatio: true, 
    status: false,   
    minWidth: 480,   
    minHeight: 450     
  });   
  
  resize.on('resize', function(args) { 
     var panelHeight = args.height, 
     padding = 20; 
     //Hack to trick IE into behaving 
     YAHOO.util.Dom.setStyle('summaryPageLayout', 'display', 'none'); 
     this.cfg.setProperty("height", panelHeight + 'px'); 
     top.document.sumPanel.layout.set('height', this.body.offsetHeight - padding); 
     top.document.sumPanel.layout.set('width', this.body.offsetWidth - padding); 
     YAHOO.util.Dom.setStyle('summaryPageLayout', 'display', 'block'); 
     top.document.sumPanel.layout.resize(); 
           
  }, top.document.sumPanel, true); 
 
  document.getElementById('summaryPage').style.display = 'block';
  top.document.sumPanel.render();
  top.document.sumPanel.show();
  top.document.sumPanel.center();
  
}

function summaryPageForAdvance() {
  if (top.document.messageDialogBox)
    top.document.messageDialogBox.hide();
    
  var myButtons;
  var headerText = "Environment summary for " + document.forms['treeForm'].wizfile.value;
  getStaticProgressBar().show();
  
  var handleSummaryCancel = function(o) {
     this.hide();
     document.forms['treeForm'].sumparams.value = '0';
     document.forms['treeForm'].wizfile.value = '';

     var fileopened = window.location.href.split(/\?/g);
     if (fileopened.length <= 1)
       invokeWizard();
     
  };

  var handleSummaryGotoAdvance = function(o) {
    if (checkForEE()) {
      top.document.advSumPanel.hide();
      var loc = window.location.href.split(/\?/g);
      var newwin = top.open(loc[0] + "?sourcefile=" + document.forms['treeForm'].wizfile.value, "_self");
    }
  };

  if(!top.document.advSumPanel){
    top.document.advSumPanel = new YAHOO.widget.Dialog('sumPage',
    {
    width:500,
    height :550,
    visible : false, 
    draggable : true,
    modal : false,
    close : false,
    constraintoviewport: true,
    underlay: 'none',
    buttons :[ { text:"ok", handler:handleSummaryCancel, isDefault:true},
               { text:"Advanced View", handler:handleSummaryGotoAdvance }]
    });
    
    top.document.advSumPanel.renderEvent.subscribe(function(){
     if (!top.document.advSumPanel.layout) {
        top.document.advSumPanel.layout = new YAHOO.widget.Layout('sumPageLayout', {
        height: (top.document.advSumPanel.body.offsetHeight - 25),
        units: [ { position: 'center' , header:headerText , body: 'sumPageTable', scroll:true} ]
        });
      }
      top.document.advSumPanel.layout.render();
      populateSummaryDetails(true, 'sumPageTable');
         
      if(document.forms['treeForm'].wizops.value != '3'){
        var btn = top.document.advSumPanel.cfg.getProperty("buttons");
        (top.document.advSumPanel.getButtons())[1].set("disabled", true);
        btn[1].htmlButton.title = "This operation is only supported in Enterprise and above editions. Please contact HPCC Systems at http://www.hpccsystems.com/contactus";
      }
     
    });
  }
  
  resize = new YAHOO.util.Resize('sumPage', { 
    handles: ['br'], 
    autoRatio: true, 
    status: false,   
    minWidth: 480,   
    minHeight: 450   
  });   
  
  resize.on('resize', function(args) { 
     var panelHeight = args.height, 
     padding = 20; 
     //Hack to trick IE into behaving 
     YAHOO.util.Dom.setStyle('sumPageLayout', 'display', 'none'); 
     this.cfg.setProperty("height", panelHeight + 'px'); 
     top.document.advSumPanel.layout.set('height', this.body.offsetHeight - padding); 
     top.document.advSumPanel.layout.set('width', this.body.offsetWidth - padding); 
     YAHOO.util.Dom.setStyle('sumPageLayout', 'display', 'block'); 
     top.document.advSumPanel.layout.resize(); 
           
   }, top.document.advSumPanel, true); 
  
  document.getElementById('sumPage').style.display = 'block';
  top.document.advSumPanel.render();
  top.document.advSumPanel.show();
  top.document.advSumPanel.center();
  
}
 

function populateSummaryDetails(linkFlag, sumDataTable)
{
  getStaticProgressBar().show();
  var button;
  if(top.document.numNodesDialog)
      button = top.document.numNodesDialog.getButtons();
  YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/GetSummary', {
    success: function(o) {
    if(top.document.numNodesDialog)
      top.document.numNodesDialog.hide();
      getStaticProgressBar().hide();
      var dt;
      if(!linkFlag)
        dt = top.document.summaryDataSourceWiz;
      else
        dt = top.document.summaryDataSource; 
      
      if (!dt) {
          var summaryColumnDefs = [ { key: "name", width: 175, label: "Component/Esp Services", formatter: function(el, oRecord, oColumn, oData){
             if(oRecord.getData('espservice') === 'true'){
               el.innerHTML =  oData + " (ESP Service)";
             }
             else
               el.innerHTML = oData;
            },sortable: true,  resizeable:true},
            { key: "buildset", width: 100, label: "BuildSet" ,sortable: true,  resizeable:true},
            { key: "netaddresses", width: 196, className:"classForSum", label: "Net Addresses/Port", sortable: true,  resizeable:true }];
          
        var xmlStr = '<?xml version="1.0" encoding="UTF-8"?><Component name="" buildset="" netaddresses=""/>';
        var summaryDataSource = new YAHOO.util.DataSource(xmlStr);
        summaryDataSource.responseType = YAHOO.util.DataSource.TYPE_XML;
        summaryDataSource.responseSchema = { resultNode: "Component", fields: ["name", "buildset", "netaddresses", "espservice"] };
        var summaryDataTable = new YAHOO.widget.DataTable(sumDataTable, summaryColumnDefs,
                              summaryDataSource,{width: "100%", resize: true, initialLoad : false});

        if(linkFlag) {
          top.document.summaryDataTable = summaryDataTable;
          top.document.summaryDataSource = summaryDataSource;
        }
        else {
          top.document.summaryDataTableWiz = summaryDataTable;
          top.document.summaryDataSourceWiz = summaryDataSource;
        }
      }
      
      if(linkFlag){
        top.document.summaryDataSource.handleResponse("", o, { success: top.document.summaryDataTable.onDataReturnInitializeTable,
          scope: top.document.summaryDataTable
        }, this, 999);
      }
      else {
        top.document.summaryDataSourceWiz.handleResponse("", o, { success: top.document.summaryDataTableWiz.onDataReturnInitializeTable,
          scope: top.document.summaryDataTableWiz
        }, this, 999);
      }  
   },
   failure: function(o) {
      getStaticProgressBar().hide();
      alert(o.statusText);
      button[0].set("disabled",false);
      button[1].set("disabled",false);

   },
      scope: this
   },
   getFileName(true, true)  + 'PrepareLinkFlag=' + true);
}


function isRangeClicked(flag) {
  var form = document.forms['treeForm'];

  if (!flag) {
    form.computerRangeEnd.value = cfgAddComputersStopIP.value;
    cfgAddComputersStopIP.value = "";
    cfgAddComputersStopIP.disabled = true;
    cfgAddComputersStopIPLabel.disabled = true;
  }
  else {
    cfgAddComputersStopIP.value = form.computerRangeEnd.value;
    cfgAddComputersStopIP.disabled = false;
    cfgAddComputersStopIPLabel.disabled = false;
  }
}

function displayWizardFirstScreen() {
  if(window.self.navigate)
    window.self.navigate(window.location);
   window.location.reload();
}

function passwordChanged(param) {
 document.forms['treeForm'].encryptionNeeded.value = parseInt(param) + parseInt(document.forms['treeForm'].encryptionNeeded.value);
}

function loadAndCheckFileNames(value)
{
 getWaitDlg().show();
  var params = "queryType=sourceEnvironments";
  YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/GetValue', {
    success: function(o) {
      getWaitDlg().hide();
      if (o.responseText.indexOf("<?xml") === 0) {
        var tmp = o.responseText.split(/<ReqValue>/g);
        var tmp1;
        if (tmp.length > 1) {
          tmp1 = tmp[1].split(/<\/ReqValue>/g);
          if (tmp1.length > 1)
            result = tmp1[0];
          else
            result = '';

          var files = result.split(/;/g);
          if (value === '2' || value === '4') {
            //first remove the items 
            var element;
            if (value === '2')
              element = document.getElementById('fileDropDownMenu');
            else
              element = document.getElementById('sumDropDownMenu');

            var children = element.getElementsByTagName("option");
            for (i = 0; i < children.length; )
              element.removeChild(children.item(i));

            var optn = document.createElement("OPTION");
            optn.text = "";
            optn.value = "";
            element.options.add(optn);
            
            for (var i = 0; i < files.length; i++) {
              if (files[i] !== '') {
                var optn = document.createElement("OPTION");
                if (files[i] == "<StagedConfiguration>")
                {
                  i++;
                  optn.style.backgroundColor = "#004ADE";
                  optn.style.color = "white";
                  optn.title = "Currently Staged";

                  optn.text = files[i];
                  optn.value = files[i];
                  element.options.add(optn);

                  i++;
                }
                else
                {
                  optn.text = files[i];
                  optn.value = files[i];
                  element.options.add(optn);
                }
              }
            }
          }
          else {
            var filename = '';
            var regEx = new RegExp('[,/\:*?""<>|]', 'g');

            if (value === '1')
              filename = document.getElementById('NewEnvTextBox');
            else if (value === '3')
              filename = document.getElementById('saveAsFileName');
            else
              filename = document.getElementById('blankEnvtextbox');

            filename.value = trimStr(filename.value);

            if (filename.value === '.xml') {
              alert("File name cannot be '.xml'. Please enter file name.");
              filename.focus();
            }
            else if (filename.value === '') {
              alert("File name cannot be all spaces or empty. Please enter file name.");
              filename.focus();
            }
            else if ((filename.value).search(regEx) != -1) {
              alert("Invalid characters ',/\:*?\"<>|' found in filename. Please enter a valid file name.");
              filename.focus();
            }
            else {
              var form = document.forms['treeForm'];
              addFileExtToSourceFile(value);
              var foundFile = false;
              for (var i = 0; i < files.length; i++) {
                if (filename.value === files[i]) {
                  foundFile = true;
                  var errorString = filename.value + " already exists. Do you want to overwrite the file?";
                  var r = confirm(errorString);
                  if (r == true)
                    foundFile = false;
                  else (r == false)
                  filename.focus();
                  break;
                }
              }
              if (!foundFile && value === '1') {
                form.wizfile.value = document.getElementById('NewEnvTextBox').value;
                lockEnvForWizard();
              }
              else if (!foundFile && value === '0')
                handleBlankEnvironment();
              else if (!foundFile && value === '3') {
                top.document.envSaveAsDialog.hide();
                saveEnvironment(filename.value);
              }
            }
            getWaitDlg().hide();
          }
        }
      }
    },
    failure: function(o) {
      getWaitDlg().hide();
    },
    scope: this
  },
  getFileName(true, true) + 'Params=' + params);
}

function enableCurrentOption(value)
{
  if(value === 0)
  {
    document.getElementById('fileDropDownMenu').disabled=true;
    document.getElementById('blankEnvtextbox').disabled=false;
    document.getElementById('NewEnvTextBox').disabled=true;
    document.getElementById('sumDropDownMenu').disabled=true;
  }
  else if (value === 1)
  {
    document.getElementById('fileDropDownMenu').disabled=true;
    document.getElementById('blankEnvtextbox').disabled=true;
    document.getElementById('NewEnvTextBox').disabled=false;
    document.getElementById('sumDropDownMenu').disabled=true;
  }
  else if(value === 2)
  {
    document.getElementById('fileDropDownMenu').disabled=false;
    document.getElementById('blankEnvtextbox').disabled=true;
    document.getElementById('NewEnvTextBox').disabled=true;
    document.getElementById('sumDropDownMenu').disabled=true;
  }
  else if(value === 4)
  {
    document.getElementById('fileDropDownMenu').disabled=true;
    document.getElementById('blankEnvtextbox').disabled=true;
    document.getElementById('NewEnvTextBox').disabled=true;
    document.getElementById('sumDropDownMenu').disabled=false;
  }
}

function handleBlankEnvironment() {
  document.forms['treeForm'].sourcefile.value = document.getElementById('blankEnvtextbox').value;
  var xmlArgs = 'reloadEnv=true::lockEnv=false::createFile=true';

  YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/GetNavTreeDefn', {
    success: function(o) {
      if (o.responseText.indexOf("<?xml") === 0) {
        var loc = window.location.href.split(/\?/g);
        var newwin = top.open(loc[0] + "?sourcefile=" + document.forms['treeForm'].sourcefile.value, "_self");  
      }
      else if (o.responseText.indexOf("<html") === 0) {
        var temp = o.responseText.split(/td align=\"left\">/g);
        var temp1 = temp[1].split(/<\/td>/g);
        alert(temp1[0]);
      }
    },
    failure: function(o) {
      top.document.stopWait();
      alert(o.statusText);
    },
    scope: this
  },
  getFileName(true) + 'XmlArgs=' + xmlArgs);
}

function clearTextBox(textBoxName)
{
  var pattern=/<Enter/;
  if( (textBoxName.value).match(pattern) != null)
    textBoxName.value ='';
}

function addFileExtToSourceFile(mode)
{
 var filename ='' ;
 switch(mode)
 {
   case '0':
     filename = document.getElementById('blankEnvtextbox');
     break;
   case '1':
     filename = document.getElementById('NewEnvTextBox');
     break;
   case '2':
     filename = document.getElementById('fileDropDownMenu');
     break;
   case '3':
     filename = document.getElementById('saveAsFileName');
     break;
   case '4':
     filename = document.getElementById('sumDropDownMenu');
     break;
   default:
     break;
 }

 if (filename.value.length < 4 || (filename.value).lastIndexOf(".xml") !== filename.value.length - 4)
   filename.value += ".xml";
}

function callHtmlSummaryPage()
{
  top.document.wizardpanel.hide();
  top.document.sumPanel.hide();
  var loc = window.location.href.split(/\?/g);
  var url = loc[0] + "?sourcefile=" + document.forms['treeForm'].wizfile.value;
  
  var msgToDisplay = 'Successfully generated the file ' + '<a href="' + url + '">' + document.forms['treeForm'].wizfile.value + '</a>';
  top.document.layout.render();
  
  if (document.getElementById('ReadWrite') && !top.document.navDT)
  {
    document.getElementById('ReadWrite').disabled = true;
    document.getElementById('savebutton').disabled = true;
    document.getElementById('saveasbutton').disabled = false;
    document.getElementById('validatebutton').disabled = true;
  }
  
  document.getElementById('top1').style.display = 'block';
  getMessagePanel(msgToDisplay).show();
}

function getMessagePanel(msgToDisplay)
{
  var handleClose = function() {
    this.hide();
  };
    
  if(!top.document.messageDialogBox)
  {
    top.document.messageDialogBox = new YAHOO.widget.Dialog("messagePanel",
      { width: "300px",
        height: "100px",
        resizable: false,
        fixedcenter: true,
        close: false,
        draggable: false,
        zindex:4,
        visible: true,
        modal: true
      });
  }
  
  top.document.messageDialogBox.renderEvent.subscribe(function() {
     if(top.document.navDT){
       this.cfg.setProperty("modal", true);
     }
     else if( document.forms['treeForm'].sumparams.value === '1' ) {
       this.cfg.setProperty("modal", true);
       if(top.document.layout.getUnitByPosition('left'))
         top.document.layout.getUnitByPosition('left').close(true);
     }
     else{
       if(top.document.layout.getUnitByPosition('left'))
         top.document.layout.getUnitByPosition('left').close(true);
       this.cfg.setProperty("modal", false);
     }
  });

  if(top.document.navDT || document.forms['treeForm'].sumparams.value === '1'){
     top.document.messageDialogBox.cfg.queueProperty("buttons", [{text: "Close", handler: handleClose, isDefault: true}]);
  }

  top.document.messageDialogBox.setBody(msgToDisplay);
  document.getElementById('messagePanel').style.display = 'block' ;
  top.document.messageDialogBox.render(document.body);
  top.document.messageDialogBox.center();
  
  return top.document.messageDialogBox;
}     

function checkForEE() {
  if (document.forms['treeForm'].wizops.value != '3') {
    alert("This operation is only supported in Enterprise and above editions. Please contact HPCC Systems at http://www.hpccsystems.com/contactus");
    return false;
  }
  return true;
}

function versionOperation(callbackCE, callbackEE) {
  var params = "queryType=customType::params=wizops";
  YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/GetValue', {
    success: function(o) {
      getWaitDlg().hide();
      if (o.responseText.indexOf("<?xml") === 0) {
        var xml = o.responseText.split(/<ReqValue>/g);
        var xml1 = xml[1].split(/<\/ReqValue>/g);
        var form = document.forms['treeForm'];

        if (xml1.length > 0) {
          var arrayXml = xml1[0].split(",");
          for (var j = 0; j < arrayXml.length; j++) {
            var keyValue = arrayXml[j].split("=");
            var key = keyValue[0];
            var value = keyValue[1];
            if (key == "wizops") {
              form.wizops.value = value;
              if (value === "3") {
                if (callbackEE)
                  callbackEE();
              }
              else if (callbackCE)
                callbackCE();
            }
          }
        }
      }
    },
    failure: function(o) {
      getWaitDlg().hide();
    },
    scope: this
  },
      getFileName(true) + 'Params=' + params);
}
function showTooltipForButtons(event) {
 if (document.forms['treeForm'].wizops.value !== '3') {
    if (!top.document.EEOnlyTooltip) {
      top.document.EEOnlyTooltip = new YAHOO.widget.Tooltip("EEOnlyTooltip", {
        width: "300px",
        showDelay: 25,
        text: "Support for this operation is coming soon."
      });
    }

    var xy = [parseInt(event.clientX, 10) + 10, parseInt(event.clientY, 10) + 10];
    top.document.EEOnlyTooltip.cfg.setProperty('xy', xy);
    top.document.EEOnlyTooltip.show();
    top.document.EEOnlyTooltip.bringToTop();
    top.document.hideTimer = window.setTimeout(function() {
    top.document.EEOnlyTooltip.hide();
    }, 1000);
  }
}

function focusIPList() {
   var t=document.getElementById('ipListText');
    len=t.value.length;
    if(t.setSelectionRange){
      t.setSelectionRange(len,len)
      t.focus()
    }else if(t.createTextRange){
      var rn=t.createTextRange();
      rn.moveStart('character',len)
      rn.select()
   }
} 

function getSummaryPage()
{
  summaryPageForAdvance();
 
  var fileopened = window.location.href.split(/\?/g);
  if (fileopened.length <= 1) {
    top.document.layout.render();
    document.getElementById('top1').style.display = 'block';
    top.document.layout.getUnitByPosition('left').close(true);
  }
  
  document.forms['treeForm'].sumparams.value = '1';
  updateWizCtrls();
  return;
}

function onSelectComputersContextMenuBeforeShow(p_sType, p_aArgs) {
  if (!this.configContextMenuItems) {
    this.configContextMenuItems = {
      "Select": [{ text: "Select/Unselect", onclick: { fn: onMenuItemSelectUnselect} }]
    };
  }

  if (this.getRoot() === this) {
    this.clearContent();
    this.addItems(this.configContextMenuItems['Select']);

    if (top.document.forms['treeForm'].isLocked.value === 'false') {
      var groups = this.getItemGroups();
      for (iGroup = 0; iGroup < groups.length; iGroup++) {
        if (typeof (groups[iGroup]) !== 'undefined')
          for (i = 0; i < groups[iGroup].length; i++)
          groups[iGroup][i].cfg.setProperty("disabled", true);
      }
    }

    this.render();
  }
}

function onMenuItemSelectUnselect() {
  if (top.document.forms['treeForm'].isLocked.value === 'false')
    return;

  var oTarget = this.parent.contextEventTarget;
  var Dom = YAHOO.util.Dom;
  var oSelectedTR = oTarget.nodeName.toUpperCase() === "TR" ?
                            oTarget : Dom.getAncestorByTagName(oTarget, "TR");
  var dt = this.parent.dt;
  var recSet = dt.getRecordSet();
  var record = recSet.getRecord(oSelectedTR.id);
  var selRows = dt.getSelectedRows();

  if (selRows.length > 0) {
    var flag = dt.getRecord(selRows[0]).getData('check');
    for (var idx = 0; idx < selRows.length; idx++) {
      var rec = dt.getRecord(selRows[idx]);

      rec.setData('check', !flag);
    }
  }
  else  {
   var flag = record.getData('check');
   record.setData('check', !flag);
  }

  top.document.getElementById('SelectAllComputers').checked = false;
  dt.render();
}

function trimStr(str)
{
  return str.replace(/^\s\s*/, '').replace(/\s\s*$/, '');
}
