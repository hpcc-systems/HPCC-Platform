/*##############################################################################

## Copyright © 2011 HPCC Systems.  All rights reserved.
############################################################################## */

function getNextAtDepth (dt, id, depth, prev) {
  if (typeof (depth) === 'undefined' || typeof (id) === 'undefined' || id === 0 || depth === -1)
    return;

  var ret, rec;
  var recSet = dt.getRecordSet();
  var recSetLen = recSet.getLength();
  for (var i = 0; i < recSetLen; i++) {
    var r = recSet.getRecord(i);
    if (r.getData('depth') === depth) {
      if (prev && r.getData('id') < id)
        ret = r;

      if (r.getData('id') === id) {
        rec = r;
        if (prev)
          break;
      }

      if (r.getData('id') > id) {
        ret = r;
        break;
      }
    }
  }

  if (!ret && rec) {
    return getNextAtDepth(dt, rec.getData('parent'), depth - 1, prev);
  }

  return ret;
}
function handleComplexTableKeyDown(oArgs, dt) {
  var Dom = YAHOO.util.Dom;
  if (oArgs.event.keyCode === 38 || oArgs.event.keyCode === 40) {
    var rec = dt.getRecord(dt.getLastSelectedRecord());
    if (oArgs.event.keyCode === 40) {
      var prevSelRec = dt.getRecord(dt.getRecordIndex(rec) - 1);
      var tdEl = dt.getFirstTdEl(prevSelRec);
      var children = Dom.getChildren(tdEl);
      if (Dom.hasClass(children[0].children[0].children[0], 'yui-button') &&
              Dom.hasClass(children[0].children[0].children[0], 'buttoncollapsed')) {
        var next = getNextAtDepth(dt, prevSelRec.getData('id'), prevSelRec.getData('depth'), oArgs.event.keyCode === 38);

        if (next && next !== rec) {
          dt.unselectRow(rec);
          dt.selectRow(next);
        }
        else if (!next) {
          dt.unselectRow(rec);
          dt.selectRow(prevSelRec);
        }
      }
    }
    else {
      var prevSelRec = dt.getRecord(dt.getRecordIndex(rec) + 1);
      var tmprec = rec;
      while (true) {
        var tmpparrec = getRecord(dt, tmprec.getData('parent'));
        var tdEl = dt.getFirstTdEl(tmpparrec);
        var children = Dom.getChildren(tdEl);
        if (Dom.hasClass(children[0].children[0].children[0], 'yui-button') &&
              Dom.hasClass(children[0].children[0].children[0], 'buttoncollapsed')) {
          tmprec = tmpparrec;
          continue;
        }
        else {
          var next = tmprec; //getNextAtDepth(dt, tmprec.getData('id'), tmprec.getData('depth'), true);

          if (next && next !== rec) {
            dt.unselectRow(rec);
            dt.selectRow(next);
          }
          break;
        }
      }
    }
  }
}

function getRecord(table, id) {
  var recSet = table.getRecordSet();
  var recSetLen = recSet.getLength();
  for (var i = 0; i < recSetLen; i++) {
    var r = recSet.getRecord(i);
    if (r.getData('id') === id) {
      return r;
    }
  }
}

function setFocusToNavTable() {
  var tabView = top.document.RightTabView;
  top.document.navDT.focus();
  if (tabView) {
    var dt = tabView.get("activeTab").dt;
    saveOpenEditors(dt);
    dt.fireEvent("tableBlurEvent");

    if (dt.subTables) {
      for (var stIdx = 0; stIdx < dt.subTables.length; stIdx++) {
        dt.subTables[stIdx].oDT.fireEvent("tableBlurEvent");
      }
    }
  }
}

function setFocusToActiveTabDT() {
  var tabView = top.document.RightTabView;
  if (tabView) {
    tabView.get("activeTab").dt.focus();
    top.document.navDT.fireEvent("tableBlurEvent");
  }
}

function setFocusToTable(dt) {
  dt.focus();
  top.document.navDT.fireEvent("tableBlurEvent");
}

function clickCurrentSelOrName(dt, name, donotclick) {
  var rec = getCurrentSelRec(dt, name);

  if (typeof (rec) !== 'undefined') {
    expandRecordWithId(dt, rec.getData('parent'));
    var el = dt.getTrEl(rec);
    dt.unselectAllCells();
    dt.unselectAllRows();
    var tdEl = dt.getFirstTdEl(rec);
    if (!donotclick)
      YAHOO.util.UserAction.click(tdEl);
    top.document.lastSelectedRow = rec.getData('Name');
  }
}

function getCurrentSelRec(dt, name) {
  var rec;
  if (typeof (name) === 'undefined') {
    var selectedRows = dt.getSelectedRows();

    if (typeof (selectedRows) !== 'undefined') {
      if (selectedRows.length > 0)
        rec = dt.getRecord(selectedRows[0]);
    }
  }
  else {
    var recSet = dt.getRecordSet();
    var recSetLen = recSet.getLength();
    for (var i = 0; i < recSetLen; i++) {
      var r = recSet.getRecord(i);
      if (r.getData('Name') === name) {
        rec = r;
        break;
      }
    }
  }

  return rec;
}

function getFirstNodeName(dt, parentName,fldType) {
  var recSet = dt.getRecordSet();
  var recSetLen = recSet.getLength();
  var rec;
  if(fldType === '')
    fldType = 'Name';
  for (var i = 0; i < recSetLen; i++) {
    var r = recSet.getRecord(i);
    if (r.getData(fldType) === parentName) {
      rec = r;
      break;
    }
  }
  
  if (typeof (rec) === 'undefined')
    return;

  for (var i = 0; i < recSetLen; i++) {
    var r = recSet.getRecord(i);
    if (r.getData('parent') === rec.getData('id')) {
      return r;
      break;
    }
  }
}

function selectRecordAndClick(dt, recName, flag, recField) {
  var recSet = dt.getRecordSet();
  var recSetLen = recSet.getLength();
  if (!recField)
    recField = "name";
  var self = dt;
  for (var i = 0; i < recSetLen; i++) {
    var r = recSet.getRecord(i);
    if (r.getData(recField) === recName) {
      dt.selectRow(r);
      if (flag) {
        var tdEl = dt.getFirstTdEl(r);
        tdEl.click();
        break;
      }
    }
  }
}

function saveOpenEditors(myDataTable) {
  if (typeof (myDataTable.editors) !== 'undefined') {
    for (var colIdx = 1; colIdx < 7; colIdx++)
      if (myDataTable.editors[colIdx].isActive) {
      myDataTable.editors[colIdx].save();
    }
  }

  if (myDataTable.subTables) {
    for (var stIdx = 0; stIdx < myDataTable.subTables.length; stIdx++) {
      saveOpenEditors(myDataTable.subTables[stIdx].oDT);
    }
  }
}

function isValidIPAddress(ipList, theName, ignoredot, checkspecial) {
  var errorString = "";
  var k = 0;
  var pattern =/-/;
  ipList = ipList.replace(/;$/, "");
  var ipArr = new Array();
  ipArr = ipList.split(";");
  var ipPattern = /^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/;

  for (k = 0; k < ipArr.length; k++) {
    var IPvalue = ipArr[k];
    if (IPvalue.match(pattern) != null) {
      var newIpArr = IPvalue.split("-");
      if (newIpArr.length > 1) {
       if ( !isInteger(newIpArr[1]) || (parseInt(newIpArr[1]) > 255)){
          errorString = errorString + theName + ": " + IPvalue + " is not a valid IP address.";
       }

        IPvalue = newIpArr[0];
       }
    }
    var ipArray = IPvalue.match(ipPattern);

    if (checkspecial) {
      if (IPvalue == "0.0.0.0")
        errorString = errorString + theName + ': ' + IPvalue + ' is a special IP address and cannot be used here.';
      else if (IPvalue == "255.255.255.255")
        errorString = errorString + theName + ': ' + IPvalue + ' is a special IP address and cannot be used here.';
    }

    if (ignoredot && IPvalue === ".") 
      continue;

    if (ipArray == null)
      errorString = errorString + theName + ': ' + IPvalue + ' is not a valid IP address.';
    else {
      for (i = 0; i < 4; i++) {
        thisSegment = ipArray[i];
        if (thisSegment > 255) {
          errorString = errorString + theName + ': ' + IPvalue + ' is not a valid IP address.';
          i = 4;
        }

        if ((i == 0) && (thisSegment > 255)) {
          errorString = errorString + theName + ': ' + IPvalue + ' is a special IP address and cannot be used here.';
          i = 4;
        }
      }
    }
  }
  return errorString;
}

function isInteger(val){
if (isBlank(val)){return false;}
    for(var i=0;i<val.length;i++){
        if(!isDigit(val.charAt(i))){return false;}
    }
    return true;
}

function isBlank(val){
    if(val==null){return true;}
    for(var i=0;i<val.length;i++) {
    if ((val.charAt(i)!=' ')&&(val.charAt(i)!="\t")&&(val.charAt(i)!="\n")&&(val.charAt(i)!="\r")){return false;}
    }
    return true;
}

function isDigit(num) {
    if (num.length>1){return false;}
    var string="1234567890";
    if (string.indexOf(num)!=-1){return true;}
    return false;
}

function removeSpaces(string) {
 return string.split(' ').join('');
}

function addUniqueToArray(arr, itm) {
  var flag = false;
  var s = typeof arr;
    if (s === 'object') {
        if (arr) {
          if (typeof arr.length === 'number' &&
                    !(arr.propertyIsEnumerable('length')) &&
                    typeof arr.splice === 'function')
            flag = true;
          else
            return;
        } 
    }

    if (arr.length == 0)
      flag = true;
    else
      for (var i = 0; i < arr.length; i++) {
        if (arr[i] == itm) {
          flag = false;
          break;
        }
    }

    if (flag)
      arr[arr.length] = itm;
}
