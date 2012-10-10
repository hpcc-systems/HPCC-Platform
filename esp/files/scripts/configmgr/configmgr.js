/*##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
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
function createRowArraysForComp(compName, rows) {
  if (typeof (compTabs[compName]) === 'undefined')
    return;

  for (var i = 0; i < compTabs[compName].length; i++)
    rows[compTabs[compName][i]] = new Array();
}

function createTablesForComp(compName, rows) {
  if (typeof (compTabs[compName]) === 'undefined')
    return;

  for (var i = 0; i < compTabs[compName].length; i++) {
    if (compTabs[compName][i] !== 'Servers' &&
        compTabs[compName][i] !== 'Agents' &&
        compTabs[compName][i] !== 'Topology')
      createTable(rows[compTabs[compName][i]], compTabs[compName][i], i, compName);
  }
}

function argsToXml(category, params, attrName, oldValue, newValue, rowIndex, onChange) {
  var xmlArgs = "<XmlArgs><Setting category=\"" + category;
  xmlArgs += "\" params=\"" + params;
  xmlArgs += "\" attrName=\"" + attrName;
  xmlArgs += "\" rowIndex=\"" + rowIndex;
  xmlArgs += "\" onChange=\"" + onChange;
  xmlArgs += "\" oldValue=\"" + escape(oldValue);
  xmlArgs += "\" newValue=\"" + escape(newValue) + "\"/></XmlArgs>";

  return xmlArgs;
}

var formatterDispatcher = function(elCell, oRecord, oColumn, oData) {
  var meta = oRecord.getData(oColumn.key + '_ctrlType'); //('meta_' + oColumn.key);
  //oColumn.editorOptions = meta.editorOptions;
  switch (meta) {
    case '0':
      YAHOO.widget.DataTable.formatNumber.call(this, elCell, oRecord, oColumn, oData);
      break;
    case '4':
      YAHOO.widget.DataTable.formatDropdown.call(this, elCell, oRecord, oColumn, oData);
      break;
    case 5:
      elCell.innerHTML = oData.replace(/./g, '*');
      break;
    case '3':
      elCell.innerHTML = oData;
      break;
    case '1':
    default:
      YAHOO.widget.DataTable.formatText.call(this, elCell, oRecord, oColumn, oData);
      break;
  }

  var notInEnv = oRecord.getData('_not_in_env');

  if (notInEnv === 1)
    YAHOO.util.Dom.addClass(elCell, 'not_in_env');
};

function createSubTable(rows, tabDivName, index, name) {
  var tabNameTemp = tabDivName + 'On';
  var myColumnDefs2;
  var myDataSource2 = new YAHOO.util.DataSource(rows);
  myDataSource2.responseType = YAHOO.util.DataSource.TYPE_JSARRAY;
  myColumnDefs2 = new Array();

  var colIndex1 = 0;
  var colIndex2 = 0;
  var columnFields = new Array();
  var tableName = name.split("_");
  var newName = tableName[1];

  for (var realCol in rows[0]) {
    if ((realCol.indexOf('_') === -1) && (realCol.indexOf('params') === -1) && (realCol.indexOf('compType') === -1)) {
      newName = cS[realCol + tableName[1]].tab;
      break;
    }
  }

  for (var i in rows[0]) {
    var lbl = i;
    if (rows[0][i + "_caption"])
      lbl = rows[0][i + "_caption"];

    //if((i.indexOf('_extra') === -1) && (i.indexOf('_ctrlType') === -1) && (i.indexOf('params') === -1) && (i.indexOf('compType') === -1))
    if ((i.indexOf('_') === -1) && (i.indexOf('params') === -1) && (i.indexOf('compType') === -1)) {
      if (typeof (colIndex) !== 'undefined' && colIndex[i + newName])
        myColumnDefs2[colIndex[i + newName]] = { key: i, label: lbl, minWidth: 200, /*maxAutoWidth:auto,*/scrollable: true, resizeable: true, formatter: formatterDispatcher, editor: new YAHOO.widget.BaseCellEditor() };
      else
        myColumnDefs2[colIndex1++] = { key: i, minWidth: 200, label: lbl, /*maxAutoWidth:auto,*/scrollable: true, resizeable: true, formatter: formatterDispatcher, editor: new YAHOO.widget.BaseCellEditor() };
    }

    columnFields[colIndex2++] = i;
  }
  //Add missing columns or columns for tables with no rows
  if (typeof (tabCols) !== 'undefined' && YAHOO.lang.isArray(tabCols) && typeof (tabCols[newName]) !== 'undefined' && tabCols[newName].length > 0) {
    for (var colHdrIndex = 0; colHdrIndex < tabCols[newName].length; colHdrIndex++) {
      if (typeof (tabCols[newName][colHdrIndex]) !== 'undefined') {
        var flag = false;
        for (var tmpIdx = 0; tmpIdx < myColumnDefs2.length; tmpIdx++) {
          if (typeof (myColumnDefs2[tmpIdx]) !== 'undefined' && myColumnDefs2[tmpIdx]['label'] === tabCols[newName][colHdrIndex]) {
            flag = true;
            break;
          }
        }

        if (flag)
          continue;

        myColumnDefs2[myColumnDefs2.length] = { key: tabCols[newName][colHdrIndex], minWidth: 200, /*maxAutoWidth:auto,*/scrollable: true, resizeable: true, formatter: formatterDispatcher, editor: new YAHOO.widget.BaseCellEditor() };
        columnFields[colIndex2++] = tabCols[newName][colHdrIndex];
      }
    }
  }

  myDataSource2.responseSchema = { fields: columnFields };

  tabNameTemp = tabDivName + 'Tab';
  var parentdiv = document.getElementById(tabNameTemp);
  var newDiv = document.createElement('div');
  YAHOO.util.Dom.setStyle(newDiv, 'width', '100%');
  parentdiv.parentNode.appendChild(newDiv);

  var newColDefs = new Array();
  var nIndx = 0;
  for (idx = 0; idx < colIndex2; idx++) {
    if (typeof (myColumnDefs2[idx]) !== 'undefined')
      newColDefs[nIndx++] = myColumnDefs2[idx];
  }

  if (nIndx === 1)
    newColDefs[0].minWidth = '400';

  var myDataTable2 = new YAHOO.widget.DataTable(newDiv, newColDefs/*myColumnDefs2*/, myDataSource2, { width: "100%", caption: "<B>" + newName + "</B>" });
  // Set up editing flow   
  var highlightEditableCell = function(oArgs) {
    var elCell = oArgs.target;
    if (YAHOO.util.Dom.hasClass(elCell, "yui-dt-editable")) {
      this.highlightCell(elCell);
    }
  };

  var oContextMenu2 = new YAHOO.widget.ContextMenu(newName + 'Menu', {
    trigger: newDiv,
    lazyload: true
  });

  myDataTable2.tt = new YAHOO.widget.Tooltip(newName + "tooltip");

  oContextMenu2.dt = myDataTable2;
  oContextMenu2.subscribe("beforeShow", onContextMenuBeforeShowRegular);

  myDataTable2.subscribe("cellMouseoverEvent", function(oArgs) {
    var target = oArgs.target;
    var cols = this.getColumnSet();
    var txt = cS[cols.keys[target.cellIndex].key + tableName[1]].tip;

    if (cS[cols.keys[target.cellIndex].key + tableName[1]].defaultValue)
      txt += "<br>Default value = '" + cS[cols.keys[target.cellIndex].key + tableName[1]].defaultValue + "'";

    if (myDataTable2.showTimer) {
      window.clearTimeout(myDataTable2.showTimer);
      myDataTable2.showTimer = 0;
    }

    var xy = [parseInt(oArgs.event.clientX, 10) + 10, parseInt(oArgs.event.clientY, 10) + 10];

    if (txt.length) {
      myDataTable2.showTimer = window.setTimeout(function() {
        myDataTable2.tt.setBody(txt);
        myDataTable2.tt.cfg.setProperty('xy', xy);
        myDataTable2.tt.show();
        myDataTable2.hideTimer = window.setTimeout(function() {
          myDataTable2.tt.hide();
        }, 5000);
      }, 500);
    }
  });
  myDataTable2.subscribe("cellMouseoutEvent", function(oArgs) {
    if (myDataTable2.showTimer) {
      window.clearTimeout(myDataTable2.showTimer);
      myDataTable2.showTimer = 0;
    }
    if (myDataTable2.hideTimer) {
      window.clearTimeout(myDataTable2.hideTimer);
      myDataTable2.hideTimer = 0;
    }
    myDataTable2.tt.hide();
  });
  //myDataTable.subscribe("cellClickEvent", myDataTable.onEventShowCellEditor);   
  myDataTable2.subscribe("rowClickEvent", function(oArgs) {
    //this messes with the cell click event by taking away the focus away from the cell editor
    /*myDataTable2.clearTextSelection();*/
    myDataTable2.onEventSelectRow(oArgs)
  });
  myDataTable2.subscribe("tableKeyEvent", function(oArgs) {
    if (oArgs.event.keyCode === 9) {
      showCurrentCellEditor(myDataTable2);
      YAHOO.util.Event.stopEvent(oArgs.event);
    }
    else if (oArgs.event.keyCode === 192 && oArgs.event.ctrlKey === true)
      setFocusToNavTable();
  });

  myDataTable2.subscribe("tableFocusEvent", function(oArgs) {
    var Dom = YAHOO.util.Dom;
    var selRows = myDataTable2.getSelectedRows();
    for (var idx = 0; idx < selRows.length; idx++) {
      Dom.removeClass(myDataTable2.getTrEl(selRows[idx]), 'outoffocus');
    }

//    var dt = myDataTable2.parentDT;
//    dt.fireEvent("tableBlurEvent");
//    if (dt.subTables) {
//      for (var stIdx = 0; stIdx < dt.subTables.length; stIdx++) {
//        if (myDataTable2 !== dt.subTables[stIdx].oDT)
//          dt.subTables[stIdx].oDT.fireEvent("tableBlurEvent");
//      }
//    }
  });
  myDataTable2.subscribe("tableBlurEvent", function(oArgs) {
    var Dom = YAHOO.util.Dom;
    var selRows = myDataTable2.getSelectedRows();
    for (var idx = 0; idx < selRows.length; idx++) {
      Dom.addClass(myDataTable2.getTrEl(selRows[idx]), 'outoffocus');
    }
  });
  
  myDataTable2.getDefault = function(oTarget, record) {
    var column = this.getColumn(oTarget);
    if( (column.key + tableName[1]) in cS ) {
      if (cS[column.key + tableName[1]].defaultValue)
        return cS[column.key + tableName[1]].defaultValue;
    }
    else
       return '';
    
  }
  
  myDataTable2.subscribe("cellMousedownEvent", function(oArgs) {
    //if left button click return as it is handled in handleConfigCellClickEvent
    if (oArgs.event.button === 1 || oArgs.event.button === 0)
      return;

    var selRows = this.getSelectedRows();
    if (selRows && selRows.length > 0) {
      if (this.isSelected(this.getRecord(oArgs.target)))
        return;
    }
    this.onEventSelectRow(oArgs);
  });
  myDataTable2.subscribe("cellClickEvent", function(oArgs) { handleConfigCellClickEvent(oArgs, this, false); });

  myDataTable2.cleanup = function() {
    this.unsubscribeAll("cellMouseoverEvent");
    this.unsubscribeAll("rowClickEvent");
    this.unsubscribeAll("tableKeyEvent");
    this.unsubscribeAll("cellMousedownEvent");
    this.unsubscribeAll("tableFocusEvent");
    this.unsubscribeAll("tableBlurEvent");
    this.unsubscribeAll("cellClickEvent");
    this.unsubscribeAll("rowSelectEvent");

    oContextMenu2.dt = null;
    oContextMenu2.unsubscribeAll("beforeShow");
  }
  return { oDS: myDataSource2, oDT: myDataTable2, oName: name, oTableName: newName};
}



function createTable(rows, tabDivName, index, compName) {
  if (!top.document.RightTabView)
    top.document.RightTabView = new YAHOO.widget.TabView('tabviewcontainer');

  var tabView = top.document.RightTabView;
  var tabs = tabView.get("tabs");
  var tab;
  for (i = 0; i < tabs.length; i++) {
    if (tabs[i].get("label") === tabDivName) {
      tab = tabs[i];
      break;
    }
  }

  var listenEvent = "activeChange";
  if (typeof (tab) === 'undefined') {
    tab = window;
    listenEvent = "load";
  }

  //For simple view, some of the tabs may not have any rows. In that case, do not add
  if ((tabDivName !== "Instances" && tabDivName !== "Notes" && (rows.length == 0 && !(typeof (tabCols) !== 'undefined' && YAHOO.lang.isArray(tabCols) && typeof (tabCols[tabDivName]) !== 'undefined' && tabCols[tabDivName].length > 0)) && (top.document.forms['treeForm'].displayMode.value === 1)))// ||
  //(tabDivName === "URL Authentication" || tabDivName === "Feature Authentication" || tabDivName === "Configuration" || tabDivName === "Gateway"))
  {
    tabView.removeTab(tab);
    return;
  }

  if (typeof (hiddenTabs) !== 'undefined' && YAHOO.lang.isArray(hiddenTabs) && isPresentInArr(hiddenTabs[compName], tabDivName))
    return;

  var flag = tab.addListener(listenEvent, function(event) {
    if (!event.newValue) return;
    YAHOO.example.Basic = function() {
      var tabNameTemp = tabDivName + 'On';
      var myColumnDefs;
      var myDataSource = new YAHOO.util.DataSource(rows);
      myDataSource.responseType = YAHOO.util.DataSource.TYPE_JSARRAY;
      myColumnDefs = new Array();

      var colIndex1 = 0;
      var colIndex2 = 0;
      var columnFields = new Array();
      var subTables = new Array();

      if (typeof (rows) !== 'undefined' && rows.length > 0) {
        for (var i in rows[0]) {
          var lbl = i;
          if (rows[0][i + "_caption"])
            lbl = rows[0][i + "_caption"];
          //if((i.indexOf('_extra') === -1) && (i.indexOf('_ctrlType') === -1) && (i.indexOf('params') === -1) && (i.indexOf('compType') === -1))
          if ((i.indexOf('_') === -1) && (i.indexOf('params') === -1) && (i.indexOf('compType') === -1)) {
            if (typeof (colIndex) !== 'undefined' && colIndex[i + tabDivName])
              myColumnDefs[colIndex[i + tabDivName]] = { key: i, label: lbl, minWidth: 200, /*maxAutoWidth:auto,*/scrollable: true, resizeable: true, formatter: formatterDispatcher, editor: new YAHOO.widget.BaseCellEditor() };
            else
              myColumnDefs[colIndex1++] = { key: i, label: lbl, minWidth: 200, /*maxAutoWidth:auto,*/scrollable: true, resizeable: true, formatter: formatterDispatcher, editor: new YAHOO.widget.BaseCellEditor() };
          }
          else if ((i.indexOf('_') === 0) && YAHOO.lang.isArray(rows[0][i]))
            subTables[subTables.length] = createSubTable(rows[0][i], tabDivName, 0, i);
          columnFields[colIndex2++] = i;
        }
      }
      //Add missing columns or columns for tables with no rows
      if (typeof (tabCols) !== 'undefined' && YAHOO.lang.isArray(tabCols) && typeof (tabCols[tabDivName]) !== 'undefined' && tabCols[tabDivName].length > 0) {
        for (var colHdrIndex = 0; colHdrIndex < tabCols[tabDivName].length; colHdrIndex++) {
          if (typeof (tabCols[tabDivName][colHdrIndex]) !== 'undefined') {
            var flag = false;
            for (var tmpIdx = 0; tmpIdx < myColumnDefs.length; tmpIdx++) {
              if (typeof (myColumnDefs[tmpIdx]) !== 'undefined' && myColumnDefs[tmpIdx]['label'] === tabCols[tabDivName][colHdrIndex]) {
                flag = true;
                break;
              }
            }

            if (flag)
              continue;
            else if (tabCols[tabDivName][colHdrIndex].indexOf('_') === 0) {
              var subTablePresent = false;
              if (subTables) {
                for (var idx = 0; idx < subTables.length; idx++) {
                  var tableName = tabCols[tabDivName][colHdrIndex].split("_");
                  if (tableName[1] === subTables[idx].oTableName) {
                    subTablePresent = true;
                    break;
                  }
                }
              }

              if (!subTablePresent)
                subTables[subTables.length] = createSubTable({}, tabDivName, 0, tabCols[tabDivName][colHdrIndex]);
            }
            else {
              myColumnDefs[myColumnDefs.length] = { key: tabCols[tabDivName][colHdrIndex], minWidth: 200, /*maxAutoWidth:auto,*/scrollable: true, resizeable: true, formatter: formatterDispatcher, editor: new YAHOO.widget.BaseCellEditor() };
              columnFields[colIndex2++] = tabCols[tabDivName][colHdrIndex];
            }
          }
        }
      }

      myDataSource.responseSchema = { fields: columnFields };
      tabNameTemp = tabDivName + 'Tab';

      var newColDefs = new Array();
      var nIndx = 0;
      for (idx = 0; idx < colIndex2; idx++) {
        if (typeof (myColumnDefs[idx]) !== 'undefined')
          newColDefs[nIndx++] = myColumnDefs[idx];
      }

      var myDataTable = new YAHOO.widget.DataTable(tabNameTemp, newColDefs/*myColumnDefs*/, myDataSource, { width: "100%" });
      tab.dt = myDataTable;
      tab.dataSource = myDataSource;
      myDataTable.subTables = subTables;
      for (i = 0; i < subTables.length; i++)
        subTables[i].oDT.parentDT = myDataTable;
      // Set up editing flow   
      var highlightEditableCell = function(oArgs) {
        var elCell = oArgs.target;
        if (YAHOO.util.Dom.hasClass(elCell, "yui-dt-editable")) {
          this.highlightCell(elCell);
        }
      };

      var oContextMenu = new YAHOO.widget.ContextMenu(tabDivName + 'Menu', {
        trigger: tabNameTemp,
        lazyload: true
      });

      oContextMenu.dt = myDataTable;
      oContextMenu.subscribe("beforeShow", onContextMenuBeforeShowRegular);
      top.document.ContextMenuCenter = oContextMenu;

      myDataTable.tt = new YAHOO.widget.Tooltip(tabDivName + "tooltip");

      myDataTable.subscribe("cellMouseoverEvent", function(oArgs) {
        var target = oArgs.target;
        var record = this.getRecord(target);
        var txt = "";

        if (record.getData('_not_in_env') === 1)
          txt += "This default optional value is not present in the environment. Use context menu to write to environment.<br>";

        //firefox has option to control status bar msg writing that is off by default. 
        //need to doc this to let the user enable the setting.
        if (record.getData('_key')) {
          if (cS[record.getData('_key')].tip)
            txt += cS[record.getData('_key')].tip;
          else if (cS[record.getData('_key') + tabDivName].tip)
            txt += cS[record.getData('_key') + tabDivName].tip;

          if (cS[record.getData('_key')].defaultValue)
            txt += "<br>Default value = '" + cS[record.getData('_key')].defaultValue + "'";
          else if (cS[record.getData('_key') + tabDivName].defaultValue)
            txt += "<br>Default value = '" + cS[record.getData('_key') + tabDivName].defaultValue + "'";
        }
        else {
          var cols = this.getColumnSet();
          var tmp = record.getData('params').split("subType=");
          var tmp1;
          if (tmp.length > 1)
            tmp1 = tmp[1].split("::");
          if (cS[cols.keys[target.cellIndex].key + tmp1[0]].tip)
            txt += cS[cols.keys[target.cellIndex].key + tmp1[0]].tip;

          if (cS[cols.keys[target.cellIndex].key + tmp1[0]].defaultValue)
            txt += "<br>Default value = '" + cS[cols.keys[target.cellIndex].key + tmp1[0]].defaultValue + "'";
        }

        if (myDataTable.showTimer) {
          window.clearTimeout(myDataTable.showTimer);
          myDataTable.showTimer = 0;
        }

        var xy = [parseInt(oArgs.event.clientX, 10) + 10, parseInt(oArgs.event.clientY, 10) + 10];

        if (txt.length) {
          myDataTable.showTimer = window.setTimeout(function() {
            myDataTable.tt.setBody(txt);
            myDataTable.tt.cfg.setProperty('xy', xy);
            myDataTable.tt.show();
            myDataTable.hideTimer = window.setTimeout(function() {
              myDataTable.tt.hide();
            }, 5000);
          }, 500);
        }

      });
      myDataTable.subscribe("cellMouseoutEvent", function(oArgs) {
        if (myDataTable.showTimer) {
          window.clearTimeout(myDataTable.showTimer);
          myDataTable.showTimer = 0;
        }
        if (myDataTable.hideTimer) {
          window.clearTimeout(myDataTable.hideTimer);
          myDataTable.hideTimer = 0;
        }
        myDataTable.tt.hide();
      });
      //myDataTable.subscribe("cellClickEvent", myDataTable.onEventShowCellEditor);
      myDataTable.subscribe("rowClickEvent", function(oArgs) {
        //this messes with the cell click event by taking away the focus away from the cell editor
        /*myDataTable.clearTextSelection();*/
        myDataTable.onEventSelectRow(oArgs)
      });
      myDataTable.subscribe("tableKeyEvent", function(oArgs) {
        if (oArgs.event.keyCode === 9) {
          showCurrentCellEditor(myDataTable);
          YAHOO.util.Event.stopEvent(oArgs.event);
        } else if (oArgs.event.keyCode === 192 && oArgs.event.ctrlKey === true)
          setFocusToNavTable();
      });
      myDataTable.subscribe("cellMousedownEvent", function(oArgs) {
        //if left button click return as it is handled in handleConfigCellClickEvent
        if (oArgs.event.button === 1 || oArgs.event.button === 0)
          return;

        var selRows = this.getSelectedRows();
         for (i = 0; i < selRows.length; i++) {
            rec = this.getRecord(selRows[i]);
            if (YAHOO.util.Dom.hasClass(this.getTrEl(rec), 'outoffocus'))
              YAHOO.util.Dom.removeClass(this.getTrEl(rec), 'outoffocus');
         }

        if (selRows && selRows.length > 0) {
          if (this.isSelected(this.getRecord(oArgs.target)))
            return;
        }

        this.onEventSelectRow(oArgs)
      });

      myDataTable.subscribe("tableFocusEvent", function(oArgs) {
        document.getElementById('hiddenButton').click();
        var Dom = YAHOO.util.Dom;
        var selRows = myDataTable.getSelectedRows();
        for (var idx = 0; idx < selRows.length; idx++) {
          Dom.removeClass(myDataTable.getTrEl(selRows[idx]), 'outoffocus');
        }
      });

      myDataTable.subscribe("tableBlurEvent", function(oArgs) {
        //var tabView = top.document.RightTabView;
        if (tabView && tabView.get("activeTab").dt !== myDataTable)
          return;
        var Dom = YAHOO.util.Dom;
        var selRows = myDataTable.getSelectedRows();
        for (var idx = 0; idx < selRows.length; idx++) {
          Dom.addClass(myDataTable.getTrEl(selRows[idx]), 'outoffocus');
        }
      });

      myDataTable.subscribe("cellClickEvent", function(oArgs) { handleConfigCellClickEvent(oArgs, this, false); });
      myDataTable.subscribe("rowSelectEvent", function(oArgs) {
        var tmpRec = oArgs.record;
        if (myDataTable.subTables && myDataTable.subTables.length > 0) {
          for (i = 0; i < myDataTable.subTables.length; i++) {
            myDataTable.subTables[i].oDS.handleResponse("", tmpRec.getData(myDataTable.subTables[i].oName), { success: myDataTable.subTables[i].oDT.onDataReturnInitializeTable,
              scope: myDataTable.subTables[i].oDT
            }, myDataTable, 999);
          }
        }
      });

      myDataTable.cleanup = function() {
        this.unsubscribeAll("cellMouseoverEvent");
        this.unsubscribeAll("rowClickEvent");
        this.unsubscribeAll("tableKeyEvent");
        this.unsubscribeAll("cellMousedownEvent");
        this.unsubscribeAll("tableFocusEvent");
        this.unsubscribeAll("tableBlurEvent");
        this.unsubscribeAll("cellClickEvent");
        this.unsubscribeAll("rowSelectEvent");
        for (var idx = 0; idx < this.subTables.length; idx++) {
          this.subTables[idx].oDT.cleanup();
          this.subTables[idx].oDT.parentDT = null;
          this.subTables[idx].oDT.destroy();
          this.subTables[idx].oDT = null;
          this.subTables[idx].oDS = null;
        }

        oContextMenu.dt = null;
        oContextMenu.unsubscribeAll("beforeShow");
      }
      
      myDataTable.getDefault = function(oTarget, record) {
        if ( record.getData('_key') in cS ) {
          if(cS[record.getData('_key')].defaultValue)
            return cS[record.getData('_key')].defaultValue ;
        }
        else if ( ( record.getData('_key') + tabDivName) in cS){
            if ( cS[record.getData('_key')+ tabDivName].defaultValue )
               return cS[record.getData('_key') + tabDivName].defaultValue ;
        }
        else {
           var cols = this.getColumnSet();
           var tmp = record.getData('params').split("subType=");
           var tmp1;
           if (tmp.length > 1)
             tmp1 = tmp[1].split("::");
           
           if( (record.getData('_key') + tmp1[0]) in cS) {
              if (cS[record.getData('_key') + tmp1[0]].defaultValue)
                return ( cS[record.getData('_key') + tmp1[0]].defaultValue);
           }
           else 
            return '';
        }
      }
      
      tabView.addListener("activeIndexChange", function(oArgs) {
        top.document.activeTab = tabView.getTab(oArgs.newValue).get("label");
        setFocusToTable(myDataTable);
        if (top.document.needsRefresh === true) {
          top.document.navDT.clickCurrentSelOrName(top.document.navDT);
          return false;
        }
      });

      tab.removeListener(listenEvent);

      if (typeof (top.document.selectRecord) !== 'undefined' && top.document.selectRecord.length > 0) {
        if (!top.document.selectRecordField || top.document.selectRecordField.length === 0)
          top.document.selectRecordField = 'name';

        top.document.navDT.selectRecordAndClick(myDataTable,
                                                         top.document.selectRecord,
                                                         top.document.selectRecordClick,
                                                         top.document.selectRecordField);
        top.document.selectRecord = '';
        top.document.selectRecordField = '';
        top.document.selectRecordClick = false;
      }
      else
        myDataTable.selectRow(0);

      if (tabView.inInit) {
        top.document.navDT.focus();
        myDataTable.fireEvent("tableBlurEvent");
        tabView.inInit = false;
      }
      else
        setFocusToTable(myDataTable);

      return { oDS: myDataSource, oDT: myDataTable };
    } ();
  });
}

function createTabDivsForComp(compName) {
  if (typeof (compTabs[compName]) === 'undefined')
    return;

  var selected = true;
  document.getElementById('tabviewcontainerul').innerHTML = '';
  var tabIdx = 0;

  for (i = 0; i < compTabs[compName].length; i++) {
    if (typeof (hiddenTabs) !== 'undefined' && YAHOO.lang.isArray(hiddenTabs) && isPresentInArr(hiddenTabs[compName], compTabs[compName][i]))
      continue;

    selected = createTabDivForComp(compTabs[compName][i], selected, tabIdx);
    tabIdx++;
  }
}

function createTabDivForComp(compName, selected, i) {
  var htmlString;
  if (selected)
    htmlString = '<li class="selected"><a id="tab' + (i + 1) + '" href="#tab' + (i + 1) + '"><em>' + compName + '</em></a></li>';
  else
    htmlString = '<li><a id="tab' + (i + 1) + '" href="#tab' + (i + 1) + '"><em>' + compName + '</em></a></li>';

  document.getElementById('tabviewcontainerul').innerHTML += htmlString;
  return false;
}

function createDivInTabsForComp(compName) {
  if (typeof (compTabs[compName]) === 'undefined')
    return;

  var htmlStringForTabs = '';
  for (i = 0; i < compTabs[compName].length; i++) {
    if (typeof (hiddenTabs) !== 'undefined' && YAHOO.lang.isArray(hiddenTabs) && isPresentInArr(hiddenTabs[compName], compTabs[compName][i]))
      continue;

    htmlStringForTabs += createDivsInTabsForComp(compTabs[compName][i], compName);
  }
   htmlStringForTabs +='<div id="testbutton"><input type="button" id="hiddenButton" name="hiddenButton" style="display:none" onclick="moveScrollBar()"></div>'; 

  document.getElementById('tabviewcontainercontent').innerHTML = htmlStringForTabs;
}

function createDivsInTabsForComp(tabName, compType) {
  var htmlString = '<div id="' + tabName + 'Tab">';

  if (typeof(viewChildNodes) !== 'undefined' && isPresentInArr(viewChildNodes, compTabToNode[tabName]))
    htmlString = '<div><div id="' + tabName + 'Tab"></div>';

  htmlString += '</div>';

  return htmlString;
}

function initItemForRoxieSlaves(item) {
  item.depth = 0;
  item.name = "Roxie Cluster";
  item.name_extra = "";
  item.name_ctrlType = 0;
  item.parent = -1;
  item.netAddress = "";
  item.netAddress_extra = "";
  item.netAddress_ctrlType = 0;
  item.dataDirectory = "";
  item.dataDirectory_extra = "";
  item.dataDirectory_ctrlType = 0;
  item.itemType = "";
  item.itemType_extra = "";
  item.itemType_ctrlType = 0;
  item.compType = "";
}

function initItemForThorTopology(item) {
  item.depth = 0;
  item.name = "Topology";
  item.name_extra = "";
  item.name_ctrlType = 0;
  item.process = "";
  item.process_extra = "";
  item.process_ctrlType = 0;
  item.parent = -1;
  item.netAddress = "";
  item.netAddress_extra = "";
  item.netAddress_ctrlType = 0;
  item.compType = "ThorCluster";
  item._processId = "";
}

function initItemForRoxieServers(item) {
  item.depth = 0;
  item.name = "Roxie Cluster";
  item.name_extra = "";
  item.name_ctrlType = 0;
  item.parent = -1;
  item.netAddress = "";
  item.netAddress_extra = "";
  item.netAddress_ctrlType = 0;
  item.port = "";
  item.port_extra = "";
  item.port_ctrlType = 0;
  item.dataDirectory = "";
  item.dataDirectory_extra = "";
  item.dataDirectory_ctrlType = 0;
  item.listenQueue = "";
  item.listenQueue_extra = "";
  item.listenQueue_ctrlType = 0;
  item.numThreads = "";
  item.numThreads_extra = "";
  item.numThreads_ctrlType = 0;
  item.requestArrayThreads = "";
  item.requestArrayThreads_extra = "";
  item.requestArrayThreads_ctrlType = 0;
  item.aclName = "";
  item.aclName_extra = "";
  item.aclName_ctrlType = 0;
  item.process = "";
  item.process_extra = "";
  item.process_ctrlType = 0;
  item.compType = "";
  item.params = "";
}

function handleConfigCellClickEvent(oArgs, caller, isComplex) {
  caller.clearTextSelection();

  var target = oArgs.target;
  var record = caller.getRecord(target);
  var selRows = caller.getSelectedRows();
  var alreadySel = false;
  var column = caller.getColumn(target);

  if (typeof (caller.editors) !== 'undefined') {
    for (i = 1; i < 7; i++)
    //caller.editors[i].cancel();
      if (/*caller.editors[i] != column.editor && */caller.editors[i].isActive) {
      //caller.editors[i].save();//on blur already saves the changes
      return;
    }
  }

  var rec;
  for (i = 0; i < selRows.length; i++) {
    rec = caller.getRecord(selRows[i]);
    if (YAHOO.util.Dom.hasClass(caller.getTrEl(rec), 'outoffocus'))
      YAHOO.util.Dom.removeClass(caller.getTrEl(rec), 'outoffocus');
    if (rec === record) {
      alreadySel = true;
    }
  }

  if (!alreadySel) {
    caller.unselectAllRows();
    //row select is called after cellselect and it handles updating subtables, if any
    return;
  }

  if (oArgs.event.ctrlKey || top.document.forms['treeForm'].isLocked.value === 'false')
    return;

  var column = caller.getColumn(target);
  var type = record.getData(column.key + '_ctrlType');
  var meta = record.getData(column.key + '_extra');
  var asyncSubmitFn = function(callback, newValue) {
    var record = this.getRecord(),
        column = this.getColumn(),
        oldValue = this.value,
        datatable = this.getDataTable();

    if (record.getData(column.key + '_required') === 1 && oldValue.length > 0 && newValue.length === 0) {
      alert("This is a required field and cannot be empty. Please enter a value");
      column.editor.cancel();
      return;
    }

    if (record.getData(column.key + '_onChange') === 1 && record.getData(column.key + '_onChangeMsg').length > 0) {
      if (!confirm(record.getData(column.key + '_onChangeMsg'))) {
        column.editor.cancel();
        return;
      }
    }

    if (oldValue === newValue) {
      if (column.editor)
        column.editor.cancel();

      if (datatable.gotonexttab === true)
        showNextCellEditor(this);
      else if (datatable.gotoprevtab === true)
        showPrevCellEditor(this);

      return;
    }

    if (record._oData.name === "name")
    {
      top.document.ResetFocus = true;
      top.document.ResetFocusValueName = newValue;
      if (typeof(record._oData.compType) !== "undefined")
       top.document.ResetFocusCompType = record._oData.compType;
    }
    var attrName = getAttrName(datatable, column, record, isComplex); // = record.getData('name');
    var recordIndex = datatable.getRecordIndex(record);


    var compName = top.document.navDT.getRecord(top.document.navDT.getSelectedRows()[0]).getData('Name');
    var bldSet = top.document.navDT.getRecord(top.document.navDT.getSelectedRows()[0]).getData('BuildSet');

    if (compName === "Hardware" && attrName === "netAddress") {
      var errMsg = "";
      errMsg = isValidIPAddress(newValue, "netAddress", true, false);

      if (errMsg.length > 0) {
        alert(errMsg);
        column.editor.cancel();
        return false;
      }
    }

    if (bldSet === "roxie" && attrName === "dataDirectory") {
      if (newValue === '') {
        alert('Roxie data directory cannot be empty');
        return;
      }

      if (!confirm("The primary data directory for other farms and agents will be changed to the same value. Do you want to continue?")) {
        for (i = 1; i < 7; i++)
          if (caller.editors[i].isActive)
          caller.editors[i].cancel();
        return false;
      }
    }
    else if (compName === 'Directories' && attrName === 'dir' && caller.subTables) {
      if (newValue.indexOf('[NAME]') === -1 || (newValue.indexOf('[INST]') === -1 && newValue.indexOf('[COMPONENT]') === -1)) {
        alert('Common directory entries must have "[NAME]" and either "[INST]" or "[COMPONENT]"');
        return;
      }
    }
    var category = "Software";
    var params = record.getData('params');
    if (compName === "Hardware")
      category = "Hardware";
    else if (compName === "Builds")
      category = "Programs";
    else if (compName === "Environment")
      category = "Environment";
    else if (compName === "topology") {
      category = 'Topology';
      params = '';
      var parRec = record;
      var i = 0;
      while (true) {
        if (parRec.getData('parent') != -1) {
          params += 'inner' + i + '_name=' + parRec.getData('name') + '::' + 'inner' + i++ + '_value=' + parRec.getData('value') + "::";
          parRec = getRecord(datatable, parRec.getData('parent'));
        }
        else
          break;
      }
    }

    if (record.getData(column.key + '_ctrlType') === 5) {
      caller.editors[5].cancel();
      top.document.navDT.promptVerifyPwd(category, params, attrName, oldValue, newValue, recordIndex + 1, callback);
      return false;
    }

    var parRec;
    if (caller.parentDT) {
      for (var idx = 0; idx < caller.parentDT.subTables.length; idx++) {
        if (caller === caller.parentDT.subTables[idx].oDT) {
          parRec = caller.parentDT.getRecord(caller.parentDT.getSelectedRows()[0]).getData(caller.parentDT.subTables[idx].oName);
          var paramstr = caller.parentDT.getRecord(caller.parentDT.getSelectedRows()[0]).getData('params');
          var index = paramstr.indexOf("::Update");
          if (index !== -1)
            params += paramstr.substr(index);

          break;
        }
      }
    }

    var form = top.window.document.forms['treeForm'];
    top.document.startWait(document);

    var refreshConfirm = true;

    if ((record.getData('compType') == 'EspProcess' || record.getData('compType') == "DaliServerProcess") && (record.getData('params').indexOf('subType=EspBinding') != -1 || record.getData('_key') == "ldapServer") && (typeof(column.field) !== 'undefined' && (column.field == 'service' || column.field == 'value')))
    {
      bUpdateFilesBasedn = confirm("If available, proceed with update of filesBasedn value?\n\n(If you are unsure select 'Ok')");
      if (column.field == 'service')
        refreshConfirm = false;
    }
    else if (record.getData('compType') == 'LDAPServerProcess' && record.getData('name') == 'filesBasedn')
      bUpdateFilesBasedn = confirm("If available, proceed with update of filesBasedn value in dependent components?\n\n(If you are unsure select 'Ok')");
    else
      bUpdateFilesBasedn = false;

    var xmlArgs = argsToXml(category, params, attrName, oldValue, newValue, recordIndex + 1, record.getData(column.key + '_onChange'));
    YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/SaveSetting', {
      success: function(o) {
        if (o.status === 200) {
          if (o.responseText.indexOf("<?xml") === 0) {
            form.isChanged.value = "true";
            top.document.needsRefresh = true;
            var updateAttr = o.responseText.split(/<UpdateAttr>/g);
            var prevVal, prevVal1, updateAttr1, updateVal, updateVal1;

            if (updateAttr[1].indexOf("</UpdateAttr>") !== 0) {
              updateAttr1 = updateAttr[1].split(/<\/UpdateAttr>/g);
              updateVal = o.responseText.split(/<UpdateValue>/g);
              updateVal1 = updateVal[1].split(/<\/UpdateValue>/g);
              if (updateVal1[0].indexOf("<") === 0)
                caller.updateCell(record, updateAttr1[0], "");
              else
                caller.updateCell(record, updateAttr1[0], updateVal1[0]);

              prevVal = o.responseText.split(/<PrevValue>/g);
              prevVal1 = prevVal[1].split(/<\/PrevValue>/g);
            }

            var newParams = updateParams(record, attrName, oldValue, newValue, updateAttr1, prevVal1, updateVal1);
            var newkey = '::Update';
            var idx = 1;
            if (typeof (newParams) !== 'undefined') {
              while (true) {
                var tmpArr = newParams.split(newkey + idx);

                if (tmpArr.length === 1)
                  break;

                idx++;
              }

              if (newParams.length > 0)
                record.setData('params', newParams);

              if (caller.subTables) {
                for (var tabidx = 0; tabidx < caller.subTables.length; tabidx++) {
                  var subtablerecset = caller.subTables[tabidx].oDT.getRecordSet();
                  var subrecsetlen = subtablerecset.getLength();
                  for (var recidx = 0; recidx < subrecsetlen; recidx++) {
                    var subrec = subtablerecset.getRecord(recidx);
                    newParams = updateParams(subrec, attrName, oldValue, newValue, updateAttr1, prevVal1, updateVal1);
                    subrec.setData('params', newParams);
                  }
                }
              }
              else if (attrName === 'name' && record.getData('compType') === 'Environment') {
                var recSet = datatable.getRecordSet();
                var recSetLen = recSet.getLength();
                var parent = record.getData('parent');
                for (var i = 0; i < recSetLen; i++) {
                  var r = recSet.getRecord(i);
                  if (r != record && r.getData('parent') === parent)
                    r.setData('params', newParams);
                }
              }
            }

            callback(true, newValue);

            if (record.getData("_not_in_env") === 1)
              removeExtraClassFromCell(caller, record, column, "not_in_env");

            if (compName === "topology") {
              //refresh for topology to refresh the left hand side values
              top.document.navDT.clickCurrentSelOrName(top.document.navDT);
            }
            if (caller.parentDT) {
              parRec[recordIndex] = record.getData();
            }
            if (datatable.gotonexttab === true)
              showNextCellEditor(this);
            else if (datatable.gotoprevtab === true)
              showPrevCellEditor(this);

            var refreshPage = o.responseText.split(/<Refresh>/g);

            if (refreshPage[1].indexOf("</Refresh>") !== 0) {
              var refreshPage1 = refreshPage[1].split(/<\/Refresh>/g);
              if (refreshPage1[0] === "true" && refreshConfirm == true)
                doPageRefresh();
            }
          }
          else if (o.responseText.indexOf("<html") === 0) {
            var temp = o.responseText.split(/td align=\"left\">/g);
            var temp1 = temp[1].split(/<\/td>/g);
            alert(temp1[0]);
            column.editor.cancel();
            top.document.stopWait(document);
            callback();

            if (temp1[0].indexOf("Cannot modify") === 0) {
              form.isLocked.value = "false";
              doPageRefresh();
            }
          }
          top.document.navDT.clickCurrentSelOrName(top.document.navDT); // refresh
        } else {
          alert(r.replyText);
          callback();
        }

        top.document.stopWait(document);
      },
      failure: function(o) {
        alert(o.statusText);
        callback();
        top.document.stopWait(document);
      },
      scope: this
    },
    top.document.navDT.getFileName(true) + 'XmlArgs=' + xmlArgs + '&bUpdateFilesBasedn='  + bUpdateFilesBasedn);
  };

  if (typeof (caller.editors) === 'undefined') {
    caller.editors = {
      1: new YAHOO.widget.TextboxCellEditor({ disableBtns: true, asyncSubmitter: asyncSubmitFn }),
      2: new YAHOO.widget.TextboxCellEditor({ disableBtns: true, asyncSubmitter: asyncSubmitFn, validator: function(val) {
        val = parseFloat(val);
        if (YAHOO.lang.isNumber(val)) { return val; }
      } 
      }),
      4: new YAHOO.widget.DropdownCellEditor({ dropdownOptions: meta, disableBtns: true, asyncSubmitter: asyncSubmitFn }),
      3: new YAHOO.widget.RadioCellEditor({ radioOptions: ["true", "false"], disableBtns: true, asyncSubmitter: asyncSubmitFn }),
      5: new YAHOO.widget.PasswordCellEditor({ disableBtns: true, asyncSubmitter: asyncSubmitFn }),
      6: new YAHOO.widget.TextareaCellEditor({ disableBtns: true, asyncSubmitter: asyncSubmitFn })
    };
  }

  if (typeof (type) === 'undefined')
    type = 1;
    
  if (type === 0)
    return;
  else {
    if (type === 4) {
      caller.editors[type].destroy();

      if (caller.parentDT && caller.parentDT.subTables[0].oName === '_Override' && column.key == 'instance') {
        var parRec = caller.parentDT.getRecord(caller.parentDT.getSelectedRows()[0]);
        var params = "queryType=multiple::category=Software::buildSet=" + record.getData('component') + "::attrName=name";

        if (record.getData('instance') === '')
          params += "::excludePath=Directories/Category[@name='" + parRec.getData('name') + "']/Override[@component='" + record.getData('component') + "']::excludeAttr=@instance";

        YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/GetValue', {
          success: function(o) {
            if (o.responseText.indexOf("<?xml") === 0) {
              var tmp = o.responseText.split(/<ReqValue>/g);
              var tmp1;
              if (tmp.length > 1) {
                tmp1 = tmp[1].split(/<\/ReqValue>/g);
                if (tmp1.length > 1)
                  instances = tmp1[0];
                else
                  instances = '';

                meta = instances.split(/,/g);
                if (typeof (meta[0]) !== 'undefined' && meta[0] != "")
                  meta.unshift("");
                caller.editors[type] = new YAHOO.widget.DropdownCellEditor({ dropdownOptions: meta, disableBtns: true, asyncSubmitter: asyncSubmitFn });
                column.editor = caller.editors[type];
                caller.showCellEditor(target);
              }
            }
          },
          failure: function(o) {
          },
          scope: this
        },
      top.document.navDT.getFileName(true) + 'Params=' + params);
        return;
      }
      else if (YAHOO.lang.isString(meta) && meta.indexOf('#') === 0) {
        var extrainfo = meta.split(/#/g);
        if (extrainfo.length >= 1) {
          var attribName = getAttrName(caller, column, record, isComplex);
          var extra = extrainfo[0];
          if (extra.length === 0)
            extra = extrainfo[1];
          var params = "queryType=xpathType::" + record.getData('params') + "::attrName=" + attribName + "::xpath=" + extra;

          YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/GetValue', {
            success: function(o) {
              if (o.responseText.indexOf("<?xml") === 0) {
                var tmp = o.responseText.split(/<ReqValue>/g);
                var tmp1, values;
                
                if (tmp.length > 1) {
                  tmp1 = tmp[1].split(/<\/ReqValue>/g);
                  if (tmp1.length > 1)
                    values = tmp1[0];
                  else
                    values = '';

                  meta = values.split(/,/g);
                  if (typeof (meta[0]) !== 'undefined' && meta[0] != "")
                    meta.unshift("");
                  caller.editors[type] = new YAHOO.widget.DropdownCellEditor({ dropdownOptions: meta, disableBtns: true, asyncSubmitter: asyncSubmitFn });
                  column.editor = caller.editors[type];
                  caller.showCellEditor(target);
                }
              }
            },
            failure: function(o) {
            },
            scope: this
          },
          top.document.navDT.getFileName(true) + 'Params=' + params);
          return;
        }
      }
      else if (record.getData(column.key) === '') {
        if (typeof (meta[0]) !== 'undefined' && meta[0] != "")
          meta.unshift("");
      }
      caller.editors[type] = new YAHOO.widget.DropdownCellEditor({ dropdownOptions: meta, disableBtns: true, asyncSubmitter: asyncSubmitFn });
    }
    else if (type === 6) {
      if (record.getData(column.key).indexOf('&#13;&#10;') != -1) {
        var str = record.getData(column.key);
        //str.replace(/\&##13;\&##10;/g, '\r\n');
        var exp = '&#13;&#10;';
        var re = new RegExp(exp);
        var s = re.exec(str);
        var x = str.substr(0, s.index);
        x += '\n';
        x += str.substr(s.lastIndex);

        //not working.. need to replace above workaround
        //str.replace(re, '\r\n');
        record.setData(column.key, x);
      }
    }

    column.editor = caller.editors[type];
    caller.editors[type].subscribe("cancelEvent", function(oArgs) {
      this.unsubscribe("cancelEvent");

      if (caller.parentDT) {
        for (var idx = 0; idx < caller.parentDT.subTables.length; idx++) {
          var tmpdt = caller.parentDT.subTables[idx].oDT;
          if (typeof (tmpdt.editors) !== 'undefined') {
            for (i = 1; i < 7; i++)
              if (tmpdt.editors[i].isActive)
              return;
          }
        }
      }

      caller.focus();
    });

    caller.editors[type].subscribe("saveEvent", function(oArgs) {
      this.unsubscribe("saveEvent");

      if (caller.parentDT) {
        for (var idx = 0; idx < caller.parentDT.subTables.length; idx++) {
          var tmpdt = caller.parentDT.subTables[idx].oDT;
          if (typeof (tmpdt.editors) !== 'undefined') {
            for (i = 1; i < 7; i++)
              if (tmpdt.editors[i].isActive)
              return;
          }
        }
      }
      
      caller.focus();
    });
    caller.editors[type].subscribe("blurEvent", function(oArgs) {
      this.unsubscribe("blurEvent");    
      this.save();
    });
    caller.editors[type].subscribe("keydownEvent", function(oArgs) {
      if (oArgs.event.keyCode === 9) {
        this.unsubscribe("keydownEvent");
        if (oArgs.event.shiftKey)
          caller.gotoprevtab = true;
        else
          caller.gotonexttab = true;
        this.fireEvent("blurEvent", { editor: this });
        YAHOO.util.Event.stopEvent(oArgs.event);
      }
    });
  }

  caller.showCellEditor(target);
}

function createMultiColTreeCtrlForComp(rows, compName, subRecordIndex) {
  var createFn = function() {
    YAHOO.example.Basic = function() {
      var Dom = YAHOO.util.Dom, Lang = YAHOO.lang;
      var hasChildren = {};
      var myDTDropTgts = new Array();
      var myColumnDefs = new Array();
      var myDataSource = new YAHOO.util.DataSource(rows);
      myDataSource.responseType = YAHOO.util.DataSource.TYPE_JSARRAY;
      var colIndex1 = 0;
      var colIndex2 = 0;
      var columnFields = new Array();
      for (var i in rows[subRecordIndex]) {
        if ((i.indexOf('_extra') === -1) &&
           (i.indexOf('_ctrlType') === -1) &&
           (i.indexOf('_required') === -1) &&
           (i.indexOf('_onChange') === -1) &&
           (i.indexOf('params') === -1) &&
           (i.indexOf('depth') === -1) &&
           (i.indexOf('parent') === -1) &&
           (i.indexOf('compType') === -1) &&
           (i.indexOf('id') === -1)) {
          if (i === 'icon')
            myColumnDefs[0] = { key: "icon", label: "" };
          else if (i === 'name')
            myColumnDefs[colIndex[i + compName]] = { key: "name", resizeable: true, width: 250, maxAutoWidth: 250, formatter: function(el, oRecord, oColumn, oData) {
              el.innerHTML = "<div id='depth" + oRecord.getData('depth') + "'>" + oData + "</div>";
              Dom.addClass(el, 'yui-dt-liner depth' + oRecord.getData('depth'));
            }, scrollable: true
            };
          else
            myColumnDefs[colIndex[i + compName]] = { key: i, resizeable: true, formatter: formatterDispatcher, editor: new YAHOO.widget.BaseCellEditor() };
        }
        columnFields[colIndex2++] = i;
      }

      myDataSource.responseSchema = { fields: columnFields };
      tabNameTemp = compName + 'Tab';
      var dt = new YAHOO.widget.DataTable(tabNameTemp, myColumnDefs, myDataSource, {
        formatRow: function(row, record) {
          var prnt = record.getData('parent');
          if (prnt !== -1 && compName !== 'Deploy') {
            Dom.addClass(row, 'hidden');
          }
          else //reset if we are starting or refreshing just the table
            hasChildren = {};
          hasChildren[prnt] = true;
          return true;
        }, width: "100%"/*, initialLoad:false*/
      });

      tab.dt = dt;
      tab.dataSource = myDataSource;
      dt.myDTDropTgts = myDTDropTgts;
      //dt.subscribe("rowMouseoverEvent", dt.onEventHighlightRow); 
      dt.subscribe("rowMouseoutEvent", dt.onEventUnhighlightRow);
      dt.subscribe("cellMousedownEvent", function(oArgs) {
        //on left click, the cellClickEvent is fired, taking care of the behaviour
        //on right click, we use cellMousedownEvent to select the row, as well as cause the page to display on the right
        if (oArgs.event.button === 1 || oArgs.event.button === 0)
          return;

        var selRows = this.getSelectedRows();
        if (selRows && selRows.length > 0) {
          if (this.isSelected(this.getRecord(oArgs.target)))
            return;
        }

        this.onEventSelectRow(oArgs)
      });

      dt.subscribe("rowClickEvent", dt.onEventSelectRow);

      dt.selectionToXML = function(targetRec) {
        var xmlStr = "<XmlArgs>";
        var parentName;

        if (typeof (targetRec) !== 'undefined') {
          var parentId = targetRec.getData('parent');
          var ds = dt.getDataSource();
          //var parentRec = ds.liveData[parentId];                      
          var parentRec = getRecord(ds, parentId);
          xmlStr += "<BuildSet name=\"" + targetRec.getData('name');
          xmlStr += "\" path=\"" + targetRec.getData('path');
          xmlStr += "\" build=\"" + parentRec.name;
          xmlStr += "\" buildpath=\"" + parentRec.path;
          xmlStr += "\"/>";
        }

        xmlStr += "</XmlArgs>";

        return xmlStr;
      }

      dt.subscribe('renderEvent', function() {
        var recSet = this.getRecordSet();
        var recSetLen = recSet.getLength();
        var expand = true;
        var ddindex = 0;
        if (compName !== 'Deploy') {
          for (var i = 0; i < recSetLen; i++) {
            var r = recSet.getRecord(i);
            if (hasChildren[r.getData('id')]) {
              var tdEl = this.getFirstTdEl(r);
              var divEl = Dom.getChildren(tdEl);
              var children = Dom.getChildren(divEl[0]);
              children[0].id = "depth" + (r.getData('depth') - 1);
              var inner = children[0].innerHTML;
              var temp = inner.split(" ");
              if (temp[0].toLowerCase() !== "<button") {
                children[0].innerHTML = "<button type='button' class='yui-button buttoncollapsed' id='pushbutton' name='button'></button>" + inner;
                Dom.addClass(this.getTrEl(r), 'collapsed');
              }
              else
                expand = false;
            }
            else {
              if (compName === "Servers") {
                myDTDropTgts[ddindex] = top.document.navDT.createDDRows(this.getTrEl(r).id, "default", { isTarget: true }, null, Dom);
                var nav = top.document.getElementById("center1");
                var xy = Dom.getXY(nav);
                myDTDropTgts[ddindex].setPadding(-xy[1], xy[0], xy[1], -xy[0]);
                ddindex++;
              }
            }
          }
        }
        else
          for (var i = 0; i < recSetLen; i++) {
          var r = recSet.getRecord(i);
          var tdEl = this.getFirstTdEl(r);
          var divEl = Dom.getChildren(tdEl);
          var children = Dom.getChildren(divEl[0]);
          var inner = children[0].innerHTML;
          if (hasChildren[r.getData('id')]) {
            children[0].id = "depth" + (r.getData('depth') - 1);
            children[0].innerHTML = "<button type=\"button\" class=\"yui-button buttoncollapsed\" id=\"pushbutton\" name=\"button\"></button><input type=\"checkbox\" class=\"yui-checkbox\" id=\"deployCheck" + r.getData('id') + "\" name=\"button\"></input>" + inner;
            Dom.addClass(this.getTrEl(r), 'collapsed');
          }
          else
            children[0].innerHTML = "<input type=\"checkbox\" class=\"yui-checkbox\" id=\"deployCheck" + r.getData('id') + "\" name=\"button\"></input>" + inner;
        }

        if (top.document.navDT.lastSelIndex > 0)
          dt.expandRecord(top.document.navDT.lastSelIndex);
        else if (expand)
          dt.expandRecord();
      });

      dt.sendCommand = function(record) {
        if (record.getData('parent') === -1) {
          Dom.addClass(Dom.get('tabviewcontainer1'), 'hidden');
          Dom.setStyle(Dom.get('tabviewcontainer'), 'height', '100%');
          Dom.setStyle(Dom.get('tabviewcontainer'), 'border-bottom-style', 'none');
        }
        else {
          Dom.removeClass(Dom.get('tabviewcontainer1'), 'hidden');
          Dom.setStyle(Dom.get('tabviewcontainer'), 'height', '50%');
          Dom.setStyle(Dom.get('tabviewcontainer'), 'overflow', 'auto');
          Dom.setStyle(Dom.get('tabviewcontainer'), 'border-bottom-style', 'double');
          var xmlArgs = dt.selectionToXML(record);
          YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/GetBuildSetInfo', {
            success: function(o) {
              if (o.status === 200) {
                if (o.responseText.indexOf("<?xml") === 0) {
                  var temp = o.responseText.split(/<XmlArgs>/g);
                  var temp1 = temp[1].split(/<\/XmlArgs>/g);
                  dt.buildSetDataSource.handleResponse("", o, { success: dt.buildSetTable.onDataReturnInitializeTable,
                    scope: dt.buildSetTable
                  }, this, 999);
                }
                else if (o.responseText.indexOf("<html") === 0) {
                  var temp = o.responseText.split(/body class=\" yui-skin-sam\">/g);
                  var temp1 = temp[1].split(/<\/body>/g);
                  //dt.buildSetTable.deleteRows(0);
                  dt.buildSetDataSource.push(temp1[0]);
                  dt.buildSetDataSource.sendRequest("moredata", { success: dt.buildSetTable.onDataReturnInsertRows,
                    scope: dt.buildSetTable
                  });
                }
              } else {
                alert(r.replyText);
              }
            },
            failure: function(o) {
              alert(o.statusText);
            },
            scope: this
          },
            top.document.navDT.getFileName(true) + 'cmd=SaveSetting&XmlArgs=' + xmlArgs);
        }
      }

      dt.subscribe("tableKeyEvent", function(oArgs) {
        if (oArgs.event.keyCode === 192 && oArgs.event.ctrlKey === true)
          setFocusToNavTable();
        else if (oArgs.event.keyCode === 39 || oArgs.event.keyCode === 37) {
          var rec = dt.getRecord(dt.getSelectedRows()[0]);
          if (rec)
            dt.expColRecord(rec.getData('id'));
        }
        else
          handleComplexTableKeyDown(oArgs, dt);
      });

      dt.subscribe("cellClickEvent", function(oArgs) {
        dt.clearTextSelection();

        var target = oArgs.target;
        var record = this.getRecord(target);
        var selectedRows;
        var eventTarget = YAHOO.util.Event.getTarget(oArgs.event);
        if (eventTarget.id.indexOf("deployCheck") !== -1) {
          var recSet = this.getRecordSet();
          var recSetLen = recSet.getLength();
          var self = this;
          var id = record.getData('id');
          var checkDependents = function(id, checked) {
            for (var i = 0; i < recSetLen; i++) {
              var r = recSet.getRecord(i);
              if (r.getData('parent') === id) {
                var elem = document.getElementById("deployCheck" + r.getData('id'));
                elem.checked = checked;
                checkDependents(r.getData('id'), checked);
              }
            }
          };

          var checkParents = function(id, parentId, checked) {
            if (!checked) {
              for (var i = 0; i < recSetLen; i++) {
                var r = recSet.getRecord(i);
                if (r.getData('id') === parentId) {
                  var elem = document.getElementById("deployCheck" + r.getData('id'));
                  elem.checked = checked;
                  checkParents(0, r.getData('parent'), checked);
                }
              }
            }
            else {
              if (parentId === -1)
                return;

              var allChecks = true;
              var newParentId = 0;
              for (var i = 0; i < recSetLen; i++) {
                var r = recSet.getRecord(i);
                if (r.getData('parent') === parentId) {
                  allChecks &= document.getElementById("deployCheck" + r.getData('id')).checked;

                  if (!allChecks)
                    break;
                }
              }

              for (var i = 0; i < recSetLen; i++) {
                var r = recSet.getRecord(i);
                if (r.getData('id') === parentId) {
                  document.getElementById("deployCheck" + parentId).checked = allChecks;
                  newParentId = r.getData('parent');
                  break;
                }
              }

              checkParents(parentId, newParentId, checked);
            }
          };

          var saveSelectedComps = function(parentId) {
            var xmlStr = '';
            var flag = false;
            for (var i = 1; i < recSetLen; i++) {
              var r = recSet.getRecord(i);
              if (r.getData('parent') === parentId) {
                if (document.getElementById("deployCheck" + r.getData('id')).checked) {
                  if (r.getData('buildSet') === "Instance") {
                    xmlStr += "<Instance ";
                    xmlStr += " name=\"" + r.getData('instanceName');
                    xmlStr += "\" computer=\"" + r.getData('name');
                    xmlStr += "\" />";
                  }
                  else {
                    xmlStr += "<" + r.getData('buildSet');
                    xmlStr += " name=\"" + r.getData('name');
                    xmlStr += "\" >";
                    xmlStr += saveSelectedComps(r.getData('id'));
                    xmlStr += "</" + r.getData('buildSet') + ">";
                  }
                }
              }
            }

            return xmlStr;
          }

          checkDependents(id, eventTarget.checked);
          checkParents(id, record.getData('parent'), eventTarget.checked);
          var xmlStr = "<Deploy><SelectedComponents>";
          xmlStr += saveSelectedComps(0);
          xmlStr += "</SelectedComponents></Deploy>";
          top.document.forms['treeForm'].compsToBeDeployed.value = xmlStr;

          return;
        }


        var eventTarget = YAHOO.util.Event.getTarget(oArgs.event);
        if (eventTarget.id.indexOf("pushbutton") === -1) {
          if (compName === 'Programs') {
            if (record.getData('parent') === -1) {
              Dom.addClass(Dom.get('tabviewcontainer1'), 'hidden');
              Dom.setStyle(Dom.get('tabviewcontainer'), 'height', '100%');
              Dom.setStyle(Dom.get('tabviewcontainer'), 'border-bottom-style', 'none');
            }
            else
              dt.sendCommand(record);
            return;

          }
          else
            handleConfigCellClickEvent(oArgs, this, true);

          return true;
        }

        var parentId = record.getData('id');
        var recSet = dt.getRecordSet(); //this.getRecordSet();
        var recSetLen = recSet.getLength();
        var self = dt; //this;
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
          var tdEl = this.getFirstTdEl(record);
          var divEl = Dom.getChildren(tdEl);
          var children = Dom.getChildren(divEl[0]);
          var innerChild = children[0].children[0];
          if (record.getData('expanded')) {
            Dom.addClass(this.getTrEl(record), 'expanded');
            Dom.removeClass(this.getTrEl(record), 'collapsed');
            Dom.addClass(innerChild, 'buttonexpanded');
            Dom.removeClass(innerChild, 'buttoncollapsed');
          } else {
            Dom.addClass(this.getTrEl(record), 'collapsed');
            Dom.removeClass(this.getTrEl(record), 'expanded');
            Dom.addClass(innerChild, 'buttoncollapsed');
            Dom.removeClass(innerChild, 'buttonexpanded');
          }
        }
      });

      if (compName === 'Programs') {
        //also create the hidden table to display the buildset information
        var myBSColumnDefs = [{ key: "name", label: "File Name", resizeable: true },
                                                                { key: "method", label: "Method", resizeable: true },
                                                                { key: "srcPath", label: "Source Path", resizeable: true },
                                                                { key: "destPath", label: "Destination Path", resizeable: true },
                                                                { key: "destName", label: "Destination Name", resizeable: true}];
        var xmlStr = '<?xml version="1.0" encoding="UTF-8"?><File name="" method="" srcPath="" destPath="" destName=""></File>';

        var myBSDataSource = new YAHOO.util.DataSource(xmlStr);
        myBSDataSource.responseType = YAHOO.util.DataSource.TYPE_XML;
        myBSDataSource.responseSchema = {
          resultNode: "File",
          fields: ["name", "method", "srcPath", "destPath", "destName"]
        };
        var myBSDataTable = new YAHOO.widget.DataTable("tabviewcontainercontent1", myBSColumnDefs,
              myBSDataSource, { width: "100%"/*, initialLoad:false*/ });

        dt.buildSetTable = myBSDataTable;
        dt.buildSetDataSource = myBSDataSource;
      }

      if (typeof (tabView) !== 'undefined' && tabView !== null)
        tabView.addListener("activeIndexChange", function(oArgs) {
          top.document.activeTab = tabView.getTab(oArgs.newValue).get("label");
          setFocusToTable(dt);
          if (top.document.needsRefresh === true) {
            top.document.navDT.clickCurrentSelOrName(top.document.navDT);
          }
        });


      dt.subscribe("rowSelectEvent", function(oArgs) {
        top.document.navDT.lastSelIndex = this.getRecordIndex(oArgs.record);
      });


      var oContextMenu = new YAHOO.widget.ContextMenu(compName + 'Menu', {
        trigger: tabNameTemp,
        lazyload: true
      });

      top.document.ContextMenuCenter = oContextMenu;

      oContextMenu.dt = dt;
      oContextMenu.subscribe("beforeShow", onContextMenuBeforeShow);
      dt.expandRecord = function(id) {
        var recSet = dt.getRecordSet();

        if (typeof (id) === 'undefined') {
          var tdEl = dt.getFirstTdEl(recSet.getRecord(0));
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
            if (r.getData('parent') != -1)
              dt.expandRecord(r.getData('parent'));
            var tdEl = dt.getFirstTdEl(r);
            var children = Dom.getChildren(tdEl);
            if (Dom.hasClass(children[0].children[0].children[0], 'yui-button') &&
                  Dom.hasClass(children[0].children[0].children[0], 'buttoncollapsed')) {
              children[0].children[0].children[0].click();
              break;
            }
            else {
              dt.unselectAllRows();
              dt.selectRow(r);
              break;
            }
          }
        }
      }

      dt.expColRecord = function(id) {
        if (typeof (id) === 'undefined')
          return;

        var recSet = dt.getRecordSet();
        var recSetLen = recSet.getLength();
        for (var i = 0; i < recSetLen; i++) {
          var r = recSet.getRecord(i);
          if (r.getData('id') === id) {
            if (r.getData('parent') != -1)
              dt.expandRecord(r.getData('parent'));
            var tdEl = dt.getFirstTdEl(r);
            var children = Dom.getChildren(tdEl);
            if (Dom.hasClass(children[0].children[0].children[0], 'yui-button')) {
              children[0].children[0].children[0].click();
              break;
            }
          }
        }
      }

      dt.cleanup = function() {
        this.unsubscribeAll("cellMouseoverEvent");
        this.unsubscribeAll("rowClickEvent");
        this.unsubscribeAll("tableKeyEvent");
        this.unsubscribeAll("cellMousedownEvent");
        this.unsubscribeAll("tableFocusEvent");
        this.unsubscribeAll("tableBlurEvent");
        this.unsubscribeAll("cellClickEvent");
        this.unsubscribeAll("rowSelectEvent");
        this.unsubscribeAll("rowMouseoutEvent");
        this.unsubscribeAll("renderEvent");

        var i;
        for (i = 0; i < this.myDTDropTgts.length; i++) {
          if (this.myDTDropTgts[i]) {
            this.myDTDropTgts[i].unreg();
            delete this.myDTDropTgts[i];
          }
        }

        oContextMenu.dt = null;
        oContextMenu.unsubscribeAll("beforeShow");
      }

      if (tabView.inInit) {
        top.document.navDT.focus();
        dt.fireEvent("tableBlurEvent");
        tabView.inInit = false;
      }
      else
        setFocusToTable(dt);

      if (listenEvent != "load")
        tab.removeListener(listenEvent);

      return { oDS: myDataSource, oDT: dt/*, oTV: myTabView*/ };
    } ();
  };

  if (compName !== "Deploy") {
    if (!top.document.RightTabView)
      top.document.RightTabView = new YAHOO.widget.TabView('tabviewcontainer');

    var tabView = top.document.RightTabView;

    var tabs = tabView.get("tabs");
    var tab;
    for (i = 0; i < tabs.length; i++) {
      if (tabs[i].get("label") === compName) {
        tab = tabs[i];
        break;
      }
    }
  }

  var listenEvent = "activeChange";
  if (typeof (tab) === 'undefined') {
    tab = window;
    listenEvent = "load";
    YAHOO.util.Event.addListener(tab, listenEvent, createFn);
  }
  else
    tab.addListener(listenEvent, createFn);
}

function createEnvXmlView(allrows, compName, subRecordIndex) {
  var createFn = function() {
    YAHOO.example.Basic = function() {
      var Dom = YAHOO.util.Dom, Lang = YAHOO.lang;
      var hasChildren = {};
      top.document.startWait(document);
      var myDTDropTgts = new Array();
      var myColumnDefs = new Array();
      var rows = new Array();
      rows[0] = allrows[0];
      var expanded = {};
      var myDataSource = new YAHOO.util.DataSource(rows);
      myDataSource.responseType = YAHOO.util.DataSource.TYPE_JSARRAY;
      var colIndex1 = 0;
      var colIndex2 = 0;
      var columnFields = new Array();
      for (var i in rows[subRecordIndex]) {
        if ((i.indexOf('_extra') === -1) &&
           (i.indexOf('_ctrlType') === -1) &&
           (i.indexOf('_required') === -1) &&
           (i.indexOf('_onChange') === -1) &&
           (i.indexOf('params') === -1) &&
           (i.indexOf('depth') === -1) &&
           (i.indexOf('parent') === -1) &&
           (i.indexOf('compType') === -1) &&
           (i.indexOf('id') === -1)) {
          if (i === 'icon')
            myColumnDefs[0] = { key: "icon", label: "" };
          else if (i === 'name')
            myColumnDefs[colIndex[i + compName]] = { key: "name", resizeable: true, width: 250, maxAutoWidth: 250, formatter: function(el, oRecord, oColumn, oData) {
              el.innerHTML = "<div id='depth" + oRecord.getData('depth') + "'>" + oData + "</div>";
              Dom.addClass(el, 'yui-dt-liner depth' + oRecord.getData('depth'));
            }, scrollable: true
            };
          else
            myColumnDefs[colIndex[i + compName]] = { key: i, resizeable: true, formatter: formatterDispatcher, editor: new YAHOO.widget.BaseCellEditor() };
        }
        columnFields[colIndex2++] = i;
      }

      myDataSource.responseSchema = { fields: columnFields };
      tabNameTemp = compName + 'Tab';
      var dt = new YAHOO.widget.DataTable(tabNameTemp, myColumnDefs, myDataSource, {
        formatRow: function(row, record) {
          var prnt = record.getData('parent');
          setChildrenOf(prnt, record);
          if (prnt !== -1 && compName !== 'Deploy') {
            Dom.addClass(row, 'hidden');
          }
          return true;
        },
        width: "100%"
      });

      function onContextMenuBeforeAddAttribute(p_sType, p_aArgs, menuitem)
      {
        var name = prompt("Enter Attribute Name:");

        if (name.length == 0)
        {
          alert("Attribute name can not be empty!");
          return;
        }

        var record = top.document.rightDT.getRecordSet().getRecord(top.document.rightDT.getSelectedRows()[0]);
        var pp = parseParamsForXPath(record.getData('params'), "", "", false, true);

        var xmlStr = "<XmlArgs><Setting operation=\"add\" params= \"" + pp + "\" attrib= \"" + (menuitem.element.outerText.trimRight() == 'Add Tag' ? name : "@" + name) + "\"/></XmlArgs>";

        YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/HandleAttributeAdd', {
          success: function(o) {
            top.document.forms['treeForm'].isChanged.value = "true";
            top.document.choice = new Array();
            top.document.choice[0] = top.document.rightDT.getRecordIndex(top.document.rightDT.getSelectedRows()[0])+1;
            var recDepth =  top.document.rightDT.getRecord(top.document.choice[0])._oData.depth;

            var index = 0;
            for (counter = top.document.choice[0]; counter >= 0; counter--)
            {
              if (top.document.rightDT.getRecord(counter)._oData.depth < recDepth)
              {
                top.document.choice[index] = top.document.rightDT.getRecord(counter).getData('params');
                recDepth = top.document.rightDT.getRecord(counter)._oData.depth;
                index++;
              }
            }

            top.document.doJumpToChoice = true;
            doPageRefresh();

             YAHOO.util.UserAction.click(top.document.rightDT.getFirstTrEl());
           },
          failure: function(o) {
            alert("Failed to add attribute.  (XPath maybe ambiguous. A manual edit of the XML configuration file maybe required to add this attribute.) ");
            },
          scope: this
        },
          top.document.navDT.getFileName(true) + 'XmlArgs=' + xmlStr + '&bLeaf=' +  false );
        }

      function onContextMenuBeforeShowDeleteContextMenu(p_sType, p_aArgs) {
        var record = top.document.rightDT.getRecordSet().getRecord(top.document.rightDT.getSelectedRows()[0]);
        var pp = parseParamsForXPath( record.getData('params'), top.document.rightDT.getRecordSet().getRecord(top.document.rightDT.getSelectedRows()[0]).getData('name'),
                   top.document.rightDT.getRecordSet().getRecord(top.document.rightDT.getSelectedRows()[0]).getData('value'),
                   record.getData('hasChildren') == undefined ? false : record.getData('hasChildren'), false);

        var xmlStr = "<XmlArgs><Setting operation=\"delete\" params= \"" + pp + "\"/></XmlArgs>";

        YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/HandleAttributeDelete', {
          success: function(o) {
            top.document.forms['treeForm'].isChanged.value = "true";
            top.document.choice = new Array();
            top.document.choice[0] = top.document.rightDT.getRecordIndex(top.document.rightDT.getSelectedRows()[0]);
            var recDepth =  top.document.rightDT.getRecord(top.document.choice[0])._oData.depth;

            var index = 0;
            for (counter = top.document.choice[0]; counter >= 0; counter--)
            {
              if (top.document.rightDT.getRecord(counter)._oData.depth < recDepth)
              {
                top.document.choice[index] = top.document.rightDT.getRecord(counter).getData('params');
                recDepth = top.document.rightDT.getRecord(counter)._oData.depth;
                index++;
              }
            }

            top.document.doJumpToChoice = true;
            doPageRefresh();

             YAHOO.util.UserAction.click(top.document.rightDT.getFirstTrEl());
           },
          failure: function(o) {
            alert("Failed to delete attribute.  (XPath maybe ambiguous. A manual edit of the XML configuration file maybe required to delete this attribute.) ");
            },
          scope: this
        },
          top.document.navDT.getFileName(true) + 'XmlArgs=' + xmlStr + '&bLeaf=' + (record.getData('hasChildren') == undefined ? false : !record.getData('hasChildren') ));
        }
      function onContextMenuXBeforeShow(p_sType, p_aArgs)
      {
        if (top.document.getElementById('ReadWrite').checked == true)
          oContextMenuX.cfg.setProperty('disabled',false);
        else
          oContextMenuX.cfg.setProperty('disabled',true);

        top.document.ContextMenuCenter = this;
      }

      function onShowContextMenu(p_oEvent)
      {
        if (top.document.rightDT.getRecord(top.document.rightDT.getSelectedRows()[0])._oData.hasChildren == true)  // only allow attributes to be delete
        {
          top.document.ContextMenuCenter.getRoot().body.lastElementChild.childNodes[0].hidden = true;
          top.document.ContextMenuCenter.getRoot().body.lastElementChild.childNodes[1].hidden = false;
        }
        else
        {
          top.document.ContextMenuCenter.getRoot().body.lastElementChild.childNodes[0].hidden = false;
          top.document.ContextMenuCenter.getRoot().body.lastElementChild.childNodes[1].hidden = true;
        }
      }

     var aMenuItemsX = [ { text: "Delete",        onclick: { fn: onContextMenuBeforeShowDeleteContextMenu} },
                          { text: "Add Attribute", onclick: { fn: onContextMenuBeforeAddAttribute}, }
                        ];
      top.document.rightDT = dt;
      top.document.rightDT.expandRecord = function(id) {
      var recSet = top.document.rightDT.getRecordSet();

      if (typeof (id) === 'undefined') {
        var tdEl = top.document.rightDT.getFirstTdEl(recSet.getRecord(0));
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
          if (r.getData('parent') != -1)
            top.document.rightDT.expandRecord(r.getData('parent'));
          var tdEl = top.document.rightDT.getFirstTdEl(r);
          var children = Dom.getChildren(tdEl);
          if (Dom.hasClass(children[0].children[0].children[0], 'yui-button') &&
                Dom.hasClass(children[0].children[0].children[0], 'buttoncollapsed')) {
            children[0].children[0].children[0].click();
            break;
          }
          else {
            top.document.rightDT.unselectAllRows();
            top.document.rightDT.selectRow(r);
            break;
          }
        }
        }
      }

      var oContextMenuX = new YAHOO.widget.ContextMenu( "EnvironmentTabCM2", { trigger: dt.getTbodyEl(), lazyload: true, itemdata: aMenuItemsX, container: tabNameTemp});
      top.document.ContextMenuCenter = oContextMenuX;

      oContextMenuX.subscribe("show", onShowContextMenu);

      oContextMenuX.dt = dt;
      oContextMenuX.subscribe("beforeShow",onContextMenuXBeforeShow);

      tab.dt = dt;
      tab.dataSource = myDataSource;
      dt.subscribe("rowMouseoutEvent", dt.onEventUnhighlightRow);
      dt.subscribe("cellMousedownEvent", function(oArgs) {
        //on left click, the cellClickEvent is fired, taking care of the behaviour
        //on right click, we use cellMousedownEvent to select the row, as well as cause the page to display on the right
        if (oArgs.event.button === 1 || oArgs.event.button === 0)
          return;

        var selRows = this.getSelectedRows();
        if (selRows && selRows.length > 0) {
          if (this.isSelected(this.getRecord(oArgs.target)))
            return;
        }

        this.onEventSelectRow(oArgs)
      });

      dt.subscribe("rowClickEvent", dt.onEventSelectRow);

      dt.selectionToXML = function(targetRec) {
        var xmlStr = "<XmlArgs>";
        var parentName;

        if (typeof (targetRec) !== 'undefined') {
          var parentId = targetRec.getData('parent');
          var ds = dt.getDataSource();
          var parentRec = getRecord(ds, parentId);
          xmlStr += "<BuildSet name=\"" + targetRec.getData('name');
          xmlStr += "\" path=\"" + targetRec.getData('path');
          xmlStr += "\" build=\"" + parentRec.name;
          xmlStr += "\" buildpath=\"" + parentRec.path;
          xmlStr += "\"/>";
        }

        xmlStr += "</XmlArgs>";

        return xmlStr;
      }

      dt.subscribe('renderEvent', function() {
        var recSet = this.getRecordSet();
        var recSetLen = recSet.getLength();
        var recs = recSet.getRecords();
        var expand = true;
        var fnDomGetCh = Dom.getChildren;
        var fnDomGetFCh = Dom.getFirstChild;
        var fnDomAddCl = Dom.addClass;
        var hC = hasChildren;
        for (var i = 0; i < recSetLen; i++) {
          var r = recs[i];
          var id = r.getData('id');
          if (id === 0 && (hC[id] || allrows.length)) {
            hC[0] = true;
            var children = fnDomGetCh(fnDomGetFCh(this.getFirstTdEl(r)));
            children[0].id = "depth" + (r.getData('depth') - 1);
            var inner = children[0].innerHTML;
            if (inner.indexOf("<BUTTON") !== 0 && inner.indexOf("<button") !== 0) {
              children[0].innerHTML = "<button type='button' class='yui-button buttoncollapsed' id='pushbutton' name='button'></button>" + inner;
              fnDomAddCl(this.getTrEl(r), 'collapsed');
            }
            else
              expand = false;

            break;
          }
        }

        if (expand)
          dt.expandRecord();

        top.document.navDT.getWaitDlg().hide();

        top.document.stopWait(document);

        var lastCounter2 = 0;
        if (top.document.doJumpToChoice == true)
        {
          for (counter = top.document.choice.length-1; counter >= 0; counter--)
          {
            for (counter2 = lastCounter2; true; counter2++)
            {
              if (this.getRecord(counter2).getData('params') == top.document.choice[counter])
              {
                this.expandRecord(counter2);
                lastCounter2 = counter2;
                break;
              }
            }
          }

          Dom.getChildren(this.getFirstTdEl(this.getRecord(this.getRecordIndex(this.getSelectedRows()[0]))))[0].children[0].children[0].focus();
          top.document.doJumpToChoice = false;
        }
      });

      dt.subscribe("tableKeyEvent", function(oArgs) {
        if (oArgs.event.keyCode === 192 && oArgs.event.ctrlKey === true)
          setFocusToNavTable();
        else if (oArgs.event.keyCode === 39 || oArgs.event.keyCode === 37) {
          var rec = dt.getRecord(dt.getSelectedRows()[0]);
          if (rec)
            dt.expColRecord(rec.getData('id'));
        }
        else
          handleComplexTableKeyDown(oArgs, dt);
      });

      dt.subscribe("cellClickEvent", function(oArgs) {
        dt.clearTextSelection();
        var target = oArgs.target;
        var record = this.getRecord(target);
        top.document.startWait(document);

        if (!expanded[record.getData('id')]) {
          var atindex = 0;
          expanded[record.getData('id')] = true;
          var recid = record.getData('id');
          var newrows = new Array();
          for (var allrowcount = 0; allrowcount < allrows.length; allrowcount++) {
            if (allrows[allrowcount].parent === recid) {
              newrows[newrows.length] = allrows[allrowcount];
            }
          }

          if (newrows.length) {
            top.document.navDT.getWaitDlg().show();
            var recst = this.getRecordSet();
            var recstlen = recst.getLength();
            for (var recstcount = 0; recstcount < recstlen; recstcount++) {
              if (record === recst.getRecord(recstcount)) {
                atindex = recstcount;
                break;
              }
            }

            dt.addRows(newrows, atindex + 1);
          }
        }

        var selectedRows;
        var eventTarget = YAHOO.util.Event.getTarget(oArgs.event);
        if (eventTarget.id.indexOf("pushbutton") === -1) {
          top.document.stopWait(document);
          top.document.navDT.getWaitDlg().hide();
          handleConfigCellClickEvent(oArgs, this, true);
          return true;
        }

        var parentId = record.getData('id');
        var recSet = dt.getRecordSet();
        var recSetLen = recSet.getLength();
        var self = dt;
        var fnDomAC = Dom.addClass;
        var fnDomRC = Dom.removeClass;
        var fnDomGetCh = Dom.getChildren;
        var fnDomGetFCh = Dom.getFirstChild;
        var fnDomAddCl = Dom.addClass;
        var fnDomHasCl = Dom.hasClass;
        var hC = hasChildren;
        var visibility = function(parentId, visible) {
          var count = 0;
          var flag = false;
          var childRecSet = childrenOf[parentId];
          if (!childRecSet)
            return count;
          for (var i = 0; i < childRecSet.length; i++) {
            var r = childRecSet[i];
            if (visible) {
              flag = true;
              fnDomRC(self.getTrEl(r), 'hidden');
              if (r.getData('hasChildren')) {
                var children = fnDomGetCh(fnDomGetFCh(self.getFirstTdEl(r)));
                children[0].id = "depth" + (r.getData('depth') - 1);
                var inner = children[0].innerHTML;
                if (inner.indexOf("<BUTTON") !== 0 && inner.indexOf("<button") !== 0) {
                  children[0].innerHTML = "<button type='button' class='yui-button buttoncollapsed' id='pushbutton' name='button'></button>" + inner;
                  fnDomAddCl(self.getTrEl(r), 'collapsed');
                }
              }
            } else {
              if (!fnDomHasCl(self.getTrEl(r), 'hidden')) {
                flag = true;
                fnDomAC(self.getTrEl(r), 'hidden');
              }
            }

            if (flag)
              count += visibility(r.getData('id'), visible && r.getData('expanded'));
            count++;
            flag = false;
          }
          return count;
        };
        record.setData('expanded', !record.getData('expanded'));
        if (visibility(parentId, record.getData('expanded'))) {
          var tdEl = this.getFirstTdEl(record);
          var divEl = Dom.getChildren(tdEl);
          var children = Dom.getChildren(divEl[0]);
          var innerChild = children[0].children[0];
          if (record.getData('expanded')) {
            Dom.addClass(this.getTrEl(record), 'expanded');
            Dom.removeClass(this.getTrEl(record), 'collapsed');
            Dom.addClass(innerChild, 'buttonexpanded');
            Dom.removeClass(innerChild, 'buttoncollapsed');
          } else {
            Dom.addClass(this.getTrEl(record), 'collapsed');
            Dom.removeClass(this.getTrEl(record), 'expanded');
            Dom.addClass(innerChild, 'buttoncollapsed');
            Dom.removeClass(innerChild, 'buttonexpanded');
          }
        }
        top.document.stopWait(document);
        top.document.navDT.getWaitDlg().hide();
      });

      if (typeof (tabView) !== 'undefined' && tabView !== null)
        tabView.addListener("activeIndexChange", function(oArgs) {
          top.document.activeTab = tabView.getTab(oArgs.newValue).get("label");
          setFocusToTable(dt);
          if (top.document.needsRefresh === true) {
            top.document.navDT.clickCurrentSelOrName(top.document.navDT);
          }
        });


      dt.subscribe("rowSelectEvent", function(oArgs) {
        top.document.navDT.lastSelIndex = this.getRecordIndex(oArgs.record);
      });


      var oContextMenu = new YAHOO.widget.ContextMenu(compName + 'Menu', {
        trigger: tabNameTemp,
        lazyload: true
      });

      top.document.ContextMenuCenter = oContextMenu;

      oContextMenu.dt = dt;
      oContextMenu.subscribe("beforeShow", onContextMenuBeforeShow);
      dt.expandRecord = function(id) {
        var recSet = dt.getRecordSet();

        if (typeof (id) === 'undefined') {
          var tdEl = dt.getFirstTdEl(recSet.getRecord(0));
          var children = Dom.getChildren(tdEl);
          if (Dom.hasClass(children[0].children[0].children[0], 'yui-button')) {
            children[0].children[0].children[0].click();
            return;
          }
        }

        var r = dt.getRecord(id);
        if (r) {
          if (r.getData('parent') != -1)
            dt.expandRecord(r.getData('parent'));
          var tdEl = dt.getFirstTdEl(r);
          var children = Dom.getChildren(tdEl);
          if (Dom.hasClass(children[0].children[0].children[0], 'yui-button') &&
                  Dom.hasClass(children[0].children[0].children[0], 'buttoncollapsed')) {
            children[0].children[0].children[0].click();
          }
          else {
            dt.unselectAllRows();
            dt.selectRow(r);
          }
        }
      }

      dt.expColRecord = function(id) {
        if (typeof (id) === 'undefined')
          return;

        var recSet = dt.getRecordSet();
        var recSetLen = recSet.getLength();
        for (var i = 0; i < recSetLen; i++) {
          var r = recSet.getRecord(i);
          if (r.getData('id') === id) {
            if (r.getData('parent') != -1)
              dt.expandRecord(r.getData('parent'));
            var tdEl = dt.getFirstTdEl(r);
            var children = Dom.getChildren(tdEl);
            if (Dom.hasClass(children[0].children[0].children[0], 'yui-button')) {
              children[0].children[0].children[0].click();
              break;
            }
          }
        }
      }

      dt.cleanup = function() {
        this.unsubscribeAll("cellMouseoverEvent");
        this.unsubscribeAll("rowClickEvent");
        this.unsubscribeAll("tableKeyEvent");
        this.unsubscribeAll("cellMousedownEvent");
        this.unsubscribeAll("tableFocusEvent");
        this.unsubscribeAll("tableBlurEvent");
        this.unsubscribeAll("cellClickEvent");
        this.unsubscribeAll("rowSelectEvent");
        this.unsubscribeAll("rowMouseoutEvent");
        this.unsubscribeAll("renderEvent");

        oContextMenu.dt = null;
        oContextMenu.unsubscribeAll("beforeShow");
      }

      if (tabView.inInit) {
        top.document.navDT.focus();
        dt.fireEvent("tableBlurEvent");
        tabView.inInit = false;
      }
      else
        setFocusToTable(dt);

      if (listenEvent != "load")
        tab.removeListener(listenEvent);

      return { oDS: myDataSource, oDT: dt };
    } ();
  };

  if (compName !== "Deploy") {
    if (!top.document.RightTabView)
      top.document.RightTabView = new YAHOO.widget.TabView('tabviewcontainer');

    var tabView = top.document.RightTabView;

    var tabs = tabView.get("tabs");
    var tab;
    for (i = 0; i < tabs.length; i++) {
      if (tabs[i].get("label") === compName) {
        tab = tabs[i];
        break;
      }
    }
  }

  var listenEvent = "activeChange";
  if (typeof (tab) === 'undefined') {
    tab = window;
    listenEvent = "load";
    YAHOO.util.Event.addListener(tab, listenEvent, createFn);
  }
  else
    tab.addListener(listenEvent, createFn);
}

function initRowsForComplexComps(rows, compName) {
  var item = {};
  item.id = 0;
  if (compName === 'RoxieServers')
    initItemForRoxieServers(item);
  else if (compName === 'RoxieSlaves')
    initItemForRoxieSlaves(item);
  else if (compName === 'Topology')
    initItemForThorTopology(item);

  rows[rows.length] = item;
}

function setParentIds(item, rows, parentIds) {
  if (item.depth > rows[parentIds[parentIds.length - 1]].depth)
    parentIds[parentIds.length] = item.id;
  else if (item.depth === rows[parentIds[parentIds.length - 1]].depth) {
    item.parent = parentIds[parentIds.length - 2];
    parentIds[parentIds.length - 1] = item.id;
  }
  else {
    var depth = rows[parentIds[parentIds.length - 1]].depth - item.depth;

    while (depth--)
      parentIds.pop();

    item.parent = parentIds[parentIds.length - 2];
    parentIds[parentIds.length - 1] = item.id;
  }
}

function onMenuItemClickAddFarm(p_sType, p_aArgs, p_oValue) {
  if (top.document.forms['treeForm'].isLocked.value === 'false')
    return;

  top.document.navDT.displayRoxieClusterAddFarm(top.document.navDT, "");
  return;
}

function onMenuItemClickAddServers(p_sType, p_aArgs, p_oValue) {
  if (top.document.forms['treeForm'].isLocked.value === 'false')
    return;

  top.document.navDT.displayRoxieClusterAddFarm(top.document.navDT, this.parent.contextEventTarget.lastChild.nodeValue);
  return;
}

function roxieSelectionToXML(table, type, selectedRows, compName) {
  var xmlStr = "<RoxieData type=\"" + type + "\" roxieName=\"" + compName + "\" >";

  if (typeof (selectedRows) !== 'undefined') {
    for (var i = 0; i < selectedRows.length; i++) {
      var parentRec = getRecord(table, table.getRecord(selectedRows[i]).getData('parent'));

      if (typeof (parentRec) === 'undefined')
        xmlStr += "<RoxieSlave name=\"" + table.getRecord(selectedRows[i]).getData('name') + "\"/>";
      else
        xmlStr += "<" + table.getRecord(selectedRows[i]).getData('compType') + " name=\"" + table.getRecord(selectedRows[i]).getData('name') + "\" parent=\"" + parentRec.getData('name') + "\"/>";
    }
  }

  xmlStr += "</RoxieData>";

  return xmlStr;
}

function onMenuItemClickDelete(p_sType, p_aArgs, p_oValue) {
  var Dom = YAHOO.util.Dom;
  var oTarget = this.parent.contextEventTarget;
  var oSelectedTR = oTarget.nodeName.toUpperCase() === "TR" ?
                                        oTarget : Dom.getAncestorByTagName(oTarget, "TR");

  var recSet = this.parent.dt.getRecordSet();
  var selRows = this.parent.dt.getSelectedRows();
  var msg = "";

  if (this.parent.id === "AgentsMenu") {
    if (this.parent.dt.getRecord(selRows[0]).getData('name') !== 'Roxie Cluster')
      msg = "You cannot selectively delete agents or channels!\n";

    msg += "Are you sure you want to delete the entire agent configuration?";
  }
  else {
    if (selRows.length > 1)
      msg = "Are you sure you want to delete the selected items?";
    else {
      var record = recSet.getRecord(oSelectedTR.id);
      msg = "Are you sure you want to delete " + record.getData('name') + "?";
    }
  }

  if (confirm(msg)) {
    top.document.startWait(document);
    var compName = top.document.navDT.getRecord(top.document.navDT.getSelectedRows()[0]).getData('Name');
    var xmlStr = roxieSelectionToXML(this.parent.dt, "RoxieFarm", selRows, compName);

    YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/HandleRoxieOperation', {
      success: function(o) {
        var form = top.window.document.forms['treeForm'];
        form.isChanged.value = "true";
        top.document.stopWait(document);
        top.document.navDT.clickCurrentSelOrName(top.document.navDT);
      },
      failure: function(o) {
      top.document.stopWait(document);
        alert(o.statusText);
      },
      scope: this
    },
      top.document.navDT.getFileName(true) + 'Cmd=DeleteRoxieFarm&XmlArgs=' + xmlStr);
  }
}

function onMenuItemClickSlaveConfig(p_sType, p_aArgs, p_oValue) {
  if (top.document.forms['treeForm'].isLocked.value === 'false')
    return;
  top.document.navDT.promptSlaveConfig(top.document.navDT);
}
function onMenuItemClickFarmReplace(p_sType, p_aArgs, p_oValue) {
  if (top.document.forms['treeForm'].isLocked.value === 'false')
    return;

  top.document.navDT.displayRoxieClusterReplaceServer();
  return;
}

function onMenuItemClickImportServers(p_sType, p_aArgs, p_oValue) {
}

function onMenuItemClickExportServers(p_sType, p_aArgs, p_oValue) {
}

function onMenuItemClick(p_sType, p_aArgs, p_oValue) {
}

function onMenuItemClickTopology(p_sType, p_aArgs, p_oValue) {
  if (top.document.forms['treeForm'].isLocked.value === 'false')
    return;

  var oTarget = this.parent.contextEventTarget;
  var Dom = YAHOO.util.Dom;
  var oSelectedTR = oTarget.nodeName.toUpperCase() === "TR" ?
                    oTarget : Dom.getAncestorByTagName(oTarget, "TR");
  var dt = this.parent.dt;
  var recSet = dt.getRecordSet();
  var selRows = dt.getSelectedRows();
  var menuItemName = this.cfg.getProperty("text");
  var record = recSet.getRecord(selRows[0]);
  var type;
  //use 'name' instead of '_key' for complex tables
  var recName = record.getData('name');
  var temp = new Array();
  temp = recName.split(' ');
  var temp1 = new Array();
  temp1 = menuItemName.split(' ');
  var oper = "Add";
  if (menuItemName.indexOf("Delete") === 0)
    oper = "Delete";

  var params = '';
  var parRec = record;
  var i = 0;
  while (true) {
    if (parRec.getData('parent') != -1) {
      params += 'inner' + i + '_name=' + parRec.getData('name') + '::' + 'inner' + i++ + '_value=' + parRec.getData('value') + "::";
      parRec = getRecord(dt, parRec.getData('parent'));
    }
    else
      break;
  }

  var xmlArgs = "<XmlArgs compType = '" + temp[0] + "' name = '" + temp[temp.length - 1] +
                "' newType = '" + temp1[temp1.length - 1] + "' params = '" + params + "'/>";

  YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/HandleTopology', {
    success: function(o) {
      var form = top.window.document.forms['treeForm'];
      form.isChanged.value = "true";
      top.document.stopWait(document);
      top.document.navDT.clickCurrentSelOrName(top.document.navDT);
    },
    failure: function(o) {
    top.document.stopWait(document);
      alert(o.statusText);
    },
    scope: this
  },
  top.document.navDT.getFileName(true) + 'Operation=' + oper + '&XmlArgs=' + xmlArgs);
}
function onMenuItemClickThorTopologyDelete(p_sType, p_aArgs, p_oValue) {
  if (top.document.forms['treeForm'].isLocked.value === 'false')
    return;

  var oTarget = this.parent.contextEventTarget;
  var Dom = YAHOO.util.Dom;
  var oSelectedTR = oTarget.nodeName.toUpperCase() === "TR" ?
                    oTarget : Dom.getAncestorByTagName(oTarget, "TR");
  var dt = this.parent.dt;
  var recSet = dt.getRecordSet();
  var selRows = dt.getSelectedRows();
  var menuItemName = this.cfg.getProperty("text");
  //use 'name' instead of '_key' for complex tables
  var compName = top.document.navDT.getRecord(top.document.navDT.getSelectedRows()[0]).getData('Name');
  var xmlStr = "<ThorData name=\"" + compName + "\" >";
  for (var idx = 0; idx < selRows.length; idx++) {
    xmlStr += "<Node processName=\"" + dt.getRecord(selRows[idx]).getData('_processId') + "\" type=\"" + dt.getRecord(selRows[idx]).getData('process') + "\"/>";
  }

  xmlStr += "</ThorData>";

  YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/HandleThorTopology', {
    success: function(o) {
      if (o.responseText.indexOf("<html") === 0) {
        var temp = o.responseText.split(/td align=\"left\">/g);
        var temp1 = temp[1].split(/<\/td>/g);
        top.document.stopWait(document);
        alert(temp1[0]);
      }
      else {
        var form = top.window.document.forms['treeForm'];
        form.isChanged.value = "true";
        top.document.stopWait(document);
        top.document.navDT.clickCurrentSelOrName(top.document.navDT);
      }
    },
    failure: function(o) {
    top.document.stopWait(document);
      alert(o.statusText);
    },
    scope: this
  },
      top.document.navDT.getFileName(true) + 'Operation=Delete&XmlArgs=' + xmlStr);
}
function onMenuItemClickThorTopology(p_sType, p_aArgs, p_oValue) {
  if (top.document.forms['treeForm'].isLocked.value === 'false')
    return;

  var oTarget = this.parent.contextEventTarget;
  var Dom = YAHOO.util.Dom;
  var oSelectedTR = oTarget.nodeName.toUpperCase() === "TR" ?
                    oTarget : Dom.getAncestorByTagName(oTarget, "TR");
  var dt = this.parent.dt;
  var recSet = dt.getRecordSet();
  var selRows = dt.getSelectedRows();
  var menuItemName = this.cfg.getProperty("text");
  var record = recSet.getRecord(selRows[0]);
  var slavesPresent = false;
  var recSetLen = recSet.getLength();
  for (var i = 0; i < recSetLen; i++) {
      var r = recSet.getRecord(i);
      if (r.getData('process') === 'Slave') {
        slavesPresent = true;
        break;
      }
  }

  var slavesPerNode = getAttrValFromArr(rows.Attributes, 'slavesPerNode');
  if (slavesPerNode === "")
    slavesPerNode = "1";

  var type;
  //use 'name' instead of '_key' for complex tables
  var recName = record.getData('name');
  var temp = new Array();
  temp = recName.split(' ');
  var temp1 = new Array();
  temp1 = menuItemName.split(' ');
  var type = "Slave";
  if (menuItemName === "Add Master...")
    type = "Master";
  else if (menuItemName === "Add Spares...")
    type = "Spare";
  top.document.navDT.promptThorTopology(top.document.navDT, type, slavesPresent, slavesPerNode);
}
function onContextMenuBeforeShow(p_sType, p_aArgs) {
  if (top.document.ContextMenuLeft != null)
    top.document.ContextMenuLeft.clearContent();

  if (!this.configContextMenuItems) {
    this.configContextMenuItems = {
      "Roxie Cluster": [
                                { text: "Add Farm", onclick: { fn: onMenuItemClickAddFarm} }
                                ],
      "RoxieFarmProcess": [
      //{text: "AddFarm", onclick: { fn: onMenuItemClickAddFarm}},
                                {text: "Add Servers", onclick: { fn: onMenuItemClickAddServers}}//,
                                //{ text: "Replace...", onclick: { fn: onMenuItemClickFarmReplace}}//,
                                //{text: "ImportServers...", onclick: { fn: onMenuItemClickImportServers}},
                                //{text: "ExportServers...", onclick: { fn: onMenuItemClickExportServers}}
                            ],
      "RoxieServerProcess": [
                                { text: "Replace...", onclick: { fn: onMenuItemClickFarmReplace}}//,
                                //{text: "ImportServers", onclick: { fn: onMenuItemClickImportServers}},
                                //{text: "ExportServers", onclick: { fn: onMenuItemClickExportServers}}
                            ],
      "RoxieSlave": [
                                { text: "Reconfigure Agents...", onclick: { fn: onMenuItemClickSlaveConfig}}
                            ],
      "Delete": [
                                { text: "Delete", onclick: { fn: onMenuItemClickDelete} }
                            ],
      "Topology": [
                                { text: "Add Cluster", onclick: { fn: onMenuItemClickTopology} }
                            ],
      "TopologyEclCCServer": [
                                { text: "Delete", onclick: { fn: onMenuItemClickTopology} }
                            ],
      "TopologyEclServer": [
                                { text: "Delete", onclick: { fn: onMenuItemClickTopology} }
                           ],
      "TopologyEclScheduler": [
                                { text: "Delete", onclick: { fn: onMenuItemClickTopology} }
                            ],                            
      "TopologyCluster": [
                               { text: "Add EclAgent", onclick: { fn: onMenuItemClickTopology} },
                               { text: "Add EclCCServer", onclick: { fn: onMenuItemClickTopology} },
                               { text: "Add EclServer", onclick: { fn: onMenuItemClickTopology} },
                               { text: "Add EclScheduler", onclick: { fn: onMenuItemClickTopology} },
                               { text: "Add Thor", onclick: { fn: onMenuItemClickTopology} },
                               { text: "Add Roxie", onclick: { fn: onMenuItemClickTopology} },
                               { text: "Delete", onclick: { fn: onMenuItemClickTopology} }
                            ],
      "TopologyThor": [
                               { text: "Delete", onclick: { fn: onMenuItemClickTopology} }
                            ],
      "TopologyRoxie": [
                               { text: "Delete", onclick: { fn: onMenuItemClickTopology} }
                            ],
      "TopologyEclAgent": [
                               { text: "Delete", onclick: { fn: onMenuItemClickTopology} }
                            ],
      "ThorClusterRoot": [
                               { text: "Add Master...", onclick: { fn: onMenuItemClickThorTopology} },
                               { text: "Add Spares...", onclick: { fn: onMenuItemClickThorTopology} }
                            ],
      "ThorClusterMaster": [
                               { text: "Add Slaves...", onclick: { fn: onMenuItemClickThorTopology} },
                               { text: "Add Spares...", onclick: { fn: onMenuItemClickThorTopology} }
                            ],
      "ThorClusterSlave": [
                               { text: "Add Spares...", onclick: { fn: onMenuItemClickThorTopology} }
                            ],
      "ThorClusterDelete": [
                               { text: "Delete", onclick: { fn: onMenuItemClickThorTopologyDelete} }
                            ]
    };
  }

  var oTarget = this.contextEventTarget, aMenuItems, aClasses;
  if (this.getRoot() === this) {
    var Dom = YAHOO.util.Dom;
    var oSelectedTR = oTarget.nodeName.toUpperCase() === "TR" ?
                        oTarget : Dom.getAncestorByTagName(oTarget, "TR");
    var dt = this.dt;
    var recSet = dt.getRecordSet();
    var record = recSet.getRecord(oSelectedTR.id);
    var parentName = record.getData('compType');

    if (parentName.length <= 0) {
      if (dt.configs.element === "AgentsTab")
        parentName = "RoxieSlave";
      else if (dt.configs.element === "ServersTab")
        parentName = "Roxie Cluster";
    }
    else if (parentName === "Topology") {
      //use 'name' instead of '_key' for complex tables.
      var recName = record.getData('name');

      if (recName.indexOf("Cluster") === 0)
        parentName += "Cluster";
      else if (recName.indexOf("EclCCServer") === 0)
        parentName += "EclCCServer";
      else if (recName.indexOf("EclServer") === 0)
        parentName += "EclServer";
      else if (recName.indexOf("EclScheduler") === 0)
        parentName += "EclScheduler";  
      else if (recName.indexOf("ThorCluster") === 0)
        parentName += "Thor";
      else if (recName.indexOf("RoxieCluster") === 0)
        parentName += "Roxie";        
      else if (recName.indexOf("EclAgent") === 0)
        parentName += "EclAgent";
      else if (recName.indexOf("process") === 0 || recName.indexOf("name") === 0 ||
                recName.indexOf("prefix") === 0 || recName.indexOf("build") === 0)
        parentName = "";
    }
    else if (parentName === "ThorCluster") {
      var recType = record.getData('process');

      if (recType === "Master")
        parentName += "Master";
      else if (recType === "Slave" || recType === "Spare")
        parentName += "Slave";
      else if (recType === "")
        parentName += "Root";
    }

    var oContextMenuItems = this.configContextMenuItems;

    aMenuItems = oContextMenuItems[parentName];
    this.clearContent();

    if (typeof (aMenuItems) === 'undefined' || aMenuItems.length === 0)
      return false;

    // Add the new set of items to the ContentMenu instance                    
    this.addItems(aMenuItems);

    if (parentName === "RoxieFarmProcess" || parentName === "RoxieServerProcess" || parentName === "RoxieSlave")
      this.addItems(oContextMenuItems["Delete"], 1);
    else if (parentName.indexOf('Topology') === 0) {
      if (parentName === 'TopologyCluster') {
        for (var cIdx = 0; cIdx < recSet.getLength(); cIdx++) {
          var r = recSet.getRecord(cIdx);
          if (r.getData('parent') === record.getData('id'))
          {
            if (r.getData('name').indexOf('ThorCluster') == 0) {
              this.getItem(5).cfg.setProperty("disabled", true);
            }
            else if (r.getData('name').indexOf('RoxieCluster') == 0) {
              this.getItem(4).cfg.setProperty("disabled", true);
              this.getItem(0).cfg.setProperty("disabled", true);
            }
            else if (r.getData('name').indexOf('EclAgent') == 0) {
              this.getItem(5).cfg.setProperty("disabled", true);
            }
            else if (r.getData('name').indexOf('EclServer') == 0) {
              this.getItem(1).cfg.setProperty("disabled", true);
            }
            else if (r.getData('name').indexOf('EclCCServer') == 0) {
              this.getItem(2).cfg.setProperty("disabled", true);
            }
            else {
              var flag = this.getItem(2).cfg.getProperty("disabled");

              if (!flag) {
                for (i = 0; i < top.document.navDT.navTreeData[0]["menuComps"].length; i++)
                  if (top.document.navDT.navTreeData[0]["menuComps"][i] === 'eclserver') {
                    flag = true;
                    break;
                  }

                if (!flag)
                  this.getItem(2).cfg.setProperty("disabled", true);
              }
            }
          }
        }
      }
    }
    else if (parentName === "ThorClusterMaster" || parentName === "ThorClusterSlave")
      this.addItems(oContextMenuItems["ThorClusterDelete"], 1);
    else if (parentName === "ThorClusterRoot") {
      for (var tIdx = 0; tIdx < recSet.getLength(); tIdx++) {
        var r = recSet.getRecord(tIdx);
        if (r.getData('process') === 'Master') {
          this.getItem(0).cfg.setProperty("disabled", true);
          break;
        }
      }
    }

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

function selectLastActiveTab() {
  var tabView = top.document.RightTabView;

  if (typeof (top.document.activeTab) !== 'undefined' && (typeof (tabView) !== 'undefined' && tabView !== null)) {
    tabView.inInit = false;
    var tabs = tabView.get("tabs");
    var tab = null;
    for (i = 0; i < tabs.length; i++) {
      if (tabs[i].get("label") === top.document.activeTab) {
        tab = tabs[i];
        break;
      }
    }

    if (i == tabs.length)
      i = 0;

    if (i == 0 && top.document.needsRefresh !== true)
      tabView.inInit = true;

    top.document.needsRefresh = false;
    top.document.navDT._elBdContainer.scrollTop =  top.document.navigatorScrollOffset;
    tabView.selectTab(i);
  }
  else if (typeof (tabView) !== 'undefined' && tabView !== null) {
  tabView.inInit = true;
  top.document.needsRefresh = false;
  tabView.selectTab(0);
  }
}

function doPageRefresh(msg) {
  top.document.navDT.doPageRefresh(msg);
}

function onMenuItemClickAddInstance(p_sType, p_aArgs, p_oValue) {
  if (top.document.forms['treeForm'].isLocked.value === 'false')
    return;

  top.document.navDT.displayAddInstance();
  return;
}

function onMenuItemClickGenericAddDelete(p_sType, p_aArgs, p_oValue) {
  if (top.document.forms['treeForm'].isLocked.value === 'false')
    return;
  var oTarget = this.parent.contextEventTarget;
  var Dom = YAHOO.util.Dom;
  var oSelectedTR = oTarget.nodeName.toUpperCase() === "TR" ?
                            oTarget : Dom.getAncestorByTagName(oTarget, "TR");
  var dt = this.parent.dt;
  var recSet = dt.getRecordSet();

  var menuid = this.parent.id.split("Menu");
  if (menuid.length > 1)
    parentName = menuid[0];

  var type = compTabToNode[parentName];

  if (dt.parentDT)
    type = dt.parentDT.getRecord(dt.parentDT.getSelectedRows()[0]).getData('name');

  var subRecs = null;
  var subRecSet = null;

  if (dt.parentDT) {
    for (i = 0; i < dt.parentDT.subTables.length; i++)
      if (dt === dt.parentDT.subTables[i].oDT) {
      type = dt.parentDT.subTables[i].oName;
      var subSelRows = dt.getSelectedRows();
      subRecs = subSelRows; //dt.getRecord(subSelRows[0]);
      subRecSet = dt.getRecordSet();
      break;
    }

    dt = dt.parentDT;
  }
  var recSet = dt.getRecordSet();
  var selRows = dt.getSelectedRows();
  var menuItemName = this.cfg.getProperty("text");
  var oper = "Delete";
  if (menuItemName === "Add")
    oper = "Add";

  var curNavRec = top.document.navDT.getRecord(top.document.navDT.getSelectedRows()[0]);
  var xmlArgs;

  if (subRecs !== null) {
    var prnt;
    var prntType = type;
    if (typeof (dt.configs.element) === 'string') {
      prnt = dt.configs.element.split("Tab");
      if (prnt.length > 1)
        prntType = prnt[0];
    }

    var typeName = type.split("_");
    if (typeName.length > 1)
      type = typeName[1];

    if (compTabToNode[type]) {
      var t = type;
      type = compTabToNode[t];
    }

    xmlArgs = '<XmlArgs rowType = \"' + type + '\" compName=\"' + curNavRec.getData('Name') + '\" buildSet = \"' + curNavRec.getData('BuildSet') + '\">';

    if (oper === "Delete") {
      if (subRecs.length)
        for (i = 0; i < subRecs.length; i++) {
        var subRec = subRecSet.getRecord(subRecs[i]);
        xmlArgs += '<Row params = \"' + subRec.getData('params') + '\"/>';
      }
    }
    else //use name instead of '_key' for multirow tables
    {
      var r = recSet.getRecord(selRows[0]);
      var str="";
      for (var fld in r._oData) {
        if (fld.indexOf("_") == -1 && fld !== "params" && fld !== "compType" && r.getData(fld) != null)
          str += fld + "=" + r.getData(fld) + "::";
      }

      xmlArgs += '<Row params=\"' + r.getData('params') + '\" attrs=\"' + str + '\"/>';
    }

    xmlArgs += '</XmlArgs>';
  }
  else {
    xmlArgs = "<XmlArgs rowType = '" + type + "' compName='" + curNavRec.getData('Name') + "' buildSet = '" + curNavRec.getData('BuildSet') + "'>";

    //generic records have no _key, so use name
    if (oper === "Delete") {
      var selRows = dt.getSelectedRows();
      for (var idx = 0; idx < selRows.length; idx++) {
        if (curNavRec.getData('Name') !== 'Directories') {
          if (type !== "Notes") {
            var r = dt.getRecord(selRows[idx]);
            if (r.getData('name'))
              xmlArgs += "<Row rowName='" + dt.getRecord(selRows[idx]).getData('name') + "'/>";
            else
              xmlArgs += "<Row rowName=\"\" params=\"" + r.getData('params') + "\"/>";
          }
          else
            xmlArgs += "<Row rowDate='" + dt.getRecord(selRows[idx]).getData('date') + "'/>";
        }
        else
          xmlArgs += "<Row rowName='" + dt.getRecord(selRows[idx]).getData('instance') + "' compType='" + dt.getRecord(selRows[idx]).getData('component') + "'/>";
      }
    }

    xmlArgs += "</XmlArgs>";
  }


  YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/HandleRows', {
    success: function(o) {
      if (o.responseText.indexOf("<html") === 0) {
        var temp = o.responseText.split(/td align=\"left\">/g);
        var temp1 = temp[1].split(/<\/td>/g);
        top.document.stopWait(document);
        alert(temp1[0]);
      }
      else {
        var form = top.window.document.forms['treeForm'];
        form.isChanged.value = "true";
        top.document.stopWait(document);
        top.document.navDT.clickCurrentSelOrName(top.document.navDT);

        if (dt.parentDT) {
          top.document.selectRecord = dt.parentDT.getRecord(dt.parentDT.getSelectedRows()[0]).getData('name');
          top.document.selectRecordClick = true;
        }
        else {
          var temp = o.responseText.split(/<CompName>/g);
          if (temp.length > 1) {
            var temp1 = temp[1].split(/<\/CompName>/g);
            if (temp1.length > 1) {
              top.document.selectRecord = temp1[0];
              top.document.selectRecordClick = true;

              if (type === 'Notes')
                top.document.selectRecordField = 'date';
            }
          }
        }
      }
    },
    failure: function(o) {
    top.document.stopWait(document);
      alert(o.statusText);
    },
    scope: this
  },
  top.document.navDT.getFileName(true) + 'Operation=' + oper + '&XmlArgs=' + xmlArgs);

  return;
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


function instanceSelectionToXML(table, selectedRows) {
  var rec = getCurrentSelRec(top.document.navDT);
  var xmlStr = "<Instances buildSet='" + rec.getData("BuildSet") + "' compName='" + rec.getData("Name") + "' >";

  if (typeof (selectedRows) !== 'undefined')
    for (var i = 0; i < selectedRows.length; i++)
    xmlStr += "<Instance params=\"" + table.getRecord(selectedRows[i]).getData('params') + "\"/>";

  xmlStr += "</Instances>";

  return xmlStr;
}


function onMenuItemClickRemoveInstance(p_sType, p_aArgs, p_oValue) {
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
  var xmlStr = instanceSelectionToXML(dt, selRows);

  YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/HandleInstance', {
    success: function(o) {
      var form = top.window.document.forms['treeForm'];
      form.isChanged.value = "true";
      top.document.stopWait(document);
      top.document.navDT.clickCurrentSelOrName(top.document.navDT);
    },
    failure: function(o) {
    top.document.stopWait(document);
      alert(o.statusText);
    },
    scope: this
  },
  top.document.navDT.getFileName(true) + 'Operation=Delete&XmlArgs=' + xmlStr);
}

function onMenuItemClickHandleEspServiceBindings(p_sType, p_aArgs, p_oValue) {
  if (top.document.forms['treeForm'].isLocked.value === 'false')
    return;

  var oTarget = this.parent.contextEventTarget;
  var Dom = YAHOO.util.Dom;
  var oSelectedTR = oTarget.nodeName.toUpperCase() === "TR" ?
                    oTarget : Dom.getAncestorByTagName(oTarget, "TR");
  var dt = this.parent.dt;
  var type = "EspBinding";
  var subRecs = null;
  var subRecSet = null;

  if (dt.parentDT) {
    for (i = 0; i < dt.parentDT.subTables.length; i++)
      if (dt === dt.parentDT.subTables[i].oDT) {
      type = dt.parentDT.subTables[i].oName;
      var subSelRows = dt.getSelectedRows();
      subRecs = subSelRows; //dt.getRecord(subSelRows[0]);
      subRecSet = dt.getRecordSet();
      break;
    }

    dt = dt.parentDT;
  }
  var recSet = dt.getRecordSet();
  var selRows = dt.getSelectedRows();
  var menuItemName = this.cfg.getProperty("text");

  var oper = "Add";
  if (menuItemName === "Delete")
    oper = "Delete";

  var compName = top.document.navDT.getRecord(top.document.navDT.getSelectedRows()[0]).getData('Name');

  var xmlArgs;

  if (subRecs !== null) {
    xmlArgs = '<EspServiceBindings type = \"' + type + '\" compName=\"' + compName + '\">';

    if (subRecs.length)
      for (i = 0; i < subRecs.length; i++) {
      var subRec = subRecSet.getRecord(subRecs[i]);
      xmlArgs += '<Item resource=\"' + subRec.getData('resource') + '\" params = \"' + subRec.getData('params') +
                '\" path=\"' + subRec.getData('path') + '\" access = \"' + subRec.getData('access') + '\"/>';
    }
    else //use name instead of '_key' for multirow tables
      xmlArgs += '<Item params=\"subType=EspBinding[@name=\'' + recSet.getRecord(selRows[0]).getData('name') + '\']/::\" />';

    xmlArgs += '</EspServiceBindings>';
  }
  else {
    xmlArgs = '<EspServiceBindings type = \"' + type + '\" compName=\"' + compName + '\">';

    for (i = 0; i < selRows.length; i++) {
      //use name instead of '_key' for multirow tables
      var record = recSet.getRecord(selRows[i]);
      xmlArgs += '<Item name=\"' + record.getData('name') + '\" params = \"' + record.getData('params') + '\"/>';
    }

    xmlArgs += '</EspServiceBindings>';
  }

  YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/HandleEspServiceBindings', {
    success: function(o) {
      if (o.responseText.indexOf("<?xml") === 0) {
        var form = document.forms['treeForm'];
        var status = o.responseText.split(/<Status>/g);
        var status1 = status[1].split(/<\/Status>/g);

        if (status1[0] !== 'true') {
          alert(status1[0]);
          return;
        }
        else {
          var form = top.window.document.forms['treeForm'];
          form.isChanged.value = "true";
          top.document.stopWait(document);
          top.document.navDT.clickCurrentSelOrName(top.document.navDT);
          if (subRecs !== null) {
            top.document.selectRecord = recSet.getRecord(selRows[0]).getData('name');
            top.document.selectRecordClick = true;
          }
          else {
            var temp = o.responseText.split(/<NewName>/g);
            if (temp.length > 1) {
              var temp1 = temp[1].split(/<\/NewName>/g);
              if (temp1.length > 1) {
                top.document.selectRecord = temp1[0];
                top.document.selectRecordClick = true;
              }
            }
          }
        }
      }
    },
    failure: function(o) {
    top.document.stopWait(document);
      alert(o.statusText);
    },
    scope: this
  },
  top.document.navDT.getFileName(true) + 'Operation=' + oper + '&XmlArgs=' + xmlArgs);
}

function onMenuItemClickHandleComputerItemsCopy(p_sType, p_aArgs, p_oValue)
{
  var label = top.document.RightTabView.getTab(top.document.RightTabView.get('activeIndex')).get('label');

  if (label === "Computer Types")
    label = "ComputerType";
  else if (label === "Switches")
    label = "Switch";
  else if (label === "Domains")
    label = "Domain"
  else if (label === "Computers")
    label = "Computer"

  var dt = top.document.RightTabView.getTab(top.document.RightTabView.get('activeIndex')).dt;

  for (counter = 0; counter < dt.getSelectedRows().length; counter++)
  {
    var rec = dt.getRecord(dt.getRecordIndex(dt.getSelectedRows()[counter]));
    var param = label + rec.getData('params').split("subTypeKey=")[1];

    param = param.replace(/\@/g,'').replace(/\[/g,' ').replace(/\]/g,' ');

    top.document.copyHWSWTo(p_oValue.element.innerText, true, param);
  }
}

function onMenuItemClickHandleComputer(p_sType, p_aArgs, p_oValue) {
  var form = top.document.forms['treeForm'];
  if (form.isLocked.value === 'false')
    return;

  var oTarget = this.parent.contextEventTarget;
  var Dom = YAHOO.util.Dom;
  var oSelectedTR = oTarget.nodeName.toUpperCase() === "TR" ?
                    oTarget : Dom.getAncestorByTagName(oTarget, "TR");
  var dt = this.parent.dt;
  var recSet = dt.getRecordSet();
  var selRows = dt.getSelectedRows();
  var menuItemName = this.cfg.getProperty("text");
  var record = recSet.getRecord(selRows[0]);
  var menuid = this.parent.id.split("Menu");
  if (menuid.length > 1)
    parentName = menuid[0];

  var type = compTabToNode[parentName];

  var menuItemName = this.cfg.getProperty("text");
  var oper = "Delete";
  if (menuItemName === "New" || menuItemName === "New Range...")
    oper = "New";

  if (parentName === "Computers" && ( menuItemName === "New Range..." || menuItemName === "New")) {
    for (i = 0; i < recSet.getLength(); i++) {
      var arr = recSet.getRecord(i).getData('domain_extra');
      if (YAHOO.lang.isArray(arr))
        break;
    }

    if (i >= recSet.length) {
      alert("get domain from backend");
      return;
    }
    
    var type =( menuItemName === "New Range..." ) ? 0 : 3;
    top.document.navDT.promptNewRange(recSet.getRecord(i).getData('domain_extra'), recSet.getRecord(i).getData('computerType_extra'), type);
    return;
  }

  var xmlArgs = "<XmlArgs type = '" + type + "'>";

  if (oper === 'Delete') {
    //use name instead of '_key' for multi row tables
    for (i = 0; i < selRows.length; i++)
      xmlArgs += "<Item name='" + recSet.getRecord(selRows[i]).getData('name') + "'/>";
  }

  xmlArgs += '</XmlArgs>';

  YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/HandleComputer', {
    success: function(o) {
      if (o.responseText.indexOf("<html") === 0) {
        var temp = o.responseText.split(/td align=\"left\">/g);
        var temp1 = temp[1].split(/<\/td>/g);
        top.document.stopWait(document);
        alert(temp1[0]);
      }
      else {
        form.isChanged.value = "true";
        top.document.stopWait(document);
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
    },
    failure: function(o) {
      top.document.stopWait(document);
      alert(o.statusText);
    },
    scope: this
  },
  top.document.navDT.getFileName(true) + 'Operation=' + oper + '&XmlArgs=' + xmlArgs);
}

function onContextMenuBeforeShowRegular(p_sType, p_aArgs) {
  if (!this.suppressMenus) {
    this.suppressMenus = new Array();
    this.suppressMenus[this.suppressMenus.length] = 'CSRMenu';
    this.suppressMenus[this.suppressMenus.length] = 'CertificateMenu';
    this.suppressMenus[this.suppressMenus.length] = 'PrivateKeyMenu';
  }
  if (isPresentInArr(this.suppressMenus, this.id)) {
    this.clearContent();
    return false;
  }

  if (!this.configContextMenuItems) {
    this.configContextMenuItems = {
      "Instances": [
                                { text: "Add Instances...", onclick: { fn: onMenuItemClickAddInstance} },
                                { text: "Remove Instances", onclick: { fn: onMenuItemClickRemoveInstance} }
                                ],
      "ESP Service Bindings": [
                                { text: "Add", onclick: { fn: onMenuItemClickHandleEspServiceBindings} },
                                { text: "Delete", onclick: { fn: onMenuItemClickHandleEspServiceBindings} }
                                ],
      "URL Authentication": [
                                { text: "Add", onclick: { fn: onMenuItemClickHandleEspServiceBindings} },
                                { text: "Delete", onclick: { fn: onMenuItemClickHandleEspServiceBindings} }
                                ],
      "Feature Authentication": [
                                { text: "Add", onclick: { fn: onMenuItemClickHandleEspServiceBindings} },
                                { text: "Delete", onclick: { fn: onMenuItemClickHandleEspServiceBindings} }
                                ],
      "Computers": [
                                { text: "New", onclick: { fn: onMenuItemClickHandleComputer} },
                                { text: "New Range...", onclick: { fn: onMenuItemClickHandleComputer} },
                                { text: "Delete", onclick: { fn: onMenuItemClickHandleComputer} },
                                { text: "Copy Hardware Item(s) To",
                                  submenu: {
                                    id: "HWCopyItems",
                                    lazyload: true,
                                    itemdata: top.document.copyCompMenu2
                                    } } ],
      "Hardware": [
                                { text: "New", onclick: { fn: onMenuItemClickHandleComputer} },
                                { text: "Delete", onclick: { fn: onMenuItemClickHandleComputer} },
                                { text: "Copy Hardware Item(s) To",
                                  submenu: {
                                     id: "HWCopyItems",
                                     lazyload: true,
                                     itemdata: top.document.copyCompMenu2
                                    } }
                                ],
      "GenericAddDelete": [
                                { text: "Add", onclick: { fn: onMenuItemClickGenericAddDelete} },
                                { text: "Delete", onclick: { fn: onMenuItemClickGenericAddDelete} }
                                ],
      "ResetToDefault":         [ {text: "Reset to default", onclick: {fn: onMenuItemClickResetToDefault} }],
      "WriteToEnvironment":     [ {text: "Write to environment", onclick: {fn: onMenuItemClickResetToDefault} }]
     };
  }

  for (var count = 0; count < this.configContextMenuItems.Hardware[2].submenu.itemdata.length; count++)
  {
    if (typeof(this.configContextMenuItems.Hardware[2].submenu.itemdata[count]) !== 'undefined')
      this.configContextMenuItems.Hardware[2].submenu.itemdata[count].onclick.fn  = onMenuItemClickHandleComputerItemsCopy;
  }


var oTarget = this.contextEventTarget, aMenuItems, aClasses;
  if (this.getRoot() === this) {
    var Dom = YAHOO.util.Dom;
    var oSelectedTR = oTarget.nodeName.toUpperCase() === "TR" ?
                                    oTarget : Dom.getAncestorByTagName(oTarget, "TR");
    var dt = this.dt;
    var recSet = dt.getRecordSet();
    var record = recSet.getRecord(oSelectedTR.id);
    var column = dt.getColumn(oTarget);
    var compType;
    if (record)
      compType = record.getData('compType');
    else {
      if (recSet.getLength() > 0)
        compType = recSet.getRecord(0).getData('compType');
    }

    var parentName;


    var menuid = this.id.split("Menu");
    if (menuid.length > 1)
      parentName = menuid[0];

    if (typeof (parentName) === 'undefined')
      parentName = "Roxie Cluster";

    var oContextMenuItems = this.configContextMenuItems;
    aMenuItems = oContextMenuItems[parentName];

    if (!aMenuItems) {
      if (isPresentInArr(menuEnabled, compTabToNode[parentName])) {
        if (compTabToNode[parentName] === "Domain" || compTabToNode[parentName] === "Switch" || compTabToNode[parentName] === "ComputerType")
          parentName = "Hardware";
        else
          parentName = "GenericAddDelete";
        aMenuItems = oContextMenuItems[parentName];
      }
    }
    this.clearContent();
    
    if (aMenuItems)
      this.addItems(aMenuItems);
        
    if (parentName === "RoxieFarmProcess" || parentName === "RoxieServerProcess" || parentName === "RoxieSlave")
      this.addItems(oContextMenuItems["Delete"], 2);
    else if (parentName === "GenericAddDelete" && recSet.getLength() === 0) {
      this.getItem(1).cfg.setProperty("disabled", true);
    }

    if( record && record.getData('_not_in_env') === 1 && record.getData(column.key + '_ctrlType') !== 0)
    {
      top.document.ContextMenuCenter = this;
      this.addItems(oContextMenuItems["WriteToEnvironment"]);
    }

    if( record && record.getData(column.key + '_ctrlType') !== 0 ) {
        var defaultValue=dt.getDefault(oTarget, record);
        if( defaultValue !== '' && typeof(defaultValue) !== 'undefined' && defaultValue !== record.getData('value') )
           this.addItems(oContextMenuItems["ResetToDefault"]);
      }

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
function isPresentInArr(arr, val) {
  for (idx = 0; idx < arr.length; idx++)
    if (arr[idx] === val)
    return true;

  return false;
}

function getAttrValFromArr(arr, attr) {
  for (idx = 0; idx < arr.length; idx++)
    if (arr[idx]['name'] && arr[idx]['value'] && arr[idx]['name'] === attr)
    return arr[idx]['value'];

  return "";
}

//only to be called from unload page
function handleunload(fromUnload) {
  if (top.window.frames["DisplaySettingsFrame"].event && 
      top.window.frames["DisplaySettingsFrame"].event.srcElement &&
      top.window.frames["DisplaySettingsFrame"].event.srcElement.nodeName !== 'BODY' &&
      top.window.frames["DisplaySettingsFrame"].event.srcElement.nodeName !== '#document')
    return;
  var tabView = top.document.RightTabView;
  if (tabView) {
    var actTab = tabView.get("activeTab");
    saveOpenEditors(actTab.dt);
    var tabs = tabView.get("tabs");
    for (var idx = 0; idx < tabs.length; idx++) {
      if (!tabs[idx].dt)
        continue;
      tabs[idx].dt.cleanup();
      tabs[idx].dt.destroy();
      tabs[idx].dt = null;
      tabs[idx].dataSource = null;
    }

    tabView.removeListener("activeIndexChange");
  }
  
  if (fromUnload)
    top.document.RightTabView = null;
}

function showNextCellEditor(celled) {
  var record = celled.getRecord(),
      column = celled.getColumn(),
      datatable = celled.getDataTable();

  datatable.gotonexttab = false;
  var recIndex = datatable.getRecordIndex(record);
  var exit = false;
  var doneAll = false;
  var colIndex = column.getKeyIndex() + 1;
  while (true) {
    var rec = datatable.getRecord(recIndex);

    if (!rec && !doneAll) {
      rec = datatable.getRecord(0);
      doneAll = true;
    }

    if (!rec)
      break;
    while (true) {
      if (colIndex >= datatable.getColumnSet().getDefinitions().length)
        break;

      nextCol = datatable.getColumn(colIndex);

      if (!nextCol)
        break;

      var colType = rec.getData(nextCol.key + '_ctrlType');
      if (colType !== 0) {
        var tdEl = datatable.getTdEl({ record: rec, column: nextCol });
        if (tdEl) {
          datatable.unselectAllRows();
          datatable.selectRow(rec);
          YAHOO.util.UserAction.click(tdEl);
          exit = true;
          break;
        }
      }
      else
        colIndex++;
    }

    if (exit)
      break;
    else {
      recIndex++;
      colIndex = 0;
    }
  }
}

function showPrevCellEditor(celled) {
  var record = celled.getRecord(),
      column = celled.getColumn(),
      datatable = celled.getDataTable();

  datatable.gotoprevtab = false;
  var recIndex = datatable.getRecordIndex(record);
  var exit = false;
  var doneAll = false;
  var colIndex = column.getKeyIndex() - 1;
  while (true) {
    var rec = datatable.getRecord(recIndex);

    if (!rec && !doneAll) {
      rec = datatable.getRecord(datatable.getRecordSet().getLength()-1);
      doneAll = true;
    }

    if (!rec)
      break;
    while (true) {
      if (colIndex < 0)
        break;

      nextCol = datatable.getColumn(colIndex);

      if (!nextCol)
        break;

      var colType = rec.getData(nextCol.key + '_ctrlType');
      if (colType !== 0) {
        var tdEl = datatable.getTdEl({ record: rec, column: nextCol });
        if (tdEl) {
          datatable.unselectAllRows();
          datatable.selectRow(rec);
          YAHOO.util.UserAction.click(tdEl);
          exit = true;
          break;
        }
      }
      else
        colIndex--;
    }

    if (exit)
      break;
    else {
      recIndex--;
      colIndex = datatable.getColumnSet().getDefinitions().length - 1;
    }
  }
}

function showCurrentCellEditor(datatable) {
  var record = datatable.getRecord(datatable.getSelectedRows()[0]);
  var recIndex = datatable.getRecordIndex(record);
  var doneAll = false;
  var colIndex = 0;
  while (true) {
    if (colIndex >= datatable.getColumnSet().getDefinitions().length)
      break;

    nextCol = datatable.getColumn(colIndex);

    if (!nextCol)
      break;

    var colType = record.getData(nextCol.key + '_ctrlType');
    if (colType !== 0) {
      var tdEl = datatable.getTdEl({ record: record, column: nextCol });
      if (tdEl) {
        datatable.unselectAllRows();
        datatable.selectRow(record);
        YAHOO.util.UserAction.click(tdEl);
        break;
      }
    }
    else
      colIndex++;
  }
}

function handlekeydown(event) {
  //ctrl+`
  if (event.keyCode === 192 && event.ctrlKey === true)
    setFocusToNavTable();
  else if ((event.keyCode === 37 || event.keyCode === 39) && event.ctrlKey === true) {
  var tabView = top.document.RightTabView;
  if (tabView) {
    saveOpenEditors(tabView.get("activeTab").dt);
      var actTab = tabView.get("activeIndex");
      if (event.keyCode === 39) {
        actTab++;
        if (actTab >= tabView.get("tabs").length)
          actTab = 0;
      }
      else if (event.keyCode === 37) {
        actTab--;
        if (actTab < 0)
          actTab = tabView.get("tabs").length - 1;
      }

      tabView.selectTab(actTab);
      //setTimeout("setFocusToActiveTabDT()", 250);
      
    }
  }
}

function handlemousedown(event) {
top.document.navDT.fireEvent("tableBlurEvent");
if (top.document.ContextMenuLeft != null)
  top.document.ContextMenuLeft.clearContent();
}

function getAttrName(datatable, column, record, isComplex) {
  var attrName;
  var cols = datatable.getColumnSet();
  if (cols.keys.length === 2 && cols.keys[0].key === 'name' && cols.keys[1].key === 'value') {
    if (isComplex)
      attrName = record.getData('name');
    else
      attrName = record.getData('_key');
  }
  else
    attrName = column.key;

  if (typeof (attrName) === 'undefined')
    attrName = column.key;

  return attrName;
}

function moveScrollBar()
{
  window.scrollTo(0,0);
}

function setChildrenOf(parent, rec) {
  if (typeof(childrenOf) === 'undefined')
    childrenOf = new Array();
  
  if (!childrenOf[parent])
    childrenOf[parent] = new Array();
 
  childrenOf[parent][childrenOf[parent].length] = rec;
}

function parseParamsForXPath(params, key, value, hasChildren, skip)
{
  var splitParams = params.split(":");
  var xpath = "";

  for (idx = splitParams.length-1; idx >= 0; idx--)
  {
    var pcTypePos = splitParams[idx].indexOf("pcType");
    var pcNamePos = splitParams[idx].indexOf("pcName");

    if (pcTypePos != -1)
    {
      if ( splitParams[idx].substr(pcTypePos+7) === "Environment" )
        xpath = "./";
      else
       xpath = xpath + splitParams[idx].substr(pcTypePos+7) + "/";
    }
    else if (pcNamePos != -1)
    {
      if (splitParams[idx].substr(pcNamePos+8) != "")
      {
        if (xpath[xpath.length-1] === ']')
        {
          xpath = xpath + "/";
        }
        xpath = xpath + splitParams[idx-1].substr(pcTypePos+8);
        xpath = xpath + "[@name='" + splitParams[idx].substr(pcNamePos+7) + "']";
        idx--;
      }
      else if (xpath[xpath.length-1] == ']')
      {
        xpath = xpath + "/";
      }
    }
  }

  if (hasChildren == true || xpath.substr(0,14) == "./EnvSettings/")
    return xpath;

  if (key === "name")
  {
    xpath = xpath + "]";
  }
  else if (skip == false)
  {
    if (xpath[xpath.length-1] == '/')
    {
      xpath = xpath.substring(0,xpath.length-1);
    }
    xpath = xpath + "[@" + key + "='" + value + "']"
  }

  return xpath;
}

function initEnvXmlType(i) {
  i.name_extra = "";
  i.name_ctrlType = 0;
  i.value_extra = "";
  i.value_ctrlType = 1;
}

function updateParams(record, attrName, oldValue, newValue, updateAttr, prevVal, updateVal, paramstr) {
  var newParams = '';
  if (record)
    newParams = record.getData('params');
  else
    newParams = paramstr;

  if (typeof (newParams) === 'undefined' || !newParams)
    return '';

  if (record && record.getData('compType') == 'Environment') {
    if (attrName !== 'name')
      return '';
    var level = record.getData('depth');
    var fl = false;
    var oldAttrVal = 'pcName=' + oldValue;
    var newAttrVal = 'pcName=' + newValue;
    var a = newParams.split('parentParams');
    if (a.length > 1) {
      for (var idx = 0; idx < a.length; idx++) {
        if (a[idx].indexOf(level) === 0) {
          var strs = a[idx].split(oldAttrVal);
          if (strs.length > 1) {
            a[idx] = strs.join(newAttrVal);
            fl = true;
          }
        }
      }

      if (fl)
        newParams = a.join('parentParams');
      else
        newParams = '';
    }

    return newParams;
  }
  
  var oldAttrVal = "@" + attrName + "=\'" + oldValue + "\'";
  var newAttrVal = "@" + attrName + "=\'" + newValue + "\'";
  var a = newParams.split(oldAttrVal);
  if (a.length > 1)
    newParams = a.join(newAttrVal);

  if (YAHOO.lang.isArray(updateAttr) && updateAttr.length) {
    if (prevVal[0].indexOf("<") === 0)
      oldAttrVal = "@" + updateAttr[0] + "=\'\'";
    else
      oldAttrVal = "@" + updateAttr[0] + "=\'" + prevVal[0] + "\'";
      
    if (updateVal[0].indexOf("<") === 0)
      newAttrVal = "@" + updateAttr[0] + "=\'\'";
    else
      newAttrVal = "@" + updateAttr[0] + "=\'" + updateVal[0] + "\'";
    a = newParams.split(oldAttrVal);
    if (a.length > 1)
      newParams = a.join(newAttrVal);
  }

  if (record)
    for (var subrec in record._oData) {
      if (subrec.indexOf("_") === 0) {
        var sub = record.getData(subrec);
        if (YAHOO.lang.isArray(sub)) {
          for (var idx = 0; idx < sub.length; idx++) {
            var sparams = sub[idx].params;
            var newsparams = updateParams(null, attrName, oldValue, newValue, updateAttr, prevVal, updateVal, sparams);
            if (newsparams.length > 0)
              sub[idx].params = newsparams;
          }
          record.setData(subrec, sub);
        }
        else if (sub){
          var sparams = sub.params;
          var newsparams = updateParams(null, attrName, oldValue, newValue, updateAttr, prevVal, updateVal, sparams);
          if (newsparams.length > 0)
            sub.params = newsparams;
        }
      }
    }

    return newParams;
}

function onMenuItemClickResetToDefault(p_sType, p_aArgs, p_oValue)
{
 if (top.document.forms['treeForm'].isLocked.value === 'false')
    return;

  var oTarget = this.parent.contextEventTarget;
  var dt = this.parent.dt;
  var recSet = dt.getRecordSet();
  var selRows = dt.getSelectedRows();
  var menuItemName = this.cfg.getProperty("text");
  var record = recSet.getRecord(selRows[0]);
  var oldValue = record.getData('value');
  
  var column = dt.getColumn(oTarget);
  var compName = top.document.navDT.getRecord(top.document.navDT.getSelectedRows()[0]).getData('Name');
  var bldSet = top.document.navDT.getRecord(top.document.navDT.getSelectedRows()[0]).getData('BuildSet');
  var meta = record.getData(column.key + '_extra');
  var menuItemName = this.cfg.getProperty("text");
  var flag = (menuItemName === 'Write to environment');
  var newValue = (flag ? oldValue : dt.getDefault(oTarget, record));

  if (flag || newValue !== oldValue) {
     var form = top.window.document.forms['treeForm'];
     top.document.startWait(document);
     var attrName = getAttrName(dt, column, record, false); // = record.getData('name');
     var recordIndex = dt.getRecordIndex(record);
     var category = "Software";
     var params = record.getData('params');
     if (compName === "Hardware")
       category = "Hardware";
     else if (compName === "Builds")
       category = "Programs";
     else if (compName === "Environment")
       category = "Environment";
     else if (compName === "topology") {
       category = 'Topology';
     }
   
   var params=record.getData('params'); 
   
    var xmlArgs = argsToXml(category, params, attrName, oldValue, newValue, recordIndex + 1, record.getData(column.key + '_onChange'));
    YAHOO.util.Connect.asyncRequest('POST', '/WsDeploy/SaveSetting', {
        success: function(o) {
          if (o.status === 200) {
            if (o.responseText.indexOf("<?xml") === 0) {
              form.isChanged.value = "true";
              top.document.needsRefresh = true;
              var updateAttr = o.responseText.split(/<UpdateAttr>/g);
              var prevVal, prevVal1, updateAttr1, updateVal, updateVal1;

              if (updateAttr[1].indexOf("</UpdateAttr>") !== 0) {
                updateAttr1 = updateAttr[1].split(/<\/UpdateAttr>/g);
                updateVal = o.responseText.split(/<UpdateValue>/g);
                updateVal1 = updateVal[1].split(/<\/UpdateValue>/g);
                if (updateVal1[0].indexOf("<") === 0)
                  this.updateCell(record, updateAttr1[0], "");
                else
                  this.updateCell(record, updateAttr1[0], updateVal1[0]);

                prevVal = o.responseText.split(/<PrevValue>/g);
                prevVal1 = prevVal[1].split(/<\/PrevValue>/g);
              }

              var newParams = updateParams(record, attrName, oldValue, newValue, updateAttr1, prevVal1, updateVal1);
              var newkey = '::Update';
              var idx = 1;
              if (typeof (newParams) !== 'undefined') {
                while (true) {
                  var tmpArr = newParams.split(newkey + idx);

                  if (tmpArr.length === 1)
                    break;

                  idx++;
                }

                if (newParams.length > 0)
                  record.setData('params', newParams);

                if (this.subTables) {
                  for (var tabidx = 0; tabidx < this.subTables.length; tabidx++) {
                    var subtablerecset = this.subTables[tabidx].oDT.getRecordSet();
                    var subrecsetlen = subtablerecset.getLength();
                    for (var recidx = 0; recidx < subrecsetlen; recidx++) {
                      var subrec = subtablerecset.getRecord(recidx);
                      newParams = updateParams(subrec, attrName, oldValue, newValue, updateAttr1, prevVal1, updateVal1);
                      subrec.setData('params', newParams);
                    }
                  }
                }
                else if (attrName === 'name' && record.getData('compType') === 'Environment') {
                  var recSet = datatable.getRecordSet();
                  var recSetLen = recSet.getLength();
                  var parent = record.getData('parent');
                  for (var i = 0; i < recSetLen; i++) {
                    var r = recSet.getRecord(i);
                    if (r != record && r.getData('parent') === parent)
                      r.setData('params', newParams);
                  }
                }
              }

              dt.updateCell(record, column, newValue);

              if (flag)
                removeExtraClassFromCell(dt, record, column, "not_in_env");

              if (compName === "topology") {
                //refresh for topology to refresh the left hand side values
                top.document.navDT.clickCurrentSelOrName(top.document.navDT);
              }
              if (this.parentDT) {
                parRec[recordIndex] = record.getData();
              }
              if (dt.gotonexttab === true)
                showNextCellEditor(this);
              else if (dt.gotoprevtab === true)
                showPrevCellEditor(this);

              var refreshPage = o.responseText.split(/<Refresh>/g);

              if (refreshPage[1].indexOf("</Refresh>") !== 0) {
                var refreshPage1 = refreshPage[1].split(/<\/Refresh>/g);
                if (refreshPage1[0] === "true")
                  doPageRefresh();
              }
            }
            else if (o.responseText.indexOf("<html") === 0) {
              var temp = o.responseText.split(/td align=\"left\">/g);
              var temp1 = temp[1].split(/<\/td>/g);
              alert(temp1[0]);
              column.editor.cancel();
              top.document.stopWait(document);
              dt.updateCell(record, column, oldValue);

              if (temp1[0].indexOf("Cannot modify") === 0) {
                form.isLocked.value = "false";
                doPageRefresh();
              }
            }
          } else {
            alert(r.replyText);
           dt.updateCell(record, column, newValue);
          }

          top.document.stopWait(document);
        },
        failure: function(o) {
          alert(o.statusText);
          callback();
          top.document.stopWait(document);
        },
        scope: this
      },
      top.document.navDT.getFileName(true) + 'XmlArgs=' + xmlArgs);
  }
}

function removeExtraClassFromCell(dt, record, column, classes)
{
  var Dom = YAHOO.util.Dom;
  var elCell = dt.getTdEl({record:record, column:column});
  var divEl = Dom.getChildren(elCell);
  Dom.removeClass(divEl[0], classes);
  var elPrevCell = dt.getPreviousTdEl(elCell);
  divEl = Dom.getChildren(elPrevCell);
  Dom.removeClass(divEl[0], classes);
}
