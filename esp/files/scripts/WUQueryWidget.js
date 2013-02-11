/*##############################################################################
#   HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
############################################################################## */
define([
    "dojo/_base/declare",
    "dojo/dom",
    "dojo/on",
    "dojo/dom-class",
    "dojo/data/ObjectStore",
    "dojo/date",
    "dijit/Menu",
    "dijit/MenuItem",
    "dijit/MenuSeparator",
    "dijit/PopupMenuItem",
    "dijit/Dialog",

    "dijit/layout/_LayoutWidget",
    "dijit/_TemplatedMixin",
    "dijit/_WidgetsInTemplateMixin",
    "dijit/registry",

    "dojox/grid/EnhancedGrid",
    "dojox/grid/enhanced/plugins/Pagination",
    "dojox/grid/enhanced/plugins/IndirectSelection",
    "dojox/widget/Calendar",

    "hpcc/WsWorkunits",
    "hpcc/WUDetailsWidget",

    "dojo/text!../templates/WUQueryWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/form/Textarea",
    "dijit/form/TimeTextBox",
    "dijit/form/Button",
    "dijit/form/RadioButton",
    "dijit/form/Select",
    "dijit/Toolbar",
    "dijit/TooltipDialog",
    "dijit/form/DateTextBox"
    
], function (declare, dom, on, domClass, ObjectStore, date, Menu, MenuItem, MenuSeparator, PopupMenuItem, Dialog,
                _LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin, registry,
                EnhancedGrid, Pagination, IndirectSelection, Calendar,
                WsWorkunits, WUDetailsWidget,
                template) {
    return declare("WUQueryWidget", [_LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
        templateString: template,
        baseClass: "WUQueryWidget",
        borderContainer: null,
        tabContainer: null,
        workunitsGrid: null,
        legacyPane: null,
        legacyPaneLoaded: false,

        tabMap: [],

        buildRendering: function (args) {
            this.inherited(arguments);
        },

        postCreate: function (args) {
            this.inherited(arguments);
            this.borderContainer = registry.byId(this.id + "BorderContainer");
            this.tabContainer = registry.byId(this.id + "TabContainer");
            this.workunitsGrid = registry.byId(this.id + "WorkunitsGrid");
            this.legacyPane = registry.byId(this.id + "Legacy");

            var context = this;
            this.tabContainer.watch("selectedChildWidget", function (name, oval, nval) {
                if (nval.id == context.id + "Workunits") {
                } else if (nval.id == context.id + "Legacy") {
                    if (!context.legacyPaneLoaded) {
                        context.legacyPaneLoaded = true;
                        context.legacyPane.set("content", dojo.create("iframe", {
                            src: "/WsWorkunits/WUQuery",
                            style: "border: 0; width: 100%; height: 100%"
                        }));
                    }
                } else {
                    if (!nval.initalized) {
                        nval.init(nval.params);
                    }
                }
                context.selectedTab = nval;
            });
        },

        startup: function (args) {
            this.inherited(arguments);
            this.refreshActionState();
            this.initWorkunitsGrid();
            domClass.add(this.id + "IconFilter", "hidden");
            
            validate = new Dialog({
                title: "Missing Fields",
                content: "Please make sure you have set at least one filter."
            });
            var today = new Date();
            var filterThreeDays = date.add(today, "day",- 3);
            dijit.byId(this.id + "FromDate").set("value",filterThreeDays);
            dijit.byId(this.id + "ToDate").set("value",today);
            dijit.byId(this.id + "threeDays").set("checked",true);
        },



        resize: function (args) {
            this.inherited(arguments);
            this.borderContainer.resize();
        },

        layout: function (args) {
            this.inherited(arguments);
        },

        destroy: function (args) {
            this.inherited(arguments);
        },

        //  Hitched actions  ---
        _onOpen: function (event) {
            var selections = this.workunitsGrid.selection.getSelected();
            for (var i = selections.length - 1; i >= 0; --i) {
                this.ensurePane(selections[i].Wuid, {
                    Wuid: selections[i].Wuid
                });
            }
        },
        _onDelete: function (event) {
            if (confirm('Delete selected workunits?')) {
                var context = this;
                WsWorkunits.WUAction(this.workunitsGrid.selection.getSelected(), "Delete", {
                    load: function (response) {
                        context.workunitsGrid.rowSelectCell.toggleAllSelection(false);
                        context.refreshGrid(response);
                    }
                });
            }
        },
        _onSetToFailed: function (event) {
            var context = this;
            WsWorkunits.WUAction(this.workunitsGrid.selection.getSelected(), "SetToFailed", {
                load: function (response) {
                    context.refreshGrid(response);
                }
            });
        },
        _onProtect: function (event) {
            var context = this;
            WsWorkunits.WUAction(this.workunitsGrid.selection.getSelected(), "Protect", {
                load: function (response) {
                    context.refreshGrid(response);
                }
            });
        },
        _onUnprotect: function (event) {
            var context = this;
            WsWorkunits.WUAction(this.workunitsGrid.selection.getSelected(), "Unprotect", {
                load: function (response) {
                    context.refreshGrid(response);
                }
            });
        },
        _onReschedule: function (event) {
        },
        _onDeschedule: function (event) {
        },
        _onClickFilterApply: function(event){
            this.workunitsGrid.rowSelectCell.toggleAllSelection(false);

            this.refreshGrid();
        },
        _onFilterApply: function (event) {
            this.workunitsGrid.rowSelectCell.toggleAllSelection(false);
            if(
               dom.byId(this.id + "Owner").value !== "" ||
               dom.byId(this.id + "Jobname").value !== "" ||
               dom.byId(this.id + "Cluster").value !== "" ||
               dom.byId(this.id + "State").value !== "" ||
               dom.byId(this.id + "ECL").value !== "" ||
               dom.byId(this.id + "LogicalFile").value !== "" ||
               dom.byId(this.id + "FromDate").value !== "" ||
               dom.byId(this.id + "FromTime").value !== "" ||
               dom.byId(this.id + "ToDate").value !== "" ||
               dom.byId(this.id + "LastNDays").value !== "" 
               ){
                domClass.remove(this.id + "IconFilter", "hidden");
                domClass.add(this.id + "IconFilter", "iconFilter");
                dijit.byId(this.id+"FilterDropDown").closeDropDown();
                this.refreshGrid();
            }else{
                validate.show();
            }
        },

        _onFilterClear: function(event, supressGridRefresh) {
            this.workunitsGrid.rowSelectCell.toggleAllSelection(false);
            dom.byId(this.id + "Owner").value = "";
            dom.byId(this.id + "Jobname").value = "";
            dom.byId(this.id + "Cluster").value = "";
            dom.byId(this.id + "State").value = "";
            dom.byId(this.id + "ECL").value = "";
            dom.byId(this.id + "LogicalFile").value = "";
            dom.byId(this.id + "LogicalFileSearchType").value = "";
            dom.byId(this.id + "FromDate").value = "";
            dom.byId(this.id + "FromTime").value = "";
            dom.byId(this.id + "ToDate").value = "";
            dom.byId(this.id + "LastNDays").value = "";
            dijit.byId(this.id + "threeDays").set("checked",false);
            dijit.byId(this.id + "thisWeek").set("checked",false);
            dijit.byId(this.id + "specify").set("checked",false);
            dijit.byId(this.id + "thisMonth").set("checked",false);
            dijit.byId(this.id + "older").set("checked",false);
            domClass.remove(this.id + "IconFilter", "iconFilter");
            domClass.add(this.id + "IconFilter", "hidden");
            if (!supressGridRefresh) {
                this.refreshGrid();
            }
        },

        getFilter: function () {
            var retVal = {
                Owner: dom.byId(this.id + "Owner").value,
                Jobname: dom.byId(this.id + "Jobname").value,
                Cluster: dom.byId(this.id + "Cluster").value,
                State: dom.byId(this.id + "State").value,
                ECL: dom.byId(this.id + "ECL").value,
                LogicalFile: dom.byId(this.id + "LogicalFile").value,
                LogicalFileSearchType: registry.byId(this.id + "LogicalFileSearchType").get("value"),
                StartDate: this.getISOString("FromDate", "FromTime"),
                EndDate: this.getISOString("ToDate", "ToTime"),
                LastNDays: dom.byId(this.id + "LastNDays").value
            };
            if (retVal.StartDate != "" && retVal.EndDate != "") {
                retVal["DateRB"] = "0";
            } else if (retVal.LastNDays != "") {
                retVal["DateRB"] = "0";
                var now = new Date();
                retVal.StartDate = date.add(now, "day", dom.byId(this.id + "LastNDays").value * -1).toISOString();
                retVal.EndDate = now.toISOString();
            }
            return retVal;
        },

        getISOString: function (dateField, timeField) {
            var d = registry.byId(this.id + dateField).attr("value");
            var t = registry.byId(this.id + timeField).attr("value");
            if (d) {
                if (t) {
                    d.setHours(t.getHours());
                    d.setMinutes(t.getMinutes());
                    d.setSeconds(t.getSeconds());
                }
                return d.toISOString();
            }
            return "";
        },

        //  Implementation  ---
        init: function (params) {
            if (params.Wuid) {
            }
        },

        initWorkunitsGrid: function() {
            var pMenu;
            var context = this;
            
            

            pMenu = new Menu({
                targetNodeIds: [this.id + "WorkunitsGrid"]
            });
            pMenu.addChild(new MenuItem({
                label: "Open",
                onClick: function(){context._onOpen();}
            }));
            pMenu.addChild(new MenuSeparator());
            pMenu.addChild(new MenuItem({
                label: "Delete",
                onClick: function(){context._onDelete();}
            }));
            pMenu.addChild(new MenuSeparator());
            pMenu.addChild(new MenuItem({
                label: "Set To Failed",
                onClick: function(){context._onSetToFailed();}
            }));
            pMenu.addChild(new MenuSeparator());
            pMenu.addChild(new MenuItem({
                label: "Protect",
                id: "isProtected",
                onClick: function(){context._onProtect();}
            }));
            pMenu.addChild(new MenuSeparator());
            pMenu.addChild(new MenuItem({
                label: "Un-Protect",
                id: "isNotProtected",
                onClick: function(){context._onUnprotect();}
            }));
            pMenu.addChild(new MenuSeparator());
            pMenu.addChild(new MenuItem({
                label: "Reschedule",
                onClick: function(){context._onReschedule();}
            }));
            pMenu.addChild(new MenuSeparator());
            pMenu.addChild(new MenuItem({
                label: "Deschedule",
                onClick: function(){context._onDeschedule();}
            }));
            pMenu.addChild(new MenuSeparator());
            var pSubMenu = new Menu();
            //var pSubMenuDate = new Menu();
             /*for(var i = 0;i < today.getDay(); i++){
                pSubMenuDate.addChild(new MenuItem({
                    id: weekday[i],
                    label: weekday[i],
                    onClick: function (args) {
                        /*dijit.byId(context.id + "FromDate").set("value",today);
                        dijit.byId(context.id + "ToDate").set("value", today)
                        context._onClickFilterApply();
                    }
                }));
                pSubMenuDate.addChild(new MenuSeparator());
            }*/
            pSubMenu.addChild(new MenuItem({
                id: "filterClear",
                label: "Clear Filter",
                onClick: function(){context._onFilterClear();}
            }));
            pSubMenu.addChild(new MenuSeparator());
            pSubMenu.addChild(new MenuItem({
                id: "filterOwner",
                onClick: function (args) {
                    context._onFilterClear(null, true);
                    dijit.byId(context.id + "Owner").set("value", dijit.byId("filterOwner").get("hpcc_value"));
                    context._onClickFilterApply();
                }
            }));
            pSubMenu.addChild(new MenuSeparator());
            pSubMenu.addChild(new MenuItem({
                id: "filterJobname",
                onClick: function (args) {
                    context._onFilterClear(null, true);
                    dijit.byId(context.id + "Jobname").set("value", dijit.byId("filterJobname").get("hpcc_value"));
                    context._onClickFilterApply();
                }
            }));
            pSubMenu.addChild(new MenuSeparator());
            pSubMenu.addChild(new MenuItem({
                id: "filterCluster",
                onClick: function (args) {
                    context._onFilterClear(null, true);
                    dijit.byId(context.id + "Cluster").set("value", dijit.byId("filterCluster").get("hpcc_value"));
                    context._onClickFilterApply();
                }
            }));
            pSubMenu.addChild(new MenuSeparator());
            pSubMenu.addChild(new MenuItem({
                id: "filterState",
                onClick: function (args) {
                    context._onFilterClear(null, true);
                    dijit.byId(context.id + "State").set("value", dijit.byId("filterState").get("hpcc_value"));
                    context._onClickFilterApply();
                }
            }));
            pSubMenu.addChild(new MenuSeparator());
            pSubMenu.addChild(new /*Popup*/MenuItem({
                label: "Date",
                //popup: pSubMenuDate,
                onClick: function(args){
                    //dojo.addClass(this.id + "Highlight", "Highlight");
                    dijit.byId(context.id+"FilterDropDown").openDropDown();
                    //dijit.byId(context.id + "FromDate").disabled = 'disabled';
                    //dijit.byId(context.id + "ToDate").set("placeholder",today);
                    //dijit.byId(context.id + "ToDate").disabled = 'disabled';
                    //dijit.byId(context.id + "FromTime").disabled = 'disabled';
                    //dijit.byId(context.id + "ToTime").disabled = 'disabled';

                }
            }));
            
            /*pSubMenuDate.addChild(new MenuItem({
                id: "filterToday",
                label: "Today",
                onClick: function (args) {
                    dijit.byId(context.id + "FromDate").set("value",today);
                    dijit.byId(context.id + "ToDate").set("value", today)
                    context._onClickFilterApply();
                }
            }));*/
            /*pSubMenuDate.addChild(new MenuSeparator());*/
            /*pSubMenuDate.addChild(new MenuItem({
                id: "filterLastWeek",
                label: "This Week",
                onClick: function (args) {
                    dijit.byId(context.id + "FromDate").set("value",filterThisWeek);
                    dijit.byId(context.id + "ToDate").set("value", filterDate)
                    context._onClickFilterApply();
                    //console.log(date.difference(filterDate,filterThisWeekDate,"day"));
                }
            }));*/
            /*pSubMenuDate.addChild(new MenuSeparator());
            pSubMenuDate.addChild(new MenuItem({
                id: "filterTwoWeeks",
                label: "Two Weeks Ago",
                onClick: function (args) {
                    /*dijit.byId(context.id + "FromDate").set("value",filterLastWeek);
                    dijit.byId(context.id + "ToDate").set("value", today);
                    context._onClickFilterApply();
                }
            }));*/
            /*pSubMenuDate.addChild(new MenuSeparator());
            pSubMenuDate.addChild(new MenuItem({
                id: "filterLastMonth",
                label: "Last Month",
                onClick: function (args) {
                    dijit.byId(context.id + "FromDate").set("value",filterThisMonth);
                    dijit.byId(context.id + "ToDate").set("value", today)
                    context._onClickFilterApply();
                }
            }));*/
            /*pSubMenuDate.addChild(new MenuSeparator());
            pSubMenuDate.addChild(new MenuItem({
                id: "filterOlder",
                label: "Older",
                onClick: function (args) {
                    dijit.byId(context.id + "FromDate").set("value",filterStartDate);
                    dijit.byId(context.id + "ToDate").set("value", today);
                    context._onClickFilterApply();
                }
            }));*/
            pMenu.addChild(new PopupMenuItem({
                label: "Filter By:",
                popup: pSubMenu
            }));
            pMenu.startup();

            this.workunitsGrid.setStructure([
                {
                    name: "P",
                    field: "Protected",
                    width: "20px",
                    formatter: function (protected) {
                        if (protected == true){
                            return ("<img src='../files/img/locked.png'>");
                        }
                        return "";
                    }
                },
                { name: "Wuid", field: "Wuid", width: "12" },
                { name: "Owner", field: "Owner", width: "8" },
                { name: "Job Name", field: "Jobname", width: "16" },
                { name: "Cluster", field: "Cluster", width: "8" },
                { name: "Roxie Cluster", field: "RoxieCluster", width: "8" },
                { name: "State", field: "State", width: "8" },
                { name: "Total Thor Time", field: "TotalThorTime", width: "8" }
            ]);

            var store = new WsWorkunits.WUQuery();
            var objStore = new ObjectStore({ objectStore: store });
            this.workunitsGrid.setStore(objStore);
            this.workunitsGrid.setQuery(this.getFilter());
            this.workunitsGrid.noDataMessage = "<span class='dojoxGridNoData'>No results? Please try another filter.</span>";

            var context = this;
            this.workunitsGrid.on("RowDblClick", function (evt) {
                if (context.onRowDblClick) {
                    var idx = evt.rowIndex;
                    var item = this.getItem(idx);
                    var Wuid = this.store.getValue(item, "Wuid");
                    context.onRowDblClick(Wuid);
                }
            }, true);

            this.workunitsGrid.on("RowContextMenu", function (evt){
                if (context.onRowContextMenu) {
                    var idx = evt.rowIndex;
                    var colField = evt.cell.field;
                    var item = this.getItem(idx);
                    var mystring = "item." + colField;
                    context.onRowContextMenu(idx,item,colField,mystring);
                }
            }, true);
            var today = new Date();

            dojo.connect(this.workunitsGrid.selection, 'onSelected', function (idx) {
                context.refreshActionState();
            });
            dojo.connect(this.workunitsGrid.selection, 'onDeselected', function (idx) {
                context.refreshActionState();
            });

            dojo.connect(dijit.byId(context.id + "FromDate"), 'onClick', function(evt){

            });

            dojo.connect(dijit.byId(context.id + "ToDate"), 'onClick', function(evt){
                //TODO var cal_1 = new dojox.widget.Calendar({}, dojo.byId("cal_1"));
            });

            dojo.connect(dijit.byId(context.id + "threeDays"), 'onClick', function(evt){
                var filterThreeDays = date.add(today, "day",- 3);
                dijit.byId(context.id + "FromDate").set("value",filterThreeDays);
                dijit.byId(context.id + "ToDate").set("value",today);
            });

            dojo.connect(dijit.byId(context.id + "thisWeek"), 'onClick', function(evt){
                var getToday = today.getDay();
                var filterThisWeek = date.add(today, "day",- getToday)
                dijit.byId(context.id + "FromDate").set("value",filterThisWeek);
                dijit.byId(context.id + "ToDate").set("value",today);
            });

            /*dojo.connect(dijit.byId(context.id + "twoWeeks"), 'onClick', function(evt){
                var getToday = today.getDay()*2;
                alert(getToday)
                //var filterTwoWeeks = date.add(today, "day",- 14);
                dijit.byId(context.id + "FromDate").set("value",getToday);
                dijit.byId(context.id + "ToDate").set("value",today);
            });*/
            dojo.connect(dijit.byId(context.id + "thisMonth"), 'onClick', function(evt){
                var filterDaysGoneBy = today.getDate();
                var filterThisMonth = date.add(today, "day",- filterDaysGoneBy + 1);
                var filterDaysOfTheWeek = today.getDay();
                dijit.byId(context.id + "FromDate").set("value",filterThisMonth);
                dijit.byId(context.id + "ToDate").set("value",today);
            });
            dojo.connect(dijit.byId(context.id + "older"), 'onClick', function(evt){
                var filterOlderDate = new Date(1970, 0, 1);
                dijit.byId(context.id + "FromDate").set("value",filterOlderDate);
                dijit.byId(context.id + "ToDate").set("value",today);
            });
            dojo.connect(dijit.byId(context.id + "specify"), 'onClick', function(evt){
                dijit.byId(context.id +"FromDate").openDropDown();
                dijit.byId(context.id + "ToDate").set("value",today);
            });
           
            /*
            var filterStartDate = new Date(1970, 0, 1);
            
            var weekday = new Array(7);
            weekday[0]="Sunday";
            weekday[1]="Monday";
            weekday[2]="Tuesday";
            weekday[3]="Wednesday";
            weekday[4]="Thursday";
            weekday[5]="Friday";
            weekday[6]="Saturday";
            var getNameOfDay = weekday[today.getDay()];

            /*var filterDaysGoneBy = today.getDate();
            var filterThisMonth = date.add(today, "day",- filterDaysGoneBy + 1);
            var filterDaysOfTheWeek = today.getDay();

            alert(filterDaysGoneBy);*/

            this.workunitsGrid.startup();
        },

        refreshGrid: function (args) {
            this.workunitsGrid.setQuery(this.getFilter());
            var context = this;
            setTimeout(function () {
                context.refreshActionState()
            }, 200);
        },


        refreshActionState: function () {
            var selection = this.workunitsGrid.selection.getSelected();
            var hasSelection = false;
            var hasProtected = false;
            var hasNotProtected = false;
            var hasFailed = false;
            var hasNotFailed = false;
            for (var i = 0; i < selection.length; ++i) {
                hasSelection = true;
                if (selection[i] && selection[i].Protected && selection[i].Protected != "0") {
                    hasProtected = true;
                    dijit.byId("isProtected").set("disabled", true);
                    dijit.byId("isNotProtected").set("disabled", false);
                } else {
                    hasNotProtected = true;
                    dijit.byId("isProtected").set("disabled", false);
                    dijit.byId("isNotProtected").set("disabled", true);
                }
                if (selection[i] && selection[i].StateID && selection[i].StateID == "4") {
                    hasFailed = true;
                } else {
                    hasNotFailed = true;
                }
            }
            registry.byId(this.id + "Open").set("disabled", !hasSelection);
            registry.byId(this.id + "Delete").set("disabled", !hasNotProtected);
            registry.byId(this.id + "SetToFailed").set("disabled", !hasNotProtected);
            registry.byId(this.id + "Protect").set("disabled", !hasNotProtected);
            registry.byId(this.id + "Unprotect").set("disabled", !hasProtected);
            registry.byId(this.id + "Reschedule").set("disabled", true);    //TODO
            registry.byId(this.id + "Deschedule").set("disabled", true);    //TODO
        },

        ensurePane: function (id, params) {
            var retVal = this.tabMap[id];
            if (!retVal) {
                var context = this;
                retVal = new WUDetailsWidget({
                    id: id,
                    title: id,
                    closable: true,
                    onClose: function () {
                        delete context.tabMap[id];
                        return true;
                    },
                    params: params
                });
                this.tabMap[id] = retVal;
                this.tabContainer.addChild(retVal, 2);
            }
            return retVal;
        },

        onRowDblClick: function (wuid) {
            var wuTab = this.ensurePane(wuid, {
                Wuid: wuid
            });
            this.tabContainer.selectChild(wuTab);
        },

        onRowContextMenu: function (idx,item,colField,mystring) {
            this.workunitsGrid.selection.clear(idx,true);
            this.workunitsGrid.selection.setSelected(idx,true);
            dijit.byId("filterOwner").set("disabled", false);
            dijit.byId("filterJobname").set("disabled", false);
            dijit.byId("filterCluster").set("disabled", false);
            dijit.byId("filterState").set("disabled", false);

            if(item){
                dijit.byId("filterOwner").set("label", "Owner: " + item.Owner);
                dijit.byId("filterOwner").set("hpcc_value", item.Owner);
                dijit.byId("filterJobname").set("label", "Jobname: " + item.Jobname);
                dijit.byId("filterJobname").set("hpcc_value", item.Jobname);
                dijit.byId("filterCluster").set("label", "Cluster: " + item.Cluster);
                dijit.byId("filterCluster").set("hpcc_value", item.Cluster);
                dijit.byId("filterState").set("label", "State: " + item.State);
                dijit.byId("filterState").set("hpcc_value", item.State);
            }

            if(item.Owner == ""){
                 dijit.byId("filterOwner").set("disabled", true);
                 dijit.byId("filterOwner").set("label", "Owner: " + "N/A");
            }
            if(item.Jobname == ""){
                dijit.byId("filterJobname").set("disabled", true);
                dijit.byId("filterJobname").set("label", "Jobname: " + "N/A");
            }
            if(item.Cluster == ""){
                dijit.byId("filterCluster").set("disabled", true);
                dijit.byId("filterCluster").set("label", "Cluster: " + "N/A");
            }
            if(item.State == ""){
                dijit.byId("filterState").set("disabled", true);
                dijit.byId("filterState").set("label", "State: " + "N/A");
            }
        }
    });
});