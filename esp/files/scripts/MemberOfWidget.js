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
define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/dom",
    "dojo/dom-form",
    "dojo/request/iframe",
    "dojo/_base/array",

    "dijit/layout/_LayoutWidget",
    "dijit/_TemplatedMixin",
    "dijit/_WidgetsInTemplateMixin",
    "dijit/registry",
    "dijit/Menu",
    "dijit/MenuItem",
    "dijit/MenuSeparator",

    "dgrid/OnDemandGrid",
    "dgrid/Keyboard",
    "dgrid/Selection",
    "dgrid/selector",
    "dgrid/extensions/ColumnResizer",
    "dgrid/extensions/DijitRegistry",
    "dgrid/extensions/Pagination",

    "hpcc/WsAccess",
    "hpcc/ESPUtil",

    "dojo/text!../templates/MemberOfWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/Toolbar",
    "dijit/form/Button",
    "dijit/ToolbarSeparator",
    "dijit/form/TextBox",
    "dijit/Dialog"
], function (declare, lang, dom, domForm, iframe, arrayUtil,
                _LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin, registry, Menu, MenuItem, MenuSeparator,
                OnDemandGrid, Keyboard, Selection, selector, ColumnResizer, DijitRegistry, Pagination,
                WsAccess, ESPUtil,
                template) {
    return declare("MemberOfWidget", [_LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
        templateString: template,
        baseClass: "MemberOfWidget",

        borderContainer: null,
        memberOfGrid: null,

        initalized: false,
        loaded: false,

        user:null,

         buildRendering: function (args) {
            this.inherited(arguments);
        },

        postCreate: function (args) {
            this.inherited(arguments);
            this.borderContainer = registry.byId(this.id + "BorderContainer");
            this.memberOfTab = registry.byId(this.id + "_MemberOf");
        },

        startup: function (args) {
            this.inherited(arguments);
            this.initContextMenu();
        },

        resize: function (args) {
            this.inherited(arguments);
            this.borderContainer.resize(); //is needed
        },


        init: function (params) {
            this.initMemberOfGrid();
            this.refreshGrid();
            this.refreshActionState();

            if (this.initalized)
                return;
            this.initalized = true;
            
            if (params.username) {
                this.user = params.username;
                this.memberOfGrid.set("query", {
                    username: this.user
                 });
            }
        },

        getTitle: function () {
            return "Member Of";
        },

        _onRefresh:function(){
            this.refreshGrid();
        },

        initContextMenu: function() {
            var context = this;
            var pMenu = new Menu({
                targetNodeIds: [this.id + "MemberOfGrid"]
            });
            pMenu.addChild(new MenuItem({
                label: "Open",
                onClick: function(args){context._onOpen();}
            }));
            pMenu.addChild(new MenuSeparator());
            pMenu.addChild(new MenuItem({
                label: "Add",
                onClick: function(args){context._onAdd();}
            }));
            pMenu.addChild(new MenuItem({
                label: "Delete",
                onClick: function(args){context._onDelete();}
            }));
        },

        initMemberOfGrid: function () {
            var context = this;
            var store = new WsAccess.CreateUserMemberOfStore();
            this.memberOfGrid = declare([OnDemandGrid, Keyboard, Selection, ColumnResizer, DijitRegistry, ESPUtil.GridHelper])({
                allowSelectAll: false,
                deselectOnRefresh: false,
                store: store,
                columns: {
                  check: selector({
                        width: 1, 
                        label: " "
                    },"checkbox"),
                    name: {
                        width: 27,
                        label: "Group Name"
                    }
                },
            },

            this.id + "MemberOfGrid");
            this.memberOfGrid.set("noDataMessage", "<span class='dojoxGridNoData'>Zero Workunits (check filter).</span>");
            this.memberOfGrid.onSelectionChanged(function (event) {
                context.refreshActionState();
            });
            this.memberOfGrid.onContentChanged(function (event) {
                context.refreshActionState();
            });
            this.memberOfGrid.startup();
            this.refreshActionState();
        },

        //  Hitched actions  ---

        /*_onOpen: function (event) {
            var selections = this.usersGrid.getSelected();
            var firstTab = null;
            for (var i = selections.length - 1; i >= 0; --i) {
                var tab = this.ensurePane(this.id + "_" + selections[i].username, {
                    Username: selections[i].username,
                    Fullname: selections[i].fullname,
                    Passwordexpiration: selections[i].passwordexpiration
                });
                if (i == 0) {
                    firstTab = tab;
                }
            }
            if (firstTab) {
                this.selectChild(firstTab);
            }
        },*/

        _onAdd: function (event) {
                var context = this;
                var selection = this.memberOfGrid.getSelected();
                WsWorkunits.WUAction(selection, "Deschedule", {
                    load: function (response) {
                        context.refreshGrid(response);
                    }
                });
        },

        _onDelete: function (params){
            var selections = this.memberOfGrid.getSelected();
            if (confirm('Delete this user from selected group?')) {
                var context = this;
                for (var i = selections.length - 1; i >= 0; --i) {
                    WsAccess.UserGroupEdit({
                        request:{
                            action: "delete",
                            groupnames: selections[i].name,
                            username: this.user
                        }
                    }).then(function (response) {
                    if(lang.exists("UserGroupEditResponse.retmsg", response)){
                        dojo.publish("hpcc/brToaster", {
                            message: "<p>" + response.UserGroupEditResponse.retmsg + "</p>",
                            type: "error",
                            duration: -1
                        });
                    }
                    });
                }
            }
            setTimeout(this.refreshGrid(), 2000);
        },


        refreshActionState: function () {
            var selection = this.memberOfGrid.getSelected();
            var hasSelection = selection.length;
            registry.byId(this.id + "Delete").set("disabled", !hasSelection);
        },

        updateInput: function (name, oldValue, newValue) {
            var registryNode = registry.byId(this.id + name);
            if (registryNode) {
                registryNode.set("value", newValue);
            } else {
                var domElem = dom.byId(this.id + name);
                if (domElem) {
                    switch (domElem.tagName) {
                        case "SPAN":
                        case "DIV":
                            domAttr.set(this.id + name, "innerHTML", newValue);
                            break;
                        case "INPUT":
                        case "TEXTAREA":
                            domAttr.set(this.id + name, "value", newValue);
                            break;
                        default:
                            alert(domElem.tagName);
                    }
                }
            }
        },

         refreshGrid: function (args) {
            this.memberOfGrid.set("query",{
               username: this.user
            });
        }
    });
});