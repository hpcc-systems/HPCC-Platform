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
define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/_base/array",
    "dojo/promise/all",

    "dijit/registry",
    "dijit/form/Button",
    "dijit/ToolbarSeparator",
    "dijit/Dialog",

    "dgrid/selector",

    "hpcc/GridDetailsWidget",
    "hpcc/ws_access",
    "hpcc/ESPUtil",
    "hpcc/TargetSelectWidget"

], function (declare, lang, i18n, nlsHPCC, arrayUtil, all,
                registry, Button, ToolbarSeparator, Dialog,
                selector,
                GridDetailsWidget, WsAccess, ESPUtil, TargetSelectWidget) {
    return declare("MembersWidget", [GridDetailsWidget], {
        i18n: nlsHPCC,

        gridTitle: nlsHPCC.title_Members,
        idProperty: "username",

        //  Implementation  ---
        init: function (params) {
            if (this.inherited(arguments))
                return;

            this.refreshGrid();
            this.memberDropDown = new TargetSelectWidget({});
            this.memberDropDown.init({
                loadUsersNotInGroup: true,
                groupname: params.groupname
            });
            this.dialog.addChild(this.memberDropDown);
        },

        createGrid: function (domID) {
            var context = this;
            this.openButton = registry.byId(this.id + "Open");
            this.refreshButton = registry.byId(this.id + "Refresh");
            this.addButton = new Button({
                id: this.id + "Add",
                label: this.i18n.Add,
                onClick: function (event) {
                    context._onAddMember(event);
                }
            }).placeAt(this.openButton.domNode, "after");
            this.deleteButton = new Button({
                id: this.id + "Delete",
                label: this.i18n.Delete,
                disabled: true,
                onClick: function (event) {
                    context._onDeleteMember(event);
                }
            }).placeAt(this.addButton.domNode, "after");
            var tmpSplitter = new ToolbarSeparator().placeAt(this.addButton.domNode, "before");

            this.dialog = new Dialog({
                title: this.i18n.PleaseSelectAUserToAdd,
                style: "width: 300px;"
            });
            this.dialogBtn = new Button({
                label: this.i18n.Add,
                style: "float:right;",
                onClick: function (event) {
                    context._onSubmitAddMember(event);
                }
            }).placeAt(this.dialog.domNode);
            
            var retVal = new declare([ESPUtil.Grid(true, true)])({
                store: this.store,
                columns: {
                    check: selector({
                        width: 27,
                        label: " "
                    }, "checkbox"),
                    username: {
                        label: this.i18n.Username
                    }
                }
            }, domID);

             retVal.onSelectionChanged(function (event) {
                context.refreshActionState();
            });

            return retVal;
        },

        _onAddMember: function (event) {
            this.dialog.show();
        },

        _onSubmitAddMember: function (event) {
            var context = this;
            WsAccess.GroupMemberEdit({
                request: {
                    groupname:context.params.groupname,
                    usernames_i7:context.memberDropDown.get("value"),
                    action: "Add"
                }
            }).then(function (response) {
               context.dialog.hide();
               context._onRefresh();
            });
        },

        _onDeleteMember: function (event) {
            var context = this;
            var selections = this.grid.getSelected();
            if (confirm(this.i18n.YouAreAboutToRemoveUserFrom)) {
                var promises = [];
                arrayUtil.forEach(selections, function (row, idx) {
                    promises.push(WsAccess.GroupMemberEdit({
                        request: {
                            groupname: context.params.groupname,
                            usernames_i1:row.username,
                            action: "Delete"
                        }
                    }));
                });
                all(promises).then(function() {
                    context._onRefresh();
                });
            }
        },

        _onRefresh: function () {
            this.refreshGrid();
        },

        refreshActionState: function (selection) {
            registry.byId(this.id + "Open").set("disabled", true);
            var selection = this.grid.getSelected();
            var hasUserSelection = selection.length;

            this.deleteButton.set("disabled", !hasUserSelection);
        },

        refreshGrid: function () {
            var context = this;
            WsAccess.Members({
                request: {
                    groupname: context.params.groupname
                }
            }).then(function (response) {
                var results = [];

                if (lang.exists("GroupEditResponse.Users.User", response)) {
                    results = response.GroupEditResponse.Users.User;
                    arrayUtil.forEach(results, function (row, idx) {
                        lang.mixin(row, {
                            username: row.username
                        });
                    });
                }

                context.store.setData(results);
                context.grid.set("query", {});
            });
        }
    });
});