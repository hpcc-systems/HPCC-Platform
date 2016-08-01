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

    "dijit/registry",

    "dgrid/editor",

    "hpcc/GridDetailsWidget",
    "hpcc/ws_access",
    "hpcc/ws_account",
    "hpcc/ESPUtil"

], function (declare, lang, i18n, nlsHPCC,
                registry,
                editor,
                GridDetailsWidget, WsAccess, WsAccount, ESPUtil) {
    return declare("MemberOfWidget", [GridDetailsWidget], {
        i18n: nlsHPCC,

        gridTitle: nlsHPCC.title_MemberOf,
        idProperty: "__hpcc_id",
        currentUser: null,

        //  Hitched Actions  ---
        _onRefresh: function (args) {
            this.grid.refresh();
        },

        //  Implementation  ---
        init: function (params) {
            var context = this;
            if (this.inherited(arguments))
                return;

            WsAccount.MyAccount({
            }).then(function (response) {
                if (lang.exists("MyAccountResponse.username", response)) {
                    context.currentUser = response.MyAccountResponse.username;
                }
            });

            this.store = WsAccess.CreateGroupsStore(params.username, false);
            this.grid.setStore(this.store);
            this.grid.on("dgrid-datachange", function(event){
                if (dojoConfig.isAdmin && params.username === context.currentUser && event.oldValue === true) {
                    var msg = confirm(context.i18n.RemoveUser + " " + event.rowId + ". " + context.i18n.ConfirmRemoval);
                    if (msg) {
                        location.hash = "";
                        location.reload();
                    } else {
                        event.preventDefault();
                    }
                }
            });
            this._refreshActionState();
        },

        createGrid: function (domID) {
            var retVal = new declare([ESPUtil.Grid(false, false)])({
                sort: [{ attribute: "name" }],
                store: this.store,
                columns: {
                    isMember: editor({
                        label: "",
                        width: 27,
                        editor: "checkbox",
                        autoSave: true
                    }),
                    name: {
                        label: this.i18n.GroupName,
                        sortable: true
                    }
                }
            }, domID);

            return retVal;
        },

        refreshActionState: function (selection) {
            registry.byId(this.id + "Open").set("disabled", true);
        }
    });
});