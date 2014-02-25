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
    "dojo/i18n",
    "dojo/i18n!./nls/common",
    "dojo/i18n!./nls/UserQueryWidget",

    "dijit/registry",

    "dgrid/OnDemandGrid",
    "dgrid/Keyboard",
    "dgrid/Selection",
    "dgrid/editor",
    "dgrid/extensions/ColumnResizer",
    "dgrid/extensions/DijitRegistry",

    "hpcc/GridDetailsWidget",
    "hpcc/WsAccess",
    "hpcc/ESPUtil"

], function (declare, lang, i18n, nlsCommon, nlsSpecific,
                registry,
                OnDemandGrid, Keyboard, Selection, editor, ColumnResizer, DijitRegistry,
                GridDetailsWidget, WsAccess, ESPUtil) {
    return declare("MembersWidget", [GridDetailsWidget], {
        i18n: lang.mixin(nlsCommon, nlsSpecific),

        gridTitle: nlsSpecific.Members,
        idProperty: "__hpcc_id",

        //  Hitched Actions  ---
        _onRefresh: function (args) {
            this.grid.refresh();
        },

        //  Implementation  ---
        init: function (params) {
            if (this.inherited(arguments))
                return;

            this.store = WsAccess.CreateUsersStore(params.groupname);
            this.grid.setStore(this.store);
            this._refreshActionState();
        },

        createGrid: function (domID) {
            var retVal = new declare([OnDemandGrid, Keyboard, ColumnResizer, DijitRegistry, ESPUtil.GridHelper])({
                store: this.store,
                columns: {
                    isMember: editor({
                        label: "",
                        width: 27,
                        editor: "checkbox",
                        autoSave: true
                    }),
                    username: {
                        width: 180,
                        label: this.i18n.Username
                    },
                    fullname: {
                        label: this.i18n.FullName
                    },
                    passwordexpiration: {
                        width: 180,
                        label: this.i18n.PasswordExpiration
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