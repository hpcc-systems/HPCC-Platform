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
    "dojo/store/Memory",
    "dojo/store/Observable",

    "dijit/registry",

    "dgrid/OnDemandGrid",
    "dgrid/Keyboard",
    "dgrid/Selection",
    "dgrid/editor",
    "dgrid/selector",
    "dgrid/extensions/ColumnResizer",
    "dgrid/extensions/DijitRegistry"

], function (declare, lang, Memory, Observable,
        registry,
        OnDemandGrid, Keyboard, Selection, editor, selector, ColumnResizer, DijitRegistry) {
    return declare("SelectionGridWidget", [OnDemandGrid, Keyboard, Selection, ColumnResizer, DijitRegistry], {
        store: null,
        idProperty: "Change Me",

        constructor: function (args) {
            this.inherited(arguments);
            this.idProperty = args.idProperty;
            var store = new Memory({
                idProperty: this.idProperty,
                data: []
            });
            this.store = Observable(store);
            lang.mixin(args, {
                store: this.store
            });
        },

        postCreate: function (args) {
            this.inherited(arguments);
        },

        startup: function (args) {
            this.inherited(arguments);
        },

        //  Implementation ---
        setData: function (data) {
            this.store.setData(data);
            this.refresh();
        }
    });
});
