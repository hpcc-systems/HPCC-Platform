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
    "dojo/i18n!./nls/hpcc",
    "dojo/_base/array",
    "dojo/on",
    "dojo/store/util/QueryResults",

    "dgrid/OnDemandGrid",
    "dgrid/Keyboard",
    "dgrid/Selection",
    "dgrid/tree",
    "dgrid/selector",
    "dgrid/extensions/ColumnResizer",
    "dgrid/extensions/DijitRegistry",

    "hpcc/GridDetailsWidget",
    "hpcc/SFDetailsWidget",
    "hpcc/ESPUtil",
    "hpcc/ESPQuery"
], function (declare, lang, i18n, nlsHPCC, arrayUtil, on, QueryResults,
                OnDemandGrid, Keyboard, Selection, tree, selector, ColumnResizer, DijitRegistry,
                GridDetailsWidget, SFDetailsWidget, ESPUtil, ESPQuery) {
    return declare("QuerySetSuperFilesWidget", [GridDetailsWidget], {
        i18n: nlsHPCC,
        query: null,

        gridTitle: nlsHPCC.title_QuerySetSuperFiles,
        idProperty: "__hpcc_id",

        init: function (params) {
            if (this.inherited(arguments))
                return;
            this.query = ESPQuery.Get(params.QuerySet, params.QueryId);
            this.refreshGrid();
            this.store.getChildren = function(parent, options){
                var children = [];
                arrayUtil.forEach(parent.SubFiles.File, function(item, idx) {
                    children.push({
                        __hpcc_id: item,
                        __hpcc_display: item
                    });
                });
                return QueryResults(children);
            }
            this.store.mayHaveChildren = function (object) {
              return object.__hpcc_type;
            };
        },

        createGrid: function (domID) {
            var context = this;
            var retVal = new declare([OnDemandGrid, Keyboard, Selection, ColumnResizer, DijitRegistry, ESPUtil.GridHelper])({
                allowSelectAll: true,
                deselectOnRefresh: false,
                store: this.store,
                columns: {
                    col1: selector({
                        width: 27,
                        selectorType: 'checkbox',
                    }),
                    __hpcc_display: tree({
                        label: this.i18n.SuperFiles,
                        collapseOnRefresh: true,
                        sortable: false
                    })
                }
            }, domID);
            return retVal;
        },

        createDetail: function (id, row, params) {
            if (row.Name) {
                return new SFDetailsWidget.fixCircularDependency({
                    id: id,
                    title: row.Name,
                    closable: true,
                    hpcc: {
                        params: {
                            Name: row.Name
                        }
                    }
                });
            }
            if (row.__hpcc_id) {
                return new SFDetailsWidget.fixCircularDependency({
                    id: id,
                    title: row.__hpcc_id,
                    closable: true,
                    hpcc: {
                        params: {
                            Name: row.__hpcc_id
                        }
                    }
                });
            }
            if (params.Name) {
                return new SFDetailsWidget.fixCircularDependency({
                    id: id,
                    title: params.Name,
                    closable: true,
                    hpcc: {
                        params: {
                            Name: params.Name
                        }
                    }
                });
            }
        },

        refreshGrid: function (args) {
            if (this.query) {
                var superfiles = [];
                if (lang.exists("SuperFiles.SuperFile", this.query)) {
                    arrayUtil.forEach(this.query.SuperFiles.SuperFile, function (item, idx) {
                        superfiles.push(lang.mixin({
                            __hpcc_id: item.Name,
                            __hpcc_display:  item.Name,
                            __hpcc_type: item.Name
                        }, item));
                    });
                }
            this.store.setData(superfiles);
            this.grid.refresh();
            }
        }
    });
});