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

    "dgrid/tree",
    "dgrid/selector",

    "hpcc/GridDetailsWidget",
    "hpcc/DelayLoadWidget",
    "hpcc/ESPUtil",
    "hpcc/ESPQuery"
], function (declare, lang, i18n, nlsHPCC, arrayUtil, on, QueryResults,
                tree, selector,
                GridDetailsWidget, DelayLoadWidget, ESPUtil, ESPQuery) {
    return declare("QuerySetSuperFilesWidget", [GridDetailsWidget], {
        i18n: nlsHPCC,
        query: null,

        gridTitle: nlsHPCC.title_QuerySetSuperFiles,
        idProperty: "__hpcc_id",

        init: function (params) {
            if (this.inherited(arguments))
                return;
            this.query = ESPQuery.Get(params.QuerySetId, params.Id);
            this.refreshGrid();
        },

        createGrid: function (domID) {
            var context = this;
            this.store.getChildren = function (parent, options) {
                var children = [];
                arrayUtil.forEach(parent.SubFiles.File, function (item, idx) {
                    children.push({
                        __hpcc_id: item,
                        __hpcc_display: item,
                        __hpcc_type: "LF"
                    });
                });
                return QueryResults(children);
            }
            this.store.mayHaveChildren = function (object) {
                return object.__hpcc_type;
            };
            var retVal = new declare([ESPUtil.Grid(false, true)])({
                store: this.store,
                columns: {
                    col1: selector({
                        width: 27,
                        selectorType: 'checkbox'
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
            switch (row.__hpcc_type) {
                case "SF": {
                    return new DelayLoadWidget({
                        id: id,
                        title: row.__hpcc_id,
                        closable: true,
                        delayWidget: "SFDetailsWidget",
                        hpcc: {
                            type: "SFDetailsWidget",
                            params: {
                                Name: row.__hpcc_id
                            }
                        }
                    });
                }
                case "LF": {
                    return new SFDetailsWidget.fixCircularDependency({
                        id: id,
                        title: row.__hpcc_id,
                        closable: true,
                        delayWidget: "LFDetailsWidget",
                        hpcc: {
                            type: "LFDetailsWidget",
                            params: {
                                Name: row.__hpcc_id
                            }
                        }
                    });
                }
            }
            return null;
        },

        refreshGrid: function (args) {
            var context = this;
            this.query.refresh().then(function (response) {
                var superfiles = [];
                if (lang.exists("SuperFiles.SuperFile", context.query)) {
                    arrayUtil.forEach(context.query.SuperFiles.SuperFile, function (item, idx) {
                        superfiles.push(lang.mixin({
                            __hpcc_id: item.Name,
                            __hpcc_display: item.Name,
                            __hpcc_type: "SF"
                        }, item));
                    });
                }
                context.store.setData(superfiles);
                context.grid.refresh();
            });
        }
    });
});