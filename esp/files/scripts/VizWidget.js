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
    "dojo/_base/array",
    "dojo/_base/Deferred",
    "dojo/dom-construct",
    "dojo/dom-form",
    "dojo/io-query",
    "dojo/promise/all",

    "dijit/registry",
    "dijit/layout/ContentPane",
    "dijit/form/Select",

    "dojox/layout/TableContainer",

    "hpcc/_Widget",
    "hpcc/ESPWorkunit",
    "hpcc/WsWorkunits",

    "dojo/text!../templates/VizWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/ContentPane",
    "dijit/Toolbar",
    "dijit/ToolbarSeparator",
    "dijit/TooltipDialog",
    "dijit/form/Form",
    "dijit/form/Button",
    "dijit/form/DropDownButton"

], function (declare, lang, arrayUtil, Deferred, domConstruct, domForm, ioQuery, all,
                registry, ContentPane, Select,
                TableContainer,
                _Widget, ESPWorkunit, WsWorkunits,
                template) {
    return declare("VizWidget", [_Widget], {
        templateString: template,

        borderContainer: null,
        grid: null,

        foundMatchingViz: false,
        foundMatchingFields: false,

        loaded: false,

        buildRendering: function (args) {
            this.inherited(arguments);
        },

        postCreate: function (args) {
            this.inherited(arguments);
            this.borderContainer = registry.byId(this.id + "BorderContainer");
            this.vizSelect = registry.byId(this.id + "VizSelect");
            this.mappingDropDown = registry.byId(this.id + "Mappings");
            this.mappingItems = registry.byId(this.id + "MappingItems");
        },

        startup: function (args) {
            this.inherited(arguments);
        },

        resize: function (args) {
            this.inherited(arguments);
            this.borderContainer.resize();
            if (this.d3Viz) {
                this.d3Viz.resize();
            }
        },

        layout: function (args) {
            this.inherited(arguments);
        },

        destroy: function (args) {
            this.inherited(arguments);
        },

        _onRefresh: function (evt) {
            this.refreshData();
        },

        _onRefreshData: function (evt) {
            this.refreshData();
        },

        _onVizSelect: function (value) {
            this.vizOnChange(value);
        },

        _onMappingsApply: function (evt) {
            this.refreshData();
            this.mappingDropDown.closeDropDown();
        },

        //  Implementation  ---
        onErrorClick: function (line, col) {
        },

        reset: function () {
            this.initalized = false;
            this.params = null;
            this.wu = null;
            this.vizType = null;
            this.vizSelect.set("options", []);
        },

        init: function (params) {
            if (this.inherited(arguments))
                return;

            this.loading = true;

            var context = this;
            WsWorkunits.GetVisualisations().then(function (vizResponse) {
                context.vizSelect.set("options", vizResponse);
                if (params.viz) {
                    context.vizSelect.set("value", params.viz);
                } else {
                    context.vizSelect.set("value", vizResponse[0].value);
                }
                if (params.mapping) {
                    context.defaultSelection = ioQuery.queryToObject(params.mapping);
                }
                if (params.Wuid) {
                    context.wu = ESPWorkunit.Get(params.Wuid);
                    context.wu.fetchResults(function (response) {
                        var newSel = null;
                        arrayUtil.forEach(response, function(item, idx) {
                            arrayUtil.forEach(vizResponse, function(vizItem, idx) {
                                if (vizItem.value.indexOf(item.Name) >= 0) {
                                    newSel = vizItem.value;
                                    return true;
                                }
                            });
                            if (newSel) {
                                return true;
                            }
                        });
                        if (newSel) {
                            context.foundMatchingViz = true;
                            context.vizSelect.set("value", newSel);
                        }
                        context.doFetchAllStructures().then(function (response) {
                            context.loading = false;
                            context.vizOnChange(context.vizSelect.get("value"), params.mapping);
                        });
                    });
                }
            });
        },

        doFetchStructure: function (result) {
            var deferred = new Deferred();
            result.fetchStructure(function (response) {
                deferred.resolve(response);
            });
            return deferred.promise;
        },

        doFetchAllStructures: function () {
            var promiseArray = [];
            var context = this;
            this.resultStructures = {};
            arrayUtil.forEach(this.wu.results, function (item, idx) {
                promiseArray.push(context.doFetchStructure(item).then(function (response) {
                    context.resultStructures[item.Sequence] = response;
                    return response;
                }));
            });
            return all(promiseArray);
        },

        getResultOptions: function() {
            var retVal = [];
            arrayUtil.forEach(this.wu.results, function (item, idx) {
                retVal.push({
                    label: item.Name,
                    value: item.Sequence
                });
            });
            return retVal;
        },

        getFieldOptions: function (sequence) {
            var retVal = [];
            arrayUtil.forEach(this.resultStructures[sequence], function (item, idx) {
                if (item.field.indexOf("_") !== 0) {
                    retVal.push({
                        label: item.field,
                        value: item.field
                    });
                }
            });
            return retVal;
        },

        vizOnChange: function (value, autoShow) {
            if (this.loading)
                return;

            var context = this;
            return this.refreshVizType(value).then(function (vizWidget) {
                context.refreshMappings();
                if (autoShow || (context.foundMatchingViz && context.foundMatchingFields)) {
                    setTimeout(function () {
                        context.refreshData();
                    }, 1);
                } else {
                    var isVisible = document.getElementById(context.id).offsetHeight != 0;
                    if (isVisible) {
                        context.mappingDropDown.focus();
                        context.mappingDropDown.loadAndOpenDropDown();
                    }
                }
            });
        },

        datasetOnChange: function (select, sequence) {
            if (this.loading)
                return;

            if (sequence != null) {
                select.datasetMapping.sequence = sequence;
                select.datasetMapping.result = this.wu.results[sequence];
                select.datasetMapping.data = null;

                this.foundMatchingFields = false;
                var foundMatchingFieldCount = 0;
                var fieldMappings = select.datasetMapping.getFieldMappings();
                var context = this;
                arrayUtil.forEach(fieldMappings, function (fieldMapping, idx) {
                    var options = context.getFieldOptions(sequence);
                    fieldMapping.select.set("options", options);

                    //  Auto select field priority:
                    //  1.  "defaultSelection" (typically from URL mapping)
                    //  2.  Match by field name
                    //  3.  Match by field index
                    //  4.  Just use option[0]
                    var value = options[idx < options.length ? idx : 0].value;  //  If no default value or matching name, revert to field order and failing that use options[0]
                    if (lang.exists("defaultSelection." + fieldMapping.select.name, context)) {
                        value = context.defaultSelection[fieldMapping.select.name];
                    } else {
                        arrayUtil.forEach(fieldMapping.select.options, function (optionItem, idx) {
                            if (optionItem.label === fieldMapping.getID()) {
                                ++foundMatchingFieldCount;
                                value = optionItem.value;
                                return true;
                            }
                        });
                    }
                    fieldMapping.select.set("value", value);
                });
                if (foundMatchingFieldCount === fieldMappings.length) {
                    this.foundMatchingFields = true;
                }
            }
        },

        fieldOnChange: function (select, value) {
            if (this.loading)
                return;

            this.d3Viz.setFieldMapping(select.fieldMapping.getID(), value, select.datasetMapping.getID());
        },

        refreshMappings: function () {
            this.datasetMappings = this.d3Viz.cloneDatasetMappings();

            var placeHolder = registry.byId(this.id + "MappingItems");
            if (this.mappingWidget) {
                this.mappingWidget.destroyRecursive();
            }
            this.mappingWidget = new ContentPane({
            });
            var context = this;
            arrayUtil.forEach(this.datasetMappings, function (datasetMapping, idx) {
                var tableContainer = new TableContainer({
                    cols: 1,
                    customClass: "labelsAndValues",
                    "labelWidth": "120"
                });

                datasetMapping.select = new Select({
                    id: context.id + datasetMapping.getID(),
                    label: datasetMapping.getDisplay(),
                    name: datasetMapping.getID(),
                    datasetMapping: datasetMapping,
                    options: context.getResultOptions(),
                    onChange: function (value) {
                        context.datasetOnChange(this, value);
                    }
                });
                tableContainer.addChild(datasetMapping.select);

                arrayUtil.forEach(datasetMapping.getFieldMappings(), function (fieldMapping, idx) {
                    fieldMapping.select = new Select({
                        id: context.id + datasetMapping.getID() + ":" + fieldMapping.getID(),
                        datasetMapping: datasetMapping,
                        fieldMapping: fieldMapping,
                        label: "&nbsp;&nbsp;&nbsp;&nbsp;" + fieldMapping.getDisplay(),
                        name: datasetMapping.getID() + ":" + fieldMapping.getID(),
                        options: [],
                        onChange: function (value) {
                            context.fieldOnChange(this, value);
                        }
                    });
                    tableContainer.addChild(fieldMapping.select);
                });
                context.mappingWidget.addChild(tableContainer);

                //  "sequence" auto select priority:
                //  1.  "defaultSelection" (typically from URL mapping)
                //  2.  Match by field name
                //  3.  Just use sequence = 0
                var sequence = 0;
                if (lang.exists("defaultSelection." + datasetMapping.select.name, context)) {
                    sequence = context.defaultSelection[datasetMapping.select.name];
                } else  {
                    arrayUtil.forEach(datasetMapping.select.options, function (optionItem, idx) {
                        if (optionItem.label === datasetMapping.getID()) {
                            sequence = optionItem.value;
                            return true;
                        }
                    });
                }
                if (datasetMapping.select.get("value") != sequence) {
                    datasetMapping.select.set("value", sequence);
                } else {
                    context.datasetOnChange(datasetMapping.select, sequence);
                }
            });
            placeHolder.addChild(this.mappingWidget);
        },

        vizType: "",
        refreshVizType: function (value) {
            var deferred = new Deferred();
            if (this.vizType !== value) {
                this.vizType = value;
                var context = this;
                require(["viz/" + this.vizType], function (D3Viz, d3) {
                    context.d3Viz = new D3Viz();
                    domConstruct.empty(context.id + "VizCP");
                    context.d3Viz.renderTo({
                        domNodeID: context.id + "VizCP"
                    });
                    deferred.resolve(context.vizType);
                });
            } else {
                deferred.resolve(this.vizType);
            }
            return deferred.promise;
        },

        getFilter: function() {
            var filter = domForm.toObject(this.id + "FilterDialog");
            var retVal = {};
            for (var key in filter) {
                if (filter[key]) {
                    retVal[key] = filter[key];
                }
            }
            return retVal;
        },

        refreshData: function () {
            if (this.d3Viz) {
                var context = this;
                var allArray = [];
                arrayUtil.forEach(this.datasetMappings, function (datasetMapping, idx) {
                    allArray.push(datasetMapping.result.fetchContent().then(function (response) {
                        context.d3Viz.setData(response, datasetMapping.getID());
                        return response;
                    }));
                });
                all(allArray).then(function (response) {
                    context.params.viz = context.vizSelect.get("value");
                    context.params.mapping = domForm.toQuery(context.id + "MappingForm");
                    context.d3Viz.display();
                });
            }
        }
    });
});
