define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "src/nlsHPCC",
    "dojo/_base/array",
    "dojo/_base/Deferred",
    "dojo/dom-construct",
    "dojo/dom-form",
    "dojo/io-query",
    "dojo/promise/all",

    "dijit/registry",
    "dijit/form/Select",

    "dgrid/editor",

    "@hpcc-js/common",

    "hpcc/_Widget",
    "src/ESPWorkunit",
    "src/WsWorkunits",
    "src/Utility",

    "dojo/text!../templates/VizWidget.html",

    "hpcc/TableContainer",
    "hpcc/SelectionGridWidget",
    "dijit/layout/BorderContainer",
    "dijit/layout/ContentPane",
    "dijit/Toolbar",
    "dijit/ToolbarSeparator",
    "dijit/TooltipDialog",
    "dijit/form/Form",
    "dijit/form/Button",
    "dijit/form/ToggleButton",
    "dijit/form/NumberSpinner",
    "dijit/form/DropDownButton",
    "dijit/Fieldset"

], function (declare, lang, nlsHPCCMod, arrayUtil, Deferred, domConstruct, domForm, ioQuery, all,
    registry, Select,
    editor,
    hpccCommon,
    _Widget, ESPWorkunit, WsWorkunits, Utility,
    template) {

    var nlsHPCC = nlsHPCCMod.default;
    return declare("VizWidget", [_Widget], {
        templateString: template,
        i18n: nlsHPCC,

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
            this.limit = registry.byId(this.id + "Limit");
            this.aggregateMode = registry.byId(this.id + "AggregateMode");
            this.vizSelect = registry.byId(this.id + "VizSelect");

            this.vizSelect = registry.byId(this.id + "VizSelect");
            this.mappingDropDown = registry.byId(this.id + "Mappings");
            this.mappingForm = registry.byId(this.id + "MappingForm");
            this.mappingLabel = registry.byId(this.id + "MappingLabel");
            this.mappingValues = registry.byId(this.id + "MappingValues");
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
            this.vizOnChange(value, true);
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

            this.rows = [];

            this.loading = true;

            var context = this;
            if (params.limit) {
                this.limit.set("value", params.limit);
            }
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
                        arrayUtil.forEach(response, function (item, idx) {
                            arrayUtil.forEach(vizResponse, function (vizItem, idx) {
                                if (vizItem.label.split(" ").join("").indexOf(item.Name) >= 0) {
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
                            context.vizOnChange(context.vizSelect.get("value"), true);
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

        getResultOptions: function () {
            var retVal = [];
            arrayUtil.forEach(this.wu.results, function (item, idx) {
                retVal.push({
                    label: item.Name,
                    value: item.Sequence
                });
            });
            return retVal;
        },

        getFieldOptions: function (sequence, optional) {
            var retVal = optional ? [{ label: "&nbsp;", value: "" }] : [];
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

        getFieldValue: function (options, id, defIdx) {
            defIdx = defIdx || 0;
            var retVal = options[defIdx].value;
            if (lang.exists("defaultSelection." + id, this)) {
                retVal = this.defaultSelection[id];
            } else {
                arrayUtil.forEach(options, function (optionItem, idx) {
                    if (optionItem.label === id) {
                        retVal = optionItem.value;
                        return true;
                    }
                });
            }
            return retVal;
        },

        getFieldAggregation: function (id) {
            if (lang.exists("defaultSelection." + id + "_aggr", this)) {
                return this.defaultSelection[id + "_aggr"];
            }
            return "mean";
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
                    var isVisible = document.getElementById(context.id).offsetHeight !== 0;
                    if (isVisible) {
                        context.mappingDropDown.focus();
                        context.mappingDropDown.loadAndOpenDropDown();
                    }
                }
            });
        },

        refreshMappings: function () {
            this.datasetMappings = this.d3Viz.cloneDatasetMappings();

            var context = this;
            arrayUtil.forEach(this.datasetMappings, function (datasetMapping, idx) {
                context.datasetOnChange(datasetMapping.getFieldMappings(), context.params.Sequence);
            });
        },

        datasetOnChange: function (fieldMappings, sequence) {
            if (this.loading)
                return;

            if (sequence != null) {
                var result = this.wu.results[sequence];
                var data = null;

                this.foundMatchingFields = false;
                var foundMatchingFieldCount = 0;
                var options = this.getFieldOptions(sequence);
                this.mappingLabel.set("options", options);
                var value = this.getFieldValue(options, "label");
                this.mappingLabel.set("value", value);

                var options2 = this.getFieldOptions(sequence, true);
                if (!this.mappingValues.grid) {
                    this.mappingValues.createGrid({
                        idProperty: "id",
                        columns: {
                            field: editor({
                                label: "Field",
                                autoSave: true,
                                editorArgs: {
                                    style: "width:75px;",
                                    options: options2
                                }
                            }, Select),
                            aggregation: editor({
                                label: "Value",
                                autoSave: true,
                                editorArgs: {
                                    style: "width:75px;",
                                    options: [
                                        { value: "", label: "&nbsp;" },
                                        { value: "mean", label: "Mean" },
                                        { value: "sum", label: "Sum" },
                                        { value: "max", label: "Max" },
                                        { value: "min", label: "Min" },
                                        { value: "median", label: "Median" },
                                        { value: "variance", label: "Variance" },
                                        { value: "deviation", label: "Deviation" },
                                        { value: "cnt", label: "Count" }
                                    ]
                                }
                            }, Select)
                        }
                    });
                }

                var data = [];
                var context = this;
                arrayUtil.forEach(fieldMappings, function (fieldMapping, idx) {
                    if (idx > 0) {
                        var value = context.getFieldValue(options2, fieldMapping._id, idx === 1 ? 2 : 0);
                        var aggr = context.getFieldAggregation(fieldMapping._id);
                        data.push({
                            id: "value" + (idx > 1 ? idx : ""),
                            field: value,
                            aggregation: value ? aggr : ""
                        });
                    }
                });
                this.mappingValues.setData(data);
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

        vizType: "",
        refreshVizType: function (_value) {
            var valueParts = _value.split(" ");
            var value = valueParts[0];
            var chartType = valueParts[1];
            var deferred = new Deferred();
            var context = this;

            function requireWidget() {
                Utility.resolve("viz/" + context.vizType, function (D3Viz) {
                    context.d3Viz = new D3Viz();
                    context.d3Viz._chartType = chartType;
                    domConstruct.empty(context.id + "VizCP");
                    context.d3Viz.renderTo({
                        domNodeID: context.id + "VizCP"
                    });
                    deferred.resolve(context.vizType);
                });
            }

            if (this.vizType !== value || this.chartType !== chartType) {
                this.vizType = value;
                this.chartType = chartType;
                requireWidget();
            }

            return deferred.promise;
        },

        getFilter: function () {
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
            if (this.limit.get("value") > this.rows.length) {
                var result = this.wu.results[this.params.Sequence];
                var context = this;
                result.fetchNRows(this.rows.length, this.limit.get("value")).then(function (response) {
                    context.rows = context.rows.concat(response);
                    context.loadData();
                });
            } else {
                this.loadData();
            }
        },

        loadData: function () {
            var request = domForm.toObject(this.id + "MappingForm");
            arrayUtil.forEach(this.mappingValues.store.data, function (row, idx) {
                if (row.field) {
                    request[row.id] = row.field;
                    request[row.id + "_aggr"] = row.aggregation;
                }
            }, this);
            var context = this;
            var data = hpccCommon.nest()
                .key(function (d) { return d[request.label]; })
                .rollup(function (leaves) {
                    var retVal = {
                    };
                    arrayUtil.forEach(context.mappingValues.store.data, function (row, idx) {
                        if (row.field) {
                            switch (row.aggregation) {
                                case "cnt":
                                    retVal[row.id] = leaves.length;
                                    break;
                                default:
                                    retVal[row.id] = hpccCommon[row.aggregation || "mean"](leaves, function (d) {
                                        return d[row.field];
                                    });
                                    break;
                            }
                        }
                    }, this);
                    return retVal;
                })
                .entries(this.rows).map(function (d) {
                    var retVal = d.value;
                    retVal.label = d.key;
                    return retVal;
                })
                ;
            this.d3Viz.setData(data, null, request);

            this.params.limit = this.limit.get("value");
            this.params.viz = this.vizSelect.get("value");
            this.params.mapping = ioQuery.objectToQuery(request);
            this.defaultSelection = request;
            this.refreshHRef();
            this.d3Viz.display();
        }
    });
});
