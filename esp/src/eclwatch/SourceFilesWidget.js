define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "src/nlsHPCC",
    "dojo/_base/array",
    "dojo/dom-class",

    "dijit/registry",

    "dgrid/tree",
    "dgrid/selector",

    "hpcc/GridDetailsWidget",
    "src/ESPWorkunit",
    "hpcc/DelayLoadWidget",
    "src/ESPUtil",
    "src/Utility",

    "dojo/text!../templates/SourceFilesWidget.html",

], function (declare, lang, nlsHPCCMod, arrayUtil, domClass,
    registry, tree, selector,
    GridDetailsWidget, ESPWorkunit, DelayLoadWidget, ESPUtil, Utility,
    template) {

    var nlsHPCC = nlsHPCCMod.default;
    return declare("SourceFilesWidget", [GridDetailsWidget], {
        i18n: nlsHPCC,
        templateString: template,
        gridTitle: nlsHPCC.title_Inputs,
        idProperty: "sequence",

        wu: null,

        postCreate: function (args) {
            this.inherited(arguments);
            this.filter = registry.byId(this.id + "Filter");
        },

        init: function (params) {
            if (this.inherited(arguments))
                return;

            if (params.Wuid) {
                this.wu = ESPWorkunit.Get(params.Wuid);
                var monitorCount = 4;
                var context = this;
                this.wu.monitor(function () {
                    if (context.wu.isComplete() || ++monitorCount % 5 === 0) {
                        context.refreshGrid();
                    }
                });
            }

            this.filter.init({
                ws_key: "SourceFilesFilter",
                widget: this.widget
            });
            this.filter.on("clear", function (evt) {
                context.refreshGrid();
            });
            this.filter.on("apply", function (evt) {
                context.refreshGrid();
            });

            this._refreshActionState();
        },

        createGrid: function (domID) {
            var context = this;
            this.store.mayHaveChildren = function (item) {
                return item.IsSuperFile;
            };
            this.store.getChildren = function (parent, options) {
                return context.store.query({ __hpcc_parentName: parent.Name });
            };
            var retVal = new declare([ESPUtil.Grid(false, true)])({
                store: this.store,
                query: this.getFilter(),
                columns: {
                    col1: selector({
                        width: 27,
                        selectorType: "checkbox"
                    }),
                    Name: tree({
                        label: "Name", sortable: true,
                        formatter: function (Name, row) {
                            return Utility.getImageHTML(row.IsSuperFile ? "folder_table.png" : "file.png") + "&nbsp;<a href='#' onClick='return false;' class='dgrid-row-url'>" + Name + "</a>";
                        }
                    }),
                    FileCluster: { label: this.i18n.FileCluster, width: 300, sortable: false },
                    Count: {
                        label: this.i18n.Usage, width: 72, sortable: true,
                        renderCell: function (object, value, node, options) {
                            domClass.add(node, "justify-right");
                            node.innerText = Utility.valueCleanUp(value);
                        },
                    }
                }
            }, domID);

            retVal.on(".dgrid-row-url:click", function (evt) {
                if (context._onRowDblClick) {
                    var row = context.grid.row(evt).data;
                    context._onRowDblClick(row);
                }
            });
            return retVal;
        },

        getFilter: function () {
            var retVal = this.filter.toObject();
            return retVal;
        },

        getDetailTitle: function (row, params) {
            return row.Name;
        },

        createDetail: function (id, row) {
            if (lang.exists("IsSuperFile", row) && row.IsSuperFile) {
                return new DelayLoadWidget({
                    id: id,
                    title: row.Name,
                    closable: true,
                    delayWidget: "SFDetailsWidget",
                    hpcc: {
                        type: "SFDetailsWidget",
                        params: {
                            Name: row.Name
                        }
                    }
                });
            } else {
                return new DelayLoadWidget({
                    id: id,
                    title: row.Name,
                    closable: true,
                    delayWidget: "LFDetailsWidget",
                    hpcc: {
                        type: "LFDetailsWidget",
                        params: {
                            NodeGroup: row.FileCluster,
                            Name: row.Name
                        }
                    }
                });
            }
        },

        refreshGrid: function (args) {
            var context = this;
            this.wu.getInfo({
                onGetSourceFiles: function (sourceFiles) {
                    var filter = context.filter.toObject();
                    arrayUtil.forEach(sourceFiles, function (row, idx) {
                        row.sequence = idx;
                    });
                    var files = sourceFiles;
                    var name = filter?.LogicalName ?? "";
                    if (name) {
                        files = files.filter(file => file.Name.match(name.replace(/\*/g, ".*")));
                    }
                    context.store.setData(files);
                    context.grid.set("query", { __hpcc_parentName: "" });
                }
            });
        }

    });
});
