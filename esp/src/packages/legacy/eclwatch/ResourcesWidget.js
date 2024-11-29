define([
    "dojo/_base/declare",
    "src/nlsHPCC",
    "dojo/_base/array",

    "dijit/registry",
    "dijit/layout/ContentPane",
    "dijit/form/Button",

    "dgrid/selector",

    "hpcc/GridDetailsWidget",
    "src/ESPRequest",
    "src/ESPWorkunit",
    "hpcc/DelayLoadWidget",
    "src/WsTopology",
    "src/ESPUtil"

], function (declare, nlsHPCCMod, arrayUtil,
    registry, ContentPane, Button,
    selector,
    GridDetailsWidget, ESPRequest, ESPWorkunit, DelayLoadWidget, WsTopology, ESPUtil) {

    var nlsHPCC = nlsHPCCMod.default;
    return declare("ResourcesWidget", [GridDetailsWidget], {
        i18n: nlsHPCC,

        gridTitle: nlsHPCC.Resources,
        idProperty: "__hpcc_id",

        wu: null,
        query: null,

        init: function (params) {
            if (this.inherited(arguments))
                return;

            var context = this;
            if (params.Wuid) {
                this.wu = ESPWorkunit.Get(params.Wuid);
                var monitorCount = 4;
                this.wu.monitor(function () {
                    if (context.wu.isComplete() || ++monitorCount % 5 === 0) {
                        context.refreshGrid();
                    }
                });
            }
            this._refreshActionState();
        },

        createGrid: function (domID) {
            var context = this;
            this.openButton = registry.byId(this.id + "Open");
            this.clusterPauseButton = new Button({
                id: this.id + "Content",
                label: this.i18n.Content,
                onClick: function (event) {
                    context._onOpen(event, {
                        showSource: true
                    });
                }
            }).placeAt(this.openButton.domNode, "after");

            var retVal = new declare([ESPUtil.Grid(false, true)])({
                store: this.store,
                columns: {
                    col1: selector({
                        width: 27,
                        selectorType: "checkbox"
                    }),
                    DisplayPath: {
                        label: this.i18n.Name, sortable: true,
                        formatter: function (url, row) {
                            return "<a href='#' onClick='return false;' class='dgrid-row-url'>" + url + "</a>";
                        }
                    }
                }
            }, domID);

            retVal.on(".dgrid-row-url:click", function (evt) {
                if (context._onRowDblClick) {
                    var row = retVal.row(evt).data;
                    context._onRowDblClick(row);
                }
            });
            return retVal;
        },

        setContent: function (target, type, postfix) {
            var context = this;
            WsTopology.GetWsEclIFrameURL(type).then(function (response) {
                var targetSrc = context.params.QuerySetId + "/" + context.params.Id + (postfix ? postfix : "");
                var src = response + encodeURIComponent(targetSrc);
                target.set("content", dojo.create("iframe", {
                    src: src,
                    style: "border: 0; width: 100%; height: 100%"
                }));
            });
        },

        getDetailID: function (row, params) {
            var retVal = "Detail" + row[this.idProperty];
            if (params && params.showSource) {
                retVal += "Source";
            }
            return retVal;
        },

        createDetail: function (id, row, params) {
            if (params && params.showSource) {
                return new DelayLoadWidget({
                    id: id,
                    title: row.DisplayPath,
                    closable: true,
                    delayWidget: "ECLSourceWidget",
                    hpcc: {
                        params: {
                            sourceMode: "text",
                            sourceURL: ESPRequest.getBaseURL("WsWorkunits") + "/" + row.URL
                        }
                    }
                });
            }
            var retVal = new ContentPane({
                id: id,
                title: row.DisplayPath,
                closable: true,
                style: "padding: 0px; border:0px; border-color:none; overflow: hidden"
            });
            if (this.params.QuerySetId && this.params.Id) {
                this.setContent(retVal, "res", "/" + row.DisplayPath);
            } else {
                retVal.set("content", dojo.create("iframe", {
                    src: dojoConfig.urlInfo.pathname + "?Widget=IFrameWidget&src=" + encodeURIComponent("/WsWorkunits/" + row.URL),
                    style: "border: 0; width: 100%; height: 100%"
                }));
            }
            return retVal;
        },

        refreshGrid: function (args) {
            if (this.wu) {
                var context = this;
                this.wu.getInfo({
                    onGetResourceURLs: function (resourceURLs) {
                        arrayUtil.some(resourceURLs, function (item, idx) {
                            if (!context.firstLoad && (item.DisplayName === "index.htm" || item.DisplayName === "index.html")) {
                                context.firstLoad = true;
                                context._onRowDblClick(item);
                                return false;
                            }
                        });
                        context.store.setData(resourceURLs);
                        context.grid.refresh();
                    }
                });
            }
        },

        refreshActionState: function (selection) {
            this.inherited(arguments);
            registry.byId(this.id + "Content").set("disabled", !selection.length);
        }
    });
});
