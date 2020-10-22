define([
    "dojo/_base/declare",
    "src/nlsHPCC",

    "dijit/registry",

    "hpcc/_Widget",
    "src/ESPUtil",
    "src/DiskUsage",

    "dojo/text!../templates/DiskUsageDetails.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/Toolbar",
    "dijit/ToolbarSeparator",
    "dijit/form/Button",
    "dijit/form/ToggleButton",
    "dijit/form/Select",
    "dijit/form/TextBox",
    "dijit/form/DateTextBox",
    "dijit/form/TimeTextBox"

], function (declare, nlsHPCCMod,
    registry,
    _Widget, ESPUtil, DiskUsage,
    template) {

    var nlsHPCC = nlsHPCCMod.default;
    return declare("DiskUsageDetails", [_Widget, ESPUtil.FormHelper], {
        templateString: template,
        baseClass: "DiskUsageDetails",
        i18n: nlsHPCC,

        postCreate: function (args) {
            this.inherited(arguments);
            var context = this;

            this._diskSummaryPane = registry.byId(this.id + "DiskSummaryCP");
            var origResize = this._diskSummaryPane.resize;
            this._diskSummaryPane.resize = function (size) {
                origResize.apply(this, arguments);
                if (context._diskSummary) {
                    context._diskSummary
                        .resize({ width: size.w, height: size.h || context._diskSummaryPane.h })
                        .lazyRender()
                        ;
                }
            }
        },

        resize: function (args) {
            this.inherited(arguments);
            this.widget.BorderContainer.resize();
        },

        getTitle: function () {
            return this.i18n.title_DiskUsage;
        },

        //  Hitched actions  ---
        _onRefresh: function (event) {
            this.refreshGrid();
        },

        //  Implementation  ---
        init: function (params) {
            if (this.inherited(arguments))
                return;

            var context = this;
            this._diskSummary = new DiskUsage.Summary(params.name)
                .target(this.id + "DiskSummary")
                .render()
                .refresh()
                ;

            this._diskUsage = new DiskUsage.Details(params.name)
                .target(this.id + "DiskUsageGrid")
                .render()
                .refresh()
                ;

            this._diskUsagePane = registry.byId(this.id + "DiskUsageGridCP");
            var origResize = this._diskUsagePane.resize;
            this._diskUsagePane.resize = function (size) {
                origResize.apply(this, arguments);
                if (context._diskUsage) {
                    context._diskUsage
                        .resize({ width: size.w, height: size.h })
                        .lazyRender()
                        ;
                }
            }

            this.widget.BorderContainer.resize();
        },

        refreshGrid: function (clearSelection) {
            this._diskSummary.refresh();
            this._diskUsage.refresh();
        }
    });
});
