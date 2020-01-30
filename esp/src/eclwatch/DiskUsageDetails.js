define([
    "dojo/_base/declare",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",

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

], function (declare, i18n, nlsHPCC,
    registry,
    _Widget, ESPUtil, DiskUsage,
    template) {
    return declare("DiskUsageDetails", [_Widget, ESPUtil.FormHelper], {
        templateString: template,
        baseClass: "DiskUsageDetails",
        i18n: nlsHPCC,

        postCreate: function (args) {
            this.inherited(arguments);
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

            this._diskUsage = new DiskUsage.Details(params.details.Name)
                .target(this.id + "DiskUsageGrid")
                .details(params.details)
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
            this._diskUsage.refresh();
        }
    });
});
