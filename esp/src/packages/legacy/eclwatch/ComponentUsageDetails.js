define([
    "dojo/_base/declare",
    "src/nlsHPCC",

    "dijit/registry",

    "hpcc/_Widget",
    "src/ESPUtil",
    "src/DiskUsage",

    "dojo/text!../templates/ComponentUsageDetails.html",

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
            this._diskUsage = new DiskUsage.ComponentDetails(params.component)
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
            };

            this.widget.BorderContainer.resize();
        },

        initTab: function () {
            var currSel = this.getSelectedChild();
            if (currSel && !currSel.initalized) {
                if (currSel.id === this.workunitsTab.id) {
                } else {
                    if (!currSel.initalized) {
                        currSel.init(currSel.params);
                    }
                }
            }
        },

        refreshGrid: function (clearSelection) {
            this._diskUsage.refresh();
        }
    });
});
