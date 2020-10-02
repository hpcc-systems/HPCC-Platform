define([
    "dojo/_base/declare",
    "src/nlsHPCC",
    "dojo/dom-construct",

    "dijit/registry",

    "hpcc/_Widget",

    "dojo/text!../templates/IFrameWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/Toolbar",
    "dijit/ToolbarSeparator",
    "dijit/form/Button",
    "dijit/layout/ContentPane"
], function (declare, nlsHPCCMod, domConstruct,
    registry,
    _Widget,
    template) {

    var nlsHPCC = nlsHPCCMod.default;
    return declare("IFrameWidget", [_Widget], {
        templateString: template,
        baseClass: "IFrameWidget",
        i18n: nlsHPCC,

        postCreate: function (args) {
            this.inherited(arguments);
            this.borderContainer = registry.byId(this.id + "BorderContainer");
            this.contentPane = registry.byId(this.id + "ContentPane");
        },

        resize: function (args) {
            this.inherited(arguments);
            if (this.borderContainer) {
                this.borderContainer.resize();
            }
        },

        _onNewPageNoFrame: function (event) {
            var win = window.open(this.params.src);
            win.focus();
        },

        //  Hitched actions  ---
        _onRefresh: function (event) {
            this.contentPane.set("content", domConstruct.create("iframe", {
                id: this.id + "IFrame",
                src: this.params.src,
                style: "border:1px solid lightgray; width: 100%; height: 100%"
            }));
        },

        //  Implementation  ---
        init: function (params) {
            if (this.inherited(arguments))
                return;

            this.wrapInHRef(this.id + "NewPageNoFrame", this.getURL());
            this._onRefresh();
        }
    });
});
