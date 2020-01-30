define([
    "dojo/_base/declare",

    "dijit/registry",

    "hpcc/_Widget",
    "src/ESPWorkunit",

    "dojo/text!../templates/DFUSearchWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/Toolbar",
    "dijit/form/Textarea",
    "dijit/TitlePane",
    "hpcc/TargetSelectWidget",
    "hpcc/ResultsWidget",
    "hpcc/InfoGridWidget"

], function (declare,
    registry,
    _Widget, Workunit,
    template) {
    return declare("DFUSearchWidget", [_Widget], {
        templateString: template,
        baseClass: "DFUSearchWidget",
        borderContainer: null,
        tabContainer: null,

        wu: null,
        loaded: false,

        buildRendering: function (args) {
            this.inherited(arguments);
        },

        postCreate: function (args) {
            this.inherited(arguments);
            this.borderContainer = registry.byId(this.id + "BorderContainer");
        },

        startup: function (args) {
            this.inherited(arguments);
        },

        resize: function (args) {
            this.inherited(arguments);
            this.borderContainer.resize();
        },

        layout: function (args) {
            this.inherited(arguments);
        },

        //  Hitched actions  ---
        _onSave: function (event) {
        },
        _onReset: function (event) {
        },
        _onClone: function (event) {
        },
        _onDelete: function (event) {
        },
        _onAbort: function (event) {
        },
        _onResubmit: function (event) {
        },
        _onRestart: function (event) {
        },
        _onPublish: function (event) {
        },

        //  Implementation  ---
        init: function (params) {
            if (this.inherited(arguments))
                return;

            if (params.Wuid) {
                this.wu = new Workunit({
                    wuid: params.Wuid
                });
                var context = this;
                this.wu.monitor(function (workunit) {
                    context.monitorWorkunit(workunit);
                });
            }
            // this.infoGridWidget.init(params);
        },

        resetPage: function () {
        },

        objectToText: function (obj) {
            var text = ""
            for (var key in obj) {
                text += "<tr><td>" + key + ":</td>";
                if (typeof obj[key] === "object") {
                    text += "[<br/>";
                    for (var i = 0; i < obj[key].length; ++i) {
                        text += this.objectToText(obj[key][i]);
                    }
                    text += "<br/>]<br/>";
                } else {
                    text += "<td>" + obj[key] + "</td></tr>";

                }
            }
            return text;

        },

        monitorWorkunit: function (response) {
            if (!this.loaded) {
                this.loaded = true;
            }

            var context = this;
            if (this.wu.isComplete()) {
                this.wu.getInfo({
                    onGetResults: function (response) {

                    },

                    onGetSourceFiles: function (response) {

                    },

                    onGetTimers: function (response) {

                    },

                    onGetGraphs: function (response) {

                    },

                    onAfterSend: function (response) {
                    }
                });
            }
        }
    });
});
