define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/dom",
    "dojo/dom-class",
    "dojo/dom-style",
    "dojo/Evented",

    "dijit/registry",
    "dijit/layout/BorderContainer",
    "dijit/layout/ContentPane",

    "hpcc/_Widget",

    "@hpcc-js/eclwatch",

    "dojo/text!../templates/Graph7Widget.html"
], function (declare, lang, i18n, nlsHPCC, dom, domClass, domStyle, Evented,
    registry, BorderContainer, ContentPane,
    _Widget,
    hpccEclWatch,
    template) {

        return declare("Graph7Widget", [_Widget], {
            templateString: template,
            baseClass: "Graph7Widget",
            i18n: nlsHPCC,

            KeyState_None: 0,
            KeyState_Shift: 1,
            KeyState_Control: 2,
            KeyState_Menu: 4,

            borderContainer: null,
            graphContentPane: null,

            graph: null,

            constructor: function () {
                this._options = {};
            },

            option: function (key, _) {
                if (arguments.length < 1) throw Error("Invalid Call:  option");
                if (arguments.length === 1) return this._options[key];
                this._options[key] = _ instanceof Array ? _.length > 0 : _;
                return this;
            },

            _onClickRefresh: function () {
            },

            _onChangeZoom: function (args) {
                var selection = this.zoomDropCombo.get("value");
                switch (selection) {
                    case this.i18n.All:
                        this.centerOnItem(0, true);
                        break;
                    case this.i18n.Width:
                        this.centerOnItem(0, true, true);
                        break;
                    default:
                        var scale = parseFloat(selection);
                        if (!isNaN(scale)) {
                            this.setScale(scale);
                        }
                        break;
                }
            },

            _onOptionsApply: function () {
            },

            _onOptionsReset: function () {
            },

            buildRendering: function (args) {
                this.inherited(arguments);
            },

            postCreate: function (args) {
                this.inherited(arguments);
                this.borderContainer = registry.byId(this.id + "BorderContainer");
                this.graphContentPane = registry.byId(this.id + "GraphContentPane");
                this.zoomDropCombo = registry.byId(this.id + "ZoomDropCombo");
                this.optionsDropDown = registry.byId(this.id + "OptionsDropDown");
                this.optionsForm = registry.byId(this.id + "OptionsForm");
            },

            startup: function (args) {
                this.inherited(arguments);
            },

            init: function (params) {
                if (this.inherited(arguments))
                    return;

                this.graph = new hpccEclWatch.WUGraph()
                    .target(this.id + "GraphContentPane")
                    .baseUrl("")
                    .render(function (w) {
                        w
                            .wuid(params.Wuid)
                            .graphID(params.GraphName)
                            .subgraphID(params.SubGraphId)
                            .render(function (w) {
                                if (params.ActivityId) {
                                    w.selection(params.ActivityId);
                                }
                            })
                            ;
                    })
                    ;
            },

            resize: function (args) {
                this.inherited(arguments);
                this.borderContainer.resize();
                if (this.graph) {
                    this.graph
                        .resize()
                        .render()
                        ;
                }
            },

            layout: function (args) {
                this.inherited(arguments);
            },

            //  Plugin wrapper  ---
            hasOptions: function () {
                return false;
            },

            showToolbar: function (show) {
                if (show) {
                    domClass.remove(this.id + "Toolbar", "hidden");
                } else {
                    domClass.add(this.id + "Toolbar", "hidden");
                }
                this.resize();
            },

            showOptions: function (show) {
                if (show) {
                    domStyle.set(this.optionsDropDown.domNode, 'display', 'block');
                } else {
                    domStyle.set(this.optionsDropDown.domNode, 'display', 'none');
                }
                this.resize();
            },

            clear: function () {
            },

            find: function (findText) {
                return [];
            },

            findAsGlobalID: function (findText) {
                return [];
            },

            setScale: function (percent) {
                return 100;
            }
        });
    });
