define([
    "dojo/_base/declare", // declare
    "dojo/io-query",
    "dojo/dom",
    "dojo/dom-attr",
    "dojo/dom-style",

    "dijit/layout/_LayoutWidget",
    "dijit/_TemplatedMixin",
    "dijit/_WidgetsInTemplateMixin",
    "dijit/registry"

], function (declare, ioQuery, dom, domAttr, domStyle,
    _LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin, registry) {

    return declare("_Widget", [_LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
        baseClass: "_Widget",

        dojoConfig: dojoConfig,
        
        initalized: false,

        constructor: function (args) {
            this.widget = {};
        },

        postCreate: function() {
            this.inherited(arguments);
            this.registerChildWidgets(this.domNode);
        },

        _onNewPage: function (event) {
            var baseUrl = document.URL.split("#")[0];
            baseUrl = baseUrl.split("?")[0];
            var paramsString = ioQuery.objectToQuery(this.params);
            var win = window.open(baseUrl + "?" + paramsString, "_blank");
            win.focus();
        },

        init: function (params) {
            if (this.initalized)
                return true;
            this.initalized = true;
            this.params = params;
            if (!this.params.Widget) {
                this.params.Widget = this.declaredClass;
            }
            
            return false;
        },

        //  Dijit functions  ---
        registerChildWidgets: function (domNode) {
            var childWidgets = registry.findWidgets(domNode);
            for (var i = 0; i < childWidgets.length; ++i) {
                var childWidget = childWidgets[i];
                if (!childWidget.registerChildWidgets) {
                    this.registerChildWidgets(childWidget.domNode);
                }
                if (childWidget.params && childWidget.params.id) {
                    if (childWidget.params.id.indexOf(this.id) === 0) {
                        this.widget[childWidget.params.id.substring(this.id.length)] = childWidget;
                    }
                }
            }
        },

        //  DOM functions  ---
        setDisabled: function (id, disabled, icon, disabledIcon) {
            var target = registry.byId(id);
            if (target) {
                target.set("disabled", disabled);
                target.set("iconClass", disabled ? disabledIcon : icon);
            }
        },

        setVisible: function (id, visible) {
            var target = dom.byId(id);
            if (target) {
                domStyle.set(target, "display", visible ? "block" : "none");
                domStyle.set(target, "opacity", visible ? "255" : "0");
            }
        },

        updateInput: function (name, oldValue, newValue) {
            var registryNode = registry.byId(this.id + name);
            if (registryNode) {
                registryNode.set("value", newValue);
            } else {
                var domElem = dom.byId(this.id + name);
                if (domElem) {
                    switch (domElem.tagName) {
                        case "SPAN":
                        case "DIV":
                            domAttr.set(this.id + name, "innerHTML", newValue);
                            break;
                        case "INPUT":
                        case "TEXTAREA":
                            domAttr.set(this.id + name, "value", newValue);
                            break;
                        default:
                            throw new Error("Unknown DOM element:  " + domElem.tagName);
                    }
                }
            }
        },

        //  String functions  ---
        endsWith: function (str, suffix) {
            return str.indexOf(suffix, str.length - suffix.length) !== -1;
        },

        isCharCodePrintable: function (charCode) {
            if (charCode < 32)
                return false;
            else if (charCode >= 127 && charCode <= 159)
                return false;
            else if (charCode === 173)
                return false;
            else if (charCode > 255)
                return false;
            return true;
        },

        isCharPrintable: function (_char) {
            return this.isCharCodePrintable(_char.charCodeAt(0));
        },

        isalpha: function(c) {
            return (((c >= 'a') && (c <= 'z')) || ((c >= 'A') && (c <= 'Z')));
        },

        isdigit: function(c) {
            return ((c >= '0') && (c <= '9'));
        },

        isalnum: function(c) {
            return (this.isalpha(c) || this.isdigit(c));
        },

        setCharAt: function(str, index, chr) {
            if(index > str.length-1) 
                return str;
            return str.substr(0, index) + chr + str.substr(index + 1);
        },

        formatXml: function (xml) {
            var reg = /(>)(<)(\/*)/g;
            var wsexp = / *(.*) +\n/g;
            var contexp = /(<.+>)(.+\n)/g;
            xml = xml.replace(reg, '$1\n$2$3').replace(wsexp, '$1\n').replace(contexp, '$1\n$2');
            var pad = 0;
            var formatted = '';
            var lines = xml.split('\n');
            var indent = 0;
            var lastType = 'other';
            // 4 types of tags - single, closing, opening, other (text, doctype, comment) - 4*4 = 16 transitions 
            var transitions = {
                'single->single': 0,
                'single->closing': -1,
                'single->opening': 0,
                'single->other': 0,
                'closing->single': 0,
                'closing->closing': -1,
                'closing->opening': 0,
                'closing->other': 0,
                'opening->single': 1,
                'opening->closing': 0,
                'opening->opening': 1,
                'opening->other': 1,
                'other->single': 0,
                'other->closing': -1,
                'other->opening': 0,
                'other->other': 0
            };

            for (var i = 0; i < lines.length; i++) {
                var ln = lines[i];
                var single = Boolean(ln.match(/<.+\/>/)); // is this line a single tag? ex. <br />
                var closing = Boolean(ln.match(/<\/.+>/)); // is this a closing tag? ex. </a>
                var opening = Boolean(ln.match(/<[^!].*>/)); // is this even a tag (that's not <!something>)
                var type = single ? 'single' : closing ? 'closing' : opening ? 'opening' : 'other';
                var fromTo = lastType + '->' + type;
                lastType = type;
                var padding = '';

                indent += transitions[fromTo];
                for (var j = 0; j < indent; j++) {
                    padding += '  ';
                }
                if (fromTo == 'opening->closing')
                    formatted = formatted.substr(0, formatted.length - 1) + ln + '\n'; // substr removes line break (\n) from prev loop
                else
                    formatted += padding + ln + '\n';
            }

            return formatted;
        },

        //  Util functions  ---
        createChildTabID: function (someStr) {
            var retVal = "";
            for (var i = 0; i < someStr.length; ++i) {
                var c = someStr[i];
                if (!this.isalnum(c)) {
                    someStr = this.setCharAt(someStr, i, "x");
                }
            }
            return this.id + "_" + someStr;
        }
    });
});
