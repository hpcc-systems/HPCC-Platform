define([
    "dojo/_base/declare",
    "dojo/_base/array",
    "dojo/_base/lang",
    "src/nlsHPCC",
    "dojo/io-query",
    "dojo/dom",
    "dojo/dom-construct",
    "dojo/dom-attr",
    "dojo/dom-style",

    "src/ESPUtil",

    "dijit/layout/_LayoutWidget",
    "dijit/_TemplatedMixin",
    "dijit/_WidgetsInTemplateMixin",
    "dijit/registry"

], function (declare, arrayUtil, lang, nlsHPCCMod, ioQuery, dom, domConstruct, domAttr, domStyle,
    ESPUtil,
    _LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin, registry) {

    var nlsHPCC = nlsHPCCMod.default;
    //  IE8 textContent polyfill  ---
    if (Object.defineProperty
        && Object.getOwnPropertyDescriptor
        && Object.getOwnPropertyDescriptor(Element.prototype, "textContent")
        && !Object.getOwnPropertyDescriptor(Element.prototype, "textContent").get) {
        (function () {
            var innerText = Object.getOwnPropertyDescriptor(Element.prototype, "innerText");
            Object.defineProperty(Element.prototype, "textContent",
                {
                    get: function () {
                        return innerText.get.call(this);
                    },
                    set: function (s) {
                        innerText.set.call(this, s);
                    }
                }
            );
        })();
    }

    // Production steps of ECMA-262, Edition 5, 15.4.4.21
    // Reference: http://es5.github.io/#x15.4.4.21
    // https://tc39.github.io/ecma262/#sec-array.prototype.reduce
    if (!Array.prototype.reduce) {
        Array.prototype.reduce = function (callback /*, initialValue*/) {   // jshint ignore:line
            if (this === null) {
                throw new TypeError("Array.prototype.reduce called on null or undefined");
            }
            if (typeof callback !== "function") {
                throw new TypeError(callback + " is not a function");
            }

            // 1. Let O be ? ToObject(this value).
            var o = Object(this);

            // 2. Let len be ? ToLength(? Get(O, "length")).
            var len = o.length >>> 0;

            // Steps 3, 4, 5, 6, 7      
            var k = 0;
            var value;

            if (arguments.length === 2) {
                value = arguments[1];
            } else {
                while (k < len && !(k in o)) {
                    k++;
                }

                // 3. If len is 0 and initialValue is not present, throw a TypeError exception.
                if (k >= len) {
                    throw new TypeError("Reduce of empty array with no initial value");
                }
                value = o[k++];
            }

            // 8. Repeat, while k < len
            while (k < len) {
                // a. Let Pk be ! ToString(k).
                // b. Let kPresent be ? HasProperty(O, Pk).
                // c. If kPresent is true, then
                //    i. Let kValue be ? Get(O, Pk).
                //    ii. Let accumulator be ? Call(callbackfn, undefined, "accumulator, kValue, k, O").
                if (k in o) {
                    value = callback(value, o[k], k, o);
                }

                // d. Increase k by 1.      
                k++;
            }

            // 9. Return accumulator.
            return value;
        };
    }

    return declare("_Widget", [_LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
        baseClass: "_Widget",
        i18n: nlsHPCC,

        dojoConfig: dojoConfig,

        initalized: false,

        constructor: function (args) {
            this.widget = {};
        },

        postCreate: function () {
            this.inherited(arguments);
            this.registerChildWidgets(this.domNode);
        },

        getFilterParams: function () {
            var retVal = null;
            if (lang.exists("filter.toObject", this)) {
                var obj = this.filter.toObject();
                for (var key in obj) {
                    if (!retVal) {
                        retVal = {};
                    }
                    if (obj[key]) {
                        retVal[key] = obj[key];
                    }
                }
            }
            return retVal;
        },

        getURL: function () {
            var baseUrl = document.URL.split("#")[0];
            var baseUrlParts = baseUrl.split("?");
            baseUrl = baseUrlParts[0];
            var args = baseUrlParts[1];
            delete this.params.__filter;
            var filterParams = this.getFilterParams();
            if (filterParams) {
                this.params.__filter = ioQuery.objectToQuery(filterParams);
            }
            var paramsString = ioQuery.objectToQuery(this.params);
            return baseUrl + "?" + paramsString;
        },

        _onNewPage: function (event) {
            var win = window.open(this.getURL(), "_blank");
            if (win) {
                win.focus();
            }
        },

        _prevMax: undefined,
        _onMaximize: function (max) {
            this._prevMax = ESPUtil.maximizeWidget(this, max, this._prevMax);
        },

        init: function (params) {
            if (this.initalized)
                return true;
            this.initalized = true;
            this.params = params;
            if (!this.params.Widget) {
                this.params.Widget = this.declaredClass;
            }
            if (lang.exists("params.__filter", this) && lang.exists("filter.toObject", this)) {
                var filterObj = ioQuery.queryToObject(this.params.__filter);
                this.filter.fromObject(filterObj);
            }
            this.wrapInHRef(this.id + "NewPage", this.getURL());
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
            disabled = disabled || false;
            var target = registry.byId(id);
            if (target) {
                target.set("disabled", disabled);
                if ((!disabled && icon) || (disabled && disabledIcon)) {
                    target.set("iconClass", disabled ? disabledIcon : icon);
                }
            }
        },

        setVisible: function (id, visible) {
            var target = dom.byId(id);
            if (target) {
                domStyle.set(target, "display", visible ? "block" : "none");
                domStyle.set(target, "opacity", visible ? "255" : "0");
            }
        },

        isDefined: function (variable) {
            if (typeof variable === "undefined") {
                return false;
            } else if (variable === null) {
                return false;
            }
            return true;
        },

        updateInput: function (name, oldValue, _newValue) {
            var newValue = this.isDefined(_newValue) ? _newValue : "";
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

        wrapInHRef: function (node, href) {
            var nodeToWrap = dom.byId(node);
            if (nodeToWrap) {
                var hrefNode = domConstruct.create("a", {
                    href: href,
                    onClick: function (event) {
                        event.preventDefault();
                    }
                }, nodeToWrap, "after");
                domConstruct.place(nodeToWrap, hrefNode, "first");
            }
        },

        refreshHRef: function (node, href) {
            var nodeToWrap = dom.byId(this.id + "NewPage");
            if (nodeToWrap && nodeToWrap.parentNode && nodeToWrap.parentNode.tagName === "A") {
                nodeToWrap.parentNode.href = this.getURL();
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

        isalpha: function (c) {
            return (((c >= "a") && (c <= "z")) || ((c >= "A") && (c <= "Z")));
        },

        isdigit: function (c) {
            return ((c >= "0") && (c <= "9"));
        },

        isalnum: function (c) {
            return (this.isalpha(c) || this.isdigit(c));
        },

        setCharAt: function (str, index, chr) {
            if (index > str.length - 1)
                return str;
            return str.substr(0, index) + chr + str.substr(index + 1);
        },

        formatXml: function (xml) {
            var reg = /(>)(<)(\/*)/g;
            var wsexp = / *(.*) +\n/g;
            var contexp = /(<.+>)(.+\n)/g;
            xml = xml.replace(reg, "$1\n$2$3").replace(wsexp, "$1\n").replace(contexp, "$1\n$2");
            var pad = 0;
            var formatted = "";
            var lines = xml.split("\n");
            var indent = 0;
            var lastType = "other";
            // 4 types of tags - single, closing, opening, other (text, doctype, comment) - 4*4 = 16 transitions 
            var transitions = {
                "single->single": 0,
                "single->closing": -1,
                "single->opening": 0,
                "single->other": 0,
                "closing->single": 0,
                "closing->closing": -1,
                "closing->opening": 0,
                "closing->other": 0,
                "opening->single": 1,
                "opening->closing": 0,
                "opening->opening": 1,
                "opening->other": 1,
                "other->single": 0,
                "other->closing": -1,
                "other->opening": 0,
                "other->other": 0
            };

            for (var i = 0; i < lines.length; i++) {
                var ln = lines[i];
                var single = Boolean(ln.match(/<.+\/>/)); // is this line a single tag? ex. <br />
                var closing = Boolean(ln.match(/<\/.+>/)); // is this a closing tag? ex. </a>
                var opening = Boolean(ln.match(/<[^!?].*>/)); // is this even a tag (that's not <!something>)
                var type = single ? "single" : closing ? "closing" : opening ? "opening" : "other";
                var fromTo = lastType + "->" + type;
                lastType = type;
                var padding = "";

                indent += transitions[fromTo];
                for (var j = 0; j < indent; j++) {
                    padding += "  ";
                }
                if (fromTo === "opening->closing")
                    formatted = formatted.substr(0, formatted.length - 1) + ln + "\n"; // substr removes line break (\n) from prev loop
                else
                    formatted += padding + ln + "\n";
            }

            return formatted;
        },

        arrayToList: function (arr, field) {
            var retVal = "";
            arrayUtil.some(arr, function (item, idx) {
                if (retVal.length) {
                    retVal += "\n";
                }
                if (idx >= 10) {
                    retVal += "\n..." + (arr.length - 10) + " " + this.i18n.More + "...";
                    return true;
                }
                var lineStr = field ? item[field] : item;
                if (lineStr.length > 50) {
                    retVal += "..." + item[field].slice(25, item[field].length);
                }
                else {
                    retVal += lineStr;
                }
            }, this);
            return retVal;
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
