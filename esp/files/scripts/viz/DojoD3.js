define([
  "dojo/_base/declare",
  "dojo/_base/lang",
  "dojo/dom",
  "dojo/dom-construct",
  "dojo/dom-geometry",
  "dojo/Evented",

  "d3/d3.v3.min"
], function (declare, lang, dom, domConstruct, domGeom, Evented) {
    return declare(null, {
        constructor: function () {
        },

        resize: function () {
            var _debounce = function (fn, timeout) {
                var timeoutID = -1;
                return function () {
                    if (timeoutID > -1) {
                        window.clearTimeout(timeoutID);
                    }
                    timeoutID = window.setTimeout(fn, timeout);
                }
            };

            var _debounced_draw = _debounce(lang.hitch(this, function () {
                domConstruct.empty(this.target.domNodeID);
                this.renderTo(this._target);
                if (this.hasData()) {
                    this.display();
                }
            }), 125);

            _debounced_draw();
        },

        renderTo: function (_target) {
            this._target = _target;
            this.calcGeom();
            this.injectStyleSheet();
            this.Svg = d3.select(this.target.domDivID).append("svg")
                .attr("width", this.target.width)
                .attr("height", this.target.height)
            ;
            this.SvgG = this.Svg.append("g");
        },

        calcGeom: function () {
            var node = dom.byId(this._target.domNodeID);
            var pos = domGeom.position(node);
            var pad = domGeom.getPadExtents(node);
            this.target = lang.mixin({
                domDivID: "#" + this._target.domNodeID,
                width: pos.w - pad.w,
                height: pos.h - pad.h,
                margin: { top: 0, right: 0, bottom: 0, left: 0 }
            }, this._target);
            lang.mixin(this.target, {
                diameter: Math.min(this.target.width, this.target.height)
            });
            lang.mixin(this.target, {
                radius: this.target.diameter / 2
            });
            return this.target;
        },

        injectStyleSheet: function() {
            if (this.target.css) {
                var styleNode = dom.byId(this.target.domNodeID + "Style");
                if (styleNode) {
                    domConstruct.destroy(this.target.domNodeID + "Style");
                }
                var style = domConstruct.create("style", {
                    id: this.target.domNodeID + "Style",
                    innerHTML: this.target.css
                });
                dojo.query("head").append(style);
            }
        }
    });
});
