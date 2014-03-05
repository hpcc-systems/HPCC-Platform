define([
  "dojo/_base/declare",
  "dojo/_base/lang",
  "dojo/_base/array",
  "dojo/dom",
  "dojo/dom-construct",
  "dojo/dom-style",

  "dijit/form/HorizontalSlider",
  "dijit/form/HorizontalRule",
  "dijit/form/HorizontalRuleLabels",

  "./DojoD3",
  "./Mapping"
], function (declare, lang, arrayUtil, dom, domConstruct, domStyle,
    HorizontalSlider, HorizontalRule, HorizontalRuleLabels,
    DojoD3, Mapping) {
    return declare([Mapping, DojoD3], {

        constructor: function (mappings, target) {
            this.setDatasetMappings({
                slider: {
                    display: "Slider Data",
                    fields: {
                        label: "Label"
                    }
                }
            });
            this.setFieldMappings(mappings);
            this.renderTo(target);
        },

        setValue: function (value) {
            this.vertSlider.set("value", value);
        },

        renderTo: function (_target) {
            _target = lang.mixin({
                css: ""
            }, _target);
            this._target = _target;
            this.calcGeom();

            this.display();
        },

        display: function (data) {
            this.inherited(arguments);
            if (data)
                this.setData(data);
            var data = this.getMappedData();
            if (!data || !data.length) {
                return
            }
            var labels = [];
            arrayUtil.forEach(data, lang.hitch(this, function (item, idx) {
                labels.push(item.label);
            }));
            var value = labels[0];
            // Create the rules
            // Create the labels
            if (this.sliderNode) {
                value = this.vertSlider.get("value");
                this.vertSlider.destroyRecursive();
                this.sliderRules.destroyRecursive();
                this.sliderLabels.destroyRecursive();
                domConstruct.destroy(this.sliderNode);
            }
            this.sliderNode = domConstruct.create("div", {}, dom.byId(this.target.domNodeID), "first");
            this.labelsNode = domConstruct.create("div", {}, this.sliderNode, "first");
            this.sliderLabels = new HorizontalRuleLabels({
                container: "bottomDecoration",
                labelStyle: "font-style: italic; height: 2em;",
                //numericMargin: 1,
                labels: labels
            }, this.labelsNode);

            this.rulesNode = domConstruct.create("div", {}, this.sliderNode, "first");
            this.sliderRules = new HorizontalRule({
                container: "bottomDecoration",
                count: labels.length,
                style: "height: 5px;"
            }, this.rulesNode);

            // Create the vertical slider programmatically
            this.vertSlider = new HorizontalSlider({
                minimum: labels[0],
                maximum: labels[labels.length - 1],
                //pageIncrement: 20,
                value: value,
                intermediateChanges: false,
                discreteValues: labels.length,
                //style: "height: " + this.target.height + "px;"
                onChange: lang.hitch(this, function (value) {
                    var evt = {};
                    evt[this.getFieldMapping("label")] = value;
                    this.emit("click", evt);
                })
            }, this.sliderNode);

            // Start up the widgets
            this.vertSlider.startup();
            this.sliderRules.startup();
            this.sliderLabels.startup();
        }
    });
});
