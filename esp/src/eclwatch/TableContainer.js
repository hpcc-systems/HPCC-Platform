define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/_base/array",

    "dojox/layout/TableContainer"
], function (declare, lang, arrayUtil,
    DojoxTableContainer) {
    return declare("hpcc.TableContainer", [DojoxTableContainer], {

        layout: function (params) {
            if (!this._initialized) {
                return;
            }

            arrayUtil.forEach(this.getChildren(), lang.hitch(this, function (child, index) {
                child.set("label", child.get("label").split(" ").join("&nbsp;"));
                child.set("title", child.get("title").split(" ").join("&nbsp;"));
            }));

            this.inherited(arguments);
        }
    });
});
