define([
    "dojo/_base/declare",

    "dijit/form/Select",

    "hpcc/TargetSelectClass"
], function (declare,
    Select,
    TargetSelectClass) {

    return declare("TargetSelectWidget", [Select], TargetSelectClass);
});
