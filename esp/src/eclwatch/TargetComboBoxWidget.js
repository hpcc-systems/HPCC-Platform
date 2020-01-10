define([
    "dojo/_base/declare",

    "dijit/form/ComboBox",

    "hpcc/TargetSelectClass"
], function (declare,
    ComboBox,
    TargetSelectClass) {

    return declare("TargetComboBoxWidget", [ComboBox], TargetSelectClass);
});
