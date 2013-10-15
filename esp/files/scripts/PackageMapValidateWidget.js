/*##############################################################################
#   HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
############################################################################## */
define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/dom",
    "dojo/dom-attr",
    "dojo/dom-class",
    "dojo/topic",

    "dijit/layout/_LayoutWidget",
    "dijit/_TemplatedMixin",
    "dijit/_WidgetsInTemplateMixin",
    "dijit/registry",

    "hpcc/WsPackageMaps",

    "dojo/text!../templates/PackageMapValidateWidget.html",

    "dijit/form/Form",
    "dijit/form/Button",
    "dijit/form/RadioButton",
    "dijit/form/Select",
    "dijit/form/SimpleTextarea"
], function (declare, lang, dom, domAttr, domClass, topic,
    _LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin, registry,
    WsPackageMaps, template) {
    return declare("PackageMapValidateWidget", [_LayoutWidget, _TemplatedMixin, _WidgetsInTemplateMixin], {
        templateString: template,
        baseClass: "PackageMapValidateWidget",
        validateForm: null,
        targetSelect: null,
        packageContent: null,
        validateResult: null,
        validateButton: null,
        targets: null,
        processes: new Array(),
        initalized: false,

        buildRendering: function (args) {
            this.inherited(arguments);
        },

        postCreate: function (args) {
            this.inherited(arguments);
            this.validateForm = registry.byId(this.id + "ValidatePM");
            this.targetSelect = registry.byId(this.id + "TargetSelect");
            this.processSelect = registry.byId(this.id + "ProcessSelect");
            this.packageContent = registry.byId(this.id + "Package");
            this.validateButton = registry.byId(this.id + "Validate");
            this.validateResult = registry.byId(this.id + "ValidateResult");
        },

        startup: function (args) {
            this.inherited(arguments);
        },

        resize: function (args) {
            this.inherited(arguments);
        },

        layout: function (args) {
            this.inherited(arguments);
        },

        init: function (params) {
            if (this.initalized)
                return;
            this.initalized = true;
            this.validateResult.set('style', 'visibility:hidden');
            if (params.targets == undefined)
                return;
            this.initSelection(params.targets);
        },

        initSelections: function (targets) {
            this.targets = targets;
            if (this.targets.length > 0) {
                for (var i = 0; i < this.targets.length; ++i)
                    this.targetSelect.options.push({label: this.targets[i].Name, value: this.targets[i].Name});
                this.targetSelect.set("value", this.targets[0].Name);
                if (this.targets[0].Processes != undefined)
                    this.updateProcessSelections(this.targets[0].Name);
            }
        },

        addProcessSelections: function (processes) {
            for (var i = 0; i < processes.length; ++i) {
                var process = processes[i];
                if ((this.processes != null) && (this.processes.indexOf(process) != -1))
                    continue;
                this.processes.push(process);
                this.processSelect.options.push({label: process, value: process});
            }
        },

        updateProcessSelections: function (targetName) {
            this.processSelect.removeOption(this.processSelect.getOptions());
            this.processSelect.options.push({label: 'ANY', value: '' });
            for (var i = 0; i < this.targets.length; ++i) {
                var target = this.targets[i];
                if ((target.Processes != undefined) && ((targetName == '') || (targetName == target.Name)))
                    this.addProcessSelections(target.Processes.Item);
            }
            this.processSelect.set("value", '');
        },

        addArrayToText: function (arrayTitle, arrayItems, text) {
            if ((arrayItems.Item != undefined) && (arrayItems.Item.length > 0)) {
                text += arrayTitle + ":\n";
                for (i=0;i<arrayItems.Item.length;i++)
                    text += "  " + arrayItems.Item[i] + "\n";
                text += "\n";
            }
            return text;
        },

        validateResponseToText: function (response) {
            var text = "";
            if (!lang.exists("Errors", response) || (response.Errors.length < 1))
                text += "No errors found\n";
            else
                text = this.addArrayToText("Error(s)", response.Errors, text);
            if (!lang.exists("Warnings", response) || (response.Warnings.length < 1))
                text += "No warnings found\n";
            else
                text = this.addArrayToText("Warning(s)", response.Warnings, text);

            text += "\n";
            text = this.addArrayToText("Queries without matching package", response.queries.Unmatched, text);
            text = this.addArrayToText("Packages without matching queries", response.packages.Unmatched, text);
            text = this.addArrayToText("Files without matching package definitions", response.files.Unmatched, text);
            return text;
        },

        _onChangeTarget: function (event) {
            this.processes.length = 0;
            this.targetSelected  = this.targetSelect.getValue();
            this.updateProcessSelections(this.targetSelected);
        },

        _onValidate: function (event) {
            var request = { target: this.targetSelect.getValue() };
            var type = this.validateForm.attr('value').ValidateType;
            if (type == 'ActivePM') {
                request['active'] = true;
                request['process'] = this.processSelect.getValue();
            } else {
                var content = this.packageContent.getValue();
                if (content == '') {
                    alert('Package content not set');
                    return;
                }
                request['content'] = content;
            }
            var context = this;
            this.validateResult.setValue("");
            this.validateButton.set("disabled", true);
            WsPackageMaps.validatePackage(request, {
                load: function (response) {
                    var responseText = context.validateResponseToText(response);
                    if (responseText == '')
                        context.validateResult.setValue("(Empty)");
                    else {
                        responseText = '=====Validate Result=====\n\n' + responseText;
                        context.validateResult.setValue(responseText);
                    }
                    context.validateResult.set('style', 'visibility:visible');
                    context.validateButton.set("disabled", false);
                },
                error: function (errMsg, errStack) {
                    context.showErrors(errMsg, errStack);
                    context.validateButton.set("disabled", false);
                }
            });
        },

        showErrors: function (errMsg, errStack) {
            dojo.publish("hpcc/brToaster", {
                Severity: "Error",
                Source: errMsg,
                Exceptions: [{ Message: errStack }]
            });
        }
    });
});
