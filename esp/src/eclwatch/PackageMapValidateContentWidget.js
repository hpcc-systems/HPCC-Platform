/*##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems®.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
############################################################################## */
define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/dom",
    "dojo/query",
    "dojo/topic",
    "dijit/registry",

    "hpcc/_TabContainerWidget",
    "hpcc/DelayLoadWidget",
    "hpcc/ECLSourceWidget",
    "src/WsPackageMaps",

    "dojo/text!../templates/PackageMapValidateContentWidget.html",

    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/form/Select",
    "dijit/form/Button"
], function (declare, lang, i18n, nlsHPCC, dom, query, topic, registry,
    _TabContainerWidget, DelayLoadWidget, EclSourceWidget, WsPackageMaps, template) {
    return declare("PackageMapValidateContentWidget", [_TabContainerWidget], {
        templateString: template,
        baseClass: "PackageMapValidateContentWidget",
        i18n: nlsHPCC,

        targets: null,

        targetSelectControl: null,
        processSelectControl: null,
        selectFileControl: null,
        validateButton: null,
        editorControl: null,
        resultControl: null,

        constructor: function() {
            this.targets = [];
            this.processes = [];
        },

        buildRendering: function (args) {
            this.inherited(arguments);
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

        getTitle: function () {
            return this.i18n.ValidatePackageContent;
        },

        postCreate: function (args) {
            this.inherited(arguments);
            this.borderContainer = registry.byId(this.id + "BorderContainer");
            this.targetSelectControl = registry.byId(this.id + "TargetSelect");
            this.processSelectControl = registry.byId(this.id + "ProcessSelect");
            this.validateButton = registry.byId(this.id + "ValidateBtn");
        },

        //  Init  ---
        init: function (params) {
            if (this.inherited(arguments))
                return;

            if ((params.targets !== undefined) && (params.targets[0].Name !== undefined))
                this.initSelections(params.targets);
            else
                this.getSelections();

            this.editorControl = registry.byId(this.id + "Source");
            this.editorControl.init(params);
            this.editorControl.setText(this.i18n.LoadPackageContentHere);
            this.initResultDisplay();

            var context = this;
            this.selectFileControl = document.getElementById(this.id + "SelectFile");
            this.selectFileControl.addEventListener('change', function(event) {
                var reader = new FileReader();
                reader.onload = function(e){
                    context.editorControl.setText(e.target.result);
                };
                reader.readAsText(event.target.files[0]);
            }, false);
        },

        getSelections: function () {
            var context = this;
            WsPackageMaps.GetPackageMapSelectOptions({
                includeTargets: true,
                IncludeProcesses: true,
                IncludeProcessFilters: true
            }).then(function (response) {
                if (lang.exists("Targets.TargetData", response.GetPackageMapSelectOptionsResponse)) {
                    context.targets = response.GetPackageMapSelectOptionsResponse.Targets.TargetData;
                    context.initSelections(context.targets);
                }
                return response;
            }, function (err) {
                context.showErrors(err);
                return err;
            });
        },

        initSelections: function (targets) {
            this.targets = targets;
            if (this.targets.length > 0) {
                var defaultTarget = 0;
                for (var i = 0; i < this.targets.length; ++i) {
                    if ((defaultTarget === 0) && (this.targets[i].Type === 'roxie'))
                        defaultTarget = i; //first roxie
                    this.targetSelectControl.options.push({label: this.targets[i].Name, value: this.targets[i].Name});
                }
                this.targetSelectControl.set("value", this.targets[defaultTarget].Name);
                if (this.targets[defaultTarget].Processes !== undefined)
                    this.updateProcessSelections(this.targets[defaultTarget], '');
            }
        },

        updateProcessSelections: function (target, targetName) {
            this.processSelectControl.removeOption(this.processSelectControl.getOptions());
            if (target !== null)
                this.addProcessSelections(target.Processes.Item);
            else {
                for (var i = 0; i < this.targets.length; ++i) {
                    var target = this.targets[i];
                    if ((target.Processes !== undefined) && (targetName === target.Name)) {
                        this.addProcessSelections(target.Processes.Item);
                        break;
                    }
                }
            }
            this.processSelectControl.options.push({label: this.i18n.ANY, value: 'ANY' });
            this.processSelectControl.set("value", '');
        },

        addProcessSelections: function (processes) {
            this.processes.length = 0;

            if (processes.length < 1)
                return;
            for (var i = 0; i < processes.length; ++i) {
                var process = processes[i];
                if ((this.processes !== null) && (this.processes.indexOf(process) !== -1))
                    continue;
                this.processes.push(process);
                this.processSelectControl.options.push({label: process, value: process});
            }
        },

        initResultDisplay: function () {
            this.resultControl = registry.byId(this.id + "Result");
            this.resultControl.init({sourceMode: 'text/plain', readOnly: true});
            this.resultControl.setText(this.i18n.ValidateResultHere);
        },

        //  action
        _onChangeTarget: function (event) {
            this.targetSelected  = this.targetSelectControl.getValue();
            this.updateProcessSelections(null, this.targetSelected);
        },

        _onLoadBtnClicked: function (event) {
            this.selectFileControl.click();
        },

        _onValidate: function (evt) {
            var content = this.editorControl.getText();
            if (content === '') {
                alert(this.i18n.PackageContentNotSet);
                return;
            }
            var request = { target: this.targetSelectControl.getValue() };
            request['content'] = content;

            var context = this;
            this.resultControl.setText("");
            this.validateButton.set("disabled", true);
            WsPackageMaps.validatePackage(request).then(function (response) {
                var responseText = context.validateResponseToText(response.ValidatePackageResponse);
                if (responseText === '')
                    context.resultControl.setText(context.i18n.Empty);
                else {
                    responseText = context.i18n.ValidateResult + responseText;
                    context.resultControl.setText(responseText);
                }
                context.validateButton.set("disabled", false);
                return response;
            }, function (err) {
                context.showErrors(err);
                return err;
            });
        },

        validateResponseToText: function (response) {
            var text = "";
            if (!lang.exists("Errors", response) || (response.Errors.length < 1))
                text += this.i18n.NoErrorFound;
            else
                text = this.addArrayToText(this.i18n.Errors, response.Errors, text);
            if (!lang.exists("Warnings", response) || (response.Warnings.length < 1))
                text += this.i18n.NoWarningFound;
            else
                text = this.addArrayToText(this.i18n.Warnings, response.Warnings, text);

            text += "\n";
            text = this.addArrayToText(this.i18n.QueriesNoPackage, response.queries.Unmatched, text);
            text = this.addArrayToText(this.i18n.PackagesNoQuery, response.packages.Unmatched, text);
            text = this.addArrayToText(this.i18n.FilesNoPackage, response.files.Unmatched, text);
            return text;
        },

        addArrayToText: function (arrayTitle, arrayItems, text) {
            if ((arrayItems.Item !== undefined) && (arrayItems.Item.length > 0)) {
                text += arrayTitle + ":\n";
                for (var i=0;i<arrayItems.Item.length;i++)
                    text += "  " + arrayItems.Item[i] + "\n";
                text += "\n";
            }
            return text;
        },

        showErrors: function (err) {
            topic.publish("hpcc/brToaster", {
                Severity: "Error",
                Source: err.message,
                Exceptions: [{ Message: err.stack }]
            });
        }
    });
});
