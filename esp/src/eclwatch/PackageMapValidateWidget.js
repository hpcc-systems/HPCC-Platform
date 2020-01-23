define([
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/topic",
    "dijit/registry",

    "hpcc/_TabContainerWidget",
    "src/WsPackageMaps",

    "dojo/text!../templates/PackageMapValidateWidget.html",

    "hpcc/DelayLoadWidget",
    "hpcc/ECLSourceWidget",
    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/form/Button",
    "dijit/form/ToggleButton",
    "dijit/form/Select"
], function (declare, lang, i18n, nlsHPCC, topic, registry,
    _TabContainerWidget, WsPackageMaps, template) {
    return declare("PackageMapValidateWidget", [_TabContainerWidget], {
        templateString: template,
        baseClass: "PackageMapValidateWidget",
        i18n: nlsHPCC,

        targets: null,

        targetSelectControl: null,
        processSelectControl: null,
        validateButton: null,
        editorControl: null,
        resultControl: null,

        validatePackageMapContentWidget: null,
        validatePackageMapContentWidgetLoaded: false,

        constructor: function () {
            this.processes = [];
            this.targets = [];
        },

        buildRendering: function (args) {
            this.inherited(arguments);
        },

        startup: function (args) {
            this.inherited(arguments);
        },

        destroy: function (args) {
            this.inherited(arguments);
        },

        getTitle: function () {
            return this.i18n.ValidateActivePackageMap;
        },

        postCreate: function (args) {
            this.inherited(arguments);
            this.targetSelectControl = registry.byId(this.id + "TargetSelect");
            this.processSelectControl = registry.byId(this.id + "ProcessSelect");
            this.validateButton = registry.byId(this.id + "ValidateBtn");
            this.validatePackageMapContentWidget = registry.byId(this.id + "_ValidatePackageMapContent");
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

        //  init this page
        init: function (params) {
            if (this.inherited(arguments))
                return;

            if (params.params.targets !== undefined)
                this.initSelections(params.params.targets);
            else
                this.getSelections();

            this.editorControl = registry.byId(this.id + "Source");
            this.editorControl.init(params);
            this.initResultDisplay();
        },

        initSelections: function (targets) {
            this.targets = targets;
            if (this.targets.length > 0) {
                var defaultTarget = 0;
                for (var i = 0; i < this.targets.length; ++i) {
                    if ((defaultTarget === 0) && (this.targets[i].Type === 'roxie'))
                        defaultTarget = i; //first roxie
                    this.targetSelectControl.options.push({ label: this.targets[i].Name, value: this.targets[i].Name });
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
            this.processSelectControl.options.push({ label: this.i18n.ANY, value: 'ANY' });
            this.processSelectControl.set("value", '');
        },

        addProcessSelections: function (processes) {
            this.processes.length = 0;
            for (var i = 0; i < processes.length; ++i) {
                var process = processes[i];
                if ((this.processes !== null) && (this.processes.indexOf(process) !== -1))
                    continue;
                this.processes.push(process);
                this.processSelectControl.options.push({ label: process, value: process });
            }
        },

        initResultDisplay: function () {
            this.resultControl = registry.byId(this.id + "Result");
            this.resultControl.init({ sourceMode: 'text/plain', readOnly: true });
            this.resultControl.setText(this.i18n.ValidateResultHere);
        },

        //  init tab
        initTab: function () {
            var currSel = this.getSelectedChild();
            if (!this.validatePackageMapContentWidgetLoaded && (currSel.id === this.validatePackageMapContentWidget.id)) {
                this.validatePackageMapContentWidgetLoaded = true;
                this.validatePackageMapContentWidget.init({
                    targets: this.targets
                });
            }
        },

        //  action
        _onChangeTarget: function (event) {
            this.targetSelected = this.targetSelectControl.getValue();
            this.updateProcessSelections(null, this.targetSelected);
        },

        _onChangeProcess: function (event) {
            var process = this.processSelectControl.getValue();
            if (process === 'ANY')
                process = '*';

            var context = this;
            this.editorControl.setText('');
            WsPackageMaps.getPackage({
                target: this.targetSelectControl.getValue(),
                process: process
            }).then(function (response) {
                if (!lang.exists("GetPackageResponse.Info", response))
                    context.editorControl.setText(i18n.NoContent);
                else
                    context.editorControl.setText(response.GetPackageResponse.Info);
                return response;
            }, function (err) {
                context.showErrors(err);
                return err;
            });
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
                for (var i = 0; i < arrayItems.Item.length; i++)
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
