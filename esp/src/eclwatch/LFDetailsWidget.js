define([
    "exports",
    "dojo/_base/declare",
    "dojo/_base/lang",
    "dojo/i18n",
    "dojo/i18n!./nls/hpcc",
    "dojo/dom",
    "dojo/dom-attr",
    "dojo/dom-class",
    "dojo/dom-form",

    "dijit/registry",

    "hpcc/_TabContainerWidget",
    "hpcc/DelayLoadWidget",
    "src/Clippy",
    "src/ESPLogicalFile",
    "src/ESPDFUWorkunit",
    "src/FileSpray",
    "src/DataPatternsWidget",

    "dojo/text!../templates/LFDetailsWidget.html",

    "hpcc/TargetSelectWidget",
    "hpcc/TargetComboBoxWidget",
    "hpcc/FileBelongsToWidget",
    "hpcc/FileHistoryWidget",
    "hpcc/FileBloomsWidget",
    "dijit/layout/BorderContainer",
    "dijit/layout/TabContainer",
    "dijit/layout/ContentPane",
    "dijit/Toolbar",
    "dijit/TooltipDialog",
    "dijit/form/Form",
    "dijit/form/SimpleTextarea",
    "dijit/form/TextBox",
    "dijit/form/Button",
    "dijit/form/DropDownButton",
    "dijit/TitlePane",
    "dijit/ToolbarSeparator",
    "dijit/TooltipDialog",
    "dijit/form/ValidationTextBox",
    "dijit/form/ToggleButton",
    "dijit/form/CheckBox",
    "dijit/form/NumberTextBox",
    "dijit/Fieldset",

    "hpcc/TableContainer"

], function (exports, declare, lang, i18n, nlsHPCC, dom, domAttr, domClass, domForm,
    registry,
    _TabContainerWidget, DelayLoadWidget, Clippy, ESPLogicalFile, ESPDFUWorkunit, FileSpray, DataPatternsWidget,
    template) {
    exports.fixCircularDependency = declare("LFDetailsWidget", [_TabContainerWidget], {
        templateString: template,
        baseClass: "LFDetailsWidget",
        i18n: nlsHPCC,

        borderContainer: null,

        copyForm: null,
        renameForm: null,
        desprayForm: null,
        replicateForm: null,
        summaryWidget: null,
        contentWidget: null,
        dataPatternsWidget: null,
        sourceWidget: null,
        defWidget: null,
        xmlWidget: null,
        filePartsWidget: null,
        queriesWidget: null,
        workunitWidget: null,
        dfuWorkunitWidget: null,
        fileBelongsTo: null,
        fileHistoryWidget: null,
        fileBloomsWidget: null,

        logicalFile: null,
        prevState: "",

        postCreate: function (args) {
            this.inherited(arguments);
            this.copyForm = registry.byId(this.id + "CopyForm");
            this.renameForm = registry.byId(this.id + "RenameForm");
            this.desprayForm = registry.byId(this.id + "DesprayForm");
            this.replicateForm = registry.byId(this.id + "ReplicateForm");
            this.summaryWidget = registry.byId(this.id + "_Summary");
            this.contentWidget = registry.byId(this.id + "_Content");
            this.dataPatternsWidget = registry.byId(this.id + "_DataPatterns");
            this.sourceWidget = registry.byId(this.id + "_Source");
            this.defWidget = registry.byId(this.id + "_DEF");
            this.xmlWidget = registry.byId(this.id + "_XML");
            this.filePartsWidget = registry.byId(this.id + "_FileParts");
            this.queriesWidget = registry.byId(this.id + "_Queries");
            this.workunitWidget = registry.byId(this.id + "_Workunit");
            this.dfuWorkunitWidget = registry.byId(this.id + "_DFUWorkunit");
            this.fileHistoryWidget = registry.byId(this.id + "_FileHistory");
            this.fileBloomsWidget = registry.byId(this.id + "_FileBlooms");
            this.copyTargetSelect = registry.byId(this.id + "CopyTargetSelect");
            this.desprayTargetSelect = registry.byId(this.id + "DesprayTargetSelect");
            this.desprayTooltiopDialog = registry.byId(this.id + "DesprayTooltipDialog");
            this.replicateTargetSelect = registry.byId(this.id + "ReplicateCluster");
            this.replicateSourceLogicalFile = registry.byId(this.id + "ReplicateSourceLogicalFile");
            this.replicateDropDown = registry.byId(this.id + "ReplicateDropDown");
            this.desprayIPSelect = registry.byId(this.id + "DesprayTargetIPAddress");
            this.isProtected = registry.byId(this.id + "isProtected");
            this.isRestricted = registry.byId(this.id + "isRestricted");
            var context = this;
            var origOnOpen = this.desprayTooltiopDialog.onOpen;
            this.desprayTooltiopDialog.onOpen = function () {
                var targetRow;
                if (!context.desprayTargetSelect.initalized) {
                    context.desprayTargetSelect.init({
                        DropZones: true,
                        callback: function (value, item) {
                            if (context.desprayIPSelect) {
                                context.desprayIPSelect.defaultValue = context.desprayIPSelect.get("value");
                                context.desprayIPSelect.loadDropZoneMachines(value);
                                targetRow = item;
                            }
                        }
                    });
                }
                origOnOpen.apply(context.desprayTooltiopDialog, arguments);

                if (!context.desprayIPSelect.initalized) {
                    var pathSepChar;
                    context.desprayIPSelect.init({
                        DropZoneMachines: true,
                        callback: function (value, row) {
                            var path = targetRow.machine.Directory.indexOf("\\");
                            targetRow.machine.Name = value
                            targetRow.machine.Netaddress = value
                            if (context.desprayTargetPath) {
                                context.desprayTargetPath._dropZoneTarget = targetRow;
                                if (path > -1) {
                                    pathSepChar = "\\"
                                    context.pathSepCharG = "\\"
                                } else {
                                    pathSepChar = "/";
                                    context.pathSepCharG = "/"
                                }
                                context.desprayTargetPath.loadDropZoneFolders(pathSepChar, targetRow.machine.Directory);
                            }
                        }
                    });
                }
            }
            this.desprayTargetPath = registry.byId(this.id + "DesprayTargetPath");
            this.fileBelongsToWidget = registry.byId(this.id + "_FileBelongs");

            Clippy.attach(this.id + "ClippyButton");
        },

        //  Hitched actions  ---
        _onRefresh: function (event) {
            this.logicalFile.refresh();
        },
        _onSave: function (event) {
            var context = this;
            var protectedCheckbox = registry.byId(this.id + "isProtected");
            var restrictedCheckbox = registry.byId(this.id + "isRestricted");
            this.logicalFile.save({
                Description: dom.byId(context.id + "Description").value,
                isProtected: protectedCheckbox.get("checked"),
                isRestricted: restrictedCheckbox.get("checked")
            }, null);
        },
        _onDelete: function (event) {
            if (confirm(this.i18n.YouAreAboutToDeleteThisFile)) {
                this.logicalFile.doDelete({
                });
            }
        },

        getTitle: function () {
            return this.i18n.title_LFDetails;
        },

        _handleResponse: function (wuidQualifier, response) {
            if (lang.exists(wuidQualifier, response)) {
                var wu = ESPDFUWorkunit.Get(lang.getObject(wuidQualifier, false, response));
                wu.startMonitor(true);
                var tab = this.ensurePane(wu.ID, {
                    Wuid: wu.ID
                });
                if (tab) {
                    this.selectChild(tab);
                }
            }
        },
        _onCopyOk: function (event) {
            if (this.copyForm.validate()) {
                var context = this;
                this.logicalFile.copy({
                    request: domForm.toObject(this.id + "CopyForm")
                }).then(function (response) {
                    context._handleResponse("CopyResponse.result", response);
                });
                registry.byId(this.id + "CopyDropDown").closeDropDown();
            }
        },
        _onRenameOk: function (event) {
            if (this.renameForm.validate()) {
                var context = this;
                this.logicalFile.rename({
                    request: domForm.toObject(this.id + "RenameForm")
                }).then(function (response) {
                    context._handleResponse("RenameResponse.wuid", response);
                });
                registry.byId(this.id + "RenameDropDown").closeDropDown();
            }
        },
        _onDesprayOk: function (event) {
            if (this.desprayForm.validate()) {
                var context = this;
                var request = domForm.toObject(this.id + "DesprayForm");
                request.destPath = this.desprayTargetPath.getDropZoneFolder();
                if (!context.endsWith(request.destPath, "/")) {
                    request.destPath += "/";
                }
                request.destPath += registry.byId(this.id + "DesprayTargetName").get("value");
                this.logicalFile.despray({
                    request: request
                }).then(function (response) {
                    context._handleResponse("DesprayResponse.wuid", response);
                });
                registry.byId(this.id + "DesprayDropDown").closeDropDown();
            }
        },

        _onReplicateOk: function (event) {
            if (this.replicateForm.validate()) {
                var context = this;
                var request = domForm.toObject(this.id + "ReplicateForm");
                FileSpray.Replicate({
                    request: request
                }).then(function (response) {
                    context._handleResponse("ReplicateResponse.wuid", response);
                });
                registry.byId(this.id + "ReplicateDropDown").closeDropDown();
            }
        },

        //  Implementation  ---
        init: function (params) {
            if (this.inherited(arguments))
                return;

            var context = this;
            if (params.Name) {
                this.logicalFile = ESPLogicalFile.Get(params.NodeGroup, params.Name);
                var data = this.logicalFile.getData();
                for (var key in data) {
                    this.updateInput(key, null, data[key]);
                }
                this.logicalFile.watch(function (name, oldValue, newValue) {
                    context.updateInput(name, oldValue, newValue);
                });
                this.replicateSourceLogicalFile.set("value", params.Name);
            }
            this.copyTargetSelect.init({
                Groups: true
            });
            this.desprayTargetPath.init({
                DropZoneFolders: true
            });
            this.replicateTargetSelect.init({
                Groups: true
            });
            this.logicalFile.refresh();

            this.isProtected.on("change", function (evt) {
                context._onSave();
            });

            this.isRestricted.on("change", function (evt) {
                context._onSave();
            });
        },

        initTab: function () {
            var currSel = this.getSelectedChild();
            if (currSel && !currSel.initalized) {
                if (currSel.id === this.summaryWidget.id) {
                } else if (currSel.id === this.contentWidget.id) {
                    this.contentWidget.init({
                        NodeGroup: this.logicalFile.NodeGroup,
                        LogicalName: this.logicalFile.Name
                    });
                } else if (currSel.id === this.dataPatternsWidget.id) {
                    this.dataPatternsWidget.init({
                        NodeGroup: this.logicalFile.NodeGroup,
                        LogicalName: this.logicalFile.Name
                    });
                } else if (currSel.id === this.sourceWidget.id) {
                    this.sourceWidget.init({
                        ECL: this.logicalFile.Ecl
                    });
                } else if (currSel.id === this.defWidget.id) {
                    var context = this;
                    this.logicalFile.fetchDEF(function (response) {
                        context.defWidget.init({
                            ECL: response
                        });
                    });
                } else if (currSel.id === this.xmlWidget.id) {
                    var context = this;
                    this.logicalFile.fetchXML(function (response) {
                        context.xmlWidget.init({
                            ECL: response
                        });
                    });
                } else if (currSel.id === this.filePartsWidget.id) {
                    this.filePartsWidget.init({
                        fileParts: lang.exists("logicalFile.DFUFileParts.DFUPart", this) ? this.logicalFile.DFUFileParts.DFUPart : []
                    });
                } else if (currSel.id === this.widget._Queries.id && !this.widget._Queries.__hpcc_initalized) {
                    this.widget._Queries.init({
                        LogicalName: this.logicalFile.Name
                    });
                } else if (currSel.id === this.widget._Graphs.id && !this.widget._Graphs.__hpcc_initalized) {
                    this.widget._Graphs.init({
                        NodeGroup: this.logicalFile.NodeGroup,
                        LogicalName: this.logicalFile.Name
                    });
                } else if (this.workunitWidget && currSel.id === this.workunitWidget.id) {
                    this.workunitWidget.init({
                        Wuid: this.logicalFile.Wuid
                    });
                } else if (this.dfuWorkunitWidget && currSel.id === this.dfuWorkunitWidget.id) {
                    this.dfuWorkunitWidget.init({
                        Wuid: this.logicalFile.Wuid
                    });
                } else if (currSel.id === this.fileBelongsToWidget.id) {
                    this.fileBelongsToWidget.init({
                        NodeGroup: this.logicalFile.NodeGroup,
                        Name: this.logicalFile.Name
                    });
                } else if (currSel.id === this.fileHistoryWidget.id) {
                    this.fileHistoryWidget.init({
                        Name: this.logicalFile.Name
                    });
                } else if (currSel.id === this.fileBloomsWidget.id) {
                    this.fileBloomsWidget.init({
                        Name: this.logicalFile.Name
                    });
                } else {
                    currSel.init(currSel.params);
                }
            }
        },

        showMessage: function (msg) {
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
                            dom.byId(this.id + name).textContent = newValue;
                            break;
                        case "INPUT":
                        case "TEXTAREA":
                            domAttr.set(this.id + name, "value", newValue);
                            break;
                        default:
                            alert(domElem.tagName);
                    }
                }
            }
            if (name === "Wuid") {
                if (!newValue) {
                    this.removeChild(this.workunitWidget);
                    this.workunitWidget = null;
                    this.removeChild(this.dfuWorkunitWidget);
                    this.dfuWorkunitWidget = null;
                } else if (this.workunitWidget && newValue[0] === "D") {
                    this.removeChild(this.workunitWidget);
                    this.workunitWidget = null;
                } else if (this.dfuWorkunitWidget) {
                    this.removeChild(this.dfuWorkunitWidget);
                    this.dfuWorkunitWidget = null;
                }
                this.contentWidget.reset();
                this.sourceWidget.reset();
                this.defWidget.reset();
                this.xmlWidget.reset();
                this.filePartsWidget.reset();
                this.widget._Queries.reset();
                this.widget._Graphs.reset();
                if (this.workunitWidget) {
                    this.workunitWidget.reset();
                }
                if (this.dfuWorkunitWidget) {
                    this.dfuWorkunitWidget.reset();
                }
                this.fileBelongsToWidget.reset();
                this.fileHistoryWidget.reset();
                this.fileBloomsWidget.reset();
            } else if (name === "Name") {
                this.updateInput("RenameSourceName", oldValue, newValue);
                this.updateInput("RenameTargetName", oldValue, newValue);
                this.updateInput("DespraySourceName", oldValue, newValue);
                this.updateInput("CopySourceName", oldValue, newValue);
                this.updateInput("CopyTargetName", oldValue, newValue);
            } else if (name === "ProtectList") {
                dom.byId(this.id + "ProtectedImage").src = this.logicalFile.getProtectedImage();
            } else if (name === "IsCompressed") {
                dom.byId(this.id + "CompressedImage").src = this.logicalFile.getCompressedImage();
            } else if (name === "IsProtected") {
                this.updateInput("isProtected", oldValue, newValue);
            } else if (name === "IsRestricted") {
                this.updateInput("isRestricted", oldValue, newValue);
            } else if (name === "Ecl" && newValue) {
                this.setDisabled(this.id + "_Source", false);
                this.setDisabled(this.id + "_DEF", false);
                this.setDisabled(this.id + "_XML", false);
            } else if (name === "StateID") {
                this.summaryWidget.set("iconClass", this.logicalFile.getStateIconClass());
                domClass.remove(this.id + "StateIdImage");
                domClass.add(this.id + "StateIdImage", this.logicalFile.getStateIconClass());
                domAttr.set(this.id + "Name", "innerHTML", this.logicalFile.Name + (this.logicalFile.isDeleted() ? " (" + this.i18n.Deleted + ")" : ""));
            } else if (name === "Superfiles") {
                this.fileBelongsToWidget.set("title", this.i18n.Superfile + " (" + newValue.DFULogicalFile.length + ")");
                var superOwner = [];
                if (newValue.DFULogicalFile.length > 0) {
                    this.setDisabled(this.id + "_FileBelongs", false);
                    for (var i = 0; i < newValue.DFULogicalFile.length; ++i) {
                        superOwner.push(newValue.DFULogicalFile[i].Name);
                        this.updateInput("SuperOwner", oldValue, superOwner);
                    }
                }
            } else if (name === "__hpcc_changedCount" && newValue > 0) {
                this.refreshActionState();
                //  Force Icon to Show (I suspect its not working due to Circular Reference Loading)
                this.queriesWidget.set("iconClass", "dijitInline dijitIcon dijitTabButtonIcon iconFind");
            } else if (name === "DFUFilePartsOnClusters") {
                // Currently only checking first cluster may add loop through clusters and add a tab at a later date
                if (lang.exists("DFUFilePartsOnCluster", newValue) && newValue.DFUFilePartsOnCluster.length) {
                    this.updateInput("DFUFilePartsOnClusters", oldValue, newValue.DFUFilePartsOnCluster[0].Replicate);
                }
            } else if (name === "RecordSize" && newValue === "0") {
                this.updateInput("RecordSize", oldValue, this.i18n.NoPublishedSize);
            }
        },

        ensurePane: function (id, params) {
            id = this.createChildTabID(id);
            var retVal = registry.byId(id);
            if (!retVal) {
                var context = this;
                retVal = new DelayLoadWidget({
                    id: id,
                    title: params.Wuid,
                    closable: true,
                    delayWidget: "DFUWUDetailsWidget",
                    params: params
                });
                this.addChild(retVal);
            }
            return retVal;
        },

        refreshActionState: function () {
            this.setDisabled(this.id + "Save", this.logicalFile.isDeleted());
            this.setDisabled(this.id + "Delete", this.logicalFile.isDeleted());
            this.setDisabled(this.id + "CopyDropDown", this.logicalFile.isDeleted());
            this.setDisabled(this.id + "RenameDropDown", this.logicalFile.isDeleted());
            this.setDisabled(this.id + "DesprayDropDown", this.logicalFile.isDeleted());
            this.setDisabled(this.id + "_Content", this.logicalFile.isDeleted() || !this.logicalFile.Ecl);
            this.setDisabled(this.id + "_DataPatterns", this.logicalFile.isDeleted() || !DataPatternsWidget.supportedFileType(this.logicalFile.ContentType));
            this.setDisabled(this.id + "_Source", this.logicalFile.isDeleted() || !this.logicalFile.Ecl);
            this.setDisabled(this.id + "_DEF", this.logicalFile.isDeleted() || !this.logicalFile.Ecl);
            this.setDisabled(this.id + "_XML", this.logicalFile.isDeleted() || !this.logicalFile.Ecl);
            this.setDisabled(this.id + "_FileBelongs", this.logicalFile.isDeleted() || !this.logicalFile.Superfiles);
            this.setDisabled(this.id + "_FileParts", this.logicalFile.isDeleted());
            this.setDisabled(this.id + "_Queries", this.logicalFile.isDeleted());
            this.setDisabled(this.id + "_Graphs", this.logicalFile.isDeleted() || !this.logicalFile.Graphs);
            this.setDisabled(this.id + "_Workunit", this.logicalFile.isDeleted());
            this.setDisabled(this.id + "_DFUWorkunit", this.logicalFile.isDeleted());
            this.setDisabled(this.id + "ReplicateDropDown", !this.logicalFile.CanReplicateFlag || this.logicalFile.ReplicateFlag === false);

            if (this.params.Name) {
                var nameParts = this.params.Name.split("::");
                if (nameParts.length) {
                    var filename = nameParts[nameParts.length - 1];
                    registry.byId(this.id + "DesprayTargetName").set("value", filename);
                }
            }
        }
    });
});
