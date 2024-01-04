/* eslint-disable @typescript-eslint/no-unsafe-declaration-merging */
import * as dom from "dojo/dom";
import * as domClass from "dojo/dom-class";
import * as domForm from "dojo/dom-form";
import nlsHPCC from "./nlsHPCC";

import * as registry from "dijit/registry";

import { select as d3Select } from "@hpcc-js/common";
import { Workunit } from "@hpcc-js/comms";
import { DPWorkunit } from "./DataPatterns/DPWorkunit";
import { Report } from "./DataPatterns/Report";
import { getStateIconClass } from "./ESPWorkunit";

// @ts-ignore
import * as _TabContainerWidget from "hpcc/_TabContainerWidget";
// @ts-ignore
import * as DelayLoadWidget from "hpcc/DelayLoadWidget";

// @ts-ignore
import * as template from "dojo/text!hpcc/templates/DataPatternsWidget.html";

import "dijit/Fieldset";
import "dijit/form/Button";
import "dijit/form/CheckBox";
import "dijit/form/DropDownButton";
import "dijit/form/Form";
import "dijit/layout/BorderContainer";
import "dijit/layout/ContentPane";
import "dijit/layout/TabContainer";
import "dijit/Toolbar";
import "dijit/ToolbarSeparator";
import "dijit/TooltipDialog";
import "hpcc/TableContainer";
import "hpcc/TargetSelectWidget";

import { declareDecorator } from "./DeclareDecorator";
import { WUStatus } from "./WUStatus";

type _TabContainerWidget = {
    id: string;
    widget: any;
    params: { [key: string]: any };
    inherited(args: any);
    setDisabled(id: string, disabled: boolean, icon?: string, disabledIcon?: string);
    getSelectedChild(): any;
    createChildTabID(id: string): string;
    addChild(tabItem: any, pos: number);
};

export const supportedFileType = (contentType: string): boolean => ["flat", "csv", "thor"].indexOf((contentType || "").toLowerCase()) >= 0;

export interface DataPatternsWidget extends _TabContainerWidget {
}

@declareDecorator("DataPatternsWidget", _TabContainerWidget)
export class DataPatternsWidget {
    templateString = template;
    static baseClass = "DataPatternsWidget";
    i18n = nlsHPCC;

    summaryWidget;
    rawDataWidget;
    workunitWidget;
    centerContainer;
    targetSelectWidget;
    optimizeForm;
    optimizeTargetSelect;
    optimizeTarget;

    wuStatus: WUStatus;
    dpReport: Report;

    _wu: Workunit;
    _dpWu: DPWorkunit;

    constructor() {
    }

    //  Data ---
    //  --- ---

    buildRendering(args) {
        this.inherited(arguments);
    }

    postCreate(args) {
        this.inherited(arguments);

        this.summaryWidget = registry.byId(this.id + "_Summary");
        this.rawDataWidget = registry.byId(this.id + "_RawData");
        this.workunitWidget = registry.byId(this.id + "_Workunit");
        this.centerContainer = registry.byId(this.id + "CenterContainer");
        this.targetSelectWidget = registry.byId(this.id + "TargetSelect");

        this.optimizeForm = registry.byId(this.id + "OptimizeForm");
        this.optimizeTargetSelect = registry.byId(this.id + "OptimizeTargetSelect");
        this.optimizeTarget = registry.byId(this.id + "OptimizeTarget");

        const context = this;
        const origResize = this.centerContainer.resize;
        this.centerContainer.resize = function (s) {
            origResize.apply(this, arguments);
            d3Select(`#${context.id}DPReport`).style("height", `${s.h - 16}px`);  // 8 * 2 margins
            if (context.dpReport && context.dpReport.renderCount()) {
                context.dpReport
                    .resize()
                    .lazyRender()
                    ;
            }
        };

        this.wuStatus = new WUStatus()
            .baseUrl("")
            ;
        this.dpReport = new Report();
    }

    startup(args) {
        this.inherited(arguments);
    }

    resize(s) {
        this.inherited(arguments);
    }

    layout(args) {
        this.inherited(arguments);
    }

    destroy(args) {
        this.inherited(arguments);
    }

    //  Implementation  ---
    _onRefresh() {
        this.refreshData(true);
    }

    _onAnalyze() {
        const target = this.targetSelectWidget.get("value");
        this._dpWu.create(target).then(() => {
            this.refreshData();
        });
    }

    _onOptimizeOk() {
        if (this.optimizeForm.validate()) {
            const request = domForm.toObject(this.optimizeForm.domNode);
            this._dpWu.optimize(request.target, request.name, request.overwrite === "on").then(wu => {
                this.ensureWUPane(wu.Wuid);
            });
        }
        registry.byId(this.id + "OptimizeDropDown").closeDropDown();
    }

    _onDelete() {
        this._dpWu.delete().then(() => {
            this.rawDataWidget.reset();
            this.workunitWidget.reset();
            this.refreshData();
        });
    }

    init(params) {
        if (this.inherited(arguments))
            return;

        this.targetSelectWidget.init({});
        this.optimizeTargetSelect.init({});
        this.optimizeTarget.set("value", params.LogicalName + "::optimized");

        this._dpWu = new DPWorkunit(params.NodeGroup, params.LogicalName, params.Modified);

        this.wuStatus.target(this.id + "WUStatus");
        this.dpReport.target(this.id + "DPReport");

        this.refreshData();
    }

    initTab() {
        const currSel = this.getSelectedChild();
        if (currSel && !currSel.initalized) {
            if (currSel.id === this.summaryWidget.id) {
            } else if (this.rawDataWidget && currSel.id === this.rawDataWidget.id) {
                if (this._wu) {
                    this.rawDataWidget.init({
                        Wuid: this._wu.Wuid,
                        Name: "profileResults"
                    });
                }
            } else if (this.workunitWidget && currSel.id === this.workunitWidget.id) {
                if (this._wu) {
                    this.workunitWidget.init({
                        Wuid: this._wu.Wuid
                    });
                }
            } else {
                currSel.init(currSel.params);
            }
        }
    }

    ensureWUPane(wuid: string) {
        const id = this.createChildTabID(wuid);
        let retVal = registry.byId(id);
        if (!retVal) {
            retVal = new DelayLoadWidget({
                id,
                title: wuid,
                closable: true,
                delayWidget: "WUDetailsWidget",
                params: { Wuid: wuid }
            });
            this.addChild(retVal, 3);
        }
        return retVal;
    }

    refreshData(full: boolean = false) {
        if (full) {
            this._dpWu.clearCache();
        }
        this._dpWu.resolveWU().then(wu => {
            if (this._wu !== wu) {
                this._wu = wu;
                dom.byId(this.id + "Wuid").textContent = this._wu ? this._wu.Wuid : "";
                this.wuStatus
                    .wuid(this._wu ? this._wu.Wuid : "")
                    .lazyRender()
                    ;

                if (this._wu) {
                    this._wu.watchUntilComplete(changes => {
                        if (this._wu && this._wu.isComplete()) {
                            this.dpReport
                                .visible(true)
                                .wu(this._dpWu)
                                .render(w => {
                                    w
                                        .resize()
                                        .render()
                                        ;
                                });
                        }
                        this.refreshActionState();
                    });
                } else {
                    this.dpReport
                        .visible(false)
                        .render()
                        ;
                }
            }
            this.refreshActionState();
        });
    }

    refreshActionState() {
        const isComplete = this._wu && this._wu.isComplete();
        this.setDisabled(this.id + "Analyze", !!this._wu);
        d3Select(`#${this.id}TargetSelectLabel`).style("color", this._wu ? "rgb(158,158,158)" : null);
        this.setDisabled(this.id + "TargetSelect", !!this._wu);
        this.setDisabled(this.id + "Delete", !this._wu);
        this.setDisabled(this.id + "OptimizeDropDown", !isComplete);
        this.setDisabled(this.id + "_RawData", !isComplete);
        this.setDisabled(this.id + "_Workunit", !this._wu);

        if (this._wu) {
            this.targetSelectWidget.set("value", this._wu.Cluster);
        }

        const stateIconClass = this._wu ? getStateIconClass(this._wu.StateID, this._wu.isComplete(), this._wu.Archived) : "";
        this.workunitWidget.set("iconClass", stateIconClass);
        domClass.remove(this.id + "StateIdImage");
        domClass.add(this.id + "StateIdImage", stateIconClass);

        d3Select(`#${this.id}WU`).style("display", isComplete ? "none" : null);

        const msg = [];
        if (!this._wu) {
            msg.push(this.i18n.DataPatternsNotStarted);
        } else if (!this._wu.isComplete()) {
            msg.push(this.i18n.DataPatternsStarted);
        }

        const dpMessage = d3Select(`#${this.id + "DPReport"}`).selectAll(".DPMessage").data(msg);
        dpMessage.enter().insert("p", ":first-child")
            .attr("class", "DPMessage")
            .style("text-align", "center")
            .merge(dpMessage as any)
            .text(d => d)
            ;
        dpMessage.exit()
            .remove()
            ;
    }
}
