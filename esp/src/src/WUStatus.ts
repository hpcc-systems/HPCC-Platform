/* eslint-disable @typescript-eslint/no-unsafe-declaration-merging */
import { Workunit, WUStateID } from "@hpcc-js/comms";
import { Edge, Graph, Vertex } from "@hpcc-js/graph";
import { hashSum, IObserverHandle } from "@hpcc-js/util";
import nlsHPCC from "./nlsHPCC";

export class WUStatus extends Graph {

    protected _hpccWU: Workunit | undefined;
    protected _hpccWatchHandle: IObserverHandle;

    protected _create: Vertex;
    protected _compile: Vertex;
    protected _execute: Vertex;
    protected _complete: Vertex;

    constructor() {
        super();
        this
            .zoomable(false)
            .zoomToFitLimit(1)
            .layout("Hierarchy")
            .hierarchyRankDirection("LR")
            .showToolbar(false)
            .allowDragging(false)
            ;
    }

    private _prevHash;
    attachWorkunit() {
        const hash = hashSum({
            baseUrl: this.baseUrl(),
            wuid: this.wuid()
        });
        if (this._prevHash !== hash) {
            this._prevHash = hash;
            const wuid = this.wuid();
            this._hpccWU = wuid ? Workunit.attach({ baseUrl: this.baseUrl() }, wuid) : undefined;
            this.stopMonitor();
            this.startMonitor();
        }
    }

    isMonitoring(): boolean {
        return !!this._hpccWatchHandle;
    }

    disableMonitor(disableMonitor: boolean) {
        if (disableMonitor) {
            this.stopMonitor();
        } else {
            this.startMonitor();
        }
        this.lazyRender();
    }

    startMonitor() {
        if (!this._hpccWU || this.isMonitoring())
            return;

        this._hpccWatchHandle = this._hpccWU.watch(changes => {
            this.lazyRender();
        }, true);
    }

    stopMonitor() {
        if (this._hpccWatchHandle) {
            this._hpccWatchHandle.release();
            delete this._hpccWatchHandle;
        }
    }

    createVertex(faChar: string) {
        return new Vertex()
            .icon_diameter(32)
            .icon_shape_colorFill("none")
            .icon_shape_colorStroke("none")
            .icon_image_colorFill("darkgray")
            .iconAnchor("middle")
            .textbox_shape_colorFill("none")
            .textbox_shape_colorStroke("none")
            .textbox_text_colorFill("darkgray")
            .faChar(faChar)
            ;
    }

    updateVertex(vertex: Vertex, color: string) {
        vertex
            .icon_image_colorFill(color)
            .textbox_text_colorFill(color)
            ;
    }

    updateVertexStatus(level: 0 | 1 | 2 | 3 | 4, active: boolean = false) {
        const completeColor = this._hpccWU && this._hpccWU.isFailed() ? "darkred" : "darkgreen";
        this._create.text(nlsHPCC.Created);
        this._compile.text(nlsHPCC.Compiled);
        this._execute.text(nlsHPCC.Executed);
        this._complete.text(nlsHPCC.Completed);
        if (!this._hpccWatchHandle && level < 4) {
            level = 0;
        }
        switch (level) {
            case 0:
                this.updateVertex(this._create, "darkgray");
                this.updateVertex(this._compile, "darkgray");
                this.updateVertex(this._execute, "darkgray");
                this.updateVertex(this._complete, "darkgray");
                break;
            case 1:
                this._create.text(nlsHPCC.Creating);
                this.updateVertex(this._create, active ? "orange" : completeColor);
                this.updateVertex(this._compile, "darkgray");
                this.updateVertex(this._execute, "darkgray");
                this.updateVertex(this._complete, "darkgray");
                break;
            case 2:
                this._compile.text(nlsHPCC.Compiling);
                this.updateVertex(this._create, completeColor);
                this.updateVertex(this._compile, active ? "orange" : completeColor);
                this.updateVertex(this._execute, completeColor);
                this.updateVertex(this._complete, "darkgray");
                break;
            case 3:
                this._execute.text(nlsHPCC.Executing);
                this.updateVertex(this._create, completeColor);
                this.updateVertex(this._compile, completeColor);
                this.updateVertex(this._execute, active ? "orange" : completeColor);
                this.updateVertex(this._complete, "darkgray");
                break;
            case 4:
                this.updateVertex(this._create, completeColor);
                this.updateVertex(this._compile, completeColor);
                this.updateVertex(this._execute, completeColor);
                this.updateVertex(this._complete, completeColor);
                break;
        }
    }

    createEdge(source, target) {
        return new Edge()
            .sourceVertex(source)
            .targetVertex(target)
            .strokeColor("black")
            .showArc(false)
            ;
    }

    enter(domNode, element) {
        super.enter(domNode, element);
        this._create = this.createVertex("\uf11d");
        this._compile = this.createVertex("\uf085");
        this._execute = this.createVertex("\uf275");
        this._complete = this.createVertex("\uf11e");
        const e1 = this.createEdge(this._create, this._compile);
        const e2 = this.createEdge(this._compile, this._execute);
        const e3 = this.createEdge(this._execute, this._complete);
        this.data({
            vertices: [this._create, this._compile, this._execute, this._complete],
            edges: [e1, e2, e3]
        });
    }

    update(domNode, element) {
        this.attachWorkunit();
        switch (this._hpccWU ? this._hpccWU.StateID : WUStateID.Unknown) {
            case WUStateID.Blocked:
            case WUStateID.Wait:
            case WUStateID.Scheduled:
            case WUStateID.UploadingFiled:
                this.updateVertexStatus(1);
                break;
            case WUStateID.Compiling:
                this.updateVertexStatus(2, true);
                break;
            case WUStateID.Submitted:
                this.updateVertexStatus(1, true);
                break;
            case WUStateID.Compiled:
                this.updateVertexStatus(2);
                break;
            case WUStateID.Aborting:
            case WUStateID.Running:
                this.updateVertexStatus(3, true);
                break;
            case WUStateID.Aborted:
            case WUStateID.Archived:
            case WUStateID.Completed:
                this.updateVertexStatus(4);
                break;
            case WUStateID.Failed:
                this.updateVertexStatus(4, false);
                break;
            case WUStateID.DebugPaused:
            case WUStateID.DebugRunning:
            case WUStateID.Paused:
            case WUStateID.Unknown:
            default:
                this.updateVertexStatus(0);
                break;
        }
        super.update(domNode, element);
        this.zoomToFit();
    }

    exit(domNode, element) {
        if (this._hpccWatchHandle) {
            this._hpccWatchHandle.release();
        }
        super.exit(domNode, element);
    }
}
WUStatus.prototype._class += " eclwatch_WUStatus";

export interface WUStatus {
    baseUrl(): string;
    baseUrl(_: string): this;
    wuid(): string;
    wuid(_: string): this;
}

WUStatus.prototype.publish("baseUrl", "", "string", "HPCC Platform Base URL");
WUStatus.prototype.publish("wuid", "", "string", "Workunit ID");
