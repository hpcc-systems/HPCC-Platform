import { IScope } from "@hpcc-js/comms";
import { XMLNode, xml2json } from "@hpcc-js/util";
import nlsHPCC from "src/nlsHPCC";

export class Archive {
    build: string = "";
    eclVersion: string = "";
    legacyImport: string = "";
    legacyWhen: string = "";

    query: Query;

    modAttrs: Array<Module | Attribute> = [];
    modules: Module[] = [];
    attributes: Attribute[] = [];

    private attrId_Attribute: { [id: string]: Attribute } = {};
    private sourcePath_Attribute: { [path: string]: Attribute } = {};
    private sourcePath_Metrics: { [id: string]: Set<string> } = {};
    private sourcePath_TimeTotalExecute: { [path: string]: { total: number, line: { [no: number]: { total: number } } } } = {};
    private _metrics: any[] = [];

    private _timeTotalExecute = 0;
    get timeTotalExecute() { return this._timeTotalExecute; }

    constructor(xml: string) {
        const archiveJson = xml2json(xml);
        this.walkArchiveJson(archiveJson);
    }

    protected walkArchiveJson(node: XMLNode, parentModule?: Module, parentQualifiedId?: string, depth: number = 0) {
        let qualifiedId = "";
        if (parentQualifiedId && node.$?.key) {
            qualifiedId = `${parentQualifiedId}.${node.$?.key}`;
        } else if (node.$?.key) {
            qualifiedId = node.$?.key;
        }

        let module: Module;
        switch (node.name) {
            case "Archive":
                this.build = node.$.build;
                this.eclVersion = node.$.eclVersion;
                this.legacyImport = node.$.legacyImport;
                this.legacyWhen = node.$.legacyWhen;
                break;
            case "Query":
                this.query = new Query(node.$.attributePath, node.content.trim());
                break;
            case "Module":
                if (qualifiedId && node.children().length) {
                    module = new Module(parentQualifiedId, qualifiedId, node.$.name, depth);
                    this.modAttrs.push(module);
                    this.modules.push(module);
                    if (parentModule) {
                        parentModule.modules.push(module);
                    }
                }
                break;
            case "Attribute":
                const attribute = new Attribute(parentQualifiedId, qualifiedId, node.$.name, node.$.sourcePath, node.$.ts, node.content.trim(), depth);
                this.modAttrs.push(attribute);
                this.attributes.push(attribute);
                this.sourcePath_Attribute[attribute.sourcePath] = attribute;
                this.attrId_Attribute[attribute.id] = attribute;
                if (parentModule) {
                    parentModule.attributes.push(attribute);
                }
                break;
            default:
        }
        node.children().forEach(child => {
            this.walkArchiveJson(child, module, qualifiedId, depth + 1);
        });
        switch (node.name) {
            case "Archive":
                if (this.modAttrs.length === 0) {
                    const attribute = new Attribute("", "undefined", `<${nlsHPCC.undefined}>`, "", "", this.query.content, depth);
                    this.query = new Query("undefined", "");
                    this.modAttrs.push(attribute);
                    this.attributes.push(attribute);
                    this.attrId_Attribute[attribute.id] = attribute;
                }
                break;
            default:
        }
    }

    updateMetrics(metrics: any[]) {
        this._metrics = metrics;
        this.sourcePath_TimeTotalExecute = {};
        this._timeTotalExecute = metrics.filter(metric => !!metric.DefinitionList).reduce((prev, metric) => {
            const totalTime = metric.TimeMaxTotalExecute ?? metric.TimeAvgTotalExecute ?? metric.TimeTotalExecute ?? 0;
            metric.DefinitionList?.forEach((definition, idx) => {
                if (!this.sourcePath_TimeTotalExecute[definition.filePath]) {
                    this.sourcePath_TimeTotalExecute[definition.filePath] = { total: 0, line: {} };
                }
                this.sourcePath_TimeTotalExecute[definition.filePath].total += totalTime;
                const line = definition.line;
                if (!this.sourcePath_TimeTotalExecute[definition.filePath].line[line]) {
                    this.sourcePath_TimeTotalExecute[definition.filePath].line[line] = { total: 0 };
                }
                this.sourcePath_TimeTotalExecute[definition.filePath].line[line].total += totalTime ?? 0;

                if (!this.sourcePath_Metrics[definition.filePath]) {
                    this.sourcePath_Metrics[definition.filePath] = new Set<string>();
                }
                this.sourcePath_Metrics[definition.filePath].add(metric.id);
            });
            return prev + totalTime ?? 0;
        }, 0);
    }

    sourcePathTime(sourcePath: string) {
        return this.sourcePath_TimeTotalExecute[sourcePath]?.total ?? 0;
    }

    attribute(attrId: string) {
        return this.attrId_Attribute[attrId];
    }

    sourcePath(attrId: string) {
        const attr = this.attribute(attrId);
        return attr?.sourcePath ?? "";
    }

    content(attrId: string) {
        const attr = this.attribute(attrId);
        return attr?.content ?? "";
    }

    markers(id: string) {
        const retVal = [];
        const attr = this.attribute(id);
        if (!attr) return retVal;
        const fileTime = this.sourcePath_TimeTotalExecute[attr.sourcePath];
        if (!fileTime) return retVal;
        for (const lineNum in fileTime.line) {
            const label = Math.round((fileTime.line[lineNum].total / fileTime.total) * 100);
            retVal.push({
                lineNum,
                label
            });
        }
        return retVal;
    }

    queryId(): string {
        return this.query?.attributePath?.toLowerCase() ?? "";
    }

    metricIDs(attrId: string): string[] {
        const attr = this.attribute(attrId);
        if (!attr) return [];
        const set = this.sourcePath_Metrics[attr.sourcePath];
        if (!set) return [];
        return [...set.values()];
    }

    metrics(attrId: string): IScope[] {
        const metricIDs = this.metricIDs(attrId);
        return this._metrics.filter(metric => metricIDs.includes(metric.id));
    }
}

class Query {
    constructor(readonly attributePath: string, readonly content: string) {
    }
}

export class Module {
    readonly type: string = "Module";
    modules: Module[] = [];
    attributes: Attribute[] = [];

    constructor(readonly parentId: string, readonly id: string, readonly name: string, readonly depth: number) {
    }
}

export class Attribute {
    readonly type: string = "Attribute";
    constructor(readonly parentId: string, readonly id: string, readonly name: string, readonly sourcePath: string, readonly ts: string, readonly content: string, readonly depth: number) {
    }
}
export function isAttribute(obj: any): obj is Attribute {
    return obj.type === "Attribute";
}

export interface FileMetric {
    path: string;
    row: number;
    col: number;
    metric: object;
}

