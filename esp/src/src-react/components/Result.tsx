import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "./CommandBarV9";
import { Button, Checkbox, Dialog, DialogActions, DialogBody, DialogContent, DialogOpenChangeData, DialogOpenChangeEvent, DialogSurface, DialogTitle, Field, MessageBar, MessageBarActions, MessageBarBody, MessageBarIntent, SpinButton, SpinButtonChangeEvent, SpinButtonOnChangeData, Spinner } from "@fluentui/react-components";
import { DismissRegular } from "@fluentui/react-icons";
import { useConst } from "@fluentui/react-hooks";
import { Result as CommsResult, XSDXMLNode } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import { WUResult } from "@hpcc-js/eclwatch";
import nlsHPCC from "src/nlsHPCC";
import { csvEncode } from "src/Utility";
import { ReactRoot } from "src/react/render";
import { useWorkunit, useMyAccount, useConfirm } from "../hooks/index";
import { HolyGrail } from "../layouts/HolyGrail";
import { AutosizeHpccJSComponent } from "../layouts/HpccJSAdapter";
import { pushParams, replaceUrl } from "../util/history";
import { getESPBaseURL } from "../util/espUrl";
import { Fields } from "./forms/Fields";
import { Filter } from "./forms/Filter";

import "src-react-css/components/DojoGrid.css";

const logger = scopedLogger("src-react/components/Result.tsx");

function eclTypeTPL(type: string, isSet: boolean) {
    const prefix = isSet ? "SET OF " : "";
    switch (type) {
        case "xs:boolean":
            return prefix + "BOOLEAN";
        case "xs:integer":
            return prefix + "INTEGER";
        case "xs:nonNegativeInteger":
            return prefix + "UNSIGNED INTEGER";
        case "xs:real":
            return prefix + "REAL";
        case "xs:string":
            return prefix + "VARSTRING";
        case "xs:hexBinary":
            return prefix + "DATA";
        default:
            return prefix + type.toUpperCase();
    }
}

function valueTPL(value: string | number | boolean) {
    switch (typeof value) {
        case "string":
            return `'${value.split("'").join("\\'").trimEnd()}'`;
        case "number":
            return value;
        case "boolean":
            return value === true ? "TRUE" : "FALSE";
    }
}
function rowTPL(row: object) {
    return `{${Object.values(row).map(field => {
        if (field.Item) {
            return `[${field.Item.map(valueTPL).join(", ")}]`;
        }
        return valueTPL(field);
    }).join(", ")}}`;
}

function eclRowsTPL(row: object[], prefix = "    ") {
    return row.map(row => `${prefix}${rowTPL(row)}`).join(",\n");
}

function copyECLRowsTPL(fields: XSDXMLNode[], row: object[]) {
    return `
r := RECORD
${fields.map(f => `    ${eclTypeTPL(f.type, f.isSet)} ${f.name};`).join("\n")}
END;

d := DATASET([
${eclRowsTPL(row)}
], r);
`;
}

interface DownloadDialogProps {
    totalRows: number;
    column: boolean;
    onClose: (rowsToDownload: number, dedup: boolean) => void;
}

const DownloadDialog: React.FunctionComponent<DownloadDialogProps> = ({
    totalRows,
    column = false,
    onClose,
}) => {
    const stackStyle: React.CSSProperties = { display: "flex", flexDirection: "column", gap: "10px" };

    const [open, setOpen] = React.useState(true);
    const handleOk = () => {
        setOpen(false);
        onClose(downloadTotal, dedup);
    };
    const handleCancel = () => {
        setOpen(false);
        onClose(0, dedup);
    };
    const onOpenChange = (_: DialogOpenChangeEvent, data: DialogOpenChangeData) => {
        if (!data.open) handleCancel();
    };

    const [downloadTotal, setDownloadTotal] = React.useState(totalRows);
    const onDownloadTotalChange = (_ev: SpinButtonChangeEvent, data: SpinButtonOnChangeData) => {
        let v: number = data.value ?? parseInt(data.displayValue ?? "", 10);
        if (isNaN(v)) {
            v = totalRows;
        } else if (v < 0) {
            v = 0;
        } else if (v > totalRows) {
            v = totalRows;
        }
        setDownloadTotal(v);
    };

    const [dedup] = React.useState(true);
    const onDedup = (ev: React.ChangeEvent<HTMLInputElement>, data: { checked: boolean | "mixed" }) => {
    };

    return <Dialog open={open} modalType="modal" onOpenChange={onOpenChange}>
        <DialogSurface>
            <DialogBody>
                <DialogTitle>Download Results</DialogTitle>
                <DialogContent>
                    <p>{`Confirm total number of rows to download(max ${totalRows} rows).`}</p>
                    <div style={stackStyle}>
                        <Field label="Download:">
                            <SpinButton
                                defaultValue={totalRows}
                                min={0}
                                max={totalRows}
                                step={1}
                                incrementButton={{ "aria-label": "Increase value by 1" }}
                                decrementButton={{ "aria-label": "Decrease value by 1" }}
                                onChange={onDownloadTotalChange}
                            />
                        </Field>
                        {column ?
                            <Checkbox label="De-duplicate" defaultChecked onChange={onDedup} /> :
                            undefined}
                    </div>
                </DialogContent>
                <DialogActions>
                    <Button appearance="primary" onClick={handleOk}>Ok</Button>
                    <Button onClick={handleCancel}>Cancel</Button>
                </DialogActions>
            </DialogBody>
        </DialogSurface>
    </Dialog>;
};

class ResultWidget extends WUResult {

    reset() {
        delete this._prevResultHash;
        delete this._prevStoreHash;
        delete this._prevQueryHash;
    }

    confirmDownload(column: boolean = false): Promise<{ downloadTotal: number, dedup: boolean }> {
        if (!column && this._result.Total <= 1000) return Promise.resolve({ downloadTotal: this._result.Total, dedup: false });
        return new Promise<{ downloadTotal: number, dedup: boolean }>(resolve => {
            const div = document.createElement("div");
            document.body.appendChild(div);
            const root = ReactRoot.create(div);
            root.themedRender(DownloadDialog, {
                totalRows: this._result.Total,
                column: column,
                onClose: (downloadTotal: number, dedup: boolean) => {
                    resolve({ downloadTotal, dedup });
                    root.dispose();
                    div.remove();
                }
            });
        });
    }

    async copyAsCSV() {
        const copyCSVHeaderTPL = (fields: XSDXMLNode[]) => `${fields.map(f => `${csvEncode(f.name)}`).join("\t")}`;
        const copyCSVRowTPL = (row: object) => `${Object.values(row).map(cell => `${csvEncode(cell)}`).join("\t")}`;
        const copyCSVRowsTPL = (rows: object[]) => `${rows.map(copyCSVRowTPL).join("\n")}`;

        const { downloadTotal } = await this.confirmDownload();
        if (downloadTotal > 0) {
            this.fetch(0, downloadTotal).then(rows => {
                const tsv = `\
${copyCSVHeaderTPL(this._result.fields())}
${copyCSVRowsTPL(rows)} \
`;
                navigator?.clipboard?.writeText(tsv);
            });
        }
    }

    async copyAsECL() {
        const { downloadTotal } = await this.confirmDownload();
        if (downloadTotal > 0) {
            this.fetch(0, downloadTotal).then(rows => {
                const tsv = copyECLRowsTPL(this._result.fields(), rows);
                navigator?.clipboard?.writeText(tsv);
            });
        }
    }
}

interface doDownloadOpts {
    type: string;
    wuid?: string;
    resultName?: string;
    sequence?: number;
    logicalName?: string;
}

function doDownload(opts: doDownloadOpts) {
    const { type, wuid, resultName, sequence, logicalName } = { ...opts };
    if (wuid && resultName) {
        window.open(getESPBaseURL() + "/WUResultBin?Format=" + type + "&Wuid=" + wuid + "&ResultName=" + resultName, "_blank");
    } else if (wuid && sequence !== undefined) {
        window.open(getESPBaseURL() + "/WUResultBin?Format=" + type + "&Wuid=" + wuid + "&Sequence=" + sequence, "_blank");
    } else if (logicalName) {
        window.open(getESPBaseURL() + "/WUResultBin?Format=" + type + "&LogicalName=" + logicalName, "_blank");
    }
}

interface MessageBarContent {
    type: MessageBarIntent;
    message: string;
}

interface ResultProps {
    wuid?: string;
    resultName?: string;
    logicalFile?: string;
    cluster?: string;
    filter?: { [key: string]: any };
    hasEcl?: boolean;
}

const emptyFilter: { [key: string]: any } = {};

export const Result: React.FunctionComponent<ResultProps> = ({
    wuid,
    resultName,
    logicalFile,
    cluster,
    filter = emptyFilter,
    hasEcl
}) => {

    const hasFilter = React.useMemo(() => Object.keys(filter).length > 0, [filter]);
    const [renderHTML, setRenderHTML] = React.useState(false);

    const resultTable: ResultWidget = useConst(() => new ResultWidget()
        .baseUrl("")
        .pagination(true)
        .pageSize(50) as ResultWidget
    );

    const { currentUser } = useMyAccount();
    const { workunit: wu } = useWorkunit(wuid);
    const [result, setResult] = React.useState<CommsResult>(resultTable.calcResult());
    const [FilterFields, setFilterFields] = React.useState<Fields>({});
    const [loading, setLoading] = React.useState(true);
    const [espReturnedError, setEspReturnedError] = React.useState(false);
    const [showFilter, setShowFilter] = React.useState(false);

    const [messageBarContent, setMessageBarContent] = React.useState<MessageBarContent | undefined>();
    const dismissMessageBar = React.useCallback(() => setMessageBarContent(undefined), []);

    React.useEffect(() => {
        resultTable
            .wuid(wuid)
            .resultName(resultName)
            .nodeGroup(cluster)
            .logicalFile(logicalFile)
            .filter(filter)
            .renderHtml(renderHTML)
            .render(() => setResult(resultTable.calcResult()))
            ;
    }, [cluster, filter, logicalFile, renderHTML, resultName, resultTable, wuid]);

    React.useEffect(() => {
        resultTable.filter(filter);
    }, [filter, resultTable]);

    React.useEffect(() => {
        result?.fetchXMLSchema().then(() => {
            const filterFields: Fields = {};
            const fields = result.fields();
            fields.forEach(field => {
                filterFields[field.name] = {
                    type: "string",
                    label: field.name
                };
            });
            setFilterFields(filterFields);
            setLoading(false);
            setEspReturnedError(false);
        }).catch(err => {
            logger.error(err);
            setEspReturnedError(true);
            setMessageBarContent({ type: "error", message: `${nlsHPCC.Error} ${nlsHPCC.fetchingresults}` });
            setLoading(false);
            if (err.message.indexOf("Cannot open the workunit result") > -1) {
                replaceUrl(`/workunits/${wuid}/outputs/`);
            }
        });
    }, [result, wuid]);

    //  Filter  ---
    const [filterFields, hasHtml] = React.useMemo(() => {
        const filterFields: Fields = {};
        let hasHtml = false;
        for (const fieldID in FilterFields) {
            filterFields[fieldID] = { ...FilterFields[fieldID], value: filter[fieldID] };
            if (fieldID.indexOf("__html") >= 0) {
                hasHtml = true;
            }
        }
        return [filterFields, hasHtml];
    }, [FilterFields, filter]);

    React.useEffect(() => {
        if (logicalFile) {
            if (hasEcl === false) {
                setMessageBarContent({ type: "warning", message: nlsHPCC.ECLLayoutNotAvailable });
            } else {
                if (messageBarContent?.type === "warning" && messageBarContent?.message === nlsHPCC.ECLLayoutNotAvailable) {
                    setMessageBarContent(undefined);
                }
            }
        }
    }, [hasEcl, logicalFile, messageBarContent?.message, messageBarContent?.type]);

    const securityMessageHTML = React.useMemo(() => nlsHPCC.SecurityMessageHTML.split("{__placeholder__}").join(wu?.Owner ? wu?.Owner : nlsHPCC.Unknown).split("\n"), [wu?.Owner]);
    const [ViewHTMLConfirm, showViewHTMLConfirm] = useConfirm({
        title: nlsHPCC.SecurityWarning,
        message: securityMessageHTML[0],
        items: securityMessageHTML.slice(1),
        onSubmit: React.useCallback(() => {
            setRenderHTML(true);
            showViewHTMLConfirm(false);
            // eslint-disable-next-line react-hooks/exhaustive-deps
        }, [])
    });

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => {
                resultTable.bypassCache(true);
                try {
                    resultTable.render(() => setResult(resultTable.calcResult()));
                } finally {
                    resultTable.bypassCache(false);
                }
            }
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider },
        {
            key: "filter", text: nlsHPCC.Filter, iconProps: { iconName: hasFilter ? "FilterSolid" : "Filter" },
            onClick: () => {
                setShowFilter(true);
            }
        },
        { key: "divider_2", itemType: ContextualMenuItemType.Divider },
        {
            key: "html", text: nlsHPCC.HTML, iconProps: { iconName: "FileHTML" }, canCheck: true, checked: renderHTML, disabled: !hasHtml,
            onClick: (ev, item) => {
                if (!renderHTML) {
                    if (!currentUser?.username || (currentUser?.username !== wu?.Owner)) {
                        showViewHTMLConfirm(true);
                    } else {
                        setRenderHTML(true);
                    }
                } else {
                    setRenderHTML(false);
                }
            }
        }
    ], [currentUser?.username, hasFilter, hasHtml, renderHTML, resultTable, showViewHTMLConfirm, wu?.Owner]);

    const rightButtons: ICommandBarItemProps[] = [
        {
            key: "copy", text: nlsHPCC.CopyToClipboard, iconOnly: true, iconProps: { iconName: "Copy" },
            subMenuProps: {
                items: [
                    { key: "tsv", text: nlsHPCC.CSV, onClick: () => resultTable.copyAsCSV() },
                    { key: "ecl", text: nlsHPCC.ECL, onClick: () => resultTable.copyAsECL() }
                ]
            }
        },
        {
            key: "download", text: nlsHPCC.DownloadToCSV, iconOnly: true, iconProps: { iconName: "Download" },
            subMenuProps: {
                items: [
                    { key: "zip", text: nlsHPCC.Zip, onClick: () => doDownload({ type: "zip", wuid, resultName: result.ResultName, sequence: result.Sequence, logicalName: result.LogicalFileName }) },
                    { key: "gzip", text: nlsHPCC.GZip, onClick: () => doDownload({ type: "gzip", wuid, resultName: result.ResultName, sequence: result.Sequence, logicalName: result.LogicalFileName }) },
                    { key: "json", text: nlsHPCC.JSON, onClick: () => doDownload({ type: "json", wuid, resultName: result.ResultName, sequence: result.Sequence, logicalName: result.LogicalFileName }) },
                    { key: "csv", text: nlsHPCC.CSV, title: nlsHPCC.DownloadToCSVNonFlatWarning, onClick: () => doDownload({ type: "csv", wuid, resultName: result.ResultName, sequence: result.Sequence, logicalName: result.LogicalFileName }) },
                ]
            }
        }
    ];

    return <HolyGrail
        header={<>
            <CommandBar items={buttons} farItems={rightButtons} />
            {messageBarContent &&
                <MessageBar intent={messageBarContent.type}>
                    <MessageBarBody>{messageBarContent.message}</MessageBarBody>
                    <MessageBarActions containerAction={<Button onClick={dismissMessageBar} aria-label={nlsHPCC.Close} appearance="transparent" icon={<DismissRegular />} />} />
                </MessageBar>
            }
        </>}
        main={
            <>
                {loading ?
                    <Spinner label={nlsHPCC.Loading} /> :
                    espReturnedError ?
                        <></> :
                        <AutosizeHpccJSComponent widget={resultTable} />
                }
                <Filter showFilter={showFilter} setShowFilter={setShowFilter} filterFields={filterFields} onApply={pushParams} />
                <ViewHTMLConfirm />
            </>
        }
    />;
};
