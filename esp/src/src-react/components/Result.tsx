import * as React from "react";
import * as ReactDOM from "react-dom";
import { Checkbox, CommandBar, ContextualMenuItemType, DefaultButton, Dialog, DialogFooter, DialogType, ICommandBarItemProps, PrimaryButton, SpinButton, Stack } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import { Result as CommsResult, XSDXMLNode } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import { WUResult } from "@hpcc-js/eclwatch";
import nlsHPCC from "src/nlsHPCC";
import { ESPBase } from "src/ESPBase";
import { csvEncode } from "src/Utility";
import { useWorkunit, useMyAccount, useConfirm } from "../hooks/index";
import { HolyGrail } from "../layouts/HolyGrail";
import { AutosizeHpccJSComponent } from "../layouts/HpccJSAdapter";
import { pushParams } from "../util/history";
import { ShortVerticalDivider } from "./Common";
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
            return `'${value.split("'").join("\\'").trimRight()}'`;
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
    const dialogContentProps = {
        type: DialogType.largeHeader,
        title: "Download Results",
        subText: `Confirm total number of rows to download(max ${totalRows} rows).`,
    };
    const stackTokens = { childrenGap: 10 };

    const [hideDialog, setHideDialog] = React.useState(false);
    const handleOk = () => {
        setHideDialog(true);
        onClose(downloadTotal, dedup);
    };
    const handleCancel = () => {
        setHideDialog(true);
        onClose(0, dedup);
    };

    const [downloadTotal, setDownloadTotal] = React.useState(totalRows);
    const onDownloadTotalValidate = (value: string) => {
        let v: number = parseInt(value);
        if (isNaN(v)) {
            v = totalRows;
        } else if (v < 0) {
            v = 0;
        } else if (v > totalRows) {
            v = totalRows;
        }
        setDownloadTotal(v);
        return String(v);
    };

    const [dedup] = React.useState(true);
    const onDedup = (ev: React.FormEvent<HTMLElement>, isChecked: boolean) => {
    };

    return <Dialog
        hidden={hideDialog}
        onDismiss={handleCancel}
        dialogContentProps={dialogContentProps}
    >
        <Stack tokens={stackTokens}>
            <SpinButton
                defaultValue={`${totalRows} `}
                label={"Download:"}
                min={0}
                max={totalRows}
                step={1}
                incrementButtonAriaLabel={"Increase value by 1"}
                decrementButtonAriaLabel={"Decrease value by 1"}
                onValidate={onDownloadTotalValidate}
            />
            {column ?
                <Checkbox label="De-duplicate" boxSide="end" defaultChecked onChange={onDedup} /> :
                undefined}
        </Stack>
        <DialogFooter>
            <PrimaryButton onClick={handleOk} text="Ok" />
            <DefaultButton onClick={handleCancel} text="Cancel" />
        </DialogFooter>
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
        return new Promise(resolve => {
            const element = document.createElement("div");
            ReactDOM.render(<DownloadDialog
                totalRows={this._result.Total}
                column={column}
                onClose={(downloadTotal, dedup) => resolve({ downloadTotal, dedup })}
            />, element);
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
    const base = new ESPBase();
    const { type, wuid, resultName, sequence, logicalName } = { ...opts };
    if (wuid && resultName) {
        window.open(base.getBaseURL() + "/WUResultBin?Format=" + type + "&Wuid=" + wuid + "&ResultName=" + resultName, "_blank");
    } else if (wuid && sequence !== undefined) {
        window.open(base.getBaseURL() + "/WUResultBin?Format=" + type + "&Wuid=" + wuid + "&Sequence=" + sequence, "_blank");
    } else if (logicalName) {
        window.open(base.getBaseURL() + "/WUResultBin?Format=" + type + "&LogicalName=" + logicalName, "_blank");
    }
}

interface ResultProps {
    wuid?: string;
    resultName?: string;
    logicalFile?: string;
    cluster?: string;
    filter?: { [key: string]: any };
}

const emptyFilter: { [key: string]: any } = {};

export const Result: React.FunctionComponent<ResultProps> = ({
    wuid,
    resultName,
    logicalFile,
    cluster,
    filter = emptyFilter
}) => {

    const hasFilter = React.useMemo(() => Object.keys(filter).length > 0, [filter]);
    const [renderHTML, setRenderHTML] = React.useState(false);

    const resultTable: ResultWidget = useConst(() => new ResultWidget()
        .baseUrl("")
        .pagination(true)
        .pageSize(50) as ResultWidget
    );

    React.useEffect(() => {
        resultTable
            .wuid(wuid)
            .resultName(resultName)
            .nodeGroup(cluster)
            .logicalFile(logicalFile)
            .filter(filter)
            .renderHtml(renderHTML)
            .lazyRender()
            ;
    }, [cluster, filter, logicalFile, renderHTML, resultName, resultTable, wuid]);

    const { currentUser } = useMyAccount();
    const [wu] = useWorkunit(wuid);
    const [result] = React.useState<CommsResult>(resultTable.calcResult());
    const [FilterFields, setFilterFields] = React.useState<Fields>({});
    const [showFilter, setShowFilter] = React.useState(false);

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
        }).catch(err => logger.error(err));
    }, [result]);

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
                resultTable.reset();
                resultTable.lazyRender();
            }
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "filter", text: nlsHPCC.Filter, iconProps: { iconName: hasFilter ? "FilterSolid" : "Filter" },
            onClick: () => {
                setShowFilter(true);
            }
        },
        { key: "divider_2", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
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
            key: "copy", text: nlsHPCC.CopyWUIDs, iconOnly: true, iconProps: { iconName: "Copy" },
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
                    { key: "xls", text: nlsHPCC.XLS, title: nlsHPCC.DownloadToCSVNonFlatWarning, onClick: () => doDownload({ type: "xls", wuid, resultName: result.ResultName, sequence: result.Sequence, logicalName: result.LogicalFileName }) },
                    { key: "csv", text: nlsHPCC.CSV, title: nlsHPCC.DownloadToCSVNonFlatWarning, onClick: () => doDownload({ type: "csv", wuid, resultName: result.ResultName, sequence: result.Sequence, logicalName: result.LogicalFileName }) },
                ]
            }
        }
    ];

    return <HolyGrail
        header={<CommandBar items={buttons} farItems={rightButtons} />}
        main={
            <>
                <AutosizeHpccJSComponent widget={resultTable} />
                <Filter showFilter={showFilter} setShowFilter={setShowFilter} filterFields={filterFields} onApply={pushParams} />
                <ViewHTMLConfirm />
            </>
        }
    />;
};
