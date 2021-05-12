import * as React from "react";
import * as ReactDOM from "react-dom";
import { Checkbox, CommandBar, ContextualMenuItemType, DefaultButton, Dialog, DialogFooter, DialogType, ICommandBarItemProps, PrimaryButton, SpinButton, Stack } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import { Result as CommsResult, XSDXMLNode } from "@hpcc-js/comms";
import { WUResult } from "@hpcc-js/eclwatch";
import nlsHPCC from "src/nlsHPCC";
import { ESPBase } from "src/ESPBase";
import { csvEncode } from "src/Utility";
import { HolyGrail } from "../layouts/HolyGrail";
import { pushParams } from "../util/history";
import { AutosizeHpccJSComponent } from "../layouts/HpccJSAdapter";
import { ShortVerticalDivider } from "./Common";
import { Fields } from "./forms/Fields";
import { Filter } from "./forms/Filter";

import "srcReact/components/DojoGrid.css";

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

function doDownload(type: string, wuid: string, sequence?: number, logicalName?: string) {
    const base = new ESPBase();
    if (sequence !== undefined) {
        window.open(base.getBaseURL() + "/WUResultBin?Format=" + type + "&Wuid=" + wuid + "&Sequence=" + sequence, "_blank");
    } else if (logicalName !== undefined) {
        window.open(base.getBaseURL() + "/WUResultBin?Format=" + type + "&LogicalName=" + logicalName, "_blank");
    }
}

interface ResultProps {
    wuid: string;
    resultName: string;
    filter?: { [key: string]: any };
}

const emptyFilter: { [key: string]: any } = {};

export const Result: React.FunctionComponent<ResultProps> = ({
    wuid,
    resultName,
    filter = emptyFilter
}) => {

    const resultTable: ResultWidget = useConst(new ResultWidget()
        .baseUrl("")
        .wuid(wuid)
        .resultName(resultName)
        .pagination(true)
        .pageSize(50) as ResultWidget
    );

    resultTable
        .filter(filter)
        .lazyRender()
        ;

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
        });
    }, [result]);

    //  Command Bar  ---
    const buttons: ICommandBarItemProps[] = [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => {
                resultTable.reset();
                resultTable.lazyRender();
            }
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "filter", text: nlsHPCC.Filter, iconProps: { iconName: "Filter" },
            onClick: () => {
                setShowFilter(true);
            }
        },
    ];

    //  Filter  ---
    const filterFields: Fields = {};
    for (const fieldID in FilterFields) {
        filterFields[fieldID] = { ...FilterFields[fieldID], value: filter[fieldID] };
    }

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
                    { key: "zip", text: nlsHPCC.Zip, onClick: () => doDownload("zip", wuid, result.Sequence) },
                    { key: "gzip", text: nlsHPCC.GZip, onClick: () => doDownload("gzip", wuid, result.Sequence) },
                    { key: "xls", text: nlsHPCC.XLS, onClick: () => doDownload("xls", wuid, result.Sequence) },
                    { key: "csv", text: nlsHPCC.CSV, onClick: () => doDownload("csv", wuid, result.Sequence) },
                ]
            }
        }
    ];

    return <HolyGrail
        header={<CommandBar items={buttons} overflowButtonProps={{}} farItems={rightButtons} />}
        main={
            <>
                <AutosizeHpccJSComponent widget={resultTable} />
                <Filter showFilter={showFilter} setShowFilter={setShowFilter} filterFields={filterFields} onApply={pushParams} />
            </>
        }
    />;
};
