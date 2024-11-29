import * as React from "react";
import { DefaultButton, DetailsList, DetailsListLayoutMode, IColumn } from "@fluentui/react";
import { SizeMe } from "react-sizeme";
import { csvParse } from "d3-dsv";
import { DaliService, WsDali } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import { TableGroup } from "./forms/Groups";
import nlsHPCC from "src/nlsHPCC";
import { HolyGrail } from "../layouts/HolyGrail";

class MyDaliService extends DaliService {
    GetDFSCSV(request: WsDali.GetDFSCSVRequest): Promise<WsDali.ResultResponse> {
        return this._connection.send("GetDFSCSV", request, "json", false, undefined, "ResultResponse");
    }
}

const logger = scopedLogger("src-react/components/GetDFSCSV.tsx");

const myDaliService = new MyDaliService({ baseUrl: "" });

interface GetDFSCSVProps {
}

export const GetDFSCSV: React.FunctionComponent<GetDFSCSVProps> = ({

}) => {

    const [columns, setColumns] = React.useState<IColumn[]>([]);
    const [items, setItems] = React.useState<any[]>([]);
    const [logicalNameMask, setLogicalNameMask] = React.useState<string>("");

    const onSubmit = React.useCallback(() => {
        myDaliService.GetDFSCSV({ LogicalNameMask: logicalNameMask }).then(response => {
            const data = csvParse(response.Result);
            setColumns(data.columns.map((col, idx) => {
                return {
                    key: col,
                    name: col,
                    fieldName: col,
                    minWidth: 100
                };
            }));
            setItems(data);
        }).catch(err => logger.error(err));
    }, [logicalNameMask]);

    return <HolyGrail
        header={<span><TableGroup fields={{
            "LogicalNameMask": { label: nlsHPCC.LogicalNameMask, type: "string", value: logicalNameMask },
        }} onChange={(id, value) => {
            setLogicalNameMask(value);
        }} /><DefaultButton onClick={onSubmit} text={nlsHPCC.Submit} /></span>}
        main={<SizeMe monitorHeight>{({ size }) => {
            const height = `${size.height}px`;
            return <div style={{ position: "relative", width: "100%", height: "100%" }}>
                <div style={{ position: "absolute", width: "100%", height: `${size.height}px` }}>
                    <DetailsList compact={true}
                        items={items}
                        columns={columns}
                        setKey="key"
                        layoutMode={DetailsListLayoutMode.justified}
                        selectionPreservedOnEmptyClick={true}
                        styles={{ root: { height, minHeight: height, maxHeight: height } }}
                    />
                </div>
            </div>;
        }}</SizeMe>}
    />;
}; 