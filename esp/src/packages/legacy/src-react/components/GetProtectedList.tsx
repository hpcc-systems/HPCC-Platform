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
    GetProtectedList(request: WsDali.GetProtectedListRequest): Promise<WsDali.ResultResponse> {
        return this._connection.send("GetProtectedList", request, "json", false, undefined, "ResultResponse");
    }
}

const logger = scopedLogger("src-react/components/GetProtectedList.tsx");

const myDaliService = new MyDaliService({ baseUrl: "" });

interface GetProtectedListProps {
}

export const GetProtectedList: React.FunctionComponent<GetProtectedListProps> = ({

}) => {

    const [columns, setColumns] = React.useState<IColumn[]>([]);
    const [items, setItems] = React.useState<any[]>([]);
    const [fileName, setFileName] = React.useState<string>("");
    const [callerId, setCallerId] = React.useState<string>("");

    const onSubmit = React.useCallback(() => {
        myDaliService.GetProtectedList({ FileName: fileName, CallerId: callerId }).then(response => {
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
    }, [fileName, callerId]);

    return <HolyGrail
        header={<span><TableGroup fields={{
            "FileName": { label: nlsHPCC.FileName, type: "string", value: fileName },
            "CallerId": { label: nlsHPCC.CallerID, type: "string", value: callerId },
        }} onChange={(id, value) => {
            switch (id) {
                case "FileName":
                    setFileName(value);
                    break;
                case "CallerId":
                    setCallerId(value);
                    break;
                default:
                    logger.debug(`${id}:  ${value}`);
            }
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