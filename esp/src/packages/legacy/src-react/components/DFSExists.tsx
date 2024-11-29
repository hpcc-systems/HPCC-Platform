import * as React from "react";
import { DefaultButton, DetailsList, DetailsListLayoutMode, IColumn } from "@fluentui/react";
import { SizeMe } from "react-sizeme";
import { DaliService } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import { TableGroup } from "./forms/Groups";
import nlsHPCC from "src/nlsHPCC";
import { HolyGrail } from "../layouts/HolyGrail";
import { parser } from "dojo/main";

const logger = scopedLogger("src-react/components/DFSExist.tsx");

const myDaliService = new DaliService({ baseUrl: "" });

interface DFSExistProps {
}

export const DFSExists: React.FunctionComponent<DFSExistProps> = ({

}) => {

    const [columns, setColumns] = React.useState<IColumn[]>([]);
    const [items, setItems] = React.useState<any[]>([]);
    const [fileName, setFileName] = React.useState<string>("");

    const onSubmit = React.useCallback(() => {
        myDaliService.DFSExists({ FileName: fileName }).then(response => {
            const data = parser.parse(response.Result);
            setColumns(data.columns.map((col, idx) => {
                return {
                    key: col,
                    name: col,
                    fileName: col,
                    minWidth: 100
                };
            }));
            setItems(data);
        }).catch(err => logger.error(err));
    }, [fileName]);

    return <HolyGrail
        header={<span><TableGroup fields={{
            "FileName": { label: nlsHPCC.FileName, type: "string", value: fileName },
        }} onChange={(id, value) => {
            setFileName(value);
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