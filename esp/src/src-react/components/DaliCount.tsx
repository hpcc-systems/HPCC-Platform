import * as React from "react";
import { DefaultButton, DetailsList, DetailsListLayoutMode, IColumn } from "@fluentui/react";
import { SizeMe } from "react-sizeme";
import { DaliService } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import { TableGroup } from "./forms/Groups";
import nlsHPCC from "src/nlsHPCC";
import { HolyGrail } from "../layouts/HolyGrail";

const logger = scopedLogger("src-react/components/DaliCount.tsx");

const myDaliService = new DaliService({ baseUrl: "" });

interface CountRequestProps {

}

export const DaliCount: React.FunctionComponent<CountRequestProps> = ({

}) => {

    const [columns, setColumns] = React.useState<IColumn[]>([]);
    const [items, setItems] = React.useState<any[]>([]);
    const [path, setPath] = React.useState<string>("");

    const onSubmit = React.useCallback(() => {
        myDaliService.Count({ Path: path }).then(response => {
            const data = [{
                key: "Result",
                result: response.Result
            }];
            const columns = [{
                key: "Result",
                name: "Result",
                fieldName: "result",
                minWidth: 100
            }];
            setItems(data);
            setColumns(columns); // Add this line to set the columns as well
        }).catch(err => logger.error(err));
    }, [path]);

    return <HolyGrail
        header={<span><TableGroup fields={{
            "Path": { label: nlsHPCC.Path, type: "string", value: path },
        }} onChange={(id, value) => {
            setPath(value);
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