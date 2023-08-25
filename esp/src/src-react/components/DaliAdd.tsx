import * as React from "react";
import { DefaultButton, DetailsList, DetailsListLayoutMode, IColumn } from "@fluentui/react";
import { SizeMe } from "react-sizeme";
import { csvParse } from "d3-dsv";
import { DaliService } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import { TableGroup } from "./forms/Groups";
import { useConfirm } from "../hooks/confirm";
import nlsHPCC from "src/nlsHPCC";
import { HolyGrail } from "../layouts/HolyGrail";

const logger = scopedLogger("src-react/components/DaliAdd.tsx");

const myDaliService = new DaliService({ baseUrl: "" });

interface DaliAddProps {
}

export const DaliAdd: React.FunctionComponent<DaliAddProps> = ({

}) => {

    const [columns, setColumns] = React.useState<IColumn[]>([]);
    const [items, setItems] = React.useState<any[]>([]);
    const [path, setPath] = React.useState<string>("");
    const [value, setValue] = React.useState<string>("");

    const [DaliPromptConfirm, setDaliPromptConfirm] = useConfirm({
        title: nlsHPCC.DaliAdmin,
        message: nlsHPCC.DaliPromptConfirm,
        onSubmit: React.useCallback(() => {
            myDaliService.Add({ Path: path, Value: value }).then(response => {
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
        }, [path, value])
    }); 

    const onSubmit = React.useCallback(() => {
        setDaliPromptConfirm(true);
    }, [setDaliPromptConfirm]);

    return <HolyGrail
        header={<span><TableGroup fields={{
            "Path": { label: nlsHPCC.Path, type: "string", value: path },
            "Value": { label: nlsHPCC.Value, type: "string", value: value },
        }} onChange={(id, value) => {
            switch (id) {
                case "Path":
                    setPath(value);
                    break;
                case "Value":
                    setValue(value);
                    break; 
                default:
                    logger.debug(`${id}: ${value}`);
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
                    <DaliPromptConfirm />                    
                </div>
            </div>;
        }}</SizeMe>}
    />;
}; 