import * as React from "react";
import { DefaultButton, DetailsList, DetailsListLayoutMode, IColumn } from "@fluentui/react";
import { SizeMe } from "react-sizeme";
import { csvParse } from "d3-dsv";
import { DaliService } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import { TableGroup } from "./forms/Groups";
import nlsHPCC from "src/nlsHPCC";
import { HolyGrail } from "../layouts/HolyGrail";


const logger = scopedLogger("src-react/components/DFSLS.tsx");

const myDaliService = new DaliService({ baseUrl: "" });

interface DFSLSProps {
}

export const DFSLS: React.FunctionComponent<DFSLSProps> = ({

}) => {

    const [columns, setColumns] = React.useState<IColumn[]>([]);
    const [items, setItems] = React.useState<any[]>([]);
    const [Name, setName] = React.useState<string>("");
    const [pathAndNameOnly, setPathAndNameOnly] = React.useState(true);
    const [includeSubFileInfo, setIncludeSubFileInfo] = React.useState(false);
    const [recursively, setRecursively] = React.useState(false);

    const onSubmit = React.useCallback(() => {
        myDaliService.DFSLS({ Name: Name, PathAndNameOnly: pathAndNameOnly, IncludeSubFileInfo: includeSubFileInfo, Recursively: recursively }).then(response => {
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
    }, [name, pathAndNameOnly, includeSubFileInfo, recursively]);

    return <HolyGrail
        header={<span><TableGroup fields={{
            "Name": { label: nlsHPCC.Name, type: "string", value: Name },
            "PathAndNameOnly": { label: nlsHPCC.PathAndNameOnly, type: "checkbox", value: true ? true : false },
            "IncludeSubFileInfo": { label: nlsHPCC.IncludeSubFileInfo, type: "checkbox", value: true ? true : false },
            "Recursively": { label: nlsHPCC.Recursively, type: "checkbox", value: true ? true : false },

        }} onChange={(id, value) => {
            switch (id) {
                case "name":
                    setName(value);
                    break;
                case "pathAndNameOnly":
                    setPathAndNameOnly(value);
                    break;                
                case "includeSubFileInfo":
                    setIncludeSubFileInfo(value);
                    break;
                case "recursively":
                    setRecursively(value);
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