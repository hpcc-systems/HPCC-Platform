import * as React from "react";
import { Pivot, PivotItem } from "@fluentui/react";
import { SizeMe } from "react-sizeme";
import { pushUrl } from "../util/history";
import { pivotItemStyle } from "../layouts/pivot";
import { DFSCheck } from "./DFSCheck";
import { DFSExists } from "./DFSExists";
import { DFSLS } from "./DFSLS";
import { GetDFSCSV } from "./GetDFSCSV";
import { GetDFSMap } from "./GetDFSMap";
import { GetDFSParents } from "./GetDFSParents";
import { GetLogicalFile } from "./GetLogicalFile";
import { GetLogicalFilePart } from "./GetLogicalFilePart";
import { GetProtectedList } from "./GetProtectedList";
import { GetValue } from "./GetValue";
import nlsHPCC from "src/nlsHPCC";

interface DaliAdminProps {
    tab?: string;
}

export const DaliAdmin: React.FunctionComponent<DaliAdminProps> = ({
    tab = "getdfscsv"
}) => {

    return <>
    <SizeMe monitorHeight>{({ size }) =>
        <Pivot overflowBehavior="menu" style={{ height: "100%" }} selectedKey={tab} onLinkClick={evt => pushUrl(`/daliadmin/${evt.props.itemKey}`)}>
            <PivotItem headerText={nlsHPCC.GetDFSCSV} itemKey="getdfscsv" style={pivotItemStyle(size)} >
                <GetDFSCSV />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.DFSCheck} itemKey="dfscheck" style={pivotItemStyle(size)} >
                <DFSCheck />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.DFSExists} itemKey="dfsexists" style={pivotItemStyle(size)} >
                <DFSExists />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.DFSLS} itemKey="dfsls" style={pivotItemStyle(size)} >
                <DFSLS />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.GetDFSMap} itemKey="getdfsmap"  style={pivotItemStyle(size)} >
                <GetDFSMap />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.GetDFSParents} itemKey="getdfsparents" style={pivotItemStyle(size)} >
                <GetDFSParents />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.GetLogicalFile} itemKey="getlogicalfile" style={pivotItemStyle(size)} >
                <GetLogicalFile />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.GetLogicalFilePart} itemKey="getlogicalfilepart" style={pivotItemStyle(size)} >
                <GetLogicalFilePart />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.GetProtectedList} itemKey="getprotectedlist" style={pivotItemStyle(size)} >
                <GetProtectedList />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.GetValue} itemKey="getvalue" style={pivotItemStyle(size)} >
                <GetValue  />
            </PivotItem>
        </Pivot>
    }</SizeMe>;
    </>;

};