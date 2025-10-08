import * as React from "react";
import { Pivot, PivotItem, TextField, Checkbox, DefaultButton } from "@fluentui/react";
import { StackShim } from "@fluentui/react-migration-v8-v9";
import { SizeMe } from "../layouts/SizeMe";
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
import { SetLogicalFilePartAttr } from "./SetLogicalFilePartAttr";
import { DaliSDSUnlock } from "./DaliSDSUnlock";
import { SetProtected } from "./SetProtected";
import { SetUnprotected } from "./SetUnprotected";
import { SetValue } from "./SetValue";
import { DaliAdd } from "./DaliAdd";
import { DaliDelete } from "./DaliDelete";
import { DaliCount } from "./DaliCount";
import { DaliImport } from "./DaliImport";
import { useConfirm } from "../hooks/confirm";
import nlsHPCC from "src/nlsHPCC";

interface DaliAdminProps {
    tab?: string;
}

export const DaliAdmin: React.FunctionComponent<DaliAdminProps> = ({
    tab = "getdfscsv"
}) => {
    const [path, setPath] = React.useState<string>("");
    const [safe, setSafe] = React.useState<boolean>(false);

    const exportMessage = React.useMemo(
        () => path === "/" ? nlsHPCC.DaliExportPathConfirm : nlsHPCC.DaliExportConfirm,
        [path]
    );
    const [ExportConfirmDialog, confirmExport] = useConfirm({
        title: nlsHPCC.Export,
        message: exportMessage,
        onSubmit: React.useCallback(() => {
            window.location.href = `/WsDali/Export?Path=${encodeURIComponent(path)}&Safe=${safe}`;
        }, [path, safe])
    });

    return <>
        <SizeMe>{({ size }) =>
            <Pivot overflowBehavior="menu" style={{ height: "100%" }} selectedKey={tab} onLinkClick={evt => pushUrl(`/topology/daliadmin/${evt.props.itemKey}`)}>
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
                <PivotItem headerText={nlsHPCC.GetDFSMap} itemKey="getdfsmap" style={pivotItemStyle(size)} >
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
                    <GetValue />
                </PivotItem>
                <PivotItem headerText={nlsHPCC.SetLogicalFileAttribute} itemKey="setlogicalfilepartattr" style={pivotItemStyle(size)} >
                    <SetLogicalFilePartAttr />
                </PivotItem>
                <PivotItem headerText={nlsHPCC.UnlockSDSLock} itemKey="unlockSdsLock" style={pivotItemStyle(size)} >
                    <DaliSDSUnlock />
                </PivotItem>
                <PivotItem headerText={nlsHPCC.SetProtected} itemKey="setprotected" style={pivotItemStyle(size)} >
                    <SetProtected />
                </PivotItem>
                <PivotItem headerText={nlsHPCC.SetUnprotected} itemKey="setunprotected" style={pivotItemStyle(size)} >
                    <SetUnprotected />
                </PivotItem>
                <PivotItem headerText={nlsHPCC.SetValue} itemKey="setvalue" style={pivotItemStyle(size)} >
                    <SetValue />
                </PivotItem>
                <PivotItem headerText={nlsHPCC.Add} itemKey="daliadd" style={pivotItemStyle(size)} >
                    <DaliAdd />
                </PivotItem>
                <PivotItem headerText={nlsHPCC.Delete} itemKey="dalidelete" style={pivotItemStyle(size)} >
                    <DaliDelete />
                </PivotItem>
                <PivotItem headerText={nlsHPCC.Count} itemKey="dalicount" style={pivotItemStyle(size)} >
                    <DaliCount />
                </PivotItem>
                <PivotItem headerText={nlsHPCC.Import} itemKey="daliimport" style={pivotItemStyle(size)} >
                    <DaliImport />
                </PivotItem>
                <PivotItem headerText={nlsHPCC.Export} itemKey="daliExport" style={pivotItemStyle(size)}>
                    <StackShim tokens={{ childrenGap: 12 }} styles={{ root: { maxWidth: 400 } }}>
                        <TextField label="Path" placeholder="/your/dfs/path" value={path} onChange={(_, v) => setPath(v || "")} />
                        <Checkbox label="Safe" checked={safe} onChange={(_, c) => setSafe(!!c)} />
                        <DefaultButton onClick={() => confirmExport(true)}>
                            {nlsHPCC.Export}
                        </DefaultButton>
                    </StackShim>
                </PivotItem>
            </Pivot>
        }</SizeMe>
        <ExportConfirmDialog />
    </>;
};