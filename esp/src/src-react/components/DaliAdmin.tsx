import * as React from "react";
import { TextField, Checkbox, DefaultButton } from "@fluentui/react";
import { StackShim } from "@fluentui/react-migration-v8-v9";
import { makeStyles } from "@fluentui/react-components";
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
import { OverflowTabList, TabInfo } from "./controls/TabbedPanes/index";

const useStyles = makeStyles({
    container: {
        height: "100%",
        position: "relative"
    }
});

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

    const tabs = React.useMemo((): TabInfo[] => [
        { id: "getdfscsv", label: nlsHPCC.GetDFSCSV },
        { id: "dfscheck", label: nlsHPCC.DFSCheck },
        { id: "dfsexists", label: nlsHPCC.DFSExists },
        { id: "dfsls", label: nlsHPCC.DFSLS },
        { id: "getdfsmap", label: nlsHPCC.GetDFSMap },
        { id: "getdfsparents", label: nlsHPCC.GetDFSParents },
        { id: "getlogicalfile", label: nlsHPCC.GetLogicalFile },
        { id: "getlogicalfilepart", label: nlsHPCC.GetLogicalFilePart },
        { id: "getprotectedlist", label: nlsHPCC.GetProtectedList },
        { id: "getvalue", label: nlsHPCC.GetValue },
        { id: "setlogicalfilepartattr", label: nlsHPCC.SetLogicalFileAttribute },
        { id: "unlockSdsLock", label: nlsHPCC.UnlockSDSLock },
        { id: "setprotected", label: nlsHPCC.SetProtected },
        { id: "setunprotected", label: nlsHPCC.SetUnprotected },
        { id: "setvalue", label: nlsHPCC.SetValue },
        { id: "daliadd", label: nlsHPCC.Add },
        { id: "dalidelete", label: nlsHPCC.Delete },
        { id: "dalicount", label: nlsHPCC.Count },
        { id: "daliimport", label: nlsHPCC.Import },
        { id: "daliExport", label: nlsHPCC.Export }
    ], []);

    const onTabSelect = React.useCallback((tabInfo: TabInfo) => {
        pushUrl(`/topology/daliadmin/${tabInfo.id}`);
    }, []);

    const renderTabPanel = React.useCallback(() => {
        switch (tab) {
            case "getdfscsv":
                return <GetDFSCSV />;
            case "dfscheck":
                return <DFSCheck />;
            case "dfsexists":
                return <DFSExists />;
            case "dfsls":
                return <DFSLS />;
            case "getdfsmap":
                return <GetDFSMap />;
            case "getdfsparents":
                return <GetDFSParents />;
            case "getlogicalfile":
                return <GetLogicalFile />;
            case "getlogicalfilepart":
                return <GetLogicalFilePart />;
            case "getprotectedlist":
                return <GetProtectedList />;
            case "getvalue":
                return <GetValue />;
            case "setlogicalfilepartattr":
                return <SetLogicalFilePartAttr />;
            case "unlockSdsLock":
                return <DaliSDSUnlock />;
            case "setprotected":
                return <SetProtected />;
            case "setunprotected":
                return <SetUnprotected />;
            case "setvalue":
                return <SetValue />;
            case "daliadd":
                return <DaliAdd />;
            case "dalidelete":
                return <DaliDelete />;
            case "dalicount":
                return <DaliCount />;
            case "daliimport":
                return <DaliImport />;
            case "daliExport":
                return (
                    <StackShim tokens={{ childrenGap: 12 }} styles={{ root: { maxWidth: 400 } }}>
                        <TextField label="Path" placeholder="/your/dfs/path" value={path} onChange={(_, v) => setPath(v || "")} />
                        <Checkbox label="Safe" checked={safe} onChange={(_, c) => setSafe(!!c)} />
                        <DefaultButton onClick={() => confirmExport(true)}>
                            {nlsHPCC.Export}
                        </DefaultButton>
                    </StackShim>
                );
            default:
                return null;
        }
    }, [confirmExport, path, safe, tab]);

    const styles = useStyles();

    return <>
        <SizeMe>{({ size }) =>
            <div className={styles.container}>
                <OverflowTabList tabs={tabs} selected={tab} onTabSelect={onTabSelect} size="medium" />
                <div style={pivotItemStyle(size)}>
                    {renderTabPanel()}
                </div>
            </div>
        }</SizeMe>
        <ExportConfirmDialog />
    </>;
};