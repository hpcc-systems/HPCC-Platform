import * as React from "react";
import { Pivot, PivotItem } from "@fluentui/react";
import { SizeMe } from "react-sizeme";
import nlsHPCC from "src/nlsHPCC";
import { FileParts } from "./FileParts";
import { useFile, useDefFile } from "../hooks/file";
import { pivotItemStyle } from "../layouts/pivot";
import { pushUrl, replaceUrl } from "../util/history";
import { FileBlooms } from "./FileBlooms";
import { FileHistory } from "./FileHistory";
import { ProtectedBy } from "./ProtectedBy";
import { SuperFiles } from "./SuperFiles";
import { SubFiles } from "./SubFiles";
import { ECLSourceEditor, XMLSourceEditor } from "./SourceEditor";
import { FileDetailsGraph } from "./FileDetailsGraph";
import { LogicalFileSummary } from "./LogicalFileSummary";
import { SuperFileSummary } from "./SuperFileSummary";
import { DataPatterns } from "./DataPatterns";
import { Result } from "./Result";
import { Queries } from "./Queries";

import "react-reflex/styles.css";

interface FileDetailsProps {
    cluster?: string;
    logicalFile: string;
    tab?: string;
}

export const FileDetails: React.FunctionComponent<FileDetailsProps> = ({
    cluster,
    logicalFile,
    tab = "summary"
}) => {

    const [file] = useFile(cluster, logicalFile);
    React.useEffect(() => {
        if (file?.NodeGroup && cluster === undefined) {
            replaceUrl(`/files/${file.NodeGroup}/${logicalFile}`);
        }
    }, [cluster, file?.NodeGroup, logicalFile]);
    const [defFile] = useDefFile(cluster, logicalFile, "def");
    const [xmlFile] = useDefFile(cluster, logicalFile, "xml");

    const isDFUWorkunit = file?.Wuid?.length && file?.Wuid[0] === "D";

    return <SizeMe monitorHeight>{({ size }) =>
        <Pivot overflowBehavior="menu" style={{ height: "100%" }} selectedKey={tab} onLinkClick={evt => pushUrl(`/files/${cluster}/${logicalFile}/${evt.props.itemKey}`)}>
            <PivotItem headerText={nlsHPCC.Summary} itemKey="summary" style={pivotItemStyle(size)}>
                {file?.isSuperfile !== undefined
                    ? file.isSuperfile
                        ? <SuperFileSummary cluster={cluster} logicalFile={logicalFile} />
                        : <LogicalFileSummary cluster={cluster} logicalFile={logicalFile} />
                    : <></>
                }
            </PivotItem>
            <PivotItem headerText={nlsHPCC.Contents} itemKey="contents" style={pivotItemStyle(size, 0)}>
                <Result cluster={cluster} logicalFile={logicalFile} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.DataPatterns} itemKey="datapatterns" style={pivotItemStyle(size, 0)}>
                <DataPatterns cluster={cluster} logicalFile={logicalFile} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.ECL} itemKey="ecl" style={pivotItemStyle(size, 0)}>
                <ECLSourceEditor text={file?.Ecl} readonly={true} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.DEF} itemKey="def" style={pivotItemStyle(size, 0)}>
                <XMLSourceEditor text={defFile} readonly={true} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.XML} itemKey="xml" style={pivotItemStyle(size, 0)}>
                <XMLSourceEditor text={xmlFile} readonly={true} />
            </PivotItem>
            {file?.isSuperfile
                ? <PivotItem headerText={nlsHPCC.Subfiles} itemKey="subfiles" itemCount={file?.subfiles?.Item.length ?? 0} style={pivotItemStyle(size, 0)}>
                    <SubFiles cluster={cluster} logicalFile={logicalFile} />
                </PivotItem>
                : <PivotItem headerText={nlsHPCC.Superfiles} itemKey="superfiles" itemCount={file?.Superfiles?.DFULogicalFile.length ?? 0} style={pivotItemStyle(size, 0)}>
                    <SuperFiles cluster={cluster} logicalFile={logicalFile} />
                </PivotItem>
            }
            <PivotItem headerText={nlsHPCC.FileParts} itemKey="parts" itemCount={file?.fileParts().length ?? 0} style={pivotItemStyle(size, 0)}>
                <FileParts cluster={cluster} logicalFile={logicalFile} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.Queries} itemKey="queries" style={pivotItemStyle(size, 0)}>
                <Queries filter={{ FileName: logicalFile }} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.Graphs} itemKey="graphs" itemCount={file?.Graphs?.ECLGraph?.length} headerButtonProps={{ disabled: isDFUWorkunit }} style={pivotItemStyle(size, 0)}>
                <FileDetailsGraph cluster={cluster} logicalFile={logicalFile} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.History} itemKey="history" style={pivotItemStyle(size, 0)}>
                <FileHistory cluster={cluster} logicalFile={logicalFile} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.Blooms} itemKey="blooms" itemCount={file?.Blooms?.DFUFileBloom?.length} style={pivotItemStyle(size, 0)}>
                <FileBlooms cluster={cluster} logicalFile={logicalFile} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.ProtectBy} itemKey="protectby" style={pivotItemStyle(size, 0)}>
                <ProtectedBy cluster={cluster} logicalFile={logicalFile} />
            </PivotItem>
        </Pivot>
    }</SizeMe>;
};
