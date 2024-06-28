import * as React from "react";
import { WsDfu } from "@hpcc-js/comms";
import { SizeMe } from "react-sizeme";
import nlsHPCC from "src/nlsHPCC";
import { QuerySortItem } from "src/store/Store";
import { FileParts } from "./FileParts";
import { useFile, useDefFile } from "../hooks/file";
import { pushUrl, replaceUrl } from "../util/history";
import { FileBlooms } from "./FileBlooms";
import { FileHistory } from "./FileHistory";
import { ProtectedBy } from "./ProtectedBy";
import { SuperFiles } from "./SuperFiles";
import { SubFiles } from "./SubFiles";
import { SourceEditor, XMLSourceEditor } from "./SourceEditor";
import { FileDetailsGraph } from "./FileDetailsGraph";
import { LogicalFileSummary } from "./LogicalFileSummary";
import { SuperFileSummary } from "./SuperFileSummary";
import { DataPatterns } from "./DataPatterns";
import { Result } from "./Result";
import { Queries } from "./Queries";
import { IndexFileSummary } from "./IndexFileSummary";
import { DelayLoadedPanel, OverflowTabList, TabInfo } from "./controls/TabbedPanes/index";

import "react-reflex/styles.css";

type StringStringMap = { [key: string]: string };
interface FileDetailsProps {
    cluster?: string;
    logicalFile: string;
    tab?: string;
    sort?: { subfiles?: QuerySortItem, superfiles?: QuerySortItem, parts?: QuerySortItem, graphs?: QuerySortItem, history?: QuerySortItem, blooms?: QuerySortItem, protectby?: QuerySortItem };
    queryParams?: { contents?: StringStringMap };
}

export const FileDetails: React.FunctionComponent<FileDetailsProps> = ({
    cluster,
    logicalFile,
    tab = "summary",
    sort = {},
    queryParams = {}
}) => {
    const [file] = useFile(cluster, logicalFile);
    React.useEffect(() => {
        if (file?.NodeGroup && cluster === undefined && !file?.isSuperfile) {
            replaceUrl(`/files/${file.NodeGroup}/${logicalFile}`);
        }
    }, [cluster, file?.NodeGroup, file?.isSuperfile, logicalFile]);
    const [defFile] = useDefFile(cluster, logicalFile, WsDfu.DFUDefFileFormat.def);
    const [xmlFile] = useDefFile(cluster, logicalFile, WsDfu.DFUDefFileFormat.xml);

    const onTabSelect = React.useCallback((tab: TabInfo) => {
        pushUrl(tab.__state ?? `/files/${cluster}/${logicalFile}/${tab.id}`);
    }, [cluster, logicalFile]);

    const tabs = React.useMemo((): TabInfo[] => {
        return [{
            id: "summary",
            label: nlsHPCC.Summary,
        }, {
            id: "contents",
            label: nlsHPCC.Contents,
        }, {
            id: "datapatterns",
            label: nlsHPCC.DataPatterns,
        }, {
            id: "ecl",
            label: nlsHPCC.ECL,
        }, {
            id: "def",
            label: nlsHPCC.DEF,
        }, {
            id: "xml",
            label: nlsHPCC.XML,
        }, file?.isSuperfile ? {
            id: "subfiles",
            label: nlsHPCC.Subfiles,
            count: file?.subfiles?.Item.length ?? 0
        } : {
            id: "superfiles",
            label: nlsHPCC.Superfiles,
            count: file?.Superfiles?.DFULogicalFile.length ?? 0
        }, {
            id: "parts",
            label: nlsHPCC.FileParts,
            count: file?.fileParts().length ?? 0
        }, {
            id: "queries",
            label: nlsHPCC.Queries,
        }, {
            id: "graphs",
            label: nlsHPCC.Graphs,
            count: file?.Graphs?.ECLGraph?.length ?? 0
        }, {
            id: "history",
            label: nlsHPCC.History
        }, {
            id: "blooms",
            label: nlsHPCC.Blooms,
            count: file?.Blooms?.DFUFileBloom?.length ?? 0
        }, {
            id: "protectby",
            label: nlsHPCC.ProtectBy
        }];
    }, [file]);

    return <SizeMe monitorHeight>{({ size }) =>
        <div style={{ height: "100%" }}>
            <OverflowTabList tabs={tabs} selected={tab} onTabSelect={onTabSelect} size="medium" />
            <DelayLoadedPanel visible={tab === "summary"} size={size}>
                {file?.ContentType === "key"
                    ? <IndexFileSummary cluster={cluster} logicalFile={logicalFile} />
                    : file?.isSuperfile
                        ? <SuperFileSummary cluster={cluster} logicalFile={logicalFile} />
                        : <LogicalFileSummary cluster={cluster} logicalFile={logicalFile} />
                }
            </DelayLoadedPanel>
            <DelayLoadedPanel visible={tab === "contents"} size={size}>
                <Result cluster={cluster} logicalFile={logicalFile} filter={queryParams.contents} />
            </DelayLoadedPanel>
            <DelayLoadedPanel visible={tab === "datapatterns"} size={size}>
                <DataPatterns cluster={cluster} logicalFile={logicalFile} />
            </DelayLoadedPanel>
            <DelayLoadedPanel visible={tab === "ecl"} size={size}>
                <SourceEditor text={file?.Ecl} mode="ecl" readonly={true} />
            </DelayLoadedPanel>
            <DelayLoadedPanel visible={tab === "def"} size={size}>
                <XMLSourceEditor text={defFile} readonly={true} />
            </DelayLoadedPanel>
            <DelayLoadedPanel visible={tab === "xml"} size={size}>
                <XMLSourceEditor text={xmlFile} readonly={true} />
            </DelayLoadedPanel>
            <DelayLoadedPanel visible={tab === "subfiles"} size={size}>
                <SubFiles cluster={cluster} logicalFile={logicalFile} sort={sort.subfiles} />
            </DelayLoadedPanel>
            <DelayLoadedPanel visible={tab === "superfiles"} size={size}>
                <SuperFiles cluster={cluster} logicalFile={logicalFile} sort={sort.superfiles} />
            </DelayLoadedPanel>
            <DelayLoadedPanel visible={tab === "parts"} size={size}>
                <FileParts cluster={cluster} logicalFile={logicalFile} sort={sort.parts} />
            </DelayLoadedPanel>
            <DelayLoadedPanel visible={tab === "queries"} size={size}>
                <Queries filter={{ FileName: logicalFile }} />
            </DelayLoadedPanel>
            <DelayLoadedPanel visible={tab === "graphs"} size={size}>
                <FileDetailsGraph cluster={cluster} logicalFile={logicalFile} sort={sort.graphs} />
            </DelayLoadedPanel>
            <DelayLoadedPanel visible={tab === "history"} size={size}>
                <FileHistory cluster={cluster} logicalFile={logicalFile} sort={sort.history} />
            </DelayLoadedPanel>
            <DelayLoadedPanel visible={tab === "blooms"} size={size}>
                <FileBlooms cluster={cluster} logicalFile={logicalFile} sort={sort.blooms} />
            </DelayLoadedPanel>
            <DelayLoadedPanel visible={tab === "protectby"} size={size}>
                <ProtectedBy cluster={cluster} logicalFile={logicalFile} sort={sort.protectby} />
            </DelayLoadedPanel>
        </div>
    }</SizeMe>;
};
