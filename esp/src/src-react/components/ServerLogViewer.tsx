import * as React from "react";
import { Dropdown, Option, Toolbar, ToolbarButton, ToolbarDivider } from "@fluentui/react-components";
import { ArrowClockwise20Regular, ArrowDownload20Regular } from "@fluentui/react-icons";
import { TopologyService, WsTopology } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import nlsHPCC from "src/nlsHPCC";
import { HolyGrail } from "../layouts/HolyGrail";
import { hashHistory, parseQuery, updateParam } from "../util/history";
import { getESPBaseURL } from "../util/espUrl";
import { SourceEditor } from "./SourceEditor";

const service = new TopologyService({ baseUrl: "" });

const logger = scopedLogger("src-react/components/ServerLogViewer.tsx");

export interface ServerLogViewerQueryParams {
    NetAddress?: string;
    LogDirectory?: string;
}

interface ServerLogViewerProps {
    netAddress?: string;
    logDirectory?: string;
}

export const ServerLogViewer: React.FunctionComponent<ServerLogViewerProps> = ({
    netAddress = "",
    logDirectory = ""
}) => {
    const [logFiles, setLogFiles] = React.useState<{ label: string; value: string }[]>([]);
    const [selectedLog, setSelectedLog] = React.useState<string>("");
    const [prevNetAddress, setPrevNetAddress] = React.useState(netAddress);
    const [prevLogDirectory, setPrevLogDirectory] = React.useState(logDirectory);
    const [logText, setLogText] = React.useState<string>("");

    if (prevNetAddress !== netAddress || prevLogDirectory !== logDirectory) {
        setPrevNetAddress(netAddress);
        setPrevLogDirectory(logDirectory);
        setSelectedLog("");
        setLogFiles([]);
        setLogText("");
    }

    const logName = React.useMemo(() => {
        if (!netAddress || !selectedLog) return "";
        return `//${netAddress}${logDirectory}/${selectedLog}`;
    }, [netAddress, logDirectory, selectedLog]);

    const refreshLogList = React.useCallback(() => {
        if (!netAddress || !logDirectory) return;
        service.TpListLogFiles({
            NetworkAddress: netAddress,
            Path: logDirectory
        }).then((response) => {
            const files: WsTopology.LogFileStruct[] = response?.Files?.LogFileStruct ?? [];
            const options = files.map(f => ({ label: f.Name ?? "", value: f.Name ?? "" }));
            options.sort((l, r) => -l.label.localeCompare(r.label));
            setLogFiles(options);
            if (options.length > 0) {
                const { logFile } = parseQuery<{ logFile?: string }>(hashHistory.location.search);
                const fromUrl = logFile && options.some(o => o.value === logFile) ? logFile : undefined;
                const shortest = options.reduce((prev, curr) => curr.label.length < prev.label.length ? curr : prev);
                setSelectedLog(fromUrl || shortest.value);
            }
        }).catch(err => logger.error(err));
    }, [netAddress, logDirectory]);

    React.useEffect(() => {
        refreshLogList();
    }, [refreshLogList]);

    React.useEffect(() => {
        if (!logName) return;
        service.TpLogFile({
            Name: logName,
            Type: "tpcomp_log",
            FilterType: 4,
            LoadData: true,
            PageNumber: 0,
            IncludeLogFieldNames: false
        }).then((response) => {
            setLogText(response?.LogData ?? "");
        }).catch(err => logger.error(err));
    }, [logName]);

    const doDownload = React.useCallback((zip: number) => {
        if (!logName) return;
        window.open(`${getESPBaseURL("WsTopology")}/SystemLog?Name=${encodeURIComponent(logName)}&Type=tpcomp_log&Zip=${zip}`, "_blank");
    }, [logName]);

    return <HolyGrail
        header={
            <Toolbar>
                <ToolbarButton icon={<ArrowClockwise20Regular />} onClick={refreshLogList}>{nlsHPCC.Refresh}</ToolbarButton>
                <Dropdown value={selectedLog} onOptionSelect={(_, data) => {
                    const val = data.optionValue ?? "";
                    setSelectedLog(val);
                    updateParam("logFile", val);
                }} style={{ minWidth: "300px" }}>
                    {logFiles.map(opt => (
                        <Option key={opt.value} value={opt.value}>{opt.label}</Option>
                    ))}
                </Dropdown>
                <ToolbarDivider />
                <ToolbarButton icon={<ArrowDownload20Regular />} disabled={!logName} onClick={() => doDownload(1)}>{nlsHPCC.Text}</ToolbarButton>
                <ToolbarButton icon={<ArrowDownload20Regular />} disabled={!logName} onClick={() => doDownload(2)}>{nlsHPCC.Zip}</ToolbarButton>
                <ToolbarButton icon={<ArrowDownload20Regular />} disabled={!logName} onClick={() => doDownload(3)}>{nlsHPCC.GZip}</ToolbarButton>
            </Toolbar>
        }
        main={<SourceEditor text={logText} readonly={true} mode="text" wordWrap={true} />}
    />;
};