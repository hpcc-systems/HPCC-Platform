import * as React from "react";
import { Checkbox, CommandBar, ICommandBarItemProps } from "@fluentui/react";
import { Level } from "@hpcc-js/util";
import { logColor } from "src/Utility";
import nlsHPCC from "src/nlsHPCC";
import { HolyGrail } from "../layouts/HolyGrail";
import { useECLWatchLogger } from "../hooks/logging";
import { FluentGrid, useCopyButtons, useFluentStoreState, FluentColumns } from "./controls/Grid";

interface LogViewerProps {
}

export const LogViewer: React.FunctionComponent<LogViewerProps> = ({
}) => {

    const [errorChecked, setErrorChecked] = React.useState(true);
    const [warningChecked, setWarningChecked] = React.useState(true);
    const [infoChecked, setInfoChecked] = React.useState(true);
    const [otherChecked, setOtherChecked] = React.useState(true);
    const [filterCounts, setFilterCounts] = React.useState<any>({});
    const [log, lastUpdate] = useECLWatchLogger();
    const [data, setData] = React.useState<any[]>([]);
    const {
        selection, setSelection,
        setTotal,
        refreshTable } = useFluentStoreState({});

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        { key: "errors", onRender: () => <Checkbox defaultChecked label={`${filterCounts.error || 0} ${nlsHPCC.Errors}`} onChange={(ev, value) => setErrorChecked(value)} styles={{ root: { paddingTop: 8, paddingRight: 8 } }} /> },
        { key: "warnings", onRender: () => <Checkbox defaultChecked label={`${filterCounts.warning || 0} ${nlsHPCC.Warnings}`} onChange={(ev, value) => setWarningChecked(value)} styles={{ root: { paddingTop: 8, paddingRight: 8 } }} /> },
        { key: "infos", onRender: () => <Checkbox defaultChecked label={`${filterCounts.info || 0} ${nlsHPCC.Infos}`} onChange={(ev, value) => setInfoChecked(value)} styles={{ root: { paddingTop: 8, paddingRight: 8 } }} /> },
        { key: "others", onRender: () => <Checkbox defaultChecked label={`${filterCounts.other || 0} ${nlsHPCC.Others}`} onChange={(ev, value) => setOtherChecked(value)} styles={{ root: { paddingTop: 8, paddingRight: 8 } }} /> }
    ], [filterCounts.error, filterCounts.info, filterCounts.other, filterCounts.warning]);

    //  Grid ---
    const columns = React.useMemo((): FluentColumns => {
        return {
            dateTime: { label: nlsHPCC.Time, width: 160, sortable: false },
            level: {
                label: nlsHPCC.Severity,
                width: 112,
                sortable: false,
                formatter: level => {
                    const colors = logColor(level);
                    const styles = { backgroundColor: colors.background, padding: "2px 6px", color: colors.foreground };
                    return <span style={styles}>{Level[level].toUpperCase()}</span>;
                },
                csvFormatter: level => Level[level].toUpperCase()
            },
            id: { label: nlsHPCC.Source, width: 212, sortable: false },
            message: { label: nlsHPCC.Message, width: 720, sortable: false }
        };
    }, []);

    const copyButtons = useCopyButtons(columns, selection, "errorwarnings");

    React.useEffect(() => {
        const filterCounts = {
            error: 0,
            warning: 0,
            info: 0,
            other: 0
        };
        const filteredExceptions = log.map((row, idx) => {
            switch (row.level) {
                case Level.error:
                    filterCounts.error++;
                    break;
                case Level.warning:
                    filterCounts.warning++;
                    break;
                case Level.info:
                    filterCounts.info++;
                    break;
                default:
                    filterCounts.other++;
                    break;
            }
            return {
                id: idx,
                ...row
            };
        }).filter(row => {
            if (!errorChecked && row.level === Level.error) {
                return false;
            } else if (!warningChecked && row.level === Level.warning) {
                return false;
            } else if (!infoChecked && row.level === Level.info) {
                return false;
            } else if (!otherChecked && row.level !== Level.error && row.level !== Level.warning && row.level !== Level.info) {
                return false;
            }
            return true;
        }).sort((l, r) => {
            return l.level - r.level;
        });
        setData(filteredExceptions);
        setFilterCounts(filterCounts);
    }, [errorChecked, infoChecked, log, otherChecked, warningChecked, lastUpdate]);

    return <HolyGrail
        header={<CommandBar items={buttons} farItems={copyButtons} />}
        main={
            <FluentGrid
                data={data}
                primaryID={"dateTime"}
                columns={columns}
                setSelection={setSelection}
                setTotal={setTotal}
                refresh={refreshTable}
            ></FluentGrid>}
    />;
};
