import * as React from "react";
import { Checkbox, CommandBar, ICommandBarItemProps } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import * as Observable from "dojo/store/Observable";
import { Memory } from "src/Memory";
import nlsHPCC from "src/nlsHPCC";
import { useGrid } from "../hooks/grid";
import { HolyGrail } from "../layouts/HolyGrail";
import { useECLWatchLogger } from "../hooks/logging";
import { Level } from "@hpcc-js/util";

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

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        { key: "errors", onRender: () => <Checkbox defaultChecked label={`${filterCounts.error || 0} ${nlsHPCC.Errors}`} onChange={(ev, value) => setErrorChecked(value)} styles={{ root: { paddingTop: 8, paddingRight: 8 } }} /> },
        { key: "warnings", onRender: () => <Checkbox defaultChecked label={`${filterCounts.warning || 0} ${nlsHPCC.Warnings}`} onChange={(ev, value) => setWarningChecked(value)} styles={{ root: { paddingTop: 8, paddingRight: 8 } }} /> },
        { key: "infos", onRender: () => <Checkbox defaultChecked label={`${filterCounts.info || 0} ${nlsHPCC.Infos}`} onChange={(ev, value) => setInfoChecked(value)} styles={{ root: { paddingTop: 8, paddingRight: 8 } }} /> },
        { key: "others", onRender: () => <Checkbox defaultChecked label={`${filterCounts.other || 0} ${nlsHPCC.Others}`} onChange={(ev, value) => setOtherChecked(value)} styles={{ root: { paddingTop: 8, paddingRight: 8 } }} /> }
    ], [filterCounts.error, filterCounts.info, filterCounts.other, filterCounts.warning]);

    //  Grid ---
    const store = useConst(new Observable(new Memory("dateTime")));
    const [Grid, _selection, refreshTable, copyButtons] = useGrid({
        store,
        filename: "errorwarnings",
        columns: {
            dateTime: { label: nlsHPCC.Time, width: 160, sortable: false },
            level: { label: nlsHPCC.Severity, width: 112, sortable: false, formatter: level => Level[level].toUpperCase() },
            id: { label: nlsHPCC.Source, width: 212, sortable: false },
            message: { label: nlsHPCC.Message, sortable: false }
        }
    });

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
        store.setData(filteredExceptions);
        refreshTable();
        setFilterCounts(filterCounts);
    }, [errorChecked, store, infoChecked, log, otherChecked, refreshTable, warningChecked, lastUpdate]);

    return <HolyGrail
        header={<CommandBar items={buttons} overflowButtonProps={{}} farItems={copyButtons} />}
        main={
            <Grid />
        }
    />;
};
