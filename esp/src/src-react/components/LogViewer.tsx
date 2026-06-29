import * as React from "react";
import { CommandBar, ICommandBarItemProps } from "./CommandBarV9";
import { Badge, Checkbox, tokens } from "@fluentui/react-components";
import { Level } from "@hpcc-js/util";
import { logColor } from "src/Utility";
import nlsHPCC from "src/nlsHPCC";
import { QuerySortItem } from "src/store/Store";
import { HolyGrail } from "../layouts/HolyGrail";
import { SizeMe } from "../layouts/SizeMe";
import { useECLWatchLogger } from "../hooks/logging";
import { FluentGrid, useCopyButtons, useFluentStoreState, FluentColumns } from "./controls/Grid";

interface LogViewerProps {
    sort?: QuerySortItem;
}

export const defaultSort = { attribute: "dateTime", descending: true };

export const LogViewer: React.FunctionComponent<LogViewerProps> = ({
    sort = defaultSort
}) => {

    const [errorChecked, setErrorChecked] = React.useState(true);
    const [warningChecked, setWarningChecked] = React.useState(true);
    const [infoChecked, setInfoChecked] = React.useState(true);
    const [otherChecked, setOtherChecked] = React.useState(true);
    const [filterCounts, setFilterCounts] = React.useState<any>({});
    const { log, lastUpdate } = useECLWatchLogger();
    const [data, setData] = React.useState<any[]>([]);
    const {
        selection, setSelection,
        setTotal,
        refreshTable } = useFluentStoreState({});

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        { key: "errors", onRender: () => <Checkbox defaultChecked onChange={(_, data) => setErrorChecked(!!data.checked)} style={{ marginRight: 8 }} label={<><span style={{ color: tokens.colorStatusDangerForeground1 }}>{nlsHPCC.Errors} </span><Badge appearance="tint" color="danger">{filterCounts.error || 0}</Badge></>} /> },
        { key: "warnings", onRender: () => <Checkbox defaultChecked onChange={(_, data) => setWarningChecked(!!data.checked)} style={{ marginRight: 8 }} label={<><span style={{ color: tokens.colorStatusWarningForeground1 }}>{nlsHPCC.Warnings} </span><Badge appearance="tint" color="warning">{filterCounts.warning || 0}</Badge></>} /> },
        { key: "infos", onRender: () => <Checkbox defaultChecked onChange={(_, data) => setInfoChecked(!!data.checked)} style={{ marginRight: 8 }} label={<><span>{nlsHPCC.Infos} </span><Badge appearance="tint" color="informative">{filterCounts.info || 0}</Badge></>} /> },
        { key: "others", onRender: () => <Checkbox defaultChecked onChange={(_, data) => setOtherChecked(!!data.checked)} style={{ marginRight: 8 }} label={<><span>{nlsHPCC.Others} </span><Badge appearance="tint" color="informative">{filterCounts.other || 0}</Badge></>} /> }
    ], [filterCounts.error, filterCounts.info, filterCounts.other, filterCounts.warning]);

    //  Grid ---
    const columns = React.useMemo((): FluentColumns => {
        return {
            dateTime: { label: nlsHPCC.Time, width: 160, },
            level: {
                label: nlsHPCC.Severity,
                width: 112,
                formatter: level => {
                    const colors = logColor(level);
                    const styles = { backgroundColor: colors.background, padding: "2px 6px", color: colors.foreground };
                    return <span style={styles}>{Level[level].toUpperCase()}</span>;
                },
                csvFormatter: level => Level[level].toUpperCase()
            },
            id: { label: nlsHPCC.Source, width: 212 },
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
        });
        setData(filteredExceptions);
        setFilterCounts(filterCounts);
    }, [errorChecked, infoChecked, log, otherChecked, warningChecked, lastUpdate]);

    return <HolyGrail
        header={<CommandBar items={buttons} farItems={copyButtons} />}
        main={
            <SizeMe>{({ size }) =>
                <div style={{ width: "100%", height: "100%" }}>
                    <div style={{ position: "absolute", width: "100%", height: `${size.height}px` }}>
                        <FluentGrid
                            data={data}
                            primaryID={"dateTime"}
                            sort={sort}
                            columns={columns}
                            height={`${size.height}px`}
                            setSelection={setSelection}
                            setTotal={setTotal}
                            refresh={refreshTable}
                        ></FluentGrid>
                    </div>
                </div>
            }</SizeMe>
        }
    />;
};
