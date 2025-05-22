import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import nlsHPCC from "src/nlsHPCC";
import { QuerySortItem } from "src/store/Store";
import { useBuildInfo } from "../hooks/platform";
import { useWorkunitProcesses } from "../hooks/workunit";
import { HolyGrail } from "../layouts/HolyGrail";
import { FluentGrid, useCopyButtons, useFluentStoreState, FluentColumns } from "./controls/Grid";
import { ShortVerticalDivider } from "./Common";

interface WorkflowsProps {
    wuid: string;
    sort?: QuerySortItem;
}

const defaultSort = { attribute: "Wuid", descending: true };

export const Processes: React.FunctionComponent<WorkflowsProps> = ({
    wuid,
    sort = defaultSort
}) => {

    const [, { isContainer }] = useBuildInfo();

    const [processes, , refresh] = useWorkunitProcesses(wuid);
    const [data, setData] = React.useState<any[]>([]);
    const {
        selection, setSelection,
        setTotal,
        refreshTable } = useFluentStoreState({});

    //  Grid ---
    const columns = React.useMemo((): FluentColumns => {
        return {
            Name: { label: nlsHPCC.Name, width: 180 },
            Type: { label: nlsHPCC.Type },
            ...(
                isContainer ?
                    {
                        PodName: { label: nlsHPCC.PodName, width: 240 }
                    } :
                    {
                        Log: { label: nlsHPCC.Log, width: 400 },
                        PID: { label: nlsHPCC.ProcessID },
                    }
            ),
            InstanceNumber: { label: nlsHPCC.InstanceNumber, width: 120 },
            Max: { label: nlsHPCC.Max }
        };
    }, [isContainer]);

    React.useEffect(() => {
        setData(processes.map(row => {
            return {
                ...row,
                __hpcc_id: row.PID
            };
        }));
    }, [processes]);

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => {
                refresh();
            }
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
    ], [refresh]);

    const copyButtons = useCopyButtons(columns, selection, "processes");

    return <HolyGrail
        header={<CommandBar items={buttons} farItems={copyButtons} />}
        main={
            <FluentGrid
                data={data}
                primaryID={"__hpcc_id"}
                alphaNumColumns={{ PodName: true, Log: true }}
                sort={sort}
                columns={columns}
                setSelection={setSelection}
                setTotal={setTotal}
                refresh={refreshTable}
            ></FluentGrid>
        }
    />;
};
