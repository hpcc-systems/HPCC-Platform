import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import { AlphaNumSortMemory } from "src/Memory";
import * as Observable from "dojo/store/Observable";
import nlsHPCC from "src/nlsHPCC";
import { useGrid } from "../hooks/grid";
import { useWorkunitWorkflows } from "../hooks/workunit";
import { HolyGrail } from "../layouts/HolyGrail";
import { ShortVerticalDivider } from "./Common";

interface WorkflowsProps {
    wuid: string;
}

export const Workflows: React.FunctionComponent<WorkflowsProps> = ({
    wuid
}) => {

    const [workflows, , refreshWorkflow] = useWorkunitWorkflows(wuid);

    //  Grid ---
    const store = useConst(new Observable(new AlphaNumSortMemory("__hpcc_id", { Name: true, Value: true })));
    const [Grid, _selection, refreshTable, copyButtons] = useGrid({
        store,
        sort: [{ attribute: "Wuid", "descending": true }],
        filename: "workflows",
        columns: {
            EventName: { label: nlsHPCC.Name, width: 180 },
            EventText: { label: nlsHPCC.Subtype },
            Count: {
                label: nlsHPCC.Count, width: 180,
                formatter: function (count) {
                    if (count === -1) {
                        return 0;
                    }
                    return count;
                }
            },
            CountRemaining: {
                label: nlsHPCC.Remaining, width: 180,
                formatter: function (countRemaining) {
                    if (countRemaining === -1) {
                        return 0;
                    }
                    return countRemaining;
                }
            }
        }
    });

    React.useEffect(() => {
        store.setData(workflows.map(row => {
            return {
                ...row,
                __hpcc_id: row.WFID
            };
        }));
        refreshTable();
    }, [store, refreshTable, workflows]);

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => {
                refreshWorkflow();
            }
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
    ], [refreshWorkflow]);

    return <HolyGrail
        header={<CommandBar items={buttons} overflowButtonProps={{}} farItems={copyButtons} />}
        main={
            <Grid />
        }
    />;
};
