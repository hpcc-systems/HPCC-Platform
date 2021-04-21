import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import { AlphaNumSortMemory } from "src/Memory";
import * as Observable from "dojo/store/Observable";
import nlsHPCC from "src/nlsHPCC";
import { useWorkunitWorkflows } from "../hooks/Workunit";
import { HolyGrail } from "../layouts/HolyGrail";
import { createCopyDownloadSelection, ShortVerticalDivider } from "./Common";
import { DojoGrid } from "./DojoGrid";

interface WorkflowsProps {
    wuid: string;
}

export const Workflows: React.FunctionComponent<WorkflowsProps> = ({
    wuid
}) => {

    const [grid, setGrid] = React.useState<any>(undefined);
    const [selection, setSelection] = React.useState([]);
    const [workflows, , refreshWorkflow] = useWorkunitWorkflows(wuid);

    //  Command Bar  ---
    const buttons: ICommandBarItemProps[] = [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => {
                refreshWorkflow();
            }
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
    ];

    const rightButtons: ICommandBarItemProps[] = [
        ...createCopyDownloadSelection(grid, selection, "workflows.csv")
    ];

    //  Grid ---
    const gridStore = useConst(new Observable(new AlphaNumSortMemory("__hpcc_id", { Name: true, Value: true })));
    const gridQuery = useConst({});
    const gridSort = useConst([{ attribute: "Wuid", "descending": true }]);
    const gridColumns = useConst({
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
    });

    const refreshTable = (clearSelection = false) => {
        grid?.set("query", gridQuery);
        if (clearSelection) {
            grid?.clearSelection();
        }
    };

    React.useEffect(() => {
        gridStore.setData(workflows.map(row => {
            return {
                ...row,
                __hpcc_id: row.WFID
            };
        }));
        refreshTable();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [gridStore, workflows]);

    return <HolyGrail
        header={<CommandBar items={buttons} overflowButtonProps={{}} farItems={rightButtons} />}
        main={
            <DojoGrid type="SimpleGrid" store={gridStore} query={gridQuery} sort={gridSort} columns={gridColumns} setGrid={setGrid} setSelection={setSelection} />
        }
    />;
};
