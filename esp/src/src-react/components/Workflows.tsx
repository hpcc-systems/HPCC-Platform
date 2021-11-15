import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, ScrollablePane, Sticky } from "@fluentui/react";
import nlsHPCC from "src/nlsHPCC";
import { useFluentGrid } from "../hooks/grid";
import { useWorkunitWorkflows } from "../hooks/workunit";
import { ShortVerticalDivider } from "./Common";

interface WorkflowsProps {
    wuid: string;
}

export const Workflows: React.FunctionComponent<WorkflowsProps> = ({
    wuid
}) => {

    const [workflows, , refreshWorkflow] = useWorkunitWorkflows(wuid);
    const [data, setData] = React.useState<any[]>([]);

    //  Grid ---
    const [Grid, _selection, copyButtons] = useFluentGrid({
        data,
        primaryID: "__hpcc_id",
        alphaNumColumns: { Name: true, Value: true },
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
        setData(workflows.map(row => {
            return {
                ...row,
                __hpcc_id: row.WFID
            };
        }));
    }, [workflows]);

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

    return <ScrollablePane>
        <Sticky>
            <CommandBar items={buttons} farItems={copyButtons} />
        </Sticky>
        <Grid />
    </ScrollablePane>;
};
