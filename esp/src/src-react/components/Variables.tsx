import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, ScrollablePane, Sticky } from "@fluentui/react";
import nlsHPCC from "src/nlsHPCC";
import { useFluentGrid } from "../hooks/grid";
import { useWorkunitVariables } from "../hooks/workunit";
import { ShortVerticalDivider } from "./Common";

interface VariablesProps {
    wuid: string;
}

export const Variables: React.FunctionComponent<VariablesProps> = ({
    wuid
}) => {

    const [variables, , , refreshData] = useWorkunitVariables(wuid);
    const [data, setData] = React.useState<any[]>([]);

    //  Grid ---
    const [Grid, _selection, copyButtons] = useFluentGrid({
        data,
        primaryID: "__hpcc_id",
        alphaNumColumns: { Name: true, Value: true },
        sort: [{ attribute: "Wuid", "descending": true }],
        filename: "variables",
        columns: {
            Type: { label: nlsHPCC.Type, width: 180 },
            Name: { label: nlsHPCC.Name, width: 360 },
            Value: { label: nlsHPCC.Value }
        }
    });

    React.useEffect(() => {
        setData(variables.map((row, idx) => {
            return {
                __hpcc_id: idx,
                ...row
            };
        }));
    }, [variables]);

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => refreshData()
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
    ], [refreshData]);

    return <ScrollablePane>
        <Sticky>
            <CommandBar items={buttons} farItems={copyButtons} />
        </Sticky>
        <Grid />
    </ScrollablePane>;
};
