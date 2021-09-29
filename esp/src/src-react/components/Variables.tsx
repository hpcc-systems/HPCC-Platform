import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import * as Observable from "dojo/store/Observable";
import { AlphaNumSortMemory } from "src/Memory";
import nlsHPCC from "src/nlsHPCC";
import { useGrid } from "../hooks/grid";
import { useWorkunitVariables } from "../hooks/workunit";
import { HolyGrail } from "../layouts/HolyGrail";
import { ShortVerticalDivider } from "./Common";

interface VariablesProps {
    wuid: string;
}

export const Variables: React.FunctionComponent<VariablesProps> = ({
    wuid
}) => {

    const [variables, , , refreshData] = useWorkunitVariables(wuid);

    //  Grid ---
    const store = useConst(new Observable(new AlphaNumSortMemory("__hpcc_id", { Name: true, Value: true })));
    const [Grid, _selection, refreshTable, copyButtons] = useGrid({
        store,
        sort: [{ attribute: "Wuid", "descending": true }],
        filename: "variables",
        columns: {
            Type: { label: nlsHPCC.Type, width: 180 },
            Name: { label: nlsHPCC.Name, width: 360 },
            Value: { label: nlsHPCC.Value }
        }
    });

    React.useEffect(() => {
        store.setData(variables.map((row, idx) => {
            return {
                __hpcc_id: idx,
                ...row
            };
        }));
        refreshTable();
    }, [store, refreshTable, variables]);

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => refreshData()
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
    ], [refreshData]);

    return <HolyGrail
        header={<CommandBar items={buttons} farItems={copyButtons} />}
        main={
            <Grid />
        }
    />;
};
