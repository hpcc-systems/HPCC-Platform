import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import nlsHPCC from "src/nlsHPCC";
import { QuerySortItem } from "src/store/Store";
import { Variable } from "../hooks/workunit";
import { HolyGrail } from "../layouts/HolyGrail";
import { FluentGrid, useCopyButtons, useFluentStoreState, FluentColumns } from "./controls/Grid";
import { ShortVerticalDivider } from "./Common";

interface VariablesProps {
    variables: Variable[];
    refreshData: () => void;
    sort?: QuerySortItem;
}

const defaultSort = { attribute: "Wuid", descending: true };

export const Variables: React.FunctionComponent<VariablesProps> = ({
    variables,
    refreshData,
    sort = defaultSort
}) => {

    const [data, setData] = React.useState<any[]>([]);
    const {
        selection, setSelection,
        setTotal,
        refreshTable } = useFluentStoreState({});

    //  Grid ---
    const columns = React.useMemo((): FluentColumns => {
        return {
            Type: { label: nlsHPCC.Type, width: 180 },
            Name: { label: nlsHPCC.Name, width: 360 },
            Value: { label: nlsHPCC.Value }
        };
    }, []);

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

    const copyButtons = useCopyButtons(columns, selection, "variables");

    return <HolyGrail
        header={<CommandBar items={buttons} farItems={copyButtons} />}
        main={
            <FluentGrid
                data={data}
                primaryID={"__hpcc_id"}
                alphaNumColumns={{ Value: true }}
                sort={sort}
                columns={columns}
                setSelection={setSelection}
                setTotal={setTotal}
                refresh={refreshTable}
            ></FluentGrid>
        }
    />;
};
