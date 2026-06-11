import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "./CommandBarV9";
import nlsHPCC from "src/nlsHPCC";
import { QuerySortItem } from "src/store/Store";
import { useWorkunitFileSummaries } from "../hooks/workunit";
import { HolyGrail } from "../layouts/HolyGrail";
import { FluentGrid, useCopyButtons, useFluentStoreState, FluentColumns } from "./controls/Grid";

interface WUFilesSummaryProps {
    wuid: string;
    sort?: QuerySortItem;
}

const defaultSort = { attribute: "Name", descending: false };

export const WUFilesSummary: React.FunctionComponent<WUFilesSummaryProps> = ({
    wuid,
    sort = defaultSort
}) => {

    const [fileSummaries, , refresh] = useWorkunitFileSummaries(wuid);
    const data = React.useMemo(() => fileSummaries.map(row => ({
        ...row,
        __hpcc_id: `${row.Name}_${row.Type}`
    })), [fileSummaries]);
    const {
        selection, setSelection,
        setTotal,
        refreshTable } = useFluentStoreState({});

    //  Grid ---
    const columns = React.useMemo((): FluentColumns => {
        return {
            Name: { label: nlsHPCC.Name, width: 400, sortable: true },
            Type: { label: nlsHPCC.Type, width: 120, sortable: true },
            IsOpt: { label: nlsHPCC.IsOptional, width: 100, sortable: true },
            IsSigned: { label: nlsHPCC.IsSigned, width: 100, sortable: true },
        };
    }, []);

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => { refresh(); }
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider },
    ], [refresh]);

    const copyButtons = useCopyButtons(columns, selection, "wuFilesSummary");

    return <HolyGrail
        header={<CommandBar items={buttons} farItems={copyButtons} />}
        main={
            <FluentGrid
                data={data}
                primaryID={"__hpcc_id"}
                sort={sort}
                columns={columns}
                setSelection={setSelection}
                setTotal={setTotal}
                refresh={refreshTable}
            ></FluentGrid>
        }
    />;
};
