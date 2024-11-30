import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, Image, Link } from "@fluentui/react";
import * as Utility from "src/Utility";
import nlsHPCC from "src/nlsHPCC";
import { QuerySortItem } from "src/store/Store";
import { useFile } from "../hooks/file";
import { HolyGrail } from "../layouts/HolyGrail";
import { FluentGrid, useCopyButtons, useFluentStoreState, FluentColumns } from "./controls/Grid";
import { ShortVerticalDivider } from "./Common";

function getStateImageName(row) {
    if (row.Complete) {
        return "workunit_completed.png";
    } else if (row.Running) {
        return "workunit_running.png";
    } else if (row.Failed) {
        return "workunit_failed.png";
    }
    return "workunit.png";
}

const defaultUIState = {
    hasSelection: false
};

interface FileDetailsGraphProps {
    cluster?: string;
    logicalFile: string;
    sort?: QuerySortItem;
}

const defaultSort = { attribute: "Name", descending: false };

export const FileDetailsGraph: React.FunctionComponent<FileDetailsGraphProps> = ({
    cluster,
    logicalFile,
    sort = defaultSort
}) => {

    const [file, , , refreshData] = useFile(cluster, logicalFile);
    const [uiState, setUIState] = React.useState({ ...defaultUIState });
    const [data, setData] = React.useState<any[]>([]);
    const {
        selection, setSelection,
        setTotal,
        refreshTable } = useFluentStoreState({});

    //  Grid ---
    const columns = React.useMemo((): FluentColumns => {
        return {
            col1: {
                width: 27,
                selectorType: "checkbox"
            },
            Name: {
                label: nlsHPCC.Name, sortable: true,
                formatter: (Name, row) => {
                    return <>
                        <Image src={Utility.getImageURL(getStateImageName(row))} />
                        &nbsp;
                        <Link href={`#/workunits/${row?.Wuid}/metrics/${Name}`}>{Name}</Link>
                    </>;
                }
            }
        };
    }, []);

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => refreshData()
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "open", text: nlsHPCC.Open, disabled: !uiState.hasSelection, iconProps: { iconName: "WindowEdit" },
            onClick: () => {
                if (selection.length === 1) {
                    window.location.href = `#/workunits/${file?.Wuid}/metrics/${selection[0].Name}`;
                } else {
                    for (let i = 0; i < selection.length; ++i) {
                        window.open(`#/workunits/${file?.Wuid}/metrics/${selection[i].Name}`, "_blank");
                    }
                }
            }
        }
    ], [file?.Wuid, refreshData, selection, uiState.hasSelection]);

    //  Selection  ---
    React.useEffect(() => {
        const state = { ...defaultUIState };
        state.hasSelection = selection.length > 0;
        setUIState(state);
    }, [selection]);

    React.useEffect(() => {
        setData((file?.Graphs?.ECLGraph || []).map(item => {
            return {
                Name: item,
                Label: "",
                Completed: "",
                Time: 0,
                Type: "",
                Wuid: file?.Wuid
            };
        }));
    }, [file?.Graphs?.ECLGraph, file?.Wuid]);

    const copyButtons = useCopyButtons(columns, selection, "graphs");

    return <HolyGrail
        header={<CommandBar items={buttons} farItems={copyButtons} />}
        main={
            <FluentGrid
                data={data}
                primaryID={"Name"}
                sort={sort}
                columns={columns}
                setSelection={setSelection}
                setTotal={setTotal}
                refresh={refreshTable}
            ></FluentGrid>
        }
    />;
};
