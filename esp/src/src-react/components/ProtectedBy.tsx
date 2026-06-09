import * as React from "react";
import { CommandBar, ICommandBarItemProps } from "./CommandBarV9";
import { WsDfu } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import nlsHPCC from "src/nlsHPCC";
import { QuerySortItem } from "src/store/Store";
import { useConfirm } from "../hooks/confirm";
import { useFile } from "../hooks/file";
import { useMyAccount } from "../hooks/user";
import { HolyGrail } from "../layouts/HolyGrail";
import { FluentGrid, useCopyButtons, useFluentStoreState, FluentColumns } from "./controls/Grid";

const logger = scopedLogger("src-react/components/ProtectedBy.tsx");

interface ProtectedByProps {
    cluster: string;
    logicalFile: string;
    sort?: QuerySortItem;
}

const defaultSort = { attribute: "Owner", descending: false };

export const ProtectedBy: React.FunctionComponent<ProtectedByProps> = ({
    cluster,
    logicalFile,
    sort = defaultSort
}) => {

    const { file, refreshData } = useFile(cluster, logicalFile);
    const { isAdmin } = useMyAccount();
    const [data, setData] = React.useState<any[]>([]);
    const {
        selection, setSelection,
        setTotal,
        refreshTable } = useFluentStoreState({});

    const [UnprotectAllConfirm, setShowUnprotectAllConfirm] = useConfirm({
        title: nlsHPCC.UnprotectAll,
        message: nlsHPCC.UnprotectAllConfirm,
        onSubmit: React.useCallback(() => {
            file?.update({ Protect: WsDfu.DFUChangeProtection.UnprotectAll })
                .then(() => refreshData())
                .catch(err => logger.error(err));
        }, [file, refreshData])
    });

    //  Grid ---
    const columns = React.useMemo((): FluentColumns => {
        return {
            Owner: { label: nlsHPCC.Owner, width: 320 },
            Modified: { label: nlsHPCC.Modified, width: 320 },
        };
    }, []);

    React.useEffect(() => {
        setData(file?.ProtectList?.DFUFileProtect ?? []);
    }, [file?.ProtectList?.DFUFileProtect]);

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => refreshData()
        },
        {
            key: "unprotectAll", text: nlsHPCC.UnprotectAll, iconProps: { iconName: "Unlock" },
            disabled: data.length === 0 || !isAdmin,
            onClick: () => setShowUnprotectAllConfirm(true)
        },
    ], [data.length, isAdmin, refreshData, setShowUnprotectAllConfirm]);

    const copyButtons = useCopyButtons(columns, selection, "protectedBy");

    return <HolyGrail
        header={<CommandBar items={buttons} farItems={copyButtons} />}
        main={
            <>
                <FluentGrid
                    data={data}
                    primaryID={"Owner"}
                    sort={sort}
                    columns={columns}
                    setSelection={setSelection}
                    setTotal={setTotal}
                    refresh={refreshTable}
                ></FluentGrid>
                <UnprotectAllConfirm />
            </>
        }
    />;
};