import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, Icon } from "@fluentui/react";
import { SizeMe } from "react-sizeme";
import nlsHPCC from "src/nlsHPCC";
import { HolyGrail } from "../layouts/HolyGrail";
import { useServices } from "../hooks/resources";
import { FluentGrid, useCopyButtons, useFluentStoreState, FluentColumns } from "./controls/Grid";
import { ShortVerticalDivider } from "./Common";

interface ServicesProps {
}

export const Services: React.FunctionComponent<ServicesProps> = ({
}) => {
    const [services, refreshData] = useServices();
    const {
        selection, setSelection,
        setTotal,
        refreshTable } = useFluentStoreState({});

    //  Grid ---
    const columns = React.useMemo((): FluentColumns => {
        return {
            Name: { label: nlsHPCC.Name, width: 200 },
            Type: { label: nlsHPCC.Container, width: 200 },
            Port: { label: nlsHPCC.Port, width: 120 },
            TLSSecure: {
                headerIcon: "LockSolid", label: nlsHPCC.TLS, width: 16,
                formatter: secure => secure === true ? <Icon iconName="LockSolid" /> : ""
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
    ], [refreshData]);

    const copyButtons = useCopyButtons(columns, selection, "services");

    return <HolyGrail
        header={<CommandBar items={buttons} farItems={copyButtons} />}
        main={
            <SizeMe monitorHeight>{({ size }) =>
                <div style={{ width: "100%", height: "100%" }}>
                    <div style={{ position: "absolute", width: "100%", height: `${size.height}px` }}>
                        <FluentGrid
                            data={services}
                            primaryID={"Name"}
                            columns={columns}
                            height={`${size.height}px`}
                            setSelection={setSelection}
                            setTotal={setTotal}
                            refresh={refreshTable}
                        ></FluentGrid>
                    </div>
                </div>
            }</SizeMe>
        }
    />;
};
