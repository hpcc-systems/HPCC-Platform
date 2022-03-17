import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, Image, Link } from "@fluentui/react";
import { MachineService } from "@hpcc-js/comms";
import { ShortVerticalDivider } from "./Common";
import { tree } from "./DojoGrid";
import { useFluentGrid } from "../hooks/grid";
import { HolyGrail } from "../layouts/HolyGrail";
import * as Utility from "src/Utility";
import nlsHPCC from "src/nlsHPCC";

function getStatusImageName(row) {
    switch (row.Status) {
        case "Error":
            return "error.png";
        case "Warning":
            return "warning.png";
        case "Normal":
        default:
            return "normal.png";
    }
}

const machine = new MachineService({ baseUrl: "" });

const defaultUIState = {
    hasSelection: false
};

interface MonitoringProps {

}

export const Monitoring: React.FunctionComponent<MonitoringProps> = ({

}) => {

    const [uiState, setUIState] = React.useState({ ...defaultUIState });
    const [data, setData] = React.useState<any[]>([]);

    //  Grid ---
    const { Grid, selection, copyButtons } = useFluentGrid({
        data,
        primaryID: "__hpcc_id",
        sort: { attribute: "Name", "descending": false },
        filename: "monitoring",
        columns: {
            StatusID: { label: "", width: 0, sortable: false, hidden: true },
            ComponentType: tree({
                label: "Name", sortable: true, width: 200,
                formatter: function (Name, row) {
                    return <Image src={Utility.getImageURL(getStatusImageName(row))} />;
                }
            }),
            StatusDetails: { label: "Details", sortable: false },
            URL: {
                label: "URL", width: 200, sortable: false,
                formatter: function (Name, row) {
                    if (Name) {
                        return <Link href={`http://${Name}`} target="_blank">{Name}</Link>;
                    } else {
                        return "";
                    }
                }
            },
            EndPoint: { label: "IP", sortable: true, width: 140 },
            TimeReportedStr: { label: "Time Reported", width: 140, sortable: true },
            Status: {
                label: nlsHPCC.Severity, width: 130, sortable: false,
                formatter: function (object, value, node, options) {
                    switch (value) {
                        case "Error":
                            node.classList.add("ErrorCell");
                            break;
                        case "Warning":
                            node.classList.add("WarningCell");
                            break;
                        case "Normal":
                            node.classList.add("NormalCell");
                            break;
                    }
                    node.innerText = value;
                }
            }
        }
    });

    const refreshData = React.useCallback(() => {
        machine.GetComponentStatus({
            request: {}
        }).then(({ ComponentStatusList }) => {
            const componentStatuses = ComponentStatusList?.ComponentStatus;
            if (componentStatuses) {
                setData(componentStatuses.map((status, idx) => {
                    return status?.StatusReports?.StatusReport.map((statusReport, idx) => {
                        return {
                            __hpcc_parentName: status.ComponentType + status.EndPoint,
                            __hpcc_id: status.ComponentType + status.EndPoint + "_" + idx,
                            ComponentType: statusReport.Reporter,
                            Status: statusReport.Status,
                            StatusDetails: statusReport.StatusDetails,
                            URL: statusReport.URL
                        };
                    });
                }).flat());
            }
        });
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
                    window.location.href = `http://${selection[0].Name}`;
                } else {
                    for (let i = 0; i < selection.length; ++i) {
                        window.open(`http://${selection[i].Name}`, "_blank");
                    }
                }
            }
        }
    ], [refreshData, selection, uiState.hasSelection]);

    //  Selection  ---
    React.useEffect(() => {
        const state = { ...defaultUIState };
        state.hasSelection = selection.length > 0;
        setUIState(state);
    }, [selection]);

    return <HolyGrail
        header={<CommandBar items={buttons} farItems={copyButtons} />}
        main={
            <Grid />
        }
    />;
};