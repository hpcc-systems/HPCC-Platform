import * as React from "react";
import { CommandBar, ICommandBarItemProps, ScrollablePane, ScrollbarVisibility, Sticky, StickyPositionType } from "@fluentui/react";
import { scopedLogger } from "@hpcc-js/util";
import { useConfirm } from "../hooks/confirm";
import { TableGroup } from "./forms/Groups";
import * as WsESDLConfig from "src/WsESDLConfig";
import nlsHPCC from "src/nlsHPCC";
import { replaceUrl } from "../util/history";

const logger = scopedLogger("src-react/components/ESDLBindingSummary.tsx");

interface DESDLBindingSummaryProps {
    processName: string,
    serviceName: string,
    port: string,
    bindingName: string
}

export const DESDLBindingSummary: React.FunctionComponent<DESDLBindingSummaryProps> = (props) => {

    const [DeleteConfirm, setShowDeleteConfirm] = useConfirm({
        title: nlsHPCC.Delete,
        message: nlsHPCC.YouAreAboutToDeleteBinding,
        onSubmit: React.useCallback(() => {
            WsESDLConfig.DeleteESDLBinding({ request: { Id: props.bindingName } })
                .then(({ DeleteESDLRegistryEntryResponse }) => {
                    if (DeleteESDLRegistryEntryResponse?.status?.Code === 0) {
                        replaceUrl("/desdl/bindings");
                    }
                })
                .catch(err => logger.error(err))
                ;
        }, [props.bindingName])
    });

    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.DeleteBinding, iconProps: { iconName: "Delete" },
            onClick: () => setShowDeleteConfirm(true)
        }
    ], [setShowDeleteConfirm]);

    return <>
        <ScrollablePane scrollbarVisibility={ScrollbarVisibility.auto}>
            <Sticky stickyPosition={StickyPositionType.Header}>
                <CommandBar items={buttons} />
            </Sticky>
            <TableGroup fields={{
                "ProcessName": { label: nlsHPCC.ESPProcessName, type: "string", value: props.processName, readonly: true },
                "ServiceName": { label: nlsHPCC.ServiceName, type: "string", value: props.serviceName, readonly: true },
                "Port": { label: nlsHPCC.Port, type: "string", value: props.port, readonly: true },
                "Binding": { label: nlsHPCC.Binding, type: "string", value: props.bindingName, readonly: true }
            }} />
        </ScrollablePane>
        <DeleteConfirm />
    </>;
};