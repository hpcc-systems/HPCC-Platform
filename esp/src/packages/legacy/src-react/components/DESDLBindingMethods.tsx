import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, MessageBar, MessageBarType, ScrollablePane, ScrollbarVisibility, Sticky, StickyPositionType } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import { scopedLogger } from "@hpcc-js/util";
import * as Observable from "dojo/store/Observable";
import { MemoryTreeStore } from "src/store/Tree";
import nlsHPCC from "src/nlsHPCC";
import * as WsESDLConfig from "src/WsESDLConfig";
import { useGrid } from "../hooks/grid";
import { ShortVerticalDivider } from "./Common";
import { editor, tree } from "./DojoGrid";

const logger = scopedLogger("src-react/components/ESDLBindingMethods.tsx");

interface DESDLBindingMethodsProps {
    name: string
}

export const DESDLBindingMethods: React.FunctionComponent<DESDLBindingMethodsProps> = ({
    name
}) => {

    const [binding, setBinding] = React.useState<any>();
    const [showError, setShowError] = React.useState(false);
    const [errorMessage, setErrorMessage] = React.useState("");
    const [messageBarType, setMessageBarType] = React.useState<MessageBarType>();

    //  Grid ---
    const store = useConst(() => new Observable(new MemoryTreeStore("__hpcc_id", "__hpcc_parentName", { Name: true })));
    const { Grid, refreshTable } = useGrid({
        store,
        query: { __hpcc_parentName: null },
        sort: { attribute: "Name", descending: false },
        filename: "esdlBindingsMethods",
        columns: {
            Name: tree({
                label: nlsHPCC.Methods,
                width: 500
            }),
            Value: editor({
                label: nlsHPCC.MethodConfiguration,
                autoSave: true,
                canEdit: function (object, value) {
                    if (object.Attributes || !object.__hpcc_parentName) {
                        return false;
                    }
                    return true;
                },
                editor: "textarea",
                editorArgs: {
                    rows: 10
                }
            })
        }
    });

    const refreshGrid = React.useCallback(() => {
        let results = [];
        const rows = [];

        WsESDLConfig.GetESDLBinding({ request: { EsdlBindingId: name, IncludeInterfaceDefinition: true, ReportMethodsAvailable: true } })
            .then(({ GetESDLBindingResponse }) => {

                setBinding(GetESDLBindingResponse);

                results = GetESDLBindingResponse?.ESDLBinding?.Configuration?.Methods?.Method;
                results.forEach((row, idx) => {
                    results[idx] = { ...row, ...{ __hpcc_parentName: null, __hpcc_id: row.Name } };
                    if (row.XML) {
                        rows.push({
                            __hpcc_parentName: row.Name,
                            __hpcc_id: row.Name + idx,
                            Name: row.Name,
                            Value: row.XML
                        });
                    } else {
                        rows.push({
                            __hpcc_parentName: row.Name,
                            __hpcc_id: row.Name + idx,
                            Name: row.Name,
                            Value: "<Method name=\"" + row.Name + "\"/>"
                        });
                    }
                });
                rows.forEach(row => results.push(row));
                store.setData(results);
                refreshTable();
            })
            .catch(err => logger.error(err))
            ;
    }, [name, refreshTable, store]);

    const saveConfiguration = React.useCallback(() => {

        store.query().then(results => {
            let userXML = "";

            results.forEach((row, idx) => {
                if (row.__hpcc_parentName !== null && row.Value !== "") {
                    userXML += row.Value;
                }
            });

            const xmlBuilder = "<Methods>" + userXML + "</Methods>";
            WsESDLConfig.PublishESDLBinding({
                request: {
                    EspProcName: binding?.EspProcName,
                    EspBindingName: binding?.BindingName,
                    EspPort: binding?.EspPort,
                    EsdlDefinitionID: binding?.ESDLBinding?.Definition?.Id,
                    Overwrite: true,
                    Config: xmlBuilder
                }
            }).then(({ PublishESDLBindingResponse }) => {
                setErrorMessage(PublishESDLBindingResponse?.status?.Description);
                setShowError(true);
                if (PublishESDLBindingResponse?.status.Code === 0) {
                    setMessageBarType(MessageBarType.success);
                } else {
                    setMessageBarType(MessageBarType.error);
                }
                refreshGrid();
            });
        });
    }, [binding, refreshGrid, store]);

    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => refreshGrid()
        },
        { key: "divider_4", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "save", text: nlsHPCC.Save,
            onClick: () => saveConfiguration()
        },
    ], [refreshGrid, saveConfiguration]);

    React.useEffect(() => {
        refreshGrid();
    }, [refreshGrid]);

    return <>
        <ScrollablePane scrollbarVisibility={ScrollbarVisibility.auto}>
            {showError &&
                <MessageBar messageBarType={messageBarType} isMultiline={true} onDismiss={() => setShowError(false)} dismissButtonAriaLabel="Close">
                    {errorMessage}
                </MessageBar>
            }
            <Sticky stickyPosition={StickyPositionType.Header}>
                <CommandBar items={buttons} />
            </Sticky>
            <Grid />
        </ScrollablePane>
    </>;

};