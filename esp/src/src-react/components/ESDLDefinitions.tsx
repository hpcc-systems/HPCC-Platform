import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import { scopedLogger } from "@hpcc-js/util";
import * as Observable from "dojo/store/Observable";
import { Memory } from "src/Memory";
import { useConfirm } from "../hooks/confirm";
import { useGrid } from "../hooks/grid";
import nlsHPCC from "src/nlsHPCC";
import * as WsESDLConfig from "src/WsESDLConfig";
import { ShortVerticalDivider } from "./Common";
import { selector } from "./DojoGrid";
import { XMLSourceEditor } from "./SourceEditor";
import { HolyGrail } from "../layouts/HolyGrail";
import { ReflexContainer, ReflexElement, ReflexSplitter } from "../layouts/react-reflex";
import { AddBindingForm } from "./forms/AddBinding";

const logger = scopedLogger("src-react/components/DynamicESDL.tsx");

const defaultUIState = {
    hasSelection: false,
};

interface ESDLDefinitonsProps {
}

export const ESDLDefinitions: React.FunctionComponent<ESDLDefinitonsProps> = ({
}) => {

    const [definition, setDefinition] = React.useState("");
    const [showAddBinding, setShowAddBinding] = React.useState(false);
    const [uiState, setUIState] = React.useState({ ...defaultUIState });

    //  Grid ---
    const store = useConst(new Observable(new Memory("__hpcc_id")));
    const [Grid, selection, refreshTable] = useGrid({
        store: store,
        query: {},
        sort: [{ attribute: "Name", "descending": false }],
        filename: "esdlDefinitions",
        columns: {
            col1: selector({ width: 30, selectorType: "radio", unhidable: true }),
            Name: { label: nlsHPCC.Process, width: 140 },
            PublishBy: { label: nlsHPCC.PublishedBy, width: 140 },
            CreatedTime: { label: nlsHPCC.CreatedTime, width: 140 },
            LastEditBy: { label: nlsHPCC.LastEditedBy, width: 140 },
            LastEditTime: { label: nlsHPCC.LastEditTime, width: 140 }
        }
    });

    //  Selection  ---
    React.useEffect(() => {
        const state = { ...defaultUIState };
        state.hasSelection = selection.length > 0;
        setUIState(state);
        if (selection[0]) {
            WsESDLConfig.GetESDLDefinition({
                request: { Id: selection[0].Name }
            })
                .then(({ GetESDLDefinitionResponse }) => {
                    setDefinition(GetESDLDefinitionResponse?.Definition?.Interface);
                })
                .catch(logger.error)
                ;
        }
    }, [selection]);

    const refreshGrid = React.useCallback(() => {
        WsESDLConfig.ListESDLDefinitions({})
            .then(({ ListESDLDefinitionsResponse }) => {
                const definitions = ListESDLDefinitionsResponse?.Definitions?.Definition;
                if (definitions) {
                    store.setData(definitions.map((defn, idx) => {
                        return {
                            __hpcc_id: idx,
                            Name: defn.Id,
                            PublishBy: defn?.History.PublishBy,
                            CreatedTime: defn?.History.CreatedTime,
                            LastEditBy: defn?.History.LastEditBy,
                            LastEditTime: defn?.History.LastEditTime
                        };
                    }));
                    refreshTable();
                }
            })
            .catch(logger.error)
            ;
    }, [store, refreshTable]);

    const [DeleteConfirm, setShowDeleteConfirm] = useConfirm({
        title: nlsHPCC.Delete,
        message: nlsHPCC.YouAreAboutToDeleteBinding + "\n\n" + selection.map(binding => binding.Name).join("\n"),
        onSubmit: React.useCallback(() => {
            const requests = [];
            selection.forEach(binding => {
                const name = binding.Name.split(".");
                requests.push(
                    WsESDLConfig.DeleteESDLDefinition({
                        request: {
                            Id: binding.Name,
                            Name: name[0],
                            Seq: name[1]
                        }
                    })
                );
            });
            Promise
                .all(requests)
                .then(() => refreshGrid())
                .catch(logger.error)
                ;
        }, [refreshGrid, selection])
    });

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => refreshTable()
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "delete", text: nlsHPCC.Delete, disabled: !uiState.hasSelection,
            onClick: () => setShowDeleteConfirm(true)
        },
        {
            key: "add", text: nlsHPCC.AddBinding,
            onClick: () => setShowAddBinding(true)
        },
    ], [refreshTable, setShowDeleteConfirm, uiState]);

    React.useEffect(() => {
        refreshGrid();
    }, [refreshGrid]);

    return <>
        <HolyGrail
            header={<CommandBar items={buttons} overflowButtonProps={{}} />}
            main={
                <ReflexContainer orientation="vertical">
                    <ReflexElement>
                        <Grid />
                    </ReflexElement>
                    <ReflexSplitter />
                    <ReflexElement>
                        <XMLSourceEditor text={definition} readonly={true} />
                    </ReflexElement>
                </ReflexContainer>
            }
        />
        <AddBindingForm showForm={showAddBinding} setShowForm={setShowAddBinding} minWidth={420} />
        <DeleteConfirm />
    </>;

};