import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import { scopedLogger } from "@hpcc-js/util";
import nlsHPCC from "src/nlsHPCC";
import * as WsESDLConfig from "src/WsESDLConfig";
import { useConfirm } from "../hooks/confirm";
import { useFluentGrid } from "../hooks/grid";
import { HolyGrail } from "../layouts/HolyGrail";
import { ReflexContainer, ReflexElement, ReflexSplitter } from "../layouts/react-reflex";
import { ShortVerticalDivider } from "./Common";
import { selector } from "./DojoGrid";
import { XMLSourceEditor } from "./SourceEditor";
import { AddBindingForm } from "./forms/AddBinding";

const logger = scopedLogger("src-react/components/DynamicESDL.tsx");

const defaultUIState = {
    hasSelection: false,
};

interface DESDLDefinitonsProps {
}

export const DESDLDefinitions: React.FunctionComponent<DESDLDefinitonsProps> = ({
}) => {

    const [definition, setDefinition] = React.useState("");
    const [showAddBinding, setShowAddBinding] = React.useState(false);
    const [uiState, setUIState] = React.useState({ ...defaultUIState });
    const [data, setData] = React.useState<any[]>([]);

    //  Grid ---
    const [Grid, selection, copyButtons] = useFluentGrid({
        data,
        primaryID: "__hpcc_id",
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
                .catch(err => logger.error(err))
                ;
        }
    }, [selection]);

    const refreshData = React.useCallback(() => {
        WsESDLConfig.ListESDLDefinitions({})
            .then(({ ListESDLDefinitionsResponse }) => {
                const definitions = ListESDLDefinitionsResponse?.Definitions?.Definition;
                if (definitions) {
                    setData(definitions.map((defn, idx) => {
                        return {
                            __hpcc_id: idx,
                            Name: defn.Id,
                            PublishBy: defn?.History.PublishBy,
                            CreatedTime: defn?.History.CreatedTime,
                            LastEditBy: defn?.History.LastEditBy,
                            LastEditTime: defn?.History.LastEditTime
                        };
                    }));
                }
            })
            .catch(err => logger.error(err))
            ;
    }, []);

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
                .then(() => refreshData())
                .catch(err => logger.error(err))
                ;
        }, [refreshData, selection])
    });

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => refreshData()
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
    ], [refreshData, setShowDeleteConfirm, uiState.hasSelection]);

    React.useEffect(() => {
        refreshData();
    }, [refreshData]);

    return <>
        <HolyGrail
            header={<CommandBar items={buttons} farItems={copyButtons} />}
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