import * as React from "react";
import { List, ListItem, makeStyles, mergeClasses, SelectionItemId, tokens } from "@fluentui/react-components";
import { TopologyService } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import { SourceEditor } from "./SourceEditor";
import { HolyGrail } from "../layouts/HolyGrail";
import { DockPanel, DockPanelItem, ResetableDockPanel } from "../layouts/DockPanel";
import { AutosizeComponent } from "../layouts/HpccJSAdapter";
import nlsHPCC from "src/nlsHPCC";

const logger = scopedLogger("src-react/components/Configuration.tsx");

const useStyles = makeStyles({
    listContainer: {
        width: "100%",
        height: "100%",
        overflow: "auto"
    },
    item: {
        cursor: "pointer",
        padding: "8px 12px",
        justifyContent: "space-between",
    },
    itemSelected: {
        backgroundColor: tokens.colorSubtleBackgroundSelected,
        "@media (forced-colors:active)": {
            background: "Highlight",
        },
    },
});

const service = new TopologyService({ baseUrl: "" });

interface ConfigurationProps {
}

export const Configuration: React.FunctionComponent<ConfigurationProps> = ({
}) => {

    const classes = useStyles();
    const [dockpanel, setDockpanel] = React.useState<ResetableDockPanel>();
    const [components, setComponents] = React.useState<string[]>([]);
    const [selectedComponent, setSelectedComponent] = React.useState<string>("");
    const [selectedItems, setSelectedItems] = React.useState<SelectionItemId[]>([]);
    const [configYaml, setConfigYaml] = React.useState<string>("");

    React.useEffect(() => {
        let cancelled = false;
        service.TpConfiguredComponents({}).then(response => {
            if (!cancelled) {
                const comps = response.ConfiguredComponents?.Item ?? [];
                setComponents(comps);
                if (comps.length > 0) {
                    setSelectedComponent(comps[0] ?? "");
                    setSelectedItems([comps[0] ?? ""]);
                }
            }
        }).catch(err => {
            logger.error(err);
        });
        return () => {
            cancelled = true;
        };
    }, []);

    React.useEffect(() => {
        let cancelled = false;
        if (!selectedComponent) {
            setConfigYaml("");
            return;
        }

        service.TpComponentConfiguration({
            ComponentNames: {
                Item: [selectedComponent]
            }
        }).then(response => {
            if (!cancelled) {
                const config = response.Results.Result[0].Configuration ?? "";
                setConfigYaml(config);
            }
        }).catch(err => {
            logger.error(err);
            if (!cancelled) {
                setConfigYaml(`${nlsHPCC.Error}: ${err.message}`);
            }
        });
        return () => {
            cancelled = true;
        };
    }, [selectedComponent]);

    const onSelectionChange = React.useCallback((_evt: React.SyntheticEvent | Event, data: { selectedItems: SelectionItemId[] }) => {
        setSelectedItems(data.selectedItems);
        if (data.selectedItems.length > 0) {
            setSelectedComponent(data.selectedItems[0].toString());
        }
    }, []);

    const onFocus = React.useCallback((evt: React.FocusEvent<HTMLLIElement>) => {
        if (evt.target !== evt.currentTarget) {
            return;
        }
        const value = evt.currentTarget.dataset.value as string;
        setSelectedItems([value]);
        setSelectedComponent(value);
    }, []);

    React.useEffect(() => {
        if (dockpanel) {
            //  Should only happen once on startup  ---
            const t = window.setTimeout(() => {
                const layout: any = dockpanel.layout();
                if (Array.isArray(layout?.main?.sizes) && layout.main.sizes.length === 2) {
                    layout.main.sizes = [0.2, 0.8];
                    // calling the sync render here instead of lazyRender fixes an issue
                    // where the hideSingleTabs wasn't repected on initial load
                    dockpanel.layout(layout).render();
                }
                window.clearTimeout(t);
            }, 100);
        }
    }, [dockpanel]);

    return <HolyGrail
        main={
            <DockPanel hideSingleTabs onCreate={setDockpanel}>
                <DockPanelItem key="componentsList" title="Components">
                    <AutosizeComponent>
                        <div className={classes.listContainer}>
                            <List
                                selectionMode="single"
                                selectedItems={selectedItems}
                                onSelectionChange={onSelectionChange}
                            >
                                {components.map((component, idx) => {
                                    return <ListItem
                                        key={component ?? `config_${idx}`}
                                        value={component}
                                        className={mergeClasses(
                                            classes.item,
                                            selectedItems.includes(component) && classes.itemSelected
                                        )}
                                        onFocus={onFocus}
                                        checkmark={null}
                                    >{component}</ListItem>;
                                })}
                            </List>
                        </div>
                    </AutosizeComponent>
                </DockPanelItem>
                <DockPanelItem key="configEditor" title="Configuration" padding={4} location="split-right" relativeTo="componentsList">
                    <SourceEditor text={configYaml} readonly={true} mode="yaml" />
                </DockPanelItem>
            </DockPanel>
        }
    />;

};
