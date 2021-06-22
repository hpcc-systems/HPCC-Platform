import * as React from "react";
import { IDragOptions, ContextualMenu, DialogType, Dialog, DialogFooter, DefaultButton, PrimaryButton, Checkbox, Pivot, PivotItem } from "@fluentui/react";
import nlsHPCC from "src/nlsHPCC";
import { useMetricMeta, useMetricsOptions } from "../hooks/metrics";

const dragOptions: IDragOptions = {
    moveMenuItemText: "Move",
    closeMenuItemText: "Close",
    menu: ContextualMenu,
};

interface MetricsOptionsProps {
    show: boolean;
    setShow: (_: boolean) => void;
    layout: any;
}

export const MetricsOptions: React.FunctionComponent<MetricsOptionsProps> = ({
    show,
    setShow,
    layout
}) => {

    const [scopeTypes, properties] = useMetricMeta();
    const [options, setOptions, save, reset] = useMetricsOptions();

    const closeOptions = () => setShow(false);

    const allChecked = scopeTypes.length === options.scopeTypes.length;

    return <Dialog
        hidden={!show}
        onDismiss={closeOptions}
        dialogContentProps={{
            type: DialogType.close,
            title: nlsHPCC.Options,
        }}
        modalProps={{
            isBlocking: false,
            dragOptions,
        }}>
        <Pivot>
            <PivotItem headerText={nlsHPCC.Metrics}>
                <Checkbox key="all" label={nlsHPCC.All} checked={allChecked} onChange={(ev, checked) => {
                    if (checked) {
                        setOptions({
                            ...options, scopeTypes: [...scopeTypes]
                        });
                    }
                }} />
                {scopeTypes.map(st => {
                    return <Checkbox key={st} label={st} checked={options.scopeTypes.indexOf(st) >= 0} onChange={(ev, checked) => {
                        const scopeTypes = options.scopeTypes.filter(row => row !== st);
                        if (checked) {
                            scopeTypes.push(st);
                        }
                        setOptions({ ...options, scopeTypes });
                    }} />;
                })}
            </PivotItem>
            <PivotItem headerText={nlsHPCC.Columns}>
                {properties.map(p => {
                    return <Checkbox key={p} label={p} checked={options.properties.indexOf(p) >= 0} onChange={(ev, checked) => {
                        const properties = options.properties.filter(row => row !== p);
                        if (checked) {
                            properties.push(p);
                        }
                        setOptions({ ...options, properties });
                    }} />;
                })}
            </PivotItem>
            <PivotItem headerText={nlsHPCC.Graph}>
                <Checkbox label={nlsHPCC.IgnoreGlobalStoreOutEdges} checked={options.ignoreGlobalStoreOutEdges} onChange={(ev, checked) => {
                    setOptions({ ...options, ignoreGlobalStoreOutEdges: !!checked });
                }} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.Layout}>
            </PivotItem>
        </Pivot>
        <DialogFooter>
            <PrimaryButton
                text={nlsHPCC.OK}
                onClick={() => {
                    save();
                    closeOptions();
                }}
            />
            <DefaultButton
                text={nlsHPCC.Cancel}
                onClick={() => {
                    reset();
                    closeOptions();
                }}
            />
            <DefaultButton
                text={nlsHPCC.Defaults}
                onClick={() => {
                    reset(true);
                }}
            />
        </DialogFooter>
    </Dialog>;
};