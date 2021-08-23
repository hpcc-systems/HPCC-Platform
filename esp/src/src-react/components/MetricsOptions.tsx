import * as React from "react";
import { DefaultButton, PrimaryButton, Checkbox, Pivot, PivotItem, TextField } from "@fluentui/react";
import nlsHPCC from "src/nlsHPCC";
import { useMetricMeta, useMetricsOptions } from "../hooks/metrics";
import { MessageBox } from "../layouts/MessageBox";

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

    return <MessageBox title={nlsHPCC.Options} show={show} setShow={setShow}
        footer={<>
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
        </>} >
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
                <TextField label={nlsHPCC.SubgraphLabel} value={options.subgraphTpl} multiline autoAdjustHeight onChange={(evt, newValue) => {
                    setOptions({ ...options, subgraphTpl: newValue });
                }} />
                <TextField label={nlsHPCC.ActivityLabel} value={options.activityTpl} multiline autoAdjustHeight onChange={(evt, newValue) => {
                    setOptions({ ...options, activityTpl: newValue });
                }} />
                <TextField label={nlsHPCC.EdgeLabel} value={options.edgeTpl} multiline autoAdjustHeight onChange={(evt, newValue) => {
                    setOptions({ ...options, edgeTpl: newValue });
                }} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.Layout}>
            </PivotItem>
        </Pivot>
    </MessageBox>;
};