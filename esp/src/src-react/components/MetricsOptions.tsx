import * as React from "react";
import { DefaultButton, PrimaryButton, Checkbox, Pivot, PivotItem, TextField } from "@fluentui/react";
import nlsHPCC from "src/nlsHPCC";
import { useMetricMeta, useMetricsOptions } from "../hooks/metrics";
import { MessageBox } from "../layouts/MessageBox";
import { JSONSourceEditor } from "./SourceEditor";

const width = 640;
const innerHeight = 400;

interface MetricsOptionsProps {
    show: boolean;
    setShow: (_: boolean) => void;
}

export const MetricsOptions: React.FunctionComponent<MetricsOptionsProps> = ({
    show,
    setShow
}) => {

    const [scopeTypes, properties] = useMetricMeta();
    const [options, setOptions, save, reset] = useMetricsOptions();

    const closeOptions = React.useCallback(() => {
        setShow(false);
    }, [setShow]);

    const allChecked = scopeTypes.length === options.scopeTypes.length;

    return <MessageBox title={nlsHPCC.Options} show={show} setShow={setShow} minWidth={width}
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
                <div style={{ height: innerHeight, overflow: "auto" }}>
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
                </div>
            </PivotItem>
            <PivotItem headerText={nlsHPCC.Columns}>
                <div style={{ height: innerHeight, overflow: "auto" }}>
                    {properties.map(p => {
                        return <Checkbox key={p} label={p} checked={options.properties.indexOf(p) >= 0} onChange={(ev, checked) => {
                            const properties = options.properties.filter(row => row !== p);
                            if (checked) {
                                properties.push(p);
                            }
                            setOptions({ ...options, properties });
                        }} />;
                    })}
                </div>
            </PivotItem>
            <PivotItem headerText={nlsHPCC.Graph}>
                <div style={{ height: innerHeight, overflow: "auto" }}>
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
                </div>
            </PivotItem>
            <PivotItem headerText={nlsHPCC.Layout} >
                <div style={{ height: innerHeight }}>
                    <JSONSourceEditor json={options.layout} onChange={obj => {
                        if (obj) {
                            setOptions({ ...options, layout: obj });
                        }
                    }} />
                </div>
            </PivotItem>
        </Pivot>
    </MessageBox>;
};