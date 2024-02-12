import * as React from "react";
import { useConst } from "@fluentui/react-hooks";
import { IScope } from "@hpcc-js/comms";
import { Table } from "@hpcc-js/dgrid";
import nlsHPCC from "src/nlsHPCC";
import { AutosizeHpccJSComponent } from "../layouts/HpccJSAdapter";

interface MetricsPropertiesTablesProps {
    scopes?: IScope[];
}

export const MetricsPropertiesTables: React.FunctionComponent<MetricsPropertiesTablesProps> = ({
    scopes = []
}) => {

    //  Props Table  ---
    const propsTable = useConst(() => new Table()
        .columns([nlsHPCC.Property, nlsHPCC.Value, "Avg", "Min", "Max", "Delta", "StdDev", "SkewMin", "SkewMax", "NodeMin", "NodeMax"])
        .columnWidth("auto")
    );

    React.useEffect(() => {
        const props = [];
        scopes.forEach((item, idx) => {
            for (const key in item.__groupedProps) {
                const row = item.__groupedProps[key];
                props.push([row.Key, row.Value, row.Avg, row.Min, row.Max, row.Delta, row.StdDev, row.SkewMin, row.SkewMax, row.NodeMin, row.NodeMax]);
            }
            if (idx < scopes.length - 1) {
                props.push(["------------------------------", "------------------------------"]);
            }
        });
        propsTable
            ?.data(props)
            ?.lazyRender()
            ;
    }, [propsTable, scopes]);

    return <AutosizeHpccJSComponent widget={propsTable}></AutosizeHpccJSComponent>;
};
