import * as React from "react";
import { Widget } from "@hpcc-js/common";

export interface VisualizationProps {
    widget: new () => Widget;
    widgetProps?: { [key: string]: any };
    width?: string;
    height?: string;
}

let g_id = 0;
export const VisualizationComponent: React.FunctionComponent<VisualizationProps> = ({
    widget,
    widgetProps,
    width = "100%",
    height = "240px"
}) => {

    const [divID] = React.useState("viz-component-" + ++g_id);
    const [widgetInstance, setWidgetInstance] = React.useState<Widget>(undefined);

    React.useEffect(() => {
        const w = new widget()
            .target(divID)
            ;
        setWidgetInstance(w);
        return () => {
            w.target(null);
        };
    }, []);

    if (widgetInstance) {
        if (widgetProps.columns) {
            widgetInstance.columns(widgetProps.columns);
        }
        if (widgetProps.data) {
            widgetInstance.data(widgetProps.data);
        }
        widgetInstance
            .deserialize({
                __class: undefined,
                ...widgetProps
            })
            .lazyRender()
            ;
    }
    return <div id={divID} style={{ width, height }}></div>;
};
