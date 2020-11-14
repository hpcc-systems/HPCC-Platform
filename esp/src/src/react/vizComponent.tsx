import * as React from "react";
import { useId } from "@fluentui/react-hooks";
import { Widget } from "@hpcc-js/common";

export interface VisualizationProps {
    widget: new () => Widget;
    widgetProps?: { [key: string]: any };
    width?: string;
    height?: string;
}

export const VisualizationComponent: React.FunctionComponent<VisualizationProps> = ({
    widget,
    widgetProps,
    width = "100%",
    height = "240px"
}) => {

    const divID = useId("viz-component-");
    const [widgetInstance, setWidgetInstance] = React.useState<Widget>(undefined);

    React.useEffect(() => {
        const w = new widget()
            .target(divID)
            ;
        setWidgetInstance(w);
        return () => {
            w.target(null);
        };
    }, [divID, widget]);

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
