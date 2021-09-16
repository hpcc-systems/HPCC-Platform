import * as React from "react";
import { useId } from "@fluentui/react-hooks";
import { SizeMe } from "react-sizeme";
import { Widget } from "@hpcc-js/common";

import "src-react-css/layouts/HpccJSAdapter.css";

export interface HpccJSComponentProps {
    widget: Widget;
    width: number;
    height: number;
    debounce?: boolean;
}

export const HpccJSComponent: React.FunctionComponent<HpccJSComponentProps> = ({
    widget,
    width,
    height,
    debounce = true
}) => {
    const divID = useId("viz-component-");

    React.useEffect(() => {
        const w = widget?.target(divID)
            .render()
            ;
        return () => {
            w?.target(null);
        };
    }, [divID, widget]);

    React.useEffect(() => {
        if (widget.target()) {
            widget.resize({ width, height });
            if (debounce) {
                widget.lazyRender();
            } else {
                widget.render();
            }
        }
    }, [debounce, height, widget, width]);

    return (isNaN(width) || isNaN(height)) ?
        <></> :
        <div id={divID} className="hpcc-js-component" style={{ width, height }}>
        </div>;
};

export interface AutosizeHpccJSComponentProps {
    widget: Widget;
    fixedHeight?: string;
    padding?: number;
    debounce?: boolean;
}

export const AutosizeHpccJSComponent: React.FunctionComponent<AutosizeHpccJSComponentProps> = ({
    widget,
    fixedHeight = "100%",
    padding = 0,
    debounce = true
}) => {

    return <SizeMe monitorHeight>{({ size }) => {
        const width = size?.width || padding * 2;
        const height = size?.height || padding * 2;
        return <div style={{ width: "100%", height: fixedHeight, position: "relative" }}>
            <div style={{ position: "absolute", padding: `${padding}px` }}>
                <HpccJSComponent widget={widget} debounce={debounce} width={width - padding * 2} height={height - padding * 2} />
            </div>
        </div>;
    }}
    </SizeMe >;
};
