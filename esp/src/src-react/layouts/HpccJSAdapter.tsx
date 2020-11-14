import * as React from "react";
import { useId } from "@fluentui/react-hooks";
import { SizeMe } from "react-sizeme";
import { Widget } from "@hpcc-js/common";

import "srcReact/layouts/HpccJSAdapter.css";

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
        widget
            .target(divID)
            .render()
            ;
        return () => {
            widget.target(null);
        };
    }, [divID, widget]);

    if (widget.target()) {
        widget.resize({ width, height });
        if (debounce) {
            widget.lazyRender();
        } else {
            widget.render();
        }
    }

    return <div id={divID} className="hpcc-js-component" style={{ width, height }}>
    </div>;
};

export interface AutosizeHpccJSComponentProps {
    widget: Widget;
    fixedHeight?: string;
    debounce?: boolean;
}

export const AutosizeHpccJSComponent: React.FunctionComponent<AutosizeHpccJSComponentProps> = ({
    widget,
    fixedHeight = "100%",
    debounce = true
}) => {

    return <SizeMe monitorHeight>{({ size }) =>
        <div style={{ width: "100%", height: fixedHeight, position: "relative" }}>
            <div style={{ position: "absolute" }}>
                <HpccJSComponent widget={widget} debounce={debounce} width={size.width} height={size.height} />
            </div>
        </div>
    }
    </SizeMe>;
};
