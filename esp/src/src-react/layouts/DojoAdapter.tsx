import * as React from "react";
import * as ReactDOM from "react-dom";
import { useConst, useId } from "@fluentui/react-hooks";
import * as registry from "dijit/registry";
import nlsHPCC from "src/nlsHPCC";
import { resolve } from "src/Utility";

export interface DojoAdapterProps {
    widgetClassID?: string;
    widgetClass?: any;
    params?: object;
    onWidgetMount?: (widget) => void;
}

export interface DojoState {
    uid: number;
    widgetClassID?: string;
    widget: any;
}

export const DojoAdapter: React.FunctionComponent<DojoAdapterProps> = ({
    widgetClassID,
    widgetClass,
    params,
    onWidgetMount
}) => {

    const myRef = React.useRef<HTMLDivElement>();
    const uid = useId("");

    React.useEffect(() => {

        const elem = document.createElement("div");
        const divRef = myRef.current;
        divRef.innerText = "";
        divRef.appendChild(elem);

        let widget = undefined;

        if (widgetClassID) {
            resolve(widgetClassID, widgetClass => {
                init(widgetClass);
            });
        } else if (widgetClass) {
            init(widgetClass);
        }

        function init(WidgetClass) {
            if (widget === undefined) { //  Test for race condition  --
                widget = new WidgetClass({
                    id: `dojo-component-widget-${uid}`,
                    style: {
                        margin: "0px",
                        padding: "0px",
                        width: "100%",
                        height: "100%"
                    }
                }, elem);
                // widget.placeAt(elem, "replace");
                widget.startup();
                widget.resize();
                if (widget.init) {
                    widget.init(params || {});
                }

                if (onWidgetMount) {
                    onWidgetMount(widget);
                }
            }
        }

        return () => {
            if (widget) {
                widget.destroyRecursive(true);

                //  Should not be needed  ---
                registry.toArray().filter(w => w.id.indexOf(`dojo-component-widget-${uid}`) === 0).forEach(w => {
                    w.destroyRecursive(true);
                });
                //  ---

                const domNode = ReactDOM.findDOMNode(divRef) as Element;
                domNode.innerHTML = "";
            }
            widget = null;  //  Avoid race condition  ---
        };
    }, [onWidgetMount, params, uid, widgetClass, widgetClassID]);

    return <div ref={myRef} style={{ width: "100%", height: "100%" }}>{nlsHPCC.Loading} {widgetClassID}...</div>;
};

export interface DojoComponentProps {
    Widget: any;
    WidgetParams: any;
    postCreate?: (widget: any) => void;
}

export const DojoComponent: React.FunctionComponent<DojoComponentProps> = ({
    Widget,
    WidgetParams,
    postCreate
}) => {

    const id = useId();
    const divID = useConst(`dojo-component-${id}`);

    React.useEffect(() => {
        const w = new Widget({
            ...WidgetParams,
            id: `dojo-component-widget-${id}`,
            style: {
                margin: "0px",
                padding: "0px",
                width: "100%",
                height: "100%"
            }
        }, divID);

        if (postCreate) {
            postCreate(w);
        }

        return () => {
            w.destroyRecursive();
        };
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    return <div style={{ width: "100%", height: "100%", position: "relative" }}>
        <div id={divID} className="dojo-component">
        </div>
    </div>;
};
