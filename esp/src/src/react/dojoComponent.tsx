import * as React from "react";
import { resolve } from "../Utility";

export interface DojoProps {
    widget: string;
    params?: object;
    onWidgetMount?: (widget) => void;
}

export interface DojoState {
    placeholderID: string;
    widgetID: string;
}

let g_id = 0;
export class DojoComponent extends React.Component<DojoProps, DojoState> {

    constructor(props: DojoProps) {
        super(props);
        const id = ++g_id;
        this.state = {
            placeholderID: `dojo-component-${id}`,
            widgetID: `dojo-component-widget-${id}`
        };
    }

    shouldComponentUpdate(nextProps: Readonly<DojoProps>, nextState: Readonly<DojoState>): boolean {
        return false;
    }

    componentDidMount() {
        resolve(this.props.widget, WidgetClass => {
            const widget = new WidgetClass({
                id: this.state.widgetID,
                style: {
                    margin: "0px",
                    padding: "0px",
                    width: "100%",
                    height: "100%"
                }
            });
            widget.placeAt(this.state.placeholderID, "replace");
            widget.startup();
            widget.resize();
            if (widget.init) {
                widget.init(this.props.params || {});
            }
            if (this.props.onWidgetMount) {
                this.props.onWidgetMount(widget);
            }
        });
    }

    render() {
        return <div id={this.state.placeholderID}>...loading {this.props.widget}...</div>;
    }

}
