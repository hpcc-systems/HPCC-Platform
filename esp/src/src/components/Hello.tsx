import * as React from "react";
import * as ReactDOM from "react-dom";

export interface Props { message: string; }

export class Hello extends React.Component<Props, {}> {
    render() {
        return <span>Hello from {this.props.message}!</span>;
    }
}

export function renderHello(target: HTMLElement, props: Props) {
    ReactDOM.render(<Hello message={props.message} />, target);
}

//add to widgets as such
//HelloMod.renderHello(document.getElementById(this.id + "React"), { message: "Some Message" });