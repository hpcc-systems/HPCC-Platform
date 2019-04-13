import * as React from "react";

export interface Props { message: string;}

export class Hello extends React.Component<Props, {}> {
    render() {
        return <h1>Hello from {this.props.message}!</h1>;
    }
}