import * as React from "react";

export interface NavProps { version: string; build: string; }

export class Navigation extends React.Component<NavProps, {}> {
    render() {
        return <nav>
            <ul>
                <li>Workunits</li>
                <li>Files</li>
            </ul>
            <p>{this.props.version} , {this.props.build}</p>
        </nav>
    }
}