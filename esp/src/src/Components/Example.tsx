import * as React from "react";
import { render } from "react-dom";

 interface IProps {
  message: string;
}

 export class Hello extends React.Component<IProps, {}> {
  render() {
    return <span>Hello from ECLWatch {this.props.message}!</span>;
  }
}


 export function renderHello(target: HTMLElement, props: IProps) {
    render(<Hello message={props.message} />, target);
}
// funtcional component
// const Hello2 = (props: IProps) => {
//   return <span>Hello from {props.message}!</span>;
// };

//add to widgets as such
//ExampleComponent.renderHello(document.getElementById(this.id + "React"), { message: "Some Message" });