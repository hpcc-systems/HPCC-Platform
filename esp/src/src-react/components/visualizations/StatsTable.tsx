// import * as React from "react";

// import { StyledTable as html_StyledTable } from "@hpcc-js/html";

// import { AutosizeHpccJSComponent } from "../../layouts/HpccJSAdapter";

// interface VizProps {
//     columns: string[];
//     fontSize?: number;
//     data: [string, number][];
//     fixedHeight?: number;
// }

// export const StatsTable: React.FunctionComponent<VizProps> = ({
//     columns,
//     fontSize = 16,
//     data,
//     fixedHeight = 240
// }) => {

//     const chart = React.useRef(
//         new html_StyledTable()
//             .columns(columns)
//             .tbodyColumnStyles_default([
//                 {
//                     "font-weight": "bold",
//                     "text-align": "right",
//                     "font-size": fontSize + "px"
//                 },
//                 {
//                     "text-align": "right",
//                     "font-size": fontSize + "px"
//                 }
//             ])
//     ).current;

//     chart.data(data);

//     return <AutosizeHpccJSComponent widget={chart} fixedHeight={fixedHeight+"px"} />;
// };
