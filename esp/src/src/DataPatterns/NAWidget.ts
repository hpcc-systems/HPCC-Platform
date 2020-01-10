import { Html } from "@hpcc-js/other";
import { config } from "./config";

export class NAWidget extends Html {
    constructor(message, submessage) {
        super();
        this
            .html(`
                <b style="line-height:23px;font-size:${config.secondaryFontSize}px;color: rgb(51, 51, 51);">${message}</b>
                <br/>
                <i style="font-size:${config.secondaryFontSize}px;color: rgb(51, 51, 51);">${submessage}</i>
            `)
            .overflowX("hidden")
            .overflowY("hidden")
            ;

    }
}
