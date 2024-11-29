import { local as d3Local, select as d3Select } from "@hpcc-js/common";
import { Vertex } from "@hpcc-js/graph";
import { Legend } from "@hpcc-js/layout";

export interface WUGraphLegendData {
    kind: number;
    faChar: string;
    label: string;
    count: number;
}

class LegendVertex extends Vertex {

    constructor() {
        super();
    }

    enter(domNode, element) {
        super.enter(domNode, element);
        this._textBox.text_colorFill("black");
        this._icon.on("click", () => {
            this.click(this.data());
        });
    }

    click(kind: number) {
    }
}

export class WUGraphLegend extends Legend {

    constructor(owner) {
        super(owner);
    }

    private icon = d3Local<Vertex>();
    protected _disabled2: { [kind: number]: boolean } = {
        /*  TODO:  Default some to disabled?
        43: true,
        71: true,
        82: true,
        88: true
        */
    };

    disabled(): number[];
    disabled(_: number[]): this;
    disabled(_?: number[]): number[] | this {
        if (!arguments.length) {
            const retVal = [];
            for (const key in this._disabled2) {
                if (this._disabled2[key]) {
                    retVal.push(key);
                }
            }
            return retVal;
        }
        this._disabled2 = {};
        _.forEach(kind => this._disabled2[kind] = true);
        return this;
    }

    toggle(kind: number) {
        this._disabled2[kind] = !this._disabled2[kind];
    }

    update(domNode, element) {
        super.update(domNode, element);

        const context = this;
        const items = this._g.selectAll(".legendItem").data(this.data(), (d: any) => d.kind);
        items.enter().append("g")
            .attr("class", "legendItem")
            .each(function (this: HTMLElement, d) {
                context.icon.set(this, new LegendVertex()
                    .target(this)
                    .data(d.kind)
                    .textbox_shape_colorStroke("none")
                    .textbox_shape_colorFill("none")
                    .iconAnchor("left")
                    .faChar(d.faChar)
                    .text(`${d.label} (${d.count})`)
                    .tooltip(`${d.kind} - ${d.label}`)
                    .on("click", kind => {
                        context.toggle(kind);
                        context.render();
                        context.click(kind);
                    })
                    .on("mouseover", kind => {
                        context.mouseover(kind);
                    })
                    .on("mouseout", kind => {
                        context.mouseout(kind);
                    })
                );
            })
            .merge(items)
            .each(function (this: HTMLElement, d, i) {
                const bbox = context.icon.get(this)
                    .icon_shape_colorFill(context._disabled2[d.kind] ? "gray" : null)
                    .render().getBBox();

                d3Select(this)
                    .attr("transform", `translate(${+bbox.width / 2}, ${i * 30})`)
                    ;
            })
            ;
        items.exit()
            .each(function (this: HTMLElement, d) {
                context.icon.get(this)
                    .target(null)
                    .render();
            })
            .remove();
        this._g.attr("transform", "translate(32, 16)");
        const bbox = this.getBBox(true, true);
        this.resize({ width: bbox.width + 32, height: bbox.height + 16 });
    }

    //  Events  ---
    click(kind: number) {
    }

    mouseover(kind: number) {
    }

    mouseout(kind: number) {
    }
}
WUGraphLegend.prototype._class += " eclwatch_WUGraphLegend";
