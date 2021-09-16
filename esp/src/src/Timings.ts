import { WUTimeline } from "@hpcc-js/eclwatch";

const columns = ["label", "start", "end", "icon", "color", "series", "depth"];

export class WUTimelinePatched extends WUTimeline {

    constructor() {
        super();
        this
            .columns(columns)
            .bucketColumn("icon")
            ;
        this._gantt
            .bucketHeight(22)
            .gutter(4)
            .overlapTolerence(-100)
            ;
        this._gantt["_series_idx"] = -1;
        this.strokeWidth(0);
        this.tooltipHTML(d => {
            return d[d.length - 1].calcTooltip(); 
        });
    }

    data(): any;
    data(_: any): this;
    data(_?: any): any | this {
        if (arguments.length === 0) return super.data();
        super.data(_.map(row => {
            if (row[2] === undefined || row[2] === null) {
                row[2] = row[1];
            }
            row[5] = null;
            row.push(row[6]);
            row[6] = (row[6]?.ScopeName?.split(":")?.length - 1) || 0;
            return row;
        }));
        return this;
    }
}

export class WUTimelineEx extends WUTimeline {

    constructor() {
        super();
        this._gantt.bucketHeight(16);
        this.strokeWidth(0);
    }

    data(): any;
    data(_: any): this;
    data(_?: any): any | this {
        const retVal = super.data.apply(this, arguments);
        if (arguments.length) {
            const timeData = {};
            _.map(function (row) {
                timeData[row[0]] = {
                    started: row[1],
                    finished: row[2]
                };
            });
            this.setData(timeData);
        }
        return retVal;
    }

    refresh() {
        this.clear();
        this.fetchScopes();
    }

    //  Events  ---
    setData(timeData: { [graphID: string]: { started: string, finished: string } }) {
    }
}
