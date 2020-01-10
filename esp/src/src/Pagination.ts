import * as declare from "dojo/_base/declare";
import * as on from "dojo/on";

// @ts-ignore
import * as _StoreMixin from "dgrid/_StoreMixin";
// @ts-ignore
import * as DGridPagination from "dgrid/extensions/Pagination";
// @ts-ignore
import * as DGrid from "dgrid/Grid";

export const Pagination = declare([DGridPagination], {
    refresh() {
        const self = this;

        _StoreMixin.prototype.refresh.apply(this, arguments);

        if (!this.store) {
            console.warn("Pagination requires a store to operate.");
            return;
        }

        // Reset to current page and return promise from gotoPage
        const page = Math.max(Math.min(this._currentPage, Math.ceil(this._total / this.rowsPerPage)), 1);
        return this.gotoPage(page).then(function (results) {
            // Emit on a separate turn to enable event to be used consistently for
            // initial render, regardless of whether the backing store is async
            setTimeout(function () {
                on.emit(self.domNode, "dgrid-refresh-complete", {
                    bubbles: true,
                    cancelable: false,
                    grid: self,
                    results // QueryResults object (may be a wrapped promise)
                });
            }, 0);

            return results;
        });
    }
});
