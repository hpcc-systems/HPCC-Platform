import "dojo/i18n";
// @ts-ignore
import * as nlsHPCC from "dojo/i18n!hpcc/nls/hpcc";

var i18n = nlsHPCC;



export function getCondition (int) {
    switch (int) {
        case 1:
            return i18n.Normal;
        case 2:
            return i18n.Warning;
        case 3:
            return i18n.Minor;
        case 4:
            return i18n.Major;
        case 5:
            return i18n.Critical;
        case 6:
            return i18n.Fatal;
        default:
            return i18n.Unknown;
    }
}

export function getState (int) {
    switch (int) {
        case 0:
            return i18n.Unknown;
        case 1:
            return i18n.Starting;
        case 2:
            return i18n.Stopping;
        case 3:
            return i18n.Suspended;
        case 4:
            return i18n.Recycling;
        case 5:
            return i18n.Ready;
        case 6:
            return i18n.Busy;
        default:
            return i18n.Unknown;
    }
}

