import "dojo/i18n";
// @ts-expect-error
import * as nlsHPCC from "dojo/i18n!./nls/hpcc";
import nlsHPCCT from "./nls/hpcc";

export default nlsHPCC as typeof nlsHPCCT.root;
