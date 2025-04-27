import * as React from "react";
import { Checkbox, Stack } from "@fluentui/react";
import { resetCookies, resetModernMode } from "src/Session";
import nlsHPCC from "src/nlsHPCC";
import { resetMetricsViews } from "../../hooks/metrics";
import { resetHistory } from "../../util/history";
import { resetFavorites } from "../../hooks/favorite";
import { resetTheme } from "../../hooks/theme";
import { resetStartPage } from "../../hooks/user";
import { resetCookieConsent } from "../Frame";
import { resetWorkunitOptions } from "../Workunits";

export interface ResetSettingsProps {
    handleReset: () => void
}

export const ResetSettings = React.forwardRef((props, ref) => {

    const [checkMetricOptions, setCheckMetricOptions] = React.useState(true);
    const [checkWorkunitOptions, setCheckWorkunitOptions] = React.useState(true);
    const [checkHistory, setCheckHistoryCheckbox] = React.useState(true);
    const [checkFavorites, setCheckFavorites] = React.useState(true);
    const [checkEclWatchVersion, setCheckEclWatchVersion] = React.useState(true);
    const [checkStartPage, setCheckStartPage] = React.useState(true);
    const [checkTheme, setCheckTheme] = React.useState(true);
    const [checkCookies, setCheckCookies] = React.useState(false);

    React.useImperativeHandle(ref, () => ({
        handleReset: async () => {
            if (checkMetricOptions) {
                await resetMetricsViews();
            }
            if (checkWorkunitOptions) {
                await resetWorkunitOptions();
            }
            if (checkHistory) {
                await resetHistory();
            }
            if (checkFavorites) {
                await resetFavorites();
            }
            if (checkHistory) {
                await resetHistory();
            }
            if (checkEclWatchVersion) {
                await resetModernMode();
            }
            if (checkTheme) {
                await resetTheme();
            }
            if (checkStartPage) {
                await resetStartPage();
            }
            if (checkCookies) {
                await resetCookies();
                await resetCookieConsent();
            }
        }
    }));

    return <Stack tokens={{ childrenGap: 10 }}>
        <Checkbox label={nlsHPCC.MetricOptions} checked={checkMetricOptions} onChange={(ev, checked) => setCheckMetricOptions(checked)} />
        <Checkbox label={nlsHPCC.WorkunitOptions} checked={checkWorkunitOptions} onChange={(ev, checked) => setCheckWorkunitOptions(checked)} />
        <Checkbox label={nlsHPCC.History} checked={checkHistory} onChange={(ev, checked) => setCheckHistoryCheckbox(checked)} />
        <Checkbox label={nlsHPCC.Favorites} checked={checkFavorites} onChange={(ev, checked) => setCheckFavorites(checked)} />
        <Checkbox label={nlsHPCC.ECLWatchVersion} checked={checkEclWatchVersion} onChange={(ev, checked) => setCheckEclWatchVersion(checked)} />
        <Checkbox label={nlsHPCC.DefaultStartPage} checked={checkStartPage} onChange={(ev, checked) => setCheckStartPage(checked)} />
        <Checkbox label={nlsHPCC.Theme} checked={checkTheme} onChange={(ev, checked) => setCheckTheme(checked)} />
        <Checkbox label={nlsHPCC.Cookies} checked={checkCookies} onChange={(ev, checked) => setCheckCookies(checked)} />
    </Stack>;

});
