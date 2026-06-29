import * as React from "react";
import { Button, Checkbox, makeStyles, tokens } from "@fluentui/react-components";
import { resetCookies, resetModernMode } from "src/Session";
import nlsHPCC from "src/nlsHPCC";
import { pushUrl, replaceUrl } from "../util/history";
import { resetMetricsViews } from "../hooks/metrics";
import { resetHistory } from "../util/history";
import { MessageBox } from "../layouts/MessageBox";
import { resetNavWide, resetTheme } from "../hooks/theme";
import { resetFavorites } from "../hooks/favorite";
import { resetCookieConsent } from "./Frame";
import { resetWorkunitOptions } from "./Workunits";
import { resetWorkunitSummarySplitter } from "./WorkunitSummary";
import { resetWUSummaryOptions } from "./WUSSummary";

const useResetStyles = makeStyles({
    root: {
        height: "100%",
        backgroundColor: tokens.colorNeutralBackground1
    },
});

export interface ResetDialogProps {
}

export const ResetDialog: React.FunctionComponent<ResetDialogProps> = ({
}) => {

    const [show, setShow] = React.useState(true);
    const [checkMetricOptions, setCheckMetricOptions] = React.useState(true);
    const [checkWorkunitOptions, setCheckWorkunitOptions] = React.useState(true);
    const [checkHistory, setCheckHistoryCheckbox] = React.useState(true);
    const [checkFavorites, setCheckFavorites] = React.useState(true);
    const [checkEclWatchVersion, setCheckEclWatchVersion] = React.useState(true);
    const [checkTheme, setCheckTheme] = React.useState(true);
    const [checkCookies, setCheckCookies] = React.useState(false);
    const [checkNavWide, setCheckNavWide] = React.useState(true);

    const styles = useResetStyles();

    const onClick = React.useCallback(async () => {
        if (checkMetricOptions) {
            await resetMetricsViews();
        }
        if (checkWorkunitOptions) {
            await resetWorkunitOptions();
            await resetWorkunitSummarySplitter();
            await resetWUSummaryOptions();
        }
        if (checkHistory) {
            await resetHistory();
        }
        if (checkFavorites) {
            await resetFavorites();
        }
        if (checkEclWatchVersion) {
            await resetModernMode();
        }
        if (checkTheme) {
            await resetTheme();
        }
        if (checkNavWide) {
            await resetNavWide();
        }
        if (checkCookies) {
            await resetCookies();
            await resetCookieConsent();
        }
        setShow(false);
        replaceUrl("/");
        window.location.reload();
    }, [checkCookies, checkEclWatchVersion, checkFavorites, checkHistory, checkMetricOptions, checkNavWide, checkTheme, checkWorkunitOptions]);

    return <div className={styles.root}>
        <MessageBox show={show} setShow={setShow} title={`${nlsHPCC.ResetUserSettings}?`} footer={
            <>
                <Button appearance="primary" onClick={onClick}>{nlsHPCC.Reset}</Button>
                <Button onClick={() => { setShow(false); pushUrl("/"); }}>{nlsHPCC.Cancel}</Button>
            </>
        }>
            <div style={{ display: "flex", flexDirection: "column", gap: "10px" }}>
                <Checkbox label={nlsHPCC.MetricOptions} checked={checkMetricOptions} onChange={(_, data) => setCheckMetricOptions(!!data.checked)} />
                <Checkbox label={nlsHPCC.WorkunitOptions} checked={checkWorkunitOptions} onChange={(_, data) => setCheckWorkunitOptions(!!data.checked)} />
                <Checkbox label={nlsHPCC.History} checked={checkHistory} onChange={(_, data) => setCheckHistoryCheckbox(!!data.checked)} />
                <Checkbox label={nlsHPCC.Favorites} checked={checkFavorites} onChange={(_, data) => setCheckFavorites(!!data.checked)} />
                <Checkbox label={nlsHPCC.ECLWatchVersion} checked={checkEclWatchVersion} onChange={(_, data) => setCheckEclWatchVersion(!!data.checked)} />
                <Checkbox label={nlsHPCC.Theme} checked={checkTheme} onChange={(_, data) => setCheckTheme(!!data.checked)} />
                <Checkbox label={nlsHPCC.NavWide} checked={checkNavWide} onChange={(_, data) => setCheckNavWide(!!data.checked)} />
                <Checkbox label={nlsHPCC.Cookies} checked={checkCookies} onChange={(_, data) => setCheckCookies(!!data.checked)} />
            </div>
        </MessageBox>
    </div>;
};
