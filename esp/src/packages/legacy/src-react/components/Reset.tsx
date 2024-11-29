import * as React from "react";
import { PrimaryButton, DefaultButton, mergeStyleSets, Checkbox, Stack } from "@fluentui/react";
import { resetCookies, resetModernMode } from "src/Session";
import nlsHPCC from "src/nlsHPCC";
import { pushUrl, replaceUrl } from "../util/history";
import { resetMetricsViews } from "../hooks/metrics";
import { resetHistory } from "../util/history";
import { MessageBox } from "../layouts/MessageBox";
import { resetTheme, useUserTheme } from "../hooks/theme";
import { resetFavorites } from "../hooks/favorite";
import { resetCookieConsent } from "./Frame";
import { resetWorkunitOptions } from "./Workunits";

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
    const { theme } = useUserTheme();

    const styles = React.useMemo(() => mergeStyleSets({
        root: {
            height: "100%",
            backgroundColor: theme.semanticColors.bodyBackground
        },
    }), [theme.semanticColors.bodyBackground]);

    return <div className={styles.root}>
        <MessageBox show={show} setShow={setShow} title={`${nlsHPCC.ResetUserSettings}?`} footer={
            <>
                <PrimaryButton text={nlsHPCC.Reset} onClick={async () => {
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
                    if (checkCookies) {
                        await resetCookies();
                        await resetCookieConsent();
                    }
                    setShow(false);
                    replaceUrl("/");
                    window.location.reload();
                }} />
                <DefaultButton text={nlsHPCC.Cancel} onClick={() => {
                    setShow(false);
                    pushUrl("/");
                }} />
            </>
        }>
            <Stack tokens={{ childrenGap: 10 }}>
                <Checkbox label={nlsHPCC.MetricOptions} checked={checkMetricOptions} onChange={(ev, checked) => setCheckMetricOptions(checked)} />
                <Checkbox label={nlsHPCC.WorkunitOptions} checked={checkWorkunitOptions} onChange={(ev, checked) => setCheckWorkunitOptions(checked)} />
                <Checkbox label={nlsHPCC.History} checked={checkHistory} onChange={(ev, checked) => setCheckHistoryCheckbox(checked)} />
                <Checkbox label={nlsHPCC.Favorites} checked={checkFavorites} onChange={(ev, checked) => setCheckFavorites(checked)} />
                <Checkbox label={nlsHPCC.ECLWatchVersion} checked={checkEclWatchVersion} onChange={(ev, checked) => setCheckEclWatchVersion(checked)} />
                <Checkbox label={nlsHPCC.Theme} checked={checkTheme} onChange={(ev, checked) => setCheckTheme(checked)} />
                <Checkbox label={nlsHPCC.Cookies} checked={checkCookies} onChange={(ev, checked) => setCheckCookies(checked)} />
            </Stack>
        </MessageBox>
    </div>;
};
