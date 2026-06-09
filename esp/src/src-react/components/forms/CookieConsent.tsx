import * as React from "react";

import { Button } from "@fluentui/react-components";
import nlsHPCC from "src/nlsHPCC";
import { MessageBox } from "../../layouts/MessageBox";

interface CookieConsentProps {
    onApply?: (n: boolean) => void;
    showCookieConsent: boolean;
    setShowCookieConsent?: (_: boolean) => void;
}

export const CookieConsent: React.FunctionComponent<CookieConsentProps> = ({
    onApply = () => { },
    showCookieConsent,
    setShowCookieConsent = () => { }
}) => {
    return <MessageBox title={nlsHPCC.PleaseEnableCookies} show={showCookieConsent} setShow={setShowCookieConsent}
        footer={<>
            <Button onClick={() => { window.open("https://risk.lexisnexis.com/cookie-policy", "_blank"); }}>{nlsHPCC.CookiesNoticeLinkText}</Button>
            <Button appearance="primary" onClick={() => { onApply(true); setShowCookieConsent(false); }}>{nlsHPCC.CookiesAcceptButtonText}</Button>
        </>}>
    </MessageBox>;
};
