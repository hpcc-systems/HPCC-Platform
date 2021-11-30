import * as React from "react";
import { DefaultButton, PrimaryButton } from "@fluentui/react";
import nlsHPCC from "src/nlsHPCC";
import { MessageBox } from "../../layouts/MessageBox";

interface CookieConsentProps {
    onApply?: (n: boolean) => void;
    showCookieConsent: boolean;
    setShowCookieConsent?: (_: boolean) => void;
}

export const CookieConsent: React.FunctionComponent<CookieConsentProps> = ({
    onApply = () => {},
    showCookieConsent,
    setShowCookieConsent = () => {}
}) => {
    return <MessageBox title={nlsHPCC.PleaseEnableCookies} show={showCookieConsent} setShow={setShowCookieConsent}
        footer={<>
            <DefaultButton text={nlsHPCC.CookiesNoticeLinkText} onClick={() => {
                window.open("https://risk.lexisnexis.com/cookie-policy", "_blank");
            }} />
            <PrimaryButton text={nlsHPCC.CookiesAcceptButtonText} onClick={() => {
                onApply(true);
                setShowCookieConsent(false);
            }} />
        </>}>
    </MessageBox>;
};
