import * as React from "react";
import { DefaultButton, PrimaryButton } from "@fluentui/react";
import { FluentProvider } from "@fluentui/react-components";
import { pushUrl, replaceUrl } from "../util/history";
import { useUserTheme } from "../hooks/theme";
import { MessageBox } from "../layouts/MessageBox";
import { TabInfo, OverflowTabList } from "./controls/TabbedPanes/index";
import { GeneralSettings, GeneralSettingsProps } from "./forms/GeneralSettings";
import { ResetSettings, ResetSettingsProps } from "./forms/ResetSettings";
import nlsHPCC from "src/nlsHPCC";

const tabDivStyle = { padding: "10px 0 0 0", minHeight: 220 };

interface SettingsProps {
    tab?: string;
    parentUrl?: string;
    onClose?: () => void;
}

export const Settings: React.FunctionComponent<SettingsProps> = ({
    tab = "general",
    parentUrl = "/settings",
    onClose = () => { }
}) => {

    const { themeV9 } = useUserTheme();
    const [show, setShow] = React.useState(true);

    const generalRef = React.useRef<GeneralSettingsProps>(null);
    const resetRef = React.useRef<ResetSettingsProps>(null);

    const onTabSelect = React.useCallback((tab: TabInfo) => {
        pushUrl(tab.__state ?? `${parentUrl}/${tab.id === "general" ? "" : tab.id}`);
    }, [parentUrl]);

    const resetDialog = React.useCallback(() => {
        setShow(false);
        replaceUrl("/");
    }, []);

    const tabs = React.useMemo((): TabInfo[] => {
        return [{
            id: "general",
            label: nlsHPCC.General
        }, {
            id: "reset",
            label: nlsHPCC.ResetUserSettings
        }];
    }, []);

    return <MessageBox show={show} setShow={resetDialog} blocking={true} modeless={false} title={nlsHPCC.Settings} minWidth={400}
        footer={<>
            <PrimaryButton onClick={() => {
                switch (tab) {
                    case "reset":
                        resetRef.current.handleReset();
                        break;
                    case "general":
                        generalRef.current.handleSubmit();
                        break;

                    default:
                        break;

                }
                resetDialog();
                window.location.reload();
                if (onClose) {
                    onClose();
                }
            }} text={nlsHPCC.OK} />
            <DefaultButton text={nlsHPCC.Cancel} onClick={resetDialog} />
        </>}>
        <FluentProvider theme={themeV9}>
            <OverflowTabList tabs={tabs} selected={tab} onTabSelect={onTabSelect} size="medium" />
            <div style={{ ...tabDivStyle, display: tab === "general" ? "block" : "none" }}>
                <GeneralSettings ref={generalRef} />
            </div>
            <div style={{ ...tabDivStyle, display: tab === "reset" ? "block" : "none" }}>
                <ResetSettings ref={resetRef} />
            </div>
        </FluentProvider >
    </MessageBox >;

};