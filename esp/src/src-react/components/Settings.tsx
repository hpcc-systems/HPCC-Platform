import * as React from "react";
import { DefaultButton, Pivot, PivotItem, PrimaryButton } from "@fluentui/react";
import { FluentProvider } from "@fluentui/react-components";
import { ThemeEditor } from "./ThemeEditor";
import { useUserTheme } from "../hooks/theme";
import { MessageBox } from "../layouts/MessageBox";
import nlsHPCC from "src/nlsHPCC";

interface SettingsProps {
    show?: boolean;
    onClose?: () => void;
}

export const Settings: React.FunctionComponent<SettingsProps> = ({
    show = false,
    onClose = () => { }
}) => {

    const { themeV9, primaryColor, setPrimaryColor, hueTorsion, setHueTorsion, vibrancy, setVibrancy, resetTheme } = useUserTheme();
    const [previousColor, setPreviousColor] = React.useState(primaryColor);
    const [previousHueTorsion, setPreviousHueTorsion] = React.useState(hueTorsion);
    const [previousVibrancy, setPreviousVibrancy] = React.useState(vibrancy);
    const [activePivot, setActivePivot] = React.useState("theme");

    React.useEffect(() => {
        // cache the previous color at dialog open, used to reset upon close
        if (show) {
            setPreviousColor(primaryColor);
            setPreviousHueTorsion(hueTorsion);
            setPreviousVibrancy(vibrancy);
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [show]);

    return <MessageBox show={show} setShow={onClose} blocking={true} modeless={false} title={nlsHPCC.Settings} minWidth={400}
        footer={<>
            <PrimaryButton onClick={() => {
                if (onClose) {
                    onClose();
                }
            }} text={nlsHPCC.OK} />
            <DefaultButton onClick={() => {
                switch (activePivot) {
                    case "theme":
                    default:
                        setPrimaryColor(previousColor);
                        setHueTorsion(previousHueTorsion);
                        setVibrancy(previousVibrancy);
                        break;

                }
                if (onClose) {
                    onClose();
                }
            }} text={nlsHPCC.Cancel} />
            <DefaultButton onClick={() => {
                switch (activePivot) {
                    case "theme":
                    default:
                        resetTheme();
                        break;

                }
                if (onClose) {
                    onClose();
                }
            }} text={nlsHPCC.Reset} />
        </>}>
        <FluentProvider theme={themeV9}>
            <Pivot onLinkClick={item => {
                setActivePivot(item.props.itemKey);
            }}>
                <PivotItem itemKey="theme" headerText={nlsHPCC.Theme}>
                    <ThemeEditor />
                </PivotItem>
            </Pivot>
        </FluentProvider >
    </MessageBox>;

};