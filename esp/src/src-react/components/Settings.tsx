import * as React from "react";
import { DefaultButton, Dialog, DialogFooter, DialogType, Pivot, PivotItem, PrimaryButton } from "@fluentui/react";
import { ThemeEditor } from "./ThemeEditor";
import { useUserTheme } from "../hooks/theme";
import nlsHPCC from "src/nlsHPCC";

const dialogContentProps = {
    type: DialogType.largeHeader,
    title: nlsHPCC.Settings
};

interface SettingsProps {
    show?: boolean;
    onClose?: () => void;
}

export const Settings: React.FunctionComponent<SettingsProps> = ({
    show = false,
    onClose = () => { }
}) => {

    const { resetTheme } = useUserTheme();
    const [cancelButtonText, setCancelButtonText] = React.useState(nlsHPCC.Reset);
    const [activePivot, setActivePivot] = React.useState("theme");

    return <Dialog hidden={!show} onDismiss={onClose} dialogContentProps={dialogContentProps} minWidth="640px">
        <Pivot onLinkClick={item => {
            setActivePivot(item.props.itemKey);
            switch (item.props.itemKey) {
                case "theme":
                    setCancelButtonText(nlsHPCC.Reset);
                    break;
                default:
                    setCancelButtonText(nlsHPCC.Cancel);
            }
        }}>
            <PivotItem itemKey="theme" headerText={nlsHPCC.Theme}>
                <ThemeEditor />
            </PivotItem>
        </Pivot>
        <DialogFooter>
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
            }} text={cancelButtonText} />
            <PrimaryButton onClick={() => {
                if (onClose) {
                    onClose();
                }
            }} text={nlsHPCC.Close} />
        </DialogFooter>
    </Dialog>;

};