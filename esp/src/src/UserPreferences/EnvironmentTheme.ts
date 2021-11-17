import { scopedLogger } from "@hpcc-js/util";
import { globalKeyValStore } from "../KeyValStore";

const ws_store = globalKeyValStore();
const logger = scopedLogger("src/UserPreferences/EnvironmentTheme.ts");

const toolbarThemeDefaults = { active: false, text: "", color: "#2196F3" };

export function getEnvironmentTheme() {
    const promises = [
        ws_store.get("HPCCPlatformWidget_Toolbar_Active"),
        ws_store.get("HPCCPlatformWidget_Toolbar_Text"),
        ws_store.get("HPCCPlatformWidget_Toolbar_Color")
    ];
    return Promise.all(promises).then(_theme => {
        return {
            active: _theme[0] || toolbarThemeDefaults.active,
            text: _theme[1] || toolbarThemeDefaults.text,
            color: _theme[2] || toolbarThemeDefaults.color
        };
    }).catch(err => logger.error(err));
}

export function setEnvironmentTheme(theme) {
    ws_store.set("HPCCPlatformWidget_Toolbar_Active", theme?.active ? "true" : "false");
    ws_store.set("HPCCPlatformWidget_Toolbar_Text", theme?.text || toolbarThemeDefaults.text);
    ws_store.set("HPCCPlatformWidget_Toolbar_Color", theme?.color || toolbarThemeDefaults.color);
}

export function _onResetDefaultTheme() {
    ws_store.set("HPCCPlatformWidget_Toolbar_Active", toolbarThemeDefaults.active.toString());
    ws_store.set("HPCCPlatformWidget_Toolbar_Text", toolbarThemeDefaults.text);
    ws_store.set("HPCCPlatformWidget_Toolbar_Color", toolbarThemeDefaults.color);
}
