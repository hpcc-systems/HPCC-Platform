import { scopedLogger } from "@hpcc-js/util";
import { globalKeyValStore } from "../KeyValStore";

const ws_store = globalKeyValStore();
const logger = scopedLogger("src/UserPreferences/EnvironmentTheme.ts");

export const defaults = { active: false, text: "", color: "#2196F3" };

export function getEnvironmentTheme() {
    const promises = [
        ws_store.get("HPCCPlatformWidget_Toolbar_Active"),
        ws_store.get("HPCCPlatformWidget_Toolbar_Text"),
        ws_store.get("HPCCPlatformWidget_Toolbar_Color")
    ];
    return Promise.all(promises).then(_theme => {
        return {
            active: _theme[0] === "true",
            text: _theme[1] ?? defaults.text,
            color: _theme[2] ?? defaults.color
        };
    }).catch(err => logger.error(err));
}

export function setEnvironmentTheme(theme) {
    ws_store.set("HPCCPlatformWidget_Toolbar_Active", theme?.active ? "true" : "false");
    ws_store.set("HPCCPlatformWidget_Toolbar_Text", theme?.text ?? defaults.text);
    ws_store.set("HPCCPlatformWidget_Toolbar_Color", theme?.color ?? defaults.color);
}

export function _onResetDefaultTheme() {
    ws_store.delete("HPCCPlatformWidget_Toolbar_Active");
    ws_store.delete("HPCCPlatformWidget_Toolbar_Text");
    ws_store.delete("HPCCPlatformWidget_Toolbar_Color");
}
