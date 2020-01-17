import * as Utility from "../Utility";

import { globalKeyValStore } from "../KeyValStore";

import * as dom from "dojo/dom";
import * as domConstruct from "dojo/dom-construct";
import * as domStyle from "dojo/dom-style";
import * as query from "dojo/query";

const ws_store = globalKeyValStore();
declare const dojoConfig;

export function checkCurrentState(id, context) {
    ws_store.get("HPCCPlatformWidget_Toolbar_Color").then(function (val) {
        if (val) {
            domStyle.set(id + "Titlebar", {
                backgroundColor: val
            });
            context.toolbarColor.set("value", val);
            context.textColor = Utility.textColor(val);
        } else {
            context.toolbarColor.set("value", "#2196F3");
            context.textColor = Utility.textColor("#2196F3");
        }
    });

    ws_store.get("HPCCPlatformWidget_Toolbar_Text").then(function (val) {
        const searchUserMoreComponents = dom.byId(id + "searchUserMoreComponents");
        if (val) {
            context.environmentText.set("value", val);
            context.environmentTextCB.set("value", true);
            const parent = domConstruct.create("div", { id: id + "BannerInnerText", class: "envrionmentText" }, searchUserMoreComponents, "before");
            domConstruct.create("span", { id: context.id + "BannerContent", style: { color: context.textColor }, innerHTML: val }, parent);
            document.title = val;
        }
    });
}

export function setEnvironmentTheme(id, context) {
    const searchUserMoreComponents = dom.byId(id + "searchUserMoreComponents");
    ws_store.set("HPCCPlatformWidget_Toolbar_Text", context.environmentText.get("value"));
    ws_store.set("HPCCPlatformWidget_Toolbar_Color", context.toolbarColor.get("value"));

    ws_store.get("HPCCPlatformWidget_Toolbar_Color").then(function (val) {
        context.textColor = Utility.textColor(val);
        domStyle.set(id + "Titlebar", {
            backgroundColor: val
        });
    });

    if (context.environmentTextCB.get("checked")) {
        ws_store.get("HPCCPlatformWidget_Toolbar_Text").then(function (val) {
            const customText = query("#stubBannerInnerText");
            if (customText.length > 0) {
                domConstruct.destroy(id + "BannerInnerText");
            }
            const parent = domConstruct.create("div", { id: id + "BannerInnerText", class: "envrionmentText" }, searchUserMoreComponents, "before");
            domConstruct.create("span", { id: id + "BannerContent", style: { color: context.textColor }, innerHTML: val }, parent);

            document.title = val;
        });
    } else if (!context.environmentTextCB.get("checked")) {
        domConstruct.destroy(id + "BannerInnerText");
    }
}

export function _onResetDefaultTheme(id, context) {
    ws_store.set("HPCCPlatformWidget_Toolbar_Color", "#2196F3");
    ws_store.set("HPCCPlatformWidget_Toolbar_Text", "");

    context.toolbarColor.set("value", "#2196F3");
    context.environmentTextCB.set("checked", false);
    context.environmentText.set("value", "");

    domStyle.set(id + "Titlebar", {
        backgroundColor: "#2196F3"
    });

    const customText = query("#BannerContent");
    if (customText.length === 0) {
        domConstruct.destroy(id + "BannerInnerText");
    }
    document.title = dojoConfig.pageTitle;
}
