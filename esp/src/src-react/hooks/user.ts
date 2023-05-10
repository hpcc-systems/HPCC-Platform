import * as React from "react";
import { useConst, useForceUpdate } from "@fluentui/react-hooks";
import { AccountService, WsAccount } from "@hpcc-js/comms";
import { cookieKeyValStore } from "src/KeyValStore";

const defaults = {
    ESPSessionTimeout: 7200,
    ESPAuthenticated: false,
    ECLWatchUser: false,
    Status: "Unlocked",
    ESPSessionState: false
};

const userSession = { ...defaults };

export interface UserSession {
    ESPSessionTimeout: number;
    ESPAuthenticated: boolean;
    ECLWatchUser: boolean;
    Status: string;
    ESPAuthenticationMSG?: string;
    ESPSessionState: boolean;
}

export function useUserSession(): {
    userSession: UserSession,
    createUserSession: (cookies: any) => Promise<any>,
    setUserSession: (opts: UserSession) => void,
    deleteUserSession: () => Promise<void>
} {
    const store = useConst(() => cookieKeyValStore());
    const refresh = useForceUpdate();

    const setUserSession = React.useCallback((opts: UserSession) => {
        for (const key in opts) {
            store.set(key, opts[key]);
        }
        refresh();
    }, [refresh, store]);

    const createUserSession = React.useCallback((cookies) => {
        return fetch("/esp/getauthtype").then(async response => {
            const text = await response.text();
            const authTypeXml = new DOMParser().parseFromString(text, "text/xml");
            const authType = authTypeXml.childNodes[0].textContent || "None";
            switch (authType) {
                case "Mixed":
                case "PerSessionOnly":
                    store.set("ESPSessionState", "true");
                    break;
                case "PerRequestOnly":
                case "UserNameOnly":
                case "None":
                    store.set("ESPSessionState", "false");
                    break;
                default:
                    store.set("ESPSessionState", "false");
            }
            store.set("Status", "Unlocked");
            store.set("ECLWatchUser", "true");
        }).catch(err => {
            store.set("ESPSessionState", "false");
            console.error("Authorization Request Error:  " + err.message);
        });
    }, [store]);

    const deleteUserSession = React.useCallback(() => {
        const cookies = store.getAll();
        return Promise.resolve().then(() => {
            for (const c in cookies) {
                store.delete(c);
            }
        });
    }, [store]);

    return { userSession, createUserSession, setUserSession, deleteUserSession };
}

export function useMyAccount(): { currentUser: WsAccount.MyAccountResponse } {

    const [currentUser, setCurrentUser] = React.useState<WsAccount.MyAccountResponse>({ username: "" } as WsAccount.MyAccountResponse);

    const service = useConst(() => new AccountService({ baseUrl: "" }));

    React.useEffect(() => {
        service.MyAccount({})
            .then((response) => {
                response.username = response.username ?? "";
                setCurrentUser(response);
            });
    }, [service, setCurrentUser]);

    return { currentUser };

}

