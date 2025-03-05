import * as React from "react";
import { useConst, useForceUpdate } from "@fluentui/react-hooks";
import { AccessService, AccountService, WsAccount } from "@hpcc-js/comms";
import { cookieKeyValStore, sessionKeyValStore, userKeyValStore } from "src/KeyValStore";
import { useSessionStore, useUserStore } from "./store";

declare const dojoConfig;

const defaults = {
    ESPSessionTimeout: 7200,
    ESPAuthenticated: false,
    ECLWatchUser: false,
    ESPSessionState: false
};

const userSession = { ...defaults };

export enum PasswordStatus {
    NeverExpires = -2,
    Expired = -1,
    Unexpired = 0,
}

export interface UserSession {
    ESPSessionTimeout: number;
    ESPAuthenticated: boolean;
    ECLWatchUser: boolean;
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

export function useMyAccount(): { currentUser: WsAccount.MyAccountResponse, isAdmin: boolean } {

    const [currentUser, setCurrentUser] = React.useState<WsAccount.MyAccountResponse>({ username: "" } as WsAccount.MyAccountResponse);
    const [isAdmin, setIsAdmin] = React.useState(false);

    const accountService = useConst(() => new AccountService({ baseUrl: "" }));
    const accessService = useConst(() => new AccessService({ baseUrl: "" }));

    React.useEffect(() => {
        accountService.MyAccount({})
            .then((account) => {
                account.username = account.username ?? "";
                if (account.username) {
                    accessService.UserEdit({ username: account.username }).then((response) => {
                        const groups = response.Groups.Group;
                        const adminGroupNames = ["Administrator", "Directory Administrators"];
                        if (response.isLDAPAdmin || groups.filter(group => !adminGroupNames.indexOf(group.name)).length > 0) {
                            setIsAdmin(true);
                            dojoConfig.isAdmin = true;
                        } else {
                            setIsAdmin(account.accountType === "Administrator");
                            dojoConfig.isAdmin = account.accountType === "Administrator";
                        }
                    });
                } else {
                    setIsAdmin(true);
                    dojoConfig.isAdmin = true;
                }
                setCurrentUser(account);
            });
    }, [accessService, accountService, setCurrentUser]);

    return { currentUser, isAdmin };

}

export const STORE_START_PAGE_ID = "startPage";
const STORE_START_PAGE_REDIRECTION = "redirectToStartPage";

export function resetStartPage() {
    const store = userKeyValStore();
    return store?.delete(STORE_START_PAGE_ID);
}

export function useUserStartPage(): { startPage: string, setStartPage: (value) => void, shouldRedirect: string, setShouldRedirect: (value) => void } {
    const sessionStore = sessionKeyValStore();
    const initialShouldRedirect = (!!window.sessionStorage.getItem(STORE_START_PAGE_REDIRECTION) || "true").toString();
    const [startPage, setStartPage] = useUserStore(STORE_START_PAGE_ID, "/activities", true);
    const [shouldRedirect, setShouldRedirect] = useSessionStore(STORE_START_PAGE_REDIRECTION, initialShouldRedirect, true);
    React.useEffect(() => {
        sessionStore.set(STORE_START_PAGE_REDIRECTION, String(shouldRedirect));
    }, [sessionStore, shouldRedirect]);
    return { startPage, setStartPage, shouldRedirect, setShouldRedirect };
}