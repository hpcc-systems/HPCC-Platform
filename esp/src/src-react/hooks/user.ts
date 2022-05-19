import * as React from "react";
import { useConst } from "@fluentui/react-hooks";
import { AccountService, WsAccount } from "@hpcc-js/comms";

export function useMyAccount(): { currentUser: WsAccount.MyAccountResponse } {

    const [currentUser, setCurrentUser] = React.useState<WsAccount.MyAccountResponse>();

    const service = useConst(() => new AccountService({ baseUrl: "" }));

    React.useEffect(() => {
        service.MyAccount({})
            .then((response) => {
                setCurrentUser(response);
            });
    }, [service, setCurrentUser]);

    return { currentUser };
}