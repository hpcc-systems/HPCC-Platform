import { useEffect, useState } from "react";
import { userKeyValStore } from "../../KeyValStore";

const user_store = userKeyValStore();

export const useGet = (key: string, filter?: object) => {
    const [responseState, setResponseState] = useState({ data: null, loading: true });
    useEffect(() => {
        setResponseState({ data: null, loading: true });
        user_store.get(key)
            .then(item => (item ? JSON.parse(item) : undefined))
            .then(response => {
                setResponseState({ data: response, loading: false });
            });
    }, [filter]);
    return responseState;
};
