import { useEffect, useState } from "react";
import {getRecentFilters} from "../../KeyValStore";

export const useGet = (key: string, filter?: object) => {
    const [responseState, setResponseState] = useState({ data: null, loading: true });
    useEffect(() => {
        setResponseState({ data: null, loading: true });
        getRecentFilters(key).then(response => {
            setResponseState({ data: response, loading: false });
        });
    }, [filter]);
    return responseState;
};
