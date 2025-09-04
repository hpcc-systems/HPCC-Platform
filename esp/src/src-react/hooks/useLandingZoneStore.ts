import * as React from "react";
import { CreateLandingZonesStore } from "../comms/fileSpray";

interface UseLandingZoneStoreOptions {
    filter?: any;
}

interface UseLandingZoneStoreResult {
    data: any[];
    loading: boolean;
    error: string | null;
    refresh: () => void;
}

export function useLandingZoneStore(options: UseLandingZoneStoreOptions = {}): UseLandingZoneStoreResult {
    const [data, setData] = React.useState<any[]>([]);
    const [loading, setLoading] = React.useState(false);
    const [error, setError] = React.useState<string | null>(null);

    const store = React.useMemo(() => CreateLandingZonesStore(), []);

    const refresh = React.useCallback(() => {
        setLoading(true);
        setError(null);

        // access the protected fetchData method through a type assertion
        // this is the same pattern used by the store's query method
        const storeInternal = store as any;
        const request = {};
        const queryOptions = { start: 0, count: 100 };

        storeInternal.fetchData(request, queryOptions)
            .then((response: { data: any[], total: number }) => {
                setData(response.data || []);
                setLoading(false);
            })
            .catch((err: any) => {
                console.error("useLandingZoneStore: ", err);
                setError(err?.message);
                setLoading(false);
            });
    }, [store]);

    React.useEffect(() => {
        refresh();
    }, [refresh]);

    return { data, loading, error, refresh };
}
