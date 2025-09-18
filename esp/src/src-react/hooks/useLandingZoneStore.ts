import * as React from "react";
import { useConst } from "@fluentui/react-hooks";
import { CreateLandingZonesStore, LandingZonesStore } from "../comms/fileSpray";

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

    const store: LandingZonesStore = useConst(() => CreateLandingZonesStore());

    const refresh = React.useCallback(async () => {
        setLoading(true);
        setError(null);

        try {
            const data = await store.loadData();
            setData(data || []);
            setLoading(false);
        } catch (err: any) {
            setError(err?.message);
            setLoading(false);
        }
    }, [store]);

    React.useEffect(() => {
        refresh();
    }, [refresh]);

    return { data, loading, error, refresh };
}
