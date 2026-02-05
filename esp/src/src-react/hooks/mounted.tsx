import * as React from "react";

export function useMounted(): boolean {
    const [mounted, setMounted] = React.useState(false);

    React.useEffect(() => {
        let cancelled = false;
        let innerRafId: number = 0;

        //  Wait for layout 
        const outerRafId = requestAnimationFrame(() => {
            //  Wait for render 
            innerRafId = requestAnimationFrame(() => {
                if (!cancelled) {
                    setMounted(true);
                }
            });
        });

        return () => {
            cancelled = true;
            if (outerRafId) {
                cancelAnimationFrame(outerRafId);
            }
            if (innerRafId) {
                cancelAnimationFrame(innerRafId);
            }
        };
    }, []);

    return mounted;
}
