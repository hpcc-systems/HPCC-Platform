import * as React from "react";

export function useCounter(): [number, () => void] {

    const [counter, setCounter] = React.useState(0);

    return [counter, () => setCounter(counter + 1)];
}

export function useIsMounted(): boolean {
    const isMountedRef = React.useRef(false);

    React.useEffect(() => {
        isMountedRef.current = true;
        return () => {
            isMountedRef.current = false;
        };
    }, []);

    return isMountedRef.current;
}

export function useHasFocus(): boolean {

    const [hasFocus, setHasFocus] = React.useState(true);

    const onFocus = () => {
        setHasFocus(true);
    };

    const onBlur = () => {
        setHasFocus(false);
    };

    React.useEffect(() => {
        window.addEventListener("focus", onFocus);
        window.addEventListener("blur", onBlur);

        return () => {
            window.removeEventListener("focus", onFocus);
            window.removeEventListener("blur", onBlur);
        };
    }, []);

    return hasFocus;
}
