import * as React from "react";

export function useCounter(): [number, () => void] {

    const [counter, setCounter] = React.useState(0);

    return [counter, () => setCounter(counter + 1)];
}

