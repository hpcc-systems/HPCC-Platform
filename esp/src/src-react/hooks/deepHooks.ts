import * as React from "react";
import { deepEquals } from "@hpcc-js/util";

//  Inpired from:  https://github.com/kentcdodds/use-deep-compare-effect

function useDeepCompareMemoize<T>(value: T) {
    const ref = React.useRef<T>(value);
    const signalRef = React.useRef<number>(0);

    if (!deepEquals(value, ref.current)) {
        ref.current = value;
        signalRef.current += 1;
    }

    // eslint-disable-next-line react-hooks/exhaustive-deps
    return React.useMemo(() => ref.current, [signalRef.current]);
}

type UseEffectParams = Parameters<typeof React.useEffect>
type EffectCallback = UseEffectParams[0]
type DependencyList = UseEffectParams[1]
type UseEffectReturn = ReturnType<typeof React.useEffect>

export function useDeepEffect(callback: EffectCallback, dependencies: DependencyList, deepDependencies: DependencyList): UseEffectReturn {

    // eslint-disable-next-line react-hooks/exhaustive-deps
    return React.useEffect(callback, [...dependencies, ...useDeepCompareMemoize(deepDependencies)]);
}

type UseCallbackParams = Parameters<typeof React.useCallback>
type CallbackCallback = UseCallbackParams[0]
type UseCallbackReturn = ReturnType<typeof React.useCallback>

export function useDeepCallback(callback: CallbackCallback, dependencies: DependencyList, deepDependencies: DependencyList): UseCallbackReturn {

    // eslint-disable-next-line react-hooks/exhaustive-deps
    return React.useCallback(callback, [...dependencies, ...useDeepCompareMemoize(deepDependencies)]);
}
