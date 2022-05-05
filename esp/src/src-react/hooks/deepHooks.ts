import * as React from "react";
import { deepEquals, verboseDeepEquals } from "@hpcc-js/util";

//  Inspired by:  https://github.com/kentcdodds/use-deep-compare-effect

function useDeepCompareMemoize<T>(value: T, verbose: boolean) {
    const ref = React.useRef<T>(value);
    const signalRef = React.useRef<number>(0);

    const equals = verbose ? verboseDeepEquals(value, ref.current) : deepEquals(value, ref.current);
    if (!equals) {
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

export function useDeepEffect(callback: EffectCallback, dependencies: DependencyList, deepDependencies: DependencyList, verbose = false): UseEffectReturn {

    // eslint-disable-next-line react-hooks/exhaustive-deps
    return React.useEffect(callback, [...dependencies, ...useDeepCompareMemoize(deepDependencies, verbose)]);
}

type UseCallbackParams = Parameters<typeof React.useCallback>
type CallbackCallback = UseCallbackParams[0]
type UseCallbackReturn = ReturnType<typeof React.useCallback>

export function useDeepCallback(callback: CallbackCallback, dependencies: DependencyList, deepDependencies: DependencyList, verbose = false): UseCallbackReturn {

    // eslint-disable-next-line react-hooks/exhaustive-deps
    return React.useCallback(callback, [...dependencies, ...useDeepCompareMemoize(deepDependencies, verbose)]);
}

export function useDeepMemo(memo: CallbackCallback, dependencies: DependencyList, deepDependencies: DependencyList, verbose = false) {

    // eslint-disable-next-line react-hooks/exhaustive-deps
    return React.useMemo(memo, [...dependencies, ...useDeepCompareMemoize(deepDependencies, verbose)]);
}
