import { debounce as debounceMethod } from "@hpcc-js/util";

// eslint-disable-next-line @typescript-eslint/ban-types
type TypeOfClassMethod<T, M extends keyof T> = T[M] extends Function ? T[M] : never;

export function singletonDebounce<T, M extends keyof T>(obj: T, method: M, timeoutSecs: number = 1): TypeOfClassMethod<T, M> {
    const __lazy__ = Symbol.for(`__lazy__${method as string}`);
    if (!obj[__lazy__]) {
        obj[__lazy__] = debounceMethod((...args: any[]) => (obj[method] as any)(...args), timeoutSecs * 1000);
    }
    return obj[__lazy__];
}

export function debounce(func: (...args: any[]) => void, timeout = 300): (...args: any[]) => void {
    let timer;
    const retVal = (...args: any[]) => {
        clearTimeout(timer);
        timer = setTimeout(() => { func.apply(this, args); }, timeout);
    };
    return retVal;
}
