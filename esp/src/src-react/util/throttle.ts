import { debounce } from "@hpcc-js/util";

// eslint-disable-next-line @typescript-eslint/ban-types
type TypeOfClassMethod<T, M extends keyof T> = T[M] extends Function ? T[M] : never;

export function singletonDebounce<T, M extends keyof T>(obj: T, method: M, timeoutSecs: number = 1): TypeOfClassMethod<T, M> {
    const __lazy__ = Symbol.for(`__lazy__${method}`);
    if (!obj[__lazy__]) {
        obj[__lazy__] = debounce((...args: any[]) => (obj[method] as any)(...args), timeoutSecs * 1000);
    }
    return obj[__lazy__];
}
