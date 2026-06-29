import { debounce as debounceMethod } from "@hpcc-js/util";

type TypeOfClassMethod<T, M extends keyof T> = T[M] extends (...args: any[]) => any ? T[M] : never;

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

export interface ThrottleQueueItem<T> {
    resolve: (value: T) => void;
    reject: (reason?: any) => void;
    args: any[];
}

export interface ThrottleOptions<T> {
    parallel?: number;
    timeout?: number;
    // Optional queue to allow host to empty if needed.  If not provided, a new queue will be created.
    queue?: ThrottleQueueItem<T>[]
}

export function throttle<T>(func: (...args: any[]) => T | Promise<T>, options: ThrottleOptions<T> = {}): (...args: any[]) => Promise<T> {
    const { parallel = 4, timeout = 600, queue = [] } = options;
    let activeCount = 0;

    const processQueue = async () => {
        if (queue.length === 0 || activeCount >= parallel) {
            return;
        }

        activeCount++;
        const { resolve, reject, args } = queue.shift() as ThrottleQueueItem<T>;

        try {
            const result = await func(...args);
            resolve(result);
        } catch (error) {
            reject(error);
        } finally {
            activeCount--;
            setTimeout(processQueue, timeout);
        }
    };

    return (...args) => {
        return new Promise<T>((resolve, reject) => {
            queue.push({ resolve, reject, args });
            processQueue();
        });
    };
}