import * as React from "react";
import { Dispatch, Message, IObserverHandle } from "@hpcc-js/util";

class Signal<T> {

    private _value: T;
    private _dispatch = new Dispatch<Message>();

    constructor(value: T) {
        this._value = value;
    }

    set(value: T) {
        if (this._value !== value) {
            this._value = value;
            this._dispatch.send(new Message());
        }
    }

    get() {
        return this._value;
    }

    watch(callback: (value: T) => void): IObserverHandle {
        return this._dispatch.attach(() => {
            callback(this._value);
        });
    }
}

export const signal = <T>(initialValue: T): Signal<T> => {
    return new Signal<T>(initialValue);
};

export const useSignal = <T>(signal: Signal<T>) => {
    const [value, setValue] = React.useState(signal.get());

    React.useEffect(() => {
        const watcher = signal.watch(() => setValue(signal.get()));
        return () => watcher.unwatch();
    }, [signal]);

    return value;
};
