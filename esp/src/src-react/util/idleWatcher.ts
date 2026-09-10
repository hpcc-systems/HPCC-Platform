export type IdleCallback = () => void;

//  React-owned idle watcher (no Dojo dependency).  Fires "idle" after idleDurationMs of
//  no keyboard/mouse activity;  each activity fires "active" and restarts the timer.
export class IdleWatcher {

    private _idleDurationMs: number;
    private _timer: number | undefined;
    private _running = false;
    private _onIdle = new Set<IdleCallback>();
    private _onActive = new Set<IdleCallback>();

    private _activityHandler = () => this._handleActivity();

    constructor(idleDurationMs: number) {
        this._idleDurationMs = idleDurationMs;
    }

    onIdle(callback: IdleCallback): () => void {
        this._onIdle.add(callback);
        return () => this._onIdle.delete(callback);
    }

    onActive(callback: IdleCallback): () => void {
        this._onActive.add(callback);
        return () => this._onActive.delete(callback);
    }

    setIdleDuration(idleDurationMs: number): void {
        this._idleDurationMs = idleDurationMs;
        if (this._running) {
            this._resetTimer();
        }
    }

    start(): void {
        if (this._running) return;
        this._running = true;
        document.addEventListener("keydown", this._activityHandler);
        document.addEventListener("mousedown", this._activityHandler);
        this._resetTimer();
    }

    stop(): void {
        this._running = false;
        document.removeEventListener("keydown", this._activityHandler);
        document.removeEventListener("mousedown", this._activityHandler);
        this._clearTimer();
    }

    fireIdle(): void {
        this._onIdle.forEach(cb => cb());
    }

    private _handleActivity(): void {
        this._onActive.forEach(cb => cb());
        this._resetTimer();
    }

    private _resetTimer(): void {
        this._clearTimer();
        if (this._idleDurationMs > 0) {
            this._timer = window.setTimeout(() => this.fireIdle(), this._idleDurationMs);
        }
    }

    private _clearTimer(): void {
        if (this._timer !== undefined) {
            window.clearTimeout(this._timer);
            this._timer = undefined;
        }
    }
}
