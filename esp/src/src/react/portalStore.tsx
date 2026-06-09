import * as React from "react";
import { createPortal } from "react-dom";

// ---- Store ----------------------------------------------------------------

type Subscriber = () => void;

interface PortalEntry {
    container: HTMLElement;
    children: React.ReactNode;
}

const _portals = new Map<string, PortalEntry>();
let _snapshot: ReadonlyArray<[string, PortalEntry]> = [];
const _subscribers = new Set<Subscriber>();

function _notify() {
    _snapshot = [..._portals.entries()];
    _subscribers.forEach(fn => fn());
}

/** Register a DOM container for a portal. Call before updatePortal. */
export function addPortal(key: string, container: HTMLElement): void {
    _portals.set(key, { container, children: null });
    _notify();
}

/** Push new React children into a registered portal. */
export function updatePortal(key: string, children: React.ReactNode): void {
    const entry = _portals.get(key);
    if (entry) {
        entry.children = children;
        _notify();
    }
}

/** Remove a portal and its DOM container from the registry. */
export function removePortal(key: string): void {
    _portals.delete(key);
    _notify();
}

// ---- React hook -----------------------------------------------------------

function subscribe(fn: Subscriber): () => void {
    _subscribers.add(fn);
    return () => _subscribers.delete(fn);
}

function getSnapshot(): ReadonlyArray<[string, PortalEntry]> {
    return _snapshot;
}

export function usePortals(): ReadonlyArray<[string, PortalEntry]> {
    return React.useSyncExternalStore(subscribe, getSnapshot);
}

// ---- Renderer component ---------------------------------------------------

/**
 * Render all portals registered via addPortal/updatePortal inside the current
 * React tree. Place this once inside the app's top-level ThemeProvider/
 * FluentProvider so that theme context flows into every portal automatically.
 */
export const PortalRenderer: React.FunctionComponent = () => {
    const portals = usePortals();
    return (
        <>
            {portals.map(([key, { container, children }]) =>
                createPortal(children, container, key)
            )}
        </>
    );
};
