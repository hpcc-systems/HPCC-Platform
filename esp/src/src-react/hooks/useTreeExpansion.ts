import * as React from "react";
import { scopedLogger } from "@hpcc-js/util";
import { useConst } from "@fluentui/react-hooks";
import { userKeyValStore } from "src/KeyValStore";

const logger = scopedLogger("src-react/hooks/useTreeExpansion.ts");

/**
 * Generic hook for managing tree expansion state with optional persistence
 *
 * @param storageKey - Optional storage key for persisting expansion state. If omitted, expansion is not persisted.
 * @param externalExpandedItems - External expanded items set to merge with persisted items
 * @returns Object with expandedItems, updateExpandedItems, isLoaded, and toggle functions
 */
export const useTreeExpansion = (
    storageKey?: string,
    externalExpandedItems?: Set<string>
) => {
    const store = useConst(() => storageKey ? userKeyValStore() : null);
    const [expandedItems, setExpandedItems] = React.useState<Set<string>>(new Set());
    const [isLoaded, setIsLoaded] = React.useState(!storageKey); // immediate load if no storage

    React.useEffect(() => {
        // Reset state when the key changes to avoid leaking expansion state between keys
        setExpandedItems(new Set());
        setIsLoaded(false);

        if (!store || !storageKey) {
            setIsLoaded(true);
            return;
        }

        store.get(storageKey).then((stored) => {
            if (stored) {
                try {
                    const parsed = JSON.parse(stored);
                    if (Array.isArray(parsed)) {
                        setExpandedItems(new Set(parsed));
                        logger.debug(`Loaded ${parsed.length} expanded items from storage`);
                    }
                } catch (err) {
                    logger.error(`Failed to parse expansion state from storage: ${err}`);
                }
            }
            setIsLoaded(true);
        }).catch((err) => {
            logger.warning(`Failed to retrieve expansion state from storage: ${err}`);
            setIsLoaded(true);
        });
    }, [store, storageKey]);

    const updateExpandedItems = React.useCallback((newExpandedItems: Set<string>) => {
        setExpandedItems(newExpandedItems);

        if (store && storageKey) {
            store.set(storageKey, JSON.stringify(Array.from(newExpandedItems))).catch(_err => {
                logger.error(`Failed to persist expansion state: ${_err}`);
            });
        }
    }, [store, storageKey]);

    const mergedExpandedItems = React.useMemo(() => {
        if (!isLoaded) {
            return externalExpandedItems || new Set<string>();
        }
        return new Set<string>([...expandedItems, ...(externalExpandedItems || new Set<string>())]);
    }, [expandedItems, externalExpandedItems, isLoaded]);

    const toggle = React.useCallback((itemId: string) => {
        const newExpandedItems = new Set<string>(mergedExpandedItems);
        if (mergedExpandedItems.has(itemId)) {
            newExpandedItems.delete(itemId);
        } else {
            newExpandedItems.add(itemId);
        }
        updateExpandedItems(newExpandedItems);
        return newExpandedItems;
    }, [mergedExpandedItems, updateExpandedItems]);

    const expand = React.useCallback((itemId: string) => {
        if (!mergedExpandedItems.has(itemId)) {
            const newExpandedItems = new Set<string>(mergedExpandedItems);
            newExpandedItems.add(itemId);
            updateExpandedItems(newExpandedItems);
            return newExpandedItems;
        }
        return mergedExpandedItems;
    }, [mergedExpandedItems, updateExpandedItems]);

    const collapse = React.useCallback((itemId: string) => {
        if (mergedExpandedItems.has(itemId)) {
            const newExpandedItems = new Set<string>(mergedExpandedItems);
            newExpandedItems.delete(itemId);
            updateExpandedItems(newExpandedItems);
            return newExpandedItems;
        }
        return mergedExpandedItems;
    }, [mergedExpandedItems, updateExpandedItems]);

    return {
        expandedItems: mergedExpandedItems,
        setExpandedItems: updateExpandedItems,
        isLoaded,
        toggle,
        expand,
        collapse
    };
};
