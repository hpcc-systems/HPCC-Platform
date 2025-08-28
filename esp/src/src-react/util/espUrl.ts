/**
 * Pure TypeScript utility for constructing ESP service URLs without Dojo dependencies.
 * This is intended to replace ESPBase.getBaseURL() usage in React components.
 */

function getUrlParam(key: string): string | undefined {
    if (typeof window === "undefined" || !window.location?.search) {
        return undefined;
    }

    const searchParams = new URLSearchParams(window.location.search);
    return searchParams.get(key) || undefined;
}

interface DebugConfig {
    IP?: string;
    Port?: string;
}

function getServerIP(): string | null {
    // Check for serverIP in URL params first
    const serverIP = getUrlParam("serverIP") || getUrlParam("ServerIP");
    if (serverIP) {
        return serverIP;
    }

    // Check for debugConfig (mimicking dojo config behavior)
    const debugConfig = (window as { debugConfig?: DebugConfig }).debugConfig;
    if (debugConfig?.IP) {
        return debugConfig.IP;
    }

    return null;
}

export function getESPBaseURL(service: string = "WsWorkunits"): string {
    const serverIP = getServerIP();

    if (serverIP) {
        const protocol = (typeof window !== "undefined" && window.location?.protocol) ? window.location.protocol.replace(":", "") : "http";
        return `${protocol}://${serverIP}:8010/${service}`;
    }

    return `/${service}`;
}

export function isCrossSite(): boolean {
    return getServerIP() !== null;
}

/**
 * Legacy compatibility wrapper for getESPBaseURL.
 * Use getESPBaseURL directly in new code.
 *
 * @deprecated Use getESPBaseURL instead
 */
export function getBaseURL(service?: string): string {
    return getESPBaseURL(service);
}
