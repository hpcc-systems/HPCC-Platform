import { SearchSuggestion } from "../hooks/autocomplete";

export const formatSuggestionText = (suggestion: SearchSuggestion): string => {
    switch (suggestion.type) {
        case "page":
            return `Page: ${suggestion.text}`;
        case "prefix":
            return suggestion.text;
        case "recent":
            return `Recent: ${suggestion.text}`;
        case "cluster":
            return `Cluster: ${suggestion.text}`;
        case "workunit":
            return `Workunit: ${suggestion.text}`;
        case "file":
            return `File: ${suggestion.text}`;
        case "query":
            return `Query: ${suggestion.text}`;
        case "dfuworkunit":
            return `DFU: ${suggestion.text}`;
        case "user":
            return `User: ${suggestion.text}`;
        default:
            return suggestion.text;
    }
};

export const getSuggestionRoute = (suggestion: SearchSuggestion): string => {
    // If suggestion has a route, use it
    if (suggestion.route) {
        return suggestion.route;
    }

    // Otherwise, fallback to search
    return `/search/${encodeURIComponent(suggestion.text)}`;
};
