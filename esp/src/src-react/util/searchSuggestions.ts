import { SearchSuggestion } from "../hooks/autocomplete";

export interface GroupedSuggestions {
    type: string;
    label: string;
    suggestions: SearchSuggestion[];
}

export const formatSuggestionText = (suggestion: SearchSuggestion): string => {
    return suggestion.text;
};

export const groupSuggestions = (suggestions: SearchSuggestion[]): GroupedSuggestions[] => {
    const groups: { [key: string]: SearchSuggestion[] } = {};
    const order: string[] = [];

    suggestions.forEach(suggestion => {
        const type = suggestion.type;
        if (!groups[type]) {
            groups[type] = [];
            order.push(type);
        }
        groups[type].push(suggestion);
    });

    const typeLabels: { [key: string]: string } = {
        workunit: "Workunits",
        file: "Files",
        query: "Queries",
        dfuworkunit: "DFU Workunits",
        user: "Users",
        cluster: "Clusters",
        page: "Pages",
        prefix: "Search Prefixes",
        recent: "Recent Searches"
    };

    return order.map(type => ({
        type,
        label: typeLabels[type] || type,
        suggestions: groups[type]
    }));
};

export const getSuggestionRoute = (suggestion: SearchSuggestion): string => {
    // If suggestion has a route, use it
    if (suggestion.route) {
        return suggestion.route;
    }

    // Otherwise, fallback to search
    return `/search/${encodeURIComponent(suggestion.text)}`;
};
