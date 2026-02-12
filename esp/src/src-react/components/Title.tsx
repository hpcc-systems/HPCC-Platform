import * as React from "react";
import { ContextualMenuItemType, DefaultButton, IconButton, IContextualMenuItem, IIconProps, IPersonaSharedProps, Link, mergeStyleSets, Persona, PersonaSize, Text } from "@fluentui/react";
import { StackShim, StackItemShim } from "@fluentui/react-migration-v8-v9";
import { Button, ButtonProps, CounterBadgeProps, CounterBadge, SearchBox, Toaster } from "@fluentui/react-components";
import { WindowNewRegular } from "@fluentui/react-icons";
import { Level, scopedLogger } from "@hpcc-js/util";
import { cookie } from "src-dojo/index";

import nlsHPCC from "src/nlsHPCC";
import * as Utility from "src/Utility";

import { useUserTheme } from "../hooks/theme";
import { useBanner } from "../hooks/banner";
import { useConfirm } from "../hooks/confirm";
import { replaceUrl } from "../util/history";
import { useECLWatchLogger } from "../hooks/logging";
import { useBuildInfo, useModernMode, useCheckFeatures } from "../hooks/platform";
import { useGlobalStore } from "../hooks/store";
import { PasswordStatus, useMyAccount, useUserSession } from "../hooks/user";
import { useSearchAutocomplete, SearchSuggestion } from "../hooks/autocomplete";
import { formatSuggestionText, getSuggestionRoute, groupSuggestions } from "../util/searchSuggestions";

import { TitlebarConfig } from "./forms/TitlebarConfig";
import { switchTechPreview } from "./controls/ComingSoon";
import { About } from "./About";
import { MyAccount } from "./MyAccount";
import { debounce } from "../util/throttle";

const logger = scopedLogger("src-react/components/Title.tsx");

const NewTabButton: React.FunctionComponent<ButtonProps> = (props) => {
    return <Button
        {...props}
        appearance="transparent"
        icon={<WindowNewRegular />}
        size="small"
    />;
};

const collapseMenuIcon: IIconProps = { iconName: "CollapseMenu" };

const waffleIcon: IIconProps = { iconName: "WaffleOffice365" };

const personaStyles = {
    root: {
        display: "flex",
        alignItems: "center",
        "&:hover": { cursor: "pointer" }
    }
};

const DAY = 1000 * 60 * 60 * 24;

interface DevTitleProps {
    setNavWideMode: React.Dispatch<React.SetStateAction<boolean>>;
    navWideMode: boolean;
}

export const DevTitle: React.FunctionComponent<DevTitleProps> = ({
    setNavWideMode,
    navWideMode
}) => {

    const [, { opsCategory }] = useBuildInfo();
    const { theme } = useUserTheme();
    const { userSession, setUserSession, deleteUserSession } = useUserSession();
    const [logIconColor, setLogIconColor] = React.useState<CounterBadgeProps["color"]>();

    const [showAbout, setShowAbout] = React.useState(false);
    const [searchValue, setSearchValue] = React.useState("");
    const [showMyAccount, setShowMyAccount] = React.useState(false);
    const { currentUser, isAdmin } = useMyAccount();
    const { suggestions, filterSuggestions, saveRecentSearch, isSearching } = useSearchAutocomplete();
    const [showSuggestions, setShowSuggestions] = React.useState(false);
    const [filteredSuggestions, setFilteredSuggestions] = React.useState<SearchSuggestion[]>([]);
    const searchBoxRef = React.useRef<HTMLDivElement>(null);
    const suggestionsBoxRef = React.useRef<HTMLDivElement>(null);
    const [highlightedIndex, setHighlightedIndex] = React.useState(-1);
    const searchRequestIdRef = React.useRef(0);

    const [showTitlebarConfig, setShowTitlebarConfig] = React.useState(false);
    const [showEnvironmentTitle] = useGlobalStore("HPCCPlatformWidget_Toolbar_Active", false, true);
    const [environmentTitle] = useGlobalStore("HPCCPlatformWidget_Toolbar_Text", undefined, true);
    const [titlebarColor] = useGlobalStore("HPCCPlatformWidget_Toolbar_Color", undefined, true);

    const [showBannerConfig, setShowBannerConfig] = React.useState(false);
    const [BannerMessageBar, BannerConfig] = useBanner({ showForm: showBannerConfig, setShowForm: setShowBannerConfig });

    const [PasswordExpiredConfirm, setPasswordExpiredConfirm] = useConfirm({
        title: nlsHPCC.PasswordExpiration,
        message: nlsHPCC.PasswordExpired,
        cancelLabel: null,
        onSubmit: React.useCallback(() => {
            setShowMyAccount(true);
        }, [])
    });

    // Debounced filter function to prevent excessive API calls
    const debouncedFilter = React.useRef(
        debounce((value: string, requestId: number) => {
            filterSuggestions(value).then(filtered => {
                // Ignore stale results
                if (requestId !== searchRequestIdRef.current) {
                    return;
                }
                setFilteredSuggestions(filtered);
                setShowSuggestions(filtered.length > 0);
            }).catch(err => {
                console.error("Failed to filter suggestions:", err);
                // Don't hide suggestions on error, keep showing what we have
            });
        }, 300)
    ).current;

    const onSearchChange = React.useCallback((evt, data) => {
        const value = data?.value ?? "";
        setSearchValue(value);
        setHighlightedIndex(-1);
        const requestId = ++searchRequestIdRef.current;

        // Immediately show/hide suggestions based on whether we have a value
        if (!value.trim()) {
            setFilteredSuggestions(suggestions);
            setShowSuggestions(true);
        } else {
            debouncedFilter(value, requestId);
        }
    }, [suggestions, debouncedFilter]);

    const onSearchKeyDown = React.useCallback((evt) => {
        if (evt.key === "ArrowDown") {
            evt.preventDefault();
            if (!showSuggestions || filteredSuggestions.length === 0) return;
            setHighlightedIndex(prev =>
                prev < filteredSuggestions.length - 1 ? prev + 1 : prev
            );
        } else if (evt.key === "ArrowUp") {
            evt.preventDefault();
            if (!showSuggestions || filteredSuggestions.length === 0) return;
            setHighlightedIndex(prev => prev > 0 ? prev - 1 : -1);
        } else if (evt.key === "Enter") {
            evt.preventDefault();
            const inputValue = (evt.target as HTMLInputElement)?.value;
            if (!inputValue) return;
            const trimmedValue = inputValue.trim();

            // Ctrl+Enter always searches all categories
            if (evt.ctrlKey) {
                saveRecentSearch(trimmedValue);
                setShowSuggestions(false);
                setHighlightedIndex(-1);
                window.open(`#/search/${encodeURIComponent(trimmedValue)}`);
                return;
            }

            // Enter without Ctrl: navigate to highlighted or first suggestion
            if (highlightedIndex >= 0 && highlightedIndex < filteredSuggestions.length) {
                const highlightedSuggestion = filteredSuggestions[highlightedIndex];

                if (highlightedSuggestion.requiresAdmin && !isAdmin) {
                    return;
                }

                saveRecentSearch(highlightedSuggestion.text);
                setShowSuggestions(false);
                setHighlightedIndex(-1);

                const route = getSuggestionRoute(highlightedSuggestion);
                window.location.href = `#${route}`;
                return;
            }

            // No highlighted suggestion, check for first available suggestion
            const requestId = ++searchRequestIdRef.current;

            filterSuggestions(trimmedValue).then(filtered => {
                // Ignore stale results
                if (requestId !== searchRequestIdRef.current) {
                    return;
                }

                if (filtered.length > 0) {
                    const firstSuggestion = filtered[0];

                    if (firstSuggestion.requiresAdmin && !isAdmin) {
                        return;
                    }

                    saveRecentSearch(trimmedValue);
                    setShowSuggestions(false);
                    setHighlightedIndex(-1);

                    const route = getSuggestionRoute(firstSuggestion);
                    window.location.href = `#${route}`;
                } else {
                    // No suggestions, search all categories
                    saveRecentSearch(trimmedValue);
                    setShowSuggestions(false);
                    setHighlightedIndex(-1);
                    window.location.href = `#/search/${encodeURIComponent(trimmedValue)}`;
                }
            }).catch(err => {
                console.error("Failed to navigate to suggestion:", err);
                // On error, fall back to general search
                saveRecentSearch(trimmedValue);
                setShowSuggestions(false);
                setHighlightedIndex(-1);
                window.location.href = `#/search/${encodeURIComponent(trimmedValue)}`;
            });
        } else if (evt.key === "Escape") {
            setShowSuggestions(false);
            setHighlightedIndex(-1);
        }
    }, [filterSuggestions, isAdmin, saveRecentSearch, showSuggestions, filteredSuggestions, highlightedIndex]);

    const onSearchNewTabClick = React.useCallback(() => {
        if (!searchValue) return;
        const trimmedValue = searchValue.trim();
        const requestId = ++searchRequestIdRef.current;

        filterSuggestions(trimmedValue).then(filtered => {
            // Ignore stale results
            if (requestId !== searchRequestIdRef.current) {
                return;
            }

            if (filtered.length > 0) {
                const firstSuggestion = filtered[0];

                if (firstSuggestion.requiresAdmin && !isAdmin) {
                    return;
                }

                saveRecentSearch(trimmedValue);
                setShowSuggestions(false);

                const route = getSuggestionRoute(firstSuggestion);
                window.open(`#${route}`);
            } else {
                saveRecentSearch(trimmedValue);
                setShowSuggestions(false);
                window.open(`#/search/${encodeURIComponent(trimmedValue)}`);
            }
        }).catch(err => {
            console.error("Failed to open suggestion in new tab:", err);
            // On error, fall back to general search
            saveRecentSearch(trimmedValue);
            setShowSuggestions(false);
            window.open(`#/search/${encodeURIComponent(trimmedValue)}`);
        });
    }, [filterSuggestions, isAdmin, saveRecentSearch, searchValue]);

    const onSuggestionClick = React.useCallback((suggestion: SearchSuggestion) => {
        if (suggestion.requiresAdmin && !isAdmin) {
            return;
        }

        setSearchValue(suggestion.text);
        setShowSuggestions(false);
        setHighlightedIndex(-1);
        saveRecentSearch(suggestion.text);
        const route = getSuggestionRoute(suggestion);
        window.location.href = `#${route}`;
    }, [isAdmin, saveRecentSearch]);

    const onSearchFocus = React.useCallback(() => {
        setHighlightedIndex(-1);
        const requestId = ++searchRequestIdRef.current;

        filterSuggestions(searchValue).then(filtered => {
            // Ignore stale results
            if (requestId !== searchRequestIdRef.current) {
                return;
            }
            setFilteredSuggestions(filtered);
            setShowSuggestions(filtered.length > 0);
        }).catch(err => {
            console.error("Failed to load suggestions on focus:", err);
            // Show empty state suggestions on error
            setFilteredSuggestions(suggestions);
            setShowSuggestions(suggestions.length > 0);
        });
    }, [filterSuggestions, searchValue, suggestions]);

    const onSuggestionMouseEnter = React.useCallback((e: React.MouseEvent<HTMLDivElement>) => {
        const index = parseInt(e.currentTarget.getAttribute("data-index") || "-1", 10);
        if (index >= 0) {
            setHighlightedIndex(index);
        }
    }, []);

    const onSuggestionMouseLeave = React.useCallback(() => {
        setHighlightedIndex(-1);
    }, []);

    const searchStatusText = React.useMemo(() => {
        if (isSearching) {
            return nlsHPCC.Loading;
        }

        if (!searchValue.trim()) {
            return nlsHPCC.SearchForPagesWorkunitFilesQueries;
        }

        if (filteredSuggestions.length > 0) {
            const firstSuggestion = filteredSuggestions[highlightedIndex >= 0 ? highlightedIndex : 0];
            const suggestionName = firstSuggestion?.text || "";
            // Escape HTML to prevent XSS
            const escapedName = suggestionName.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
            return nlsHPCC.PressEnterToOpen.replace("{0}", escapedName);
        }

        return nlsHPCC.PressEnterToSearchAll;
    }, [isSearching, searchValue, filteredSuggestions, highlightedIndex]);

    const titlebarColorSet = React.useMemo(() => {
        return titlebarColor && titlebarColor !== theme.palette.themeLight;
    }, [theme.palette, titlebarColor]);

    const personaProps: IPersonaSharedProps = React.useMemo(() => {
        return {
            text: (currentUser?.firstName && currentUser?.lastName) ? currentUser.firstName + " " + currentUser.lastName : currentUser?.username,
            secondaryText: currentUser?.accountType,
            size: PersonaSize.size32
        };
    }, [currentUser]);

    const { id: toasterId, log, lastUpdate: logLastUpdated } = useECLWatchLogger();

    const { setModernMode } = useModernMode();
    const onTechPreviewClick = React.useCallback(
        (ev?: React.MouseEvent<HTMLButtonElement>, item?: IContextualMenuItem): void => {
            setModernMode(String(false));
            switchTechPreview(false, opsCategory);
        },
        [opsCategory, setModernMode]
    );

    const [logCount, setLogCount] = React.useState(0);
    React.useEffect(() => {
        const errorCodes = [Level.alert, Level.critical, Level.emergency, Level.error];
        const warningCodes = [Level.warning];
        const errorAndWarningCodes = errorCodes.concat(warningCodes);

        const logCounts = {
            errors: log.filter(msg => errorCodes.includes(msg.level)).length,
            warnings: log.filter(msg => warningCodes.includes(msg.level)).length,
            other: log.filter(msg => !errorAndWarningCodes.includes(msg.level)).length,
        };

        let count = 0;
        let color: CounterBadgeProps["color"];

        if (logCounts.errors > count) {
            count = logCounts.errors;
            color = "danger";
        } else if (logCounts.warnings > count) {
            count = logCounts.warnings;
            color = "important";
        } else {
            count = logCounts.other;
            color = "informative";
        }

        setLogCount(count);
        setLogIconColor(color);
    }, [log, logLastUpdated]);

    const advMenuProps = React.useMemo(() => {
        return {
            items: [
                { key: "banner", text: nlsHPCC.SetBanner, disabled: !!currentUser?.username && !isAdmin, onClick: () => setShowBannerConfig(true) },
                { key: "toolbar", text: nlsHPCC.SetToolbar, disabled: !!currentUser?.username && !isAdmin, onClick: () => setShowTitlebarConfig(true) },
                { key: "divider_1", itemType: ContextualMenuItemType.Divider },
                { key: "docs", href: "https://hpccsystems.com/training/documentation/", text: nlsHPCC.Documentation, target: "_blank" },
                { key: "downloads", href: "https://hpccsystems.com/download", text: nlsHPCC.Downloads, target: "_blank" },
                { key: "releaseNotes", href: "https://hpccsystems.com/download/release-notes", text: nlsHPCC.ReleaseNotes, target: "_blank" },
                {
                    key: "additionalResources", text: nlsHPCC.AdditionalResources, subMenuProps: {
                        items: [
                            { key: "redBook", href: "https://hpcc-systems.github.io/HPCC-Platform/devdoc/red_book/HPCC-Systems-Red-Book.html", text: nlsHPCC.RedBook, target: "_blank" },
                            { key: "forums", href: "https://hpccsystems.com/bb/", text: nlsHPCC.Forums, target: "_blank" },
                            { key: "issues", href: "https://hpccsystems.atlassian.net/issues/", text: nlsHPCC.IssueReporting, target: "_blank" },
                        ]
                    }
                },
                { key: "divider_2", itemType: ContextualMenuItemType.Divider },
                {
                    key: "lock", text: nlsHPCC.Lock, disabled: !currentUser?.username, onClick: () => {
                        fetch("/esp/lock", {
                            method: "post"
                        }).then(() => {
                            setUserSession({ ...userSession });
                            replaceUrl("/login", true);
                        });
                    }
                },
                {
                    key: "logout", text: nlsHPCC.Logout, disabled: !currentUser?.username, onClick: () => {
                        fetch("/esp/logout", {
                            method: "post"
                        }).then(data => {
                            if (data) {
                                deleteUserSession().then(() => {
                                    Utility.deleteCookie("ECLWatchUser");
                                    Utility.deleteCookie("ESPSessionID");
                                    Utility.deleteCookie("Status");
                                    Utility.deleteCookie("User");
                                    Utility.deleteCookie("ESPSessionState");
                                    window.location.reload();
                                });
                            }
                        });
                    }
                },
                { key: "divider_3", itemType: ContextualMenuItemType.Divider },
                { key: "config", href: "#/topology/configuration", text: nlsHPCC.Configuration },
                {
                    key: "eclwatchv9", text: "ECL Watch v9",
                    canCheck: true,
                    isChecked: true,
                    onClick: onTechPreviewClick
                },
                { key: "divider_4", itemType: ContextualMenuItemType.Divider },
                {
                    key: "reset",
                    href: "/esp/files/index.html#/reset",
                    text: nlsHPCC.ResetUserSettings
                },
                { key: "about", text: nlsHPCC.About, onClick: () => setShowAbout(true) }
            ],
            directionalHintFixed: true
        };
    }, [currentUser?.username, deleteUserSession, isAdmin, onTechPreviewClick, setUserSession, userSession]);

    const btnStyles = React.useMemo(() => mergeStyleSets({
        errorsWarnings: {
            border: "none",
            background: "transparent",
            minWidth: 48,
            padding: "0 10px 0 4px",
            color: titlebarColor ? Utility.textColor(titlebarColor) : theme.semanticColors.link
        },
        errorsWarningsCount: {
            margin: "-3px 0 0 -3px"
        }
    }), [theme.semanticColors.link, titlebarColor]);

    React.useEffect(() => {
        switch (log.reduce((prev, cur) => Math.max(prev, cur.level), Level.debug)) {
            case Level.alert:
            case Level.critical:
            case Level.emergency:
                setLogIconColor("danger");
                break;
            case Level.error:
                setLogIconColor("danger");
                break;
            case Level.warning:
                setLogIconColor("important");
                break;
            case Level.info:
            case Level.notice:
            case Level.debug:
            default:
                setLogIconColor("informative");
                break;
        }
    }, [log, logLastUpdated, theme]);

    const features = useCheckFeatures();

    React.useEffect(() => {
        if (!features.timestamp) return;
        let ancient = 90;
        let veryOld = 60;
        let old = 30;
        if (features.maturity === "trunk") {
            ancient = 360;
            veryOld = 180;
            old = 90;
        } else if (features.maturity === "rc") {
            ancient = 28;
            veryOld = 21;
            old = 14;
        }
        const age = Math.floor((Date.now() - features.timestamp.getTime()) / DAY);
        const message = nlsHPCC.PlatformBuildIsNNNDaysOld.replace("NNN", `${age}`);
        if (age > ancient) {
            logger.alert(message + `  ${nlsHPCC.PleaseUpgradeToLaterPointRelease}`);
        } else if (age > veryOld) {
            logger.error(message + `  ${nlsHPCC.PleaseUpgradeToLaterPointRelease}`);
        } else if (age > old) {
            logger.warning(message + `  ${nlsHPCC.PleaseUpgradeToLaterPointRelease}`);
        } else {
            logger.info(message);
        }
    }, [features.maturity, features.timestamp]);

    // Sync suggestions from hook when search is empty
    React.useEffect(() => {
        if (!searchValue.trim() && suggestions.length > 0) {
            setFilteredSuggestions(suggestions);
        }
    }, [suggestions, searchValue]);

    React.useEffect(() => {
        const handleClickOutside = (event: MouseEvent) => {
            const target = event.target as Node;
            // Only close if clicking outside both the search box AND the suggestions dropdown
            if (searchBoxRef.current && !searchBoxRef.current.contains(target) &&
                suggestionsBoxRef.current && !suggestionsBoxRef.current.contains(target)) {
                setShowSuggestions(false);
                setHighlightedIndex(-1);
            }
        };

        document.addEventListener("mousedown", handleClickOutside);
        return () => {
            document.removeEventListener("mousedown", handleClickOutside);
        };
    }, []);

    React.useEffect(() => {
        if (!currentUser.username) return;
        if (!cookie("PasswordExpiredCheck")) {
            // cookie expires option expects whole number of days, use a decimal < 1 for hours
            cookie("PasswordExpiredCheck", "true", { expires: 0.5, path: "/" });
            switch (currentUser.passwordDaysRemaining) {
                case PasswordStatus.Expired:
                    setPasswordExpiredConfirm(true);
                    break;
                case PasswordStatus.NeverExpires:
                case null:
                    break;
                default:
                    if (currentUser?.passwordDaysRemaining != null && currentUser.passwordDaysRemaining <= (currentUser?.passwordExpirationWarningDays ?? 0)) {
                        if (confirm(nlsHPCC.PasswordExpirePrefix + currentUser.passwordDaysRemaining + nlsHPCC.PasswordExpirePostfix)) {
                            setShowMyAccount(true);
                        }
                    }
                    break;
            }
        }
    }, [currentUser, setPasswordExpiredConfirm]);

    return <div style={{ backgroundColor: titlebarColorSet ? titlebarColor : theme.palette.themeLight }}>
        <BannerMessageBar />
        <StackShim horizontal verticalAlign="center" horizontalAlign="space-between">
            <StackItemShim align="center">
                <StackShim horizontal>
                    <StackItemShim>
                        <IconButton iconProps={waffleIcon} onClick={() => setNavWideMode(!navWideMode)} style={{ width: 48, height: 48, color: titlebarColorSet ? Utility.textColor(titlebarColor) : theme.palette.themeDarker }} />
                    </StackItemShim>
                    <StackItemShim align="center">
                        <Link href="#/activities">
                            <Text variant="large" nowrap block >
                                <b title="ECL Watch" style={{ paddingLeft: "8px", color: titlebarColorSet ? Utility.textColor(titlebarColor) : theme.palette.themeDarker }}>
                                    {(showEnvironmentTitle && environmentTitle) ? environmentTitle : "ECL Watch"}
                                </b>
                            </Text>
                        </Link>
                    </StackItemShim>
                </StackShim>
            </StackItemShim>
            <StackItemShim align="center">
                <div ref={searchBoxRef} style={{ position: "relative" }}>
                    <SearchBox
                        type="text"
                        value={searchValue}
                        onChange={onSearchChange}
                        onKeyDown={onSearchKeyDown}
                        onFocus={onSearchFocus}
                        contentAfter={<NewTabButton onClick={onSearchNewTabClick} />}
                        placeholder={nlsHPCC.PlaceholderFindText}
                        style={{ minWidth: 320 }}
                        aria-autocomplete="list"
                        aria-controls={showSuggestions ? "search-suggestions-listbox" : undefined}
                        aria-expanded={showSuggestions}
                        aria-activedescendant={highlightedIndex >= 0 ? `search-suggestion-${highlightedIndex}` : undefined}
                    />
                    {showSuggestions && (filteredSuggestions.length > 0 || isSearching) && (
                        <div
                            ref={suggestionsBoxRef}
                            id="search-suggestions-listbox"
                            role="listbox"
                            aria-label="Search suggestions"
                            style={{
                                position: "absolute",
                                top: "100%",
                                left: 0,
                                right: 0,
                                backgroundColor: theme.palette.white,
                                border: `1px solid ${theme.palette.neutralLight}`,
                                borderRadius: 4,
                                boxShadow: theme.effects.elevation8,
                                maxHeight: 400,
                                overflowY: "auto",
                                zIndex: 1000,
                                marginTop: 2
                            }}>
                            {isSearching ? (
                                <div style={{
                                    padding: "12px",
                                    textAlign: "center",
                                    color: theme.palette.neutralSecondary,
                                    fontSize: 14
                                }}>
                                    {nlsHPCC.Loading}
                                </div>
                            ) : (
                                (() => {
                                    const grouped = groupSuggestions(filteredSuggestions);
                                    let globalIndex = 0;
                                    return grouped.map(group => (
                                        <div key={group.type}>
                                            {group.suggestions.length > 0 && (
                                                <>
                                                    <div style={{
                                                        padding: "6px 12px",
                                                        fontSize: 11,
                                                        fontWeight: 600,
                                                        color: theme.palette.neutralSecondary,
                                                        textTransform: "uppercase",
                                                        borderBottom: `1px solid ${theme.palette.neutralLighter}`,
                                                        backgroundColor: theme.palette.neutralLighterAlt
                                                    }}>
                                                        {group.label}
                                                    </div>
                                                    {group.suggestions.map((suggestion) => {
                                                        const currentIndex = globalIndex++;
                                                        return (
                                                            <div
                                                                key={suggestion.key}
                                                                id={`search-suggestion-${currentIndex}`}
                                                                data-index={currentIndex}
                                                                role="option"
                                                                aria-selected={highlightedIndex === currentIndex}
                                                                onClick={() => onSuggestionClick(suggestion)}
                                                                style={{
                                                                    padding: "8px 12px 8px 24px",
                                                                    cursor: "pointer",
                                                                    borderBottom: `1px solid ${theme.palette.neutralLighter}`,
                                                                    fontSize: 14,
                                                                    backgroundColor: highlightedIndex === currentIndex ? theme.palette.neutralLighter : "transparent"
                                                                }}
                                                                onMouseEnter={onSuggestionMouseEnter}
                                                                onMouseLeave={onSuggestionMouseLeave}
                                                            >
                                                                {formatSuggestionText(suggestion)}
                                                            </div>
                                                        );
                                                    })}
                                                </>
                                            )}
                                        </div>
                                    ));
                                })()
                            )}
                        </div>
                    )}
                    <div style={{
                        marginTop: 4,
                        fontSize: 12,
                        color: theme.palette.neutralSecondary,
                        textAlign: "center",
                        minHeight: 18
                    }}>
                        {searchStatusText}
                    </div>
                </div>
            </StackItemShim>
            <StackItemShim align="center" >
                <StackShim horizontal>
                    {currentUser?.username &&
                        <StackItemShim styles={personaStyles}>
                            <Persona {...personaProps} onClick={() => setShowMyAccount(true)} />
                        </StackItemShim>
                    }
                    <StackItemShim align="center">
                        <DefaultButton href="#/log" title={nlsHPCC.ErrorWarnings} iconProps={{ iconName: logCount > 0 ? "RingerSolid" : "Ringer" }} className={btnStyles.errorsWarnings}>
                            <CounterBadge appearance="filled" size="small" color={logIconColor} count={logCount} />
                        </DefaultButton>
                    </StackItemShim>
                    <StackItemShim align="center">
                        <IconButton title={nlsHPCC.Advanced} iconProps={collapseMenuIcon} menuProps={advMenuProps} style={{ color: titlebarColorSet ? Utility.textColor(titlebarColor) : theme.palette.themeDarker }} />
                    </StackItemShim>
                </StackShim>
                <Toaster toasterId={toasterId} position={"top-end"} pauseOnHover />
            </StackItemShim>
        </StackShim>
        <About eclwatchVersion="9" show={showAbout} onClose={() => setShowAbout(false)} ></About>
        <MyAccount currentUser={currentUser} show={showMyAccount} onClose={() => setShowMyAccount(false)}></MyAccount>
        <TitlebarConfig showForm={showTitlebarConfig} setShowForm={setShowTitlebarConfig} />
        <BannerConfig />
        <PasswordExpiredConfirm />
    </div>;
};

