/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

#ifndef _AccessMapGenerator_HPP_
#define _AccessMapGenerator_HPP_

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdarg.h>
#include <algorithm>
#include <functional>
#include <initializer_list>
#include <list>
#include <map>
#include <string>
#include <vector>

#ifndef _WIN32
#define stricmp strcasecmp
#define strnicmp strncasecmp
#endif

template <typename TLevel>
class TAccessMapGenerator
{
    inline static bool isEmptyString(const char *text) { return !text||!*text; }
    inline static bool streq(const std::string& s,const std::string& t) { return streq(s.c_str(),t.c_str()); }
    inline static bool streq(const std::string& s,const char* t) { return streq(s.c_str(),t); }
    inline static bool streq(const char* s,const std::string& t) { return strcmp(s,t.c_str())==0; }
    inline static bool streq(const char* s,const char* t) { return strcmp(s,t)==0; }
    inline static bool strieq(const std::string& s,const std::string& t) { return stricmp(s.c_str(),t.c_str())==0; }
    inline static bool strieq(const std::string& s,const char* t) { return stricmp(s.c_str(),t)==0; }
    inline static bool strieq(const char* s,const std::string& t) { return stricmp(s,t.c_str())==0; }
    inline static bool strieq(const char* s,const char* t) { return stricmp(s,t)==0; }
    inline static bool strnieq(const std::string& s,const std::string& t, size_t n) { return strnicmp(s.c_str(),t.c_str(),n)==0; }
    inline static bool strnieq(const std::string& s,const char* t, size_t n) { return strnicmp(s.c_str(),t,n)==0; }
    inline static bool strnieq(const char* s,const std::string& t, size_t n) { return strnicmp(s,t.c_str(),n)==0; }
    inline static bool strnieq(const char* s,const char* t, size_t n) { return strnicmp(s,t,n)==0; }
public:
    struct ScopeMapper
    {
        static const int Omitted = -1;
        static const int Default = -2;
        static const int Invalid = -3;

        ScopeMapper(const std::initializer_list<std::string>& prioritizedScopes)
            : m_scopes(prioritizedScopes.begin(), prioritizedScopes.end())
        {
        }

        inline int operator () (const std::string& label) const
        {
            return (*this)(label.c_str());
        }
        int operator () (const char* label) const
        {
            if (isEmptyString(label) || streq("*", label))
                return Omitted;
            if (strieq("default", label))
                return Default;

            int priority = 0;
            for (const std::string& scope : m_scopes)
            {
                if (strieq(scope, label))
                    return priority;
                priority++;
            }
            return Invalid;
        }

        const char* operator () (int index) const
        {
            if (0 <= index && index < int(m_scopes.size()))
                return m_scopes[index].c_str();
            if (Omitted == index)
                return "*";
            if (Default == index)
                return "default";
            return "invalid";
        }

    private:
        using Scopes = std::vector<std::string>;
        Scopes m_scopes;
    };

    struct LevelMapper
    {
        inline const char* markupUnavailable() const { return ""; }
        inline const char* markupNone() const { return "NONE"; }
        inline const char* markupDeferred() const { return "DEFERRED"; }
        inline const char* markupAccess() const { return "ACCESS"; }
        inline const char* markupRead() const { return "READ"; }
        inline const char* markupWrite() const { return "WRITE"; }
        inline const char* markupFull() const { return "FULL"; }

        virtual TLevel levelUnavailable() const = 0;
        virtual TLevel levelNone() const = 0;
        virtual TLevel levelDeferred() const = 0;
        virtual TLevel levelAccess() const = 0;
        virtual TLevel levelRead() const = 0;
        virtual TLevel levelWrite() const = 0;
        virtual TLevel levelFull() const = 0;
        virtual TLevel levelUnknown() const = 0;
        virtual bool isEqual(TLevel lhs, TLevel rhs) const = 0;
        virtual const char* toString(TLevel level) const = 0;

        inline bool isMarkupUnavailable(const std::string& level) const { return level.empty(); }
        inline bool isMarkupNone(const std::string& level) const { return strieq(markupNone(), level); }
        inline bool isMarkupDeferred(const std::string& level) const { return strieq(markupDeferred(), level); }
        inline bool isMarkupAccess(const std::string& level) const { return strieq(markupAccess(), level); }
        inline bool isMarkupRead(const std::string& level) const { return strieq(markupRead(), level); }
        inline bool isMarkupWrite(const std::string& level) const { return strieq(markupWrite(), level); }
        inline bool isMarkupFull(const std::string& level) const { return strieq(markupFull(), level); }
        inline bool isMarkupUnknown(const std::string& level) const { return isUnknown(fromMarkup(level)); }
        inline bool isMarkupKnown(const std::string& level) const { return !isMarkupUnknown(level); }

        inline bool isUnavailable(TLevel level) const { return isEqual(level, levelUnavailable()); }
        inline bool isNone(TLevel level) const { return isEqual(level, levelNone()); }
        inline bool isDeferred(TLevel level) const { return isEqual(level, levelDeferred()); }
        inline bool isAccess(TLevel level) const { return isEqual(level, levelAccess()); }
        inline bool isRead(TLevel level) const { return isEqual(level, levelRead()); }
        inline bool isWrite(TLevel level) const { return isEqual(level, levelWrite()); }
        inline bool isFull(TLevel level) const { return isEqual(level, levelFull()); }
        inline bool isUnknown(TLevel level) const { return isEqual(level, levelUnknown()); }
        inline bool isKnown(TLevel level) const { return !isUnknown(level); }

        bool isHint(const char* markup) const
        {
            return isNone(markup) || isDeferred(markup);
        }

        virtual TLevel fromMarkup(const std::string& markup) const
        {
            if (isMarkupUnavailable(markup))
                return levelUnavailable();
            if (isMarkupNone(markup))
                return levelNone();
            if (isMarkupDeferred(markup))
                return levelDeferred();
            if (isMarkupAccess(markup))
                return levelAccess();
            if (isMarkupRead(markup))
                return levelRead();
            if (isMarkupWrite(markup))
                return levelWrite();
            if (isMarkupFull(markup))
                return levelFull();
            return levelUnknown();
        }

        virtual const char* toMarkup(TLevel level) const
        {
            if (isUnavailable(level))
                return "";
            if (isNone(level))
                return "NONE";
            if (isAccess(level))
                return "ACCESS";
            if (isRead(level))
                return "READ";
            if (isWrite(level))
                return "WRITE";
            if (isFull(level))
                return "FULL";
            return "UNKNOWN";
        }
    };

    struct Reporter
    {
        virtual bool errorsAreFatal() const { return false; }
        virtual bool reportError() const { return true; }
        virtual bool reportWarning() const { return true; }
        virtual bool reportInfo() const { return true; }
        virtual bool reportDebug() const { return true; }

        virtual void error(const char* fmt, va_list& args) const
        {
            if (reportError() || errorsAreFatal())
                reportError(fmt, args);
        }
        virtual void warning(const char* fmt, va_list& args) const
        {
            if (reportWarning())
                reportWarning(fmt, args);
        }
        virtual void info(const char* fmt, va_list& args) const
        {
            if (reportInfo())
                reportInfo(fmt, args);
        }
        virtual void debug(const char* fmt, va_list& args) const
        {
            if (reportDebug())
                reportDebug(fmt, args);
        }

        virtual void preEntry(size_t termCount) const {}
        virtual void entry(const char* name, TLevel level) const = 0;
        virtual void postEntry() const {}

    protected:
        friend class TAccessMapGenerator<TLevel>;
        virtual void reportError(const char* fmt, va_list& args) const
        {
        }
        virtual void reportWarning(const char* fmt, va_list& args) const
        {
        }
        virtual void reportInfo(const char* fmt, va_list& args) const
        {
        }
        virtual void reportDebug(const char* fmt, va_list& args) const
        {
        }
    };

protected:
    struct StringICompare
    {
        bool operator () (const std::string& lhs, const std::string& rhs) const
        {
            return (stricmp(lhs.c_str(), rhs.c_str()) < 0);
        }
    };

    using Scopes = std::map<int, std::string>;
    using Variables = std::map<std::string, std::string, StringICompare>;
    using Tokens = std::list<std::string>;

    struct Term
    {
        int sourceScope = ScopeMapper::Invalid;  // defining scope
        int affectsScope = ScopeMapper::Omitted; // scope affected by the term; used only for exclusions
        std::string feature;                     // feature name
        TLevel level = TLevel();                 // access level; ignored by exclusions
        bool exclusion = false;                  // true --> this invalidates lower priority entries
        bool suppression = false;                // true --> this invalidates lower priority entries and affirms the absence of security
        bool deferral = false;                   // true --> this suggests that security should be applied within method handlers
        bool all = false;                        // true --> suppression or deferral applies to all

        bool featureMatches(const char* name) const
        {
            if (nullptr == name)
                return false;
            bool result = false;
            if (exclusion)
            {
                if (feature.empty() || streq(feature, "*"))
                    result = true;
                else if (strieq(feature, name))
                    result = true;
                // What if name is "" or "*"?
            }
            else
            {
                result = strieq(feature, name);
            }
            return result;
        }
        inline bool featureMatches(const std::string& name) const
        {
            return featureMatches(name.c_str());
        }
    };
    using Terms = std::list<Term>;
    using TermPtrs = std::list<Term*>;

public:
    TAccessMapGenerator(const ScopeMapper& scopeMapper, const LevelMapper& levelMapper, const Reporter& reporter)
        : m_scopeMapper(scopeMapper)
        , m_levelMapper(levelMapper)
        , m_reporter(reporter)
    {
    }

    bool setRequireSecurityAffirmation(bool require)
    {
        bool wasRequired = m_requireSecurityAffirmation;
        m_requireSecurityAffirmation = require;
        return wasRequired;
    }

    bool setDefaultSecurity(const char* token)
    {
        bool changed = false;

        if (isEmptyString(token))
        {
            if (m_useDefaultSecurity)
            {
                m_useDefaultSecurity = false;
                changed = true;
            }
        }
        else
        {
            Tokens tokens;
            tokenize(ScopeMapper::Default, token, tokens);
            Terms terms;
            TermPtrs filtered;
            for (std::string& t : tokens)
            {
                Term tmp;
                if (parse(ScopeMapper::Default, t, tmp))
                    terms.push_back(tmp);
            }
            if (filter(terms, filtered))
            {
                m_defaultSecurityTerms = terms;
                m_useDefaultSecurity = true;
                changed = true;
            }
        }
        return changed;
    }

    bool setVariable(const char* name, const char* value)
    {
        bool result = true;
        if (isEmptyString(name))
        {
            error("invalid auth_feature variable name (empty)");
            result = false;
        }
        else
        {
            auto it = m_variables.find(name);
            if (it != m_variables.end())
            {
                if (nullptr == value)
                    m_variables.erase(it);
                else
                    normalizeAndSetValue(it->second, value);
            }
            else
            {
                if (value != nullptr)
                    normalizeAndSetValue(m_variables[name], value);
            }
        }
        return result;
    }

    const char* getVariable(const char* name) const
    {
        if (!isEmptyString(name))
        {
            auto it = m_variables.find(name);
            if (it != m_variables.end())
                return it->second.c_str();
        }
        return nullptr;
    }

    bool insertScope(const char* scope, const char* fragment)
    {
        bool result = false;
        int mappedScope = m_scopeMapper(scope);
        if (mappedScope >= 0)
        {
            auto it = m_scopes.find(mappedScope);
            if (m_scopes.end() == it)
            {
                std::string& value = m_scopes[mappedScope];
                if (fragment != nullptr)
                    value = fragment;
                result = true;
            }
            else
            {
                error("invalid reuse of auth_feature scope '%s' (%d)", scope, mappedScope);
            }
        }
        else
        {
            error("invalid auth_feature scope '%s'", scope);
        }
        return result;
    }

    bool generateMap()
    {
        size_t rawTermCount = 0;
        const char* service = getVariable("service");
        const char* method = getVariable("method");

        info("Preparing access map for %s/%s", service, method);
        Terms allTerms;
        for (auto& scopeItem : m_scopes)
        {
            if (scopeItem.second.empty())
                continue;

            Tokens tokens;
            tokenize(scopeItem.first, scopeItem.second, tokens);
            Term term;
            for (std::string& token : tokens)
            {
                if (parse(scopeItem.first, token, term))
                    allTerms.push_front(term);
            }
        }

        TermPtrs filteredTerms;
        bool affirmed = filter(allTerms, filteredTerms);
        if (!affirmed)
        {
            if (m_useDefaultSecurity)
            {
                affirmed = filter(m_defaultSecurityTerms, filteredTerms);
                if (affirmed)
                    warning("%s/%s is using the default security specification", service, method);
            }
            if (!affirmed && m_requireSecurityAffirmation)
                error("%s/%s is missing an affirmative security specification", service, method);
        }

        preEntry(filteredTerms.size());
        for (Term* tp : filteredTerms)
        {
            info("Applying %s:%s", tp->feature.c_str(), m_levelMapper.toString(tp->level));
            entry(tp->feature.c_str(), tp->level);
        }
        postEntry();

        return true;
    }

protected:
    const ScopeMapper& m_scopeMapper;
    const LevelMapper& m_levelMapper;
    const Reporter& m_reporter;
    Variables m_variables;
    Scopes m_scopes;
    bool m_requireSecurityAffirmation = true;
    bool m_useDefaultSecurity = false;
    Terms m_defaultSecurityTerms;

private:
    void normalizeAndSetValue(std::string& str, const char* value, size_t start = 0, size_t end = std::string::npos) const
    {
        str.clear();
        for (size_t idx = start; idx < end && value[idx] != '\0'; idx++)
        {
            const char& ch = value[idx];
            if (!isspace(ch) && ch != '"')
                str.push_back(ch);
        }
    }

    void tokenize(int srcScope, const std::string& fragment, std::list<std::string>& tokens)
    {
        debug("tokenizing %s fragment \"%s\"", m_scopeMapper(srcScope), fragment.c_str());
        static const std::string ignoredCharacters(" \t\n\r\"");
        std::string token;
        bool done = false;
        bool inVariable = false;
        std::string variable;
        size_t idx = 0;

        while (!done)
        {
            const char& ch = fragment[idx++];
            switch (ch)
            {
            // token delimiters
            case '\0':
                done = true;
                // fall through
            case ',':
                if (inVariable)
                {
                    warning("%s fragment '%s' contains invalid variable markup (incomplete variable reference '%s')", m_scopeMapper(srcScope), fragment.c_str(), variable.c_str());
                    token.append("${").append(variable);
                    variable.clear();
                    inVariable = false;
                }
                if (!token.empty())
                {
                    tokens.push_back(token);
                    token.clear();
                }
                break;

            // variable replacement
            case '$':
                while (isspace(fragment[idx])) idx++;
                if ('{' == fragment[idx])
                {
                    idx++;
                    if (!inVariable)
                    {
                        inVariable = true;
                    }
                    else
                    {
                        error("%s fragment '%s' contains invalid variable markup (nested variable reference)", m_scopeMapper(srcScope), fragment.c_str());
                        token.clear();
                        variable.clear();
                        inVariable = false;
                        while (fragment[idx] != '\0' && fragment[idx] != ',') idx++;
                        done = '\0' == fragment[idx];
                        continue;
                    }
                }
                else
                {
                    token.push_back(ch);
                }
                break;
            case '{':
                while (isspace(fragment[idx])) idx++;
                if ('$' == fragment[idx])
                {
                    idx++;
                    if (!inVariable)
                    {
                        inVariable = true;
                    }
                    else
                    {
                        error("%s fragment '%s' contains invalid variable markup (nested variable reference)", m_scopeMapper(srcScope), fragment.c_str());
                        token.clear();
                        variable.clear();
                        inVariable = false;
                        while (fragment[idx] != '\0' && fragment[idx] != ',') idx++;
                        done = '\0' == fragment[idx];
                        continue;
                    }
                }
                else
                {
                    token.push_back(ch);
                }
                break;
            case '}':
                if (inVariable)
                {
                    auto vit = m_variables.find(variable);
                    if (vit != m_variables.end())
                    {
                        token.append(vit->second);
                    }
                    else
                    {
                        warning("%s fragment '%s' contains undefined variable reference '%s'", m_scopeMapper(srcScope), fragment.c_str(), variable.c_str());
                        token.append("${").append(variable).push_back('}');
                    }
                    inVariable = false;
                    variable.clear();
                }
                else
                {
                    token.push_back(ch);
                }
                break;

            // ignored characters
            case ' ':
            case '\t':
            case '\n':
            case '\r':
            case '"':
                break;

            // retained characters
            default:
                if (inVariable)
                    variable.push_back(ch);
                else
                    token.push_back(ch);
                break;
            }
        }
    }
    bool readWord(const std::string& token, size_t& idx, std::string& word) const
    {
        if (idx >= token.length())
            return false;
        size_t hit = token.find_first_of("!:", idx);
        if (hit == idx)
            return false;
        normalizeAndSetValue(word, token.c_str(), idx, hit);
        idx = (std::string::npos == hit ? token.length() : hit);
        return true;
    }
    bool skipWord(const std::string& token, size_t& idx, const char* word) const
    {
        if (idx >= token.length())
            return false;
        if (isEmptyString(word))
            return true;
        size_t wordLen = strlen(word);
        if (!strnieq(&token[idx], word, wordLen))
            return false;
        idx += wordLen;
        return true;
    }

    inline void exclusionError(int srcScope, const std::string& token, const char* reason) const
    {
        error("%s token '%s' contains invalid exclusion markup (%s)", m_scopeMapper(srcScope), token.c_str(), reason);
    }

    inline void requirementError(int srcScope, const std::string& token, const char* reason) const
    {
        error("%s token '%s' contains invalid requirement markup (%s)", m_scopeMapper(srcScope), token.c_str(), reason);
    }

    bool parse(int srcScope, const std::string& token, Term& term)
    {
        if (isEmptyString(token.c_str()))
            return false;

        bool result = false;
        const size_t length = token.length();
        size_t idx = 0;

        if ('!' == token.at(idx))
        {
            debug("parsing %s exclusion token '%s'", m_scopeMapper(srcScope), token.c_str());
            idx++;

            std::string scopeLabel;
            bool expectScope = readWord(token, idx, scopeLabel) && !scopeLabel.empty();
            int scopeIndex = (expectScope ? m_scopeMapper(scopeLabel) : ScopeMapper::Omitted);
            bool goodScope = false;

            if (scopeIndex != ScopeMapper::Invalid)
                goodScope = true;
            else
                exclusionError(srcScope, token, "invalid scope name");

            bool expectFeature = skipWord(token, idx, "::");
            std::string feature;
            bool haveFeature = expectFeature && readWord(token, idx, feature) && !feature.empty();
            bool goodFeature = false;

            if (!expectFeature)
                goodFeature = true;
            else if (!haveFeature)
                exclusionError(srcScope, token, "missing feature name");
            else if (feature == "*")
                goodFeature = true;
            else if (m_scopeMapper(feature) != ScopeMapper::Invalid)
                exclusionError(srcScope, token, "feature name cannot be scope name");
            else if (m_levelMapper.isMarkupKnown(feature))
                exclusionError(srcScope, token, "feature name cannot be access level");
            else
                goodFeature = true;

            bool goodExclusion = false;

            if (idx != length)
                exclusionError(srcScope, token, "unexpected exclusion content");
            else
                goodExclusion = goodScope && goodFeature;

            if (goodExclusion)
            {
                term.sourceScope = srcScope;
                term.exclusion = true;
                term.suppression = false;
                term.deferral = false;
                term.level = TLevel();
                term.affectsScope = scopeIndex;
                term.feature = feature;
                result = true;
            }
        }
        else
        {
            bool expectFeature = true;
            std::string feature;
            bool haveFeature = readWord(token, idx, feature) && !feature.empty();

            if (m_levelMapper.isMarkupNone(feature))
            {
                debug("parsing %s suppression token '%s'", m_scopeMapper(srcScope), token.c_str());
                bool goodSuppression = false;

                if (idx != length)
                    requirementError(srcScope, token, "unexpected suppression content");
                else
                    goodSuppression = true;

                if (goodSuppression)
                {
                    term.sourceScope = srcScope;
                    term.exclusion = false;
                    term.suppression = true;
                    term.all = true;
                    term.deferral = false;
                    term.level = TLevel();
                    term.affectsScope = ScopeMapper::Omitted;
                    term.feature = token;
                    result = true;
                }
                return result;
            }
            else if (m_levelMapper.isMarkupDeferred(feature))
            {
                debug("parsing %s deferral token '%s'", m_scopeMapper(srcScope), token.c_str());
                bool goodDeferral = false;

                if (idx != length)
                    requirementError(srcScope, token, "unexpected deferral content");
                else
                    goodDeferral = true;

                if (goodDeferral)
                {
                    term.sourceScope = srcScope;
                    term.exclusion = false;
                    term.suppression = false;
                    term.deferral = true;
                    term.all = true;
                    term.level = TLevel();
                    term.affectsScope = ScopeMapper::Omitted;
                    term.feature = token;
                    result = true;
                }
                return result;
            }

            debug("parsing %s requirement token '%s'", m_scopeMapper(srcScope), token.c_str());
            bool expectLevel = skipWord(token, idx, ":");

            if (expectLevel && !haveFeature)
            {
                auto it = m_variables.find("service");
                if (it != m_variables.end())
                {
                    feature.append(it->second).append("Access");
                    haveFeature = true;
                }
                else
                    requirementError(srcScope, token, "missing service variable for default feature name");
            }

            bool goodFeature = false;

            if (!expectFeature)
                goodFeature = true;
            else if (!haveFeature)
                requirementError(srcScope, token, "missing feature name");
            else if (feature == "*")
                requirementError(srcScope, token, "invalid wild-card notation");
            else if (m_scopeMapper(feature) != ScopeMapper::Invalid)
                requirementError(srcScope, token, "feature name cannot be scope name");
            else if (m_levelMapper.isMarkupKnown(feature))
                requirementError(srcScope, token, "feature name cannot be access level");
            else
                goodFeature = true;

            std::string level;
            bool haveLevel = expectLevel && readWord(token, idx, level) && !level.empty();
            TLevel mappedLevel = m_levelMapper.fromMarkup(level);
            bool goodLevel = false;

            if (!expectLevel)
            {
                level = "full";
                mappedLevel = m_levelMapper.fromMarkup(level);
                goodLevel = true;
            }
            else if (!haveLevel)
                requirementError(srcScope, token, "missing access level");
            else if (m_levelMapper.isUnknown(mappedLevel))
                requirementError(srcScope, token, "invalid access level");
            else
                goodLevel = true;

            bool goodRequirement = false;

            if (idx != length)
                requirementError(srcScope, token, "unexpected requirement content");
            else
                goodRequirement = goodFeature && goodLevel;

            if (goodRequirement)
            {
                term.sourceScope = srcScope;
                term.exclusion = false;
                term.suppression = m_levelMapper.isMarkupNone(level);
                term.deferral = m_levelMapper.isMarkupDeferred(level);
                term.level = mappedLevel;
                term.affectsScope = ScopeMapper::Omitted;
                term.feature = feature;
                result = true;
            }
        }

        return result;
    }

    std::string& output(const Term& term, std::string& buf) const
    {
        buf.append(m_scopeMapper(term.sourceScope));
        if (term.exclusion)
        {
            buf.append(" exclusion !")
                .append(m_scopeMapper(term.affectsScope));
            if (!term.feature.empty())
                buf.append("::").append(term.feature);
        }
        else if (term.suppression)
        {
            buf.append(" suppression ")
                .append(term.feature);
            if (!m_levelMapper.isMarkupNone(term.feature))
                buf.append(":").append(m_levelMapper.markupNone());
        }
        else if (term.deferral)
        {
            buf.append(" deferral ")
                .append(term.feature);
            if (!m_levelMapper.isMarkupNone(term.feature))
                buf.append(":").append(m_levelMapper.markupDeferred());
        }
        else
        {
            buf.append(" requirement ")
                .append(term.feature)
                .append(":")
                .append(m_levelMapper.toMarkup(term.level));
        }
        return buf;
    }

    using FoundIterator = typename TermPtrs::const_iterator;

    static FoundIterator findExclusion(const Term& term, const TermPtrs& terms)
    {
        FoundIterator it;
        for (it = terms.begin(); it != terms.end(); ++it)
        {
            if (!(*it)->exclusion)
            {
                // not an exclusion
            }
            else if ((*it)->affectsScope != ScopeMapper::Omitted && term.sourceScope != (*it)->affectsScope)
            {
                // exclusion affects a scope other than the scope that defined term
            }
            else if ((*it)->featureMatches(term.feature))
            {
                break;
            }
        }
        return it;
    }
    static FoundIterator findSuppression(const Term& term, const TermPtrs& terms)
    {
        FoundIterator it;
        for (it = terms.begin(); it != terms.end(); ++it)
        {
            if (!(*it)->suppression)
            {
                // not a suppression
            }
            else if ((*it)->all)
            {
                break;
            }
            else if (strieq(term.feature, (*it)->feature))
            {
                break;
            }
        }
        return it;
    }
    static FoundIterator findAccepted(const Term& term, const TermPtrs& terms)
    {
        FoundIterator it;
        for (it = terms.begin(); it != terms.end(); ++it)
        {
            if (strieq(term.feature, (*it)->feature))
            {
                break;
            }
        }
        return it;
    }
    using FindPredicate = std::function<FoundIterator(const Term&, const TermPtrs& terms)>;

    bool find(const Term& term, const TermPtrs& terms, FindPredicate pred) const
    {
        FoundIterator it = pred(term, terms);
        if (it != terms.end())
        {
            std::string termDesc;
            std::string matchDesc;
            output(term, termDesc);
            output(**it, matchDesc);
            debug("%s superseded by %s", termDesc.c_str(), matchDesc.c_str());
            return true;
        }
        return false;
    }
    bool filter(Terms& inputs, TermPtrs& outputs)
    {
        TermPtrs exclusions;
        TermPtrs suppressions;
        bool affirmed = false;
        bool warned = false;

        for (Term& t : inputs)
        {
            if (find(t, exclusions, findExclusion))
                continue;
            if (t.exclusion)
            {
                exclusions.push_front(&t);
                continue;
            }

            if (find(t, suppressions, findSuppression))
                continue;
            if (t.suppression)
            {
                suppressions.push_front(&t);
                if (!affirmed && t.all)
                {
                    if (outputs.empty() && !warned)
                    {
                        const char* method = getVariable("method");
                        if (nullptr == method || !strieq(method, "Ping"))
                            warning("Feature security is suppressed; ensure this is correct behavior");
                        warned = true;
                    }
                    affirmed = true;
                }
                continue;
            }

            if (t.deferral && t.all)
            {
                warning("Complete feature security is deferred; ensure this is correct behavior");
                warned = true;
                if (!affirmed)
                    affirmed = true;
                continue;
            }

            if (find(t, outputs, findAccepted))
                continue;

            affirmed = true;
            outputs.push_front(&t);
        }

        return affirmed;
    }

    void error(const char* fmt, ...) const
    {
        if (m_reporter.reportError() || m_reporter.errorsAreFatal())
        {
            va_list args;
            va_start(args, fmt);
            m_reporter.reportError(fmt, args);
            va_end(args);
        }
    }
    void warning(const char* fmt, ...) const
    {
        if (m_reporter.reportWarning())
        {
            va_list args;
            va_start(args, fmt);
            m_reporter.reportWarning(fmt, args);
            va_end(args);
        }
    }
    void info(const char* fmt, ...) const
    {
        if (m_reporter.reportInfo())
        {
            va_list args;
            va_start(args, fmt);
            m_reporter.reportInfo(fmt, args);
            va_end(args);
        }
    }
    void debug(const char* fmt, ...) const
    {
        if (m_reporter.reportDebug())
        {
            va_list args;
            va_start(args, fmt);
            m_reporter.reportDebug(fmt, args);
            va_end(args);
        }
    }
    void preEntry(size_t termCount) const
    {
        m_reporter.preEntry(termCount);
    }
    void entry(const char* name, TLevel level) const
    {
        m_reporter.entry(name, level);
    }
    void postEntry() const
    {
        m_reporter.postEntry();
    }
};

#endif // _AccessMapGenerator_HPP_
