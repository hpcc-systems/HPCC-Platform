#pragma once

#include <climits>

#include "jargv.hpp"

constexpr unsigned queryCopySetMinParallelWindowSize = 1;
constexpr unsigned queryCopySetMaxParallelWindowSize = 1024;

enum class QueryCopySetParallelOptionError
{
    None,
    OnlyCopyFiles,
    StopIfFilesCopied
};

constexpr QueryCopySetParallelOptionError validateQueryCopySetParallelOptions(bool parallel, bool onlyCopyFiles, bool stopIfFilesCopied)
{
    if (!parallel)
        return QueryCopySetParallelOptionError::None;
    if (onlyCopyFiles)
        return QueryCopySetParallelOptionError::OnlyCopyFiles;
    if (stopIfFilesCopied)
        return QueryCopySetParallelOptionError::StopIfFilesCopied;
    return QueryCopySetParallelOptionError::None;
}

constexpr bool queryCopySetParallelControlsAllowed(bool parallel, bool continueOnError, bool queueTimeoutSpecified, bool compileTimeoutSpecified, bool windowSizeSpecified)
{
    return parallel || (!continueOnError && !queueTimeoutSpecified && !compileTimeoutSpecified && !windowSizeSpecified);
}

constexpr bool queryCopySetTimeoutSecondsValid(unsigned seconds)
{
    return seconds <= UINT_MAX / 1000;
}

inline bool queryCopySetParseUnsigned(const char *text, unsigned &value)
{
    if (!text || !*text)
        return false;
    unsigned parsed = 0;
    do
    {
        unsigned digit = *text++ - '0';
        if (digit > 9 || parsed > (UINT_MAX - digit) / 10)
            return false;
        parsed = parsed * 10 + digit;
    } while (*text);
    value = parsed;
    return true;
}

constexpr bool queryCopySetParallelWindowSizeValid(unsigned windowSize)
{
    return windowSize >= queryCopySetMinParallelWindowSize && windowSize <= queryCopySetMaxParallelWindowSize;
}

enum class QueryCopySetParallelOptionMatch
{
    NoMatch,
    Match,
    InvalidQueueTimeout,
    InvalidCompileTimeout,
    InvalidWindowSize
};

class CQueryCopySetParallelOptions
{
public:
    QueryCopySetParallelOptionMatch match(ArgvIterator &iter)
    {
        StringAttr value;
        if (iter.matchOption(value, "--parallel-queue-timeout"))
        {
            queueTimeoutSpecified = true;
            return queryCopySetParseUnsigned(value, queueTimeout) ? QueryCopySetParallelOptionMatch::Match : QueryCopySetParallelOptionMatch::InvalidQueueTimeout;
        }
        if (iter.matchOption(value, "--parallel-compile-timeout"))
        {
            compileTimeoutSpecified = true;
            return queryCopySetParseUnsigned(value, compileTimeout) ? QueryCopySetParallelOptionMatch::Match : QueryCopySetParallelOptionMatch::InvalidCompileTimeout;
        }
        if (iter.matchOption(value, "--parallel-window-size"))
        {
            windowSizeSpecified = true;
            return queryCopySetParseUnsigned(value, windowSize) ? QueryCopySetParallelOptionMatch::Match : QueryCopySetParallelOptionMatch::InvalidWindowSize;
        }
        if (iter.matchFlag(parallel, "--parallel"))
            return QueryCopySetParallelOptionMatch::Match;
        if (iter.matchFlag(continueOnError, "--continue-on-error"))
            return QueryCopySetParallelOptionMatch::Match;
        return QueryCopySetParallelOptionMatch::NoMatch;
    }

    bool controlsAllowed() const
    {
        return queryCopySetParallelControlsAllowed(parallel, continueOnError, queueTimeoutSpecified, compileTimeoutSpecified, windowSizeSpecified);
    }

    template <class TRequest, class TFailurePolicy>
    void updateRequest(TRequest *req, TFailurePolicy continuePolicy, TFailurePolicy failFastPolicy) const
    {
        req->setParallel(parallel);
        req->setParallelFailurePolicy(continueOnError ? continuePolicy : failFastPolicy);
        if (queueTimeoutSpecified)
            req->setParallelQueueTimeout(queueTimeout * 1000);
        if (compileTimeoutSpecified)
            req->setParallelCompileTimeout(compileTimeout * 1000);
        if (windowSizeSpecified)
            req->setParallelWindowSize(windowSize);
    }

    bool parallel = false;
    bool continueOnError = false;
    unsigned queueTimeout = 0;
    unsigned compileTimeout = 0;
    unsigned windowSize = 0;
    bool queueTimeoutSpecified = false;
    bool compileTimeoutSpecified = false;
    bool windowSizeSpecified = false;
};
