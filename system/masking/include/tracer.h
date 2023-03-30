/*##############################################################################

    Copyright (C) 2022 HPCC Systems®.

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

#pragma once

#include "jscm.hpp"
#include "jlog.hpp"

/**
 * @brief Abstraction to replace jlog trace output convenience functions.
 *
 * While jlog defines useful functions for producing trace output, they lack context. Individual
 * requests, for example, can manage neither the level of messages produced nor the destination of
 * those messages.
 *
 * This interface declares alternate methods, similar to jlog functions, that enable implementations
 * to manage context. It is intended to be used by code that creates trace output without regard for
 * contextual requirements. Included functionality includes:
 *
 * - Message creation based on various combinations of audience and class.
 * - Determination of whether or not an audience and class combination would produce a message.
 *
 * As long as a detail level is included in a message category, levels cannot be ignored. The
 * interface does deemphasize them based on an expectation that they wull be removed in the future.
 */
interface ITracer : extends IInterface
{
    virtual void dbglog(const char* format, ...) const __attribute__((format(printf, 2, 3))) = 0;
    virtual void dislog(const char* format, ...) const __attribute__((format(printf, 2, 3))) = 0;
    virtual void uerrlog(const char* format, ...) const __attribute__((format(printf, 2, 3))) = 0;
    virtual void oerrlog(const char* format, ...) const __attribute__((format(printf, 2, 3))) = 0;
    virtual void ierrlog(const char* format, ...) const __attribute__((format(printf, 2, 3))) = 0;
    virtual void aerrlog(const char* format, ...) const __attribute__((format(printf, 2, 3))) = 0;
    virtual void uwarnlog(const char* format, ...) const __attribute__((format(printf, 2, 3))) = 0;
    virtual void owarnlog(const char* format, ...) const __attribute__((format(printf, 2, 3))) = 0;
    virtual void iwarnlog(const char* format, ...) const __attribute__((format(printf, 2, 3))) = 0;
    virtual void proglog(const char* format, ...) const __attribute__((format(printf, 2, 3))) = 0;
    virtual void log(const LogMsgCategory& category, const char* format, ...) const __attribute__((format(printf, 3, 4))) = 0;
    virtual void log(LogMsgAudience _audience, LogMsgClass _class, LogMsgDetail _detail, const char* format, ...) const __attribute__((format(printf, 5, 6))) = 0;

    virtual bool logIsActive(const LogMsgCategory& category) const = 0;
    virtual bool dbglogIsActive() const = 0;
    virtual bool dislogIsActive() const = 0;
    virtual bool uerrlogIsActive() const = 0;
    virtual bool oerrlogIsActive() const = 0;
    virtual bool ierrlogIsActive() const = 0;
    virtual bool aerrlogIsActive() const = 0;
    virtual bool uwarnlogIsActive() const = 0;
    virtual bool owarnlogIsActive() const = 0;
    virtual bool iwarnlogIsActive() const = 0;
    virtual bool proglogIsActive() const = 0;
    virtual bool logIsActive(LogMsgAudience _audience, LogMsgClass _class, LogMsgDetail _detail) const = 0;
};

/**
 * @brief Partial implementation of `ITraceProvider` handling variadic parameters and ensuring
 *        consistent use of jlog categories.
 *
 * This assumes that all message creation methods are similar enough to be funneled into a single
 * method, and that all active checks are similar enough to be handled by one method.
 *
 * Extensions must implement:
 *
 * - `bool logIsActive(const LogMsgCategory&) const`
 * - `void valog(const LogMsgCategory&, const char*, va_list) const`
 */
class CBaseTracer : public CInterfaceOf<ITracer>
{
public:
    virtual void dbglog(const char* format, ...) const override __attribute__((format(printf, 2, 3)));
    virtual void dislog(const char* format, ...) const override __attribute__((format(printf, 2, 3)));
    virtual void uerrlog(const char* format, ...) const override __attribute__((format(printf, 2, 3)));
    virtual void oerrlog(const char* format, ...) const override __attribute__((format(printf, 2, 3)));
    virtual void ierrlog(const char* format, ...) const override __attribute__((format(printf, 2, 3)));
    virtual void aerrlog(const char* format, ...) const override __attribute__((format(printf, 2, 3)));
    virtual void uwarnlog(const char* format, ...) const override __attribute__((format(printf, 2, 3)));
    virtual void owarnlog(const char* format, ...) const override __attribute__((format(printf, 2, 3)));
    virtual void iwarnlog(const char* format, ...) const override __attribute__((format(printf, 2, 3)));
    virtual void proglog(const char* format, ...) const override __attribute__((format(printf, 2, 3)));
    virtual void log(const LogMsgCategory& category, const char* format, ...) const override __attribute__((format(printf, 3, 4)));
    virtual void log(LogMsgAudience _audience, LogMsgClass _class, LogMsgDetail _detail, const char* format, ...) const override __attribute__((format(printf, 5, 6)));
protected:
    virtual void valog(const LogMsgCategory& category, const char* format, va_list arguments) const __attribute__((format(printf, 3, 0))) = 0;

public:
    using ITracer::logIsActive;
    virtual bool dbglogIsActive() const override { return logIsActive(MCdebugInfo); }
    virtual bool dislogIsActive() const override { return logIsActive(MCoperatorDisaster); }
    virtual bool uerrlogIsActive() const override { return logIsActive(MCuserError); }
    virtual bool oerrlogIsActive() const override { return logIsActive(MCoperatorError); }
    virtual bool ierrlogIsActive() const override { return logIsActive(MCdebugError); }
    virtual bool aerrlogIsActive() const override { return logIsActive(MCauditError); }
    virtual bool uwarnlogIsActive() const override { return logIsActive(MCuserWarning); }
    virtual bool owarnlogIsActive() const override { return logIsActive(MCoperatorWarning); }
    virtual bool iwarnlogIsActive() const override { return logIsActive(MCdebugWarning); }
    virtual bool proglogIsActive() const override { return logIsActive(MCuserProgress); }
    virtual bool logIsActive(LogMsgAudience _audience, LogMsgClass _class, LogMsgDetail _detail) const override { return logIsActive(LogMsgCategory(_audience, _class, _detail)); }
};

#define do_valog(cat) \
    va_list arguments; \
    va_start(arguments, format); \
    valog(cat, format, arguments); \
    va_end(arguments)

inline void CBaseTracer::dbglog(const char* format, ...) const { do_valog(MCdebugInfo); }
inline void CBaseTracer::dislog(const char* format, ...) const { do_valog(MCoperatorDisaster); }
inline void CBaseTracer::uerrlog(const char* format, ...) const { do_valog(MCuserError); }
inline void CBaseTracer::oerrlog(const char* format, ...) const { do_valog(MCoperatorError); }
inline void CBaseTracer::ierrlog(const char* format, ...) const { do_valog(MCdebugError); }
inline void CBaseTracer::aerrlog(const char* format, ...) const { do_valog(MCauditError); }
inline void CBaseTracer::uwarnlog(const char* format, ...) const { do_valog(MCuserWarning); }
inline void CBaseTracer::owarnlog(const char* format, ...) const { do_valog(MCoperatorWarning); }
inline void CBaseTracer::iwarnlog(const char* format, ...) const { do_valog(MCdebugWarning); }
inline void CBaseTracer::proglog(const char* format, ...) const { do_valog(MCuserProgress); }
inline void CBaseTracer::log(const LogMsgCategory& category, const char* format, ...) const { do_valog(category); }
inline void CBaseTracer::log(LogMsgAudience _audience, LogMsgClass _class, LogMsgDetail _detail, const char* format, ...) const { do_valog(LogMsgCategory(_audience, _class, _detail)); }

#undef do_valog
