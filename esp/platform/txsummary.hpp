/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2016 HPCC Systems®.

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

#ifndef TXSUMMARY_HPP
#define TXSUMMARY_HPP

#include <string>
#include <list>
#include <map>

#include "jiface.hpp"
#include "jstring.hpp"
#include "jmutex.hpp"
#include "cumulativetimer.hpp"
#include "tokenserialization.hpp"
#include "esp.hpp"
#include "esphttp.hpp"

// Using the existing esp_cfg_decl in this package required including
// espcfg.ipp which has additional includes that weren't found by all
// other packagates including txsummary.hpp, triggering changes to
// several other CMakeLists.txt files. This seemed cleaner.
#ifdef TXSUMMARY_EXPORTS
    #define txsummary_decl DECL_EXPORT
#else
    #define txsummary_decl DECL_IMPORT
#endif

interface ITxSummaryProfile;
class CTxOpenTelemetryConnector;

/**
 * @brief Indication of what an instrumented value represents.
 *
 * Open Telemetry attributes reflecting timing values are expected to be reported as a number of
 * nanonseconds. The trace summary records these values as a number of milliseconds. Similarly,
 * size quantities are expected as a number of bytes. The enumeration covers use cases where a
 * value is reported as one unit of measurement and Open Telemetry requires another.
 *
 * Customer defined ESPs, plugins, and ESDL scripts may instrument values of their choice. Naming
 * conventions, or similar strategies, cannot be relied upon for the platform to deduce correct
 * behavior.
 */
enum class TxUnits
{
    NA,     /// no scaling required
    millis, /// milliseconds to be scaled to nanoseconds
};

class txsummary_decl CTxSummary : extends CInterface
{
public:
    // Construct an instance with the given creation time. A non-zero value
    // allows the summary to be in sync with its owning object. A value of
    // zero causes the summary to base its elapsed time calculations on its
    // own construction time.
    CTxSummary(unsigned creationTime = 0);

    // Returns the number of summary entries.
    virtual unsigned __int64 size() const;

    // Purges all summary entries.
    virtual void clear();

    // Returns true if an entry exists for the key.
    virtual bool contains(const char* key) const;

    // Returns the number of milliseconds elapsed since the construction of
    // the summary.
    virtual unsigned getElapsedTime() const;

    // Appends all summary entries to the given buffer.
    //
    // JSON serialization supports string, integer numeric, and object values. Arrays are not
    // supported. Null values are considered equivalent to empty strings. Boolean values are
    // represented as integral 1 and 0 values.
    virtual void serialize(StringBuffer& buffer, const LogLevel logLevel = LogMin, const unsigned int group = TXSUMMARY_GRP_CORE, const unsigned int requestedStyle = TXSUMMARY_OUT_TEXT) const;

    // Adds the unique key and value to the end of the summary.
    // Returns true if the key value pair are added to the summary. Returns
    // false if the key is NULL, empty, or not unique within the summary.
    //
    // The key can be a dot-delimited JSON style path for declaring the
    // entry's position in a hierarcy used when the summary is serialized in a
    // style such as JSON. When output as text, the key is used verbatim as the
    // entry name.
    //
    // Four varations are provided to explicitly control Open Telemetry forwarding behavior. An
    // alternate key may be specified for use as the attribute name. A units identifier may be
    // given to force value scaling.
    template <typename TValue, typename TSuffix = const char*, class TSerializer = TokenSerializer>
        bool append(const char* key, const TValue& value, const LogLevel logLevel = LogMin, const unsigned int group = TXSUMMARY_GRP_CORE, const TSuffix& suffix = "", const TSerializer& serializer = TSerializer());
    template <typename TValue, typename TSuffix = const char*, class TSerializer = TokenSerializer>
        bool append(const char* key, const TValue& value, const char* otKey, LogLevel logLevel = LogMin, const unsigned int group = TXSUMMARY_GRP_CORE, const TSuffix& suffix = "", const TSerializer& serializer = TSerializer());
    template <typename TValue, typename TSuffix = const char*, class TSerializer = TokenSerializer>
        bool append(const char* key, const TValue& value, TxUnits units, LogLevel logLevel = LogMin, const unsigned int group = TXSUMMARY_GRP_CORE, const TSuffix& suffix = "", const TSerializer& serializer = TSerializer());
    template <typename TValue, typename TSuffix = const char*, class TSerializer = TokenSerializer>
        bool append(const char* key, const TValue& value, const char* otKey, TxUnits units, LogLevel logLevel = LogMin, const unsigned int group = TXSUMMARY_GRP_CORE, const TSuffix& suffix = "", const TSerializer& serializer = TSerializer());

    // Updates the value, logLevel, group and suffix associated with an existing
    // key, or appends the keyand value to the summary if it is not already
    // found. Returns false if the key is NULL or empty. Returns true otherwise.
    //
    // Four varations are provided to explicitly control Open Telemetry forwarding behavior. An
    // alternate key may be specified for use as the attribute name. A unitsidentifier may be
    // given to force value scaling.
    template <typename TValue, typename TSuffix = const char*, class TSerializer = TokenSerializer>
        bool set(const char* key, const TValue& value, const LogLevel logLevel = LogMin, const unsigned int group = TXSUMMARY_GRP_CORE, const TSuffix& suffix = "", const TSerializer& serializer = TSerializer());
    template <typename TValue, typename TSuffix = const char*, class TSerializer = TokenSerializer>
        bool set(const char* key, const TValue& value, const char* otKey, const LogLevel logLevel = LogMin, const unsigned int group = TXSUMMARY_GRP_CORE, const TSuffix& suffix = "", const TSerializer& serializer = TSerializer());
    template <typename TValue, typename TSuffix = const char*, class TSerializer = TokenSerializer>
        bool set(const char* key, const TValue& value, TxUnits units, const LogLevel logLevel = LogMin, const unsigned int group = TXSUMMARY_GRP_CORE, const TSuffix& suffix = "", const TSerializer& serializer = TSerializer());
    template <typename TValue, typename TSuffix = const char*, class TSerializer = TokenSerializer>
        bool set(const char* key, const TValue& value, const char* otKey, TxUnits units, const LogLevel logLevel = LogMin, const unsigned int group = TXSUMMARY_GRP_CORE, const TSuffix& suffix = "", const TSerializer& serializer = TSerializer());

    // Similar to the above set functions, but pulls the value from an existing entry
    // named 'sourceKey'
    virtual bool setCopyValueOf(const char* destKey, const char* sourceKey, const LogLevel logLevel = LogMin, const unsigned int group = TXSUMMARY_GRP_CORE);

    void log(const LogLevel logLevel, const unsigned int group = TXSUMMARY_GRP_CORE, const unsigned int requestedStyle = TXSUMMARY_OUT_TEXT);

    // Fetches an existing or new instance of a named CumulativeTime. The name
    // must not be NULL or empty, duplication is not permitted.
    // The supplied level and group must match those set on the found entry or
    // it is not considered a match, and an exception is thrown.
    virtual CumulativeTimer* queryTimer(const char* key, LogLevel level, const unsigned int group);

    // Adds the given milliseconds to an existing or new named CumulativeTimer.
    // The same conditions as for getTimer apply.
    // The supplied level and group must match those set on the found entry or
    // it is not considered a match, and an exception is thrown.
    virtual bool updateTimer(const char* name, unsigned long long delta, const LogLevel logLevel, const unsigned int group);

    // Call back into the ITxSummaryProfile, if set, to customize the contents
    // of the summary prior to serialization.
    virtual bool tailor(IEspContext* ctx);

    // Take an ITxSummaryProfile instance that can rename entries and customize
    // the contents of the summary prior to serialization.
    virtual void setProfile(ITxSummaryProfile* profile);

protected:
    // Log the summary contents on destruction.
    ~CTxSummary();

private:
    class TxEntryBase;
    struct TxEntryStr;

    virtual bool appendSerialized(const char* key, const char* value, const LogLevel logLevel, const unsigned int group, bool jsonQuoted, const char* suffix);
    virtual bool setSerialized(const char* key, const char* value, const LogLevel logLevel, const unsigned int group, bool jsonQuoted, const char* suffix);
    virtual bool setEntry(const char* key, TxEntryBase* value);

    // Returns true if the summary contains an entry matching key
    bool __contains(const char* key) const;

    // Returns the entry matching key if found, or nullptr otherwise
    TxEntryBase* queryEntry(const char* key);

    // Each entry is a subclass of TxEntryBase that allows us
    // to keep type information along with the value to use for
    // serialization. Each Entry has these primary attributes:
    //
    //  name
    //      The name when serialized. It may be in the form
    //      of a simplified dot-delimited JSON-style path.
    //      That form is used to give structure when
    //      serializing to JSON. The implementation tracks
    //      the name in two ways- as 'name' and 'fullname'
    //      to aid with serialization.
    //
    //  logLevel
    //      The minimum logLevel that must be requested during
    //      serialization in order for this element to appear
    //      in the output.
    //
    //  group
    //      A bit flag indicating which groups this element
    //      belongs to. Serialization can specify which groups
    //      are included in output
    //
    //  value
    //      Subclasses of TxEntryBase include a value member of
    //      a type appropriate for the entry.
    //
    class TxEntryBase : public CInterface
    {
        public:
            // The complete name of the entry, may be a dot-delimited
            // path.
            StringAttr fullname;

            // The rightmost path part of fullname. During serialization
            // to JSON an entry's fullname is tokenized and TxEntryObjects
            // are created- one from each token of the path, with its
            // name equal to the path token.
            const char* name;

            // Some Entry subclasses can assign a suffix to serialize
            // after its value
            StringAttr suffix = nullptr;

            bool shouldJsonQuote = false;

            TxEntryBase(const char* _key, const LogLevel _logLevel, const unsigned int _group, const char* _suffix, bool jsonQuoted);
            virtual bool contains(const char* path) {return false;}
            virtual TxEntryBase* queryEntry(const char* path) {return nullptr;}
            virtual TxEntryBase* ensurePath(const char* path) {return nullptr;}
            virtual bool append(TxEntryBase* entry) {return false;}
            virtual bool set(TxEntryBase* entry) {return false;}
            virtual unsigned __int64 size() {return 1;}
            virtual void clear() {};
            virtual bool shouldSerialize(const LogLevel requestedLevel, const unsigned int requestedGroup);
            virtual StringBuffer& serialize(StringBuffer& buf, const LogLevel requestedLevel, const unsigned int requestedGroup, const unsigned int requestedStyle) = 0;
            virtual TxEntryBase* cloneWithNewName(const char* newName = nullptr) = 0;
            virtual void setLogLevel(const LogLevel level) {logLevel = level;}
            virtual LogLevel queryLogLevel() {return logLevel;}
            virtual void setGroup(const unsigned int grp) {group = grp;}
            virtual unsigned int queryGroup() {return group;}

        private:
            // The minimum logLevel at which this entry is serialized
            LogLevel logLevel = LogMax;

            // The bit field indicating with groups this entry belongs to
            unsigned int group = TXSUMMARY_GRP_CORE;
    };

    struct TxEntryStr : public TxEntryBase
    {
        StringAttr value;

        TxEntryStr(const char* _key, const char* _value, const LogLevel _logLevel, const unsigned int _group, bool _jsonQuoted, const char* _suffix)
            : TxEntryBase(_key, _logLevel, _group, _suffix, _jsonQuoted), value(_value) {}

        virtual TxEntryBase* cloneWithNewName(const char* newName = nullptr) override
        {
            return new TxEntryStr(newName ? newName : fullname.get(), value.get(), queryLogLevel(), queryGroup(), shouldJsonQuote, suffix);
        }

        virtual StringBuffer& serialize(StringBuffer& buf, const LogLevel requestedLevel, const unsigned int requestedGroup, const unsigned int requestedStyle) override;
    };

    struct TxEntryTimer : public TxEntryBase
    {
        // The CumulativeTimer object owned by this Entry needs to manage
        // the unsigned int and LogLevel values. This is because external
        // users of the TxSummary class expect to be able to set those values
        // on the CumulativeTimer object. So the logLevel and group values
        // in the TxEntryBase are not used for this subclass.

        Owned<CumulativeTimer> value;

        TxEntryTimer(const char* _key, CumulativeTimer* _value, const LogLevel _logLevel = LogMin, const unsigned int _group = TXSUMMARY_GRP_CORE)
            : TxEntryBase(_key, _logLevel, _group, nullptr, false)
        {
            value.set(_value);
        }

        virtual TxEntryBase* cloneWithNewName(const char* newName = nullptr) override
        {
            return new TxEntryTimer(newName ? newName : fullname.str(), value.get(), value->getLogLevel(), value->getGroup());
        }

        // Uses logLevel and group settings stored in the contained CumulativeTimer object
        virtual bool shouldSerialize(const LogLevel requestedLevel, const unsigned int requestedGroup) override;
        virtual StringBuffer& serialize(StringBuffer& buf, const LogLevel requestedLevel, const unsigned int requestedGroup, const unsigned int requestedStyle) override;
        virtual void setLogLevel(const LogLevel level) override {value->setLogLevel(level);}
        virtual LogLevel queryLogLevel() override {return value->getLogLevel();}
        virtual void setGroup(const unsigned int grp) override {value->setGroup(grp);}
        virtual unsigned int queryGroup() override {return value->getGroup();}
    };

    using EntryValue = Linked<TxEntryBase>;
    using EntriesInOrder = std::list<EntryValue>;

    struct TxEntryObject : public TxEntryBase
    {
        // Container for holding child entries
        // Keeps an ordered list to serialize entries in order and

        TxEntryObject(const char* _key, const LogLevel _logLevel = LogMin, const unsigned int _group = TXSUMMARY_GRP_CORE)
            : TxEntryBase(_key, _logLevel, _group, nullptr, false) {}
        // Our use does not include cloning TxEntryObjects
        virtual TxEntryBase* cloneWithNewName(const char* newName = nullptr) override {return nullptr;}
        virtual bool contains(const char* path) override;
        virtual TxEntryBase* queryEntry(const char* path) override;
        virtual TxEntryBase* ensurePath(const char* path) override;
        virtual bool append(TxEntryBase* entry) override;
        virtual bool set(TxEntryBase* entry) override;
        virtual unsigned __int64 size() override;
        virtual void clear() override;
        virtual StringBuffer& serialize(StringBuffer& buf, const LogLevel requestedLevel, const unsigned int requestedGroup, const unsigned int requestedStyle) override;

        EntriesInOrder m_children;
    };

    mutable CriticalSection m_sync;
    unsigned m_creationTime;
    EntriesInOrder m_entries;
    Linked<ITxSummaryProfile> m_profile;
    Owned<CTxOpenTelemetryConnector> connector;
};

/**
 * @brief Forward trace summary values to currently active Open Telemetry span. 
 *
 * Values are filtered by configured log level and requested group. Values that will be excluded
 * from trace summary logging are not forwarded to the span. Additional filtering could be added
 * to avoid setting redundant attributes.
 *
 * Supported value types are signed integers, unsigned integers, doubles, bools, and strings.
 * Integer values will be scaled, as needed, based on the given units identifier. Doubles are
 * reported as strings to avoid data loss.
 *
 * Where `CTxSummary` serializes all values to text, this acts only on the original, unserialized,
 * values.
 */
class txsummary_decl CTxOpenTelemetryConnector : public CInterface
{
public:
    /**
     * @brief Forward an integral or floating point value to an Open Teleemetry span.
     *
     * Values of non-integral or non-floating point types are ignored. Template specializations
     * are used to support other types.
     *
     * @tparam TValue data type of the value to be forwarded
     * @param txKey required key used for trace summary output
     * @param otKey optional key used only for Open Telemtry output
     * @param value the original value
     * @param units indicator of what scaling, if any, is required
     * @param logLevel minimum requested log level needed to forward the value
     * @param groupMask set of goups that include the value
     */
    template <typename TValue>
    void forwardAttribute(const char* txKey, const char* otKey, TValue value, TxUnits units, LogLevel logLevel, unsigned groupMask) const;

    /**
     * @brief Type-specific overload for string values.
     * 
     * @tparam  
     * @param txKey required key used for trace summary output
     * @param otKey optional key used only for Open Telemtry output
     * @param value the original value
     * @param logLevel minimum requested log level needed to forward the value
     * @param groupMask set of goups that include the value
     */
    void forwardAttribute(const char* txKey, const char* otKey, const char* value, TxUnits, LogLevel logLevel, unsigned groupMask) const;

    /**
     * @brief Type-specific overload for Boolean values.
     * 
     * @tparam  
     * @param txKey required key used for trace summary output
     * @param otKey optional key used only for Open Telemtry output
     * @param value the original value
     * @param logLevel minimum requested log level needed to forward the value
     * @param groupMask set of goups that include the value
     */
    void forwardAttribute(const char* txKey, const char* otKey, bool value, TxUnits, LogLevel logLevel, unsigned groupMask) const;

protected:
    /**
     * @brief Assemble the correct Open Telemtry attribute name from them input.
     *
     * An `otKey` value takes precedence over `tcKey`. All keys are converted to snake case. Other
     * changes may be applied as needed.
     *
     * @param txKey required key used for trace summary output
     * @param otKey optional key used only for Open Telemtry output
     * @param normalized effective key for Open Telemetry output
     * @return StringBuffer& the effective key
     */
    StringBuffer& normalizeKey(const char* txKey, const char* otKey, StringBuffer& normalized) const;

    /**
     * @brief Determine if the value should not be forwarded to Open Telemetry.
     *
     * A value will be excluded when:
     * - `key` is empty; or
     * - `logLevel` is greater than the configured trace summary requested log level; or
     * - `groupMask` does not include the configured trace summary requested group mask.
     *
     * Other conditions, such as redundant keys, may be checked here. Redundancy could refer to
     * a key being forwarded multiple times, but might also refer to keys known to replicate data
     * already in the target span.
     *
     * @param key normalized value key
     * @param logLevel minimum requested log level needed to forward the value
     * @param groupMask set of goups that include the value
     * @return true when the value is not to be forwarded
     * @return false when the value is to be forwarded
     */
    bool isExcluded(const char* key, LogLevel logLevel, unsigned groupMask) const;

    void forwardUnsigned(const char* txKey, const char* otKey, uint64_t value, TxUnits units, LogLevel logLevel, unsigned groupMask) const;
    void forwardSigned(const char* txKey, const char* otKey, int64_t value, TxUnits units, LogLevel logLevel, unsigned groupMask) const;
    void forwardDouble(const char* txKey, const char* otKey, double value, LogLevel logLevel, unsigned groupMask) const;
    void forwardBool(const char* txKey, const char* otKey, bool value, LogLevel logLevel, unsigned groupMask) const;
    void forwardString(const char* txKey, const char* otKey, const char* value, LogLevel logLevel, unsigned groupMask) const;
    
    /**
     * @brief Apply required arithmetic svaling of values before forwarding.
     * 
     * @tparam TValue data tyoe of the value being scaled, assumed to be integral
     * @param value the unscaled value
     * @param units indicator of what scaling, if any, is required
     * @return TValue potentially scaled value
     */
    template <typename TValue>
    TValue scale(TValue& value, TxUnits units) const;

private:
    LogLevel     maxLogLevel = LogMin; /// configured log level filter constraint
    unsigned     groupSelector = TXSUMMARY_GRP_CORE; /// configured value group constraint
public:
    CTxOpenTelemetryConnector();
};

template <typename TValue, typename TSuffix, class TSerializer>
inline bool CTxSummary::append(const char* key, const TValue& value, const LogLevel logLevel, const unsigned int group, const TSuffix& suffix,  const TSerializer& serializer)
{
    return append(key, value, nullptr, TxUnits::NA, logLevel, group, suffix, serializer);
}

template <typename TValue, typename TSuffix, class TSerializer>
inline bool CTxSummary::append(const char* key, const TValue& value, const char* otKey, const LogLevel logLevel, const unsigned int group, const TSuffix& suffix,  const TSerializer& serializer)
{
    return append(key, value, otKey, TxUnits::NA, logLevel, group, suffix, serializer);
}

template <typename TValue, typename TSuffix, class TSerializer>
inline bool CTxSummary::append(const char* key, const TValue& value, TxUnits units, const LogLevel logLevel, const unsigned int group, const TSuffix& suffix,  const TSerializer& serializer)
{
    return append(key, value, nullptr, units, logLevel, group, suffix, serializer);
}

template <typename TValue, typename TSuffix, class TSerializer>
inline bool CTxSummary::append(const char* key, const TValue& value, const char* otKey, TxUnits units, LogLevel logLevel, const unsigned int group, const TSuffix& suffix,  const TSerializer& serializer)
{
    connector->forwardAttribute(key, otKey, value, units, logLevel, group);
    StringBuffer buffer, suffixBuf;
    serializer.serialize(value, buffer);
    serializer.serialize(suffix, suffixBuf);
    bool shouldQuote = true;
    if(std::is_arithmetic<TValue>())
        shouldQuote = false;
    return appendSerialized(key, serializer.str(buffer), logLevel, group, shouldQuote, serializer.str(suffixBuf));
}

template <typename TValue, typename TSuffix, class TSerializer>
inline bool CTxSummary::set(const char* key, const TValue& value, const LogLevel logLevel, const unsigned int group, const TSuffix& suffix, const TSerializer& serializer)
{
    return set(key, value, nullptr, TxUnits::NA, logLevel, group, suffix, serializer);
}

template <typename TValue, typename TSuffix, class TSerializer>
inline bool CTxSummary::set(const char* key, const TValue& value, const char* otKey, const LogLevel logLevel, const unsigned int group, const TSuffix& suffix,  const TSerializer& serializer)
{
    return set(key, value, otKey, TxUnits::NA, logLevel, group, suffix, serializer);
}

template <typename TValue, typename TSuffix, class TSerializer>
inline bool CTxSummary::set(const char* key, const TValue& value, TxUnits units, const LogLevel logLevel, const unsigned int group, const TSuffix& suffix,  const TSerializer& serializer)
{
    return set(key, value, nullptr, units, logLevel, group, suffix, serializer);
}

template <typename TValue, typename TSuffix, class TSerializer>
inline bool CTxSummary::set(const char* key, const TValue& value, const char* otKey, TxUnits units, LogLevel logLevel, const unsigned int group, const TSuffix& suffix,  const TSerializer& serializer)
{
    connector->forwardAttribute(key, otKey, value, units, logLevel, group);
    StringBuffer buffer, suffixBuf;
    serializer.serialize(value, buffer);
    serializer.serialize(suffix, suffixBuf);
    bool shouldQuote = true;
    if(std::is_arithmetic<TValue>())
        shouldQuote = false;
    return setSerialized(key, serializer.str(buffer), logLevel, group, shouldQuote, serializer.str(suffixBuf));
}

template <typename TValue>
inline void CTxOpenTelemetryConnector::forwardAttribute(const char* txKey, const char* otKey, TValue value, TxUnits units, LogLevel logLevel, unsigned groupMask) const
{
    if (std::is_unsigned<TValue>())
        forwardUnsigned(txKey, otKey, value, units, logLevel, groupMask);
    else if (std::is_signed<TValue>())
        forwardSigned(txKey, otKey, value, units, logLevel, groupMask);
    else if (std::is_floating_point<TValue>())
        forwardDouble(txKey, otKey, value, logLevel, groupMask);
}

inline void CTxOpenTelemetryConnector::forwardAttribute(const char* txKey, const char* otKey, const char* value, TxUnits, LogLevel logLevel, unsigned groupMask) const
{
    forwardString(txKey, otKey, value, logLevel, groupMask);
}

inline void CTxOpenTelemetryConnector::forwardAttribute(const char* txKey, const char* otKey, bool value, TxUnits, LogLevel logLevel, unsigned groupMask) const
{
    forwardBool(txKey, otKey, value, logLevel, groupMask);
}

template <typename TValue>
inline TValue CTxOpenTelemetryConnector::scale(TValue& value, TxUnits units) const
{
    switch (units)
    {
    case TxUnits::NA:
        break;
    case TxUnits::millis:
        value *= 1000000;
        break;
    default:
        IERRLOG("unhandled TxUnits %d for value scaling", int(units));
        break;
    }
    return value;
}

class txsummary_decl TxSummaryMapVal
{
    public:
        TxSummaryMapVal(unsigned int _group, unsigned int _style, const char* _newName, bool _replaceOriginal)
            : group(_group), style(_style), newName(_newName), replaceOriginal(_replaceOriginal) {}

        unsigned int group;
        unsigned int style;
        StringAttr newName;
        bool replaceOriginal;
};

interface ITxSummaryProfile : extends IInterface
{
    virtual bool tailorSummary(IEspContext* ctx) = 0;
    virtual void addMap(const char* name, TxSummaryMapVal mapval) = 0;
    virtual bool getEffectiveName(StringBuffer& effectiveName, const char* name, const unsigned int outputGroup, const unsigned int outputStyle) = 0;
    virtual const char* queryEffectiveName(const char* name, const unsigned int outputGroup, const unsigned int outputStyle) = 0;
};

class txsummary_decl CTxSummaryProfileBase : public CInterface, implements ITxSummaryProfile
{
    public:

        IMPLEMENT_IINTERFACE;

        virtual void addMap(const char* name, TxSummaryMapVal mapval) override
        {
            mapping.insert({name, mapval});
        }

        // Return true if a mapping was found. Set effectiveName parameter to
        // the effective name- the name to use in serialzed output.
        // effectiveName is set to the new name in the first matching mapping
        // found, otherwise the passed in
        virtual bool getEffectiveName(StringBuffer& effectiveName, const char* name, const unsigned int outputGroup, const unsigned int outputStyle) override
        {
            auto nameMatches = mapping.equal_range(name);
            for(auto it = nameMatches.first; it != nameMatches.second; it++)
            {
                unsigned int mappingGroup = it->second.group;
                unsigned int mappingStyle = it->second.style;
                if((mappingGroup & outputGroup) && (mappingStyle & outputStyle))
                {
                    effectiveName.set(it->second.newName);
                    return true;
                }
            }
            // we didn't find a match above
            effectiveName.set(name);
            return false;
        }

        // Return the the effective name- the name to use in serialzed output.
        // returns the new name in the first matching mapping found, otherwise
        // returns the passed in 'name'.
        virtual const char* queryEffectiveName(const char* name, const unsigned int outputGroup, const unsigned int outputStyle) override
        {
            auto nameMatches = mapping.equal_range(name);
            for(auto it = nameMatches.first; it != nameMatches.second; it++)
            {
                unsigned int mappingGroup = it->second.group;
                unsigned int mappingStyle = it->second.style;
                if((mappingGroup & outputGroup) && (mappingStyle & outputStyle))
                {
                    return it->second.newName.get();
                }
            }
            // we didn't find a match above
            return name;
        }

    private:
        using NameToMapVal = std::multimap<std::string, TxSummaryMapVal>;
        NameToMapVal mapping;
};

#endif // TXSUMMARY_HPP
