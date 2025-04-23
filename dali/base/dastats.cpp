/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems®.

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


#include "platform.h"
#include "jlib.hpp"
#include "jfile.hpp"
#include "jlzw.hpp"

#include "dastats.hpp"
#include "dasds.hpp"

static bool recordWithHourGranularity = true;
static constexpr unsigned readConnectionTimeout = 20000;
static constexpr unsigned updateConnectionTimeout = 2000; // If an update fails, we do not want to block the caller for too long
static constexpr const char * GlobalMetricsDaliRoot = "/GlobalMetrics";

/*
 * Currently global metrics are represented in dali in the following way
 *
 * /GlobalMetrics
 *   /Category
 *     /Instance[@dimension1=..., @dimension2=... @qualifierKey="dimension1_dimension2_..."]
 *       /Timeslot[@startTime="yyyymmddhh" @endTime="yyyymmddhh"]
 *           /Metrics[@metric1=... @metric2=...]
 *
 * The qualifierKey attribute is used to ensure that each unique combination of dimensions are rolled up independently
 * so we also need a key attribute to indicate which dimensions are present.  The alternative would be to require that
 * all dimensions are provided on all calls (with blank values if not relevant)).
 *
 * The timeslots contain both the start and end time.  This allows timeslots of different width - and which allows historical
 * data to be rolled up by day or week.  Using start and end dates allow ptree filters to be executed on the server - avoiding
 * returning the data to the client.
 *
 * Some examples:
 *
 * /GlobalMetrics
 *   /Queue
 *     /Instance[@component='thor'][@name='thor400][@user='ghalliday'][@qualifierKey="component_name_user_"]
 *       /Timeslot[@startTime="2024080107" @endTime="2024080107"]
 *           /Metrics[@NumFailures=1]
 *   /Storage
 *     /Instance[@name='dataplane'][@user='ghalliday'][@qualifierKey="name_user_"]
 *       /Timeslot[@startTime="2024080107" @endTime="2024080107"]
 *         /Metrics[@NumReads=1 @SizeRead=1000]
 *   /Dali
 *     /Instance
 *       /Timeslot
 *         /Metrics[@NumRequests=... ]
 *
 * Possibilities:
 *    Also split storage by component?
 *    Split queue costs down to the instance as well?
 */


//This is very similar to the MetricsDimensionList - this is the equivalent filters to apply to a ptree.
using PTreeQualifierList = std::vector<std::pair<std::string, std::string>>;

//MORE: Ideally this would be implemented by the connect() operation in dali
//NOTE: We need to ensure that each unique combination of dimensions are rolled up independently
//So we also need a key attribute to indicate which dimensions are present
IPropertyTree * ensureFilteredPTree(IPropertyTree * root, const char * element, const PTreeQualifierList & qualifiers, bool ensureDistinctQualifiers)
{
    StringBuffer childPath;
    childPath.append(element);

    StringBuffer qualificationsKey;
    for (auto & [name, value] : qualifiers)
    {
        childPath.append('[');
        childPath.append(name).append('=');
        childPath.append('"').append(value).append('"').append(']');
        qualificationsKey.append(name.c_str()+1).append("_");
    }

    if (ensureDistinctQualifiers)
        childPath.append("[@qualifierKey='").append(qualificationsKey).append("']");

    IPropertyTree * match = root->queryPropTree(childPath);
    if (!match)
    {
        match = root->addPropTree(element, createPTree());
        for (auto & [name, value] : qualifiers)
            match->setProp(name.c_str(), value.c_str());
        if (ensureDistinctQualifiers)
            match->setProp("@qualifierKey", qualificationsKey);
    }
    return match;
}


//MORE: Ideally this would be implemented as an atomic operation in daserver.  Until then it is coded less efficiently.
void daliAtomicUpdate(IPropertyTree * entry, const std::vector<std::string> & attributes, const std::vector<stat_type> & values)
{
    for (unsigned i = 0; i < attributes.size(); i++)
    {
        if (values[i])
        {
            const char * attribute = attributes[i].c_str();
            stat_type prevValue = entry->getPropInt64(attribute);
            entry->setPropInt64(attribute, prevValue + values[i]);
        }
    }
}

//---------------------------------------------------------------------------------------------------------------------

#ifdef _USE_CPPUNIT
static __uint64 timeDelta = 0;
//Used by the unit tests to set the apparent time for the following transactions.
void setGlobalMetricNowTime(const char * time)
{
    CDateTime now;
    now.setNow();

    CDateTime expected;
    expected.setString(time, nullptr, false);

    timeDelta = expected.getSimple() - now.getSimple();
}
#endif


static void convertDimensionsToQualifiers(PTreeQualifierList & qualifiers, const MetricsDimensionList & dimensions)
{
    for (auto & [name, value] : dimensions)
    {
        std::string attributeName = std::string("@") + name;
        qualifiers.emplace_back(std::move(attributeName), value);
    }
}

static void getTimeslotValue(StringBuffer & value, const CDateTime & when, bool isEnd)
{
    //MORE: Introduce CDateTime::getUtcYear() etc.??
    unsigned year, month, day;
    when.getDate(year, month, day, false);

    unsigned hour;
    if (recordWithHourGranularity)
        hour = when.getUtcHour();
    else
        hour = isEnd ? 23 : 0;

    value.appendf("%04d%02d%02d%02d", year, month, day, hour);
}

inline std::string getStatisticAttribute(StatisticKind kind)
{
    return std::string("@") + queryStatisticName(kind);
}

static void recordGlobalMetrics(const char * category, const MetricsDimensionList & dimensions, const std::vector<std::string> &attributes, const std::vector<stat_type> & deltas)
{
    //Ideally this would connect to:
    //
    //  /GlobalMetrics/<category>/Instance[@dimension1=...]/Timeslot[@start="yyyymmddhh" @rollup="h"]/Metrics
    //
    // but that would require connect to support the creation of tags with attributes if they did not exist (should be possible)
    // and the initial values for @rollup would need to be provided as extra search criteria or something similar.
    //

    PTreeQualifierList qualifiers;
    convertDimensionsToQualifiers(qualifiers, dimensions);

    StringBuffer startTimeslot;
    StringBuffer endTimeslot;
    CDateTime now;
    now.setNow();

#ifdef _USE_CPPUNIT
    if (timeDelta)
        now.adjustTimeSecs(timeDelta);
#endif

    getTimeslotValue(startTimeslot, now, false);
    getTimeslotValue(endTimeslot, now, true);

    StringBuffer rootPath;
    rootPath.append(GlobalMetricsDaliRoot).append('/').append(category);
    Owned<IRemoteConnection> conn = querySDS().connect(rootPath, myProcessSession(), RTM_LOCK_WRITE|RTM_CREATE_QUERY, updateConnectionTimeout);

    IPropertyTree * instance = ensureFilteredPTree(conn->queryRoot(), "Instance", qualifiers, true);
    //NOTE: This updates a single timeslot, which has both a start and end time.
    IPropertyTree * timeslot = ensureFilteredPTree(instance, "Timeslot", PTreeQualifierList{{"@startTime", startTimeslot.str()},{"@endTime",endTimeslot.str()}}, false);
    IPropertyTree * metrics = ensurePTree(timeslot, "Metrics");

    daliAtomicUpdate(metrics, attributes, deltas);
}

void recordGlobalMetrics(const char * category, const MetricsDimensionList & dimensions, const CRuntimeStatisticCollection & stats, const StatisticsMapping * optMapping)
{
    const StatisticsMapping * mapping = optMapping ? optMapping : &stats.queryMapping();
    std::vector<std::string> attributes;
    std::vector<stat_type> deltas;
    for (unsigned i = 0; i < mapping->numStatistics(); i++)
    {
        StatisticKind kind = mapping->getKind(i);
        attributes.emplace_back(getStatisticAttribute(kind));
        deltas.emplace_back(stats.getStatisticValue(kind));
    }

    recordGlobalMetrics(category, dimensions, attributes, deltas);
}

void recordGlobalMetrics(const char * category, const MetricsDimensionList & dimensions, const std::initializer_list<StatisticKind> & stats, const std::initializer_list<stat_type> & values)
{
    dbgassertex(stats.size() == values.size());
    std::vector<std::string> attributes;
    std::vector<stat_type> deltas;
    for (unsigned i = 0; i < stats.size(); i++)
    {
        StatisticKind kind = stats.begin()[i];
        attributes.emplace_back(getStatisticAttribute(kind));
        deltas.emplace_back(values.begin()[i]);
    }

    recordGlobalMetrics(category, dimensions, attributes, deltas);
}

//---------------------------------------------------------------------------------------------------------------------

static void getInstanceFilter(StringBuffer & xpath, const MetricsDimensionList & optDimensions)
{
    for (const auto & x : optDimensions)
    {
        xpath.append('[');
        xpath.append('@').append(x.first).append('=').append('"').append(x.second).append('"').append(']');
    }
}

static void gatherInstanceStatistics(IPropertyTree & categoryTree, const MetricsDimensionList & optDimensions, const CDateTime & from, const CDateTime & to, IGlobalMetricRecorder & walker)
{
    StringBuffer instancePath;
    instancePath.append("Instance");
    getInstanceFilter(instancePath, optDimensions);

    //Timeslot start and end times are inclusive, search end time are also inclusive
    //The timeslot is a match if endTime >= searchStartTime and startTime <= searchEndTime
    StringBuffer timeslotXPath;
    timeslotXPath.append("Timeslot");

    timeslotXPath.append("[@startTime<=");
    getTimeslotValue(timeslotXPath, to, false);
    timeslotXPath.append("][@endTime>=");
    getTimeslotValue(timeslotXPath, from, false);
    timeslotXPath.append("]");

    MetricsDimensionList dimensions;
    GlobalStatisticsList stats;
    Owned<IPropertyTreeIterator> iter = categoryTree.getElements(instancePath, iptiter_remote);

    ForEach(*iter)
    {
        IPropertyTree & instance = iter->query();

        //Gather the dimensions...
        dimensions.clear();
        Owned<IAttributeIterator> attrs = instance.getAttributes();
        ForEach(*attrs)
        {
            const char * name = attrs->queryName();
            if (!streq(name, "@qualifierKey"))
                dimensions.emplace_back(name+1, attrs->queryValue());
        }

        Owned<IPropertyTreeIterator> timeslots = instance.getElements(timeslotXPath, iptiter_remote);
        ForEach(*timeslots)
        {
            IPropertyTree & timeSlot = timeslots->query();
            const char * startTime = timeSlot.queryProp("@startTime");
            const char * endTime = timeSlot.queryProp("@endTime");

            IPropertyTree * metrics = timeSlot.queryPropTree("Metrics");
            stats.clear();

            Owned<IAttributeIterator> statsAttrs = metrics->getAttributes();
            ForEach(*statsAttrs)
            {
                const char * name = statsAttrs->queryName();
                const char * value = statsAttrs->queryValue();
                StatisticKind kind = queryStatisticKind(name+1, StKindNone);
                stat_type num = strtoll(value, nullptr, 10);

                stats.emplace_back(kind, num);
            }

            walker.processGlobalStatistics(categoryTree.queryName(), dimensions, startTime, endTime, stats);
        }
    }
}

extern da_decl void gatherGlobalMetrics(const char * optCategory, const MetricsDimensionList & optDimensions, const CDateTime & from, const CDateTime & to, IGlobalMetricRecorder & walker)
{
    if (optCategory)
    {
        StringBuffer xpath;
        xpath.append(GlobalMetricsDaliRoot).append('/').append(optCategory);

        Owned<IRemoteConnection> conn = querySDS().connect(xpath, myProcessSession(), RTM_LOCK_READ, readConnectionTimeout);
        if (!conn)
            return;

        gatherInstanceStatistics(*conn->queryRoot(), optDimensions, from, to, walker);
    }
    else
    {
        Owned<IRemoteConnection> conn = querySDS().connect(GlobalMetricsDaliRoot, myProcessSession(), RTM_LOCK_READ, readConnectionTimeout);
        if (!conn)
            return;

        Owned<IPropertyTreeIterator> iter = conn->queryRoot()->getElements("*");
        ForEach(*iter)
            gatherInstanceStatistics(iter->query(), optDimensions, from, to, walker);
    }
}

#ifdef _USE_CPPUNIT
void resetGlobalMetrics(const char * category, const MetricsDimensionList & optDimensions)
{
    assertex(category);

    StringBuffer xpath;
    xpath.append(GlobalMetricsDaliRoot).append('/').append(category);

    Owned<IRemoteConnection> conn = querySDS().connect(xpath, myProcessSession(), RTM_LOCK_WRITE, readConnectionTimeout);
    if (!conn)
        return;

    StringBuffer childPath;
    childPath.append("Instance");
    getInstanceFilter(childPath, optDimensions);

    IPropertyTree * root = conn->queryRoot();
    ICopyArrayOf<IPropertyTree> toRemove;
    Owned<IPropertyTreeIterator> it = root->getElements(childPath);
    ForEach(*it)
        toRemove.append(it->query());

    ForEachItemIn(i, toRemove)
        root->removeTree(&toRemove.item(i));
}
#endif
