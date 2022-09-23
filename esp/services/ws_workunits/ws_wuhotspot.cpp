/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.

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
#include "workunit.hpp"

#include "ws_workunitsService.hpp"
#include "ws_wuhotspot.hpp"

#include "anawu.hpp"


// The following only implements a single request - not shared between calls.
class WsStatsAnalyser
{
public:
    WsStatsAnalyser(IConstWorkUnit * _wu) : wu(_wu)
    {
    }

    void processHotspot(IEspWUAnalyseHotspotRequest &req, IEspWUAnalyseHotspotResponse &resp);

protected:
    void expandScope(IEspWUResponseScope *activity, const WuScope * scope);

protected:
    Linked<IConstWorkUnit> wu;
    bool includeFormatted = true;
    bool includeRaw = true;
    bool includeMeasure = true;
    bool includeProperties = true;
    bool includeStatistics = true;
};


void WsStatsAnalyser::expandScope(IEspWUResponseScope *activity, const WuScope * scope)
{
    StringBuffer fullname;
    activity->setScopeName(scope->getFullScopeName(fullname));
    activity->setId(scope->queryName());
    activity->setScopeType(queryScopeTypeName(SSTactivity));

    if (includeProperties || includeStatistics)
    {
        IArrayOf<IEspWUResponseProperty> properties;
        Owned<IAttributeIterator> iter = scope->queryAttrs()->getAttributes();
        ForEach(*iter)
        {
            const char * name = iter->queryName();
            StatisticKind kind = queryStatisticKind(name+1, StKindNone);
            WuAttr attr = queryWuAttribute(name+1, WaNone);
            if (((kind != StKindNone) && includeStatistics) || ((attr != WaNone) && includeProperties))
            {
                const char * value = iter->queryValue();
                IEspWUResponseProperty * property = createWUResponseProperty();
                property->setName(name+1);
                if (includeRaw)
                    property->setRawValue(value);
                if (includeMeasure && (kind != StKindNone))
                    property->setMeasure(queryMeasureName(queryMeasure(kind)));
                if (includeFormatted)
                {
                    if (kind != StKindNone)
                    {
                        StringBuffer formatted;
                        formatStatistic(formatted, atoi64(value), queryMeasure(kind));
                        property->setFormatted(formatted);
                    }
                    else
                    {
                        //Avoid duplicating attributes which may contain lots of text
                        if (!includeRaw)
                            property->setFormatted(value);
                    }
                }

                properties.append(*property);
            }
        }
        activity->setProperties(properties);
    }
}

void WsStatsAnalyser::processHotspot(IEspWUAnalyseHotspotRequest &req, IEspWUAnalyseHotspotResponse &resp)
{
    Owned<IPropertyTree> options = createPTree();
    options->setPropReal("@threshold", req.getThresholdPercent());
    options->setProp("@rootScope", req.getRootScope());

    IConstWUPropertyOptions & propertyOptions = req.getPropertyOptions();
    includeFormatted = propertyOptions.getIncludeFormatted();
    includeRaw = propertyOptions.getIncludeRawValue();
    includeMeasure = propertyOptions.getIncludeMeasure();
    includeProperties = req.getIncludeProperties();
    includeStatistics = req.getIncludeStatistics();

    WuHotspotResults results;
        analyseHotspots(results, wu, options);

    resp.setRootScope(req.getRootScope());
    resp.setRootTime(results.totalTime);
    IArrayOf<IEspWUResponseScope> activities;
    ForEachItemIn(i, results.hotspots)
    {
        const WuHotspotResult &result = results.hotspots.item(i);
        if (!result.isRoot)
        {
            IEspWUResponseScope *activity = createWUResponseScope();
            expandScope(activity, result.activity);
            activity->setSinkActivity(result.sink);
            activities.append(*activity);
        }
    }

    resp.setActivities(activities);
}

void processAnalyseHotspot(IConstWorkUnit * wu, IEspWUAnalyseHotspotRequest &req, IEspWUAnalyseHotspotResponse &resp)
{
    WsStatsAnalyser analyser(wu);
    analyser.processHotspot(req, resp);
}
