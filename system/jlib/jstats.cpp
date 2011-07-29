/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */


#include "jiface.hpp"
#include "jstats.h"
#include "jexcept.hpp"

extern jlib_decl const char *getStatName(unsigned i)
{
    switch (i)
    {
    case STATS_INDEX_SEEKS: return "Index seeks";
    case STATS_INDEX_SCANS: return "Index scans";
    case STATS_INDEX_WILDSEEKS: return "Index wildseeks";
    case STATS_INDEX_SKIPS: return "Index skips";
    case STATS_INDEX_NULLSKIPS: return "Index null skips";
    case STATS_INDEX_MERGES: return "Index merges";

    case STATS_BLOBCACHEHIT: return "Blob cache hit";
    case STATS_LEAFCACHEHIT: return "Leaf cache hit";
    case STATS_NODECACHEHIT: return "Node cache hit";
    case STATS_PRELOADCACHEHIT: return "Preload cache hit";
    case STATS_BLOBCACHEADD: return "Blob cache add";
    case STATS_LEAFCACHEADD: return "Leaf cache add";
    case STATS_NODECACHEADD: return "Node cache add";
    case STATS_PRELOADCACHEADD: return "Preload cache add";
    
    case STATS_INDEX_MERGECOMPARES: return "Index merge compares";
    case STATS_SERVERCACHEHIT: return "Server side cache potential hits";

    case STATS_ACCEPTED: return "Accepted index reads";
    case STATS_REJECTED: return "Rejected index reads";

    case STATS_DISK_SEEKS: return "Disk seeks";

    default:
        return ("???");
    }
}

extern jlib_decl const char *getStatShortName(unsigned i)
{
    switch (i)
    {
    case STATS_INDEX_SEEKS: return "seeks";
    case STATS_INDEX_SCANS: return "scans";
    case STATS_INDEX_WILDSEEKS: return "wildseeks";
    case STATS_INDEX_SKIPS: return "skips";
    case STATS_INDEX_NULLSKIPS: return "nullskips";
    case STATS_INDEX_MERGES: return "merges";

    case STATS_BLOBCACHEHIT: return "blobhit";
    case STATS_LEAFCACHEHIT: return "leafhit";
    case STATS_NODECACHEHIT: return "nodehit";
    case STATS_PRELOADCACHEHIT: return "preloadhit";
    case STATS_BLOBCACHEADD: return "blobadd";
    case STATS_LEAFCACHEADD: return "leafadd";
    case STATS_NODECACHEADD: return "nodeadd";
    case STATS_PRELOADCACHEADD: return "preloadadd";
    
    case STATS_INDEX_MERGECOMPARES: return "mergecompares";
    case STATS_SERVERCACHEHIT: return "sschits";

    case STATS_ACCEPTED: return "accepted";
    case STATS_REJECTED: return "rejected";

    case STATS_DISK_SEEKS: return "fseeks";
    default:
        return ("???");
    }
}

extern jlib_decl StatisticCombineType getStatCombineMode(unsigned  i)
{
    switch (i)
    {
    case STATS_INDEX_SEEKS:
    case STATS_INDEX_SCANS:
    case STATS_INDEX_WILDSEEKS:
    case STATS_INDEX_SKIPS:
    case STATS_INDEX_NULLSKIPS:
    case STATS_INDEX_MERGES:
    case STATS_BLOBCACHEHIT:
    case STATS_LEAFCACHEHIT:
    case STATS_NODECACHEHIT:
    case STATS_PRELOADCACHEHIT:
    case STATS_BLOBCACHEADD:
    case STATS_LEAFCACHEADD:
    case STATS_NODECACHEADD:
    case STATS_INDEX_MERGECOMPARES: 
    case STATS_SERVERCACHEHIT:
    case STATS_ACCEPTED:
    case STATS_REJECTED:
    case STATS_DISK_SEEKS:
    default: 
        return STATSMODE_COMBINE_SUM;
    }
}

class CStatsCategory : public CInterface
{
public:
    StringAttr longName;
    StringAttr shortName;
    StatisticCombineType mode;

    IMPLEMENT_IINTERFACE;

    CStatsCategory(const char *_longName, const char *_shortName, StatisticCombineType _mode)
        : longName(_longName), shortName(_shortName), mode(_mode)
    {
    }
    bool match(const char *_longName, const char *_shortName, StatisticCombineType _mode)
    {
        bool lm = stricmp(_longName, longName)==0;
        bool sm = stricmp(_shortName, shortName)==0;
        if (lm || sm)
        {
            if (lm && sm && mode==_mode)
                return true;
            throw MakeStringException(0, "A stats category %s (%s) is already registered", shortName.get(), longName.get());
        }
        return false;
    }
};

static CIArrayOf<CStatsCategory> statsCategories;
static CriticalSection statsCategoriesCrit;

extern int registerStatsCategory(const char *longName, const char *shortName, StatisticCombineType mode)
{
    CriticalBlock b(statsCategoriesCrit);
    ForEachItemIn(idx, statsCategories)
    {
        if (statsCategories.item(idx).match(longName, shortName, mode))
            return idx;
    }
    statsCategories.append(*new CStatsCategory(longName, shortName, mode));
    return statsCategories.ordinality()-1;
}
