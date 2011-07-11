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


#ifndef JSTATS_H
#define JSTATS_H

#include "jlib.hpp"

enum StatisticCombineType
{
    STATSMODE_COMBINE_SUM,
    STATSMODE_COMBINE_MAX,
    STATSMODE_COMBINE_MIN
};

enum StatisticType
{
    STATS_INDEX_SEEKS,
    STATS_INDEX_SCANS,
    STATS_INDEX_WILDSEEKS,
    STATS_INDEX_SKIPS,
    STATS_INDEX_NULLSKIPS,
    STATS_INDEX_MERGES,

    STATS_BLOBCACHEHIT,
    STATS_LEAFCACHEHIT,
    STATS_NODECACHEHIT,
    STATS_BLOBCACHEADD,
    STATS_LEAFCACHEADD,
    STATS_NODECACHEADD,

    STATS_INDEX_MERGECOMPARES,

    STATS_PRELOADCACHEHIT,
    STATS_PRELOADCACHEADD,

    STATS_SERVERCACHEHIT,

    STATS_ACCEPTED,
    STATS_REJECTED,

    STATS_DISK_SEEKS,

    STATS_SIZE
};

extern jlib_decl const char *getStatName(unsigned i);
extern jlib_decl const char *getStatShortName(unsigned i);
extern jlib_decl StatisticCombineType getStatCombineMode(unsigned i);

#endif
