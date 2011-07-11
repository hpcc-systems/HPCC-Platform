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
#ifndef __HQLRESOURCE_HPP_
#define __HQLRESOURCE_HPP_

#include "workunit.hpp"
#include "hqlcpp.hpp"
#include "hqlcpp.ipp"

IHqlExpression * resourceThorGraph(HqlCppTranslator & translator, IHqlExpression * expr, ClusterType targetClusterType, unsigned clusterSize, IHqlExpression * graphIdExpr);
IHqlExpression * resourceLibraryGraph(HqlCppTranslator & translator, IHqlExpression * expr, ClusterType targetClusterType, unsigned clusterSize, IHqlExpression * graphIdExpr, unsigned * numResults);
IHqlExpression * resourceLoopGraph(HqlCppTranslator & translator, HqlExprCopyArray & activeRows, IHqlExpression * expr, ClusterType targetClusterType, IHqlExpression * graphIdExpr, unsigned * numResults, bool insideChildGraph);
IHqlExpression * resourceNewChildGraph(HqlCppTranslator & translator, HqlExprCopyArray & activeRows, IHqlExpression * expr, ClusterType targetClusterType, IHqlExpression * graphIdExpr, unsigned * numResults);
IHqlExpression * resourceRemoteGraph(HqlCppTranslator & translator, IHqlExpression * expr, ClusterType targetClusterType, unsigned clusterSize);

IHqlExpression * convertSpillsToActivities(IHqlExpression * expr);

#endif
