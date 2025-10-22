/*##############################################################################

    Copyright (C) 2025 HPCC Systems®.

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

#include "eventindexplot.h"
#include "eventutility.hpp"
#include "jptree.hpp"
#include "jexcept.hpp"

static constexpr const char* defaultLinkId = "index-events";

bool CIndexPlotOp::ready() const
{
    // Check if configuration has been properly loaded
    return !links.empty() && valueSelector != ValueSelector::Unknown && !xAxis.empty();
}

bool CIndexPlotOp::doOp()
{
    if (!ready())
        return false;

    LinkChanges linkChanges;
    if (plotIterations.empty()) // single plot requested
        return doOnePlot(linkChanges);
    for (const Iteration& plotIteration : plotIterations) // one or more plots requested
    {
        linkChanges.push_back(&plotIteration);
        if (!doOnePlot(linkChanges))
            return false;
        linkChanges.pop_back();
    }
    return true;
}

bool CIndexPlotOp::visitFile(const char* filename, uint32_t version)
{
    return true;
}

bool CIndexPlotOp::visitEvent(CEvent& event)
{
    switch (event.queryType())
    {
    case EventIndexLoad:
        switch (valueSelector)
        {
        case ValueSelector::ReadTime:
            cellValue += event.queryNumericValue(EvAttrReadTime);
            break;
        case ValueSelector::ExpandTime:
            cellValue += event.queryNumericValue(EvAttrExpandTime);
            break;
        case ValueSelector::ElapsedTime:
            cellValue += event.queryNumericValue(EvAttrReadTime);
            cellValue += event.queryNumericValue(EvAttrExpandTime);
            break;
        default:
            break;
        }
        break;
    case EventIndexPayload:
        switch (valueSelector)
        {
        case ValueSelector::ExpandTime:
            cellValue += event.queryNumericValue(EvAttrExpandTime);
            break;
        case ValueSelector::ElapsedTime:
            cellValue += event.queryNumericValue(EvAttrExpandTime);
            break;
        default:
            break;
        }
        break;
    case EventIndexCacheMiss:
        if (valueSelector == ValueSelector::CacheMisses)
            cellValue = 1;
        break;
    default:
        break;
    }
    return true;
}

void CIndexPlotOp::departFile(uint32_t bytesRead)
{
}

bool CIndexPlotOp::hasInputPath() const
{
    // Check if any input paths are available
    return !inputPath.isEmpty();
}

void CIndexPlotOp::setOpConfig(const IPropertyTree& _config)
{
    // Clear any existing configuration
    links.clear();
    plotIterations.clear();
    xAxis.clear();
    yAxis.clear();
    valueSelector = ValueSelector::Unknown;

    // Parse command section (required)
    IPropertyTree* command = _config.queryPropTree("command");
    if (!command)
        throw makeStringException(0, "Missing required 'command' section in configuration");

    // Validate command name
    const char* commandName = command->queryProp("@name");
    if (!commandName || !strieq(commandName, "index.plot"))
        throw makeStringException(0, "Invalid or missing command name, expected 'index.plot'");

    // Accept input path (conditional)
    const char* path = command->queryProp("@input");
    if (!isEmptyString(path))
    {
        if (!hasInputPath())
            inputPath.set(path);
        else if (!strieq(path, inputPath.get()))
            throw makeStringExceptionV(0, "Conflicting input paths: '%s' and '%s'", path, inputPath.get());
    }

    // Parse link configurations (required)
    Owned<IPropertyTreeIterator> linkIter = command->getElements("link");
    ForEach(*linkIter)
    {
        IPropertyTree& linkSpec = linkIter->query();
        StringBuffer id;
        if (linkSpec.hasProp("@kind"))
            id.append(linkSpec.queryProp("@kind"));
        else
            id.append(defaultLinkId);
        if (linkSpec.hasProp("@id"))
            id.append('.').append(linkSpec.queryProp("@id"));
        links.emplace_back(id.str(), linkSpec);
    }
    if (links.empty())
        throw makeStringException(0, "Missing required index-events 'link' section in plot configuration");

    // Parse plot iterations (optional)
    parseIterations(command->getElements("plot"), plotIterations);
    validateIterations(plotIterations, false);

    // Parse x-axis configuration (required)
    IPropertyTree* xAxisTree = command->queryPropTree("x-axis");
    if (!xAxisTree)
        throw makeStringException(0, "Missing required 'x-axis' section in configuration");
    parseIterations(xAxisTree->getElements("iteration"), xAxis);
    validateIterations(xAxis, true);

    // Parse y-axis configuration (optional)
    IPropertyTree* yAxisTree = command->queryPropTree("y-axis");
    if (yAxisTree)
    {
        parseIterations(yAxisTree->getElements("iteration"), yAxis);
        validateIterations(yAxis, true);
    }

    valueSelector = parseValueSelector(command->queryProp("@valueSelector"));
    if (ValueSelector::Unknown == valueSelector)
        throw makeStringExceptionV(0, "invalid (or missing) value selector plot value selector '%s'", command->queryProp("valueSelector"));
}

CIndexPlotOp::ValueSelector CIndexPlotOp::parseValueSelector(const char* selector)
{
    if (!selector)
        return ValueSelector::Unknown;

    if (strieq(selector, readTimeSelector))
        return ValueSelector::ReadTime;
    else if (strieq(selector, expandTimeSelector))
        return ValueSelector::ExpandTime;
    else if (strieq(selector, elapsedTimeSelector))
        return ValueSelector::ElapsedTime;
    else if (strieq(selector, cacheMissesSelector))
        return ValueSelector::CacheMisses;
    else
        return ValueSelector::Unknown;
}

bool CIndexPlotOp::compareLinkIds(const char* linkLinkId, const char* deltaLinkId)
{
    bool emptyLink = isEmptyString(linkLinkId);
    bool emptyDelta = isEmptyString(deltaLinkId);
    if (emptyLink && emptyDelta)
        return true;
    if (emptyLink)
        return strieq(defaultLinkId, deltaLinkId);
    if (emptyDelta)
        return strieq(defaultLinkId, linkLinkId);
    return strieq(linkLinkId, deltaLinkId);
}

void CIndexPlotOp::parseIterations(IPropertyTreeIterator* iterIter, Iterations& iterations)
{
    Owned<IPropertyTreeIterator> owner = iterIter; // ensure iterator cleanup on completion
    if (!iterIter->first())
        return;

    ForEach(*iterIter)
    {
        IPropertyTree& iterTree = iterIter->query();
        Iteration iteration;
        iteration.name.set(iterTree.queryProp("@name"));

        Owned<IPropertyTreeIterator> deltaIter = iterTree.getElements("delta");
        ForEach(*deltaIter)
        {
            IPropertyTree& deltaTree = deltaIter->query();
            Iteration::Delta delta;
            delta.linkId.set(deltaTree.queryProp("@linkId"));
            delta.xpath.set(deltaTree.queryProp("@xpath"));
            delta.value.set(deltaTree.queryProp("@value"));
            iteration.deltas.push_back(delta);
        }

        iterations.push_back(iteration);
    }
}

void CIndexPlotOp::validateIterations(const Iterations& iterations, bool isAxis)
{
    for (const Iteration& iteration : iterations)
    {
        // Each iteration must have a non-empty name
        if (!isAxis && isEmptyString(iteration.name.get()))
            throw makeStringException(0, "Plot iteration must have a non-empty name");

        // Each iteration must have at least one delta
        if (isAxis && iteration.deltas.empty())
            throw makeStringExceptionV(0, "Axis iteration %s must have at least one delta", iteration.name.get());

        for (const Iteration::Delta& delta : iteration.deltas)
        {
            // Each delta must reference, either implicitly or explicitly, a link configuration
            LinkSpecs::const_iterator link = std::find_if(links.begin(), links.end(), [&](const LinkSpec& l) {
                return compareLinkIds(l.id.get(), delta.linkId.get());
            });
            if (link == links.end())
                throw makeStringExceptionV(0, "Invalid linkId referenced in delta: %s", delta.linkId.get());

            // Each delta must have a non-empty xpath
            if (isEmptyString(delta.xpath.get()))
                throw makeStringException(0, "Delta must have a non-empty xpath");
        }
    }
}

bool CIndexPlotOp::doOnePlot(LinkChanges& linkChanges)
{
    StringBuffer plotData;
    size_t yAxisIdx = 0;
    cellIdx = 0;

    // handle optional plot name (identified as the name of the first plot variant)
    if (!linkChanges.empty())
    {
        const char* plotName = linkChanges.front()->name.get();
        if (!isEmptyString(plotName))
        {
            plotData.append(plotName).append('\n');
            out->put(plotData.length(), plotData.str());
            plotData.clear();
        }
    }

    if (!yAxis.empty()) // 3D plot
    {
        // output the column headers (leave first column blank for row labels)
        for (size_t idx = 0; idx < xAxis.size(); idx++)
            outputCell(getAxisValueName(plotData, xAxis[idx]), true);
        outputEOLN();

        // produce x-axis values for every y-axis iterations so the values vector is a sequence of rows

        for (const Iteration& yAxisIteration : yAxis)
        {
            linkChanges.push_back(&yAxisIteration);
            if (!doXAxis(linkChanges, yAxisIdx))
                return false;
            linkChanges.pop_back();
            yAxisIdx++;
        }
    }
    else // 2D plot
    {
        // output column headers (without room for row labels)
        outputCell(getAxisValueName(plotData, xAxis[0]), false);
        for (size_t idx = 1; idx < xAxis.size(); idx++)
            outputCell(getAxisValueName(plotData, xAxis[idx]), true);
        outputEOLN();

        return doXAxis(linkChanges, yAxisIdx);
    }
    return true;
}

bool CIndexPlotOp::doXAxis(LinkChanges& linkChanges, size_t yAxisIdx)
{
    StringBuffer cellData;
    bool is3D = yAxisIdx < yAxis.size();
    if (is3D)
    {
        getAxisValueName(cellData, yAxis[yAxisIdx]);
        outputCell(cellData, false);
    }
    for (const Iteration& xAxisIteration : xAxis)
    {
        // adjust the link configurations as needed to produce one plot value
        linkChanges.push_back(&xAxisIteration);
        applyIteration(linkChanges);

        // generate the visitor chain to produce one plot value
        Owned<IEventVisitor> chain;
        chain.set(this);
        for (const LinkSpec& linkSpec : links)
        {
            const IPropertyTree& linkTree = *(linkSpec.modified ? linkSpec.modified : linkSpec.original);
            const char* kind = linkTree.queryProp("@kind");
            Owned<IEventVisitationLink> link;
            if (strieq(kind, "event-filter"))
                link.setown(createEventFilter(linkTree));
            else
                link.setown(createEventModel(linkTree));
            link->setNextLink(*chain);
            chain.setown(link.getClear());
        }

        // visit the input events to procuce one plot value
        cellValue = 0;
        if (!traverseEvents(inputPath, *chain))
            return false;

        // reset the link configurations in preparation for the next plot value
        for (LinkSpec& link : links)
        {
            if (link.modified)
                link.modified.clear();
        }

        // output the cell value
        outputCell(cellData.append(cellValue), (is3D || cellIdx % xAxis.size() == 0));

        // prepare for next iteration
        cellIdx++;
    }
    outputEOLN();

    return true;
}

void CIndexPlotOp::applyIteration(LinkChanges& linkChanges)
{
    for (const Iteration* iteration : linkChanges)
    {
        for (const Iteration::Delta& delta : iteration->deltas)
        {
            LinkSpecs::iterator linkIt = std::find_if(links.begin(), links.end(), [&](const LinkSpec& link) {
                return compareLinkIds(link.id.get(), delta.linkId.get());
            });
            if (linkIt == links.end())
                throw makeStringExceptionV(0, "Invalid link identifier '%s' referenced in iteration", delta.linkId.get());
            if (!linkIt->modified)
                linkIt->modified.setown(createPTreeFromIPT(linkIt->original.get())); // clone before modifying

            const char* xpath = delta.xpath.get();
            if (!isEmptyString(xpath))
            {
                const char* value = delta.value.get();
                if (value)
                    linkIt->modified->setProp(xpath, value);
                else
                    linkIt->modified->removeProp(xpath);
            }
        }
    }
}

StringBuffer& CIndexPlotOp::getAxisValueName(StringBuffer& name, const Iteration& iteration)
{
    name.append('"');
    if (!iteration.name.isEmpty())
        name.append(iteration.name);
    else if (iteration.deltas.size() == 1 && !iteration.deltas.front().value.isEmpty())
    {
        try
        {
            __uint64 bytes = strToBytes(iteration.deltas.front().value.get(), StrToBytesFlags::ThrowOnError);
            name.append(bytes);
        }
        catch (...)
        {
            name.append(iteration.deltas.front().value);
        }
    }
    else
        name.append("ambiguous");
    name.append('"');
    return name;
}

void CIndexPlotOp::outputCell(StringBuffer& cellData, bool leadingDelimiter)
{
    if (leadingDelimiter)
        out->put(1, ",");
    out->put(cellData.length(), cellData.str());
    out->flush();
    cellData.clear();
}

void CIndexPlotOp::outputEOLN()
{
    out->put(1, "\n");
    out->flush();
}
