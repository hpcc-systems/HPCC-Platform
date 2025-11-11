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

#pragma once

#include "eventoperation.h"
#include "eventvisitor.h"
#include <vector>

class event_decl CIndexPlotOp : public CEventConsumingOp, private IEventVisitor
{
public:
    static constexpr const char* readTimeSelector = "readTime";
    static constexpr const char* expandTimeSelector = "expandTime";
    static constexpr const char* elapsedTimeSelector = "elapsedTime";
    static constexpr const char* cacheMissesSelector = "cacheMisses";

protected:
    enum class ValueSelector : byte
    {
        Unknown,
        ReadTime,
        ExpandTime,
        ElapsedTime,
        CacheMisses,
    };

    struct Iteration
    {
        StringAttr name;
        struct Delta
        {
            StringAttr linkId;
            StringAttr xpath;
            StringAttr value;

            Delta(const IPropertyTree& delta) : linkId(delta.queryProp("@linkId")), xpath(delta.queryProp("@xpath")), value(delta.queryProp("@value")) {}
        };
        std::vector<Delta> deltas;

        Iteration() = default;
        Iteration(const char* name, std::vector<Delta>&& _deltas) : name(name), deltas(std::move(_deltas)) {}
    };

    using Iterations = std::vector<Iteration>;

    // Container for a configured link. The effective identifier is extracted from the property
    // one time. The original property tree is retained for reuse between iterations. A clone of
    // the original is used when applying iteration deltas.
    struct LinkSpec
    {
        StringAttr id;
        Linked<const IPropertyTree> original;
        Owned<IPropertyTree> modified;

        LinkSpec(const char* _id, const IPropertyTree& _original) : id(_id), original(&_original) {}
    };

    using LinkSpecs = std::vector<LinkSpec>;

    // Ordered collection of iterations applicable to one event traversal. Contents will be:
    // 1. optional plot iterations applicable to every traversal
    // 2. Y-axis iterations (3D plots only)
    // 3. X-axis iterations
    using LinkChanges = std::vector<const Iteration*>;

public:
    virtual bool ready() const override;
    virtual bool doOp() override;
    bool acceptEvents(const char* eventNames) { UNIMPLEMENTED; }
    bool acceptAttribute(EventAttr attr, const char* values) { UNIMPLEMENTED; }
    bool acceptModel(const IPropertyTree& config) { UNIMPLEMENTED; }

protected:
    virtual bool visitFile(const char* filename, uint32_t version) override;
    virtual bool visitEvent(CEvent& event) override;
    virtual void departFile(uint32_t bytesRead) override;

public:
    IMPLEMENT_IINTERFACE_USING(CEventConsumingOp);
    bool hasInputPath() const;
    void setOpConfig(const IPropertyTree& _config);

protected:
    static ValueSelector parseValueSelector(const char* selector);
    static bool compareLinkIds(const char* linkLinkId, const char* deltaLinkId);
    void parseIterations(IPropertyTreeIterator *iterIter, Iterations &iterations);
    void validateIterations(const Iterations &iterations, bool isAxis);
    bool doOnePlot(LinkChanges &linkChanges);
    bool doXAxis(LinkChanges& linkChanges, size_t yAxisIdx);
    void applyIteration(LinkChanges &linkChanges);
    StringBuffer &getAxisValueName(StringBuffer &name, const Iteration &iteration);
    void outputCell(StringBuffer& cellData, bool leadingDelimiter);
    void outputEOLN();

protected:
    LinkSpecs links;
    Iterations plotIterations;
    Iterations yAxis;
    Iterations xAxis;
    ValueSelector valueSelector{ValueSelector::Unknown};
    size_t cellIdx{0};
    __uint64 cellValue{0};
};
