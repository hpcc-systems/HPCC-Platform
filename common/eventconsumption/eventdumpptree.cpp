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

#include "eventdump.hpp"

// Concrete extension of CDumpEventVisitor<IPTreeEventVisitor> that uses an IPropertyTree to store
// visited data. The tree is constructed in memory and is available via the `queryTree` method
// after visitation.
class CPTreeEventVisitor : public CDumpEventVisitor
{
public: // IEventAttributeVisitor
    virtual bool visitFile(const char* filename, uint32_t version) override
    {
        tree.setown(createPTree(DUMP_STRUCTURE_ROOT));
        active = tree->addPropTree(DUMP_STRUCTURE_HEADER, createPTree());
        doVisitHeader(filename, version);
        return true;
    }

    virtual Continuation visitEvent(EventType id) override
    {
        active = tree->addPropTree(DUMP_STRUCTURE_EVENT, createPTree());
        doVisitEvent(id);
        return visitContinue;
    }

    virtual bool departEvent() override
    {
        active = tree.get();
        return true;
    }

    virtual void departFile(uint32_t bytesRead) override
    {
        active = tree->addPropTree(DUMP_STRUCTURE_FOOTER, createPTree());
        doVisitFooter(bytesRead);
    }

protected:
    virtual void recordAttribute(EventAttr id, const char* name, const char* value, bool quoted) override
    {
        active->addProp(VStringBuffer("@%s", name), value);
    }

public:
    virtual IPTree* queryTree() const
    {
        return tree.get();
    }

protected:
    Owned<IPTree> tree;
    IPTree* active = nullptr;
};

IEventPTreeCreator* createEventPTreeCreator()
{
    class Creator : public CInterfaceOf<IEventPTreeCreator>
    {
    public:
        virtual IEventAttributeVisitor& queryVisitor() override
        {
            return *visitor;
        }

        virtual IPTree* queryTree() const override
        {
            return visitor->queryTree();
        }

        Creator()
        {
            visitor.setown(new CPTreeEventVisitor);
        }

    private:
        Owned<CPTreeEventVisitor> visitor;
    };
    return new Creator;
}
