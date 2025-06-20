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

#include "eventindexmodel.hpp"

class CIndexEventModel : public CInterfaceOf<IEventModel>
{
public: // IEventModel
    IMPLEMENT_IEVENTVISITATIONLINK;

    virtual bool visitEvent(CEvent& event) override
    {
        if (!nextLink)
            return false;
        bool propagate = true;
        switch (event.queryType())
        {
        case MetaFileInformation:
            propagate = onMetaFileInformation(event);
            break;
        case EventIndexLoad:
            propagate = onIndexLoad(event);
            break;
        default:
            break;
        }
        return (propagate ? nextLink->visitEvent(event) : true);
    }

public:
    bool configure(const IPropertyTree& config)
    {
        const IPropertyTree* node = config.queryBranch("storage");
        if (!node)
            return false;
        storage.configure(*node);
        return true;
    }

protected:
    bool onMetaFileInformation(CEvent& event)
    {
        storage.observeFile(event.queryNumericValue(EvAttrFileId), event.queryTextValue(EvAttrPath));
        return true;
    }

    bool onIndexLoad(CEvent& event)
    {
        ModeledPage page;
        storage.describePage(event.queryNumericValue(EvAttrFileId), event.queryNumericValue(EvAttrFileOffset), page);
        event.setValue(EvAttrReadTime, page.readTime);
        return true;
    }

protected:
    Storage storage;
};

IEventModel* createIndexEventModel(const IPropertyTree& config)
{
    Owned<CIndexEventModel> model = new CIndexEventModel;
    model->configure(config);
    return model.getClear();
}
