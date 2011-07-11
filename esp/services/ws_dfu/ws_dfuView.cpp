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

#pragma warning (disable : 4786)

#include "ws_dfuService.hpp"


unsigned CLogicalView::idgen_ = 0;


CLogicalView *LVManager::createLogicalView(StringBuffer &token, const char * logicalFile, const char * cluster, unsigned rowsPerPage)
{
    token.clear();
    
    if (rsFactory_)
    {
        Owned<INewResultSet> rs = rsFactory_->createNewFileResultSet(logicalFile, cluster);
        IResultSetCursor * cursor = rs->createCursor();
        if (cursor!=NULL)
        {
            CLogicalView *view = new CLogicalView(cursor, logicalFile, rowsPerPage);
            
            if (view)
            {
                views_.append(*view);
                createLVToken(token, view->getId(), view->getRowsPerPage(), rs->supportsRandomSeek(), logicalFile, cluster);
                return view;
            }
        }
    }
    
    return NULL;
}

bool LVManager::deleteLogicalView(const StringBuffer &strToken)
{
    LVToken token(strToken.str());

    ForEachItemIn(idx, views_)
    {
        CLogicalView &view = views_.item(idx);
        
        if (view.getId() == token.getId())
        {
            views_.remove(idx);
            delete &view;
            return true;
        }
    }

    return false;
}

//  ===============================================================================================

CLogicalView::CLogicalView(IResultSetCursor *trs, const char * label, unsigned rowsPerPage)
{
    rs_=trs; 
    rsmd_ = &rs_->queryResultSet()->getMetaData();
    id_ = ++idgen_; 
    lastaccess_=time(NULL);
    rowsPerPage_=rowsPerPage;
    label_.append(label);
}

CLogicalView::~CLogicalView()
{
    if (rs_) 
        rs_->Release();
}

CLogicalView *LVManager::queryLogicalView(const StringBuffer &strToken)
{
    LVToken token(strToken.str());

    CLogicalView *lv = NULL;
    ForEachItemIn(idx, views_)
    {
        CLogicalView &view = views_.item(idx);
        
        if (view.getId() == token.getId())
        {
            view.updateLastAccessTime();
            lv = &view;
            break;
        }
    }

    return lv;
}

/*
interface IResultSetMetaData : extends IInterface
{
    virtual int getColumnCount() const = 0;
    virtual DisplayType getColumnDisplayType(int column) const = 0;
    virtual IStringVal & getColumnLabel(IStringVal & s, int column) const = 0;
    virtual IStringVal & getColumnEclType(IStringVal & s, int column) const = 0;
    virtual IStringVal & getColumnXmlType(IStringVal & s, int column) const = 0;
    virtual bool isSigned(int column) const = 0;
    virtual bool isEBCDIC(int column) const = 0;
    virtual bool isBigEndian(int column) const = 0;
    virtual unsigned getColumnRawType(int column) const = 0;
    virtual unsigned getColumnRawSize(int column) const = 0;
};
*/
const char * CLogicalView::getXsd()
{
    if (xsd_.length() == 0)
    {
        Owned<IPropertyTree> dataset = createPTree("DataSet", true);
        dataset->setProp("@xmlns", "urn:hpccsystems:ws:dfu");

        IPropertyTree * schema = createPTree("xsd:schema", true);
        schema->setProp("@id", "DFUDataSet");
//      schema->setProp("@targetNamespace", "http://tempuri.org/colTotals.xsd");
//      schema->setProp("@elementFormDefault", "qualified");
//      schema->setProp("@attributeFormDefault", "qualified");
        schema->setProp("@xmlns", "");
//      schema->setProp("@xmlns:mstns", "http://tempuri.org/colTotals.xsd");
        schema->setProp("@xmlns:xsd", "http://www.w3.org/2001/XMLSchema");
        schema->setProp("@xmlns:msdata", "urn:schemas-microsoft-com:xml-msdata");

        IPropertyTree * element = createPTree("xsd:element", true);
        element->setProp("@name", "DFUDataSet");
        element->setProp("@msdata:IsDataSet", "true");

        IPropertyTree * complexType = createPTree("xsd:complexType", true);

        IPropertyTree * choice = createPTree("xsd:choice", true);
        choice->setProp("@maxOccurs", "unbounded");

        IPropertyTree * element2 = createPTree("xsd:element", true);
        element2->setProp("@name", label_.str());

        IPropertyTree * complexType2 = createPTree("xsd:complexType", true);

        IPropertyTree * sequence = createPTree("xsd:sequence", true);

        for (int i = 0; i < rsmd_->getColumnCount(); ++i)
        {
            IPropertyTree * element3 = createPTree("xsd:element", true);
            CStringVal label, type;
            rsmd_->getColumnLabel(label, i);
            element3->setProp("@name", label.str());
            rsmd_->getColumnXmlType(type, i);
            element3->setProp("@type", type.str());
            element3->setProp("@minOccurs", "0");
            sequence->addPropTree("xsd:element", element3);
        }

        complexType2->addPropTree("xsd:sequence", sequence);
        element2->addPropTree("xsd:complexType", complexType2);
        choice->addPropTree("xsd:element", element2);
        complexType->addPropTree("xsd:choice", choice);
        element->addPropTree("xsd:complexType", complexType);
        schema->addPropTree("xsd:element", element);
        dataset->addPropTree("xsd:schema", schema);

        toXML(schema, xsd_);//, false, false);
    }
    return xsd_.str();
}

void CLogicalView::getPage(StringBuffer &page, StringBuffer &state, bool backFlag)
{
    Owned<IPropertyTree> dataset = createPTree("dataset", true);

    int prevState = atoi(state.str());
    if (rs_->absolute(prevState))
    {
        int pageSize = getRowsPerPage();
        while (pageSize--)
        {
            if (backFlag && rs_->isBeforeFirst())
                break;
            else if (!backFlag && rs_->isAfterLast())
                break;

            IPropertyTree *datarow = createPTree("table", true);
            for(int i = 0; i < rsmd_->getColumnCount(); ++i)
            {
                CStringVal label, val;
                rsmd_->getColumnLabel(label, i);
                rs_->getString(val, i);
                datarow->setProp(label.str(), val.str());
            }
            if (backFlag)
                rs_->previous();
            else
                rs_->next();
            dataset->addPropTree("table", datarow);
        }
    }
    StringBuffer xml;
    toXML(dataset, xml);

    page.clear();
    page.append(xml.str());

    state.clear();
    state.append(backFlag ? prevState - getRowsPerPage() : prevState + getRowsPerPage());
}
