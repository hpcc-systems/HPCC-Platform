/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems®.

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

#include "AppData.hpp"
#include "jlog.hpp"
#include <cassert>

// TODO: REMOVE REFERENCES TO SCHEMA
#include "SchemaCommon.hpp"
//

static const int TOP_LEVEL = 1;
static void* P_TOP_LEVEL = (void*)(&TOP_LEVEL);

//#define LOG_DATA_CALL
//#define LOG_ROW_COUNT_CALL
//#define LOG_PARENT_CALL
//#define LOG_INDEX_CALL

TableDataModel::TableDataModel()
{
}

int TableDataModel::rowCount(const QModelIndex& /*parent*/) const
{
    int nCount = CONFIGURATOR_API::getNumberOfRows(this->m_pActiveTable);
    return nCount;
}

int TableDataModel::columnCount(const QModelIndex & /*parent*/) const
{
    return 1;
}

QVariant TableDataModel::data(const QModelIndex & index, int role) const
{
    QHash<int, QByteArray> Roles = roleNames();

    const char *pValue  = CONFIGURATOR_API::getTableValue(Roles.value(role), index.row()+1);

    if (STRICTNESS_LEVEL >= DEFAULT_STRICTNESS)
        assert(pValue != NULL);

    return QString(pValue);
}

QHash<int, QByteArray> TableDataModel::roleNames() const
{
    static QHash<int, QByteArray> Roles;

    int nColumns = CONFIGURATOR_API::getNumberOfUniqueColumns();

    for (int idx = 0; idx < nColumns; idx++)
    {
        const char *pRole =  CONFIGURATOR_API::getColumnName(idx);
        Roles[Qt::UserRole + idx+1] = pRole;
    }
    return Roles;
}

ComponentDataModel::ComponentDataModel( QObject *parent) : QAbstractItemModel(parent)
{
}

ComponentDataModel::~ComponentDataModel()
{
}

int ComponentDataModel::columnCount(const QModelIndex &/*parent*/) const
{
    return 1;
}


QVariant ComponentDataModel::data(const QModelIndex &index, int role) const
{

    if (index.isValid() == false)
    {
#ifdef LOG_DATA_CALL
        PROGLOG("Function: %s() at %s:%d", __func__, __FILE__, __LINE__);
        PROGLOG("\tindex.row() = %d index.column() = %d index.internalPointer() = %p", index.row(), index.column(), index.internalPointer());
        PROGLOG("\tindex.isValid() == false");
#endif // LOG_DATA_CALL
        return QVariant();
    }

    if (Qt::DisplayRole != role)
        return QVariant();

    if (index.column() != 0)
        assert(false);

    const char *pName = CONFIGURATOR_API::getData(index.internalPointer());

#ifdef LOG_DATA_CALL
    PROGLOG("Function: %s() at %s:%d", __func__, __FILE__, __LINE__);
    PROGLOG("\tindex.row() = %d index.column() = %d index.internalPointer() = %p", index.row(), index.column(), index.internalPointer());
    PROGLOG("\tpName = %s", pName);
#endif // LOG_DATA_CALL

    if (pName == NULL)
    {
        assert(false);
        return QVariant();
    }
    return QString(pName);
}

Qt::ItemFlags ComponentDataModel::flags(const QModelIndex &index) const
{
    if (index.isValid() == false)
        return 0;
    return Qt::ItemIsEnabled | Qt::ItemIsSelectable;
}

QVariant ComponentDataModel::headerData(int section, Qt::Orientation orientation, int role) const
{
    if (role == Qt::DisplayRole)
        return QString("Components");
    return QAbstractItemModel::headerData(section, orientation, role);
}

QModelIndex ComponentDataModel::index(int row, int column, const QModelIndex & parent) const
{
    assert(column == 0);
    if (hasIndex(row, column, parent) == false)
        return QModelIndex();

#ifdef LOG_INDEX_CALL
    PROGLOG("Function: %s() at %s:%d", __func__, __FILE__, __LINE__);
    PROGLOG("\trow = %d parent.row() = %d parent.column() = %d parent.internalPointer() = %p (%s) number_of_children = %d", row, parent.row(), parent.column(), parent.internalPointer(),\
                CONFIGURATOR_API::getData(parent.internalPointer()), parent.isValid() ? CONFIGURATOR_API::getNumberOfChildren(parent.internalPointer()) : 0);
#endif // LOG_INDEX_CALL

    void *pParentNode = NULL;

    if (parent.isValid() == false)
    {
#ifdef LOG_INDEX_CALL
          PROGLOG("\tparent.isValid = false");
#endif // LOG_INDEX_CALL
        //return createIndex(row, column, CONFIGURATOR_API::getChild(CONFIGURATOR_API::getRootNode(), row));
        pParentNode = CONFIGURATOR_API::getModel();
    }
    else
    {
#ifdef LOG_INDEX_CALL
          PROGLOG("\tparent.isValid = true");
#endif // LOG_INDEX_CALL
        pParentNode = parent.internalPointer();
    }

    assert(pParentNode != NULL);
    void *pChildNode = CONFIGURATOR_API::getChild(pParentNode, row);

    if (pChildNode != NULL)
    {
#ifdef LOG_INDEX_CALL
        PROGLOG("\tcreating index for row=%d and pChildNode=%p for parent %s and child %s", row, pChildNode,\
                CONFIGURATOR_API::getData(pParentNode), CONFIGURATOR_API::getData(pChildNode));
#endif // LOG_INDEX_CALL
        return createIndex(row, column, pChildNode);

    }
    else
    {
#ifdef LOG_INDEX_CALL
        PROGLOG("\tcreating invalid index for row=%d for parent %s", row, CONFIGURATOR_API::getData(parent.internalPointer()));
#endif // LOG_INDEX_CALL
        return QModelIndex();
    }
}

QModelIndex ComponentDataModel::parent(const QModelIndex & index) const
{
#ifdef LOG_PARENT_CALL
    PROGLOG("Function: %s() at %s:%d", __func__, __FILE__, __LINE__);
    PROGLOG("\tindex.row() = %d index.column() = %d index.internalPointer() = %p (%s) number_of_children = %d", index.row(), index.column(), index.internalPointer(),\
                CONFIGURATOR_API::getData(index.internalPointer()), CONFIGURATOR_API::getNumberOfChildren(index.internalPointer()));
#endif // LOG_PARENT_CALL

    if (index.isValid() == false)
        return QModelIndex();

    void *pChildNode = index.internalPointer();
#ifdef LOG_PARENT_CALL
    if (pChildNode != CONFIGURATOR_API::getModel())
    {
        PROGLOG("\tpChildNode is %s", CONFIGURATOR_API::getData(pChildNode));
    }
    else
    {
        PROGLOG("\tpChildNode (%p) is model pointer", pChildNode);
    }
#endif
    void *pParentNode = CONFIGURATOR_API::getParent(pChildNode);

    if (pParentNode == CONFIGURATOR_API::getModel())
        return QModelIndex();

    if (pParentNode == NULL)
    {
        assert(false);
        return QModelIndex();
    }
    int nParentRow = CONFIGURATOR_API::getIndexFromParent(pParentNode);
#ifdef LOG_PARENT_CALL
    PROGLOG("\tcalling createIndex(%d, 0, %p) for %s", nParentRow, pParentNode, CONFIGURATOR_API::getData(pParentNode));
#endif // LOG_PARENT_CALL
    return createIndex(nParentRow, 0, pParentNode);
}

int ComponentDataModel::rowCount(const QModelIndex &parent) const
{
#ifdef LOG_ROW_COUNT_CALL
    PROGLOG("Function: %s() at %s:%d", __func__, __FILE__, __LINE__);
#endif // LOG_ROW_COUNT_CALL

    int nRetVal = 0;

    if (parent.column() > 0)
        return 0;

    void *pParentNode = NULL;

    if (parent.isValid() == false)
    {
#ifdef LOG_ROW_COUNT_CALL
        PROGLOG("\tusing root node");
#endif // LOG_ROW_COUNT_CALL
        pParentNode = CONFIGURATOR_API::getModel();
        //return 1;
    }
    else
        pParentNode = parent.internalPointer();

    nRetVal = CONFIGURATOR_API::getNumberOfChildren(pParentNode);

#ifdef LOG_ROW_COUNT_CALL
    PROGLOG("\tparent.row() = %d parent.column() = %d pParentNode = %p (%s) number_of_children = %d", parent.row(), parent.column(), pParentNode,\
                CONFIGURATOR_API::getData(pParentNode), CONFIGURATOR_API::getNumberOfChildren(pParentNode));
#endif
    return nRetVal;
}
