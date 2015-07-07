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

#ifndef _APP_DATA_HPP_
#define _APP_DATA_HPP_

#include <QObject>
#include <QtQuick/QQuickView>
#include <QGuiApplication>
#include <QDateTime>
#include <QString>
#include <QAbstractListModel>
#include "ConfiguratorAPI.hpp"

#define BUFFER_SIZE 2048

class TableDataModel : public QAbstractListModel
{
    Q_OBJECT

public:

    TableDataModel();

    int rowCount(const QModelIndex & parent = QModelIndex()) const;
    int columnCount(const QModelIndex & parent = QModelIndex()) const;
    QVariant data(const QModelIndex & index, int role = Qt::DisplayRole) const;
    QString getActiveTable()
    {
        return m_qstrActiveTable;
    }
    Q_INVOKABLE void setActiveTable(const QString &XPath)
    {
        m_qstrActiveTable = XPath;
        strncpy(m_pActiveTable, XPath.toStdString().c_str(), XPath.length());
        m_pActiveTable[XPath.length()] = 0;
    }

protected:
    QHash<int, QByteArray> roleNames() const;

private:
    QString m_qstrActiveTable;
    char m_pActiveTable[BUFFER_SIZE];
};

class ApplicationData : public QObject
{
    Q_OBJECT
public:
    Q_INVOKABLE QString getValue(QString XPath)
    {
        char pValue[BUFFER_SIZE];

        if (CONFIGURATOR_API::getValue(XPath.toStdString().c_str(), pValue) )
            return pValue;
        else
            return "";
    }

    Q_INVOKABLE void setValue(QString XPath, QString qstrNewValue)
    {
        CONFIGURATOR_API::setValue(XPath.toStdString().c_str(), qstrNewValue.toStdString().c_str());
    }

    Q_INVOKABLE int getIndex(QString XPath)
    {
        return CONFIGURATOR_API::getIndex(XPath.toStdString().c_str());
    }

    Q_INVOKABLE void setIndex(QString XPath, int index)
    {
        return CONFIGURATOR_API::setIndex(XPath.toStdString().c_str(), index);
    }
};

class ComponentDataModel : public QAbstractItemModel
{
    Q_OBJECT

public:

    ComponentDataModel( QObject *parent = NULL);

    virtual ~ComponentDataModel();
    int rowCount(const QModelIndex & parent = QModelIndex()) const;
    int columnCount(const QModelIndex & parent = QModelIndex()) const;
    QModelIndex parent(const QModelIndex & index) const;
    QModelIndex index(int row, int column, const QModelIndex & parent = QModelIndex()) const;
    QVariant data(const QModelIndex & index, int role = Qt::DisplayRole) const;
    Qt::ItemFlags flags(const QModelIndex &index) const;
    QVariant headerData(int section, Qt::Orientation orientation, int role = Qt::DisplayRole) const;
};

#endif // _APP_DATA_HPP_
