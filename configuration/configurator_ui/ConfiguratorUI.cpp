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

#include <QtQuick/QQuickView>
#include <QGuiApplication>
#include <QQmlContext>
#include <QObject>
#include <QtQuick/QQuickView>
#include <QGuiApplication>
#include <QString>
#include "ConfiguratorUI.hpp"
#include "AppData.hpp"
#include "../configurator/ConfiguratorMain.hpp"
#include <cassert>

int main2(int argc, char *argv[])
{
    QGuiApplication app(argc, argv);
    QQuickView view;
    ApplicationData data;
    int nTables = CONFIGURATOR_API::getNumberOfTables();
    TableDataModel tableDataModel[MAX_ARRAY_X];

    assert(nTables <= MAX_ARRAY_X);

    view.rootContext()->setContextProperty("ApplicationData", &data);

    for (int idx = 0; idx < MAX_ARRAY_X; idx++)
    {
        view.rootContext()->setContextProperty(getTableDataModelName(idx), &(tableDataModel[idx]));
    }

    view.setResizeMode(QQuickView::SizeRootObjectToView);
    view.setSource(QUrl::fromLocalFile(*argv));
    view.show();

    int retVal = app.exec();

    deleteTableModels();
    return retVal;
}

extern "C" void StartQMLUI(char* pQMLFile)
{
    main2(1,&pQMLFile);
}
