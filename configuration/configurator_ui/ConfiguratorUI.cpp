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
        view.rootContext()->setContextProperty(modelNames[idx], &(tableDataModel[idx]));
    }

    view.setResizeMode(QQuickView::SizeRootObjectToView);
    view.setSource(QUrl::fromLocalFile(*argv));
    view.show();

    return app.exec();
}

extern "C" void StartQMLUI(char* pQMLFile)
{
    main2(1,&pQMLFile);
}
