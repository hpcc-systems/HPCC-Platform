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

#ifndef MAINWINDOW_HPP
#define MAINWINDOW_HPP

#include <QMainWindow>
#include <QThread>
#include <QString>
#include "Worker.hpp"
#include "AppData.hpp"

class QQuickView;
class ApplicationData;
class TableDataModel;
class ComponentDataModel;

namespace Ui {
class MainWindow;
}

class MainWindow : public QMainWindow
{
    Q_OBJECT

public:
    explicit MainWindow(QWidget *parent = 0);

    ~MainWindow();

    void addComponentToList(char *pComponent);
    void addServiceToList(char *pService);

private:

    bool getRegenerateQML() const
    {
       return m_bRegenerateQML;
    }

private:
    Ui::MainWindow *ui;
    QQuickView *m_pView;
    QString m_envFile;
    ApplicationData *m_pAppData;
    TableDataModel *m_pTableDataModel;
    ComponentDataModel *m_pComponentDataModel;

    bool m_bRegenerateQML;

private slots:
    void on_actionOpen_triggered();
    void on_treeView_clicked(const QModelIndex &index);
    void on_actionReload_triggered();
    void on_actionGenerate_QML_triggered();
    void on_actionGenerate_DockBook_triggered();
    void on_actionGenerate_Internal_Data_Structure_Debug_triggered();
    void on_actionRegenerate_QML_toggled(bool arg1);
    void on_actionSave_triggered();
};

#endif // MAINWINDOW_HPP
