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

#ifndef _COMPONENT_SELECTOR_DIALOG_H_
#define _COMPONENT_SELECTOR_DIALOG_H_

#include <QDialog>

QT_BEGIN_NAMESPACE
class QDialogButtonBox;
class QLabel;
class QPushButton;
class QWidget;
class QListWidget;
QT_END_NAMESPACE

enum OUTPUT_TYPE
{
    QML_OUTPUT = 0,
    DOCBOOK_OUTPUT,
    DEBUG_OUTPUT
};

class CComponenetSelectorDialog : public QDialog
{
    Q_OBJECT
public:
    CComponenetSelectorDialog(QWidget *parent, enum OUTPUT_TYPE outputType);

public Q_SLOTS:
    virtual void saveOutput();

private:

    enum OUTPUT_TYPE m_eOutputMode;
    void writeOutput(int idx);

    QLabel *label;
    QPushButton *saveButton;
    QPushButton *cancelButton;
    QListWidget *listWidget;
};

#endif // _COMPONENT_SELECTOR_DIALOG_H_
