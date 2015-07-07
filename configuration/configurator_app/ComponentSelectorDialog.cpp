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

#include <QtWidgets>
#include <QMessageBox>
#include "ComponentSelectorDialog.h"
#include "ConfiguratorAPI.hpp"

CComponenetSelectorDialog::CComponenetSelectorDialog(QWidget *parent, enum OUTPUT_TYPE outputType)
    : QDialog(parent), m_eOutputMode(outputType)
{
        label = new QLabel(tr("Component List:"));
        saveButton = new QPushButton(tr("&Save"));
        cancelButton = new QPushButton(tr("&Cancel"));
        listWidget = new QListWidget();

        int nCount = CONFIGURATOR_API::getNumberOfAvailableComponents();
        for (int idx = 0; idx < nCount; idx++)
        {
            listWidget->addItem(const_cast<char*>(CONFIGURATOR_API::getComponentName(idx)));
        }

        QVBoxLayout *extensionLayout = new QVBoxLayout;
        extensionLayout->setMargin(0);
        QHBoxLayout *topLeftLayout = new QHBoxLayout;
        topLeftLayout->addWidget(label);
        QVBoxLayout *leftLayout = new QVBoxLayout;
        leftLayout->addLayout(topLeftLayout);

        QGridLayout *mainLayout = new QGridLayout;
        mainLayout->addLayout(leftLayout, 0, 0);
        mainLayout->addWidget(listWidget, 0, 0, 1, 2);
        mainLayout->addWidget(saveButton, 1, 0);
        mainLayout->addWidget(cancelButton, 1, 1);
        extensionLayout->addWidget(listWidget);
        setLayout(mainLayout);

        if (m_eOutputMode == QML_OUTPUT)
            setWindowTitle(tr("Select component for QML generation"));
        else if (m_eOutputMode == DOCBOOK_OUTPUT)
            setWindowTitle(tr("Select component for DocBook generation"));
        connect(saveButton,     SIGNAL(pressed()),  this,   SLOT(saveOutput()));
        connect(cancelButton,   SIGNAL(pressed()),  this,   SLOT(reject()));
}

void CComponenetSelectorDialog::writeOutput(int idx)
{
    QFileDialog dialog(this);
    dialog.setFileMode(QFileDialog::AnyFile);

    QString qstrFileName;

    if (m_eOutputMode == QML_OUTPUT)
        qstrFileName = dialog.getSaveFileName(this, tr("Save QML File"), "/tmp",tr("QML (*.qml)"));
    else if (m_eOutputMode == DOCBOOK_OUTPUT)
        qstrFileName = dialog.getSaveFileName(this, tr("Save DocBook File"), "/tmp",tr("XML (*.xml)"));

    const char *pFileName = qstrFileName.toLocal8Bit().data();
    qDebug() << pFileName;

    QFile::remove(qstrFileName);
    QFile qFile(qstrFileName.toLocal8Bit().data());

    if (qFile.open(QIODevice::WriteOnly | QIODevice::Truncate) == 0)
        return;

    QTextStream out(&qFile);
    char *pOutput = NULL;

    try
    {
        if (m_eOutputMode == QML_OUTPUT)
        {
            pOutput = (char*)(malloc(sizeof(char)));
            CONFIGURATOR_API::getQMLByIndex(idx, pOutput);
            out << pOutput;
        }
        else if (m_eOutputMode == DOCBOOK_OUTPUT)
        {
            pOutput = const_cast<char*>(CONFIGURATOR_API::getDocBookByIndex(idx));
            out << pOutput;
        }
        qFile.close();

        if (pOutput != NULL && *pOutput != 0)
            QMessageBox::information( this, tr("Success"), tr("Completed with no errors!") );
        else
            QMessageBox::warning( this, tr("Failure"), tr("Failed.  Nothing generated!") );
    }
    catch (...)
    {
        QMessageBox::warning( this, tr("Exception"), tr("Failed.  Exception thrown!") );
    }
    done(0);
}

void CComponenetSelectorDialog::saveOutput()
{
    writeOutput(listWidget->currentRow());
}
