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
    DOJO_OUTPUT,
    DEBUG_OUTPUT
};

class CComponenetSelectorDialog : public QDialog
{
    Q_OBJECT
public:
    CComponenetSelectorDialog(QWidget *parent, enum OUTPUT_TYPE outputType);

public Q_SLOTS:
    //virtual void saveQML();
    //virtual void saveDocBook();
    virtual void saveOutput();

private:

    enum OUTPUT_TYPE m_eOutputMode;

    //void writeQML(int idx);
    //void writeDocBook(int idx);
    void writeOutput(int idx);


    QLabel *label;
    QPushButton *saveButton;
    QPushButton *cancelButton;
    QListWidget *listWidget;
};

#endif // _COMPONENT_SELECTOR_DIALOG_H_
