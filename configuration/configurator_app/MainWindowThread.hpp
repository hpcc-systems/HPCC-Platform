#include <QThread>
class MainWindow;
class QApplication;


class MainWindowThread : public QThread
{
    public:
        void run();
        MainWindowThread( MainWindow* pMainWindow );
        void SetApplication (QApplication *pApplication);

     MainWindow * m_pMainWindow;
     QApplication *m_pApplication;
private:

};
