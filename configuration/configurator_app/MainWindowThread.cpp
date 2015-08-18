#include "MainWindowThread.hpp"
#include "MainWindow.hpp"
#include <QThread>
#include <QApplication>

MainWindowThread::MainWindowThread( MainWindow* pMW )
{
    m_pMainWindow = pMW;
}

void MainWindowThread::SetApplication (QApplication *pApplication)
{
    this->m_pApplication = pApplication;
}

void MainWindowThread::run()
{
    //int nCount = 0;
    //QApplication app(nCount, NULL);

    this->m_pMainWindow->show();

    //this->m_pApplication->exec();
}
