#include "Worker.hpp"
#include <QEventLoop>
#include <QTimer>

CWorker::CWorker(QObject *parent) : m_bAbort(false), m_bWorking(false),
    QObject(parent)
{    
}

void CWorker::requestWork()
{
    m_mutex.lock();

    m_bWorking = true;
    m_bAbort = false;

    m_mutex.unlock();

    emit workRequested();
}

void CWorker::abort()
{
    m_mutex.lock();

    if (m_bWorking == true)
    {
        m_bAbort = true;
    }

    m_mutex.unlock();
}

void CWorker::doWork()
{
    for (int i = 0; i < 2; i ++)
    {
        m_mutex.lock();

        bool abort = m_bAbort;

        m_mutex.unlock();

        if (abort)
        {
            break;
        }

        QEventLoop loop;
        QTimer::singleShot(1000, &loop, SLOT(quit()));
        loop.exec();

        //emit valueChanged(QString::number(i));
      }

      m_mutex.lock();
      m_bWorking = false;
      m_mutex.unlock();

      emit finished();
}
