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
        m_bAbort = true;

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
            break;

        QEventLoop loop;
        QTimer::singleShot(1000, &loop, SLOT(quit()));
        loop.exec();
      }

      m_mutex.lock();
      m_bWorking = false;
      m_mutex.unlock();

      emit finished();
}
