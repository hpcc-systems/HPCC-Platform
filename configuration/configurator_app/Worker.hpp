#ifndef _WORKER_HPP_
#define _WORKER_HPP_

#include <QObject>
#include <QMutex>

class CWorker : public QObject
{
    Q_OBJECT
public:

    explicit CWorker(QObject *parent = 0);

    void requestWork();

    void abort();

private:

    bool m_bAbort;
    bool m_bWorking;

    QMutex m_mutex;

signals:

    void workRequested();

    void valueChanged();

    void finished();

public slots:

    void doWork();

};

#endif // _WORKER_HPP_
