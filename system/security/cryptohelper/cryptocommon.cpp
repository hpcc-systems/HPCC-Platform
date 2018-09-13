/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.

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

#if defined(_USE_OPENSSL) && !defined(_WIN32)

#include <memory>
#include "jliball.hpp"
#include <openssl/pem.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/ssl.h>
#include "cryptocommon.hpp"


#if OPENSSL_VERSION_NUMBER < 0x10100000L
static unsigned numCryptoLocks = 0;
static std::vector<std::unique_ptr<CriticalSection>> cryptoLocks;

static void locking_function(int mode, int n, const char * file, int line)
{
    assertex(n < (int)numCryptoLocks);
    if (mode & CRYPTO_LOCK)
        cryptoLocks[n]->enter();
    else
        cryptoLocks[n]->leave();
}

static unsigned long pthreads_thread_id()
{
    return (unsigned long)GetCurrentThreadId();
}

static void installCallbacks()
{
    if (!CRYPTO_get_locking_callback())
    {
        numCryptoLocks = CRYPTO_num_locks();
        for (unsigned i=0; i<numCryptoLocks; i++)
            cryptoLocks.push_back(std::unique_ptr<CriticalSection>(new CriticalSection));
        CRYPTO_set_locking_callback(locking_function);
    }
#ifndef _WIN32
    if (!CRYPTO_get_id_callback())
        CRYPTO_set_id_callback((unsigned long (*)())pthreads_thread_id);
#endif
}
#endif

static void initSSLLibrary()
{
    SSL_load_error_strings();
    SSLeay_add_ssl_algorithms();
    OpenSSL_add_all_algorithms();
#if OPENSSL_VERSION_NUMBER < 0x10100000L
    installCallbacks();
#endif
}


MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    initSSLLibrary();
    return true;
}
MODULE_EXIT()
{
}


namespace cryptohelper
{

static void populateError(StringBuffer &msg)
{
    // openssl doesn't define max error string size, but 1K will do
    char *evpErr = msg.reserve(1024);
    ERR_error_string_n(ERR_get_error(), evpErr, 1024);
    msg.setLength(strlen(evpErr));
    msg.append(" - ");
}

static IException *makeEVPExceptionVA(int code, const char *format, va_list args)  __attribute__((format(printf,2,0)));

IException *makeEVPExceptionVA(int code, const char *format, va_list args)
{
    StringBuffer message;
    populateError(message);
    message.valist_appendf(format, args);
    return makeStringException(code, message);
}

IException *makeEVPException(int code, const char *msg)
{
    StringBuffer message;
    populateError(message);
    message.append(msg);
    return makeStringException(code, message);
}

IException *makeEVPExceptionV(int code, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    IException *e = makeEVPExceptionVA(code, format, args);
    va_end(args);
    return e;
}

void throwEVPException(int code, const char *format)
{
    throw makeEVPException(code, format);
}

void throwEVPExceptionV(int code, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    IException *e = makeEVPExceptionVA(code, format, args);
    va_end(args);
    throw e;
}

} // end of namespace cryptohelper

#endif // #if defined(_USE_OPENSSL) && !defined(_WIN32)
