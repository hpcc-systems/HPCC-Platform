/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems.

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

#ifndef DYNSHARED_H
#define DYNSHARED_H

#include "jlib.hpp"
#include "jutil.hpp"
#include "jexcept.hpp"

class DynInit
{
public:
    DynInit(const char* _name=NULL) : name(_name)
    {
        init_s = false;
    }

    virtual ~DynInit() {}
    virtual void init() = 0;
    inline void checkInit()
    {
        if ( !init_s )
            throw MakeStringException(-1, "Class not initialized [%s].", name);
    }

    inline void setInit(bool status=false)
    {
        init_s=status;
    }

private:
    const char *name;

protected:
    bool init_s;

};

template <typename T>
class SharedObjectFunction : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;
    SharedObjectFunction(HINSTANCE hndl, const char* _funcName);
    T getFuncPtr();
    const char* getFuncName();

private:
    const char *funcName;
    T funcPtr;

    T GetFuncSym(HINSTANCE hndl, const char *_funcName);

};

template <typename T>
T SharedObjectFunction<T>::GetFuncSym(HINSTANCE hndl, const char *_funcName)
{
    return (T)GetSharedProcedure(hndl, _funcName);
}

template <typename T>
SharedObjectFunction<T>::SharedObjectFunction(HINSTANCE hndl, const char* _funcName) : funcName(_funcName)
{
    funcPtr = GetFuncSym(hndl, funcName);
}

template <typename T>
T SharedObjectFunction<T>::getFuncPtr()
{
    return funcPtr;
}

template <typename T>
const char*  SharedObjectFunction<T>::getFuncName()
{
    return funcName;
}

#endif
