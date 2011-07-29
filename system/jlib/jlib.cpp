/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */


#include "jlib.hpp"
#include "jmisc.hpp"
#include <assert.h>
#include <time.h>

static InitTable *_initTable = NULL;
static DynamicScopeCtx *dynamicScopeCtx = NULL;

InitTable *queryInitTable() { if (!_initTable) _initTable = new InitTable(); return _initTable; }


void ExitModuleObjects(SoContext ctx)
{
    if (_initTable)
    {
        if (ctx)
            _initTable->exit(ctx);
        else
        {
            InitTable *t = _initTable;
            _initTable = NULL;
            t->exit(ctx);
            delete t;
        }
    }
}

void ExitModuleObjects() { ExitModuleObjects(0); }

void _InitModuleObjects()
{
    queryInitTable()->init(0);
}

InitTable::InitTable()
{
}

void InitTable::init(SoContext ctx)
{
    initializers.sort(&sortFuncDescending);
    ForEachItemIn(i, initializers)
    {
        InitializerType &iT = initializers.item(i);
//      printf("module initialization %d:%d\n", iT.modpriority, iT.priority);

        if (iT.soCtx == ctx && iT.state == ITS_Uninitialized && iT.initFunc)
        {
            bool res;
            try
            {
                res = iT.initFunc();
                iT.state = ITS_Initialized;
            }
            catch (...)
            {
                printf("module initialization %d:%d threw exception\n", iT.modpriority,iT.priority);
                throw;
            }
            if (!res)
            {
                printf("module initialization %d:%d failed\n", iT.modpriority,iT.priority);
                assertex(res);
            }
        }
    }
}

void InitTable::exit(SoContext ctx)
{
    initializers.sort(&sortFuncDescending);  // NB read in reverse so no need to sort ascending
    ForEachItemInRev(i, initializers)
    {
        InitializerType &iT = initializers.item(i);
        if (iT.soCtx == ctx)
        {
            assertex(iT.state == ITS_Initialized);

            if (iT.modExit && iT.modExit->func)
            {
                //printf("::exit - destroying prior = %d\n", iT.priority);
                iT.modExit->func();
                iT.modExit->func = NULL;
            }
            initializers.remove(i);
        }
    }
}
    
int InitTable::sortFuncDescending(InitializerType *i1, InitializerType *i2)
{
    int ret = i2->modpriority - i1->modpriority;
    return ret ? ret : i2->priority - i1->priority;
}

int InitTable::sortFuncAscending(InitializerType *i1, InitializerType *i2)
{
    int ret = i1->modpriority - i2->modpriority;
    return ret ? ret : i1->priority - i2->priority;
}

void InitTable::add(InitializerType &iT)
{
    initializers.append(iT);
}

void InitTable::add(boolFunc func, unsigned int priority, unsigned int modpriority, byte state)
{
    InitializerType iT;
    iT.initFunc = func;
    iT.modExit = NULL;
    iT.priority = priority;
    iT.modpriority = modpriority;
    iT.soCtx = (SoContext)0;
    iT.state = state;
    initializers.append(iT);
}

DynamicScopeCtx::DynamicScopeCtx() : soCtx(0)
{
    dynamicScopeCtx = this;
}

DynamicScopeCtx::~DynamicScopeCtx()
{
    if (soCtx)
    {
        // initliaze those in this context
        initTable.init();
        // add to global table with patched ctx's, for use by ExitModuleObjects(ctx) call
        ForEachItemIn(i, initTable.initializers)
        {
            InitializerType &iT = initTable.initializers.item(i);
            iT.soCtx = soCtx;
            queryInitTable()->add(iT);
        }
    }
    dynamicScopeCtx = NULL;
}

ModInit::ModInit(boolFunc func, unsigned int priority, unsigned int modpriority)
{
    InitTable *initTable = dynamicScopeCtx ? &dynamicScopeCtx->initTable : queryInitTable();
#if !defined(EXPLICIT_INIT)
    initTable->add(NULL, priority, modpriority, ITS_Initialized);
    func();
#else
    initTable->add(func, priority, modpriority);
#endif
}

ModExit::ModExit(voidFunc _func)
{
// assumes modinit preceeded this call
    InitTable *initTable = dynamicScopeCtx ? &dynamicScopeCtx->initTable : queryInitTable();
    assertex(initTable->items());

    func = _func;

    InitializerType &iT = initTable->initializers.tos();
    assertex(!iT.modExit);
    iT.modExit = this;
}

#if !defined(EXPLICIT_INIT)
ModExit::~ModExit()
{
    if (_initTable)
    {
        if (func)
        {
            try { func(); }
            catch (...)
            {
                printf("Non-prioritised (ExitModuleObjects was not called) module object destruction failure. Ignoring.\n");
            }
            func = NULL;
        }
    }
}
#endif

// check that everything compiles correctly
#if 0

typedef MapBetween<LinkedIInterface, IInterfacePtr, LinkedIInterface, IInterfacePtr> MapIItoII;

MapIItoII map;

void f(IInterface * i1, IInterface * i2)
{
  map.setValue(i1, i2);
}


//==========================================================================

class MappingStringToCInterface : public MappingStringTo<CInterfacePtr,CInterfacePtr>
{
public:
  MappingStringToCInterface(const char * k, CInterfacePtr a) :
      MappingStringTo<CInterfacePtr,CInterfacePtr>(k,a) {}
  ~MappingStringToCInterface() { ::Release(val); }
};

typedef MapStringTo<CInterfacePtr,CInterfacePtr,MappingStringToCInterface> MapStringToCInterface;

void test()
{
  MapStringToCInterface   map;

  CInterface * temp = new CInterface;
  map.setValue("gavin", temp);
  
  temp->Link();
  map.setValue("richard", temp);

  map.remove("gavin");
  map.remove("richard");
}

//==========================================================================

void test2(void)
{
  MapUnsignedToInt    map1;
  MapStrToUnsigned    map2;
  map1.setValue(1, 2);
  map1.setValue(2, 3);

  map2.setValue("gavin",30);
  map2.setValue("liz",29);

  assertex(*map1.getValue(1) == 2);
  assertex(*map1.getValue(2) == 3);

  assertex(*map2.getValue("gavin") == 30);
  assertex(*map2.getValue("liz") == 29);
  test();
};


#ifndef USING_MPATROL
#ifdef _WIN32
#define _CRTDBG_MAP_ALLOC
#include <crtdbg.h>
#endif
#endif


int main(void)
{
#ifdef _DEBUG
#ifdef _WIN32

  _CrtSetReportMode(_CRT_WARN, _CRTDBG_MODE_FILE);
  _CrtSetReportFile(_CRT_WARN, _CRTDBG_FILE_STDOUT);
  _CrtSetReportMode(_CRT_ERROR, _CRTDBG_MODE_FILE);
  _CrtSetReportFile(_CRT_ERROR, _CRTDBG_FILE_STDOUT);
  _CrtSetReportMode(_CRT_ASSERT, _CRTDBG_MODE_FILE);
  _CrtSetReportFile(_CRT_ASSERT, _CRTDBG_FILE_STDOUT);

  _CrtMemState oldMemState, newMemState, diffMemState;
  _CrtMemCheckpoint(&oldMemState);

#endif
#endif

  test();
  test2();

#ifdef _DEBUG
#ifdef _WIN32
#ifndef USING_MPATROL //using mpatrol memory leak tool
  _CrtMemCheckpoint(&newMemState);
  if(_CrtMemDifference(&diffMemState, &oldMemState, &newMemState))
  {
    _CrtMemDumpAllObjectsSince(&diffMemState);
  }
#endif //USING_MPATROL
#endif
#endif

  return 0;
}
#endif
