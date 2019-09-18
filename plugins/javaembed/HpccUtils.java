/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems(R).

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

package com.HPCCSystems;

import java.util.*;
public class HpccUtils  implements Iterator, ActivityContext
{
    private long handle;
    private native static boolean _hasNext(long handle);
    private native static java.lang.Object _next(long handle);
    private native static boolean _isLocal(long handle);
    private native static int _numSlaves(long handle);
    private native static int _numStrands(long handle);
    private native static int _querySlave(long handle);
    private native static int _queryStrand(long handle);
    

    public HpccUtils(long _handle, String dllname)
    {
        System.load(dllname);
        handle = _handle;
    }
    public static void load(String dllname)
    {
        System.load(dllname);
    }
    public native void remove();
    public boolean hasNext()
    {
        return _hasNext(handle);
    }
    public java.lang.Object next() throws NoSuchElementException
    {
        java.lang.Object ret = _next(handle);
        if (ret == null)
           throw new NoSuchElementException();
        return ret;
    }
    public native static void log(String msg);
    
    public boolean isLocal() { return _isLocal(handle); }
    public int numSlaves() { return _numSlaves(handle); }
    public int numStrands() { return _numStrands(handle); }
    public int querySlave() { return _querySlave(handle); }
    public int queryStrand() { return _queryStrand(handle); }
    
}