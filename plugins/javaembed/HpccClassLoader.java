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

package com.HPCCSystems;

import java.net.*;
import java.util.Hashtable;
public class HpccClassLoader extends java.net.URLClassLoader
{
    private long bytecode;
    private int bytecodeLen;
    private native Class defineClassForEmbed(int bytecodeLen, long bytecode, String name);
    private Hashtable<String, Class> classes = new Hashtable<>();
    private HpccClassLoader(java.net.URL [] urls, ClassLoader parent, int _bytecodeLen, long _bytecode, String dllname)
    {
        super(urls, parent);
        System.load(dllname);
        bytecodeLen = _bytecodeLen;
        bytecode = _bytecode;
    }
    public synchronized Class<?> findClass(String className) throws ClassNotFoundException
    {
        Class result = (Class) classes.get(className);
        if (result == null)
        {
            result = defineClassForEmbed(bytecodeLen, bytecode, className.replace(".","/"));
            classes.put(className, result);
        }
        return result; 
    }
    public static HpccClassLoader newInstance(java.net.URL [] urls, ClassLoader parent, int _bytecodeLen, long _bytecode, String dllname)
    {
        return new HpccClassLoader(urls, parent, _bytecodeLen, _bytecode, dllname); 
    }
}