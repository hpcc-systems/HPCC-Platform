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

/*
 * Implementation notes:
 * To make a generic 'language embedded API' we need:
 * 1. To make the BEGINC++ and ENDC++ take a parameter indicating the name of the embedder to use
 * 2. For each embedder to have a dll that exports functions to:
 *    a) generate the code to do a function call
 *      i) including generating the code for a parameter of various types
 *    b) extract and compile the language code and embed it into the workunit dll as a resource
 *    c) extract said resource from the workunit dll so that it can be used
 *    d) do any necessary startup/closedown that an engine using this embedded code would need
 * 3. We can shoot a bit lower and say that b/c above are up to the user to worry about, and all we
 *    want to support are a "lang=XXX" attribute on a service definition (requires only steps a and d above, I think)
 *    Might make sense to have two levels of external language support
 *
 *    I suspect that the plugins will need to be fairly tightly coupled to the code generator to achieve a, but will also need a runtime component for d
 */

#include "platform.h"
#include "jlib.hpp"

#include "javaembed.hpp"
#include <jni.h>

namespace javaembed {

static JavaVM *javaVM;       /* denotes a Java VM */
static JNIEnv *JNIenv;       /* pointer to native method interface */

JNIEnv *getJNIEnvironment()
{
    // MORE - should use a singletonlock
    if (!JNIenv)
    {
        JavaVMInitArgs vm_args; /* JDK/JRE 6 VM initialization arguments */
        JavaVMOption* options = new JavaVMOption[2];
        options[0].optionString = "-Djava.class.path=.";
//        options[1].optionString = "-verbose:jni";
        vm_args.version = JNI_VERSION_1_6;
        vm_args.nOptions = 1;
        vm_args.options = options;
        vm_args.ignoreUnrecognized = false;
        /* load and initialize a Java VM, return a JNI interface pointer in env */
        JNI_CreateJavaVM(&javaVM, (void**)&JNIenv, &vm_args);
        delete options;
    }
    return JNIenv;
}

extern JAVAEMBED_API void callMethod(const char *className, const char *methodName, const char *signature)
{
    JNIEnv *env = getJNIEnvironment();

    jclass cls = env->FindClass(className);
    jmethodID mid = env->GetStaticMethodID(cls, methodName, signature);
    jstring jstr = env->NewStringUTF(" from C!");
    jobjectArray args = env->NewObjectArray(1, env->FindClass("java/lang/String"), jstr);
    env->CallStaticVoidMethod(cls, mid, args);
}


MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    return true;
}

MODULE_EXIT()
{
    if (javaVM)
    {
        javaVM->DestroyJavaVM();
    }
}

} // namespace javaembed
