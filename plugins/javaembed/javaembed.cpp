/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

#include "platform.h"
#include <jni.h>
#include "jexcept.hpp"
#include "jthread.hpp"
#include "hqlplugins.hpp"
#include "deftype.hpp"
#include "eclrtl.hpp"
#include "eclrtl_imp.hpp"
#include "jprop.hpp"
#include "build-config.h"

#ifdef _WIN32
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

static const char * compatibleVersions[] = {
    "Java Embed Helper 1.0.0",
    NULL };

static const char *version = "Java Embed Helper 1.0.0";

extern "C" EXPORT bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb)
{
    if (pb->size == sizeof(ECLPluginDefinitionBlockEx))
    {
        ECLPluginDefinitionBlockEx * pbx = (ECLPluginDefinitionBlockEx *) pb;
        pbx->compatibleVersions = compatibleVersions;
    }
    else if (pb->size != sizeof(ECLPluginDefinitionBlock))
        return false;
    pb->magicVersion = PLUGIN_VERSION;
    pb->version = version;
    pb->moduleName = "java";
    pb->ECL = NULL;
    pb->flags = PLUGIN_MULTIPLE_VERSIONS;
    pb->description = "Java Embed Helper";
    return true;
}

namespace javaembed {

// Use a global object to ensure that the Java VM  is initialized once only.
// We would like to create it lazily for two reasons:
// 1. So that we only get a JVM if we need one (even if we have loaded the plugin)
// 2. It's important for the JVM to be initialized AFTER we have set up signal handlers, as it
//    likes to set its own (in particular, it seems to intercept and ignore some SIGSEGV during the
//    garbage collection).
// Unfortunately, it seems that the design of the JNI interface is such that JNI_CreateJavaVM has to be called on the 'main thread'.
// So we can't achieve 1, and 2 requires that we create via the INIT_MODULE mechanism (rather than just a static object), and that
// any engines that call InitModuleObjects() or load plugins dynamically do so AFTER setting any signal handlers or calling
// EnableSEHtoExceptionMapping
//

static class JavaGlobalState
{
public:
    JavaGlobalState()
    {
        JavaVMInitArgs vm_args; /* JDK/JRE 6 VM initialization arguments */

        StringArray optionStrings;
        const char* origPath = getenv("CLASSPATH");
        StringBuffer newPath;
        newPath.append("-Djava.class.path=");
        if (origPath && *origPath)
        {
            newPath.append(origPath).append(ENVSEPCHAR);
        }
        StringBuffer envConf;
        envConf.append(CONFIG_DIR).append(PATHSEPSTR).append("environment.conf");
        Owned<IProperties> conf = createProperties(envConf.str(), true);
        if (conf && conf->hasProp("classpath"))
        {
            conf->getProp("classpath", newPath);
            newPath.append(ENVSEPCHAR);
        }
        else
        {
            newPath.append(INSTALL_DIR).append(PATHSEPCHAR).append("classes").append(ENVSEPCHAR);
        }
        newPath.append(".");
        optionStrings.append(newPath);

        if (conf && conf->hasProp("jvmlibpath"))
        {
            StringBuffer libPath;
            libPath.append("-Djava.library.path=");
            conf->getProp("jvmlibpath", libPath);
            optionStrings.append(libPath);
        }

        if (conf && conf->hasProp("jvmoptions"))
        {
            optionStrings.appendList(conf->queryProp("jvmoptions"), ENVSEPSTR);
        }

        // Options we know we always want set
        optionStrings.append("-Xrs");

        // These may be useful for debugging
        // optionStrings.append("-Xcheck:jni");
        // optionStrings.append("-verbose:jni");

        JavaVMOption* options = new JavaVMOption[optionStrings.length()];
        ForEachItemIn(idx, optionStrings)
        {
            DBGLOG("javaembed: Setting JVM option: %s",(char *)optionStrings.item(idx));
            options[idx].optionString = (char *) optionStrings.item(idx);
            options[idx].extraInfo = NULL;
        }
        vm_args.nOptions = optionStrings.length();
        vm_args.options = options;
        vm_args.ignoreUnrecognized = true;
        vm_args.version = JNI_VERSION_1_6;
        /* load and initialize a Java VM, return a JNI interface pointer in env */
        JNIEnv *env;       /* receives pointer to native method interface */
        int createResult = JNI_CreateJavaVM(&javaVM, (void**)&env, &vm_args);

        delete [] options;

        if (createResult != 0)
            throw MakeStringException(0, "javaembed: Unable to initialize JVM (%d)",createResult);
    }
    ~JavaGlobalState()
    {
        // We don't attempt to destroy the Java VM, as it's buggy...
    }
    JavaVM *javaVM;       /* denotes a Java VM */
} *globalState;

#ifdef _WIN32
    EXTERN_C IMAGE_DOS_HEADER __ImageBase;
#endif

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    globalState = new JavaGlobalState;
    // Make sure we are never unloaded (as JVM does not support it)
    // we do this by doing a dynamic load of the javaembed library
#ifdef _WIN32
    char path[_MAX_PATH];
    ::GetModuleFileName((HINSTANCE)&__ImageBase, path, _MAX_PATH);
    if (strstr(path, "javaembed"))
    {
        HINSTANCE h = LoadSharedObject(path, false, false);
        DBGLOG("LoadSharedObject returned %p", h);
    }
#else
    FILE *diskfp = fopen("/proc/self/maps", "r");
    if (diskfp)
    {
        char ln[_MAX_PATH];
        while (fgets(ln, sizeof(ln), diskfp))
        {
            if (strstr(ln, "libjavaembed"))
            {
                const char *fullName = strchr(ln, '/');
                if (fullName)
                {
                    char *tail = (char *) strstr(fullName, SharedObjectExtension);
                    if (tail)
                    {
                        tail[strlen(SharedObjectExtension)] = 0;
                        HINSTANCE h = LoadSharedObject(fullName, false, false);
                        break;
                    }
                }
            }
        }
        fclose(diskfp);
    }
#endif
    return true;
}
MODULE_EXIT()
{
    // We don't attempt to destroy the Java VM, as it's buggy...
//    delete globalState;
//    globalState = NULL;
}

static void checkType(type_t javatype, size32_t javasize, type_t ecltype, size32_t eclsize)
{
    if (javatype != ecltype || javasize != eclsize)
        throw MakeStringException(0, "javaembed: Type mismatch"); // MORE - could provide some details!
}

// There is a singleton JavaThreadContext per thread. This allows us to
// ensure that we can make repeated calls to a Java function efficiently.

class JavaThreadContext
{
public:
    JNIEnv *JNIenv;       /* receives pointer to native method interface */
public:
    JavaThreadContext()
    {
        jint res = globalState->javaVM->AttachCurrentThread((void **) &JNIenv, NULL);
        assertex(res >= 0);
        javaClass = NULL;
        javaMethodID = NULL;
        contextClassLoaderChecked = false;
    }
    ~JavaThreadContext()
    {
        if (javaClass)
            JNIenv->DeleteGlobalRef(javaClass);

        // According to the Java VM 1.7 docs, "A native thread attached to
        // the VM must call DetachCurrentThread() to detach itself before
        // exiting."
        globalState->javaVM->DetachCurrentThread();
    }

    void checkException()
    {
        if (JNIenv->ExceptionCheck())
        {
            jthrowable exception = JNIenv->ExceptionOccurred();
            JNIenv->ExceptionClear();
            jclass throwableClass = JNIenv->FindClass("java/lang/Throwable");
            jmethodID throwableToString = JNIenv->GetMethodID(throwableClass, "toString", "()Ljava/lang/String;");
            jstring cause = (jstring) JNIenv->CallObjectMethod(exception, throwableToString);
            const char *text = JNIenv->GetStringUTFChars(cause, 0);
            VStringBuffer message("javaembed: In method %s: %s", prevtext.get(), text);
            JNIenv->ReleaseStringUTFChars(cause, text);
            rtlFail(0, message.str());
        }
    }

    void ensureContextClassLoaderAvailable ()
    {
        // JVMs that are created by native threads have a context class loader set to the
        // bootstrap class loader, which is not very useful because the bootstrap class
        // loader is interested only in getting the JVM up to speed.  In particular,
        // the classpath is ignored.  The idea here is to set, if needed, the context
        // class loader to another loader that does recognize classpath.  What follows
        // is the equivalent of the following Java code:
        //
        // if (Thread.currentThread().getContextClassLoader == NULL)
        //     Thread.currentThread().setContextClassLoader(ClassLoader.getSystemClassLoader());

        if (!contextClassLoaderChecked)
        {
            JNIenv->ExceptionClear();
            // Get the current context class loader
            jclass javaLangThreadClass = JNIenv->FindClass("java/lang/Thread");
            checkException();
            jmethodID currentThreadMethod = JNIenv->GetStaticMethodID(javaLangThreadClass, "currentThread", "()Ljava/lang/Thread;");
            checkException();
            jobject threadObj = JNIenv->CallStaticObjectMethod(javaLangThreadClass, currentThreadMethod);
            checkException();
            jmethodID getContextClassLoaderMethod = JNIenv->GetMethodID(javaLangThreadClass, "getContextClassLoader", "()Ljava/lang/ClassLoader;");
            checkException();
            jobject contextClassLoaderObj = JNIenv->CallObjectMethod(threadObj, getContextClassLoaderMethod);
            checkException();

            if (!contextClassLoaderObj)
            {
                // No context class loader, so use the system class loader (hopefully it's present)
                jclass javaLangClassLoaderClass = JNIenv->FindClass("java/lang/ClassLoader");
                checkException();
                jmethodID getSystemClassLoaderMethod = JNIenv->GetStaticMethodID(javaLangClassLoaderClass, "getSystemClassLoader", "()Ljava/lang/ClassLoader;");
                checkException();
                jobject systemClassLoaderObj = JNIenv->CallStaticObjectMethod(javaLangClassLoaderClass, getSystemClassLoaderMethod);
                checkException();

                if (systemClassLoaderObj)
                {
                    jmethodID setContextClassLoaderMethod = JNIenv->GetMethodID(javaLangThreadClass, "setContextClassLoader", "(Ljava/lang/ClassLoader;)V");
                    checkException();
                    JNIenv->CallObjectMethod(threadObj, setContextClassLoaderMethod, systemClassLoaderObj);
                    checkException();
                }
            }

            contextClassLoaderChecked = true;
        }
    }

    inline void importFunction(size32_t lenChars, const char *utf)
    {
        size32_t bytes = rtlUtf8Size(lenChars, utf);
        StringBuffer text(bytes, utf);
        if (!prevtext || strcmp(text, prevtext) != 0)
        {
            prevtext.clear();

            // Make sure there is a context class loader available; we need to
            // do this before calling FindClass() on the class we need
            ensureContextClassLoaderAvailable();

            // Name should be in the form class.method:signature
            const char *funcname = strchr(text, '.');
            if (!funcname)
                throw MakeStringException(MSGAUD_user, 0, "javaembed: Invalid import name %s - Expected classname.methodname:signature", text.str());
            const char *signature = strchr(funcname, ':');
            if (!signature)
                throw MakeStringException(MSGAUD_user, 0, "javaembed: Invalid import name %s - Expected classname.methodname:signature", text.str());
            StringBuffer classname(funcname-text, text);
            funcname++;  // skip the '.'
            StringBuffer methodname(signature-funcname, funcname);
            signature++; // skip the ':'
            if (javaClass)
                JNIenv->DeleteGlobalRef(javaClass);
            javaClass = (jclass) JNIenv->NewGlobalRef(JNIenv->FindClass(classname));
            if (!javaClass)
                throw MakeStringException(MSGAUD_user, 0, "javaembed: Failed to resolve class name %s", classname.str());
            javaMethodID = JNIenv->GetStaticMethodID(javaClass, methodname, signature);
            if (!javaMethodID)
                throw MakeStringException(MSGAUD_user, 0, "javaembed: Failed to resolve method name %s with signature %s", methodname.str(), signature);
            const char *returnSig = strrchr(signature, ')');
            assertex(returnSig);  // Otherwise how did Java accept it??
            returnSig++;
            returnType.set(returnSig);
            argsig.set(signature);
            prevtext.set(text);
        }
    }
    inline void callFunction(jvalue &result, const jvalue * args)
    {
        JNIenv->ExceptionClear();
        switch (returnType.get()[0])
        {
        case 'C': result.c = JNIenv->CallStaticCharMethodA(javaClass, javaMethodID, args); break;
        case 'Z': result.z = JNIenv->CallStaticBooleanMethodA(javaClass, javaMethodID, args); break;
        case 'J': result.j = JNIenv->CallStaticLongMethodA(javaClass, javaMethodID, args); break;
        case 'F': result.f = JNIenv->CallStaticFloatMethodA(javaClass, javaMethodID, args); break;
        case 'D': result.d = JNIenv->CallStaticDoubleMethodA(javaClass, javaMethodID, args); break;
        case 'I': result.i = JNIenv->CallStaticIntMethodA(javaClass, javaMethodID, args); break;
        case 'S': result.s = JNIenv->CallStaticShortMethodA(javaClass, javaMethodID, args); break;
        case 'B': result.s = JNIenv->CallStaticByteMethodA(javaClass, javaMethodID, args); break;

        case '[':
        case 'L': result.l = JNIenv->CallStaticObjectMethodA(javaClass, javaMethodID, args); break;

        default: throwUnexpected();
        }
        checkException();
    }
    inline __int64 getSignedResult(jvalue & result)
    {
        switch (returnType.get()[0])
        {
        case 'B': return result.b;
        case 'S': return result.s;
        case 'I': return result.i;
        case 'J': return result.j;
        case 'L':
            {
                // Result should be of class 'Number'
                if (!result.l)
                    return 0;
                jmethodID getVal = JNIenv->GetMethodID(JNIenv->GetObjectClass(result.l), "longValue", "()J");
                if (!getVal)
                    throw MakeStringException(MSGAUD_user, 0, "javaembed: Type mismatch on result");
                return JNIenv->CallLongMethod(result.l, getVal);
            }
        default:
            throw MakeStringException(MSGAUD_user, 0, "javaembed: Type mismatch on result");
        }
    }
    inline double getDoubleResult(jvalue &result)
    {
        switch (returnType.get()[0])
        {
        case 'D': return result.d;
        case 'F': return result.f;
        case 'L':
            {
                // Result should be of class 'Number'
                if (!result.l)
                    return 0;
                jmethodID getVal = JNIenv->GetMethodID(JNIenv->GetObjectClass(result.l), "doubleValue", "()D");
                if (!getVal)
                    throw MakeStringException(MSGAUD_user, 0, "javaembed: Type mismatch on result");
                return JNIenv->CallDoubleMethod(result.l, getVal);
            }
        default:
            throw MakeStringException(MSGAUD_user, 0, "javaembed: Type mismatch on result");
        }
    }
    inline bool getBooleanResult(jvalue &result)
    {
        switch (returnType.get()[0])
        {
        case 'Z': return result.z;
        case 'L':
            {
                // Result should be of class 'Boolean'
                if (!result.l)
                    return false;
                jmethodID getVal = JNIenv->GetMethodID(JNIenv->GetObjectClass(result.l), "booleanValue", "()Z");
                if (!getVal)
                    throw MakeStringException(MSGAUD_user, 0, "javaembed: Type mismatch on result");
                return JNIenv->CallBooleanMethod(result.l, getVal);
            }
        default:
            throw MakeStringException(MSGAUD_user, 0, "javaembed: Type mismatch on result");
        }
    }
    inline void getDataResult(jvalue &result, size32_t &__len, void * &__result)
    {
        if (strcmp(returnType, "[B")!=0)
            throw MakeStringException(MSGAUD_user, 0, "javaembed: Type mismatch on result");
        jbyteArray array = (jbyteArray) result.l;
        __len = (array != NULL ? JNIenv->GetArrayLength(array) : 0);
        __result = (__len > 0 ? rtlMalloc(__len) : NULL);
        if (__result)
            JNIenv->GetByteArrayRegion(array, 0, __len, (jbyte *) __result);
    }
    inline void getStringResult(jvalue &result, size32_t &__len, char * &__result)
    {
        switch (returnType.get()[0])
        {
        case 'C': // Single char returned, prototyped as STRING or STRING1 in ECL
            rtlUnicodeToStrX(__len, __result, 1, &result.c);
            break;
        case 'L':
        {
            jstring sresult = (jstring) result.l;
            if (sresult)
            {
                size_t size = JNIenv->GetStringUTFLength(sresult);  // in bytes
                const char *text =  JNIenv->GetStringUTFChars(sresult, NULL);
                size32_t chars = rtlUtf8Length(size, text);
                rtlUtf8ToStrX(__len, __result, chars, text);
                JNIenv->ReleaseStringUTFChars(sresult, text);
            }
            else
            {
                __len = 0;
                __result = NULL;
            }
            break;
        }
        default:
            throw MakeStringException(MSGAUD_user, 0, "javaembed: Type mismatch on result");
        }
    }
    inline void getUTF8Result(jvalue &result, size32_t &__chars, char * &__result)
    {
        switch (returnType.get()[0])
        {
        case 'C': // Single jchar returned, prototyped as UTF8 in ECL
            rtlUnicodeToUtf8X(__chars, __result, 1, &result.c);
            break;
        case 'L':
        {
            jstring sresult = (jstring) result.l;
            if (sresult)
            {
                size_t size = JNIenv->GetStringUTFLength(sresult); // Returns length in bytes (not chars)
                const char * text =  JNIenv->GetStringUTFChars(sresult, NULL);
                rtlUtf8ToUtf8X(__chars, __result, rtlUtf8Length(size, text), text);
                JNIenv->ReleaseStringUTFChars(sresult, text);
            }
            else
            {
                __chars = 0;
                __result = NULL;
            }
            break;
        }
        default:
            throw MakeStringException(MSGAUD_user, 0, "javaembed: Type mismatch on result");
        }
    }
    inline void getUnicodeResult(jvalue &result, size32_t &__chars, UChar * &__result)
    {
        switch (returnType.get()[0])
        {
        case 'C': // Single jchar returned, prototyped as UNICODE or UNICODE1 in ECL
            rtlUnicodeToUnicodeX(__chars, __result, 1, &result.c);
            break;
        case 'L':
        {
            jstring sresult = (jstring) result.l;
            if (sresult)
            {
                size_t size = JNIenv->GetStringUTFLength(sresult);  // in bytes
                const char *text =  JNIenv->GetStringUTFChars(sresult, NULL);
                size32_t chars = rtlUtf8Length(size, text);
                rtlUtf8ToUnicodeX(__chars, __result, chars, text);
                JNIenv->ReleaseStringUTFChars(sresult, text);
            }
            else
            {
                __chars = 0;
                __result = NULL;
            }
            break;
        }
        default:
            throw MakeStringException(MSGAUD_user, 0, "javaembed: Type mismatch on result");
        }
    }
    inline void getSetResult(jvalue &result, bool & __isAllResult, size32_t & __resultBytes, void * & __result, int _elemType, size32_t elemSize)
    {
        if (returnType.get()[0] != '[')
            throw MakeStringException(MSGAUD_user, 0, "javaembed: Type mismatch on result (array expected)");
        type_t elemType = (type_t) _elemType;
        jarray array = (jarray) result.l;
        int numResults = (array != NULL ? JNIenv->GetArrayLength(array) : 0);
        rtlRowBuilder out;
        byte *outData = NULL;
        size32_t outBytes = 0;
        if (numResults > 0)
        {
            if (elemSize != UNKNOWN_LENGTH)
            {
                out.ensureAvailable(numResults * elemSize); // MORE - check for overflow?
                outData = out.getbytes();
            }
            switch(returnType.get()[1])
            {
            case 'Z':
                checkType(type_boolean, sizeof(jboolean), elemType, elemSize);
                JNIenv->GetBooleanArrayRegion((jbooleanArray) array, 0, numResults, (jboolean *) outData);
                break;
            case 'B':
                checkType(type_int, sizeof(jbyte), elemType, elemSize);
                JNIenv->GetByteArrayRegion((jbyteArray) array, 0, numResults, (jbyte *) outData);
                break;
            case 'C':
                // we COULD map to a set of string1, but is there any point?
                throw MakeStringException(0, "javaembed: Return type mismatch (char[] not supported)");
                break;
            case 'S':
                checkType(type_int, sizeof(jshort), elemType, elemSize);
                JNIenv->GetShortArrayRegion((jshortArray) array, 0, numResults, (jshort *) outData);
                break;
            case 'I':
                checkType(type_int, sizeof(jint), elemType, elemSize);
                JNIenv->GetIntArrayRegion((jintArray) array, 0, numResults, (jint *) outData);
                break;
            case 'J':
                checkType(type_int, sizeof(jlong), elemType, elemSize);
                JNIenv->GetLongArrayRegion((jlongArray) array, 0, numResults, (jlong *) outData);
                break;
            case 'F':
                checkType(type_real, sizeof(jfloat), elemType, elemSize);
                JNIenv->GetFloatArrayRegion((jfloatArray) array, 0, numResults, (jfloat *) outData);
                break;
            case 'D':
                checkType(type_real, sizeof(jdouble), elemType, elemSize);
                JNIenv->GetDoubleArrayRegion((jdoubleArray) array, 0, numResults, (jdouble *) outData);
                break;
            case 'L':
                if (strcmp(returnType, "[Ljava/lang/String;") == 0)
                {
                    for (int i = 0; i < numResults; i++)
                    {
                        jstring elem = (jstring) JNIenv->GetObjectArrayElement((jobjectArray) array, i);
                        size_t lenBytes = JNIenv->GetStringUTFLength(elem);  // in bytes
                        const char *text =  JNIenv->GetStringUTFChars(elem, NULL);

                        switch (elemType)
                        {
                        case type_string:
                            if (elemSize == UNKNOWN_LENGTH)
                            {
                                out.ensureAvailable(outBytes + lenBytes + sizeof(size32_t));
                                outData = out.getbytes() + outBytes;
                                * (size32_t *) outData = lenBytes;
                                rtlStrToStr(lenBytes, outData+sizeof(size32_t), lenBytes, text);
                                outBytes += lenBytes + sizeof(size32_t);
                            }
                            else
                                rtlStrToStr(elemSize, outData, lenBytes, text);
                            break;
                        case type_varstring:
                            if (elemSize == UNKNOWN_LENGTH)
                            {
                                out.ensureAvailable(outBytes + lenBytes + 1);
                                outData = out.getbytes() + outBytes;
                                rtlStrToVStr(0, outData, lenBytes, text);
                                outBytes += lenBytes + 1;
                            }
                            else
                                rtlStrToVStr(elemSize, outData, lenBytes, text);  // Fixed size null terminated strings... weird.
                            break;
                        case type_utf8:
                        case type_unicode:
                        {
                            size32_t numchars = rtlUtf8Length(lenBytes, text);
                            if (elemType == type_utf8)
                            {
                                assertex (elemSize == UNKNOWN_LENGTH);
                                out.ensureAvailable(outBytes + lenBytes + sizeof(size32_t));
                                outData = out.getbytes() + outBytes;
                                * (size32_t *) outData = numchars;
                                rtlStrToStr(lenBytes, outData+sizeof(size32_t), lenBytes, text);
                                outBytes += lenBytes + sizeof(size32_t);
                            }
                            else
                            {
                                if (elemSize == UNKNOWN_LENGTH)
                                {
                                    // You can't assume that number of chars in utf8 matches number in unicode16 ...
                                    size32_t numchars16;
                                    rtlDataAttr unicode16;
                                    rtlUtf8ToUnicodeX(numchars16, unicode16.refustr(), numchars, text);
                                    out.ensureAvailable(outBytes + numchars16*sizeof(UChar) + sizeof(size32_t));
                                    outData = out.getbytes() + outBytes;
                                    * (size32_t *) outData = numchars16;
                                    rtlUnicodeToUnicode(numchars16, (UChar *) (outData+sizeof(size32_t)), numchars16, unicode16.getustr());
                                    outBytes += numchars16*sizeof(UChar) + sizeof(size32_t);
                                }
                                else
                                    rtlUtf8ToUnicode(elemSize / sizeof(UChar), (UChar *) outData, numchars, text);
                            }
                            break;
                        }
                        default:
                            JNIenv->ReleaseStringUTFChars(elem, text);
                            throw MakeStringException(0, "javaembed: Return type mismatch (ECL string type expected)");
                        }
                        JNIenv->ReleaseStringUTFChars(elem, text);
                        JNIenv->DeleteLocalRef(elem);
                        if (elemSize != UNKNOWN_LENGTH)
                            outData += elemSize;
                    }
                }
                else
                    throw MakeStringException(0, "javaembed: Return type mismatch (%s[] not supported)", returnType.get()+2);
                break;
            }
        }
        __isAllResult = false;
        __resultBytes = elemSize == UNKNOWN_LENGTH ? outBytes : elemSize * numResults;
        __result = out.detachdata();
    }
    inline const char *querySignature()
    {
        return argsig.get();
    }

private:
    StringAttr returnType;
    StringAttr argsig;
    StringAttr prevtext;
    jclass javaClass;
    jmethodID javaMethodID;
    bool contextClassLoaderChecked;
};

// Each call to a Java function will use a new JavaEmbedScriptContext object
#define MAX_JNI_ARGS 10

class JavaEmbedImportContext : public CInterfaceOf<IEmbedFunctionContext>
{
public:
    JavaEmbedImportContext(JavaThreadContext *_sharedCtx, const char *options)
    : sharedCtx(_sharedCtx)
    {
        argcount = 0;
        argsig = NULL;

        // Create a new frame for local references and increase the capacity
        // of those references to 64 (default is 16)
        sharedCtx->JNIenv->PushLocalFrame(64);
    }
    ~JavaEmbedImportContext()
    {
        // Pop local reference frame; explicitly frees all local
        // references made during that frame's lifetime
        sharedCtx->JNIenv->PopLocalFrame(NULL);
    }

    virtual bool getBooleanResult()
    {
        return sharedCtx->getBooleanResult(result);
    }
    virtual void getDataResult(size32_t &__len, void * &__result)
    {
        sharedCtx->getDataResult(result, __len, __result);
    }
    virtual double getRealResult()
    {
        return sharedCtx->getDoubleResult(result);
    }
    virtual __int64 getSignedResult()
    {
        return sharedCtx->getSignedResult(result);
    }
    virtual unsigned __int64 getUnsignedResult()
    {
        throw MakeStringException(MSGAUD_user, 0, "javaembed: Unsigned results not supported"); // Java doesn't support unsigned
    }
    virtual void getStringResult(size32_t &__len, char * &__result)
    {
        sharedCtx->getStringResult(result, __len, __result);
    }
    virtual void getUTF8Result(size32_t &__chars, char * &__result)
    {
        sharedCtx->getUTF8Result(result, __chars, __result);
    }
    virtual void getUnicodeResult(size32_t &__chars, UChar * &__result)
    {
        sharedCtx->getUnicodeResult(result, __chars, __result);
    }
    virtual void getSetResult(bool & __isAllResult, size32_t & __resultBytes, void * & __result, int elemType, size32_t elemSize)
    {
        sharedCtx->getSetResult(result, __isAllResult, __resultBytes, __result, elemType, elemSize);
    }
    virtual IRowStream *getDatasetResult(IEngineRowAllocator * _resultAllocator)
    {
        UNIMPLEMENTED;
    }
    virtual byte * getRowResult(IEngineRowAllocator * _resultAllocator)
    {
        UNIMPLEMENTED;
    }
    virtual size32_t getTransformResult(ARowBuilder & builder)
    {
        UNIMPLEMENTED;
    }
    virtual void bindBooleanParam(const char *name, bool val)
    {
        if (*argsig != 'B')
            typeError("BOOLEAN");
        argsig++;
        jvalue v;
        v.z = val;
        addArg(v);
    }
    virtual void bindDataParam(const char *name, size32_t len, const void *val)
    {
        if (argsig[0] != '[' || argsig[1] != 'B')
            typeError("DATA");
        argsig += 2;
        jvalue v;
        jbyteArray javaData = sharedCtx->JNIenv->NewByteArray(len);
        sharedCtx->JNIenv->SetByteArrayRegion(javaData, 0, len, (jbyte *) val);
        v.l = javaData;
        addArg(v);
    }
    virtual void bindRealParam(const char *name, double val)
    {
        jvalue v;
        switch(*argsig)
        {
        case 'D':
            v.d = val;
            break;
        case 'F':
            v.f = val;
            break;
        default:
            typeError("REAL");
            break;
        }
        argsig++;
        addArg(v);
    }
    virtual void bindSignedParam(const char *name, __int64 val)
    {
        jvalue v;
        switch(*argsig)
        {
        case 'I':
            v.i = val;
            break;
        case 'J':
            v.j = val;
            break;
        case 'S':
            v.s = val;
            break;
        case 'B':
            v.b = val;
            break;
        default:
            typeError("INTEGER");
            break;
        }
        argsig++;
        addArg(v);
    }
    virtual void bindUnsignedParam(const char *name, unsigned __int64 val)
    {
        throw MakeStringException(MSGAUD_user, 0, "javaembed: Unsigned parameters not supported"); // Java doesn't support unsigned
    }
    virtual void bindStringParam(const char *name, size32_t len, const char *val)
    {
        jvalue v;
        switch(*argsig)
        {
        case 'C':
            rtlStrToUnicode(1, &v.c, len, val);
            argsig++;
            break;
        case 'L':
            if (strncmp(argsig, "Ljava/lang/String;", 18) == 0)
            {
                argsig += 18;
                unsigned unicodeChars;
                UChar *unicode;
                rtlStrToUnicodeX(unicodeChars, unicode, len, val);
                v.l = sharedCtx->JNIenv->NewString(unicode, unicodeChars);
                rtlFree(unicode);
                break;
            }
            // fall into ...
        default:
            typeError("STRING");
            break;
        }
        addArg(v);
    }
    virtual void bindVStringParam(const char *name, const char *val)
    {
        bindStringParam(name, strlen(val), val);
    }
    virtual void bindUTF8Param(const char *name, size32_t numchars, const char *val)
    {
        jvalue v;
        switch(*argsig)
        {
        case 'C':
            rtlUtf8ToUnicode(1, &v.c, numchars, val);
            argsig++;
            break;
        case 'L':
            if (strncmp(argsig, "Ljava/lang/String;", 18) == 0)
            {
                argsig += 18;
                unsigned unicodeChars;
                UChar *unicode;
                rtlUtf8ToUnicodeX(unicodeChars, unicode, numchars, val);
                v.l = sharedCtx->JNIenv->NewString(unicode, unicodeChars);
                rtlFree(unicode);
                break;
            }
            // fall into ...
        default:
            typeError("UTF8");
            break;
        }
        addArg(v);
    }
    virtual void bindUnicodeParam(const char *name, size32_t numchars, const UChar *val)
    {
        jvalue v;
        switch(*argsig)
        {
        case 'C':
            rtlUnicodeToUnicode(1, &v.c, numchars, val);
            argsig++;
            break;
        case 'L':
            if (strncmp(argsig, "Ljava/lang/String;", 18) == 0)
            {
                argsig += 18;
                v.l = sharedCtx->JNIenv->NewString(val, numchars);
                break;
            }
            // fall into ...
        default:
            typeError("UNICODE");
            break;
        }
        addArg(v);
    }
    virtual void bindSetParam(const char *name, int _elemType, size32_t elemSize, bool isAll, size32_t totalBytes, void *setData)
    {
        jvalue v;
        if (*argsig != '[')
            typeError("SET");
        argsig++;
        type_t elemType = (type_t) _elemType;
        int numElems = totalBytes / elemSize;
        switch(*argsig)
        {
        case 'Z':
            checkType(type_boolean, sizeof(jboolean), elemType, elemSize);
            v.l = sharedCtx->JNIenv->NewBooleanArray(numElems);
            sharedCtx->JNIenv->SetBooleanArrayRegion((jbooleanArray) v.l, 0, numElems, (jboolean *) setData);
            break;
        case 'B':
            checkType(type_int, sizeof(jbyte), elemType, elemSize);
            v.l = sharedCtx->JNIenv->NewByteArray(numElems);
            sharedCtx->JNIenv->SetByteArrayRegion((jbyteArray) v.l, 0, numElems, (jbyte *) setData);
            break;
        case 'C':
            // we COULD map to a set of string1, but is there any point?
            typeError("");
            break;
        case 'S':
            checkType(type_int, sizeof(jshort), elemType, elemSize);
            v.l = sharedCtx->JNIenv->NewShortArray(numElems);
            sharedCtx->JNIenv->SetShortArrayRegion((jshortArray) v.l, 0, numElems, (jshort *) setData);
            break;
        case 'I':
            checkType(type_int, sizeof(jint), elemType, elemSize);
            v.l = sharedCtx->JNIenv->NewIntArray(numElems);
            sharedCtx->JNIenv->SetIntArrayRegion((jintArray) v.l, 0, numElems, (jint *) setData);
            break;
        case 'J':
            checkType(type_int, sizeof(jlong), elemType, elemSize);
            v.l = sharedCtx->JNIenv->NewLongArray(numElems);
            sharedCtx->JNIenv->SetLongArrayRegion((jlongArray) v.l, 0, numElems, (jlong *) setData);
            break;
        case 'F':
            checkType(type_real, sizeof(jfloat), elemType, elemSize);
            v.l = sharedCtx->JNIenv->NewFloatArray(numElems);
            sharedCtx->JNIenv->SetFloatArrayRegion((jfloatArray) v.l, 0, numElems, (jfloat *) setData);
            break;
        case 'D':
            checkType(type_real, sizeof(jdouble), elemType, elemSize);
            v.l = sharedCtx->JNIenv->NewDoubleArray(numElems);
            sharedCtx->JNIenv->SetDoubleArrayRegion((jdoubleArray) v.l, 0, numElems, (jdouble *) setData);
            break;
        case 'L':
            if (strncmp(argsig, "Ljava/lang/String;", 18) == 0)
            {
                argsig += 17;  // Yes, 17, because we increment again at the end of the case
                const byte *inData = (const byte *) setData;
                const byte *endData = inData + totalBytes;
                if (elemSize == UNKNOWN_LENGTH)
                {
                    numElems = 0;
                    // Will need 2 passes to work out how many elements there are in the set :(
                    while (inData < endData)
                    {
                        int thisSize;
                        switch (elemType)
                        {
                        case type_varstring:
                            thisSize = strlen((const char *) inData) + 1;
                            break;
                        case type_string:
                            thisSize = * (size32_t *) inData + sizeof(size32_t);
                            break;
                        case type_unicode:
                            thisSize = (* (size32_t *) inData) * sizeof(UChar) + sizeof(size32_t);
                            break;
                        case type_utf8:
                            thisSize = rtlUtf8Size(* (size32_t *) inData, inData + sizeof(size32_t)) + sizeof(size32_t);;
                            break;
                        default:
                            typeError("STRING");
                        }
                        inData += thisSize;
                        numElems++;
                    }
                    inData = (const byte *) setData;
                }
                int idx = 0;
                v.l = sharedCtx->JNIenv->NewObjectArray(numElems, sharedCtx->JNIenv->FindClass("java/lang/String"), NULL);
                while (inData < endData)
                {
                    jstring thisElem;
                    size32_t thisSize = elemSize;
                    switch (elemType)
                    {
                    case type_varstring:
                    {
                        size32_t numChars = strlen((const char *) inData);
                        unsigned unicodeChars;
                        rtlDataAttr unicode;
                        rtlStrToUnicodeX(unicodeChars, unicode.refustr(), numChars, (const char *) inData);
                        thisElem = sharedCtx->JNIenv->NewString(unicode.getustr(), unicodeChars);
                        if (elemSize == UNKNOWN_LENGTH)
                            thisSize = numChars + 1;
                        break;
                    }
                    case type_string:
                    {
                        if (elemSize == UNKNOWN_LENGTH)
                        {
                            thisSize = * (size32_t *) inData;
                            inData += sizeof(size32_t);
                        }
                        unsigned unicodeChars;
                        rtlDataAttr unicode;
                        rtlStrToUnicodeX(unicodeChars, unicode.refustr(), thisSize, (const char *) inData);
                        thisElem = sharedCtx->JNIenv->NewString(unicode.getustr(), unicodeChars);
                        break;
                    }
                    case type_unicode:
                    {
                        if (elemSize == UNKNOWN_LENGTH)
                        {
                            thisSize = (* (size32_t *) inData) * sizeof(UChar); // NOTE - it's in chars...
                            inData += sizeof(size32_t);
                        }
                        thisElem = sharedCtx->JNIenv->NewString((const UChar *) inData, thisSize / sizeof(UChar));
                        //checkJPythonError();
                        break;
                    }
                    case type_utf8:
                    {
                        assertex (elemSize == UNKNOWN_LENGTH);
                        size32_t numChars = * (size32_t *) inData;
                        inData += sizeof(size32_t);
                        unsigned unicodeChars;
                        rtlDataAttr unicode;
                        rtlUtf8ToUnicodeX(unicodeChars, unicode.refustr(), numChars, (const char *) inData);
                        thisElem = sharedCtx->JNIenv->NewString(unicode.getustr(), unicodeChars);
                        thisSize = rtlUtf8Size(numChars, inData);
                        break;
                    }
                    default:
                        typeError("STRING");
                    }
                    sharedCtx->checkException();
                    inData += thisSize;
                    sharedCtx->JNIenv->SetObjectArrayElement((jobjectArray) v.l, idx, thisElem);
                    sharedCtx->JNIenv->DeleteLocalRef(thisElem);
                    idx++;
                }
            }
            else
                typeError("");
            break;
        }
        argsig++;
        addArg(v);
    }
    virtual void bindRowParam(const char *name, IOutputMetaData & metaVal, byte *val)
    {
        UNIMPLEMENTED;
    }
    virtual void bindDatasetParam(const char *name, IOutputMetaData & metaVal, IRowStream * val)
    {
        UNIMPLEMENTED;
    }

    virtual void importFunction(size32_t lenChars, const char *utf)
    {
        sharedCtx->importFunction(lenChars, utf);
        argsig = sharedCtx->querySignature();
        assertex(*argsig == '(');
        argsig++;
    }
    virtual void callFunction()
    {
        sharedCtx->callFunction(result, args);
    }

    virtual void compileEmbeddedScript(size32_t lenChars, const char *script)
    {
        throwUnexpected();  // The java language helper supports only imported functions, not embedding java code in ECL.
    }
protected:
    JavaThreadContext *sharedCtx;
    jvalue result;
private:

    void typeError(const char *ECLtype) __attribute__((noreturn))
    {
        const char *javaType;
        int javaLen = 0;
        switch (*argsig)
        {
        case 'Z': javaType = "boolean"; break;
        case 'B': javaType = "byte"; break;
        case 'C': javaType = "char"; break;
        case 'S': javaType = "short"; break;
        case 'I': javaType = "int"; break;
        case 'J': javaType = "long"; break;
        case 'F': javaType = "float"; break;
        case 'D': javaType = "double"; break;
        case '[': javaType = "array"; break;
        case 'L':
            {
                javaType = argsig+1;
                const char *semi = strchr(argsig, ';');
                if (semi)
                    javaLen = semi - javaType;
                break;
            }
        case ')':
            throw MakeStringException(0, "javaembed: Too many ECL parameters passed for Java signature %s", sharedCtx->querySignature());
        default:
            throw MakeStringException(0, "javaembed: Unrecognized character %c in java signature %s", *argsig, sharedCtx->querySignature());
        }
        if (!javaLen)
            javaLen = strlen(argsig);
        throw MakeStringException(0, "javaembed: ECL type %s cannot be passed to Java type %.*s", ECLtype, javaLen, javaType);
    }
    void addArg(jvalue &arg)
    {
        assertex(argcount < MAX_JNI_ARGS);
        args[argcount] = arg;
        argcount++;
    }
    jvalue args[MAX_JNI_ARGS];
    int argcount;
    const char *argsig;
};

static __thread JavaThreadContext* threadContext;  // We reuse per thread, for speed
static __thread ThreadTermFunc threadHookChain;

static void releaseContext()
{
    if (threadContext)
    {
        delete threadContext;
        threadContext = NULL;
    }
    if (threadHookChain)
    {
        (*threadHookChain)();
        threadHookChain = NULL;
    }
}

class JavaEmbedContext : public CInterfaceOf<IEmbedContext>
{
public:
    virtual IEmbedFunctionContext *createFunctionContext(bool isImport, const char *options)
    {
        if (!threadContext)
        {
            threadContext = new JavaThreadContext;
            threadHookChain = addThreadTermFunc(releaseContext);
        }
        assertex(isImport);
        return new JavaEmbedImportContext(threadContext, options);
    }
};

extern IEmbedContext* getEmbedContext()
{
    return new JavaEmbedContext;
}

} // namespace
