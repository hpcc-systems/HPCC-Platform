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
#include "eclrtl.hpp"
#include "jexcept.hpp"
#include "jthread.hpp"
#include "hqlplugins.hpp"

#ifdef _WIN32
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

static const char * compatibleVersions[] = {
    "Java Embed Helper 1.0.0",
    NULL };

static const char *version = "Java Embed Helper 1.0.0";

static const char * EclDefinition =
    "EXPORT Language := SERVICE\n"
    "  boolean getEmbedContext():cpp,pure,namespace='javaembed',entrypoint='getEmbedContext',prototype='IEmbedContext* getEmbedContext()';\n"
    "END;"
    "EXPORT getEmbedContext := Language.getEmbedContext;"
    "EXPORT boolean supportsImport := true;"
    "EXPORT boolean supportsScript := false;";

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
    pb->ECL = EclDefinition;
    pb->flags = PLUGIN_DLL_MODULE | PLUGIN_MULTIPLE_VERSIONS;
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
// So we can't achieve 1, and 2 requires that we create via the INIT_MODLE mechanism (rather than just a static object), and that
// any engines that call InitModuleObjects() or load plugins dynamically do so AFTER setting any signal handlers or calling
// EnableSEHtoExceptionMapping
//

static class JavaGlobalState
{
public:
    JavaGlobalState()
    {
        JavaVMInitArgs vm_args; /* JDK/JRE 6 VM initialization arguments */
        JavaVMOption* options = new JavaVMOption[3];
        const char* origPath = getenv("CLASSPATH");
        StringBuffer newPath;
        newPath.append("-Djava.class.path=").append(origPath).append(ENVSEPCHAR).append(".");
        options[0].optionString = (char *) newPath.str();
        options[1].optionString = (char *) "-Xcheck:jni";
        options[2].optionString = (char *) "-verbose:jni";
        vm_args.version = JNI_VERSION_1_6;
#ifdef _DEBUG
        vm_args.nOptions = 1;  // set to 3 if you want the verbose...
#else
        vm_args.nOptions = 1;
#endif
        vm_args.options = options;
        vm_args.ignoreUnrecognized = false;
        /* load and initialize a Java VM, return a JNI interface pointer in env */
        JNIEnv *env;       /* receives pointer to native method interface */
        JNI_CreateJavaVM(&javaVM, (void**)&env, &vm_args);

        delete [] options;
    }
    ~JavaGlobalState()
    {
        // We don't attempt to destroy the Java VM, as it's buggy...
    }
    JavaVM *javaVM;       /* denotes a Java VM */
} *globalState;

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    globalState = new JavaGlobalState;
    return true;
}
MODULE_EXIT()
{
    delete globalState;
    globalState = NULL;
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
    }
    ~JavaThreadContext()
    {
        if (javaClass)
            JNIenv->DeleteGlobalRef(javaClass);
    }

    void checkException()
    {
        jthrowable exception = JNIenv->ExceptionOccurred();
        if (exception)
        {
            JNIenv->ExceptionClear();
            jclass throwableClass = JNIenv->FindClass("java/lang/Throwable");
            jmethodID throwableToString = JNIenv->GetMethodID(throwableClass, "toString", "()Ljava/lang/String;");
            jstring cause = (jstring) JNIenv->CallObjectMethod(exception, throwableToString);
            const char *text = JNIenv->GetStringUTFChars(cause, 0);
            throw MakeStringException(MSGAUD_user, 0, "javaembed: In method %s: %s", prevtext.get(), text);
        }
    }

    inline void importFunction(size32_t lenChars, const char *utf)
    {
        size32_t bytes = rtlUtf8Size(lenChars, utf);
        StringBuffer text(bytes, utf);
        if (!prevtext || strcmp(text, prevtext) != 0)
        {
            prevtext.clear();
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
        __len = JNIenv->GetArrayLength(array);
        __result = rtlMalloc(__len);
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
            size_t size = JNIenv->GetStringUTFLength(sresult);  // in bytes
            const char *text =  JNIenv->GetStringUTFChars(sresult, NULL);
            size32_t chars = rtlUtf8Length(size, text);
            rtlUtf8ToStrX(__len, __result, chars, text);
            JNIenv->ReleaseStringUTFChars(sresult, text);
            break;
        }
        default:
            throwUnexpected();
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
            size_t size = JNIenv->GetStringUTFLength(sresult); // Returns length in bytes (not chars)
            const char * text =  JNIenv->GetStringUTFChars(sresult, NULL);
            rtlUtf8ToUtf8X(__chars, __result, rtlUtf8Length(size, text), text);
            JNIenv->ReleaseStringUTFChars(sresult, text);
            break;
        }
        default:
            throwUnexpected();
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
            size_t size = JNIenv->GetStringUTFLength(sresult);  // in bytes
            const char *text =  JNIenv->GetStringUTFChars(sresult, NULL);
            size32_t chars = rtlUtf8Length(size, text);
            rtlUtf8ToUnicodeX(__chars, __result, chars, text);
            JNIenv->ReleaseStringUTFChars(sresult, text);
            break;
        }
        default:
            throwUnexpected();
        }
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
    }
    ~JavaEmbedImportContext()
    {
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
    void typeError(const char *ECLtype)
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
    delete threadContext;
    threadContext = NULL;
    if (threadHookChain)
        (*threadHookChain)();
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
