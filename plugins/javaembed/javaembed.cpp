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

// Use a global object to ensure that the Java VM  is initialized on main thread

static class JavaGlobalState
{
public:
    JavaGlobalState()
    {
        JavaVMInitArgs vm_args; /* JDK/JRE 6 VM initialization arguments */
        JavaVMOption* options = new JavaVMOption[2];
        options[0].optionString = (char *) "-Djava.class.path=.";
        options[1].optionString = (char *) "-verbose:jni";
        vm_args.version = JNI_VERSION_1_6;
        vm_args.nOptions = 1;  // set to 2 if you want the verbose...
        vm_args.options = options;
        vm_args.ignoreUnrecognized = false;
        /* load and initialize a Java VM, return a JNI interface pointer in env */
        JNIEnv *env;       /* receives pointer to native method interface */
        JNI_CreateJavaVM(&javaVM, (void**)&env, &vm_args);

        delete options;
    }
    ~JavaGlobalState()
    {
        // We don't attempt to destroy the Java VM, as it's buggy...
    }
    JavaVM *javaVM;       /* denotes a Java VM */
} globalState;

// There is a singleton JavaThreadContext per thread. This allows us to
// ensure that we can make repeated calls to a Java function efficiently.

class JavaThreadContext
{
public:
    JNIEnv *JNIenv;       /* receives pointer to native method interface */
public:
    JavaThreadContext()
    {
        jint res = globalState.javaVM->AttachCurrentThread((void **) &JNIenv, NULL);
        assertex(res >= 0);
        javaClass = NULL;
        javaMethodID = NULL;
    }
    ~JavaThreadContext()
    {
    }

    inline jmethodID importFunction(const char *text)
    {
        if (!prevtext || strcmp(text, prevtext) != 0)
        {
            prevtext.clear();
            // Name should be in the form class.method:signature
            const char *signature = strchr(text, ':');
            const char *funcname = strchr(text, '.');
            if (!signature || !funcname)
                throw MakeStringException(MSGAUD_user, 0, "javaembed: Invalid import name %s - Expected classname.methodname:signature", text);
            StringBuffer classname(funcname-text, text);
            funcname++;  // skip the '.'
            StringBuffer methodname(signature-funcname, funcname);
            signature++; // skip the ':'
            javaClass = JNIenv->FindClass(classname);
            if (!javaClass)
                throw MakeStringException(MSGAUD_user, 0, "javaembed: Failed to resolve class name %s", classname.str());
            javaMethodID = JNIenv->GetStaticMethodID(javaClass, methodname, signature);
            if (!javaMethodID)
                throw MakeStringException(MSGAUD_user, 0, "javaembed: Failed to resolve method name %s with signature %s", methodname.str(), signature);
            const char *returnSig = strrchr(signature, ')');
            assertex(returnSig);
            returnSig++;
            returnType.set(returnSig);
            prevtext.set(text);
        }
        return NULL;
    }
    inline void callFunction(jvalue &result, const jvalue * args)
    {
        switch (returnType.get()[0])
        {
        case 'C': UNIMPLEMENTED; // jchar has no real ecl equivalent
        case 'B': result.z = JNIenv->CallStaticBooleanMethodA(javaClass, javaMethodID, args); break;
        case 'J': result.j = JNIenv->CallStaticLongMethodA(javaClass, javaMethodID, args); break;
        case 'F': result.f = JNIenv->CallStaticFloatMethodA(javaClass, javaMethodID, args); break;
        case 'D': result.d = JNIenv->CallStaticDoubleMethodA(javaClass, javaMethodID, args); break;
        case 'L': result.l = JNIenv->CallStaticObjectMethodA(javaClass, javaMethodID, args); break;
        case 'I': // Others are all smaller ints, so we can use this for all
        default: result.i = JNIenv->CallStaticIntMethodA(javaClass, javaMethodID, args); break;
        }
    }
    __int64 getSignedResult(jvalue & result)
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
                jmethodID getVal = JNIenv->GetMethodID(JNIenv->GetObjectClass(result.l), "longValue", "()J");  // this could probably be cached?
                if (!getVal)
                    throw MakeStringException(MSGAUD_user, 0, "javaembed: Type mismatch on result");
                return JNIenv->CallLongMethod(result.l, getVal);
            }
        default:
            throw MakeStringException(MSGAUD_user, 0, "javaembed: Type mismatch on result");
        }
    }
    double getDoubleResult(jvalue &result)
    {
        switch (returnType.get()[0])
        {
        case 'D': return result.d;
        case 'F': return result.f;
        case 'L':
            {
                // Result should be of class 'Number'
                jmethodID getVal = JNIenv->GetMethodID(JNIenv->GetObjectClass(result.l), "doubleValue", "()D");  // this could probably be cached?
                if (!getVal)
                    throw MakeStringException(MSGAUD_user, 0, "javaembed: Type mismatch on result");
                return JNIenv->CallDoubleMethod(result.l, getVal);
            }
        default:
            throw MakeStringException(MSGAUD_user, 0, "javaembed: Type mismatch on result");
        }
    }
private:
    StringAttr returnType;
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
    }
    ~JavaEmbedImportContext()
    {
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
        jstring sresult = (jstring) result.l;
        __len = sharedCtx->JNIenv->GetStringUTFLength(sresult);
        const char * chars =  sharedCtx->JNIenv->GetStringUTFChars(sresult, NULL);
        __result = new char(__len);
        memcpy(__result, chars, __len);
        sharedCtx->JNIenv->ReleaseStringUTFChars(sresult, chars);
    }

    virtual void bindRealParam(const char *name, double val)
    {
        jvalue v;
        v.d = val;
        addArg(v);
    }
    virtual void bindSignedParam(const char *name, __int64 val)
    {
        jvalue v;
        v.j = val;
        addArg(v);
    }
    virtual void bindUnsignedParam(const char *name, unsigned __int64 val)
    {
        throw MakeStringException(MSGAUD_user, 0, "javaembed: Unsigned parameters not supported"); // Java doesn't support unsigned
    }
    virtual void bindStringParam(const char *name, size32_t len, const char *val)
    {
        StringBuffer s(len, val);
        jvalue v;
        v.l = sharedCtx->JNIenv->NewStringUTF(s.str());
        addArg(v);
    }
    virtual void bindVStringParam(const char *name, const char *val)
    {
        jvalue v;
        v.l = sharedCtx->JNIenv->NewStringUTF(val);
        addArg(v);
    }
    virtual void importFunction(const char *text)
    {
        sharedCtx->importFunction(text);
    }
    virtual void callFunction()
    {
        sharedCtx->callFunction(result, args);
    }

    virtual void compileEmbeddedScript(const char *script)
    {
        throwUnexpected();  // The java language helper supports only imported functions, not embedding java code in ECL.
    }
protected:
    JavaThreadContext *sharedCtx;
    jvalue result;
private:
    void addArg(jvalue &arg)
    {
        assertex(argcount < MAX_JNI_ARGS);
        args[argcount] = arg;
        argcount++;
    }
    jvalue args[MAX_JNI_ARGS];
    int argcount;
};

static __thread JavaThreadContext* threadContext;  // We reuse per thread, for speed
static __thread ThreadTermFunc threadHookChain;

static void releaseContext()
{
    delete threadContext;
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
