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
#include "eclhelper.hpp"
#include "eclrtl.hpp"
#include "eclrtl_imp.hpp"
#include "rtlfield_imp.hpp"
#include "rtlds_imp.hpp"
#include "jprop.hpp"
#include "build-config.h"
#include "roxiemem.hpp"
#include "nbcd.hpp"

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

static void UNSUPPORTED(const char *feature) __attribute__((noreturn));

static void UNSUPPORTED(const char *feature)
{
    throw MakeStringException(-1, "UNSUPPORTED feature: %s not supported in java plugin", feature);
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
#ifdef _DEBUG
        // optionStrings.append("-Xcheck:jni");
        // optionStrings.append("-verbose:jni");
#endif

        JavaVMOption* options = new JavaVMOption[optionStrings.length()];
        ForEachItemIn(idx, optionStrings)
        {
            // DBGLOG("javaembed: Setting JVM option: %s",(char *)optionStrings.item(idx));
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

static char helperLibraryName[_MAX_PATH];

#ifdef _WIN32
    EXTERN_C IMAGE_DOS_HEADER __ImageBase;
#endif

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    globalState = new JavaGlobalState;
    // Make sure we are never unloaded (as JVM does not support it)
    // we do this by doing a dynamic load of the javaembed library
#ifdef _WIN32
    ::GetModuleFileName((HINSTANCE)&__ImageBase, helperLibraryName, _MAX_PATH);
    if (strstr(path, "javaembed"))
    {
        HINSTANCE h = LoadSharedObject(helperLibraryName, false, false);
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
                        strcpy(helperLibraryName, fullName);
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

static void checkException(JNIEnv *JNIenv)
{
    if (JNIenv->ExceptionCheck())
    {
        jthrowable exception = JNIenv->ExceptionOccurred();
        JNIenv->ExceptionClear();
        jclass throwableClass = JNIenv->FindClass("java/lang/Throwable");
        jmethodID throwableToString = JNIenv->GetMethodID(throwableClass, "toString", "()Ljava/lang/String;");
        jstring cause = (jstring) JNIenv->CallObjectMethod(exception, throwableToString);
        const char *text = JNIenv->GetStringUTFChars(cause, 0);
        VStringBuffer message("javaembed: %s", text);
        JNIenv->ReleaseStringUTFChars(cause, text);
        rtlFail(0, message.str());
    }
}

//-------------------------------------------

// A JavaObject accessor has common functionality shared by both the builders below (Java-> ECL and ECL->Java)

class JavaObjectAccessor : public CInterface
{
protected:
    JavaObjectAccessor(JNIEnv *_JNIenv, const RtlFieldInfo *_outerRow, jobject _row)
    : JNIenv(_JNIenv), row(_row), outerRow(_outerRow), idx(0), limit(0), inSet(false), inDataSet(false)
    {
        Class = (jclass) JNIenv->NewGlobalRef(JNIenv->GetObjectClass(row));
    }
    JavaObjectAccessor(JNIEnv *_JNIenv, const RtlFieldInfo *_outerRow)
    : JNIenv(_JNIenv), outerRow(_outerRow), idx(0), limit(0), inSet(false), inDataSet(false)
    {
        row = NULL;
        Class = NULL;
    }
    ~JavaObjectAccessor()
    {
        // Unwind anything left on the stack (in case we had exceptions), to make sure the Class we release is the global one
        if (stack.length())
            Class = (jclass) stack.item(0);
        if (Class)
            JNIenv->DeleteGlobalRef(Class);
    }
    void push()
    {
        stack.append(Class);
        stack.append(row);
    }
    void pop()
    {
        row = (jobject) stack.pop();
        Class = (jclass) stack.pop();
    }
    jfieldID getFieldId(const RtlFieldInfo * field, const char *sig, const char *expected)
    {
        // MORE - if we are going to stream a dataset we really should be caching these somehow
        JNIenv->ExceptionClear();
        jfieldID fieldId = 0;
        if (sig)
        {
            if (inSet)
            {
                VStringBuffer arraySig("[%s", sig);
                fieldId = JNIenv->GetFieldID(Class, field->name->getAtomNamePtr(), arraySig.str());
            }
            else
                fieldId = JNIenv->GetFieldID(Class, field->name->getAtomNamePtr(), sig);
        }
        else
        {
            // Do it the hard way via reflection API
            // Equivalent java:
            // Field field = object.getClass().getDeclaredField(fieldName);
            jclass classClass =JNIenv->GetObjectClass(Class);
            checkException();
            jmethodID getDeclaredField = JNIenv->GetMethodID(classClass, "getDeclaredField", "(Ljava/lang/String;)Ljava/lang/reflect/Field;" );
            checkException();
            jstring fieldName = JNIenv->NewStringUTF(field->name->getAtomNamePtr());
            checkException();
            jobject reflectedField = JNIenv->CallObjectMethod(Class, getDeclaredField, fieldName);
            checkException();
            fieldId = JNIenv->FromReflectedField(reflectedField);
        }
        if (!fieldId && expected)
            throw MakeStringException(0, "javaembed: Unable to retrieve field %s of type %s", field->name->getAtomNamePtr(), expected);
        if (expected)
            checkException();
        else
            JNIenv->ExceptionClear();
        return fieldId;
    }
    void checkException()
    {
        javaembed::checkException(JNIenv);
    }

    JNIEnv *JNIenv;
    jobject row;
    const RtlFieldInfo *outerRow;
    jclass Class;
    ConstPointerArray stack;
    unsigned idx;
    UnsignedArray idxStack;
    unsigned limit;
    bool inSet;
    bool inDataSet;
};

// A JavaRowBuilder object is used to construct an ECL row from a Java object

class JavaRowBuilder : public JavaObjectAccessor, implements IFieldSource
{
public:
    IMPLEMENT_IINTERFACE;

    JavaRowBuilder(JNIEnv *_JNIenv, const RtlFieldInfo *_outerRow, jobject _row)
    : JavaObjectAccessor(_JNIenv, _outerRow, _row)
    {
    }
    virtual bool getBooleanResult(const RtlFieldInfo *field)
    {
        jboolean b;
        if (inSet)
        {
            JNIenv->GetBooleanArrayRegion((jbooleanArray) row, idx, 1, &b);
        }
        else
        {
            jfieldID fieldId = getFieldId(field, "Z", "boolean");
            b = JNIenv->GetBooleanField(row, fieldId);
        }
        checkException();
        return b;
    }
    virtual void getDataResult(const RtlFieldInfo *field, size32_t &__len, void * &__result)
    {
        jbyteArray array;
        if (inSet)
        {
            array = (jbyteArray) JNIenv->GetObjectArrayElement((jobjectArray) row, idx);
        }
        else
        {
            jfieldID fieldId = getFieldId(field, "[B", "DATA");
            array = (jbyteArray) JNIenv->GetObjectField(row, fieldId);
        }
        checkException();
        __len = (array != NULL ? JNIenv->GetArrayLength(array) : 0);
        __result = (__len > 0 ? rtlMalloc(__len) : NULL);
        if (__result)
            JNIenv->GetByteArrayRegion(array, 0, __len, (jbyte *) __result);
        checkException();
    }
    virtual double getRealResult(const RtlFieldInfo *field)
    {
        double d;
        if (inSet)
        {
            float f;
            switch (field->size(NULL, NULL))
            {
            case 4:
                JNIenv->GetFloatArrayRegion((jfloatArray) row, idx, 1, &f);
                d = f;
                break;
            case 8:
                JNIenv->GetDoubleArrayRegion((jdoubleArray) row, idx, 1, &d);
                break;
            default:
                throwUnexpected();
            }
        }
        else
        {
            jfieldID fieldId;
            switch (field->size(NULL, NULL))
            {
            case 4:
                fieldId = getFieldId(field, "F", "float");
                d = JNIenv->GetFloatField(row, fieldId);
                break;
            case 8:
                fieldId = getFieldId(field, "D", "double");
                d = JNIenv->GetDoubleField(row, fieldId);
                break;
            default:
                throwUnexpected();
            }
        }
        checkException();
        return d;
    }
    virtual __int64 getSignedResult(const RtlFieldInfo *field)
    {
        __int64 ret;
        if (inSet)
        {
            jbyte b;
            jshort s;
            jint i;
            jlong l;
            switch (field->size(NULL, NULL))
            {
            case 1:
                JNIenv->GetByteArrayRegion((jbyteArray) row, idx, 1, &b);
                ret = b;
                break;
            case 2:
                JNIenv->GetShortArrayRegion((jshortArray) row, idx, 1, &s);
                ret = s;
                break;
            case 4:
                JNIenv->GetIntArrayRegion((jintArray) row, idx, 1, &i);
                ret = i;
                break;
            case 8:
                JNIenv->GetLongArrayRegion((jlongArray) row, idx, 1, &l);
                ret = l;
                break;
            default:
                UNSUPPORTED("non-standard integer sizes");
            }
        }
        else
        {
            jfieldID fieldId;
            switch (field->size(NULL, NULL))
            {
            case 1:
                fieldId = getFieldId(field, "B", "byte");
                ret = JNIenv->GetByteField(row, fieldId);
                break;
            case 2:
                fieldId = getFieldId(field, "S", "short");
                ret = JNIenv->GetShortField(row, fieldId);
                break;
            case 4:
                fieldId = getFieldId(field, "I", "int");
                ret = JNIenv->GetIntField(row, fieldId);
                break;
            case 8:
                fieldId = getFieldId(field, "J", "long");
                ret = JNIenv->GetLongField(row, fieldId);
                break;
            default:
                UNSUPPORTED("non-standard integer sizes");
            }
        }
        checkException();
        return ret;
    }
    virtual unsigned __int64 getUnsignedResult(const RtlFieldInfo *field)
    {
        UNSUPPORTED("unsigned fields");  // No unsigned types in Java
    }
    virtual void getStringResult(const RtlFieldInfo *field, size32_t &__len, char * &__result)
    {
        jstring result;
        if (inSet)
        {
            // MORE - set of string1 mapping to Java array of char ? Not sure it's worth it.
            result = (jstring) JNIenv->GetObjectArrayElement((jobjectArray) row, idx);
        }
        else
        {
            if (field->isFixedSize() && field->size(NULL, NULL)==1)
            {
                // See if there's a char field
                jfieldID charFieldId = getFieldId(field, "C", NULL);
                if (charFieldId)
                {
                    jchar resultChar = JNIenv->GetCharField(row, charFieldId);
                    rtlUnicodeToStrX(__len, __result, 1, &resultChar);
                    return;
                }
            }
            jfieldID fieldId = getFieldId(field, "Ljava/lang/String;", "String");
            result = (jstring) JNIenv->GetObjectField(row, fieldId);
        }
        if (!result)
        {
            __len = 0;
            __result = NULL;
            return;
        }
        size_t size = JNIenv->GetStringUTFLength(result);  // in bytes
        const char *text =  JNIenv->GetStringUTFChars(result, NULL);
        size32_t chars = rtlUtf8Length(size, text);
        rtlUtf8ToStrX(__len, __result, chars, text);
        JNIenv->ReleaseStringUTFChars(result, text);
        JNIenv->DeleteLocalRef(result);
    }
    virtual void getUTF8Result(const RtlFieldInfo *field, size32_t &__len, char * &__result)
    {
        jstring result;
        if (inSet)
        {
            // MORE - set of string1 mapping to Java array of char ? Not sure it's worth it.
            result = (jstring) JNIenv->GetObjectArrayElement((jobjectArray) row, idx);
        }
        else
        {
            if (field->isFixedSize() && field->size(NULL, NULL)==1)
            {
                // See if there's a char field
                jfieldID charFieldId = getFieldId(field, "C", NULL);
                if (charFieldId)
                {
                    jchar resultChar = JNIenv->GetCharField(row, charFieldId);
                    rtlUnicodeToUtf8X(__len, __result, 1, &resultChar);
                    return;
                }
            }
            jfieldID fieldId = getFieldId(field, "Ljava/lang/String;", "String");
            result = (jstring) JNIenv->GetObjectField(row, fieldId);
        }
        if (!result)
        {
            __len = 0;
            __result = NULL;
            return;
        }
        size_t size = JNIenv->GetStringUTFLength(result);  // in bytes
        const char *text =  JNIenv->GetStringUTFChars(result, NULL);
        size32_t chars = rtlUtf8Length(size, text);
        rtlUtf8ToUtf8X(__len, __result, chars, text);
        JNIenv->ReleaseStringUTFChars(result, text);
        JNIenv->DeleteLocalRef(result);
    }
    virtual void getUnicodeResult(const RtlFieldInfo *field, size32_t &__len, UChar * &__result)
    {
        jstring result;
        if (inSet)
        {
            // MORE - set of string1 mapping to Java array of char ? Not sure it's worth it.
            result = (jstring) JNIenv->GetObjectArrayElement((jobjectArray) row, idx);
        }
        else
        {
            if (field->isFixedSize() && field->size(NULL, NULL)==1)
            {
                // See if there's a char field
                jfieldID charFieldId = getFieldId(field, "C", NULL);
                if (charFieldId)
                {
                    jchar resultChar = JNIenv->GetCharField(row, charFieldId);
                    rtlUnicodeToUnicodeX(__len, __result, 1, &resultChar);
                    return;
                }
            }
            jfieldID fieldId = getFieldId(field, "Ljava/lang/String;", "String");
            result = (jstring) JNIenv->GetObjectField(row, fieldId);
        }
        if (!result)
        {
            __len = 0;
            __result = NULL;
            return;
        }
        size_t size = JNIenv->GetStringUTFLength(result);  // in bytes
        const char *text =  JNIenv->GetStringUTFChars(result, NULL);
        size32_t chars = rtlUtf8Length(size, text);
        rtlUtf8ToUnicodeX(__len, __result, chars, text);
        JNIenv->ReleaseStringUTFChars(result, text);
        JNIenv->DeleteLocalRef(result);
    }
    virtual void getDecimalResult(const RtlFieldInfo *field, Decimal &value)
    {
        double ret = getRealResult(field);
        value.setReal(ret);
    }

    virtual void processBeginSet(const RtlFieldInfo * field, bool &isAll)
    {
        isAll = false;  // No concept of an 'all' set in Java
        push();
        jfieldID fieldId = getFieldId(field, NULL, "object");  // We assume it will be an array, but not sure of what...
        row = JNIenv->GetObjectField(row, fieldId);
        inSet = true;
        idx = -1;  // First call to next() increments it to 0
        limit = row != NULL ? JNIenv->GetArrayLength((jarray) row) : 0;
        checkException();
    }
    virtual bool processNextSet(const RtlFieldInfo * field)
    {
        assertex(inSet);
        idx++;
        return idx < limit;
    }
    virtual void processBeginDataset(const RtlFieldInfo * field)
    {
        push();
        jfieldID fieldId = getFieldId(field, NULL, "object");  // We assume it will be an array, but not sure of what...
        row = JNIenv->GetObjectField(row, fieldId);
        inDataSet = true;
        idx = -1;  // First call to next() increments it to 0
        limit = row != NULL ? JNIenv->GetArrayLength((jarray) row) : 0;
        checkException();
    }
    virtual void processBeginRow(const RtlFieldInfo * field)
    {
        if (field != outerRow)
        {
            push();
            if (inDataSet)
            {
                row = JNIenv->GetObjectArrayElement((jobjectArray) row, idx);
            }
            else
            {
                jfieldID fieldId = getFieldId(field, NULL, "object");
                row = JNIenv->GetObjectField(row, fieldId);
            }
            if (!row)
                rtlFail(0, "javaembed: child dataset object should not be NULL");
            Class = JNIenv->GetObjectClass(row);
        }
    }
    virtual bool processNextRow(const RtlFieldInfo * field)
    {
        assertex(inDataSet);
        idx++;
        return idx < limit;
    }
    virtual void processEndSet(const RtlFieldInfo * field)
    {
        inSet = false;
        JNIenv->DeleteLocalRef(row);
        pop();
    }
    virtual void processEndDataset(const RtlFieldInfo * field)
    {
        inDataSet = false;
        JNIenv->DeleteLocalRef(row);
        pop();
    }
    virtual void processEndRow(const RtlFieldInfo * field)
    {
        if (field != outerRow)
        {
            JNIenv->DeleteLocalRef(row);
            JNIenv->DeleteLocalRef(Class);
            pop();
        }
    }
};

//-------------------------------------------

// A JavaObjectBuilder object is used to construct a Java object from an ECL row

class JavaObjectBuilder : public JavaObjectAccessor, implements IFieldProcessor
{
public:
    IMPLEMENT_IINTERFACE;
    JavaObjectBuilder(JNIEnv *_JNIenv, const RtlFieldInfo *_outerRow, const char *className)
    : JavaObjectAccessor(_JNIenv, _outerRow)
    {
        JNIenv->ExceptionClear();
        Class = (jclass) JNIenv->NewGlobalRef(JNIenv->FindClass(className));  // MORE - should use the custom classloader, once that fix is merged
        checkException();
        setConstructor();
    }
    virtual void processString(unsigned numchars, const char *text, const RtlFieldInfo * field)
    {
        if (field->isFixedSize() && field->size(NULL, NULL)==1 && !inSet)  // SET OF STRING1 is not mapped to array of char...
        {
            // See if there's a char field
            jfieldID charFieldId = getFieldId(field, "C", NULL);
            if (charFieldId)
            {
                assertex(numchars==1);
                jchar c;
                rtlStrToUnicode(1, &c, 1, text);
                JNIenv->SetCharField(row, charFieldId, c);
                checkException();
                return;
            }
        }
        jfieldID fieldId = getFieldId(field, "Ljava/lang/String;", "String");
        size32_t numchars16;
        rtlDataAttr unicode16;
        rtlStrToUnicodeX(numchars16, unicode16.refustr(), numchars, text);
        jstring value = JNIenv->NewString(unicode16.getustr(), numchars16);
        checkException();
        if (inSet)
            JNIenv->SetObjectArrayElement((jobjectArray) row, idx, value);
        else
            JNIenv->SetObjectField(row, fieldId, value);
        JNIenv->DeleteLocalRef(value);
        checkException();
    }
    virtual void processBool(bool value, const RtlFieldInfo * field)
    {
        jfieldID fieldId = getFieldId(field, "Z", "boolean");
        JNIenv->SetBooleanField(row, fieldId, value);
        checkException();
    }
    virtual void processData(unsigned len, const void *value, const RtlFieldInfo * field)
    {
        jfieldID fieldId = getFieldId(field, "[B", "data");
        jbyteArray javaData = JNIenv->NewByteArray(len);
        JNIenv->SetByteArrayRegion(javaData, 0, len, (jbyte *) value);
        checkException();
        if (inSet)
            JNIenv->SetObjectArrayElement((jobjectArray) row, idx, javaData);
        else
            JNIenv->SetObjectField(row, fieldId, javaData);
        checkException();
    }
    virtual void processInt(__int64 value, const RtlFieldInfo * field)
    {
        jfieldID fieldId;
        switch (field->size(NULL, NULL))
        {
        case 1:
            fieldId = getFieldId(field, "B", "byte");
            JNIenv->SetByteField(row, fieldId, value);
            break;
        case 2:
            fieldId = getFieldId(field, "S", "short");
            JNIenv->SetShortField(row, fieldId, value);
            break;
        case 4:
            fieldId = getFieldId(field, "I", "int");
            JNIenv->SetIntField(row, fieldId, value);
            break;
        case 8:
            fieldId = getFieldId(field, "J", "long");
            JNIenv->SetLongField(row, fieldId, value);
            break;
        default:
            UNSUPPORTED("non-standard integer sizes");
            break;
        }
        checkException();
    }
    virtual void processUInt(unsigned __int64 value, const RtlFieldInfo * field)
    {
        UNSUPPORTED("unsigned fields");  // No unsigned types in Java
    }
    virtual void processReal(double value, const RtlFieldInfo * field)
    {
        jfieldID fieldId;
        switch (field->size(NULL, NULL))
        {
        case 4:
            fieldId = getFieldId(field, "F", "float");
            JNIenv->SetFloatField(row, fieldId, (float) value);
            break;
        case 8:
            fieldId = getFieldId(field, "D", "double");
            JNIenv->SetDoubleField(row, fieldId, value);
            break;
        default:
            throwUnexpected();
        }
        checkException();
    }
    virtual void processDecimal(const void *value, unsigned digits, unsigned precision, const RtlFieldInfo * field)
    {
        // we could map to doubles, but probably better to let the ECL programmer do that themselves
        UNSUPPORTED("DECIMAL fields");
    }
    virtual void processUDecimal(const void *value, unsigned digits, unsigned precision, const RtlFieldInfo * field)
    {
        UNSUPPORTED("UDECIMAL fields");
    }
    virtual void processUnicode(unsigned numchars, const UChar *text, const RtlFieldInfo * field)
    {
        jfieldID fieldId = getFieldId(field, "Ljava/lang/String;", "String");
        jstring value = JNIenv->NewString(text, numchars);
        checkException();
        if (inSet)
            JNIenv->SetObjectArrayElement((jobjectArray) row, idx, value);
        else
            JNIenv->SetObjectField(row, fieldId, value);
        JNIenv->DeleteLocalRef(value);
        checkException();
    }
    virtual void processQString(unsigned len, const char *value, const RtlFieldInfo * field)
    {
        size32_t charCount;
        rtlDataAttr text;
        rtlQStrToStrX(charCount, text.refstr(), len, value);
        processString(charCount, text.getstr(), field);
    }
    virtual void processUtf8(unsigned numchars, const char *text, const RtlFieldInfo * field)
    {
        jfieldID fieldId = getFieldId(field, "Ljava/lang/String;", "String");
        size32_t numchars16;
        rtlDataAttr unicode16;
        rtlUtf8ToUnicodeX(numchars16, unicode16.refustr(), numchars, text);
        jstring value = JNIenv->NewString(unicode16.getustr(), numchars16);
        checkException();
        if (inSet)
            JNIenv->SetObjectArrayElement((jobjectArray) row, idx, value);
        else
            JNIenv->SetObjectField(row, fieldId, value);
        JNIenv->DeleteLocalRef(value);
        checkException();
    }

    virtual bool processBeginSet(const RtlFieldInfo * field, unsigned numElems, bool isAll, const byte *data)
    {
        push();
        idx = 0;
        limit = numElems;
        const char *javaTypeSignature = NULL;
        bool processElements = false;
        // row needs to be created as an array of <whatever>
        if (isAll)
            UNSUPPORTED("ALL sets");
        const RtlTypeInfo *childType = field->type->queryChildType();
        jobject newRow;
        switch(childType->fieldType & RFTMkind)
        {
        case type_boolean:
            newRow = JNIenv->NewBooleanArray(numElems);
            JNIenv->SetBooleanArrayRegion((jbooleanArray) newRow, 0, numElems, (jboolean *) data);
            javaTypeSignature = "[Z";
            break;
        case type_int:
            if (childType->fieldType & RFTMunsigned)
                UNSUPPORTED("unsigned integers");
            switch (childType->length)
            {
            case 1:
                newRow = JNIenv->NewByteArray(numElems);
                JNIenv->SetByteArrayRegion((jbyteArray) newRow, 0, numElems, (jbyte *) data);
                javaTypeSignature = "[B";
                break;
            case 2:
                newRow = JNIenv->NewShortArray(numElems);
                JNIenv->SetShortArrayRegion((jshortArray) newRow, 0, numElems, (jshort *) data);
                javaTypeSignature = "[S";
                break;
            case 4:
                newRow = JNIenv->NewIntArray(numElems);
                JNIenv->SetIntArrayRegion((jintArray) newRow, 0, numElems, (jint *) data);
                javaTypeSignature = "[I";
                break;
            case 8:
                newRow = JNIenv->NewLongArray(numElems);
                JNIenv->SetLongArrayRegion((jlongArray) newRow, 0, numElems, (jlong *) data);
                javaTypeSignature = "[J";
                break;
            default:
                UNSUPPORTED("non-standard integer sizes");
                break;
            }
            break;
        case type_real:
            switch (childType->length)
            {
            case 4:
                newRow = JNIenv->NewFloatArray(numElems);
                JNIenv->SetFloatArrayRegion((jfloatArray) newRow, 0, numElems, (float *) data);
                javaTypeSignature = "[F";
                break;
            case 8:
                newRow = JNIenv->NewDoubleArray(numElems);
                JNIenv->SetDoubleArrayRegion((jdoubleArray) newRow, 0, numElems, (double *) data);
                javaTypeSignature = "[D";
                break;
            default:
                throwUnexpected();
                break;
            }
            break;
        case type_string:
        case type_varstring:
        case type_unicode:
        case type_utf8:
            newRow = JNIenv->NewObjectArray(numElems, JNIenv->FindClass("java/lang/String"), NULL);
            javaTypeSignature = "[Ljava/lang/String;";
            processElements = true;
            break;
        case type_data:
            newRow = JNIenv->NewObjectArray(numElems, JNIenv->FindClass("[B"), NULL);
            javaTypeSignature = "[[B";
            processElements = true;
            break;
        default:
            throwUnexpected();
        }
        checkException();
        jfieldID fieldId = getFieldId(field, javaTypeSignature, "Array");
        JNIenv->SetObjectField(row, fieldId, newRow);
        row = newRow;
        inSet = true;
        return processElements;
    }
    virtual bool processBeginDataset(const RtlFieldInfo * field, unsigned numRows)
    {
        push();
        idxStack.append(idx);
        idx = 0;
        inDataSet = true;
        // Create an empty array
        jfieldID childId = getFieldId( field, NULL, "RECORD");
        jobject newRow = NULL;
        if (numRows)
        {
            jclass arrayClass = getClassForChild(childId);
            jmethodID isArrayMethod = JNIenv->GetMethodID(JNIenv->GetObjectClass(arrayClass), "isArray", "()Z" );
            checkException();
            if (!JNIenv->CallBooleanMethod(arrayClass, isArrayMethod))
            {
                JNIenv->ExceptionClear();
                VStringBuffer message("javaembed: Array expected for field %s", field->name->getAtomNamePtr());
                rtlFail(0, message.str());
            }
            // Set up constructor etc for the child rows, so we don't do it per row
            jmethodID getTypeMethod = JNIenv->GetMethodID(JNIenv->GetObjectClass(arrayClass), "getComponentType", "()Ljava/lang/Class;" );
            checkException();
            Class = (jclass) JNIenv->CallObjectMethod(arrayClass, getTypeMethod);
            checkException();
            setConstructor();
            // Now we need to create the array
            newRow = JNIenv->NewObjectArray(numRows, Class, NULL);
            checkException();
        }
        JNIenv->SetObjectField(row, childId, newRow);
        checkException();
        row = newRow;

        return true;
    }
    virtual bool processBeginRow(const RtlFieldInfo * field)
    {
        if (field == outerRow)
            row = JNIenv->NewObject(Class, constructor);
        else
        {
            push();
            stack.append(constructor);
            // Now we have to create the child object
            jobject newRow = NULL;
            if (inDataSet)
            {
                newRow = JNIenv->NewObject(Class, constructor);
                checkException();
                JNIenv->SetObjectArrayElement((jobjectArray) row, idx++, newRow);
            }
            else
            {
                // All this is done once per dataset in the nested dataset case. But for embedded record case we have to do it here
                jfieldID childId = getFieldId( field, NULL, "RECORD");
                Class = getClassForChild(childId);
                setConstructor();
                newRow = JNIenv->NewObject(Class, constructor);
                checkException();
                JNIenv->SetObjectField(row, childId, newRow);
            }
            row = newRow;
        }
        checkException();
        return true;
    }
    virtual void processEndSet(const RtlFieldInfo * field)
    {
        JNIenv->DeleteLocalRef(row);
        pop();
        inSet = false;
    }
    virtual void processEndDataset(const RtlFieldInfo * field)
    {
        inDataSet = false;
        idx = idxStack.pop();
        pop();
    }
    virtual void processEndRow(const RtlFieldInfo * field)
    {
        if (field != outerRow)
        {
            constructor = (jmethodID) stack.pop();
            JNIenv->DeleteLocalRef(row);
            pop();
        }
    }
    inline jobject getObject()
    {
        return row;
    }
protected:
    jclass getClassForChild(jfieldID childId)
    {
        jobject reflectedField = JNIenv->ToReflectedField(Class, childId, false);
        checkException();
        jclass fieldClass =JNIenv->GetObjectClass(reflectedField);
        checkException();
        jmethodID getTypeMethod = JNIenv->GetMethodID(fieldClass, "getType", "()Ljava/lang/Class;" );
        checkException();
        jclass result = (jclass) JNIenv->CallObjectMethod(reflectedField, getTypeMethod);
        checkException();
        JNIenv->DeleteLocalRef(reflectedField);
        JNIenv->DeleteLocalRef(fieldClass);
        return result;

    }
    void setConstructor()
    {
        constructor = JNIenv->GetMethodID(Class, "<init>", "()V");
        if (!constructor)
        {
            JNIenv->ExceptionClear();
            jmethodID getNameMethod = JNIenv->GetMethodID(JNIenv->GetObjectClass(Class), "getName", "()Ljava/lang/String;" );
            checkException();
            jstring name = (jstring) JNIenv->CallObjectMethod(Class, getNameMethod);
            checkException();
            const char *nameText = JNIenv->GetStringUTFChars(name, NULL);
            VStringBuffer message("javaembed: no suitable constructor for field %s", nameText);
            JNIenv->ReleaseStringUTFChars(name, nameText);
            rtlFail(0, message.str());
        }
    }
    jmethodID constructor;
};

//----------------------------------------------------------------------

// Wrap an IRowStream into a Java Iterator

class ECLDatasetIterator : public CInterfaceOf<IInterface>
{
public:
    ECLDatasetIterator(JNIEnv *JNIenv, const RtlTypeInfo *_typeInfo, const char *className, IRowStream * _val)
    : typeInfo(_typeInfo), val(_val),
      dummyField("<row>", NULL, typeInfo),
      javaBuilder(JNIenv, &dummyField, className)
    {
        nextRead = false;
        nextPending = NULL;
    }

    bool hasNext()
    {
        if (!nextRead)
        {
            nextPending = (const byte *) val->ungroupedNextRow();
            nextRead = true;
            if (!nextPending)
                val->stop();
        }
        return nextPending != NULL;
    }

    jobject next()
    {
        if (!hasNext())
            return NULL;
        typeInfo->process(nextPending, nextPending, &dummyField, javaBuilder); // Creates a java object from the incoming ECL row
        nextRead = false;
        return javaBuilder.getObject();
    }
protected:
    const RtlTypeInfo *typeInfo;  // Not linked (or linkable)
    Linked<IRowStream> val;
    RtlFieldStrInfo dummyField;
    JavaObjectBuilder javaBuilder;
    const byte *nextPending;
    bool nextRead;
};

//-------------------------------------------

// A Java function that returns a dataset will return a JavaRowStream object that can be
// interrogated to return each row of the result in turn
// Note that we can't cache the JNIEnv here - calls may be made on different threads (though not at the same time).

static JNIEnv *queryJNIEnv();

class JavaRowStream : public CInterfaceOf<IRowStream>
{
public:
    JavaRowStream(jobject _iterator, IEngineRowAllocator *_resultAllocator)
    : resultAllocator(_resultAllocator)
    {
        iterator = queryJNIEnv()->NewGlobalRef(_iterator);
    }
    virtual const void *nextRow()
    {
        if (!iterator)
            return NULL;
        JNIEnv *JNIenv = queryJNIEnv();
        // Java code would be
        // if (!iterator.hasNext)
        // {
        //    stop();
        //    return NULL;
        // }
        // result = iterator.next();
        jclass iterClass =JNIenv->GetObjectClass(iterator);
        javaembed::checkException(JNIenv);
        jmethodID hasNextMethod = JNIenv->GetMethodID(iterClass, "hasNext", "()Z" );
        javaembed::checkException(JNIenv);
        jboolean hasNext = JNIenv->CallBooleanMethod(iterator, hasNextMethod);
        if (!hasNext)
        {
            stop();
            return NULL;
        }
        jmethodID nextMethod = JNIenv->GetMethodID(iterClass, "next", "()Ljava/lang/Object;" );
        javaembed::checkException(JNIenv);
        jobject result = JNIenv->CallObjectMethod(iterator, nextMethod);
        RtlDynamicRowBuilder rowBuilder(resultAllocator);
        const RtlTypeInfo *typeInfo = resultAllocator->queryOutputMeta()->queryTypeInfo();
        assertex(typeInfo);
        RtlFieldStrInfo dummyField("<row>", NULL, typeInfo);
        JavaRowBuilder javaRowBuilder(queryJNIEnv(), &dummyField, result);
        size32_t len = typeInfo->build(rowBuilder, 0, &dummyField, javaRowBuilder);
        return rowBuilder.finalizeRowClear(len);
    }
    virtual void stop()
    {
        resultAllocator.clear();
        if (iterator)
        {
            queryJNIEnv()->DeleteGlobalRef(iterator);
            iterator = NULL;
        }
    }

protected:
    Linked<IEngineRowAllocator> resultAllocator;
    jobject iterator;;
};


//-------------------------------------------

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
        prevClassPath.set("dummy");  // Forces the call below to actually do something...
        setThreadClassLoader("");
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

    jobject getSystemClassLoader()
    {
        JNIenv->ExceptionClear();
        jclass javaLangClassLoaderClass = JNIenv->FindClass("java/lang/ClassLoader");
        checkException();
        jmethodID getSystemClassLoaderMethod = JNIenv->GetStaticMethodID(javaLangClassLoaderClass, "getSystemClassLoader", "()Ljava/lang/ClassLoader;");
        checkException();
        jobject systemClassLoaderObj = JNIenv->CallStaticObjectMethod(javaLangClassLoaderClass, getSystemClassLoaderMethod);
        checkException();
        assertex(systemClassLoaderObj);
        return systemClassLoaderObj;
    }

    void setThreadClassLoader(jobject classLoader)
    {
        JNIenv->ExceptionClear();
        jclass javaLangThreadClass = JNIenv->FindClass("java/lang/Thread");
        checkException();
        jmethodID currentThreadMethod = JNIenv->GetStaticMethodID(javaLangThreadClass, "currentThread", "()Ljava/lang/Thread;");
        checkException();
        jobject threadObj = JNIenv->CallStaticObjectMethod(javaLangThreadClass, currentThreadMethod);
        checkException();
        jmethodID setContextClassLoaderMethod = JNIenv->GetMethodID(javaLangThreadClass, "setContextClassLoader", "(Ljava/lang/ClassLoader;)V");
        checkException();
        JNIenv->CallObjectMethod(threadObj, setContextClassLoaderMethod, classLoader);
        checkException();
    }

    jobject getThreadClassLoader()
    {
        JNIenv->ExceptionClear();
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
        assertex(contextClassLoaderObj);
        return contextClassLoaderObj;
    }

    void setThreadClassLoader(const char *classPath)
    {
        if (classPath && *classPath)
        {
            if (prevClassPath && strcmp(classPath, prevClassPath) == 0)
                return;
            jclass URLcls = JNIenv->FindClass("java/net/URL");
            checkException();
            jmethodID URLclsMid = JNIenv->GetMethodID(URLcls, "<init>","(Ljava/lang/String;)V");
            checkException();
            StringArray paths;
            paths.appendList(classPath, ";");  // NOTE - as we need to be able to include : in the urls, we can't use ENVSEP here
            jobjectArray URLArray = JNIenv->NewObjectArray(paths.length(), URLcls, NULL);
            ForEachItemIn(idx, paths)
            {
                StringBuffer usepath;
                const char *path = paths.item(idx);
                if (!strchr(path, ':'))
                    usepath.append("file:");
                usepath.append(path);
                jstring jstr = JNIenv->NewStringUTF(usepath.str());
                checkException();
                jobject URLobj = JNIenv->NewObject(URLcls, URLclsMid, jstr);
                checkException();
                JNIenv->SetObjectArrayElement(URLArray, idx, URLobj);
                JNIenv->DeleteLocalRef(URLobj);
                JNIenv->DeleteLocalRef(jstr);
            }
            checkException();
            jclass customLoaderClass = JNIenv->FindClass("java/net/URLClassLoader");
            checkException();
            jmethodID newInstance = JNIenv->GetStaticMethodID(customLoaderClass, "newInstance","([Ljava/net/URL;Ljava/lang/ClassLoader;)Ljava/net/URLClassLoader;");
            checkException();
            jobject contextClassLoaderObj = JNIenv->NewGlobalRef(JNIenv->CallStaticObjectMethod(customLoaderClass, newInstance, URLArray, getSystemClassLoader()));
            checkException();
            assertex(contextClassLoaderObj);
            setThreadClassLoader(contextClassLoaderObj);
            prevClassPath.set(classPath);
        }
        else
        {
            if (prevClassPath)
                setThreadClassLoader(getSystemClassLoader());
            prevClassPath.clear();
        }
    }

    inline void importFunction(size32_t lenChars, const char *utf, const char *options)
    {
        size32_t bytes = rtlUtf8Size(lenChars, utf);
        StringBuffer text(bytes, utf);
        setThreadClassLoader(options);
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
            // We need to patch up the provided signature - any instances of <classname; need to be replaced by Ljava.utils.iterator
            StringBuffer javaSignature;
            const char *finger = signature;
            while (*finger)
            {
                if (*finger == '<')
                {
                    javaSignature.append("Ljava/util/Iterator;");
                    finger = strchr(finger, ';');
                    if (!finger)
                        throw MakeStringException(MSGAUD_user, 0, "javaembed: Invalid java function signature %s", signature);
                }
                else
                    javaSignature.append(*finger);
                finger++;
            }
            if (javaClass)
                JNIenv->DeleteGlobalRef(javaClass);

            jobject classLoader = getThreadClassLoader();
            jmethodID loadClassMethod = JNIenv->GetMethodID(JNIenv->GetObjectClass(classLoader), "loadClass","(Ljava/lang/String;)Ljava/lang/Class;");
            jstring methodString = JNIenv->NewStringUTF(classname);
            javaClass = (jclass) JNIenv->NewGlobalRef(JNIenv->CallObjectMethod(classLoader, loadClassMethod, methodString));

            if (!javaClass)
                throw MakeStringException(MSGAUD_user, 0, "javaembed: Failed to resolve class name %s", classname.str());
            javaMethodID = JNIenv->GetStaticMethodID(javaClass, methodname, javaSignature);
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
    bool getBooleanResult(jvalue &result)
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
    size32_t getRowResult(jobject result, ARowBuilder &builder)
    {
        const RtlTypeInfo *typeInfo = builder.queryAllocator()->queryOutputMeta()->queryTypeInfo();
        assertex(typeInfo);
        RtlFieldStrInfo dummyField("<row>", NULL, typeInfo);
        JavaRowBuilder javaRowBuilder(JNIenv, &dummyField, result);
        return typeInfo->build(builder, 0, &dummyField, javaRowBuilder);
    }

private:
    StringAttr returnType;
    StringAttr argsig;
    StringAttr prevtext;
    StringAttr prevClassPath;
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
        StringArray opts;
        opts.appendList(options, ",");
        ForEachItemIn(idx, opts)
        {
            const char *opt = opts.item(idx);
            const char *val = strchr(opt, '=');
            if (val)
            {
                StringBuffer optName(val-opt, opt);
                val++;
                if (stricmp(optName, "classpath")==0)
                    classpath.set(val);
                else
                    throw MakeStringException(0, "javaembed: Unknown option %s", optName.str());
            }
        }

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
        return new JavaRowStream(result.l, _resultAllocator);
    }
    virtual byte * getRowResult(IEngineRowAllocator * _resultAllocator)
    {
        RtlDynamicRowBuilder rowBuilder(_resultAllocator);
        size32_t len = sharedCtx->getRowResult(result.l, rowBuilder);
        return (byte *) rowBuilder.finalizeRowClear(len);
    }
    virtual size32_t getTransformResult(ARowBuilder & builder)
    {
        return sharedCtx->getRowResult(result.l, builder);
    }
    virtual void bindBooleanParam(const char *name, bool val)
    {
        if (*argsig != 'Z')
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
        default:
            throwUnexpected();
        }
        argsig++;
        addArg(v);
    }
    virtual void bindRowParam(const char *name, IOutputMetaData & metaVal, byte *val)
    {
        if (*argsig != 'L')  // should tell us the type of the object we need to create to pass in
            typeError("RECORD");
        // Class name is from the char after the L up to the first ;
        const char *tail = strchr(argsig, ';');
        if (!tail)
            typeError("RECORD");
        StringAttr className(argsig+1, tail - (argsig+1));
        argsig = tail+1;
        const RtlTypeInfo *typeInfo = metaVal.queryTypeInfo();
        assertex(typeInfo);
        RtlFieldStrInfo dummyField("<row>", NULL, typeInfo);
        JavaObjectBuilder javaBuilder(sharedCtx->JNIenv, &dummyField, className);
        typeInfo->process(val, val, &dummyField, javaBuilder); // Creates a java object from the incoming ECL row
        jvalue v;
        v.l = javaBuilder.getObject();
        addArg(v);
    }
    virtual void bindDatasetParam(const char *name, IOutputMetaData & metaVal, IRowStream * val)
    {
        jvalue v;
        if (*argsig == '[')
        {
            ++argsig;
            // At present, dataset parameters have to map to arrays. May support iterators later
            if (*argsig != 'L')  // should tell us the type of the object we need to create to pass in
                typeError("DATASET");
            // Class name is from the char after the L up to the first ;
            const char *tail = strchr(argsig, ';');
            if (!tail)
                typeError("RECORD");
            StringAttr className(argsig+1, tail - (argsig+1));
            argsig = tail+1;
            PointerArrayOf<_jobject> allRows;
            const RtlTypeInfo *typeInfo = metaVal.queryTypeInfo();
            assertex(typeInfo);
            RtlFieldStrInfo dummyField("<row>", NULL, typeInfo);
            JavaObjectBuilder javaBuilder(sharedCtx->JNIenv, &dummyField, className);
            for (;;)
            {
                roxiemem::OwnedConstRoxieRow thisRow = val->ungroupedNextRow();
                if (!thisRow)
                    break;
                const byte *brow = (const byte *) thisRow.get();
                typeInfo->process(brow, brow, &dummyField, javaBuilder); // Creates a java object from the incoming ECL row
                allRows.append(javaBuilder.getObject());
            }
            jobjectArray array = sharedCtx->JNIenv->NewObjectArray(allRows.length(), sharedCtx->JNIenv->FindClass(className), NULL);
            ForEachItemIn(idx, allRows)
            {
                sharedCtx->JNIenv->SetObjectArrayElement(array, idx, allRows.item(idx));
            }
            v.l = array;
        }
        else if (*argsig == '<') // An extension to the Java signatures, used to indicate
                                  // that we will pass an iterator that generates objects of the specified type
        {
            ++argsig;
            if (*argsig != 'L')  // should tell us the type of the object we need to create to pass in
                typeError("DATASET");
            // Class name is from the char after the L up to the first ;
            const char *tail = strchr(argsig, ';');
            if (!tail)
                typeError("RECORD");
            StringAttr className(argsig+1, tail - (argsig+1));
            argsig = tail+1;
            // Create a java object of type com.HPCCSystems.HpccUtils - this acts as a proxy for the iterator
            sharedCtx->JNIenv->ExceptionClear();
            jclass proxyClass = sharedCtx->JNIenv->FindClass("com/HPCCSystems/HpccUtils");
            sharedCtx->checkException();
            jmethodID constructor = sharedCtx->JNIenv->GetMethodID(proxyClass, "<init>", "(JLjava/lang/String;)V");
            sharedCtx->checkException();
            jvalue param;
            const RtlTypeInfo *typeInfo = metaVal.queryTypeInfo();
            ECLDatasetIterator *iterator = new ECLDatasetIterator(sharedCtx->JNIenv, typeInfo, className, val);
            param.j = (jlong) iterator;
            iterators.append(*iterator);
            jobject proxy = sharedCtx->JNIenv->NewObject(proxyClass, constructor, param, sharedCtx->JNIenv->NewStringUTF(helperLibraryName));
            sharedCtx->checkException();
            v.l = proxy;
        }
        else
            typeError("DATASET");
        addArg(v);
    }

    virtual void importFunction(size32_t lenChars, const char *utf)
    {
        sharedCtx->importFunction(lenChars, utf, classpath);
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
    StringAttr classpath;
    IArrayOf<ECLDatasetIterator> iterators;   // to make sure they get freed
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

static JavaThreadContext *queryContext()
{
    if (!threadContext)
    {
        threadContext = new JavaThreadContext;
        threadHookChain = addThreadTermFunc(releaseContext);
    }
    return threadContext;
}

static JNIEnv *queryJNIEnv()
{
    return queryContext()->JNIenv;
}

class JavaEmbedContext : public CInterfaceOf<IEmbedContext>
{
public:
    virtual IEmbedFunctionContext *createFunctionContext(bool isImport, const char *options)
    {
        assertex(isImport);
        return new JavaEmbedImportContext(queryContext(), options);
    }
};

extern IEmbedContext* getEmbedContext()
{
    return new JavaEmbedContext;
}

} // namespace

// Callbacks from java

extern "C" {
JNIEXPORT jboolean JNICALL Java_com_HPCCSystems_HpccUtils__1hasNext (JNIEnv *, jclass, jlong);
JNIEXPORT jobject JNICALL Java_com_HPCCSystems_HpccUtils__1next (JNIEnv *, jclass, jlong);
}

JNIEXPORT jboolean JNICALL Java_com_HPCCSystems_HpccUtils__1hasNext (JNIEnv *JNIenv, jclass, jlong proxy)
{
    try
    {
        javaembed::ECLDatasetIterator *e = (javaembed::ECLDatasetIterator *) proxy;
        return e->hasNext();
    }
    catch (IException *E)
    {
        StringBuffer msg;
        E->errorMessage(msg);
        E->Release();
        jclass eClass = JNIenv->FindClass("java/lang/IllegalArgumentException");
        if (eClass)
            JNIenv->ThrowNew(eClass, msg.str());
        return false;
    }
}

JNIEXPORT jobject JNICALL Java_com_HPCCSystems_HpccUtils__1next (JNIEnv *JNIenv, jclass, jlong proxy)
{
    try
    {
        javaembed::ECLDatasetIterator *e = (javaembed::ECLDatasetIterator *) proxy;
        return e->next();
    }
    catch (IException *E)
    {
        StringBuffer msg;
        E->errorMessage(msg);
        E->Release();
        jclass eClass = JNIenv->FindClass("java/lang/IllegalArgumentException");
        if (eClass)
            JNIenv->ThrowNew(eClass, msg.str());
        return NULL;
    }
}
