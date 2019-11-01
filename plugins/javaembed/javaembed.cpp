/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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
#include "junicode.hpp"
#include "hqlplugins.hpp"
#include "deftype.hpp"
#include "eclhelper.hpp"
#include "eclrtl.hpp"
#include "eclrtl_imp.hpp"
#include "rtlfield.hpp"
#include "rtlds_imp.hpp"
#include "jprop.hpp"
#include "build-config.h"
#include "roxiemem.hpp"
#include "nbcd.hpp"
#include "rtlformat.hpp"
#include "esdl_def.hpp"
#include "enginecontext.hpp"

#ifndef _WIN32
 #include <sys/resource.h>
#endif

static const char * compatibleVersions[] = {
    "Java Embed Helper 1.0.0",
    NULL };

static const char *version = "Java Embed Helper 1.0.0";

#ifdef _DEBUG
//#define TRACE_GLOBALREF
//#define TRACE_CLASSFILE
//#define CHECK_JNI
//#define FORCE_GC
/* Note - if you enable CHECK_JNI and see output like:
 *   WARNING in native method: JNI call made without checking exceptions when required to from CallObjectMethodV
 * where for 'from' may be any of several functions, then the cause is likely to be a missing call to checkException()
 * after a call to the named function. One way to find the responsible call is with a breakpoint on checked_jni_CallObjectMethodV
 * The last time that breakpoint is hit before the warning is given should have a stack trace that tells you all you need to know.
 */
#endif

extern "C" DECL_EXPORT bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb)
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

__declspec(noreturn) static void UNSUPPORTED(const char *feature) __attribute__((noreturn));

static void UNSUPPORTED(const char *feature)
{
    throw MakeStringException(-1, "UNSUPPORTED feature: %s not supported in java plugin", feature);
}

namespace javaembed {

static jmethodID throwable_toString;
static jmethodID throwable_getStackTrace;
static jmethodID throwable_getCause;
static jmethodID frame_toString;


static void forceGC(class CheckedJNIEnv* JNIenv);

/**
 * CheckedJNIEnv is a wrapper around JNIEnv that ensures that we check for exceptions after every call (and turn them into C++ exceptions).
 *
 * It should probably be refactored to have the JNIEnv pointer as a member rather than a base class. As it stands, it's possible to cast
 * safely between CheckedJNIEnv * and JNIEnv*
 *
 */
class CheckedJNIEnv : private JNIEnv
{
    template<typename T> T checkException(T a) { checkException(); return a; }
public:
    StringBuffer &getString(StringBuffer &ret, jstring s)
    {
        if (s)
        {
            const char* str = GetStringUTFChars(s, NULL);
            ret.append(str);
            ReleaseStringUTFChars(s, str);
        }
        return ret;
    }
    StringAttr &getString(StringAttr &ret, jstring s)
    {
        if (s)
        {
            const char* str = GetStringUTFChars(s, NULL);
            ret.set(str);
            ReleaseStringUTFChars(s, str);
        }
        return ret;
    }

    void checkUnexpectedException()
    {
        if (JNIEnv::ExceptionCheck())
        {
            DBGLOG("javaembed: Uunexpected java exception while processing exception");
            JNIEnv::ExceptionDescribe();
            JNIEnv::ExceptionClear();
            throwUnexpected();
        }
    }

    void traceException(jthrowable exception)
    {
        // Don't use auto-checking in here, as we are already inside exception-checking code
        jstring msg = (jstring) JNIEnv::CallObjectMethod(exception, throwable_toString);
        checkUnexpectedException();
        const char *text = JNIEnv::GetStringUTFChars(msg, 0);
        DBGLOG("javaembed: exception: %s", text);
        JNIEnv::ReleaseStringUTFChars(msg, text);
        checkUnexpectedException();
        JNIEnv::DeleteLocalRef(msg);

        jobjectArray frames = (jobjectArray) JNIEnv::CallObjectMethod(exception, throwable_getStackTrace);
        checkUnexpectedException();
        jsize length = JNIEnv::GetArrayLength(frames);
        for (jsize i = 0; i < length; i++)
        {
            jobject frame = JNIEnv::GetObjectArrayElement(frames, i);
            checkUnexpectedException();
            msg = (jstring) JNIEnv::CallObjectMethod(frame, frame_toString);
            checkUnexpectedException();
            text = JNIEnv::GetStringUTFChars(msg, 0);
            DBGLOG("javaembed: exception: stack: %s", text);
            JNIEnv::ReleaseStringUTFChars(msg, text);
            checkUnexpectedException();
            JNIEnv::DeleteLocalRef(msg);
            JNIEnv::DeleteLocalRef(frame);
        }
        jthrowable cause = (jthrowable) JNIEnv::CallObjectMethod(exception, throwable_getCause);
        checkUnexpectedException();
        if (cause && cause != exception)
        {
            DBGLOG("javaembed: exception: Caused by:");
            traceException(cause);
        }
    }

    void checkException()
    {
        if (JNIEnv::ExceptionCheck())
        {
            jthrowable exception = JNIEnv::ExceptionOccurred();
            JNIEnv::ExceptionClear();
            traceException(exception);
            jstring cause = (jstring) JNIEnv::CallObjectMethod(exception, throwable_toString);
            JNIEnv::ExceptionClear();
            const char *text = JNIEnv::GetStringUTFChars(cause, 0);
            VStringBuffer message("javaembed: %s", text);
            JNIEnv::ReleaseStringUTFChars(cause, text);
            JNIEnv::ExceptionClear();
            rtlFail(0, message);
        }
    }

    jclass FindClass(const char *name)
    {
        return checkException(JNIEnv::FindClass(name));
    }
    jclass FindGlobalClass(const char *name)
    {
        return (jclass) NewGlobalRef(FindClass(name), "NewGlobalRef");
    }
    void GetBooleanArrayRegion(jbooleanArray array,
                               jsize start, jsize len, jboolean *buf) {
        functions->GetBooleanArrayRegion(this,array,start,len,buf);
        checkException();
    }
    void GetByteArrayRegion(jbyteArray array,
                            jsize start, jsize len, jbyte *buf) {
        functions->GetByteArrayRegion(this,array,start,len,buf);
        checkException();
    }
    void GetCharArrayRegion(jcharArray array,
                            jsize start, jsize len, jchar *buf) {
        functions->GetCharArrayRegion(this,array,start,len,buf);
        checkException();
    }
    void GetShortArrayRegion(jshortArray array,
                             jsize start, jsize len, jshort *buf) {
        functions->GetShortArrayRegion(this,array,start,len,buf);
        checkException();
    }
    void GetIntArrayRegion(jintArray array,
                           jsize start, jsize len, jint *buf) {
        functions->GetIntArrayRegion(this,array,start,len,buf);
        checkException();
    }
    void GetLongArrayRegion(jlongArray array,
                            jsize start, jsize len, jlong *buf) {
        functions->GetLongArrayRegion(this,array,start,len,buf);
        checkException();
    }
    void GetFloatArrayRegion(jfloatArray array,
                             jsize start, jsize len, jfloat *buf) {
        functions->GetFloatArrayRegion(this,array,start,len,buf);
        checkException();
    }
    void GetDoubleArrayRegion(jdoubleArray array,
                              jsize start, jsize len, jdouble *buf) {
        functions->GetDoubleArrayRegion(this,array,start,len,buf);
        checkException();
    }
    jsize GetArrayLength(jarray array) {
        return checkException(functions->GetArrayLength(this,array));
    }
    jsize GetStringUTFLength(jstring str) {
        return checkException(functions->GetStringUTFLength(this,str));
    }
    const char* GetStringUTFChars(jstring str, jboolean *isCopy) {
        return checkException(functions->GetStringUTFChars(this,str,isCopy));
    }
    void ReleaseStringUTFChars(jstring str, const char* chars) {
        functions->ReleaseStringUTFChars(this,str,chars);
        checkException();
    }
    jboolean GetBooleanField(jobject obj, jfieldID fieldID)
    {
        return checkException(JNIEnv::GetBooleanField(obj,fieldID));
    }
    jobject GetObjectField(jobject obj, jfieldID fieldID)
    {
        return checkException(JNIEnv::GetObjectField(obj,fieldID));
    }
    jbyte GetByteField(jobject obj, jfieldID fieldID) {
        return checkException(JNIEnv::GetByteField(obj,fieldID));
    }
    jchar GetCharField(jobject obj, jfieldID fieldID) {
        return checkException(JNIEnv::GetCharField(obj,fieldID));
    }
    jshort GetShortField(jobject obj, jfieldID fieldID) {
        return checkException(JNIEnv::GetShortField(obj,fieldID));
    }
    jint GetIntField(jobject obj, jfieldID fieldID) {
        return checkException(JNIEnv::GetIntField(obj,fieldID));
    }
    jlong GetLongField(jobject obj, jfieldID fieldID) {
        return checkException(JNIEnv::GetLongField(obj,fieldID));
    }
    jfloat GetFloatField(jobject obj, jfieldID fieldID)
    {
        return checkException(JNIEnv::GetFloatField(obj,fieldID));
    }
    jdouble GetDoubleField(jobject obj, jfieldID fieldID)
    {
        return checkException(JNIEnv::GetDoubleField(obj,fieldID));
    }
    jboolean IsSameObject(jobject obj1, jobject obj2)
    {
        return checkException(JNIEnv::IsSameObject(obj1, obj2));
    }
    void SetObjectField(jobject obj, jfieldID fieldID, jobject val) {
        functions->SetObjectField(this,obj,fieldID,val);
        checkException();
    }
    void SetBooleanField(jobject obj, jfieldID fieldID,
                         jboolean val) {
        functions->SetBooleanField(this,obj,fieldID,val);
        checkException();
    }
    void SetByteField(jobject obj, jfieldID fieldID,
                      jbyte val) {
        functions->SetByteField(this,obj,fieldID,val);
        checkException();
    }
    void SetCharField(jobject obj, jfieldID fieldID,
                      jchar val) {
        functions->SetCharField(this,obj,fieldID,val);
        checkException();
    }
    void SetShortField(jobject obj, jfieldID fieldID,
                       jshort val) {
        functions->SetShortField(this,obj,fieldID,val);
        checkException();
    }
    void SetIntField(jobject obj, jfieldID fieldID,
                     jint val) {
        functions->SetIntField(this,obj,fieldID,val);
        checkException();
    }
    void SetLongField(jobject obj, jfieldID fieldID,
                      jlong val) {
        functions->SetLongField(this,obj,fieldID,val);
        checkException();
    }
    void SetFloatField(jobject obj, jfieldID fieldID,
                       jfloat val) {
        functions->SetFloatField(this,obj,fieldID,val);
        checkException();
    }
    void SetDoubleField(jobject obj, jfieldID fieldID,
                        jdouble val) {
        functions->SetDoubleField(this,obj,fieldID,val);
        checkException();
    }

    jstring NewString(const jchar *unicode, jsize len) {
        return checkException(functions->NewString(this,unicode,len));
    }

    jbooleanArray NewBooleanArray(jsize len) {
        return checkException(functions->NewBooleanArray(this,len));
    }
    jbyteArray NewByteArray(jsize len) {
        return checkException(functions->NewByteArray(this,len));
    }
    jcharArray NewCharArray(jsize len) {
        return checkException(functions->NewCharArray(this,len));
    }
    jshortArray NewShortArray(jsize len) {
        return checkException(functions->NewShortArray(this,len));
    }
    jintArray NewIntArray(jsize len) {
        return checkException(functions->NewIntArray(this,len));
    }
    jlongArray NewLongArray(jsize len) {
        return checkException(functions->NewLongArray(this,len));
    }
    jfloatArray NewFloatArray(jsize len) {
        return checkException(functions->NewFloatArray(this,len));
    }
    jdoubleArray NewDoubleArray(jsize len) {
        return checkException(functions->NewDoubleArray(this,len));
    }

    void SetBooleanArrayRegion(jbooleanArray array, jsize start, jsize len,
                               const jboolean *buf) {
        functions->SetBooleanArrayRegion(this,array,start,len,buf);
        checkException();
    }
    void SetByteArrayRegion(jbyteArray array, jsize start, jsize len,
                            const jbyte *buf) {
        functions->SetByteArrayRegion(this,array,start,len,buf);
        checkException();
    }
    void SetCharArrayRegion(jcharArray array, jsize start, jsize len,
                            const jchar *buf) {
        functions->SetCharArrayRegion(this,array,start,len,buf);
        checkException();
    }
    void SetShortArrayRegion(jshortArray array, jsize start, jsize len,
                             const jshort *buf) {
        functions->SetShortArrayRegion(this,array,start,len,buf);
        checkException();
    }
    void SetIntArrayRegion(jintArray array, jsize start, jsize len,
                           const jint *buf) {
        functions->SetIntArrayRegion(this,array,start,len,buf);
        checkException();
    }
    void SetLongArrayRegion(jlongArray array, jsize start, jsize len,
                            const jlong *buf) {
        functions->SetLongArrayRegion(this,array,start,len,buf);
        checkException();
    }
    void SetFloatArrayRegion(jfloatArray array, jsize start, jsize len,
                             const jfloat *buf) {
        functions->SetFloatArrayRegion(this,array,start,len,buf);
        checkException();
    }
    void SetDoubleArrayRegion(jdoubleArray array, jsize start, jsize len,
                              const jdouble *buf) {
        functions->SetDoubleArrayRegion(this,array,start,len,buf);
        checkException();
    }

    jobjectArray NewObjectArray(jsize len, jclass clazz,
                                jobject init) {
        return checkException(functions->NewObjectArray(this,len,clazz,init));
    }
    jobject GetObjectArrayElement(jobjectArray array, jsize index) {
        return checkException(functions->GetObjectArrayElement(this,array,index));
    }
    void SetObjectArrayElement(jobjectArray array, jsize index,
                               jobject val) {
        functions->SetObjectArrayElement(this,array,index,val);
        checkException();
    }

    jobject ToReflectedField(jclass cls, jfieldID fieldID, jboolean isStatic) {
        return checkException(functions->ToReflectedField(this,cls,fieldID,isStatic));
    }
    jboolean IsAssignableFrom(jclass clazz1, jclass clazz2) {
        return checkException(functions->IsAssignableFrom(this,clazz1,clazz2));
    }
    jclass GetObjectClass(jobject obj)
    {
        return checkException(JNIEnv::GetObjectClass(obj));
    }
    jfieldID GetFieldID(jclass clazz, const char *name, const char *sig)
    {
        return checkException(JNIEnv::GetFieldID(clazz,name,sig));
    }
    jfieldID GetFieldIDUnchecked(jclass clazz, const char *name, const char *sig)
    {
        jfieldID ret = JNIEnv::GetFieldID(clazz,name,sig);
        ExceptionClear();
        return ret;
    }
    jmethodID GetMethodID(jclass clazz, const char *name, const char *sig)
    {
        return checkException(JNIEnv::GetMethodID(clazz, name, sig));
    }
    jmethodID GetMethodIDUnchecked(jclass clazz, const char *name, const char *sig)
    {
        jmethodID ret = JNIEnv::GetMethodID(clazz, name, sig);
        ExceptionClear();
        return ret;
    }
    jmethodID GetStaticMethodID(jclass clazz, const char *name, const char *sig)
    {
        return checkException(JNIEnv::GetStaticMethodID(clazz, name, sig));
    }
    void CallStaticVoidMethod(jclass cls, jmethodID methodID, ...) {
        va_list args;
        va_start(args,methodID);
        functions->CallStaticVoidMethodV(this,cls,methodID,args);
        va_end(args);
        checkException();
    }
    jobject CallStaticObjectMethod(jclass clazz, jmethodID methodID, ...)
    {
        va_list args;
        jobject result;
        va_start(args,methodID);
        result = JNIEnv::CallStaticObjectMethodV(clazz,methodID,args);
        va_end(args);
        checkException();
        return result;
    }
    jdouble CallDoubleMethod(jobject obj, jmethodID methodID, ...) {
        va_list args;
        jdouble result;
        va_start(args,methodID);
        result = functions->CallDoubleMethodV(this,obj,methodID,args);
        va_end(args);
        checkException();
        return result;
    }
    jlong CallLongMethod(jobject obj, jmethodID methodID, ...) {
        va_list args;
        jlong result;
        va_start(args,methodID);
        result = functions->CallLongMethodV(this,obj,methodID,args);
        va_end(args);
        checkException();
        return result;
    }
    jboolean CallBooleanMethod(jobject obj, jmethodID methodID, ...)
    {
        va_list args;
        jboolean result;
        va_start(args,methodID);
        result = JNIEnv::CallBooleanMethodV(obj,methodID,args);
        va_end(args);
        checkException();
        return result;
    }
    jobject CallObjectMethod(jobject obj, jmethodID methodID, ...)
    {
        va_list args;
        jobject result;
        va_start(args,methodID);
        result = JNIEnv::CallObjectMethodV(obj,methodID,args);
        va_end(args);
        checkException();
        return result;
    }

    jchar CallCharMethodA(jobject obj, jmethodID methodID,
                          const jvalue * args) {
        return checkException(functions->CallCharMethodA(this,obj,methodID,args));
    }
    jboolean CallBooleanMethodA(jobject obj, jmethodID methodID,
                                const jvalue * args) {
        return checkException(functions->CallBooleanMethodA(this,obj,methodID, args));
    }
    jshort CallShortMethodA(jobject obj, jmethodID methodID,
                          const jvalue * args) {
        return checkException(functions->CallShortMethodA(this,obj,methodID,args));
    }
    jlong CallLongMethodA(jobject obj, jmethodID methodID,
                          const jvalue * args) {
        return checkException(functions->CallLongMethodA(this,obj,methodID,args));
    }
    jfloat CallFloatMethodA(jobject obj, jmethodID methodID,
                          const jvalue * args) {
        return checkException(functions->CallFloatMethodA(this,obj,methodID,args));
    }
    jdouble CallDoubleMethodA(jobject obj, jmethodID methodID,
                          const jvalue * args) {
        return checkException(functions->CallDoubleMethodA(this,obj,methodID,args));
    }
    jint CallIntMethodA(jobject obj, jmethodID methodID,
                          const jvalue * args) {
        return checkException(functions->CallIntMethodA(this,obj,methodID,args));
    }
    jbyte CallByteMethodA(jobject obj, jmethodID methodID,
                          const jvalue * args) {
        return checkException(functions->CallByteMethodA(this,obj,methodID,args));
    }
    jobject CallObjectMethodA(jobject obj, jmethodID methodID,
                          const jvalue * args) {
        return checkException(functions->CallObjectMethodA(this,obj,methodID,args));
    }
    void CallVoidMethodA(jobject obj, jmethodID methodID,
                          const jvalue * args) {
        functions->CallVoidMethodA(this,obj,methodID,args);
        return checkException();
    }

    jchar CallStaticCharMethodA(jclass clazz, jmethodID methodID,
                          const jvalue * args) {
        return checkException(functions->CallStaticCharMethodA(this,clazz,methodID,args));
    }
    jboolean CallStaticBooleanMethodA(jclass clazz, jmethodID methodID,
                                const jvalue * args) {
        return checkException(functions->CallStaticBooleanMethodA(this,clazz,methodID, args));
    }
    jshort CallStaticShortMethodA(jclass clazz, jmethodID methodID,
                          const jvalue * args) {
        return checkException(functions->CallStaticShortMethodA(this,clazz,methodID,args));
    }
    jlong CallStaticLongMethodA(jclass clazz, jmethodID methodID,
                          const jvalue * args) {
        return checkException(functions->CallStaticLongMethodA(this,clazz,methodID,args));
    }
    jfloat CallStaticFloatMethodA(jclass clazz, jmethodID methodID,
                          const jvalue * args) {
        return checkException(functions->CallStaticFloatMethodA(this,clazz,methodID,args));
    }
    jdouble CallStaticDoubleMethodA(jclass clazz, jmethodID methodID,
                          const jvalue * args) {
        return checkException(functions->CallStaticDoubleMethodA(this,clazz,methodID,args));
    }
    jint CallStaticIntMethodA(jclass clazz, jmethodID methodID,
                          const jvalue * args) {
        return checkException(functions->CallStaticIntMethodA(this,clazz,methodID,args));
    }
    jbyte CallStaticByteMethodA(jclass clazz, jmethodID methodID,
                          const jvalue * args) {
        return checkException(functions->CallStaticByteMethodA(this,clazz,methodID,args));
    }
    jobject CallStaticObjectMethodA(jclass clazz, jmethodID methodID,
                          const jvalue * args) {
        return checkException(functions->CallStaticObjectMethodA(this,clazz,methodID,args));
    }
    void CallStaticVoidMethodA(jclass clazz, jmethodID methodID,
                          const jvalue * args) {
        functions->CallStaticVoidMethodA(this,clazz,methodID,args);
        return checkException();
    }

    jobject NewObjectA(jclass clazz, jmethodID methodID, const jvalue *args)
    {
        return checkException(JNIEnv::NewObjectA(clazz,methodID,args));
    }
    jobject NewObject(jclass clazz, jmethodID methodID, ...)
    {
        va_list args;
        jobject result;
        va_start(args, methodID);
        result = JNIEnv::NewObjectV(clazz,methodID,args);
        va_end(args);
        checkException();
        return result;
    }
    jstring NewStringUTF(const char *utf)
    {
        return checkException(JNIEnv::NewStringUTF(utf));
    }
    jfieldID FromReflectedField(jobject field)
    {
        return checkException(JNIEnv::FromReflectedField(field));
    }
    using JNIEnv::PushLocalFrame;
    using JNIEnv::PopLocalFrame;
    using JNIEnv::DeleteLocalRef;
    using JNIEnv::ExceptionClear;
    using JNIEnv::ExceptionCheck;
    using JNIEnv::GetObjectRefType;

#ifdef TRACE_GLOBALREF
    void DeleteGlobalRef(jobject val)
    {
        DBGLOG("DeleteGlobalRef %p", val);
        JNIEnv::DeleteGlobalRef(val);
#ifdef FORCE_GC
        forceGC(this);
#endif
    }
    jobject NewGlobalRef(jobject val, const char *why)
    {
        jobject ret = JNIEnv::NewGlobalRef(val);
        DBGLOG("NewGlobalRef %p (%s) returns %p", val, why, ret);
        return ret;
    }
#else
    inline void DeleteGlobalRef(jobject val)
    {
        JNIEnv::DeleteGlobalRef(val);
#ifdef FORCE_GC
        forceGC(this);
#endif
    }
    inline jobject NewGlobalRef(jobject val, const char *)
    {
        return JNIEnv::NewGlobalRef(val);
    }
#endif
};

static bool printNameForClass(CheckedJNIEnv *JNIenv, jobject clsObj)
{
    if (!clsObj)
    {
        printf("Object %p is null\n", clsObj);
        return false;
    }
    jclass cls = JNIenv->GetObjectClass(clsObj);
    jmethodID mid = JNIenv->GetMethodID(cls, "getName", "()Ljava/lang/String;");
    jstring strObj = (jstring) JNIenv->CallObjectMethod(clsObj, mid);
    const char* str = JNIenv->GetStringUTFChars(strObj, NULL);
    printf("class %s\n", str);
    bool ret = streq(str, "java.lang.Class");
    JNIenv->ReleaseStringUTFChars(strObj, str);
    return ret;
}

static void printClassForObject(CheckedJNIEnv *JNIenv, jobject obj)
{
    printf("Object %p ", obj);
    if (!obj)
    {
        printf("is null\n");
        return;
    }
    jclass objClass = JNIenv->GetObjectClass(obj);
    jmethodID mid = JNIenv->GetMethodID(objClass, "getClass", "()Ljava/lang/Class;");
    jobject clsObj = JNIenv->CallObjectMethod(obj, mid);
    if (printNameForClass(JNIenv, clsObj))
    {
        printf("  ");
        printNameForClass(JNIenv, obj);
    }
}

static StringBuffer &getClassNameForObject(CheckedJNIEnv *JNIenv, StringBuffer &ret, jobject obj)
{
    if (obj)
    {
        jclass objClass = JNIenv->GetObjectClass(obj);
        jmethodID mid = JNIenv->GetMethodID(objClass, "getClass", "()Ljava/lang/Class;");
        jobject clsObj = JNIenv->CallObjectMethod(obj, mid);
        jclass cls = JNIenv->GetObjectClass(clsObj);
        mid = JNIenv->GetMethodID(cls, "getName", "()Ljava/lang/String;");
        jstring strObj = (jstring) JNIenv->CallObjectMethod(clsObj, mid);
        const char* str = JNIenv->GetStringUTFChars(strObj, NULL);
        ret.append(str);
        JNIenv->ReleaseStringUTFChars(strObj, str);
    }
    return ret;
}

static jobject getClassLoader(CheckedJNIEnv *JNIenv, jclass obj)
{
    jclass objClass = JNIenv->GetObjectClass(obj);
    jmethodID mid = JNIenv->GetMethodID(objClass, "getClassLoader", "()Ljava/lang/ClassLoader;");
    jobject classloader = JNIenv->CallObjectMethod(obj, mid);
    return classloader;
}

static StringBuffer helperLibraryName;
static CheckedJNIEnv *queryJNIEnv();

// Some global objects setup at load time for efficiency and code readability

static jclass customLoaderClass;
static jmethodID clc_newInstance;
static jmethodID clc_getSignature;
static jclass hpccIteratorClass;
static jmethodID hi_constructor;
static jmethodID hi_load;
static jclass utilIteratorClass;
static jclass langIterableClass;
static jmethodID iterable_iterator;

static jclass systemClass;
static jmethodID system_gc;
static jclass javaLangClassLoaderClass;
static jmethodID cl_getSystemClassLoader;
static jclass javaLangThreadClass;
static jmethodID thread_currentThread;
static jmethodID thread_getContextClassLoader;
static jmethodID thread_setContextClassLoader;
static jclass langObjectClass;
static jmethodID object_toString;
static jclass arrayListClass;
static jmethodID arrayList_toArray;
static jmethodID arrayList_constructor;
static jmethodID arrayList_add;
static jclass langStringClass;
static jclass netURLClass;
static jmethodID netURL_constructor;
static jclass throwableClass;
//static jmethodID throwable_toString; and others declared above
static jclass stackTraceElementClass;
static jclass langIllegalArgumentExceptionClass;

static void forceGC(CheckedJNIEnv* JNIenv)
{
    JNIenv->CallStaticVoidMethod(systemClass, system_gc);
}

static void setupGlobals(CheckedJNIEnv *J)
{
    try
    {
        // Load this first as we can't report errors on the others sensibly if this one not loaded!
        throwableClass = J->FindGlobalClass("java/lang/Throwable");
        throwable_toString = J->GetMethodID(throwableClass, "toString", "()Ljava/lang/String;");
        throwable_getStackTrace = J->GetMethodID(throwableClass, "getStackTrace", "()[Ljava/lang/StackTraceElement;");
        throwable_getCause = J->GetMethodID(throwableClass, "getCause", "()Ljava/lang/Throwable;");
        stackTraceElementClass = J->FindGlobalClass("java/lang/StackTraceElement");
        frame_toString = J->GetMethodID(stackTraceElementClass, "toString", "()Ljava/lang/String;");

        systemClass = J->FindGlobalClass("java/lang/System");
        system_gc = J->GetStaticMethodID(systemClass, "gc", "()V");

        javaLangClassLoaderClass = J->FindGlobalClass("java/lang/ClassLoader");
        cl_getSystemClassLoader = J->GetStaticMethodID(javaLangClassLoaderClass, "getSystemClassLoader", "()Ljava/lang/ClassLoader;");

        javaLangThreadClass = J->FindGlobalClass("java/lang/Thread");
        thread_currentThread = J->GetStaticMethodID(javaLangThreadClass, "currentThread", "()Ljava/lang/Thread;");
        thread_getContextClassLoader = J->GetMethodID(javaLangThreadClass, "getContextClassLoader", "()Ljava/lang/ClassLoader;");
        thread_setContextClassLoader = J->GetMethodID(javaLangThreadClass, "setContextClassLoader", "(Ljava/lang/ClassLoader;)V");

        langObjectClass = J->FindGlobalClass("java/lang/Object");
        object_toString = J->GetMethodID(langObjectClass, "toString", "()Ljava/lang/String;");

        arrayListClass = J->FindGlobalClass("java/util/ArrayList");
        arrayList_constructor = J->GetMethodID(arrayListClass, "<init>", "()V");
        arrayList_add = J->GetMethodID(arrayListClass, "add", "(Ljava/lang/Object;)Z");
        arrayList_toArray = J->GetMethodID(arrayListClass, "toArray", "()[Ljava/lang/Object;" );

        langStringClass = J->FindGlobalClass("java/lang/String");
        langIterableClass = J->FindGlobalClass("java/lang/Iterable");
        iterable_iterator = J->GetMethodID(langIterableClass, "iterator", "()Ljava/util/Iterator;");
        utilIteratorClass = J->FindGlobalClass("java/util/Iterator");

        langIllegalArgumentExceptionClass = J->FindGlobalClass("java/lang/IllegalArgumentException");
    }
    catch (IException *E)
    {
        Owned<IException> e = E;
        throw makeWrappedExceptionV(E, E->errorCode(), "javaembed: Unable to load Java system classes - is classpath set properly?");
    }

    try
    {
        customLoaderClass = J->FindGlobalClass("com/HPCCSystems/HpccClassLoader");
        clc_newInstance = J->GetStaticMethodID(customLoaderClass, "newInstance","(Ljava/lang/String;Ljava/lang/ClassLoader;IJLjava/lang/String;)Lcom/HPCCSystems/HpccClassLoader;");
        clc_getSignature = J->GetStaticMethodID(customLoaderClass, "getSignature","(Ljava/lang/Class;Ljava/lang/String;)Ljava/lang/String;");
        hpccIteratorClass = J->FindGlobalClass("com/HPCCSystems/HpccUtils");
        hi_constructor = J->GetMethodID(hpccIteratorClass, "<init>", "(JLjava/lang/String;)V");
        hi_load = J->GetStaticMethodID(hpccIteratorClass, "load", "(Ljava/lang/String;)V");
        J->CallStaticVoidMethod(hpccIteratorClass, hi_load, J->NewStringUTF(helperLibraryName));
    }
    catch (IException *E)
    {
        Owned<IException> e = E;
        throw makeWrappedExceptionV(E, E->errorCode(), "javaembed: Unable to find HPCC classes - is classpath set properly?");
    }
}

static StringAttr & getSignature(StringAttr &ret, CheckedJNIEnv *J, jclass clazz, const char *funcName)
{
    StringBuffer sig;
    jstring result = (jstring) J->CallStaticObjectMethod(customLoaderClass, clc_getSignature, clazz, J->NewStringUTF(funcName));
    J->getString(sig, result);
    sig.replace('.', '/');
    ret.set(sig);
    return ret;
}

/**
 * The following classes are used to ensure that the code in loadFunction that creates an instance
 * that is shared between multiple callers is only called on one thread, while other threads will wait
 * and use the instance created by the first thread.
 *
 */
class PersistedObject : public MappingBase
{
public:
    PersistedObject(const char *_name) : name(_name) {}
    ~PersistedObject()
    {
        if (instance)
        {
#ifdef TRACE_GLOBALREF
            DBGLOG("DeleteGlobalRef(singleton): %p", instance);
#endif
            queryJNIEnv()->DeleteGlobalRef(instance);
        }
    }
    CriticalSection crit;
    jobject instance = nullptr;
    StringAttr name;
    virtual const void * getKey() const { return name; }
};

class PersistedObjectCriticalBlock
{
    PersistedObject *obj = nullptr;
public:
    inline PersistedObjectCriticalBlock()
    {
    }
    inline ~PersistedObjectCriticalBlock()
    {
        if (obj)
            obj->crit.leave();
    }
    inline void enter(PersistedObject *_obj)
    {
        // Note that the object should be locked before we are called
        assertex(!obj);
        obj = _obj;
    }
    inline void leave(jobject instance = nullptr)
    {
        if (obj)
        {
            if (instance)
                obj->instance = instance;
            obj->crit.leave();
            obj = nullptr;
        }
    }
    inline bool locked()
    {
        return obj != nullptr;
    }
    jobject getInstance()
    {
        assertex(obj);
        return obj->instance;
    }
};


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

static StringBuffer &appendClassPath(StringBuffer &classPath)
{
    const IProperties &conf = queryEnvironmentConf();
    if (conf.hasProp("classpath"))
    {
        conf.getProp("classpath", classPath);
        classPath.append(ENVSEPCHAR);
    }
    else
    {
        classPath.append(INSTALL_DIR).append(PATHSEPCHAR).append("classes").append(ENVSEPCHAR);
    }
    return classPath;
}

static class JavaGlobalState
{
public:
    JavaGlobalState() : persistedObjects(false)
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
        appendClassPath(newPath);
        newPath.append(".");
        optionStrings.append(newPath);

        const IProperties &conf = queryEnvironmentConf();
        if (conf.hasProp("jvmlibpath"))
        {
            StringBuffer libPath;
            libPath.append("-Djava.library.path=");
            conf.getProp("jvmlibpath", libPath);
            optionStrings.append(libPath);
        }

        // Options we should set (but allow for override with jvmoptions below)
        optionStrings.append("-XX:-UseLargePages");

        if (conf.hasProp("jvmoptions"))
        {
            // Use space as field sep as ':' and ';' are valid
            optionStrings.appendList(conf.queryProp("jvmoptions"), " ");
        }

        // Options we know we always want set
        optionStrings.append("-Xrs");
#ifdef RLIMIT_STACK
        // JVM has a habit of reducing the stack limit on main thread to 1M - probably dates back to when it was actually an increase...
        StringBuffer stackOption("-Xss");
        struct rlimit limit;
        rlim_t slim = 0;
        if (getrlimit (RLIMIT_STACK, &limit)==0)
            slim = limit.rlim_cur;
        if (!slim)
            slim = 8*1024*1024;
        if (slim >= 1*1024*1024)
        {
            stackOption.append((__uint64) slim);
            optionStrings.append(stackOption);
        }
#endif

        // These may be useful for debugging
#ifdef CHECK_JNI
        optionStrings.append("-Xcheck:jni");
        optionStrings.append("-verbose:jni");
        optionStrings.append("-XX:+TraceClassLoading");
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
        vm_args.version = JNI_VERSION_1_8;

        /* load and initialize a Java VM, return a JNI interface pointer in env */
        JNIEnv *env;       /* receives pointer to native method interface */
        int createResult = JNI_CreateJavaVM(&javaVM, (void**)&env, &vm_args);

        delete [] options;

        if (createResult != 0)
            throw MakeStringException(0, "javaembed: Unable to initialize JVM (%d)",createResult);
        setupGlobals((CheckedJNIEnv *) env);
        // DBGLOG("JNI environment version %x loaded", env->GetVersion()); // Comes out a bit too early
    }
    ~JavaGlobalState()
    {
        /* We could release global persisted classes here but not a lot of point. Code would look something like this:
        HashIterator it(persistedObjects);
        ForEach(it)
        {
            IMapping &entry = it.query();
            jobject *pObj = persistedObjects.mapToValue(&entry);
            if (pClass)
                queryJNIEnv()->DeleteGlobalRef(*pObj);
        }
        */
        // This function is never called anyway...
        // We don't attempt to destroy the Java VM, as it's buggy...
    }

    PersistedObject *getGlobalObject(CheckedJNIEnv *JNIenv, const char *name)
    {
        PersistedObject *p;
        {
            CriticalBlock b(hashCrit);
            p = persistedObjects.find(name);
            if (!p)
            {
                p = new PersistedObject(name);
                persistedObjects.replaceOwn(*p);
            }
        }
        p->crit.enter();  // outside the hashCrit block, otherwise I think there is a possibility of deadlock
        return p;
    }

    void doUnregister(const char *key)
    {
        CriticalBlock b(hashCrit);
        persistedObjects.remove(key);
    }
    static void unregister(const char *key);
    JavaVM *javaVM;       /* denotes a Java VM */
private:
    CriticalSection hashCrit;
    StringMapOf<PersistedObject> persistedObjects;

} *globalState;

void JavaGlobalState::unregister(const char *key)
{
    // Remove a class that was persisted via : PERSIST options - it has come to the end of its life
    globalState->doUnregister(key);
}

#ifdef _WIN32
    EXTERN_C IMAGE_DOS_HEADER __ImageBase;
#endif

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    // Make sure we are never unloaded (as JVM does not support it)
    // we do this by doing a dynamic load of the javaembed library
#ifdef _WIN32
    char ln[_MAX_PATH];
    ::GetModuleFileName((HINSTANCE)&__ImageBase, ln, _MAX_PATH);
    if (strstr(path, "javaembed"))
    {
        HINSTANCE h = LoadSharedObject(ln, false, false);
        helperLibraryName.set(ln);
        DBGLOG("LoadSharedObject returned %p", h);
    }
#else
    if (findLoadedModule(helperLibraryName, "javaembed"))
    {
        HINSTANCE h = LoadSharedObject(helperLibraryName, false, false);
        // Deliberately leak this handle
    }
#endif
    globalState = new JavaGlobalState;
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

enum PersistMode
{
    persistNone,
    persistSupplied,
    persistThread,
    persistChannel,
    persistWorkunit,
    persistQuery,
    persistGlobal
};

static PersistMode getPersistMode(const char *val, StringAttr &globalScope)
{
    StringAttr trimmed;
    const char *colon = strchr(val, ':');
    if (colon)
    {
        globalScope.set(colon+1);
        trimmed.set(val, colon-val);
        val = trimmed;
    }
    if (isEmptyString(val) || strieq(val, "none"))
        return persistNone;
    else if (strieq(val, "thread"))
        return persistThread;
    else if (strieq(val, "channel"))
        return persistChannel;
    else if (strieq(val, "workunit"))
        return persistWorkunit;
    else if (strieq(val, "query"))
        return persistQuery;
    else if (strieq(val, "global"))
        return persistGlobal;
    else
        throw MakeStringException(MSGAUD_user, 0, "javaembed: Unrecognized persist mode %s", val);
}

//-------------------------------------------

// A JavaObject accessor has common functionality shared by both the builders below (Java-> ECL and ECL->Java)

class JavaObjectAccessor : public CInterface
{
protected:
    JavaObjectAccessor(CheckedJNIEnv *_JNIenv, const RtlFieldInfo *_outerRow, jobject _row)
    : JNIenv(_JNIenv), row(_row), outerRow(_outerRow), idx(0), limit(0), inSet(false), inDataSet(false)
    {
        Class = (jclass) JNIenv->NewGlobalRef(JNIenv->GetObjectClass(row), "Class");
    }
    JavaObjectAccessor(CheckedJNIEnv *_JNIenv, const RtlFieldInfo *_outerRow, jclass _Class)
    : JNIenv(_JNIenv), outerRow(_outerRow), idx(0), limit(0), inSet(false), inDataSet(false)
    {
        row = NULL;
        Class = (jclass) JNIenv->NewGlobalRef(_Class, "Class");
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
        row = (jobject) stack.popGet();
        Class = (jclass) stack.popGet();
    }
    jfieldID checkCharField(const RtlFieldInfo * field)
    {
        return JNIenv->GetFieldIDUnchecked(Class, field->name, inSet ? "[C" : "C");
    }
    jfieldID getFieldId(const RtlFieldInfo * field, const char *sig, const char *expected)
    {
        // MORE - if we are going to stream a dataset we really should be caching these somehow
        try
        {
            jfieldID fieldId = 0;
            if (sig)
            {
                if (inSet)
                {
                    VStringBuffer arraySig("[%s", sig);
                    fieldId = JNIenv->GetFieldID(Class, field->name, arraySig.str());
                }
                else
                    fieldId = JNIenv->GetFieldID(Class, field->name, sig);
            }
            else
            {
                // Do it the hard way via reflection API
                // Equivalent java:
                // Field field = object.getClass().getDeclaredField(fieldName);
                jclass classClass =JNIenv->GetObjectClass(Class);
                jmethodID getDeclaredField = JNIenv->GetMethodID(classClass, "getDeclaredField", "(Ljava/lang/String;)Ljava/lang/reflect/Field;" );
                jstring fieldName = JNIenv->NewStringUTF(field->name);
                jobject reflectedField = JNIenv->CallObjectMethod(Class, getDeclaredField, fieldName);
                fieldId = JNIenv->FromReflectedField(reflectedField);
            }
            return fieldId;
        }
        catch (IException *E)
        {
            ::Release(E);
            throw MakeStringException(0, "javaembed: Unable to retrieve field %s of type %s", field->name, expected);
        }
    }

    CheckedJNIEnv *JNIenv;
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

    JavaRowBuilder(CheckedJNIEnv *_JNIenv, const RtlFieldInfo *_outerRow, jobject _row)
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
        __len = (array != NULL ? JNIenv->GetArrayLength(array) : 0);
        __result = (__len > 0 ? rtlMalloc(__len) : NULL);
        if (__result)
            JNIenv->GetByteArrayRegion(array, 0, __len, (jbyte *) __result);
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
                jfieldID charFieldId = checkCharField(field);
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
                jfieldID charFieldId = checkCharField(field);
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
                jfieldID charFieldId = checkCharField(field);
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
    JavaObjectBuilder(CheckedJNIEnv *_JNIenv, const RtlFieldInfo *_outerRow, jclass _Class)
    : JavaObjectAccessor(_JNIenv, _outerRow, _Class)
    {
        setConstructor();
    }
    virtual void processString(unsigned numchars, const char *text, const RtlFieldInfo * field)
    {
        if (field->isFixedSize() && field->size(NULL, NULL)==1 && !inSet)  // SET OF STRING1 is not mapped to array of char...
        {
            // See if there's a char field
            jfieldID charFieldId = checkCharField(field);
            if (charFieldId)
            {
                assertex(numchars==1);
                jchar c;
                rtlStrToUnicode(1, &c, 1, text);
                JNIenv->SetCharField(row, charFieldId, c);
                return;
            }
        }
        jfieldID fieldId = getFieldId(field, "Ljava/lang/String;", "String");
        size32_t numchars16;
        rtlDataAttr unicode16;
        rtlStrToUnicodeX(numchars16, unicode16.refustr(), numchars, text);
        jstring value = JNIenv->NewString(unicode16.getustr(), numchars16);
        if (inSet)
            JNIenv->SetObjectArrayElement((jobjectArray) row, idx, value);
        else
            JNIenv->SetObjectField(row, fieldId, value);
        JNIenv->DeleteLocalRef(value);
    }
    virtual void processBool(bool value, const RtlFieldInfo * field)
    {
        jfieldID fieldId = getFieldId(field, "Z", "boolean");
        JNIenv->SetBooleanField(row, fieldId, value);
    }
    virtual void processData(unsigned len, const void *value, const RtlFieldInfo * field)
    {
        jfieldID fieldId = getFieldId(field, "[B", "data");
        jbyteArray javaData = JNIenv->NewByteArray(len);
        JNIenv->SetByteArrayRegion(javaData, 0, len, (jbyte *) value);
        if (inSet)
            JNIenv->SetObjectArrayElement((jobjectArray) row, idx, javaData);
        else
            JNIenv->SetObjectField(row, fieldId, javaData);
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
        if (inSet)
            JNIenv->SetObjectArrayElement((jobjectArray) row, idx, value);
        else
            JNIenv->SetObjectField(row, fieldId, value);
        JNIenv->DeleteLocalRef(value);
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
        if (inSet)
            JNIenv->SetObjectArrayElement((jobjectArray) row, idx, value);
        else
            JNIenv->SetObjectField(row, fieldId, value);
        JNIenv->DeleteLocalRef(value);
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
            newRow = JNIenv->NewObjectArray(numElems, langStringClass, NULL);
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
            if (!JNIenv->CallBooleanMethod(arrayClass, isArrayMethod))
            {
                JNIenv->ExceptionClear();
                VStringBuffer message("javaembed: Array expected for field %s", field->name);
                rtlFail(0, message.str());
            }
            // Set up constructor etc for the child rows, so we don't do it per row
            jmethodID getTypeMethod = JNIenv->GetMethodID(JNIenv->GetObjectClass(arrayClass), "getComponentType", "()Ljava/lang/Class;" );
            Class = (jclass) JNIenv->CallObjectMethod(arrayClass, getTypeMethod);
            setConstructor();
            // Now we need to create the array
            newRow = JNIenv->NewObjectArray(numRows, Class, NULL);
        }
        JNIenv->SetObjectField(row, childId, newRow);
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
                JNIenv->SetObjectArrayElement((jobjectArray) row, idx++, newRow);
            }
            else
            {
                // All this is done once per dataset in the nested dataset case. But for embedded record case we have to do it here
                jfieldID childId = getFieldId( field, NULL, "RECORD");
                Class = getClassForChild(childId);
                setConstructor();
                newRow = JNIenv->NewObject(Class, constructor);
                JNIenv->SetObjectField(row, childId, newRow);
            }
            row = newRow;
        }
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
        idx = idxStack.popGet();
        pop();
    }
    virtual void processEndRow(const RtlFieldInfo * field)
    {
        if (field != outerRow)
        {
            constructor = (jmethodID) stack.popGet();
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
        jclass fieldClass =JNIenv->GetObjectClass(reflectedField);
        jmethodID getTypeMethod = JNIenv->GetMethodID(fieldClass, "getType", "()Ljava/lang/Class;" );
        jclass result = (jclass) JNIenv->CallObjectMethod(reflectedField, getTypeMethod);
        JNIenv->DeleteLocalRef(reflectedField);
        JNIenv->DeleteLocalRef(fieldClass);
        return result;

    }
    void setConstructor()
    {
        constructor = JNIenv->GetMethodIDUnchecked(Class, "<init>", "()V");
        if (!constructor)
        {
            jmethodID getNameMethod = JNIenv->GetMethodID(JNIenv->GetObjectClass(Class), "getName", "()Ljava/lang/String;" );
            jstring name = (jstring) JNIenv->CallObjectMethod(Class, getNameMethod);
            const char *nameText = JNIenv->GetStringUTFChars(name, NULL);
            VStringBuffer message("javaembed: no suitable constructor for class %s", nameText);
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
    ECLDatasetIterator(CheckedJNIEnv *JNIenv, const RtlTypeInfo *_typeInfo, jclass className, IRowStream * _val)
    : typeInfo(_typeInfo), val(_val),
      dummyField("<row>", NULL, typeInfo),
      javaBuilder(JNIenv, &dummyField, className)
    {
        nextRead = false;
    }

    bool hasNext()
    {
        if (!nextRead)
        {
            nextPending.setown(val->ungroupedNextRow());
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
        typeInfo->process((const byte *) nextPending.get(), (const byte *) nextPending.get(), &dummyField, javaBuilder); // Creates a java object from the incoming ECL row
        nextRead = false;
        return javaBuilder.getObject();
    }
protected:
    const RtlTypeInfo *typeInfo;  // Not linked (or linkable)
    Linked<IRowStream> val;
    RtlFieldStrInfo dummyField;
    JavaObjectBuilder javaBuilder;
    roxiemem::OwnedConstRoxieRow nextPending;
    bool nextRead;
};

//-------------------------------------------

// A Java function that returns a dataset will return a JavaRowStream object that can be
// interrogated to return each row of the result in turn

class JavaLocalFrame
{
public:
    JavaLocalFrame(CheckedJNIEnv *_JNIenv, unsigned size = 16) : JNIenv(_JNIenv)
    {
        JNIenv->PushLocalFrame(size);
    }
    ~JavaLocalFrame()
    {
        JNIenv->PopLocalFrame(NULL);
    }
private:
    CheckedJNIEnv *JNIenv;
};

class JavaRowStream : public CInterfaceOf<IRowStream>
{
public:
    JavaRowStream(jobject _iterator, IEngineRowAllocator *_resultAllocator)
    : resultAllocator(_resultAllocator)
    {
        CheckedJNIEnv *JNIenv = queryJNIEnv();
        iterator = JNIenv->NewGlobalRef(_iterator, "iterator");
        iterClass = (jclass) JNIenv->NewGlobalRef(JNIenv->GetObjectClass(iterator), "iterClass");
        hasNextMethod = JNIenv->GetMethodID(iterClass, "hasNext", "()Z" );
        nextMethod = JNIenv->GetMethodID(iterClass, "next", "()Ljava/lang/Object;" );
        // Note that we can't save the JNIEnv value here - calls may be made on different threads (though not at the same time).
    }
    ~JavaRowStream()
    {
        stop();
    }
    virtual const void *nextRow()
    {
        if (!iterator)
            return NULL;
        CheckedJNIEnv *JNIenv = queryJNIEnv();
        JavaLocalFrame lf(JNIenv);
        // Java code would be
        // if (!iterator.hasNext)
        // {
        //    stop();
        //    return NULL;
        // }
        // result = iterator.next();
        jboolean hasNext = JNIenv->CallBooleanMethod(iterator, hasNextMethod);
        if (!hasNext)
        {
            stop();
            return NULL;
        }
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
        CheckedJNIEnv *JNIenv = queryJNIEnv();
        if (JNIenv)
        {
            if (iterator)
            {
                JNIenv->DeleteGlobalRef(iterator);
                iterator = NULL;
            }
            if (iterClass)
            {
                JNIenv->DeleteGlobalRef(iterClass);
                iterClass = NULL;
            }
            hasNextMethod = nullptr;
            nextMethod = nullptr;
        }
    }

protected:
    Linked<IEngineRowAllocator> resultAllocator;
    jobject iterator = nullptr;
    jclass iterClass = nullptr;
    jmethodID hasNextMethod = nullptr;
    jmethodID nextMethod = nullptr;
};

const char *esdl2JavaSig(IEsdlDefinition &esdl, const char *esdlType)
{
    EsdlBasicElementType t = esdl.translateSimpleType(esdlType);
    switch (t)
    {
    case ESDLT_INT16:
    case ESDLT_UINT16:
        return "Ljava/lang/Short;";
    case ESDLT_INT32:
    case ESDLT_UINT32:
        return "Ljava/lang/Integer;";
    case ESDLT_INT64:
    case ESDLT_UINT64:
        return "Ljava/math/BigInteger;";
    case ESDLT_BOOL:
        return "Ljava/lang/Boolean;";
    case ESDLT_FLOAT:
        return "Ljava/lang/Float;";
    case ESDLT_DOUBLE:
        return "Ljava/lang/Double;";
    case ESDLT_INT8:
    case ESDLT_UINT8:
    case ESDLT_BYTE:
    case ESDLT_UBYTE:
        return "Ljava/lang/Byte;";
    case ESDLT_STRING:
        return "Ljava/lang/String;";
    case ESDLT_UNKOWN:
    case ESDLT_STRUCT:
    case ESDLT_REQUEST:
    case ESDLT_RESPONSE:
    case ESDLT_COMPLEX:
    default:
        return NULL;
    }
}

const char *esdl2JavaFullClassName(IEsdlDefinition &esdl, const char *esdlType)
{
    EsdlBasicElementType t = esdl.translateSimpleType(esdlType);
    switch (t)
    {
    case ESDLT_INT16:
    case ESDLT_UINT16:
        return "java/lang/Short";
    case ESDLT_INT32:
    case ESDLT_UINT32:
        return "java/lang/Integer";
    case ESDLT_INT64:
    case ESDLT_UINT64:
        return "java/math/BigInteger";
    case ESDLT_BOOL:
        return "java/lang/Boolean";
    case ESDLT_FLOAT:
        return "java/lang/Float";
    case ESDLT_DOUBLE:
        return "java/lang/Double";
    case ESDLT_INT8:
    case ESDLT_UINT8:
    case ESDLT_BYTE:
    case ESDLT_UBYTE:
        return "java/lang/Byte";
    case ESDLT_STRING:
        return "java/lang/String";
    case ESDLT_UNKOWN:
    case ESDLT_STRUCT:
    case ESDLT_REQUEST:
    case ESDLT_RESPONSE:
    case ESDLT_COMPLEX:
    default:
        return NULL;
    }
}

class JavaObjectXmlWriter : public CInterface
{
public:
    JavaObjectXmlWriter(CheckedJNIEnv *_JNIenv, jobject _obj, const char *_reqType, IEsdlDefinition &_esdl, const char *_esdlService, IXmlWriter &_writer)
    : JNIenv(_JNIenv), obj(_obj), writer(_writer), esdl(_esdl), esdlService(_esdlService), reqType(_reqType)
    {
        Class = (jclass) JNIenv->NewGlobalRef(JNIenv->GetObjectClass(obj), "class");
    }
    ~JavaObjectXmlWriter()
    {
        if (Class)
            JNIenv->DeleteGlobalRef(Class);
        HashIterator it(javaClasses);
        ForEach(it)
        {
            IMapping &entry = it.query();
            jclass *pClass = javaClasses.mapToValue(&entry);
            if (pClass)
                JNIenv->DeleteGlobalRef(*pClass);
        }
    }
    void writeSimpleType(const char *fieldname, jobject fieldObj)
    {
        jstring fieldStr = (jstring) JNIenv->CallObjectMethod(fieldObj, object_toString);
        if (!fieldStr)
            return;
        const char *text = JNIenv->GetStringUTFChars(fieldStr, NULL);
        if (text)
            writer.outputCString(text, fieldname);
        JNIenv->ReleaseStringUTFChars(fieldStr, text);
        JNIenv->DeleteLocalRef(fieldStr);
    }
    void writeSimpleType(jclass parentClass, jobject parentObject, const char *fieldname, const char *javaSig)
    {
        if (!fieldname || !*fieldname)
            return;
        if (!javaSig || !*javaSig)
            return;
        jfieldID fieldId = JNIenv->GetFieldID(parentClass, fieldname, javaSig);
        if (!fieldId)
            return;
        jobject fieldObj = (jobject) JNIenv->GetObjectField(parentObject, fieldId);
        if (!fieldObj)
            return;
        writeSimpleType(fieldname, fieldObj);
        JNIenv->DeleteLocalRef(fieldObj);
    }
    void writeSimpleType(jclass parentClass, jobject parentObject, IEsdlDefObject &defObject)
    {
        const char *fieldname = defObject.queryName();
        const char *javaSig = esdl2JavaSig(esdl, defObject.queryProp("type"));
        writeSimpleType(parentClass, parentObject, fieldname, javaSig);
    }
    void writeEnumType(jclass parentClass, jobject parentObject, IEsdlDefObject &defObject)
    {
        const char *fieldname = defObject.queryName();
        VStringBuffer javaSig("L%s/%s;", esdlService.str(), defObject.queryProp("enum_type"));
        jfieldID fieldId = JNIenv->GetFieldID(parentClass, fieldname, javaSig);
        if (!fieldId)
            return;
        jobject fieldObj = (jobject) JNIenv->GetObjectField(parentObject, fieldId);
        if (!fieldObj)
            return;
        jstring fieldStr = (jstring) JNIenv->CallObjectMethod(fieldObj, object_toString);
        const char *text = JNIenv->GetStringUTFChars(fieldStr, NULL);
        if (text)
            writer.outputCString(text, defObject.queryName());
        JNIenv->ReleaseStringUTFChars(fieldStr, text);
    }
    void writeComplexType(jclass parentClass, jobject parentObject, IEsdlDefObject &defObject)
    {
        IEsdlDefStruct *defStruct = esdl.queryStruct(defObject.queryProp("complex_type"));
        if (!defStruct)
            return;
        const char *fieldname = defObject.queryName();
        VStringBuffer javaSig("L%s/%s;", esdlService.str(), defObject.queryProp("complex_type"));
        jfieldID fieldId = JNIenv->GetFieldID(parentClass, fieldname, javaSig); //tbd cache this
        if (!fieldId)
            return;
        jobject fieldObj = (jobject) JNIenv->GetObjectField(parentObject, fieldId);
        if (!fieldObj)
            return;
        writer.outputBeginNested(fieldname, true);
        writeChildren(JNIenv->GetObjectClass(fieldObj), fieldObj, defStruct);
        writer.outputEndNested(fieldname);
    }

    void writeSimpleArray(jobjectArray arrayObj, jint count, const char *name, const char *item_tag)
    {
        writer.outputBeginNested(name, true);
        writer.outputBeginArray(item_tag);
        for (jint i=0; i < count; i++)
        {
            jobject elementObj = JNIenv->GetObjectArrayElement(arrayObj, i);
            writeSimpleType(item_tag, elementObj);
            JNIenv->DeleteLocalRef(elementObj);
        }
        writer.outputEndArray(item_tag);
        writer.outputEndNested(name);
    }
    void writeComplexArray(jobjectArray arrayObj, jint count, const char *name, const char *item_tag, const char *itemTypeName)
    {
        writer.outputBeginNested(name, true);
        writer.outputBeginArray(item_tag);
        {
            VStringBuffer javaClassName("%s/%s", esdlService.str(), itemTypeName);
            jclass elementClass = FindClass(javaClassName);
            if (!elementClass)
                return;
            IEsdlDefStruct *defStruct = esdl.queryStruct(itemTypeName);
            if (!defStruct)
                return;
            for (jint i=0; i < count; i++)
            {
                jobject elementObj = JNIenv->GetObjectArrayElement(arrayObj, i);
                writer.outputBeginNested(item_tag, true);
                writeChildren(elementClass, elementObj, defStruct);
                writer.outputEndNested(item_tag);
                JNIenv->DeleteLocalRef(elementObj);
            }
        }
        writer.outputEndArray(item_tag);
        writer.outputEndNested(name);
    }
    void writeArray(jclass parentClass, jobject parentObject, IEsdlDefObject &defObject)
    {
        const char *itemTypeName = defObject.queryProp("type");
        if (!itemTypeName)
            return;
        const char *item_tag = defObject.queryProp("item_tag");
        if (!item_tag)
            return;
        const char *fieldname = defObject.queryName();
        jfieldID fieldId = JNIenv->GetFieldID(parentClass, fieldname, "Ljava/util/ArrayList;");
        if (!fieldId)
            return;
        jobject arrayListObj = (jobject) JNIenv->GetObjectField(parentObject, fieldId);
        if (!arrayListObj)
            return;
        jobjectArray arrayObj = (jobjectArray) JNIenv->CallObjectMethod(arrayListObj, arrayList_toArray);
        if (arrayObj)
        {
            jint count = JNIenv->GetArrayLength(arrayObj);
            if (count)
            {
                if (esdl2JavaSig(esdl, itemTypeName))
                    writeSimpleArray(arrayObj, count, defObject.queryName(), item_tag);
                else
                    writeComplexArray(arrayObj, count, defObject.queryName(), item_tag, itemTypeName);
            }
            JNIenv->DeleteLocalRef(arrayObj);
        }
        JNIenv->DeleteLocalRef(arrayListObj);
    }
    void writeChildren(jclass javaClass, jobject javaObject, IEsdlDefStruct *defStruct)
    {
        Owned<IEsdlDefObjectIterator> children = defStruct->getChildren();
        ForEach (*children)
        {
            IEsdlDefObject &child = children->query();
            if (child.getEsdlType()==EsdlTypeElement)
            {
                if (child.hasProp("type"))
                    writeSimpleType(javaClass, javaObject, child);
                else if (child.hasProp("complex_type"))
                    writeComplexType(javaClass, javaObject, child);
            }
            else if (child.getEsdlType()==EsdlTypeEnumRef)
            {
                writeEnumType(javaClass, javaObject, child);
            }
            else if (child.getEsdlType()==EsdlTypeArray)
            {
                writeArray(javaClass, javaObject, child);
            }
        }
    }
    void write()
    {
        IEsdlDefStruct *reqStruct = esdl.queryStruct(reqType);
        const char *name = reqStruct->queryName();
        writer.outputBeginNested("Response", true);
        writer.outputBeginNested("Results", true);
        writer.outputBeginNested("Result", true);
        writer.outputBeginDataset(name, true);
        writer.outputBeginArray("Row");
        writer.outputBeginNested("Row", true);
        writeChildren(Class, obj, reqStruct);
        writer.outputEndNested("Row");
        writer.outputEndArray("Row");
        writer.outputEndDataset(name);
        writer.outputEndNested("Result");
        writer.outputEndNested("Results");
        writer.outputEndNested("Response");
    }

    jclass FindClass(const char *name)
    {
        jclass *pClass = javaClasses.getValue(name);
        if (pClass)
            return *pClass;
        jclass localClass = JNIenv->FindClass(name);
        if (!localClass)
            return 0;
        jclass Class = (jclass) JNIenv->NewGlobalRef(localClass, "class");
        javaClasses.setValue(name, Class);
        JNIenv->DeleteLocalRef(localClass);
        return Class;
    }

    CheckedJNIEnv *JNIenv;
    MapStringTo<jclass> javaClasses;
    jclass Class;
    jobject obj;
    IXmlWriter &writer;
    IEsdlDefinition &esdl;
    StringAttr reqType;
    StringAttr esdlService;
};

//-------------------------------------------

// There is a singleton JavaThreadContext per thread. This handles the interaction between
// the C++ thread and the java threading library, ensuring that we register/unregister as needed,
// and that any thread_local function contexts are destroyed before we detach from the java thread

interface IJavaEmbedFunctionContext : public IEmbedFunctionContext
{
    virtual void endThread() = 0;
};

class JavaThreadContext
{
public:
    CheckedJNIEnv *JNIenv;       /* receives pointer to native method interface */
public:
    JavaThreadContext()
    {
        jint res = globalState->javaVM->AttachCurrentThread((void **) &JNIenv, NULL);
        assertex(res >= 0);
        setThreadClassLoader(getSystemClassLoader());
    }
    ~JavaThreadContext()
    {
        // Make sure all thread-local function contexts and saved objects are destroyed before we detach from
        // the Java thread
        contexts.kill();
        persistedObjects.kill();
        loaders.kill();
        // According to the Java VM 1.7 docs, "A native thread attached to
        // the VM must call DetachCurrentThread() to detach itself before
        // exiting."
        globalState->javaVM->DetachCurrentThread();
    }
    void endThread()
    {
        persistedObjects.kill();
        ForEachItemIn(idx, contexts)
        {
            auto &context = contexts.item(idx);
            context.endThread();
        }
    }
    jobject getSystemClassLoader()
    {
        jobject systemClassLoaderObj = JNIenv->CallStaticObjectMethod(javaLangClassLoaderClass, cl_getSystemClassLoader);
        assertex(systemClassLoaderObj);
        return systemClassLoaderObj;
    }

    void setThreadClassLoader(jobject classLoader)
    {
        jobject threadObj = JNIenv->CallStaticObjectMethod(javaLangThreadClass, thread_currentThread);
        JNIenv->CallObjectMethod(threadObj, thread_setContextClassLoader, classLoader);
    }

    jobject getThreadClassLoader()
    {
        JNIenv->ExceptionClear();
        jobject threadObj = JNIenv->CallStaticObjectMethod(javaLangThreadClass, thread_currentThread);
        jobject contextClassLoaderObj = JNIenv->CallObjectMethod(threadObj, thread_getContextClassLoader);
        assertex(contextClassLoaderObj);
        return contextClassLoaderObj;
    }

    void writeObjectResult(jobject result, IEsdlDefinition *esdl, const char *esdlservice, const char *name, IXmlWriter *writer)
    {
        JavaObjectXmlWriter x(JNIenv, result, name, *esdl, esdlservice, *writer);
        x.write();
    }
    void registerContext(IJavaEmbedFunctionContext *ctx)
    {
        // Note - this object is thread-local so no need for a critsec
        contexts.append(*ctx);
    }

    PersistedObject *getLocalObject(CheckedJNIEnv *JNIenv, const char *name)
    {
        // Note - this object is thread-local so no need for a critsec
        PersistedObject *p;
        p = persistedObjects.find(name);
        if (!p)
        {
            p = new PersistedObject(name);
            persistedObjects.replaceOwn(*p);
        }
        p->crit.enter();  // needed to keep code common between local/global cases
        return p;
    }

    jobject createThreadClassLoader(const char *classPath, const char *classname, size32_t bytecodeLen, const byte *bytecode)
    {
        if (bytecodeLen || (classPath && *classPath))
        {
            jstring jClassPath = (classPath && *classPath) ? JNIenv->NewStringUTF(classPath) : nullptr;
            jobject helperName = JNIenv->NewStringUTF(helperLibraryName);
            jobject contextClassLoaderObj = JNIenv->CallStaticObjectMethod(customLoaderClass, clc_newInstance, jClassPath, getSystemClassLoader(), bytecodeLen, (uint64_t) bytecode, helperName);
            assertex(contextClassLoaderObj);
            return contextClassLoaderObj;
        }
        else
        {
            return getSystemClassLoader();
        }
    }
    jobject getThreadClassLoader(const char *classPath, const char *classname, size32_t bytecodeLen, const byte *bytecode)
    {
        StringBuffer key(classname);
        if (classPath && *classPath)
            key.append('!').append(classPath);
        PersistedObject *p;
        p = loaders.find(key);
        if (!p)
        {
            p = new PersistedObject(key);
            p->instance = JNIenv->NewGlobalRef(createThreadClassLoader(classPath, classname, bytecodeLen, bytecode), "cachedClassLoader");
            loaders.replaceOwn(*p);
        }
        return p->instance;
    }
private:
    IArrayOf<IJavaEmbedFunctionContext> contexts;
    StringMapOf<PersistedObject> persistedObjects = { false };
    StringMapOf<PersistedObject> loaders = { false };
};

class JavaXmlBuilder : implements IXmlWriterExt, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;
    JavaXmlBuilder(CheckedJNIEnv *_JNIenv, IEsdlDefinition *esdl_, const char *esdlservice, const char *esdltype_)
    : JNIenv(_JNIenv), esdl(esdl_), javaPackage(esdlservice), esdlType(esdltype_)
    {
    }
    ~JavaXmlBuilder()
    {
        while (defStack.length())
            popDefStackEntry(JNIenv);
        HashIterator it(javaClasses);
        ForEach(it)
        {
            IMapping &entry = it.query();
            jclass *pClass = javaClasses.mapToValue(&entry);
            if (pClass)
                JNIenv->DeleteGlobalRef(*pClass);
        }
    }
    void initWriter()
    {
    }
    IXmlWriterExt & clear()
    {
        throwUnexpected();
    }
    virtual size32_t length() const
    {
        return 0;
    }
    virtual const char *str() const
    {
        throwUnexpected();
    }
    virtual void finalize() override
    {
    }
    virtual void checkDelimiter() override                        {}

    virtual IInterface *saveLocation() const {return nullptr;}
    virtual void rewindTo(IInterface *loc)
    {
        //needs to be a no-op because it is  used, but the way its used to trim empty xml sections I think we're fairly safe.
        //revisit cleaning up any empty objects later.
    }
    inline IEsdlDefStruct *queryCurrentEsdlStruct()
    {
        if (!defStack.length() || !defStack.tos().defType)
            return NULL;
        return dynamic_cast<IEsdlDefStruct*>(defStack.tos().defType.get());
    }
    inline jobject getObject()
    {
        if (!defStack.length())
            return 0;
        return  defStack.item(0).obj;
    }
    inline jobject getCurJavaObject()
    {
        if (!defStack.length())
            return 0;
        return  defStack.tos().obj;
    }
    inline jclass getCurJavaClass()
    {
        if (!defStack.length())
            return 0;
        return  defStack.tos().Class;
    }
    inline jmethodID getCurJavaConstructor()
    {
        if (!defStack.length())
            return 0;
        return  defStack.tos().constructor;
    }
    virtual void outputEnumString(unsigned size, const char *text, const char *fieldname, IEsdlDefObject *defField)
    {
        const char *enum_type = defField->queryProp("enum_type");
        if (!enum_type || !*enum_type)
            return;
        VStringBuffer enumClassName("%s/%s", javaPackage.str(), enum_type);
        VStringBuffer enumSig("L%s;", enumClassName.str());
        jfieldID fieldId = JNIenv->GetFieldID(getCurJavaClass(), fieldname, enumSig);
        if (!fieldId)
            return;

        jclass enumClass = FindClass(enumClassName);
        jmethodID fromString = JNIenv->GetStaticMethodID(enumClass, "fromString", "(Ljava/lang/String;)LEsdlExample/AddressType;"); //All types currently used for ESDL mapping have string constructors
        StringAttr s(text, size);
        jstring strvalue = JNIenv->NewStringUTF(s);
        jobject value = JNIenv->CallStaticObjectMethod(enumClass, fromString, strvalue);
        JNIenv->DeleteLocalRef(strvalue);
        JNIenv->SetObjectField(getCurJavaObject(), fieldId, value);
        JNIenv->DeleteLocalRef(value);
    }
    virtual void outputString(unsigned size, const char *text, const char *fieldname)
    {
        DefStackEntry *parent = defStack.length() ? &defStack.tos() : NULL;
        if (!parent)
            return;
        const char *defTypeName = NULL;
        bool isArray = (parent->defObj && parent->defObj->getEsdlType()==EsdlTypeArray);
        if (isArray)
            defTypeName = parent->defObj->queryProp("type");
        else
        {
            IEsdlDefStruct *defStruct = queryCurrentEsdlStruct();
            if (!defStruct)
                return;
            IEsdlDefObject *defField = defStruct->queryChild(fieldname);
            if (!defField)
                return;
            if (defField->getEsdlType()==EsdlTypeEnumRef)
                return outputEnumString(size, text, fieldname, defField);
            defTypeName = defField->queryProp("type");
        }

        if (!defTypeName)
            return;
        const char *javaSig = esdl2JavaSig(*esdl, defTypeName);
        if (!javaSig)
            return;

        const char *fieldClassName = esdl2JavaFullClassName(*esdl, defTypeName);
        jclass typeClass = FindClass(fieldClassName);
        jmethodID typeStringConstructor = JNIenv->GetMethodID(typeClass, "<init>", "(Ljava/lang/String;)V"); //All types currently used for ESDL mapping have string constructors
        StringAttr s(text, size);
        jstring strvalue = JNIenv->NewStringUTF(s);
        jobject value = JNIenv->NewObject(typeClass, typeStringConstructor, strvalue);
        JNIenv->DeleteLocalRef(strvalue);
        if (!value)
            return;
        if (isArray)
            JNIenv->CallObjectMethod(parent->obj, parent->append, value);
        else
        {
            jfieldID fieldId = JNIenv->GetFieldID(getCurJavaClass(), fieldname, javaSig);
            if (fieldId)
                JNIenv->SetObjectField(getCurJavaObject(), fieldId, value);
        }
        JNIenv->DeleteLocalRef(value);
    }
    void outputString(const char *text, const char *fieldname)
    {
        outputString((unsigned)strlen(text), text, fieldname);
    }
    virtual void outputNumericString(const char *field, const char *fieldname)
    {
        outputString(field, fieldname);
    }
    virtual void outputBool(bool value, const char *fieldname)
    {
        outputString(value ? "true" : "false", fieldname);
    }
    virtual void outputUInt(unsigned __int64 field, unsigned size, const char *fieldname)
    {
        StringBuffer value;
        value.append(field);
        outputString(value.length(), value, fieldname);
    }
    virtual void outputInt(__int64 field, unsigned size, const char *fieldname)
    {
        StringBuffer value;
        value.append(field);
        outputString(value.length(), value, fieldname);
    }
    virtual void outputReal(double field, const char *fieldname)
    {
        StringBuffer value;
        value.append(field);
        outputString(value.length(), value, fieldname);
    }
    virtual void outputDecimal(const void *field, unsigned size, unsigned precision, const char *fieldname)
    {
        Decimal d;
        d.setDecimal(size, precision, field);
        outputString(d.getCString(), fieldname);
    }
    virtual void outputUDecimal(const void *field, unsigned size, unsigned precision, const char *fieldname)
    {
        Decimal d;
        d.setUDecimal(size, precision, field);
        outputString(d.getCString(), fieldname);
    }
    virtual void outputQString(unsigned len, const char *field, const char *fieldname)
    {
        MemoryAttr tempBuffer;
        char * temp;
        if (len <= 100)
            temp = (char *)alloca(len);
        else
            temp = (char *)tempBuffer.allocate(len);
        rtlQStrToStr(len, temp, len, field);
        outputString(len, temp, fieldname);
    }
    virtual void outputUnicode(unsigned len, const UChar *field, const char *fieldname)
    {
        char * buff = 0;
        unsigned bufflen = 0;
        rtlUnicodeToCodepageX(bufflen, buff, len, field, "utf-8");
        outputString(bufflen, buff, fieldname);
        rtlFree(buff);
    }
    virtual void outputUtf8(unsigned len, const char *field, const char *fieldname)
    {
        outputString(len, field, fieldname);
    }

    virtual void outputData(unsigned len, const void *value, const char *fieldname)
    {
    }
    virtual void outputQuoted(const char *text) //would have to let beginNested represent simple types with content set using this?
    {
    }
    virtual void outputBeginDataset(const char *dsname, bool nestChildren) //not used by ESDL engine
    {
    }
    virtual void outputEndDataset(const char *dsname)
    {
    }
    inline IEsdlDefObject *queryChildStructDefObj(IEsdlDefObject *child)
    {
        if (child)
        {
            switch (child->getEsdlType())
            {
            case EsdlTypeArray:
            {
                const char *structType = child->queryProp("type");
                if (structType)
                    return esdl->queryObj(structType);
                break;
            }
            case EsdlTypeElement:
            {
                const char *structType = child->queryProp("complex_type");
                if (structType)
                    return esdl->queryObj(structType);
                break;
            }
            default:
                break;
            }
        }
        return NULL;
    }
    virtual void outputBeginNested(const char *fieldname, bool nestChildren)
    {
        IEsdlDefStruct *defStruct = NULL;
        IEsdlDefObject *defField = NULL;
        IEsdlDefObject *defType = NULL;
        if (!defStack.length())
        {
            defType = esdl->queryObj(fieldname);
        }
        else
        {
            DefStackEntry &parent = defStack.tos();
            if (parent.defObj && parent.defObj->getEsdlType()==EsdlTypeArray)
            {
                defType = parent.defType;
            }
            else
            {
                defStruct = queryCurrentEsdlStruct();
                if (defStruct)
                {
                    defField = defStruct->queryChild(fieldname);
                    if (defField)
                        defType = queryChildStructDefObj(defField);
                }
            }
        }
        pushDefStackEntry(JNIenv, javaPackage, fieldname, defType, defField);
    }
    virtual void outputEndNested(const char *fieldname)
    {
        if (defStack.length()<=1)  //don't destroy root object yet
            return;
        if (!streq(fieldname, defStack.tos().name)) //should be exception? or forgive and forget?
            return;
        popDefStackEntry(JNIenv);
    }
    virtual void outputSetAll()
    {
    }
    virtual void outputBeginArray(const char *fieldname)
    {
    }
    virtual void outputEndArray(const char *fieldname)
    {
    }
    virtual void outputInlineXml(const char *text)
    {
    }
    virtual void outputXmlns(const char *name, const char *uri)
    {
    }
    virtual void cutFrom(IInterface *location, StringBuffer& databuf)
    {
    }
    virtual void outputInline(const char* text)
    {
    }

public:
    CheckedJNIEnv *JNIenv;
    Linked<IEsdlDefinition> esdl;
    StringAttr javaPackage;
    StringAttr esdlType;
    class DefStackEntry : public CInterface
    {
    public:
        DefStackEntry(const char *fieldname, IEsdlDefObject *_defType, IEsdlDefObject *_defObj) : name(fieldname), defType(_defType), defObj(_defObj), Class(0), obj(0), constructor(0), append(0), fieldId(0)
        {
        }
        ~DefStackEntry()
        {

        }
    public:
        Linked<IEsdlDefObject> defType;
        Linked<IEsdlDefObject> defObj;
        StringAttr name;
        jclass Class;
        jmethodID constructor;
        jmethodID append;
        jfieldID fieldId;
        jobject obj;
    };

    jobject MakeObjectGlobal(jobject local)
    {
        if (!local)
            return 0;
        jobject global = JNIenv->NewGlobalRef(local, "makeObjectGlobal");
        JNIenv->DeleteLocalRef(local);
        return global;
    }
    jclass FindClass(const char *name)
    {
        jclass *pClass = javaClasses.getValue(name);
        if (pClass)
            return *pClass;
        jclass Class = (jclass) MakeObjectGlobal(JNIenv->FindClass(name));
        javaClasses.setValue(name, Class); //even save if result has no class
        return Class;
    }
    void popDefStackEntry(CheckedJNIEnv *JNIenv)
    {
        if (!defStack.length())
            return;
        Owned<DefStackEntry> entry = &defStack.popGet();
        if (entry->obj)
            JNIenv->DeleteGlobalRef(entry->obj);
    }
    void pushDefStackEntry(CheckedJNIEnv *JNIenv, const char *package, const char *fieldname, IEsdlDefObject *defType, IEsdlDefObject *defObject)
    {
        DefStackEntry *parent = defStack.length() ? &defStack.tos() : NULL;
        Owned<DefStackEntry> entry = new DefStackEntry(fieldname, defType, defObject);
        JNIenv->ExceptionClear();
        if (defObject && defObject->getEsdlType()==EsdlTypeArray)
        {
            entry->Class = arrayListClass;
            entry->constructor = arrayList_constructor;
            entry->append = arrayList_add;
            entry->obj = MakeObjectGlobal(JNIenv->NewObject(entry->Class, entry->constructor));
            if (entry->obj)
            {
                if (parent && parent->Class)
                {
                    entry->fieldId = JNIenv->GetFieldID(parent->Class, fieldname, "Ljava/util/ArrayList;");
                    if (parent->obj && entry->fieldId)
                        JNIenv->SetObjectField(parent->obj, entry->fieldId, entry->obj);
                }
            }
        }
        else if (defType)
        {
            VStringBuffer javaClassName("%s/%s", package, defType->queryName());
            entry->Class = FindClass(javaClassName);
            if (entry->Class)
            {
                entry->constructor = JNIenv->GetMethodID(entry->Class, "<init>", "()V");
                entry->obj = MakeObjectGlobal(JNIenv->NewObject(entry->Class, entry->constructor));
                if (entry->obj)
                {
                    if (parent)
                    {
                        if (parent->defObj && parent->defObj->getEsdlType()==EsdlTypeArray)
                            JNIenv->CallObjectMethod(parent->obj, parent->append, entry->obj);
                        else if (parent->Class)
                        {
                            VStringBuffer javaSig("L%s;", javaClassName.str());
                            entry->fieldId = JNIenv->GetFieldID(parent->Class, fieldname, javaSig);
                            if (parent->obj && entry->fieldId)
                                JNIenv->SetObjectField(parent->obj, entry->fieldId, entry->obj);
                        }
                    }
                }
            }
        }
        defStack.append(*entry.getClear());
    }

    CIArrayOf<DefStackEntry> defStack;
    MapStringTo<jclass> javaClasses;
};

// Each call to a Java function will use a new JavaEmbedScriptContext object
#define MAX_JNI_ARGS 10

class JavaClassReader
{
public:
    JavaClassReader(const char *filename)
    {
        // Pull apart a class file to see its name and signature.
        /* From https://docs.oracle.com/javase/specs/jvms/se7/html/jvms-4.html#jvms-4.1
        ClassFile {
            u4             magic;
            u2             minor_version;
            u2             major_version;
            u2             constant_pool_count;
            cp_info        constant_pool[constant_pool_count-1];
            u2             access_flags;
            u2             this_class;
            u2             super_class;
            u2             interfaces_count;
            u2             interfaces[interfaces_count];
            u2             fields_count;
            field_info     fields[fields_count];
            u2             methods_count;
            method_info    methods[methods_count];
            u2             attributes_count;
            attribute_info attributes[attributes_count];
        }
       */
#ifdef TRACE_CLASSFILE
        DBGLOG("Reading class file created in %s", filename);
#endif
        Owned<IFile> file = createIFile(filename);
        OwnedIFileIO io = file->open(IFOread);
        assertex(io);
        read(io, 0, (size32_t)-1, b);
        b.setEndian(__BIG_ENDIAN);
        uint32_t magic;
        b.read(magic);
        if (magic != 0xcafebabe)
            throwUnexpected();
        uint16_t major, minor, cpc;
        b.read(major);
        b.read(minor);
        b.read(cpc);
        constOffsets = new unsigned[cpc];
        constOffsets[0] = 0;
        for (int i = 0; i < cpc-1; i++)  // There are only cpc-1 entries, for reasons best known to the java designers
        {
            constOffsets[i+1] = b.getPos();
            byte tag;
            b.read(tag);
            switch (tag)
            {
            case CONSTANT_Class:
                uint16_t idx;
                b.read(idx);
#ifdef TRACE_CLASSFILE
                DBGLOG("%u: Class %u", i+1, idx);
#endif
                break;
            case CONSTANT_Fieldref:
            case CONSTANT_Methodref:
            case CONSTANT_InterfaceMethodref:
                uint16_t classIdx;
                uint16_t nametypeIdx;
                b.read(classIdx);
                b.read(nametypeIdx);
#ifdef TRACE_CLASSFILE
                DBGLOG("%u: ref(%u) class %u nametype %u", i+1, tag, classIdx, nametypeIdx);
#endif
                break;
            case CONSTANT_String:
#ifdef TRACE_CLASSFILE
                DBGLOG("%u: Tag %u", i+1, tag);
#endif
                b.skip(2);
                break;
            case CONSTANT_Integer:
            case CONSTANT_Float:
#ifdef TRACE_CLASSFILE
                DBGLOG("%u: Tag %u", i+1, tag);
#endif
                b.skip(4);
                break;
            case CONSTANT_Long:
            case CONSTANT_Double:
#ifdef TRACE_CLASSFILE
                DBGLOG("%u: Tag %u", i+1, tag);
#endif
                b.skip(8);
                break;
            case CONSTANT_NameAndType:
                uint16_t nameIdx;
                uint16_t descIdx;
                b.read(nameIdx);
                b.read(descIdx);
#ifdef TRACE_CLASSFILE
                DBGLOG("%u: NameAndType(%u) name %u desc %u", i+1, tag, nameIdx, descIdx);
#endif
                break;
            case CONSTANT_Utf8:
                // length-prefixed
                uint16_t length;
                b.read(length);
                const byte *val;
                val = b.readDirect(length);
#ifdef TRACE_CLASSFILE
                DBGLOG("%u: %.*s", i+1, length, val);
#endif
                break;
            case CONSTANT_MethodHandle:
#ifdef TRACE_CLASSFILE
                DBGLOG("%u: Tag %u", i+1, tag);
#endif
                b.skip(3);
                break;
            case CONSTANT_MethodType:
#ifdef TRACE_CLASSFILE
                DBGLOG("%u: Tag %u", i+1, tag);
#endif
                b.skip(2);
                break;
            case CONSTANT_InvokeDynamic:
#ifdef TRACE_CLASSFILE
                DBGLOG("%u: Tag %u", i+1, tag);
#endif
                b.skip(4);
                break;
            default:
                DBGLOG("Unexpected tag %u reading bytecode file", tag);
                throwUnexpected();
            }
        }
        uint16_t access_flags; b.read(access_flags);
        uint16_t this_class; b.read(this_class);
        uint16_t super_class; b.read(super_class);
        uint16_t interfaces_count; b.read(interfaces_count);
        b.skip(interfaces_count*sizeof(uint16_t));
        uint16_t fields_count; b.read(fields_count);
#ifdef TRACE_CLASSFILE
        DBGLOG("Access flags %x this_class=%u super_class=%u interfaces_count=%u fields_count=%u", access_flags, this_class, super_class, interfaces_count, fields_count);
#endif
        for (unsigned i = 0; i < fields_count; i++)
        {
            b.skip(6);
            uint16_t attr_count;
            b.read(attr_count);
            for (unsigned j = 0; j < attr_count; j++)
            {
                b.skip(2);
                uint32_t attr_length;
                b.read(attr_length);
                b.skip(attr_length);
            }
        }
        uint16_t methods_count; b.read(methods_count);
#ifdef TRACE_CLASSFILE
        DBGLOG("methods_count %u", methods_count);
#endif
        for (unsigned i = 0; i < methods_count; i++)
        {
            uint16_t flags; b.read(flags);
            uint16_t name; b.read(name);
            uint16_t desc; b.read(desc);
#ifdef TRACE_CLASSFILE
            DBGLOG("Method %u name %u desc %u flags %x", i, name, desc, flags);
#endif
            if (flags & ACC_PUBLIC)
            {
                StringAttr thisName;
                readUtf(thisName, name);
                StringAttr thisSig;
                readUtf(thisSig, desc);
                methodNames.append(thisName);
                methodSigs.append(thisSig);
                methodFlags.append(flags);
            }
            uint16_t attr_count;
            b.read(attr_count);
            for (unsigned j = 0; j < attr_count; j++)
            {
                uint16_t attr_name_idx; b.read(attr_name_idx);
                StringAttr attrName;
                readUtf(attrName, attr_name_idx);
                uint32_t attr_length;
                b.read(attr_length);
                if (streq(attrName, "Signature") && attr_length==2)
                {
                    uint16_t ext_sig_idx; b.read(ext_sig_idx);
                    StringAttr extSig;
                    readUtf(extSig, ext_sig_idx);
#ifdef TRACE_CLASSFILE
                    DBGLOG("Seen extended signature %s", extSig.str());
#endif
                    if (flags & ACC_PUBLIC)
                    {
                        methodSigs.pop();
                        methodSigs.append(extSig);
                    }
                }
                else
                    b.skip(attr_length);
            }
        }
        /* Don't bother reading attributes as they are not really interesting to us
        uint16_t attributes_count; b.read(attributes_count);
#ifdef TRACE_CLASSFILE
        DBGLOG("attributes_count %u", attributes_count);
#endif
        for (unsigned i = 0; i < attributes_count; i++)
        {
            b.skip(2);
            uint32_t attr_length;
            b.read(attr_length);
            b.skip(attr_length);
        }
#ifdef TRACE_CLASSFILE
        DBGLOG("%u of %u bytes remaining", b.remaining(), b.length());
#endif
        */
        // Now we can find this class name
        readTag(this_class, CONSTANT_Class);
        readUtf(className, readIdx());
    }
    ~JavaClassReader()
    {
        delete [] constOffsets;
    }
    StringBuffer & getSignature(StringBuffer &ret, unsigned idx) const
    {
        if (!methodNames.isItem(idx))
            throw makeStringException(0, "No public static method found");
        ret.appendf("%s.%s:", className.get(), methodNames.item(idx));
        if ((methodFlags[idx] & ACC_STATIC) == 0)
            ret.append('@');
        return ret.append(methodSigs.item(idx));
    }
    const char *queryClassName() const
    {
        return className.get();
    }
    MemoryBuffer &getEmbedData(MemoryBuffer &result, const char *funcName, bool mainClass) const
    {
        result.setEndian(__BIG_ENDIAN);
        StringBuffer signature;
        if (mainClass)
        {
            unsigned methodIdx = getFunctionIdx(funcName);
            getSignature(signature, methodIdx);
        }
        else
            signature.set(className);
        result.append((size32_t) signature.length());
        result.append(signature.length(), signature.str());
        result.append((size32_t) b.length());
        result.append(b);
        return result;
    }

    enum access_flag : uint16_t
    {
        ACC_PUBLIC    = 0x0001, //  Declared public; may be accessed from outside its package.
        ACC_PRIVATE   = 0x0002, //  Declared private; accessible only within the defining class.
        ACC_PROTECTED = 0x0004, //  Declared protected; may be accessed within subclasses.
        ACC_STATIC    = 0x0008, //  Declared static.
        ACC_FINAL     = 0x0010, //  Declared final; must not be overridden (§5.4.5).
        ACC_SYNCHRONIZED = 0x0020, //  Declared synchronized; invocation is wrapped by a monitor use.
        ACC_BRIDGE    = 0x0040, //  A bridge method, generated by the compiler.
        ACC_VARARGS   = 0x0080, //  Declared with variable number of arguments.
        ACC_NATIVE    = 0x0100, //  Declared native; implemented in a language other than Java.
        ACC_ABSTRACT  = 0x0400, //  Declared abstract; no implementation is provided.
        ACC_STRICT    = 0x0800, //  Declared strictfp; floating-point mode is FP-strict.
        ACC_SYNTHETIC = 0x1000, //  Declared synthetic; not present in the source code.
    };

    unsigned getFlags(const char *funcName) const
    {
        unsigned idx = getFunctionIdx(funcName);
        return methodFlags[idx];
    }

private:
    bool isConstructor(const char *name) const
    {
        const char *shortClass = strrchr(className, '/');
        if (shortClass)
            shortClass++;
        else
            shortClass = className;
        return streq(shortClass, name);
    }
    unsigned getFunctionIdx(const char *funcName) const
    {
        if (isConstructor(funcName))
            funcName = "<init>";
        unsigned methodIdx = (unsigned) -1;
        ForEachItemIn(idx, methodNames)
        {
            if (streq(funcName, methodNames[idx]))
            {
                if (methodIdx != (unsigned) -1)
                    throw makeStringExceptionV(0, "Embedded java has multiple public methods called %s", funcName);
                methodIdx = idx;
            }
        }
        if (methodIdx == (unsigned) -1)
            throw makeStringExceptionV(0, "Embedded java should export a public method %s", funcName);
        return methodIdx;
    }
    uint16_t readIdx()
    {
        uint16_t idx;
        b.read(idx);
        return idx;
    }
    void readTag(unsigned idx, byte expected)
    {
        b.reset(constOffsets[idx]);
        byte tag;
        b.read(tag);
        assertex(tag == expected);
    }
    void readUtf(StringAttr &dest, unsigned idx)
    {
        auto savepos = b.getPos();
        readTag(idx, CONSTANT_Utf8);
        uint16_t length;
        b.read(length);
        dest.set((const char *) b.readDirect(length), length);
        b.reset(savepos);
    }
    enum const_type
    {
        CONSTANT_Class = 7,
        CONSTANT_Fieldref = 9,
        CONSTANT_Methodref = 10,
        CONSTANT_InterfaceMethodref = 11,
        CONSTANT_String = 8,
        CONSTANT_Integer = 3,
        CONSTANT_Float = 4,
        CONSTANT_Long = 5,
        CONSTANT_Double = 6,
        CONSTANT_NameAndType = 12,
        CONSTANT_Utf8 = 1,
        CONSTANT_MethodHandle = 15,
        CONSTANT_MethodType = 16,
        CONSTANT_InvokeDynamic = 18
    };
    MemoryBuffer b;
    unsigned *constOffsets = nullptr;
    StringAttr className;
    StringArray methodNames;
    StringArray methodSigs;
    UnsignedArray methodFlags;
};

// Objects of class JavaEmbedImportContext are created locally for each call of a function, or thread-local to persist from one call to the next.
// Methods in here do not need to be thread-safe

class JavaEmbedImportContext : public CInterfaceOf<IJavaEmbedFunctionContext>
{
public:
    JavaEmbedImportContext(ICodeContext *codeCtx, JavaThreadContext *_sharedCtx, jobject _instance, unsigned _flags, const char *options, const IThorActivityContext *_activityContext)
    : sharedCtx(_sharedCtx), JNIenv(sharedCtx->JNIenv), instance(_instance), flags(_flags), activityContext(_activityContext)
    {
        argcount = 0;
        argsig = NULL;
        nonStatic = (instance != nullptr);
        javaClass = nullptr;
        StringArray opts;
        opts.appendList(options, ",");
        if (codeCtx)
        {
            engine = codeCtx->queryEngineContext();
            try {
                nodeNum = codeCtx->getNodeNum();
            }
            catch (IException *E)
            {
                E->Release();  // We may get an error if calling on the master - we just want to ignore it
            }
        }
        StringBuffer lclassPath;
        if (engine)
        {
            const StringArray &manifestJars = engine->queryManifestFiles("jar");
            ForEachItemIn(idx, manifestJars)
            {
                lclassPath.append(';').append(manifestJars.item(idx));
            }
        }
        ForEachItemIn(idx, opts)
        {
            const char *opt = opts.item(idx);
            const char *val = strchr(opt, '=');
            if (val)
            {
                StringBuffer optName(val-opt, opt);
                val++;
                if (stricmp(optName, "classpath")==0)
                    lclassPath.append(';').append(val);
                else if (strieq(optName, "globalscope"))
                    globalScopeKey.set(val);
                else if (strieq(optName, "persist"))
                {
                    if (persistMode != persistNone)
                        throw MakeStringException(MSGAUD_user, 0, "javaembed: Persist option specified more than once");
                    persistMode = getPersistMode(val, globalScopeKey);
                    switch (persistMode)
                    {
                    case persistChannel:
                    case persistWorkunit:
                    case persistQuery:
                        if (!engine)
                            throw MakeStringException(MSGAUD_user, 0, "javaembed: Persist mode '%s' not supported here", val);
                        break;
                    }
                }
                else
                    throw MakeStringException(0, "javaembed: Unknown option %s", optName.str());
            }
        }
        if (lclassPath.length()>1)
            classpath.set(lclassPath.str()+1);
        if (flags & EFthreadlocal)
            sharedCtx->registerContext(this);  // Do at end - otherwise an exception thrown during construction will leave this reference dangling
    }
    ~JavaEmbedImportContext()
    {
        if (javaClass)
            JNIenv->DeleteGlobalRef(javaClass);
        if (classLoader)
            JNIenv->DeleteGlobalRef(classLoader);
    }
    virtual void endThread() override
    {
        instance = nullptr;
        if (javaClass)
        {
            JNIenv->DeleteGlobalRef(javaClass);
            javaClass = nullptr;
        }
        if (classLoader)
        {
            JNIenv->DeleteGlobalRef(classLoader);
            classLoader = nullptr;
        }
        javaMethodID = nullptr;
    }

    virtual bool getBooleanResult()
    {
        switch (*returnType)
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
                bool ret=JNIenv->CallBooleanMethod(result.l, getVal);
                return ret;
            }
        default:
            throw MakeStringException(MSGAUD_user, 0, "javaembed: Type mismatch on result");
        }
    }
    virtual void getDataResult(size32_t &__len, void * &__result)
    {
        if (strcmp(returnType, "[B")!=0)
            throw resultMismatchException("data");
        jbyteArray array = (jbyteArray) result.l;
        __len = (array != NULL ? JNIenv->GetArrayLength(array) : 0);
        __result = (__len > 0 ? rtlMalloc(__len) : NULL);
        if (__result)
            JNIenv->GetByteArrayRegion(array, 0, __len, (jbyte *) __result);
    }
    virtual double getRealResult()
    {
        switch (*returnType)
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
                    throw resultMismatchException("real");
                double ret = JNIenv->CallDoubleMethod(result.l, getVal);
                return ret;
            }
        default:
            throw resultMismatchException("real");
        }
    }
    virtual __int64 getSignedResult()
    {
        switch (*returnType)
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
                    throw resultMismatchException("integer");
                __int64 ret = JNIenv->CallLongMethod(result.l, getVal);
                return ret;
            }
        default:
            throw resultMismatchException("integer");
        }
    }
    virtual unsigned __int64 getUnsignedResult()
    {
        if (*returnType=='V' && strieq(methodName, "<init>"))
            return (unsigned __int64) result.l;
        if (*returnType=='L' && JNIenv->IsSameObject(result.l, instance) && persistMode != persistNone)
            return (unsigned __int64) instance;
        StringBuffer s;
        throw makeStringExceptionV(MSGAUD_user, 0, "javaembed: In method %s: Unsigned results not supported", getReportName(s).str()); // Java doesn't support unsigned
    }
    virtual void getStringResult(size32_t &__len, char * &__result)
    {
        switch (*returnType)
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
            throw resultMismatchException("string");
        }
    }
    virtual void getUTF8Result(size32_t &__chars, char * &__result)
    {
        switch (*returnType)
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
            throw resultMismatchException("utf8");
        }
    }
    virtual void getUnicodeResult(size32_t &__chars, UChar * &__result)
    {
        switch (*returnType)
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
            throw resultMismatchException("unicode");
        }
    }
    virtual void getSetResult(bool & __isAllResult, size32_t & __resultBytes, void * & __result, int _elemType, size32_t elemSize)
    {
        if (*returnType != '[')
            throw resultMismatchException("array");
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
            switch(returnType[1])
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
            {
                StringBuffer s;
                throw MakeStringException(0, "javaembed: In method %s: Return type mismatch (char[] not supported)", getReportName(s).str());
                break;
            }
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
                            StringBuffer s;
                            throw MakeStringException(0, "javaembed: In method %s: Return type mismatch (ECL string type expected)", getReportName(s).str());
                        }
                        JNIenv->ReleaseStringUTFChars(elem, text);
                        JNIenv->DeleteLocalRef(elem);
                        if (elemSize != UNKNOWN_LENGTH)
                            outData += elemSize;
                    }
                }
                else
                {
                    StringBuffer s;
                    throw MakeStringException(0, "javaembed: In method %s: Return type mismatch (%s[] not supported)", getReportName(s).str(), returnType+2);
                }
                break;
            }
        }
        __isAllResult = false;
        __resultBytes = elemSize == UNKNOWN_LENGTH ? outBytes : elemSize * numResults;
        __result = out.detachdata();
    }
    virtual IRowStream *getDatasetResult(IEngineRowAllocator * _resultAllocator)
    {
        jclass iterClass =JNIenv->GetObjectClass(result.l);
        if (!JNIenv->IsAssignableFrom(iterClass, utilIteratorClass))
        {
            if (JNIenv->IsAssignableFrom(iterClass, langIterableClass))
            {
                result.l = JNIenv->CallObjectMethod(result.l, iterable_iterator);
            }
            else
            {
                StringBuffer s;
                throw MakeStringException(0, "javaembed: In method %s: Java code should return an iterator or iterable object", getReportName(s).str());
            }
        }
        return new JavaRowStream(result.l, _resultAllocator);
    }
    virtual byte * getRowResult(IEngineRowAllocator * _resultAllocator)
    {
        RtlDynamicRowBuilder rowBuilder(_resultAllocator);
        size32_t len = getRowResult(result.l, rowBuilder);
        return (byte *) rowBuilder.finalizeRowClear(len);
    }
    virtual size32_t getTransformResult(ARowBuilder & builder)
    {
        return getRowResult(result.l, builder);
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
        jbyteArray javaData = JNIenv->NewByteArray(len);
        JNIenv->SetByteArrayRegion(javaData, 0, len, (jbyte *) val);
        v.l = javaData;
        addArg(v);
    }
    virtual void bindFloatParam(const char *name, float val)
    {
        // Could argue that the size should match...
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
    virtual void bindSignedSizeParam(const char *name, int size, __int64 val)
    {
        bindSignedParam(name, val);
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
    virtual void bindUnsignedSizeParam(const char *name, int size, unsigned __int64 val)
    {
        StringBuffer s;
        throw MakeStringException(MSGAUD_user, 0, "javaembed: In method %s: Unsigned parameters not supported", getReportName(s).str()); // Java doesn't support unsigned
    }
    virtual void bindUnsignedParam(const char *name, unsigned __int64 val)
    {
        if (!strchr(importName, '.') && argcount==0)  // Could require a flag, or a special parameter name...
        {
            if (!val)
            {
                StringBuffer s;
                throw MakeStringException(MSGAUD_user, 0, "javaembed: In method %s: Null value passed for \"this\"", getReportName(s).str());
            }
            instance = (jobject) val;
            persistMode = persistSupplied;
            if (JNIenv->GetObjectRefType(instance) != JNIGlobalRefType)
            {
                StringBuffer s;
                throw MakeStringException(MSGAUD_user, 0, "javaembed: In method %s: Invalid value passed for \"this\"", getReportName(s).str());
            }
            jclass newJavaClass = JNIenv->GetObjectClass(instance);
            if (!JNIenv->IsSameObject(newJavaClass, javaClass))
            {
                if (javaClass)
                {
                    JNIenv->DeleteGlobalRef(javaClass);
                    javaClass = nullptr;
                }
                if (classLoader)
                {
                    JNIenv->DeleteGlobalRef(classLoader);
                    classLoader = nullptr;
                }
                loadFunction(classpath, 0, nullptr);
            }
            reinit();
        }
        else
        {
            // We could match a java class, to allow objects returned from one embed to be passed as parameters to another
            StringBuffer s;
            throw MakeStringException(MSGAUD_user, 0, "javaembed: In method %s: Unsigned parameters not supported", getReportName(s).str()); // Java doesn't support unsigned
        }
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
                v.l = JNIenv->NewString(unicode, unicodeChars);
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
                v.l = JNIenv->NewString(unicode, unicodeChars);
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
                v.l = JNIenv->NewString(val, numchars);
                break;
            }
            // fall into ...
        default:
            typeError("UNICODE");
            break;
        }
        addArg(v);
    }
    virtual void bindSetParam(const char *name, int _elemType, size32_t elemSize, bool isAll, size32_t totalBytes, const void *setData)
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
            v.l = JNIenv->NewBooleanArray(numElems);
            JNIenv->SetBooleanArrayRegion((jbooleanArray) v.l, 0, numElems, (jboolean *) setData);
            break;
        case 'B':
            checkType(type_int, sizeof(jbyte), elemType, elemSize);
            v.l = JNIenv->NewByteArray(numElems);
            JNIenv->SetByteArrayRegion((jbyteArray) v.l, 0, numElems, (jbyte *) setData);
            break;
        case 'C':
            // we COULD map to a set of string1, but is there any point?
            typeError("");
            break;
        case 'S':
            checkType(type_int, sizeof(jshort), elemType, elemSize);
            v.l = JNIenv->NewShortArray(numElems);
            JNIenv->SetShortArrayRegion((jshortArray) v.l, 0, numElems, (jshort *) setData);
            break;
        case 'I':
            checkType(type_int, sizeof(jint), elemType, elemSize);
            v.l = JNIenv->NewIntArray(numElems);
            JNIenv->SetIntArrayRegion((jintArray) v.l, 0, numElems, (jint *) setData);
            break;
        case 'J':
            checkType(type_int, sizeof(jlong), elemType, elemSize);
            v.l = JNIenv->NewLongArray(numElems);
            JNIenv->SetLongArrayRegion((jlongArray) v.l, 0, numElems, (jlong *) setData);
            break;
        case 'F':
            checkType(type_real, sizeof(jfloat), elemType, elemSize);
            v.l = JNIenv->NewFloatArray(numElems);
            JNIenv->SetFloatArrayRegion((jfloatArray) v.l, 0, numElems, (jfloat *) setData);
            break;
        case 'D':
            checkType(type_real, sizeof(jdouble), elemType, elemSize);
            v.l = JNIenv->NewDoubleArray(numElems);
            JNIenv->SetDoubleArrayRegion((jdoubleArray) v.l, 0, numElems, (jdouble *) setData);
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
                v.l = JNIenv->NewObjectArray(numElems, langStringClass, NULL);
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
                        thisElem = JNIenv->NewString(unicode.getustr(), unicodeChars);
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
                        thisElem = JNIenv->NewString(unicode.getustr(), unicodeChars);
                        break;
                    }
                    case type_unicode:
                    {
                        if (elemSize == UNKNOWN_LENGTH)
                        {
                            thisSize = (* (size32_t *) inData) * sizeof(UChar); // NOTE - it's in chars...
                            inData += sizeof(size32_t);
                        }
                        thisElem = JNIenv->NewString((const UChar *) inData, thisSize / sizeof(UChar));
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
                        thisElem = JNIenv->NewString(unicode.getustr(), unicodeChars);
                        thisSize = rtlUtf8Size(numChars, inData);
                        break;
                    }
                    default:
                        typeError("STRING");
                    }
                    inData += thisSize;
                    JNIenv->SetObjectArrayElement((jobjectArray) v.l, idx, thisElem);
                    JNIenv->DeleteLocalRef(thisElem);
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
    virtual void bindRowParam(const char *name, IOutputMetaData & metaVal, const byte *val) override
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
        JavaObjectBuilder javaBuilder((CheckedJNIEnv *)JNIenv, &dummyField, loadClass(className));
        typeInfo->process(val, val, &dummyField, javaBuilder); // Creates a java object from the incoming ECL row
        jvalue v;
        v.l = javaBuilder.getObject();
        addArg(v);
    }
    virtual IInterface *bindParamWriter(IInterface *esdl, const char *esdlservice, const char *esdltype, const char *name)
    {
        if (*argsig != 'L')  // should tell us the type of the object we need to create to pass in
            typeError("OBJECT");
        // Class name is from the char after the L up to the first ;
        const char *tail = strchr(argsig, ';');
        if (!tail)
            typeError("OBJECT");
        StringAttr className(argsig+1, tail - (argsig+1));
        argsig = tail+1;
        Owned<JavaXmlBuilder> writer = new JavaXmlBuilder(JNIenv, dynamic_cast<IEsdlDefinition*>(esdl), esdlservice, esdltype);
        writer->initWriter();
        return (IXmlWriter*)writer.getClear();
    }
    virtual void paramWriterCommit(IInterface *writer)
    {
        JavaXmlBuilder *javaWriter = dynamic_cast<JavaXmlBuilder*>(writer);
        if (!javaWriter)
            throw MakeStringException(0, "javaembed: Invalid object writer for %s", signature.get());
        jvalue v;
        v.l = javaWriter->getObject();
        addArg(v);
    }
    virtual void bindDatasetParam(const char *name, IOutputMetaData & metaVal, IRowStream * val)
    {
        jvalue v;
        char argsigStart = *argsig;
        switch (argsigStart)
        {
        case '[':
        case '<':
            ++argsig;
            break;
        case 'L':
            if (strncmp(argsig, "Ljava/util/Iterator<", 20) == 0)
            {
                argsig += 20;
                break;
            }
            /* no break */
        default:
            typeError("DATASET");
        }
        if (*argsig != 'L')  // should tell us the type of the object we need to create to pass in
            typeError("DATASET");
        // Class name is from the char after the L up to the first ;
        const char *tail = strchr(argsig, ';');
        if (!tail)
            typeError("RECORD");
        StringAttr className(argsig+1, tail - (argsig+1));
        argsig = tail+1;
        if (argsigStart=='L')
        {
            if (argsig[0] != '>' || argsig[1] != ';')
                typeError("DATASET");
            argsig += 2;
        }
        if (argsigStart=='[')
        {
            // Pass in an array of objects
            PointerArrayOf<_jobject> allRows;
            const RtlTypeInfo *typeInfo = metaVal.queryTypeInfo();
            assertex(typeInfo);
            RtlFieldStrInfo dummyField("<row>", NULL, typeInfo);
            jclass Class = loadClass(className);
            JavaObjectBuilder javaBuilder((CheckedJNIEnv *) JNIenv, &dummyField, Class);
            for (;;)
            {
                roxiemem::OwnedConstRoxieRow thisRow = val->ungroupedNextRow();
                if (!thisRow)
                    break;
                const byte *brow = (const byte *) thisRow.get();
                typeInfo->process(brow, brow, &dummyField, javaBuilder); // Creates a java object from the incoming ECL row
                allRows.append(javaBuilder.getObject());
            }
            jobjectArray array = JNIenv->NewObjectArray(allRows.length(), Class, NULL);
            ForEachItemIn(idx, allRows)
            {
                JNIenv->SetObjectArrayElement(array, idx, allRows.item(idx));
            }
            v.l = array;
        }
        else
        {
            // Pass in an iterator
            // Create a java object of type com.HPCCSystems.HpccUtils - this acts as a proxy for the iterator
            JNIenv->ExceptionClear();
            jvalue param;
            const RtlTypeInfo *typeInfo = metaVal.queryTypeInfo();
            ECLDatasetIterator *iterator = new ECLDatasetIterator((CheckedJNIEnv *) JNIenv, typeInfo, loadClass(className), val);
            param.j = (jlong) iterator;
            iterators.append(*iterator);
            jobject proxy = JNIenv->NewObject(hpccIteratorClass, hi_constructor, param, JNIenv->NewStringUTF(helperLibraryName));
            v.l = proxy;
        }
        addArg(v);
    }
    virtual void writeResult(IInterface *esdl, const char *esdlservice, const char *esdltype, IInterface *writer)
    {
        return sharedCtx->writeObjectResult(result.l, dynamic_cast<IEsdlDefinition*>(esdl), esdlservice, esdltype, dynamic_cast<IXmlWriter*>(writer));
    }
    virtual void importFunction(size32_t lenChars, const char *utf) override
    {
        if (!javaClass)
        {
            size32_t bytes = rtlUtf8Size(lenChars, utf);
            importName.set(utf, bytes);
            if (strchr(importName, '.'))
                loadFunction(classpath, 0, nullptr);
        }
        if (javaClass)
            reinit();
    }
    void bindActivityParam()
    {
        // Note: We don't require that the function takes an activityCtx parameter - if they don't care, they can omit the param
        if (strncmp(argsig, "Lcom/HPCCSystems/ActivityContext;", 33) == 0)
        {
            argsig += 33;
            jvalue v;
            v.l = JNIenv->NewObject(hpccIteratorClass, hi_constructor, activityContext, JNIenv->NewStringUTF(helperLibraryName));
            addArg(v);
        }

    }

    IException *translateException(IException *E)
    {
        StringBuffer msg;
        E->errorMessage(msg);
        const char *text = msg;
        if (strncmp(text, "javaembed: ", 11)==0)
            text += 11;
        auto code = E->errorCode();
        auto aud = E->errorAudience();
        E->Release();
        StringBuffer s;
        return makeStringExceptionV(aud, code, "javaembed: In method %s: %s", getReportName(s).str(), text);
    }

    IException *resultMismatchException(const char *expected)
    {
        StringBuffer s;
        return makeStringExceptionV(0, "javaembed: In method %s: Type mismatch on result (%s expected)", getReportName(s).str(), expected);
    }

    virtual void callFunction()
    {
        try
        {
            if (*argsig != ')')
                throw MakeStringException(0, "Too few ECL parameters passed for Java signature %s", signature.get());
            JNIenv->ExceptionClear();
            if (nonStatic)
            {
                if (streq(methodName, "<init>"))
                {
                    if (!instance)
                    {
                        if (persistMode == persistNone)
                            throw MakeStringException(0, "Cannot return object without persist");
                        StringBuffer scopeKey;
                        getScopeKey(scopeKey);
                        PersistedObjectCriticalBlock persistBlock;
                        persistBlock.enter(persistMode==persistThread ? sharedCtx->getLocalObject(JNIenv, scopeKey) : globalState->getGlobalObject(JNIenv, scopeKey));
                        instance = persistBlock.getInstance();
                        if (instance)
                            persistBlock.leave();
                        else
                        {
                            instance = JNIenv->NewGlobalRef(JNIenv->NewObjectA(javaClass, javaMethodID, args), "constructor");
#ifdef TRACE_GLOBALREF
                            StringBuffer myClassName;
                            getClassNameForObject(JNIenv, myClassName, instance);
                            DBGLOG("Constructed object %p of class %s", instance, myClassName.str());
#endif
                            if (persistMode==persistQuery || persistMode==persistWorkunit || persistMode==persistChannel)
                            {
                                assertex(engine);
                                engine->onTermination(JavaGlobalState::unregister, scopeKey.str(), persistMode==persistWorkunit);
                            }
                            persistBlock.leave(instance);
                        }
                    }
                    result.l = instance;
                    return;
                }
                else if (!instance)
                {
                    assertex(persistMode == persistNone); // Any other persist mode should have already created the instance
                    instance = createInstance();          // Local object, will be released at exit() from function
                }
                assertex(javaMethodID);
                switch (*returnType)
                {
                case 'C': result.c = JNIenv->CallCharMethodA(instance, javaMethodID, args); break;
                case 'Z': result.z = JNIenv->CallBooleanMethodA(instance, javaMethodID, args); break;
                case 'J': result.j = JNIenv->CallLongMethodA(instance, javaMethodID, args); break;
                case 'F': result.f = JNIenv->CallFloatMethodA(instance, javaMethodID, args); break;
                case 'D': result.d = JNIenv->CallDoubleMethodA(instance, javaMethodID, args); break;
                case 'I': result.i = JNIenv->CallIntMethodA(instance, javaMethodID, args); break;
                case 'S': result.s = JNIenv->CallShortMethodA(instance, javaMethodID, args); break;
                case 'B': result.s = JNIenv->CallByteMethodA(instance, javaMethodID, args); break;
                case '[':
                case 'L': result.l = JNIenv->CallObjectMethodA(instance, javaMethodID, args); break;
                case 'V': JNIenv->CallVoidMethodA(instance, javaMethodID, args); result.l = nullptr; break;
                default: throwUnexpected();
                }
            }
            else
            {
                switch (*returnType)
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
                case 'V': JNIenv->CallStaticVoidMethodA(javaClass, javaMethodID, args); result.l = nullptr; break;
                default: throwUnexpected();
                }
            }
        }
        catch (IException *E)
        {
            throw translateException(E);
        }
    }

    virtual void compileEmbeddedScript(size32_t lenChars, const char *_script)
    {
        throwUnexpected();
    }
    virtual void loadCompiledScript(size32_t bytecodeLen, const void *bytecode) override
    {
        if (!javaClass)
        {
            MemoryBuffer b;
            b.setBuffer(bytecodeLen, (void *) bytecode, false);
            b.setEndian(__BIG_ENDIAN);
            uint32_t siglen; b.read(siglen);
            const char *sig = (const char *) b.readDirect(siglen);
            size32_t bytes = rtlUtf8Size(siglen, sig);  // MORE - check that this size is serialized in chars not bytes!
            importName.set(sig, bytes);
            loadFunction(classpath, bytecodeLen, (const byte *) bytecode);
        }
        reinit();
    }
    virtual void enter() override
    {
        reenter(nullptr);
    }
    virtual void reenter(ICodeContext *codeCtx) override
    {
        // If we rejig codegen to only call loadCompiledScript etc at construction time, then this will need to do the reinit()
        // until we do, it's too early

        if (codeCtx)
            engine = codeCtx->queryEngineContext();
        else if (flags & EFthreadlocal && persistMode > persistThread)
        {
            StringBuffer s;
            throw MakeStringException(0, "javaembed: In method %s: Workunit must be recompiled to support this persist mode", getReportName(s).str());
        }

        // Create a new frame for local references and increase the capacity
        // of those references to 64 (default is 16)

        JNIenv->PushLocalFrame(64);
    }
    virtual void exit() override
    {
        if (persistMode==persistNone)
            instance = 0;  // otherwise we leave it for next call as it saves a lot of time looking it up
        iterators.kill();
#ifdef FORCE_GC
        forceGC(JNIenv);
#endif
        JNIenv->PopLocalFrame(nullptr);
    }

protected:
    __declspec(noreturn) void typeError(const char *ECLtype) __attribute__((noreturn))
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
        {
            StringBuffer s;
            throw MakeStringException(0, "javaembed: In method %s: Too many ECL parameters passed for Java signature", getReportName(s).str());
        }
        default:
            StringBuffer s;
            throw MakeStringException(0, "javaembed: In method %s: Unrecognized character %c in Java signature", getReportName(s).str(), *argsig);
        }
        if (!javaLen)
            javaLen = strlen(javaType);
        StringBuffer s;
        throw MakeStringException(0, "javaembed: In Method %s: ECL type %s cannot be passed to Java type %.*s", getReportName(s).str(), ECLtype, javaLen, javaType);
    }
    void addArg(jvalue &arg)
    {
        assertex(argcount < MAX_JNI_ARGS);
        args[argcount] = arg;
        argcount++;
    }

    size32_t getRowResult(jobject result, ARowBuilder &builder)
    {
        const RtlTypeInfo *typeInfo = builder.queryAllocator()->queryOutputMeta()->queryTypeInfo();
        assertex(typeInfo);
        RtlFieldStrInfo dummyField("<row>", NULL, typeInfo);
        JavaRowBuilder javaRowBuilder((CheckedJNIEnv *) JNIenv, &dummyField, result);
        return typeInfo->build(builder, 0, &dummyField, javaRowBuilder);
    }

    jclass loadClass(const char *className)
    {
        StringBuffer uclassname(className);
        uclassname.replace('/', '.');
        className=uclassname.str();
        JNIenv->ExceptionClear();
        jmethodID loadClassMethod = JNIenv->GetMethodID(JNIenv->GetObjectClass(classLoader), "loadClass","(Ljava/lang/String;)Ljava/lang/Class;");
        jstring classNameString = JNIenv->NewStringUTF(className);
        jclass Class = (jclass) JNIenv->CallObjectMethod(classLoader, loadClassMethod, classNameString);
        return Class;
    }

    StringBuffer &getReportName(StringBuffer &s)
    {
        if (classname.length())
        {
            const char *report = strrchr(classname.str(), '.');
            if (report)
                report++;
            else
                report = classname.str();
            s.append(classname).append('.');
        }
        return s.append(methodName);
    }

    jobject createInstance()
    {
        jmethodID constructor;
        try
        {
            constructor = JNIenv->GetMethodID(javaClass, "<init>", "()V");
        }
        catch (IException *E)
        {
            Owned<IException> e = E;
            throw MakeStringException(0, "parameterless constructor required");
        }
        return JNIenv->NewObject(javaClass, constructor);
    }

    void loadFunction(const char *classpath, size32_t bytecodeLen, const byte *bytecode)
    {
        try
        {
            StringAttr checkedClassName;
            // Name should be in the form class.method:signature
            const char *funcname = strrchr(importName, '.');
            if (funcname)
            {
                classname.clear().append(funcname-importName, importName);
                classname.replace('/', '.');
                funcname++;  // skip the '.'
            }
            else
                funcname = importName;
            const char *coloncolon = strstr(funcname, "::");
            if (coloncolon)
            {
                // ClassName::FunctionName syntax - used to check that object passed in is of proper class
                checkedClassName.set(funcname, coloncolon-funcname);
                funcname = coloncolon+2;
            }
            const char *sig = strchr(funcname, ':');
            if (sig)
            {
                methodName.set(funcname, sig-funcname);
                sig++; // skip the ':'
                if (*sig == '@') // indicates a non-static method
                {
                    sig++;
                    nonStatic = true;
                }
                else
                    nonStatic = false;
                signature.set(sig);
            }
            else
                methodName.set(funcname);
            bool isConstructor = streq(methodName, "<init>");
            {
                PersistedObjectCriticalBlock persistBlock;
                StringBuffer scopeKey;
                if (nonStatic && !instance && persistMode >= persistThread && !isConstructor)
                {
                    // If a persist scope is specified, we may want to use a pre-existing object. If we do we share its classloader, class, etc.
                    assertex(classname.length());  // MORE - what does this imply?
                    getScopeKey(scopeKey);
                    persistBlock.enter(persistMode==persistThread ? sharedCtx->getLocalObject(JNIenv, scopeKey) : globalState->getGlobalObject(JNIenv, scopeKey));
                    instance = persistBlock.getInstance();
                    if (instance)
                        persistBlock.leave();
                }
                if (instance)
                {
                    javaClass = (jclass) JNIenv->NewGlobalRef(JNIenv->GetObjectClass(instance), "javaClass");
                    classLoader = JNIenv->NewGlobalRef(getClassLoader(JNIenv, javaClass), "classLoader");
                    sharedCtx->setThreadClassLoader(classLoader);
                }
                if (!javaClass)
                {
                    if (!classname)
                        throw MakeStringException(MSGAUD_user, 0, "Invalid import name - Expected classname.methodname:signature");
                    classLoader = JNIenv->NewGlobalRef(sharedCtx->getThreadClassLoader(classpath, classname, bytecodeLen, bytecode), "classLoader");
                    sharedCtx->setThreadClassLoader(classLoader);

                    jmethodID loadClassMethod = JNIenv->GetMethodID(JNIenv->GetObjectClass(classLoader), "loadClass","(Ljava/lang/String;)Ljava/lang/Class;");
                    try
                    {
                        javaClass = (jclass) JNIenv->CallObjectMethod(classLoader, loadClassMethod, JNIenv->NewStringUTF(classname));
                    }
                    catch (IException *E)
                    {
                        Owned<IException> e = E;
                        throw makeWrappedExceptionV(E, E->errorCode(), "Failed to resolve class name %s", classname.str());
                    }
                    javaClass = (jclass) JNIenv->NewGlobalRef(javaClass, "javaClass");
                }
                if (nonStatic && !instance && !isConstructor  && persistMode != persistNone)
                {
                    instance = createInstance();
#ifdef TRACE_GLOBALREF
                    StringBuffer myClassName;
                    getClassNameForObject(JNIenv, myClassName, instance);
                    DBGLOG("Created object %p of class %s", instance, myClassName.str());
#endif
                    if (persistBlock.locked()) // I think this should always be true?
                    {
                        instance = JNIenv->NewGlobalRef(instance, "createInstance");
                        if (persistMode==persistQuery || persistMode==persistWorkunit || persistMode==persistChannel)
                        {
                            assertex(engine);
                            engine->onTermination(JavaGlobalState::unregister, scopeKey.str(), persistMode==persistWorkunit);
                        }
                        persistBlock.leave(instance);
                    }
                }
            }
            if (!signature)
            {
                getSignature(signature, JNIenv, javaClass, funcname);
                if (signature.str()[0]=='@')
                {
                    nonStatic = true;
                    signature.set(signature.str()+1);
                }
                else
                    nonStatic = false;
            }
            StringBuffer javaSignature;
            patchSignature(javaSignature, signature);

            if (nonStatic)
                javaMethodID = JNIenv->GetMethodID(javaClass, methodName, javaSignature);
            else
                javaMethodID = JNIenv->GetStaticMethodID(javaClass, methodName, javaSignature);
            if (checkedClassName)
            {
                StringBuffer myClassName;
                getClassNameForObject(JNIenv, myClassName, instance);
#ifdef CHECK_JNI
                DBGLOG("Checking class name %s for %p matches %s for function %s", myClassName.str(), instance, checkedClassName.str(), methodName.str());
#endif
                const char *shortClassName = strrchr(myClassName, '.');
                if (shortClassName)
                    shortClassName++;
                else
                    shortClassName = myClassName;
                if (!streq(checkedClassName, shortClassName))
                    throw MakeStringException(0, "Object class %s does not match expected class name %s", shortClassName, checkedClassName.str());
            }
            returnType = strrchr(signature, ')');
            assertex(returnType);  // Otherwise how did Java accept it??
            returnType++;
        }
        catch(IException *E)
        {
            throw translateException(E);
        }
    }

    static StringBuffer &patchSignature(StringBuffer &ret, const char *signature)
    {
        // We need to patch up the provided signature - any instances of <classname; need to be replaced by Ljava.utils.iterator
        const char *finger = signature;
        while (finger && *finger)
        {
            if (*finger == '<')
            {
                // If there is a corresponding >, assume it's the 'extended' form and just strip out the bit from < to >
                const char *close = strchr(finger, '>');
                if (close)
                    finger = close;
                else
                {
                    ret.append("Ljava/util/Iterator;");
                    finger = strchr(finger, ';');
                    if (!finger)
                        throw MakeStringException(MSGAUD_user, 0, "javaembed: Invalid java function signature %s", signature);
                }
            }
            else
                ret.append(*finger);
            finger++;
        }
        return ret;
    }

    StringBuffer &getScopeKey(StringBuffer &ret)
    {
        if (globalScopeKey)
            ret.append(globalScopeKey).append('.');
        ret.append(classname).append('.');
        switch (persistMode)
        {
        case persistThread:
            ret.append(__uint64(GetCurrentThreadId()));
            break;
        case persistGlobal:
            ret.append("global");
            break;
            // Fall into
        case persistWorkunit:
            engine->getQueryId(ret, true);
            break;
        case persistChannel:
            ret.append(nodeNum).append('.');
        case persistQuery:
            engine->getQueryId(ret, false);
            break;
        }
        return ret;
    }

    JavaThreadContext *sharedCtx = nullptr;
    CheckedJNIEnv *JNIenv = nullptr;
    jvalue result = {0};
    StringAttr classpath;
    StringBuffer classname;
    IArrayOf<ECLDatasetIterator> iterators;   // to make sure they get freed
    bool nonStatic = false;
    jobject instance = nullptr; // class instance of object to call methods on
    const IThorActivityContext *activityContext = nullptr;

    unsigned flags = 0;
    unsigned nodeNum = 0;
    StringAttr globalScopeKey;
    PersistMode persistMode = persistNone;  // Defines the lifetime of the java object for which this is called.

    // The following members are set up the first time a method is called only
    IEngineContext *engine = nullptr;
    jclass javaClass = nullptr;
    jobject classLoader = nullptr;
    jmethodID javaMethodID = nullptr;
    StringAttr methodName;
    StringAttr importName;
    StringAttr signature;
    const char *returnType = nullptr;  // A pointer within signature

    // These point to the current arg/signature byte as we are binding
    int argcount = 0;
    jvalue args[MAX_JNI_ARGS];
    const char *argsig = nullptr;  // A pointer within signature

    void reinit()
    {
        argcount = 0;
        argsig = signature;
        assertex(*argsig == '(');
        argsig++;
        if (activityContext)
            bindActivityParam();
    }
};

static __thread JavaThreadContext* threadContext;  // We reuse per thread, for speed

static bool releaseContext(bool isPooled)
{
    if (threadContext)
    {
        threadContext->endThread();
        if (!isPooled)
        {
            delete threadContext;
            threadContext = NULL;
        }
        else
            return true;
    }
    return false;
}

static JavaThreadContext *queryContext()
{
    if (!threadContext)
    {
        threadContext = new JavaThreadContext;
        addThreadTermFunc(releaseContext);
    }
    return threadContext;
}

static CheckedJNIEnv *queryJNIEnv()
{
    return queryContext()->JNIenv;
}

class JavaEmbedServiceContext : public CInterfaceOf<IEmbedServiceContext>
{
public:
    JavaEmbedServiceContext(JavaThreadContext *_sharedCtx, const char *service, const char *_options)
    : sharedCtx(_sharedCtx), Class(0), options(_options), className(service), object(0)
    {
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
                if (strieq(optName, "classpath"))
                    classpath.set(val);
                else
                    throw MakeStringException(0, "javaembed: Unknown option %s", optName.str());
            }
        }
    }
    ~JavaEmbedServiceContext()
    {
        if (object)
            sharedCtx->JNIenv->DeleteGlobalRef(object);
        if (Class)
            sharedCtx->JNIenv->DeleteGlobalRef(Class);
    }
    void init()
    {
        jobject classLoader = sharedCtx->getThreadClassLoader();
        jmethodID loadClassMethod = sharedCtx->JNIenv->GetMethodID(sharedCtx->JNIenv->GetObjectClass(classLoader), "loadClass","(Ljava/lang/String;)Ljava/lang/Class;");
        jstring methodString = sharedCtx->JNIenv->NewStringUTF(className);

        Class = (jclass) sharedCtx->JNIenv->NewGlobalRef(sharedCtx->JNIenv->CallObjectMethod(classLoader, loadClassMethod, methodString), "Class");
        jmethodID constructor = sharedCtx->JNIenv->GetMethodID(Class, "<init>", "()V");
        object = sharedCtx->JNIenv->NewGlobalRef(sharedCtx->JNIenv->NewObject(Class, constructor), "constructed");
    }
    virtual IEmbedFunctionContext *createFunctionContext(const char *function)
    {
        if (!object)
            return NULL;
        Owned<JavaEmbedImportContext> fctx = new JavaEmbedImportContext(nullptr, queryContext(), object, 0, options, nullptr);
        fctx->importFunction(rtlUtf8Length(strlen(function), function), function);
        return fctx.getClear();
    }

protected:
    JavaThreadContext *sharedCtx;
    StringBuffer className;
    jclass Class;
    jobject object;
    StringAttr classpath;
    StringAttr options;
};

class JavaEmbedContext : public CInterfaceOf<IEmbedContext>
{
public:
    virtual IEmbedFunctionContext *createFunctionContext(unsigned flags, const char *options) override
    {
        return createFunctionContextEx(nullptr, nullptr, flags, options);
    }
    virtual IEmbedFunctionContext *createFunctionContextEx(ICodeContext * ctx, const IThorActivityContext *activityCtx, unsigned flags, const char *options) override
    {
        return new JavaEmbedImportContext(ctx, queryContext(), nullptr, flags, options, activityCtx);
    }
    virtual IEmbedServiceContext *createServiceContext(const char *service, unsigned flags, const char *options) override
    {
        Owned<JavaEmbedServiceContext> serviceContext = new JavaEmbedServiceContext(queryContext(), service, options);
        serviceContext->init();
        return serviceContext.getClear();
    }
};

static JavaEmbedContext embedContext;

extern DECL_EXPORT IEmbedContext* queryEmbedContext()
{
    return &embedContext;
}

extern DECL_EXPORT IEmbedContext* getEmbedContext()
{
    return new JavaEmbedContext;
}

static bool isValidIdentifier(const char *source)
{
    return isalnum(*source) || *source=='_' || *source=='$' || ::readUtf8Size(source)>1;   // This is not strictly accurate but probably good enough
}

static bool isFullClassFile(StringBuffer &className, bool &seenPublic, size32_t len, const char *source)
{
    // A heuristic to determine whether the supplied embedded source is a full class file or just a single method
    // Basically, if we see keyword "class" before we see { then we assume it's a full file
    // Also track whether the public keyword has been supplied
    bool inLineComment = false;
    bool inBlockComment = false;
    seenPublic = false;
    while (len)
    {
        if (inLineComment)
        {
            if (*source=='\n')
                inLineComment = false;
        }
        else if (inBlockComment)
        {
            if (*source=='*' && len > 1 && source[1]=='/')
            {
                inBlockComment = false;
                len--;
                source++;
            }
        }
        else switch(*source)
        {
        case '/':
            if (len > 1)
            {
                if (source[1]=='*')
                {
                    inBlockComment = true;
                    len--;
                    source++;
                }
                else if (source[1]=='/')
                    inLineComment = true;
            }
            break;
        case '{':
            return false;
        default:
            if (isValidIdentifier(source))
            {
                const char *start = source;
                while (len && isValidIdentifier(source))
                {
                    source+=::readUtf8Size(source);
                    len--;
                }
                if (source-start == 5 && memcmp(start, "class", source-start)==0)
                {
                    while (len && isspace(*source)) // MORE - a comment between the keyword and the classname will fail - tough.
                    {
                        source += ::readUtf8Size(source);
                        len--;
                    }
                    start = source;
                    while (len && isValidIdentifier(source))
                    {
                        source += ::readUtf8Size(source);
                        len--;
                    }
                    className.append(source-start, start);
                    return true;

                }
                else if (source-start == 6 && memcmp(start, "public", source-start)==0)
                    seenPublic = true;
            }
            if (!len)
                return false;
            break;
        }
        source += ::readUtf8Size(source);
        len--;
    }
    // If we get here then it doesn't have a { at all - we COULD say it needs the prototype too but for now, who knows...
    return false;
}

static bool suppressJavaError(const char *err)
{
    if (!err || !*err)
        return true;
    if (streq(err, "1 error"))
        return true;
    char *rest;
    if (strtoul(err, &rest, 10) && streq(rest, " errors"))
        return true;
    return false;
}

static StringBuffer & cleanupJavaError(StringBuffer &ret, StringBuffer &prefix, const char *err, unsigned lineNumberOffset)
{
    // Remove filename (as it's generated) and fix up line number. Errors that do not have line number use previous error's line number.
    const char *colon = strchr(err, ':');
    // Java errors are a bit of a pain - if you suppress the ones without line numbers you get too little information, if you don't you get too much
    if (colon && isdigit(colon[1]))
    {
        char *end;
        unsigned lineno = strtoul(colon+1, &end, 10) - lineNumberOffset;
        prefix.clear().appendf("(%u,1)", lineno);
        ret.append(prefix).append(end);
    }
    else if (!suppressJavaError(err))
        ret.append(prefix).appendf(": error: %s", err);
    return ret;
}

static void cleanupJavaErrors(StringBuffer &ret, const char *errors, unsigned lineNumberOffset)
{
    StringArray errlines;
    errlines.appendList(errors, "\n", false);
    StringBuffer prefix;
    ForEachItemIn(idx, errlines)
    {
        StringBuffer cleaned;
        cleanupJavaError(cleaned, prefix, errlines.item(idx), lineNumberOffset);
        if (cleaned.length())
            ret.append(cleaned).append('\n');
    }
}

static thread_local unsigned prevHash = 0;
static thread_local MemoryBuffer prevCompile;

void doPrecompile(size32_t & __lenResult, void * & __result, const char *funcName, size32_t charsBody, const char * body, const char *argNames, const char *compilerOptions, const char *persistOptions, StringBuffer &errors, bool checking)
{
    unsigned sizeBody = rtlUtf8Size(charsBody, body);  // size in bytes
    unsigned hash = rtlHash32Data(sizeBody,body,0xcafebabe);
    if (hash==prevHash)  // Reusing result from the syntax check that normally immediately precedes a precompile
    {
        __lenResult = prevCompile.length();
        __result = prevCompile.detachOwn();
        prevHash = 0;
        return;
    }
    StringAttr globalScope;
    PersistMode persistMode = getPersistMode(persistOptions, globalScope);
    StringBuffer tmpDirName;
    getTempFilePath(tmpDirName, "javaembed", nullptr);
    tmpDirName.append(PATHSEPCHAR).append("tmp.XXXXXX");
    if (!mkdtemp((char *) tmpDirName.str()))
        throw makeStringExceptionV(0, "Failed to create temporary directory %s (error %d)", tmpDirName.str(), errno);
    Owned<IFile> tempDir = createIFile(tmpDirName);
    StringBuffer classname;
    bool seenPublic = false;
    bool isFullClass = isFullClassFile(classname, seenPublic, charsBody, body);  // note - we pass in length in characters, not bytes
    if (!isFullClass)
        classname.set("embed");

    VStringBuffer javafile("%s" PATHSEPSTR "%s.java", tmpDirName.str(), classname.str());
    FILE *source = fopen(javafile.str(), "wt");
    fprintf(source, "package com.HPCCSystems.embed.x%x;\n", hash);
    unsigned lineNumberOffset = 1;  // for the /n above
    if (isFullClass)
        fprintf(source, "%.*s", sizeBody, body);
    else
    {
        if (seenPublic)
            fprintf(source, "public class embed\n{\n  %.*s\n}", sizeBody, body);
        else
            fprintf(source, "public class embed\n{\n  public static %.*s\n}", sizeBody, body);
        lineNumberOffset += 2;  // for the /n's above
    }
    fclose(source);

    MemoryBuffer result;
    Owned<IPipeProcess> pipe = createPipeProcess();
    StringBuffer options(compilerOptions);
    if (isEmptyString(compilerOptions))
        options.append("-g:none");
    appendClassPath(options.append(" -cp "));
    VStringBuffer javac("javac %s %s", options.str(), javafile.str());
    if (!pipe->run("javac", javac, tmpDirName, false, false, true, 0, false))
    {
        throw makeStringException(0, "Failed to run javac");
    }
    else
    {
        StringBuffer javaErrors;
        Owned<ISimpleReadStream> pipeReader = pipe->getErrorStream();
        readSimpleStream(javaErrors, *pipeReader);
        pipe->closeError();
        unsigned retcode = pipe->wait();
        cleanupJavaErrors(errors, javaErrors, lineNumberOffset);
        if (retcode)
            throw makeStringException(0, "Failed to precompile java code");
        VStringBuffer mainfile("%s" PATHSEPSTR "%s.class", tmpDirName.str(), classname.str());
        JavaClassReader reader(mainfile);
        DBGLOG("Analysing generated class %s", reader.queryClassName());
        // Not sure how useful this warning is.
        //if (persistMode > persistThread && (reader.getFlags(funcName) & JavaClassReader::ACC_SYNCHRONIZED)==0)
        //    errors.appendf("Warning: persist mode set but function is not synchronized\n");
        reader.getEmbedData(result, funcName, true);
        removeFileTraceIfFail(mainfile);
        // Now read nested classes
        Owned<IDirectoryIterator> classFiles = tempDir->directoryFiles("*$*.class",false,false);
        ForEach(*classFiles)
        {
            const char *thisFile = classFiles->query().queryFilename();
            JavaClassReader reader(thisFile);
            reader.getEmbedData(result, nullptr, false);
            removeFileTraceIfFail(thisFile);
        }
        // We could give a warning if persist is set to anything over "thread" and the function we are calling is not synchronized.
    }
    removeFileTraceIfFail(javafile);
    tempDir->remove();
    __lenResult = result.length();
    __result = result.detachOwn();
}

extern DECL_EXPORT void precompile(size32_t & __lenResult, void * & __result, const char *funcName, size32_t charsBody, const char * body, const char *argNames, const char *compilerOptions, const char *persistOptions)
{
    StringBuffer errors;
    doPrecompile(__lenResult, __result, funcName, charsBody, body, argNames, compilerOptions, persistOptions, errors, false);
}

extern DECL_EXPORT void syntaxCheck(size32_t & __lenResult, char * & __result, const char *funcname, size32_t charsBody, const char * body, const char *argNames, const char *compilerOptions, const char *persistOptions)
{
    StringBuffer result;
    try
    {
        size32_t ds;
        rtlDataAttr d;
        StringBuffer errors;
        doPrecompile(ds, d.refdata(), funcname, charsBody, body, argNames, compilerOptions, persistOptions, result, true);
        // Reuse result in the precompile that normally immediately follows
        unsigned sizeBody = rtlUtf8Size(charsBody, body);  // size in bytes
        prevHash = rtlHash32Data(sizeBody,body,0xcafebabe);
        prevCompile.setBuffer(ds, d.detachdata(), true);
    }
    catch (IException *E)
    {
        StringBuffer msg;
        result.append(E->errorMessage(msg));
        E->Release();
    }
    __lenResult = result.length();
    __result = result.detach();
}

extern DECL_EXPORT void checkImport(size32_t & __lenResult, char * & __result, const char *funcname, size32_t charsImport, const char * import, const char *argNames, const char *compilerOptions, const char *persistOptions)
{
    StringBuffer result;
    try
    {
        StringAttr globalScope;
        PersistMode persistMode = getPersistMode(persistOptions, globalScope);
        if (persistMode > persistThread && !globalScope)
        {
            StringBuffer b(rtlUtf8Size(charsImport, import), import);
            const char *dotpos = strrchr(b, '.');
            if (!dotpos)
                throw MakeStringException(0, "javaembed: cannot determine key for persist in function %s", b.str());
        }
    }
    catch (IException *E)
    {
        StringBuffer msg;
        result.appendf("%s\n", E->errorMessage(msg).str());
        E->Release();
    }
    __lenResult = result.length();
    __result = result.detach();
}

} // namespace

// Callbacks from java

extern "C" {
JNIEXPORT jboolean JNICALL Java_com_HPCCSystems_HpccUtils__1hasNext (JNIEnv *, jclass, jlong);
JNIEXPORT jobject JNICALL Java_com_HPCCSystems_HpccUtils__1next (JNIEnv *, jclass, jlong);
JNIEXPORT jclass JNICALL Java_com_HPCCSystems_HpccClassLoader_defineClassForEmbed(JNIEnv *env, jobject loader, jint bytecodeLen, jlong bytecode, jstring name);
JNIEXPORT void JNICALL Java_com_HPCCSystems_HpccUtils_log(JNIEnv *JNIenv, jclass, jstring msg);

JNIEXPORT jboolean JNICALL Java_com_HPCCSystems_HpccUtils__1isLocal (JNIEnv *, jclass, jlong);
JNIEXPORT jint JNICALL Java_com_HPCCSystems_HpccUtils__1numSlaves (JNIEnv *, jclass, jlong);
JNIEXPORT jint JNICALL Java_com_HPCCSystems_HpccUtils__1numStrands (JNIEnv *, jclass, jlong);
JNIEXPORT jint JNICALL Java_com_HPCCSystems_HpccUtils__1querySlave (JNIEnv *, jclass, jlong);
JNIEXPORT jint JNICALL Java_com_HPCCSystems_HpccUtils__1queryStrand (JNIEnv *, jclass, jlong);
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
        JNIenv->ThrowNew(javaembed::langIllegalArgumentExceptionClass, msg.str());
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
        JNIenv->ThrowNew(javaembed::langIllegalArgumentExceptionClass, msg.str());
        return NULL;
    }
}

JNIEXPORT jclass JNICALL Java_com_HPCCSystems_HpccClassLoader_defineClassForEmbed(JNIEnv *env, jobject loader, jint datalen, jlong data, jstring name)
{
    const char *nameChars = env->GetStringUTFChars(name, nullptr);
    size32_t namelen = strlen(nameChars);
    MemoryBuffer b;
    b.setBuffer(datalen, (void *) data, false);
    b.setEndian(__BIG_ENDIAN);
    jclass ret = nullptr;
    while (b.remaining())
    {
        uint32_t siglen; b.read(siglen);
        const char *sig = (const char *) b.readDirect(siglen);
        uint32_t bytecodeLen; b.read(bytecodeLen);
        const jbyte * bytecode = (const jbyte *) b.readDirect(bytecodeLen);
        if (siglen >= namelen && memcmp(sig, nameChars, namelen)==0 && (namelen == siglen || sig[namelen] == '.'))
        {
#ifdef TRACE_CLASSFILE
            DBGLOG("javaembed: loading class %s (%.*s)", nameChars, siglen, sig);
#endif
            ret = env->DefineClass(nameChars, loader, bytecode, bytecodeLen);
            // NOTE - if there is an exception it will get thrown next time we get into java code
            break;
        }
    }
    if (!ret)
        DBGLOG("javaembed: Failed to load class %s", nameChars);
    env->ReleaseStringUTFChars(name, nameChars);
    return ret;

}

JNIEXPORT void JNICALL Java_com_HPCCSystems_HpccUtils_log(JNIEnv *JNIenv, jclass, jstring msg)
{
    if (msg)
    {
        const char *text = JNIenv->GetStringUTFChars(msg, 0);
        DBGLOG("javaembed: user: %s", text);
        JNIenv->ReleaseStringUTFChars(msg, text);
    }
}

JNIEXPORT jboolean JNICALL Java_com_HPCCSystems_HpccUtils__1isLocal(JNIEnv *JNIenv, jclass, jlong proxy)
{
    const IThorActivityContext *a = (IThorActivityContext *) proxy;
    return a->isLocal();
}

JNIEXPORT jint JNICALL Java_com_HPCCSystems_HpccUtils__1numSlaves(JNIEnv *JNIenv, jclass, jlong proxy)
{
    const IThorActivityContext *a = (IThorActivityContext *) proxy;
    return a->numSlaves();
}
JNIEXPORT jint JNICALL Java_com_HPCCSystems_HpccUtils__1numStrands(JNIEnv *JNIenv, jclass, jlong proxy)
{
    const IThorActivityContext *a = (IThorActivityContext *) proxy;
    return a->numStrands();
}
JNIEXPORT jint JNICALL Java_com_HPCCSystems_HpccUtils__1querySlave(JNIEnv *JNIenv, jclass, jlong proxy)
{
    const IThorActivityContext *a = (IThorActivityContext *) proxy;
    return a->querySlave();
}
JNIEXPORT jint JNICALL Java_com_HPCCSystems_HpccUtils__1queryStrand(JNIEnv *JNIenv, jclass, jlong proxy)
{
    const IThorActivityContext *a = (IThorActivityContext *) proxy;
    return a->queryStrand();
}

// Used for dynamically loading in ESDL

extern "C" DECL_EXPORT IEmbedContext *getEmbedContextDynamic()
{
    return javaembed::getEmbedContext();
}
