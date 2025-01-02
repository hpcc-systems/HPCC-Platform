#include "secure-enclave.hpp"

#include "eclrtl_imp.hpp"
#include "jexcept.hpp"
#include "jiface.hpp"
#include "eclhelper.hpp"
#include "enginecontext.hpp"

#include "util.hpp"
#include "component-model/src/context.hpp"
#include "component-model/src/string.hpp"
#include "component-model/src/list.hpp"

#include <mutex>
#include <filesystem>
#include <wasmtime.hh>

// From deftype.hpp in common
#define UNKNOWN_LENGTH 0xFFFFFFF1

// #define ENABLE_TRACE
#ifdef ENABLE_TRACE
#define TRACE(format, ...) DBGLOG(format __VA_OPT__(, ) __VA_ARGS__)
#else
#define TRACE(format, ...) \
    do                     \
    {                      \
    } while (0)
#endif

using namespace cmcpp;

HostTrap trap = [](const char *msg) -> void
{
    throw makeStringException(100, msg);
};

HostUnicodeConversion convert = [](char8_t *dest, const char8_t *src, uint32_t byte_len, Encoding from_encoding, Encoding to_encoding) -> std::pair<char8_t *, size_t>
{
    if (from_encoding == to_encoding)
    {
        memcpy(dest, src, byte_len);
        return std::make_pair(dest, byte_len);
    }
    else
    {
        throw makeStringExceptionV(100, "Unsupported encoding conversion %d -> %d", static_cast<int>(from_encoding), static_cast<int>(to_encoding));
    }
};

class WasmEngine
{
private:
    std::once_flag wasmLoadedFlag;
    std::unordered_map<std::string, wasmtime::Module> wasmModules;

    wasmtime::Module createModule(const std::string &wasmName, const wasmtime::Span<uint8_t> &wasm)
    {
        TRACE("WasmEngine createModule %s", wasmName.c_str());
        try
        {
            wasmtime::Store store(engine);
            wasmtime::WasiConfig wasi;
            wasi.inherit_argv();
            wasi.inherit_env();
            wasi.inherit_stdin();
            wasi.inherit_stdout();
            wasi.inherit_stderr();
            store.context().set_wasi(std::move(wasi)).unwrap();
            return wasmtime::Module::compile(engine, wasm).unwrap();
        }
        catch (const wasmtime::Error &e)
        {
            throw makeStringExceptionV(100, "WasmEngine createModule failed: %s", e.message().c_str());
        }
    }

    void loadWasmFiles(ICodeContext *codeCtx)
    {
        TRACE("WasmEngine loadWasmFiles");
        IEngineContext *engine = codeCtx->queryEngineContext();
        if (!engine)
            throw makeStringException(100, "Failed to get engine context");

        StringArray manifestModules;
        engine->getManifestFiles("wasm", manifestModules);

        ForEachItemIn(idx, manifestModules)
        {
            const char *path = manifestModules.item(idx);
            TRACE("WasmEngine loadWasmFiles %s", path);
            std::vector<uint8_t> contents = readWasmBinaryToBuffer(path);
            auto module = createModule(path, contents);
            std::filesystem::path p(path);
            wasmModules.insert(std::make_pair(p.stem(), module));
        }
    }

public:
    wasmtime::Engine engine;

    WasmEngine()
    {
        TRACE("WASM SE WasmEngine");
    }

    ~WasmEngine()
    {
        TRACE("WASM SE ~WasmEngine");
    }

    void setCodeContext(ICodeContext *codeCtx)
    {
        TRACE("WASM SE setCodeContext");
        std::call_once(wasmLoadedFlag, &WasmEngine::loadWasmFiles, this, codeCtx);
    }

    bool hasModule(const std::string &wasmName) const
    {
        TRACE("WASM SE hasModule");
        return wasmModules.find(wasmName) != wasmModules.end();
    }

    wasmtime::Module getModule(const std::string &wasmName) const
    {
        TRACE("WASM SE getModule %s", wasmName.c_str());
        auto found = wasmModules.find(wasmName);
        if (found == wasmModules.end())
            throw makeStringExceptionV(100, "Wasm module not found: %s", wasmName.c_str());
        return found->second;
    }
};
static std::unique_ptr<WasmEngine> wasmEngine = std::make_unique<WasmEngine>();

class WasmStore
{
protected:
    wasmtime::Store store;
    std::optional<wasmtime::Instance> instance;
    cmcpp::GuestRealloc realloc;

    std::unordered_map<std::string, wasmtime::Memory> wasmMems;
    std::unordered_map<std::string, wasmtime::Func> wasmFuncs;
    std::unique_ptr<cmcpp::InstanceContext> instanceContext;

public:
    WasmStore(const std::string &wasmName) : store(wasmEngine->engine)
    {
        TRACE("WASM SE WasmStore::WasmStore");
        auto module = wasmEngine->getModule(wasmName);
        try
        {
            wasmtime::Linker linker(wasmEngine->engine);
            linker.define_wasi().unwrap();

            auto callback = [this, wasmName](wasmtime::Caller caller, uint32_t msg, uint32_t msg_len)
            {
                auto data = getData();
                auto msg_ptr = (char *)&data[msg];
                std::string str(msg_ptr, msg_len);
                DBGLOG("from wasm: %s", str.c_str());
            };
            linker.func_wrap("$root", "dbglog", callback).unwrap();
            instance = linker.instantiate(store, module).unwrap();
            linker.define_instance(store, "linking2", *instance).unwrap();

            for (auto exportItem : module.exports())
            {
                auto externType = wasmtime::ExternType::from_export(exportItem);
                std::string name(exportItem.name());
                TRACE("WASM SE exportItem: %s", name.c_str());
                if (std::holds_alternative<wasmtime::FuncType::Ref>(externType))
                {
                    TRACE("WASM SE Exported function: %s", name.c_str());
                    auto func = std::get<wasmtime::Func>(*instance->get(store, name));
                    wasmFuncs.insert(std::make_pair(name, func));
                    TRACE("WASM SE Exported function: %s", name.c_str());
                    if (name == "cabi_realloc")
                    {
                        realloc = [this, func](int ptr, int old_size, int align, int new_size) -> int
                        {
                            auto retVal = func.call(store, {ptr, old_size, align, new_size}).unwrap();
                            return retVal[0].i32();
                        };
                    }
                }
                else if (std::holds_alternative<wasmtime::MemoryType::Ref>(externType))
                {
                    TRACE("WASM SE Exported memory: %s", name.c_str());
                    auto memory = std::get<wasmtime::Memory>(*instance->get(store, name));
                    wasmMems.insert(std::make_pair(name, memory));
                    TRACE("WASM SE Exported memory: %s", name.c_str());
                }
                else if (std::holds_alternative<wasmtime::TableType::Ref>(externType))
                {
                    TRACE("WASM SE Exported table: %s", name.c_str());
                }
                else if (std::holds_alternative<wasmtime::GlobalType::Ref>(externType))
                {
                    TRACE("WASM SE Exported global: %s", name.c_str());
                }
                else
                {
                    TRACE("WASM SE Unknown export type");
                }
            }
        }
        catch (const wasmtime::Error &e)
        {
            throw makeStringExceptionV(100, "WASM SE createInstance: %s", e.message().c_str());
        }
    }

    wasmtime::Instance getInstance()
    {
        return instance.value();
    }

    wasmtime::Span<uint8_t> getData()
    {
        TRACE("WASM SE getData");
        auto found = wasmMems.find("memory");
        if (found == wasmMems.end())
            throw makeStringException(100, "Wasm memory not found");
        return found->second.data(store.context());
    }

    bool hasFunc(const std::string &name) const
    {
        auto found = wasmFuncs.find(name);
        if (found == wasmFuncs.end())
            return false;
        return true;
    }

    wasmtime::Func getFunc(const std::string &name) const
    {
        TRACE("WASM SE getFunc %s", name.c_str());
        auto found = wasmFuncs.find(name);
        if (found == wasmFuncs.end())
            throw makeStringExceptionV(100, "Wasm function not found: %s", name.c_str());
        return found->second;
    }

    wasmtime::ValType::ListRef getFuncParams(const std::string &name)
    {
        TRACE("WASM SE getFunc %s", name.c_str());
        auto func = getFunc(name);
        wasmtime::FuncType funcType = func.type(store.context());
        return funcType->params();
    }

    wasmtime::ValType::ListRef getFuncResults(const std::string &name)
    {
        TRACE("WASM SE getFuncResults");
        auto func = getFunc(name);
        wasmtime::FuncType funcType = func.type(store.context());
        return funcType->results();
    }

    std::vector<wasmtime::Val> call(const std::string &name, const std::vector<wasmtime::Val> &params)
    {
        TRACE("WASM SE call %s", name.c_str());
        auto func = getFunc(name);
        try
        {
            TRACE("WASM SE call 111 %s", name.c_str());
            auto retVal = func.call(store, params).unwrap();
            TRACE("WASM SE call 222 %s", name.c_str());
            return retVal;
        }
        catch (const wasmtime::Trap &e)
        {
            throw makeStringExceptionV(100, "WASM SE call: %s", e.message().c_str());
        }
    }

    std::unique_ptr<CallContext> createCallContext(Encoding encoding)
    {
        if (instanceContext == nullptr)
        {
            instanceContext = std::make_unique<cmcpp::InstanceContext>(trap, convert, realloc);
        }
        return instanceContext->createCallContext(getData(), encoding);
    }
};

thread_local std::unordered_map<std::string, std::unique_ptr<WasmStore>> wasmStores;
WasmStore *getWasmStore(const std::string &wasmName)
{
    TRACE("WASM SE getWasmStore %s", wasmName.c_str());
    auto found = wasmStores.find(wasmName);
    if (found == wasmStores.end())
    {
        auto result = wasmStores.insert(std::make_pair(wasmName, std::make_unique<WasmStore>(wasmName)));
        return result.first->second.get();
    }
    return found->second.get();
}

class SecureFunction : public CInterfaceOf<IEmbedFunctionContext>
{
    WasmStore *wasmStore = nullptr;
    std::string wasmName;
    std::string funcName;

    std::vector<wasmtime::Val> args;
    std::vector<wasmtime::Val> wasmResults;

public:
    SecureFunction(ICodeContext *codeCtx)
    {
        TRACE("WASM SE se:constructor");
        wasmEngine->setCodeContext(codeCtx);
    }

    virtual ~SecureFunction()
    {
        TRACE("WASM SE se:destructor");

        //  Garbage Collection  ---
        auto gc_func_name = "cabi_post_" + funcName;
        if (wasmStore->hasFunc(gc_func_name))
        {
            for (auto &result : wasmResults)
            {
                TRACE("WASM SE se:destructor %s", gc_func_name.c_str());
                wasmStore->call(gc_func_name, {result});
            }
        }
    }

    //  IEmbedFunctionContext ---
    void setActivityContext(const IThorActivityContext *activityCtx)
    {
    }

    virtual IInterface *bindParamWriter(IInterface *esdl, const char *esdlservice, const char *esdltype, const char *name)
    {
        TRACE("WASM SE paramWriterCommit");
        return NULL;
    }
    virtual void paramWriterCommit(IInterface *writer)
    {
        TRACE("WASM SE paramWriterCommit");
    }
    virtual void writeResult(IInterface *esdl, const char *esdlservice, const char *esdltype, IInterface *writer)
    {
        TRACE("WASM SE writeResult");
    }
    virtual void bindBooleanParam(const char *name, bool val)
    {
        TRACE("WASM SE bindBooleanParam %s %i", name, val);
        args.push_back(val);
    }
    virtual void bindDataParam(const char *name, size32_t len, const void *val)
    {
        TRACE("WASM SE bindDataParam %s %d", name, len);
    }
    virtual void bindFloatParam(const char *name, float val)
    {
        TRACE("WASM SE bindFloatParam %s %f", name, val);
        args.push_back(val);
    }
    virtual void bindRealParam(const char *name, double val)
    {
        TRACE("WASM SE bindRealParam %s %f", name, val);
        args.push_back(val);
    }
    virtual void bindSignedSizeParam(const char *name, int size, __int64 val)
    {
        TRACE("WASM SE bindSignedSizeParam %s %i %lld", name, size, val);
        if (size <= 4)
            args.push_back(static_cast<int32_t>(val));
        else
            args.push_back(static_cast<int64_t>(val));
    }
    virtual void bindSignedParam(const char *name, __int64 val)
    {
        TRACE("WASM SE bindSignedParam %s %lld", name, val);
        args.push_back(static_cast<int64_t>(val));
    }
    virtual void bindUnsignedSizeParam(const char *name, int size, unsigned __int64 val)
    {
        TRACE("WASM SE bindUnsignedSizeParam %s %i %llu", name, size, val);
        if (size <= 4)
            args.push_back(static_cast<int32_t>(val));
        else
            args.push_back(static_cast<int64_t>(val));
    }
    virtual void bindUnsignedParam(const char *name, unsigned __int64 val)
    {
        TRACE("WASM SE bindUnsignedParam %s %llu", name, val);
        args.push_back(static_cast<int64_t>(val));
    }
    virtual void bindStringParam(const char *name, size32_t bytes, const char *val)
    {
        TRACE("WASM SE bindStringParam %s %d %s", name, bytes, val);
        size32_t utfCharCount;
        rtlDataAttr utfText;
        rtlStrToUtf8X(utfCharCount, utfText.refstr(), bytes, val);
        bindUTF8Param(name, utfCharCount, utfText.getstr());
    }
    virtual void bindVStringParam(const char *name, const char *val)
    {
        TRACE("WASM SE bindVStringParam %s %s", name, val);
        bindStringParam(name, strlen(val), val);
    }
    virtual void bindUTF8Param(const char *name, size32_t chars, const char *val)
    {
        TRACE("WASM SE bindUTF8Param %s %d %s", name, chars, val);
        auto cx = mk_cx();
        auto [offset, bytes] = string::store_into_range(*cx, {Encoding::Utf8, (const char8_t *)val, rtlUtf8Size(chars, val)});
        args.push_back((int32_t)offset);
        args.push_back((int32_t)bytes);
    }
    virtual void bindUnicodeParam(const char *name, size32_t chars, const UChar *val)
    {
        TRACE("WASM SE bindUnicodeParam %s %d", name, chars);
        size32_t utfCharCount;
        rtlDataAttr utfText;
        rtlUnicodeToUtf8X(utfCharCount, utfText.refstr(), chars, val);
        bindUTF8Param(name, utfCharCount, utfText.getstr());
    }
    virtual void bindSetParam(const char *name, int elemType, size32_t elemSize, bool isAll, size32_t totalBytes, const void *setData)
    {
        TRACE("WASM SE bindSetParam %s %d %d %d %d %p", name, elemType, elemSize, isAll, totalBytes, setData);
        if (isAll)
            rtlFail(0, "wasmembed: Cannot pass ALL");

        auto cx = mk_cx();
        switch ((type_vals)elemType)
        {
        case type_boolean:
        {
            assert(elemSize == sizeof(bool_t));
            list_t<bool_t> bools;
            const byte *inData = (const byte *)setData;
            const byte *endData = inData + totalBytes;
            while (inData < endData)
            {
                size32_t thisSize = elemSize;
                bools.push_back(*(const bool *)inData ? true : false);
                inData += thisSize;
            }
            auto [offset, size] = list::store_into_range<bool_t>(*cx, bools);
            args.push_back(static_cast<int32_t>(offset));
            args.push_back(static_cast<int32_t>(size));
            break;
        }
        case type_int:
        {
            assert(elemSize == sizeof(int32_t));
            auto [offset, size] = list::store_into_range<int32_t>(*cx, list_t<int32_t>{(const int32_t *)setData, (const int32_t *)setData + (totalBytes / elemSize)});
            args.push_back(static_cast<int32_t>(offset));
            args.push_back(static_cast<int32_t>(size));
            break;
        }
        case type_unsigned:
        {
            assert(elemSize == sizeof(uint32_t));
            auto [offset, size] = list::store_into_range<uint32_t>(*cx, list_t<uint32_t>{(const uint32_t *)setData, (const uint32_t *)setData + (totalBytes / elemSize)});
            args.push_back(static_cast<int32_t>(offset));
            args.push_back(static_cast<int32_t>(size));
            break;
        }
        case type_real:
        {
            if (elemSize == sizeof(double))
            {
                auto [offset, size] = list::store_into_range<float64_t>(*cx, list_t<float64_t>{(const float64_t *)setData, (const float64_t *)setData + (totalBytes / elemSize)});
                args.push_back(static_cast<int32_t>(offset));
                args.push_back(static_cast<int32_t>(size));
            }
            else
            {
                auto [offset, size] = list::store_into_range<float32_t>(*cx, list_t<float32_t>{(const float32_t *)setData, (const float32_t *)setData + (totalBytes / elemSize)});
                args.push_back(static_cast<int32_t>(offset));
                args.push_back(static_cast<int32_t>(size));
            }
            break;
        }
        case type_string:
        {
            list_t<string_t> strings;
            const byte *inData = (const byte *)setData;
            const byte *endData = inData + totalBytes;
            while (inData < endData)
            {
                size32_t thisSize = elemSize;
                if (elemSize == UNKNOWN_LENGTH)
                {
                    thisSize = *(size32_t *)inData;
                    inData += sizeof(size32_t);
                }
                strings.push_back({Encoding::Utf8, (const char8_t *)inData, thisSize});
                inData += thisSize;
            }
            auto [offset, size] = list::store_into_range<string_t>(*cx, strings);
            args.push_back(static_cast<int32_t>(offset));
            args.push_back(static_cast<int32_t>(size));
            break;
        }
        default:
            throw makeStringException(200, "bindSetParam not implemented");
        }
    }

    virtual void bindRowParam(const char *name, IOutputMetaData &metaVal, const byte *val) override
    {
        TRACE("WASM SE bindRowParam %s %p", name, val);
        throw makeStringException(200, "bindRowParam not implemented");
    }
    virtual void bindDatasetParam(const char *name, IOutputMetaData &metaVal, IRowStream *val)
    {
        TRACE("WASM SE bindDatasetParam %s %p", name, val);
        throw makeStringException(200, "bindDatasetParam not implemented");
    }
    virtual bool getBooleanResult()
    {
        TRACE("WASM SE getBooleanResult");
        return wasmResults[0].i32();
    }
    virtual void getDataResult(size32_t &len, void *&result)
    {
        TRACE("WASM SE getDataResult");
        throw makeStringException(200, "getDataResult not implemented");
    }
    virtual double getRealResult()
    {
        TRACE("WASM SE getRealResult");
        if (wasmResults[0].kind() == wasmtime::ValKind::F64)
            return wasmResults[0].f64();
        return wasmResults[0].f32();
    }
    virtual __int64 getSignedResult()
    {
        TRACE("WASM SE getSignedResult1 %i", (uint8_t)wasmResults[0].kind());
        if (wasmResults[0].kind() == wasmtime::ValKind::I64)
        {
            return wasmResults[0].i64();
        }
        return static_cast<__int64>(wasmResults[0].i32());
    }
    virtual unsigned __int64 getUnsignedResult()
    {
        TRACE("WASM SE getUnsignedResult");
        if (wasmResults[0].kind() == wasmtime::ValKind::I64)
            return wasmResults[0].i64();
        return static_cast<unsigned __int64>(wasmResults[0].i32());
    }

    std::unique_ptr<cmcpp::CallContext> mk_cx()
    {
        TRACE("WASM SE mk_cx");
        return wasmStore->createCallContext(Encoding::Utf8);
    }

    virtual void getStringResult(size32_t &chars, char *&result)
    {
        TRACE("WASM SE getStringResult %zu", wasmResults.size());
        auto ptr = wasmResults[0].i32();
        auto cx = mk_cx();
        auto [encoding, strPtr, bytes] = string::load(*cx, ptr);
        size32_t codePoints = rtlUtf8Length(bytes, strPtr);
        rtlUtf8ToStrX(chars, result, codePoints, (const char *)strPtr);
    }
    virtual void getUTF8Result(size32_t &chars, char *&result)
    {
        TRACE("WASM SE getUTF8Result");
        auto ptr = wasmResults[0].i32();
        auto cx = mk_cx();
        auto [encoding, strPtr, bytes] = string::load(*cx, ptr);
        chars = rtlUtf8Length(bytes, strPtr);
        result = (char *)rtlMalloc(bytes);
        memcpy(result, strPtr, bytes);
    }
    virtual void getUnicodeResult(size32_t &chars, UChar *&result)
    {
        TRACE("WASM SE getUnicodeResult");
        auto ptr = wasmResults[0].i32();
        auto cx = mk_cx();
        auto [encoding, strPtr, bytes] = string::load(*cx, ptr);
        size32_t codePoints = rtlUtf8Length(bytes, strPtr);
        rtlUtf8ToUnicodeX(chars, result, codePoints, (const char *)strPtr);
    }
    virtual void getSetResult(bool &isAllResult, size32_t &resultBytes, void *&result, int elemType, size32_t elemSize)
    {
        TRACE("WASM SE getSetResult %d %d %zu", elemType, elemSize, wasmResults.size());
        isAllResult = false;
        auto ptr = wasmResults[0].i32();
        auto cx = mk_cx();
        switch (elemType)
        {
        case type_boolean:
        {
            auto list = list::load<bool_t>(*cx, ptr);
            resultBytes = list.size();
            result = rtlMalloc(resultBytes);
            std::copy(list.begin(), list.end(), reinterpret_cast<bool *>(result));
            break;
        }
        case type_real:
        {
            if (elemSize == sizeof(float64_t))
            {
                auto list = list::load<float64_t>(*cx, ptr);
                resultBytes = list.size() * sizeof(float64_t);
                result = rtlMalloc(resultBytes);
                std::copy(list.begin(), list.end(), reinterpret_cast<float64_t *>(result));
            }
            else
            {
                assert(elemSize == sizeof(float32_t));
                auto list = list::load<float32_t>(*cx, ptr);
                resultBytes = list.size() * sizeof(float32_t);
                result = rtlMalloc(resultBytes);
                std::copy(list.begin(), list.end(), reinterpret_cast<float32_t *>(result));
            }
            break;
        }
        case type_unsigned:
        {
            auto list = cmcpp::list::load<uint32_t>(*cx, ptr);
            resultBytes = list.size() * sizeof(uint32_t);
            result = rtlMalloc(resultBytes);
            memcpy(result, list.data(), resultBytes);
            break;
        }
        case type_string:
        {
            auto list = cmcpp::list::load<string_t>(*cx, ptr);
            rtlRowBuilder out;
            size32_t outBytes = 0;
            byte *outData = NULL;
            for (auto &item : list)
            {
                out.ensureAvailable(outBytes + item.byte_len + sizeof(size32_t));
                outData = out.getbytes() + outBytes;
                *reinterpret_cast<size32_t *>(outData) = item.byte_len;
                rtlStrToStr(item.byte_len, outData + sizeof(size32_t), item.byte_len, item.ptr);
                outBytes += item.byte_len + sizeof(size32_t);
            }
            resultBytes = outBytes;
            result = out.detachdata();
            break;
        }
        default:
            rtlFail(0, "wasmembed: Unsupported parameter type");
            break;
        }
    }
    virtual IRowStream *getDatasetResult(IEngineRowAllocator *_resultAllocator)
    {
        TRACE("WASM SE getDatasetResult");
        throw makeStringException(200, "getDatasetResult not implemented");
        return NULL;
    }
    virtual byte *getRowResult(IEngineRowAllocator *_resultAllocator)
    {
        TRACE("WASM SE getRowResult");
        throw makeStringException(200, "getRowResult not implemented");
        return NULL;
    }
    virtual size32_t getTransformResult(ARowBuilder &builder)
    {
        TRACE("WASM SE getTransformResult");
        throw makeStringException(200, "getTransformResult not implemented");
        return 0;
    }
    virtual void loadCompiledScript(size32_t chars, const void *_script) override
    {
        TRACE("WASM SE loadCompiledScript %p", _script);
        throw makeStringException(200, "loadCompiledScript not implemented");
    }
    virtual void enter() override
    {
        TRACE("WASM SE enter");
    }
    virtual void reenter(ICodeContext *codeCtx) override
    {
        TRACE("WASM SE reenter");
    }
    virtual void exit() override
    {
        TRACE("WASM SE exit");
    }
    virtual void compileEmbeddedScript(size32_t lenChars, const char *_utf) override
    {
        TRACE("WASM SE compileEmbeddedScript");
        throw makeStringException(200, "compileEmbeddedScript not supported");
    }
    virtual void importFunction(size32_t lenChars, const char *qualifiedName) override
    {
        TRACE("WASM SE importFunction: %s", qualifiedName);
        std::tie(wasmName, funcName) = splitQualifiedID({qualifiedName, lenChars});
        wasmStore = getWasmStore(wasmName);
    }
    virtual void callFunction()
    {
        TRACE("WASM SE callFunction %s.%s", wasmName.c_str(), funcName.c_str());
        wasmResults = wasmStore->call(funcName, args);
    }
};

IEmbedFunctionContext *createISecureEnclave(ICodeContext *codeCtx)
{
    TRACE("createISecureEnclave");
    return new SecureFunction(codeCtx);
}
