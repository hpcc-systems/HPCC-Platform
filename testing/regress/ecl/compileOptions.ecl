#OPTION('compileOptions', '-std=c++17');

STRING cppVersion() := EMBED(C++)
    #include <string>

#body
    std::string versionTag;
    #if __cplusplus >= 202302L
        versionTag = "C++23";
    #elif __cplusplus >= 202002L
        versionTag = "C++20";
    #elif __cplusplus >= 201703L
        versionTag = "C++17";
    #elif __cplusplus >= 201402L
        versionTag = "C++14";
    #elif __cplusplus >= 201103L
        versionTag = "C++11";
    #elif __cplusplus >= 199711L
        versionTag = "C++03";
    #else
        versionTag = "Unknown C++ version";
    #endif
    const size32_t len = static_cast<size32_t>(versionTag.size());
    char * out = (char*)rtlMalloc(len);
    memcpy(out, versionTag.data(), len);
    __lenResult = len;
    __result = out;
ENDEMBED;

ver := cppVersion();
OUTPUT(ver);
