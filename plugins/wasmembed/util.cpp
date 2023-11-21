#include "util.hpp"

#include "platform.h"
#include "jexcept.hpp"
#include "jfile.hpp"

std::vector<uint8_t> readWasmBinaryToBuffer(const char *filename)
{
    Owned<IFile> file = createIFile(filename);
    Owned<IFileIO> fileIO = file->open(IFOread);
    if (!fileIO)
        throw makeStringExceptionV(0, "Failed to open %s", filename);

    MemoryBuffer mb;
    size32_t count = read(fileIO, 0, (size32_t)-1, mb);
    uint8_t *ptr = (uint8_t *)mb.detach();
    return std::vector<uint8_t>(ptr, ptr + count);
}

std::string extractContentInDoubleQuotes(const std::string &input)
{

    std::size_t firstQuote = input.find_first_of('"');
    if (firstQuote == std::string::npos)
        return "";

    std::size_t secondQuote = input.find('"', firstQuote + 1);
    if (secondQuote == std::string::npos)
        return "";

    return input.substr(firstQuote + 1, secondQuote - firstQuote - 1);
}

std::pair<std::string, std::string> splitQualifiedID(const std::string &qualifiedName)
{
    std::size_t firstDot = qualifiedName.find_first_of('.');
    if (firstDot == std::string::npos || firstDot == 0 || firstDot == qualifiedName.size() - 1)
        throw makeStringExceptionV(3, "Invalid import function '%s', expected format: <module>.<function>", qualifiedName.c_str());

    return std::make_pair(qualifiedName.substr(0, firstDot), qualifiedName.substr(firstDot + 1));
}

std::string createQualifiedID(const std::string &wasmName, const std::string &funcName)
{
    return wasmName + "." + funcName;
}