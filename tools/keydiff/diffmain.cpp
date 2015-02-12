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

#include <stdio.h>
#include "jlog.hpp"
#include "keydiff.hpp"

void usage(bool isHelp)
{
    printf("usage:\n"
           "  keydiff [options] oldindex newindex patch\n"
           "options are:\n"
           "   -o --overwrite              overwrite patch file if it exists\n"
           "   -n --nooverwrite            abort if patch file exists (default)\n"
           "   -c --compress               use LZMA block compression\n"
           "   -z                          use LZW block compression (faster/larger)\n"
           "   -s --stats                  log statistics on diff\n"
           "   -t --tlk newtlk             include data on newtlk in header\n"
           "   -p --progress freq          log progress reading old index at intervals of freq bytes\n"
           "\n"
           "usage:\n"
           "  keydiff [-v | --version ]\n"
           "  keydiff [-h | -? | --help ]\n\n");
    releaseAtoms();
    exit(isHelp ? 0 : 2);
}

class KeyDiffProgressCallback : public CInterface, public IKeyDiffProgressCallback
{
public:
    IMPLEMENT_IINTERFACE;
    virtual void handle(offset_t bytesRead) { PROGLOG("Read %" I64F "d bytes from old index", bytesRead); }
};

void version(bool isHelp)
{
    StringBuffer buff("This is keydiff version ");
    getKeyDiffVersion(buff).append(".\nPatches it generates can be applied by keypatch since version ");
    getKeyDiffMinPatchVersionForDiff(buff).append(".\n");
    buff.append("To discover whether patches it generates can be applied by versions\n"
                "of keypatch later than itself, see keypatch -v.\n\n");
    printf("%s", buff.str());
    if(isHelp)
        usage(true);
    releaseAtoms();
    exit(0);
}

class KeyDiffParams
{
public:
    KeyDiffParams() : overwrite(false), stats(false), tlkInfo(false), compress(0), progressFrequency(0) {}
    StringBuffer oldIndex;
    StringBuffer newIndex;
    StringBuffer patch;
    bool overwrite;
    bool stats;
    bool tlkInfo;
    StringBuffer newTLK;
    unsigned compress;
    offset_t progressFrequency;
};

void getParams(unsigned argc, char * const * argv, KeyDiffParams & params)
{
    unsigned arg = 1;
    while((arg<argc) && (*argv[arg] == '-'))
    {
        if((strcmp(argv[arg], "-o") == 0) || (strcmp(argv[arg], "--overwrite") == 0))
            params.overwrite = true;
        else if((strcmp(argv[arg], "-n") == 0) || (strcmp(argv[arg], "--nooverwrite") == 0))
            params.overwrite = false;
        else if(strcmp(argv[arg], "-z") == 0)
            params.compress = COMPRESS_METHOD_LZW;
        else if((strcmp(argv[arg], "-c") == 0) || (strcmp(argv[arg], "--compress") == 0))
            params.compress = COMPRESS_METHOD_LZMA;
        else if((strcmp(argv[arg], "-s") == 0) || (strcmp(argv[arg], "--stats") == 0))
            params.stats = true;
        else if((strcmp(argv[arg], "-t") == 0) || (strcmp(argv[arg], "--tlk") == 0))
        {
            if((argc-arg)<=1) usage(false);
            params.tlkInfo = true;
            params.newTLK.append(argv[++arg]);
        }
        else if((strcmp(argv[arg], "-p") == 0) || (strcmp(argv[arg], "--progress") == 0))
        {
            if((argc-arg)<=1) usage(false);
            ++arg;
            offset_t freq = atoi64_l(argv[arg], strlen(argv[arg]));
            if(freq <= 0) usage(false);
            params.progressFrequency = freq;
        }
        else if((strcmp(argv[arg], "-v") == 0) || (strcmp(argv[arg], "--version") == 0))
            version(false);
        else if((strcmp(argv[arg], "-h") == 0) || (strcmp(argv[arg], "-?") == 0) || (strcmp(argv[arg], "--help") == 0))
            version(true);
        else
            usage(false);
        arg++;
    }
    if(argc != arg+3)
        usage(false);
    params.oldIndex.append(argv[arg++]);
    params.newIndex.append(argv[arg++]);
    params.patch.append(argv[arg++]);
}

int main(int argc, char * const * argv)
{
    InitModuleObjects();
    try
    {
        KeyDiffParams params;
        getParams(argc, argv, params);
        Owned<IKeyDiffGenerator> generator(createKeyDiffGenerator(params.oldIndex.str(), params.newIndex.str(), params.patch.str(), (params.tlkInfo ? params.newTLK.str() : 0), params.overwrite, params.compress));
        if(params.progressFrequency)
            generator->setProgressCallback(new KeyDiffProgressCallback, params.progressFrequency);
        generator->run();
        if(params.stats)
            generator->logStats();
    }
    catch(IException * e)
    {
        EXCLOG(e);
        e->Release();
        releaseAtoms();
        return 1;
    }
    releaseAtoms();
    return 0;
}
