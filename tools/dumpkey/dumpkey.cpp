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

#include "jliball.hpp"
#include "jhtree.hpp"
#include "ctfile.hpp"

void fatal(const char *format, ...) __attribute__((format(printf, 1, 2)));
void fatal(const char *format, ...)
{
    va_list      args;

    va_start(args, format);
    vfprintf(stderr, format, args);
    va_end(args);
    fflush(stderr);
    releaseAtoms();
    ExitModuleObjects();
    _exit(2);
}

bool optHex = false;
bool optRaw = false;

void usage()
{
    fprintf(stderr, "Usage: dumpkey dataset [options]\n"
        "Options:\n"
        "  node=[n]            - dump node n (0 = just header)\n"
        "  fpos=[n]            - dump node at offset fpos\n"
        "  recs=[n]            - dump n rows\n"
        "  -H                  - hex display\n"
        "  -R                  - raw output\n"
                    );
    fflush(stderr);
    releaseAtoms();
    ExitModuleObjects();
    _exit(2);
}

void doOption(const char *opt)
{
    switch (toupper(opt[1]))
    {
    case 'H':
        optHex = true;
        break;
    case 'R':
        optRaw = true;
        break;
    default:
        usage();
    }
}

int main(int argc, const char **argv)
{
    InitModuleObjects();
#ifdef _WIN32
    _setmode( _fileno( stdout ), _O_BINARY );
    _setmode( _fileno( stdin ), _O_BINARY );
#endif
    Owned<IProperties> globals = createProperties("dumpkey.ini", true);
    for (int i = 1; i < argc; i++)
    {
        if (argv[i][0] == '-')
            doOption(argv[i]);
        else if (strchr(argv[i], '='))
            globals->loadProp(argv[i]);
        else
            globals->setProp("keyfile", argv[i]);
    }
    try
    {
        StringBuffer logname("dumpkey.");
        logname.append(GetCachedHostName()).append(".");
        StringBuffer lf;
        openLogFile(lf, logname.append("log").str());
        
        Owned <IKeyIndex> index;
        const char * keyName = globals->queryProp("keyfile");
        if (keyName)
            index.setown(createKeyIndex(keyName, 0, false, false));
        else
            usage();
        Owned <IKeyCursor> cursor = index->getCursor(NULL);

        size32_t key_size = index->keySize();
        Owned<IFile> in = createIFile(keyName);
        Owned<IFileIO> io = in->open(IFOread);
        if (!io)
            throw MakeStringException(999, "Failed to open file %s", keyName);
        Owned<CKeyHdr> header = new CKeyHdr;
        MemoryAttr block(sizeof(KeyHdr));
        io->read(0, sizeof(KeyHdr), (void *)block.get());
        header->load(*(KeyHdr*)block.get());
        unsigned nodeSize = header->getNodeSize();

        if (!optRaw)
        {
            printf("Key '%s'\nkeySize=%d NumParts=%x, Top=%d\n", keyName, key_size, index->numParts(), index->isTopLevelKey());
            printf("File size = %"I64F"d, nodes = %"I64F"d\n", in->size(), in->size() / nodeSize - 1);
            printf("rootoffset=%"I64F"d[%"I64F"d]\n", header->getRootFPos(), header->getRootFPos()/nodeSize);
        }
        char *buffer = (char*)alloca(key_size);

        if (globals->hasProp("node"))
        {
            if (stricmp(globals->queryProp("node"), "all")==0)
            {
            }
            else
            {
                int node = globals->getPropInt("node");
                if (node != 0)
                    index->dumpNode(stdout, node * nodeSize, globals->getPropInt("recs", 0), optRaw);
            }
        }
        else if (globals->hasProp("fpos"))
        {
            index->dumpNode(stdout, globals->getPropInt("fpos"), globals->getPropInt("recs", 0), optRaw);
        }
        else
        {
            bool backwards=false;
            bool ok;
            if (globals->hasProp("end"))
            {
                memset(buffer, 0, key_size);
                strcpy(buffer, globals->queryProp("end"));
                ok = cursor->ltEqual(buffer, buffer);
                backwards = true;
            }
            else if (globals->hasProp("start"))
            {
                memset(buffer, 0, key_size);
                strcpy(buffer, globals->queryProp("start"));
                ok = cursor->gtEqual(buffer, buffer);
            }
            else
                ok = cursor->first(buffer);
            
            unsigned count = globals->getPropInt("recs", 1);
            while (ok && count--)
            {
                offset_t pos = cursor->getFPos();
                unsigned __int64 seq = cursor->getSequence();
                size32_t size = cursor->getSize();
                if (optRaw)
                {
                    fwrite(buffer, 1, size, stdout);
                }
                else if (optHex)
                {
                    for (unsigned i = 0; i < size; i++)
                        printf("%02x", ((unsigned char) buffer[i]) & 0xff);
                    printf("  :%"I64F"u:%012"I64F"x\n", seq, pos);
                }
                else
                    printf("%.*s  :%"I64F"u:%012"I64F"x\n", size, buffer, seq, pos);
                if (backwards)
                    ok = cursor->prev(buffer);
                else
                    ok = cursor->next(buffer);
            }
        }
    }
    catch (IException *E)
    {
        StringBuffer msg;
        E->errorMessage(msg);
        E->Release();
        fatal("%s", msg.str());
    }
    releaseAtoms();
    ExitModuleObjects();
    return 0;
}


