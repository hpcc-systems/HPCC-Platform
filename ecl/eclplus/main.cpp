/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */
#include "jlib.hpp"
#include "jmisc.hpp"
#include "jprop.hpp"
#include "jsocket.hpp"
#include "workunit.hpp"
#include "mpbase.hpp"
#include "daclient.hpp"
#include "dalienv.hpp"
#include "ws_workunits.hpp"

#include "eclplus.hpp"
#ifdef __linux__
#include "termios.h"
#endif

#define ERROR_CODE_SUCCESS                0
#define ERROR_CODE_FAILURE                1
#define ERROR_CODE_FILE_CANNOT_BE_WRITTEN 2
#define ERROR_NO_SERVER                   3
#define ERROR_CODE_USAGE                  4

//copied from workunit.dll to avoid a dependency.  Should possibly go in jlib.
static bool localLooksLikeAWuid(const char * wuid)
{
    if (!wuid)
        return false;
    if (wuid[0] != 'W')
        return false;
    if (!isdigit(wuid[1]) || !isdigit(wuid[2]) || !isdigit(wuid[3]) || !isdigit(wuid[4]))
        return false;
    if (!isdigit(wuid[5]) || !isdigit(wuid[6]) || !isdigit(wuid[7]) || !isdigit(wuid[8]))
        return false;
    return (wuid[9]=='-');
}

void usage()
{
    printf("eclplus action=[list|view|dump|delete|abort|query|graph]\n");
    printf("\t{owner=<userid>| wuid=<wuid>}\n");
    printf("\tpassword=<password>\n");
    printf("\tcluster=<cluster>\n");
    printf("\tserver=<server | server:port | http://server:port | https://server:port>\n");
    printf("\ttimeout=<query_timeout_in_seconds_or_0_for_asynchronous>\n");
    printf("\t{ecl=<ecl_query> | @inputfile }\n");
    printf("\t{file=logicalName | !logicalName[startrow,endrow]}\n");
    printf("\tformat=[default|xml|csv|csvh|runecl|bin(ary)]\n");
    printf("\toutput=<outputfile>\n");
    printf("\tjobname=<name>\n");
    printf("\tpagesize=<pagesize>\n");
    printf("\t{-fdebugparam1=debugvalue1 -fdebugparam2=debugvalue2 ... }\n");
    printf("\t{-Ipath -Lpath -g -E as eclcc}\n");
    printf("\t{_applicationparam1=applicationvalue1 _applicationparam2=applicationvalue2 ... }\n");
    printf("\t{/storedname1=simplevalue1 /storedname2=simplevalue2 ... }\n");
    printf("\t{stored=<storedname>...</storedname>} stored=<storedname2>...</storedname2>} ...\n");
    printf("\t{query=full-query-xml | query=@filename}\n");
}

void promptFor(const char *prompt, const char *prop, bool hide, IProperties * globals)
{
    StringBuffer result;
    fprintf(stdout, "%s", prompt);
    fflush(stdout);
    if (hide)
    {
#ifdef _WIN32
        HANDLE hStdIn = GetStdHandle(STD_INPUT_HANDLE);   
        DWORD dwInputMode;
        GetConsoleMode(hStdIn, &dwInputMode);   
        SetConsoleMode(hStdIn, dwInputMode & ~ENABLE_LINE_INPUT & ~ENABLE_ECHO_INPUT);
        loop
        {
            /* read a character from the console input */   
            char ch;
            DWORD dwRead;
            if (!ReadFile(hStdIn, &ch, sizeof(ch), &dwRead, NULL))
                break;
            if (ch == '\n' || ch=='\r' || !ch)
                break;
            result.append(ch);
        }
        SetConsoleMode(hStdIn, dwInputMode); 
#else
        int fn = fileno(stdin);
#if defined (__linux__)
        struct termio t;
        /* If ioctl fails, we're probably not connected to a terminal. */
        if(!ioctl(fn, TCGETA, &t))
        {
            t.c_lflag &= ~ECHO;
            ioctl(fn, TCSETA, &t);
        }
#endif
        loop
        {
            char ch = fgetc(stdin);
            if (ch == '\n' || ch=='\r' || !ch)
                break;
            result.append(ch);
        }
#if defined (__linux__)
        if(!ioctl(fn, TCGETA, &t))
        {
            t.c_lflag |= ECHO;
            ioctl(fn, TCSETA, &t);
        }
#endif
#endif
        printf("\n");
    }
    else
    {
        char buf[100];
        if (fgets(buf, 100, stdin))
            result.append(buf);
        if (result.length() && result.charAt(result.length()-1)=='\n')
            result.remove(result.length()-1, 1);
    }
    globals->setProp(prop, result);
}

bool build_globals(int argc, const char *argv[], IProperties * globals)
{
    const char * inputName = NULL;
    StringBuffer ecl;
    unsigned eclccseq = 0;
    for (int i = 1; i < argc; i++)
    {
        const char * arg = argv[i];
        const char * eq = strchr(arg, '=');
        if (arg[0] == '-')
        {
            StringBuffer name;
            const char * value = NULL;

            if (arg[1] == 'f')
            {
                if (eq)
                {
                    name.append(eq - arg, arg);
                    globals->setProp(name, eq+1);
                }
                else
                {
                    globals->setProp(arg, "1");
                }
            }
            else if (arg[1] == 'I')
            {
                name.append("-feclcc-includeLibraryPath-").append(++eclccseq);
                globals->setProp(name, arg+2);
            }
            else if (arg[1] == 'L')
            {
                name.append("-feclcc-libraryPath-").append(++eclccseq);
                globals->setProp(name, arg+2);
            }
            else if (strcmp(arg, "-g") == 0)
            {
                globals->setProp("-fdebugQuery", "1");
            }
            else if (strcmp(arg, "-legacy") == 0)
            {
                globals->setProp("-feclcc-legacy", "1");
            }
            else if (strcmp(arg, "-save-temps") == 0)
            {
                globals->setProp("-fsaveEclTempFiles", "1");
            }
            else if (strcmp(arg, "-E") == 0)
            {
                globals->setProp("-farchiveToCpp", "1");
            }
            else
            {
                printf("Unrecognised option %s\n", arg);
                return false;
            }
        }
        else if (eq)
        {
            globals->loadProp(arg);
        }
        else if (localLooksLikeAWuid(arg))
        {
            globals->setProp("WUID", arg);
        }
        else if (strchr(arg, '?'))
        {
            usage();
            return false;
        }
        else if (arg[0] == '@')
        {
            inputName = arg[1] ? &arg[1] : "stdin:";
        }
        else if ((arg[0] == '!') && strlen(arg) > 1)
        {
            const char * name = arg+1;
            const char * open = strchr(name, '[');
            if (open)
            {
                const char * comma = strchr(name,',');
                const char * close = strchr(name,']');
                StringAttr temp(name, open-name);
                globals->setProp("file", temp);

                if (comma && close && comma > open && close > comma)
                {
                    temp.set(open+1, comma-(open+1));
                    if (temp.length())
                        globals->setProp("rangelow", atol(temp));
                    temp.set(comma+1, close-(comma+1));
                    if (temp.length())
                        globals->setProp("rangehigh", atol(temp));
                }
                else if (close && close > open)
                {
                    temp.set(open+1, close-(open+1));
                    globals->setProp("rangelow", atol(temp));
                    globals->setProp("rangehigh", atol(temp));
                }
                else
                {
                    printf("Invalid range specified on file");
                    return false;
                }
            }
            else
                globals->setProp("file", name);
        }
        else
        {
            globals->setProp("ACTION", arg);
        }
    }

    const char *insertXML = globals->queryProp("xml");
    if (insertXML)
        ecl.appendf("loadxml('%s');\n", insertXML);

    if (inputName)
    {
        try
        {
            Owned<IFile> input = createIFile(inputName);
            ecl.loadFile(input);
            globals->setProp("ecl", ecl.str());
            if (!globals->hasProp("jobname") && (stdIoHandle(inputName) == -1))
                globals->setProp("jobname", inputName);
        }
        catch(IException * e)
        {
            StringBuffer m;
            e->errorMessage(m);
            e->Release();
            printf("%s", m.str());
            return false;
        }
    }

    // If the user sent some ecl, but didn't tell me what to do, then I think he meant
    // 'execute this ecl'
    //
    // This assumption may have to go when more ecl related options are added.
    if (!globals->hasProp("action"))
    {
        if (globals->hasProp("ecl") || globals->hasProp("main"))
            globals->setProp("action", "query");
        else if (globals->hasProp("file"))
            globals->setProp("action", "view");
        else if (globals->hasProp("service"))
        {
            globals->setProp("action", "query");
            const char * source = globals->queryProp("service");
            if (localLooksLikeAWuid(source))
                globals->setProp("-fsourceWuid", source);
            else
                globals->setProp("-fsourceService", source);
        }
    }
    return true;
}

int main(int argc, const char *argv[])
{
    InitModuleObjects();
    queryLogMsgManager()->changeMonitorFilterOwn(queryStderrLogMsgHandler(), getPassNoneLogMsgFilter());

    if (argc==1)
    {
        usage();
        releaseAtoms();
        return 0;
    }

    Owned<IProperties> globals = createProperties("eclplus.ini", true);

    // Read the command line
    if(!build_globals(argc, argv, globals))
    {
        releaseAtoms();
        return ERROR_CODE_USAGE;
    }

    // Open the output file
    FILE *fp = stdout;
    if(globals->hasProp("output"))
    {
        bool isBinary = true;
        Owned<IFormatType> format = createFormatter(globals);
        if (format)
            isBinary = format->isBinary();
        if ((fp = fopen(globals->queryProp("output"), isBinary ? "wb" : "wt")) == NULL)
        {
            StringBuffer m;
            m.append("Error: couldn't open ").append(globals->queryProp("output")).append(" for writing").newline();
            printf("%s", m.str());
            releaseAtoms();
            return ERROR_CODE_FILE_CANNOT_BE_WRITTEN;
        }
    }

    // Start up SDS communications
    if (!globals->hasProp("SERVER"))
    {
        printf("Error: No server specified\n\n");
        usage();
        releaseAtoms();
        return ERROR_NO_SERVER;
    }

    bool ok = false;
    try
    {
        Owned<IEclPlusHelper> eclPlus = createEclPlusHelper(globals);

        try
        {
            // Let's go to work !
            if(eclPlus)
                ok = eclPlus->doit(fp);
            else
                usage();
        }
        catch (IException * e)
        {
            StringBuffer x;
            e->errorMessage(x);
            printf("Error: %s\n", x.str());
            EXCLOG(e);
            e->Release();
        }
        catch(...)
        {
            printf("Error: unknown exception\n");
        }

        // Clean up
        fclose(fp);
    }
    catch (IException * e)
    {
        StringBuffer x;
        e->errorMessage(x);
        e->Release();
        printf("Error: %s\n", x.str());
    }
    releaseAtoms();

    return ok ? ERROR_CODE_SUCCESS : ERROR_CODE_FAILURE;
}

