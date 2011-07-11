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

#include "jliball.hpp"
#include "roxieclient.hpp"

int main(int argc, char *argv[])
{
    InitModuleObjects();

    Owned<ISmartSocketFactory> smartSocketFactory;
    int in_width = 0;
    int out_width = 0;
    int recordsPerQuery = 0;
    StringBuffer query;
    StringBuffer hosts;
    StringBuffer resultName;
    attachStandardFileLogMsgMonitor("C:\\roxieclienttest.log", NULL, MSGFIELD_STANDARD, MSGAUD_all, MSGCLS_all, TopDetail, false);
    unsigned numThreads = 1;
    EnableSEHtoExceptionMapping();
    StringBuffer inputfile, outputfile;
    unsigned maxRetries = 0;
    int readTimeout = 300;

    int i;

    for (i=1; i<argc; i++)
    {
        if (stricmp(argv[i], "-iw") == 0)
        {
            i++;
            if (i < argc)
                in_width = atoi(argv[i]);
        }
        else if (stricmp(argv[i], "-ow") == 0)
        {
            i++;
            if (i < argc)
                out_width = atoi(argv[i]);
        }
        else if (stricmp(argv[i], "-q") == 0)
        {
            i++;
            if (i < argc)
                query.append(argv[i]);
        }
        else if (stricmp(argv[i], "-h") == 0)
        {
            i++;
            if (i < argc)
                hosts.append(argv[i]);
        }
        else if (stricmp(argv[i], "-r") == 0)
        {
            i++;
            if (i < argc)
                resultName.append(argv[i]);
        }
        else if (stricmp(argv[i], "-i") == 0)
        {
            i++;
            if (i < argc)
                inputfile.append(argv[i]);
        }
        else if (stricmp(argv[i], "-o") == 0)
        {
            i++;
            if (i < argc)
                outputfile.append(argv[i]);
        }
        else if (stricmp(argv[i], "-n") == 0)
        {
            i++;
        }
        else if (stricmp(argv[i], "-t") == 0)
        {
            i++;
            if (i < argc)
                numThreads = atoi(argv[i]);
        }
        else if (stricmp(argv[i], "-b") == 0)
        {
            i++;
            if (i < argc)
                recordsPerQuery = atoi(argv[i]);
        }
        else if (stricmp(argv[i], "-mr") == 0)
        {
            i++;
            if (i <argc)
                maxRetries = atoi(argv[i]);
        }
        else if (stricmp(argv[i], "-to") == 0)
        {
            i++;
            if (i <argc)
                readTimeout = atoi(argv[i]);
        }
#if 0
        else if (stricmp(argv[i], "-d") == 0)
        {
            DebugBreak();
        }
#endif
    }

    if (query.length() == 0 || hosts.length() == 0)
    {
        DBGLOG("Missing required parameter (-q & -h are all required)");
        exit(0);
    }
    
    numThreads = (numThreads < 1)? 1: ((numThreads > MAX_ROXIECLIENT_THREADS)? MAX_ROXIECLIENT_THREADS: numThreads);
    recordsPerQuery = (recordsPerQuery < 1)? DEFAULT_RECORDSPERQUERY : recordsPerQuery;


    smartSocketFactory.setown(createSmartSocketFactory(hosts.str()));
    Owned<IRoxieClient> roxieclient = createRoxieClient(smartSocketFactory, numThreads);
    
    // File input/output stream example
    int fh1 = -1;
    Owned<IByteInputStream> istream = NULL;
    if(inputfile.length() > 0)
    {
        fh1 = open( inputfile.str(), _O_RDONLY );
        if( fh1 == -1 )
        {
            perror( "Open failed on input file" );
            exit(-1);
        }
        else
        {
            printf( "Open succeeded on input file\n" );
        }
#ifdef _WIN32
        _setmode(fh1, _O_BINARY);
#endif
        Owned<IByteInputStream> istream = createInputStream(fh1);
        roxieclient->setInput("", in_width, istream);

    }

    int fh2;
    fh2 = open(outputfile.str(), _O_WRONLY | _O_CREAT | _O_TRUNC, _S_IREAD | _S_IWRITE );
    if( fh2 == -1 )
    {
        perror( "Open failed on output file" );
        exit(-1);
    }
    else
    {
        printf( "Open succeeded on output file\n" );
    }

#ifdef _WIN32
    _setmode(fh2, _O_BINARY);
#endif

    Owned<IByteOutputStream> ostream = createOutputStream(fh2);
    roxieclient->setRecordsPerQuery(recordsPerQuery);
    roxieclient->setOutput("", out_width, ostream);
    roxieclient->setMaxRetries(maxRetries);
    roxieclient->setReadTimeout(readTimeout);
    roxieclient->runQuery(query.str());

    if(smartSocketFactory.get())
        smartSocketFactory->stop();

    if(fh1 != -1)   
        _close( fh1 );
    _close( fh2 );

    releaseAtoms();

    return 0;
}
