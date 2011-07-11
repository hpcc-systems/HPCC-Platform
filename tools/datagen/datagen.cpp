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

#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <string.h>
#include <io.h>
#include <fcntl.h>

#ifndef _WIN32
#include <unistd.h>
#else
#include <windows.h>
#endif


#if 0
// Marsaglia's MWC ... just in case we end up needing it ...

#define znew   (z=36969*(z&65535)+(z>>16))
#define wnew   (w=18000*(w&65535)+(w>>16))
#define MWC    ((znew<<16)+wnew )

typedef unsigned long UL;

/*  Global static variables: seed these!!! */
static UL z=362436069, w=521288629;
#endif


class CRandom
{
    // from Knuth if I remember correctly
#define HISTORYSIZE 55
#define HISTORYMAX (HISTORYSIZE-1)

    unsigned history[HISTORYSIZE];
    unsigned ptr;
    unsigned lower;
    static unsigned seedinc;

public:
    CRandom() 
    { 
        time_t t;
        time(&t);
        t ^= (clock()<<16);
        seed(t+seedinc++); 
    }
    
    void seed(unsigned su) 
    {
        ptr = HISTORYMAX;
        lower = 23;


        double s = 91648253+su;
        double a = 1389796;
        double m = 2147483647;  
        unsigned i;
        for (i=0;i<HISTORYSIZE;i++) { // just used for initialization
            s *= a;
            int q = (int)(s/m);
            s -= q*m;
            history[i] = (unsigned)s;
        }
    }
    

    unsigned next()
    {
        if (ptr==0) {
            ptr = HISTORYMAX;
            lower--;
        }
        else {
            ptr--;
            if (lower==0)
                lower = HISTORYMAX;
            else
                lower--;
        }
        unsigned ret = history[ptr]+history[lower];
        history[ptr] = ret;
        return ret;
    }

} RandomMain; 

unsigned CRandom::seedinc=0;

unsigned getRandom()
{
    return RandomMain.next();
}

void seedRandom(unsigned seed)
{
    RandomMain.seed(seed);
}


void generateRandomRecord(FILE *fp, const char *datetime, int is_fixed)
{
    char a[20], b[20], c[20];
    int c_flag;

    unsigned rn = getRandom() % 100000000;
    sprintf(a, "%05d%c%c%03d", (rn/1000)%100000, (rn%26)+'A', ((rn/26)%26)+'A', rn%1000);

    rn = getRandom() % 100000000;
    sprintf(b, "%05d%c%c%03d", (rn/1000)%100000, (rn%26)+'A', ((rn/26)%26)+'A', rn%1000);

    if (getRandom() % 10 == 0)
    {
        rn = getRandom() % 100000000;
        sprintf(c, "%05d%c%c%03d", (rn/1000)%100000, (rn%26)+'A', ((rn/26)%26)+'A', rn%1000);
        c_flag = 1;
    }
    else
    {
        c[0] = 0;
        c_flag = 0;
    }

    if (is_fixed)
    {
        fprintf(fp, "%-14s%c%-14s%c%s%05d%-14s%d\n",
                a,
                'A' + (getRandom() % 26),
                b,
                'A' + (getRandom() % 26),
                datetime,
                getRandom() % 32768,
                c,
                c_flag);

    }
    else
    {
        fprintf(fp, "%s:%c:%s:%c:%s:%d:%s:%d\n",
                a,
                'A' + (getRandom() % 26),
                b,
                'A' + (getRandom() % 26),
                datetime,
                getRandom() % 32768,
                c,
                c_flag);
    }
}


void do_pipe_mode(time_t start_time, time_t end_time, int recs_per_min)
{
    int minutes = (end_time - start_time) / 60;
    int i,j;

#ifdef _WIN32
    _setmode(_fileno(stdout), _O_BINARY);
#endif

    for (i=0; i<minutes; i++)
    {
        time_t long_time = start_time + (i*60);
        struct tm *newtime = localtime( &long_time );

        char datetime[20];
        sprintf(datetime, "%d%02d%02d%02d%03d", newtime->tm_year+1900, newtime->tm_mon+1, newtime->tm_mday, newtime->tm_hour, newtime->tm_min*6);

        for (j=0; j<recs_per_min; j++)
        {
            generateRandomRecord(stdout, datetime, 1);
        }
    }
}


void do_file_mode(int recs_per_min)
{
    int i;
    struct tm *newtime;
    time_t long_time;

    time( &long_time );
    newtime = localtime( &long_time );

    printf("Starting at %d\n", newtime->tm_sec);
    printf("Sleeping %d\n", 62 - newtime->tm_sec);

#ifndef _WIN32
    sleep(62 - newtime->tm_sec);
#else
    Sleep((62 - newtime->tm_sec) * 1000);
#endif

    while (1)
    {
        time( &long_time );
        newtime = localtime( &long_time );

        printf("Starting at %d\n", newtime->tm_sec);

        char datestr[20], timestr[20], filename[20], tfilename[20];

        sprintf(datestr, "%d%02d%02d", newtime->tm_year+1900, newtime->tm_mon+1, newtime->tm_mday);
        sprintf(timestr, "%02d%02d", newtime->tm_hour, newtime->tm_min);
        sprintf(tfilename, "%s%s.tmp", datestr, timestr);
        sprintf(filename, "%s%s.d00", datestr, timestr);

        FILE *fp = fopen(tfilename, "wb");

        char datetime[20];
        sprintf(datetime, "%d%02d%02d:%02d%03d", newtime->tm_year+1900, newtime->tm_mon+1, newtime->tm_mday, newtime->tm_hour, newtime->tm_min*6);

        for (i=1; i<=recs_per_min; i++)
        {
            generateRandomRecord(fp, datetime, 0);
        }

        fclose(fp);

        rename(tfilename, filename);

        time( &long_time );
        newtime = localtime( &long_time );

        printf("Done at %d\n", newtime->tm_sec);
        printf("Sleeping %d\n", 62 - newtime->tm_sec);

#ifndef _WIN32
        sleep(62 - newtime->tm_sec);
#else
        Sleep((62 - newtime->tm_sec) * 1000);
#endif
    }
}


#ifdef _WIN32

#define RDSTC       __asm _emit 0x0f __asm _emit 0x31

void get_time_now(__int64 & value)
{
  // long winded - but the only way to trick the compiler
  long temp[2];

  __asm 
  {
    RDSTC
    mov temp[0], eax
    mov temp[4], edx
  }

  value = *(__int64 *)temp;
}

typedef __int64 cycle_t;

cycle_t get_cycles_now()
{
  __int64 value;
  get_time_now(value);
  return value;
}

#endif


int main(int argc, char* argv[])
{
    int i;

    int pipe_mode = 0;
    time_t start_time = 0;
    time_t end_time = 0;
    int recs_per_min = 2777778;

    // -p pipe_mode
    // -st starting time
    // -et ending time
    // -m recs per minute

    for (i=1; i<argc; i++)
    {
        if (stricmp(argv[i], "-p") == 0)
        {
            pipe_mode = 1;
        }
        else if (stricmp(argv[i], "-st") == 0)
        {
            i++;
            if (i < argc)
                start_time = atol(argv[i]);
        }
        else if (stricmp(argv[i], "-et") == 0)
        {
            i++;
            if (i < argc)
                end_time = atol(argv[i]);
        }
        else if (stricmp(argv[i], "-m") == 0)
        {
            i++;
            if (i < argc)
                recs_per_min = atoi(argv[i]);
        }
    }

#ifdef _WIN32
    unsigned long seed = (unsigned)get_cycles_now();
#else
    unsigned long seed = time(NULL);
#endif
    seedRandom(seed);

    if (pipe_mode && (start_time == 0 || end_time == 0))
    {
        fprintf(stderr, "datagen: invalid usage\n");
        exit(1);
    }

    if (!pipe_mode && (start_time != 0 || end_time != 0))
    {
        fprintf(stderr, "datagen: invalid usage\n");
        exit(1);
    }

    if (pipe_mode)
        do_pipe_mode(start_time, end_time, recs_per_min);
    else
        do_file_mode(recs_per_min);
    
    return 0;
}