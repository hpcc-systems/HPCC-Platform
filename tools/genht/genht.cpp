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

#include "platform.h"
#include "jlib.hpp"
#include "jlzw.hpp"
#include "jfile.hpp"
#include "jlog.hpp"
#include "jstring.hpp"
#include "jbuff.hpp"


static void outECLheader(const char *eclmodname)
{
    printf("export Module_%s := MODULE\n",eclmodname);
    printf("export UNSIGNED4 Find(STRING key) := BEGINC++\n");
    printf("#option pure\n");
    printf("#define HASHONE(hash, c)        { hash *= 0x01000193; hash ^= c; }      \n");
    printf("static inline unsigned hashc( const unsigned char *k, unsigned length, unsigned initval)\n");
    printf("{\n");
    printf("    unsigned hash = initval;\n");
    printf("    unsigned char c;\n");
    printf("    while (length >= 8) {\n");
    printf("        c = (*k++); HASHONE(hash, c);\n");
    printf("        c = (*k++); HASHONE(hash, c);\n");
    printf("        c = (*k++); HASHONE(hash, c);\n");
    printf("        c = (*k++); HASHONE(hash, c);\n");
    printf("        length-=4;\n");
    printf("    }\n");
    printf("    switch (length) {\n");
    printf("    case 7: c = (*k++); HASHONE(hash, c);\n");
    printf("    case 6: c = (*k++); HASHONE(hash, c);\n");
    printf("    case 5: c = (*k++); HASHONE(hash, c);\n");
    printf("    case 4: c = (*k++); HASHONE(hash, c);\n");
    printf("    case 3: c = (*k++); HASHONE(hash, c);\n");
    printf("    case 2: c = (*k++); HASHONE(hash, c);\n");
    printf("    case 1: c = (*k++); HASHONE(hash, c);\n");
    printf("    }\n");
    printf("    return hash;\n");
    printf("}\n");
    printf("#undef HASHONE\n");
}

static void outECLbody(const char *eclmodname)
{
    printf(";\n");
    printf("#body\n");
    printf("  if (lenKey) {\n");
    printf("    unsigned h = hashc((const byte *)key,lenKey,lenKey)%%HASHTABLESIZE;\n");
    printf("    const byte *e = (const byte *)key+lenKey;\n");
    printf("    while (1) {\n");
    printf("      unsigned i = HashTab[h];\n");
    printf("      if (i==0) \n");
    printf("        break;\n");
    printf("      byte *p = _%s_StrData+i;\n",eclmodname);
    printf("      if (*p==lenKey) {\n");
    printf("        p++;\n");
    printf("        const byte *n=(byte *)key;\n");
    printf("        while (*p==*n) {\n");
    printf("          p++;\n");
    printf("          n++;\n");
    printf("          if (n==e)\n");
    printf("            return *(unsigned short *)p;\n");
    printf("        }\n");
    printf("      }\n");
    printf("      h++;\n");
    printf("      if (h==HASHTABLESIZE)\n");
    printf("        h = 0;\n");
    printf("    }\n");
    printf("  }\n");
    printf("  return 0;\n");
    printf("#undef HASHTABLESIZE\n");
    printf("ENDC++;\n");
    printf("STRING _Match(UNSIGNED idx) := BEGINC++\n");
    printf("#option pure\n");
    printf("extern byte _%s_StrData[];\n",eclmodname);
    printf("extern unsigned short _%s_Matches[];\n",eclmodname);
    printf("extern unsigned _%s_StrIdx[];\n",eclmodname);
    printf(";\n");
    printf("#body\n");
    printf("  const byte *r = _%s_StrData+_%s_StrIdx[_%s_Matches[idx]];\n",eclmodname,eclmodname,eclmodname); // could do better
    printf("  __lenResult = *(r++);\n");
    printf("  __result = (char *)rtlMalloc(__lenResult);\n");
    printf("  memcpy(__result,r,__lenResult);\n");
    printf("ENDC++;\n");
    printf("export STRING Match(STRING key) := FUNCTION return _Match(Find(key)); END;\n");
    printf("UNSIGNED4 _Value(UNSIGNED idx) := BEGINC++\n");
    printf("#option pure\n");
    printf("extern byte _%s_StrData[];\n",eclmodname);
    printf("extern unsigned short _%s_Values[];\n",eclmodname);
    printf("extern unsigned _%s_StrIdx[];\n",eclmodname);
    printf(";\n");
    printf("#body\n");
    printf("  return _%s_Values[idx];\n",eclmodname);
    printf("ENDC++;\n");
    printf("export UNSIGNED4 Value(STRING key) := FUNCTION return _Value(Find(key)); END;\n");
    printf("END;\n");
}
void process(const char *fname)
{
    MemoryBuffer mb;
    UnsignedArray matches;
    UnsignedArray values;
    FILE *inFile = fopen(fname, "r"TEXT_TRANS);
    if (!inFile) {
        printf("ERROR: Cannot open '%s'\n",fname);
        exit(1);
    }
    char eclmodname[256];
    bool valuesshort = true;
    strcpy(eclmodname,"UNKNOWN");
    char ln[1024];
    bool gotheader=false;
    unsigned count = 0;
    unsigned lastres = (unsigned)-1;
    unsigned lastpos = 0;
    while (fgets(ln,sizeof(ln),inFile)) {
        // format { NN,"SSS" }
        const char *s = ln;
        while (*s&&isspace(*s))
            s++;
        if (*s=='{') {
            s++;
            while (*s&&isspace(*s))
                s++;
            unsigned res = 0;
            while (*s&&isdigit(*s)) {
                res = 10*res+(*s-'0');
                s++;    
            }
            values.append(res);
            if (res>=0x10000)
                valuesshort = false;
            if (res!=lastres) {
                lastpos = matches.ordinality()+1;
                matches.append(lastpos);
                lastres = res;
            }
            else
                matches.append(lastpos);
            while (*s&&isspace(*s))
                s++;
            if (*s&&*s==',')
                s++;
            while (*s&&isspace(*s))
                s++;
            if (*s&&*s=='"')
                s++;
            const char *e = s;
            while (*e&&(*e!='"'))
                e++;            
            if (e!=s) {
                size32_t l = (byte)((e-s>254)?254:(e-s));
                mb.append((byte)l).append(l,s);
                count++;
            }
        }
        else { 
            if (memcmp(ln,"TITLE:",6)==0) {
                if (ln[6]>' ') {
                    strcpy(eclmodname,ln+6);
                    while (eclmodname[strlen(eclmodname)-1]<=' ')
                        eclmodname[strlen(eclmodname)-1] = 0;
                }
            }
/* not yet supported
            else if (memcmp(ln,"MATCHONLY:",10)==0)
                matchonly = ln[10]=='Y';
            else if (memcmp(ln,"USETABLE:",9)==0) {
                usetable = ln[9]=='Y';
                matchonly = true;
            }
            else if (memcmp(ln,"MAPVALUE:",9)==0) 
                mapvalue = ln[9]=='Y';
*/
            gotheader = true;
        }
    }
    fclose(inFile);
    assertex(count<0x10000);
    unsigned htsize = count*4/2+15;
    outECLheader(eclmodname);
    printf("#define HASHTABLESIZE %d\n",htsize);
    printf("byte _%s_StrData[%d] = {\n",eclmodname,mb.length()+4+count*2);
    printf("    0,   0,   0,\n");
    const byte *base = (const byte *)mb.toByteArray();
    const byte *p = base;
    unsigned *htab=(unsigned *)calloc(htsize,sizeof(unsigned));
    unsigned i=0;
    unsigned o = 3;
    size32_t l = mb.length();
    UnsignedArray offsets;
    offsets.append(0);
    StringBuffer s;
    while ((size32_t)(p-base)<l) {
        i++;
        offsets.append(o);
        size32_t sl = *p;
        unsigned h = hashc(p+1,sl,sl)%htsize;
        loop {
            if (htab[h]==0) {
                htab[h] = o;
                break;
            }
            h++;
            if (h==htsize)
                h = 0;
        }
        s.clear().append(' ');
        for (unsigned j=0; j<=sl; j++)
            s.appendf(" %3d,",(int)*(p++));
        s.appendf(" %3d, %3d,",(i&0xff),(i>>8));
        printf("%s\n",s.str());
        o+=sl+3;

    }
    printf("   0\n};\n");
    unsigned n = offsets.ordinality();
    printf("unsigned _%s_StrIdx[%d] = {\n",eclmodname,n+1);
    assertex(n<0x10000);
    for (i=0;i<n;) {
        printf("  ");
        unsigned ln = (n-i<10)?(n-i):10;
        while (ln--)
            printf("%5d, ",offsets.item(i++));
        printf("\n");
    }
    printf("   0\n};\n");
    printf("static unsigned HashTab[HASHTABLESIZE+1] = {\n");
    for (i=0;i<htsize;) {
        printf("  ");
        unsigned ln = (htsize-i<10)?(htsize-i):10;
        while (ln--)
            printf("%7d, ",htab[i++]);
        printf("\n");
    }
    printf("   0\n};\n");
    n = values.ordinality();
    printf("unsigned%s _%s_Values[%d] = {\n",valuesshort?" short":"",eclmodname,n+2);
    printf("  0,\n");
    for (i=0;i<n;) {
        assertex(i<values.ordinality());
        printf("  ");
        unsigned ln = (n-i<10)?(n-i):10;
        while (ln--)
            printf("%5d, ",values.item(i++));
        printf("\n");
    }
    printf("   0\n};\n");
    n = matches.ordinality();
    printf("unsigned%s _%s_Matches[%d] = {\n",valuesshort?" short":"",eclmodname,n+2);
    printf("  0,\n");
    for (i=0;i<n;) {
        assertex(i<matches.ordinality());
        printf("  ");
        unsigned ln = (n-i<10)?(n-i):10;
        while (ln--)
            printf("%5d, ",matches.item(i++));
        printf("\n");
    }
    printf("   0\n};\n");
    outECLbody(eclmodname);
}


        


    

int main(int argc, char* argv[])
{
    InitModuleObjects();
    EnableSEHtoExceptionMapping();
    if (argc<2) 
        printf("Usage: genht <mst-file>\n");
    else
        process(argv[1]);
    releaseAtoms();
    return 0;
}


