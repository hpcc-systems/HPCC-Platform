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

#include "jlib.hpp"
#include "ctfile.hpp"

bool skipLeafLevel = false;
bool checkCRC = false;
bool quick = false;
offset_t nodeAddress = 0;
unsigned errors = 0;
unsigned errorLimit = 10;

const char *curFileName = NULL;

inline void SwapBigEndian(KeyHdr &hdr)
{
    _WINREV(hdr.phyrec);
    _WINREV(hdr.delstk);
    _WINREV(hdr.numrec);
    _WINREV(hdr.reshdr);
    _WINREV(hdr.lstmbr);
    _WINREV(hdr.sernum);
    _WINREV(hdr.nument);
    _WINREV(hdr.root);
    _WINREV(hdr.fileid);
    _WINREV(hdr.servid);
    _WINREV(hdr.verson);
    _WINREV(hdr.nodeSize);
    _WINREV(hdr.extsiz);
    _WINREV(hdr.flmode);
    _WINREV(hdr.maxkbl);
    _WINREV(hdr.maxkbn);
//    _WINREV(hdr.updflg);
//    _WINREV(hdr.autodup);
//    _WINREV(hdr.deltyp);
//    _WINREV(hdr.keypad);
//    _WINREV(hdr.flflvr);
//    _WINREV(hdr.flalgn);
//    _WINREV(hdr.flpntr);
    _WINREV(hdr.clstyp);
    _WINREV(hdr.length);
    _WINREV(hdr.nmem);
    _WINREV(hdr.kmem);
    _WINREV(hdr.lanchr);
    _WINREV(hdr.supid);
    _WINREV(hdr.hdrpos);
    _WINREV(hdr.sihdr);
    _WINREV(hdr.timeid);
    _WINREV(hdr.suptyp);
    _WINREV(hdr.maxmrk);
    _WINREV(hdr.namlen);
    _WINREV(hdr.xflmod);
    _WINREV(hdr.defrel);
    _WINREV(hdr.hghtrn);
    _WINREV(hdr.hdrseq);
    _WINREV(hdr.tstamp);
    _WINREV(hdr.rs3[0]);
    _WINREV(hdr.rs3[1]);
    _WINREV(hdr.rs3[2]);
    _WINREV(hdr.fposOffset);
    _WINREV(hdr.fileSize);
    _WINREV(hdr.nodeKeyLength);
    _WINREV(hdr.version);
    _WINREV(hdr.blobHead);
    _WINREV(hdr.metadataHead);
}


inline void swap(NodeHdr &hdr)
{
    _WINREV(hdr.rightSib);
    _WINREV(hdr.leftSib);
    _WINREV(hdr.numKeys);
    _WINREV(hdr.keyBytes);
    _WINREV(hdr.crc32);
//   _WINREV(hdr.memNumber);
//   _WINREV(hdr.leafFlag);
}

//---------------------------------------------------------------------------

static unsigned long crc_32_tab[] = { /* CRC polynomial 0xedb88320 */
0x00000000L, 0x77073096L, 0xee0e612cL, 0x990951baL, 0x076dc419L, 0x706af48fL,
0xe963a535L, 0x9e6495a3L, 0x0edb8832L, 0x79dcb8a4L, 0xe0d5e91eL, 0x97d2d988L,
0x09b64c2bL, 0x7eb17cbdL, 0xe7b82d07L, 0x90bf1d91L, 0x1db71064L, 0x6ab020f2L,
0xf3b97148L, 0x84be41deL, 0x1adad47dL, 0x6ddde4ebL, 0xf4d4b551L, 0x83d385c7L,
0x136c9856L, 0x646ba8c0L, 0xfd62f97aL, 0x8a65c9ecL, 0x14015c4fL, 0x63066cd9L,
0xfa0f3d63L, 0x8d080df5L, 0x3b6e20c8L, 0x4c69105eL, 0xd56041e4L, 0xa2677172L,
0x3c03e4d1L, 0x4b04d447L, 0xd20d85fdL, 0xa50ab56bL, 0x35b5a8faL, 0x42b2986cL,
0xdbbbc9d6L, 0xacbcf940L, 0x32d86ce3L, 0x45df5c75L, 0xdcd60dcfL, 0xabd13d59L,
0x26d930acL, 0x51de003aL, 0xc8d75180L, 0xbfd06116L, 0x21b4f4b5L, 0x56b3c423L,
0xcfba9599L, 0xb8bda50fL, 0x2802b89eL, 0x5f058808L, 0xc60cd9b2L, 0xb10be924L,
0x2f6f7c87L, 0x58684c11L, 0xc1611dabL, 0xb6662d3dL, 0x76dc4190L, 0x01db7106L,
0x98d220bcL, 0xefd5102aL, 0x71b18589L, 0x06b6b51fL, 0x9fbfe4a5L, 0xe8b8d433L,
0x7807c9a2L, 0x0f00f934L, 0x9609a88eL, 0xe10e9818L, 0x7f6a0dbbL, 0x086d3d2dL,
0x91646c97L, 0xe6635c01L, 0x6b6b51f4L, 0x1c6c6162L, 0x856530d8L, 0xf262004eL,
0x6c0695edL, 0x1b01a57bL, 0x8208f4c1L, 0xf50fc457L, 0x65b0d9c6L, 0x12b7e950L,
0x8bbeb8eaL, 0xfcb9887cL, 0x62dd1ddfL, 0x15da2d49L, 0x8cd37cf3L, 0xfbd44c65L,
0x4db26158L, 0x3ab551ceL, 0xa3bc0074L, 0xd4bb30e2L, 0x4adfa541L, 0x3dd895d7L,
0xa4d1c46dL, 0xd3d6f4fbL, 0x4369e96aL, 0x346ed9fcL, 0xad678846L, 0xda60b8d0L,
0x44042d73L, 0x33031de5L, 0xaa0a4c5fL, 0xdd0d7cc9L, 0x5005713cL, 0x270241aaL,
0xbe0b1010L, 0xc90c2086L, 0x5768b525L, 0x206f85b3L, 0xb966d409L, 0xce61e49fL,
0x5edef90eL, 0x29d9c998L, 0xb0d09822L, 0xc7d7a8b4L, 0x59b33d17L, 0x2eb40d81L,
0xb7bd5c3bL, 0xc0ba6cadL, 0xedb88320L, 0x9abfb3b6L, 0x03b6e20cL, 0x74b1d29aL,
0xead54739L, 0x9dd277afL, 0x04db2615L, 0x73dc1683L, 0xe3630b12L, 0x94643b84L,
0x0d6d6a3eL, 0x7a6a5aa8L, 0xe40ecf0bL, 0x9309ff9dL, 0x0a00ae27L, 0x7d079eb1L,
0xf00f9344L, 0x8708a3d2L, 0x1e01f268L, 0x6906c2feL, 0xf762575dL, 0x806567cbL,
0x196c3671L, 0x6e6b06e7L, 0xfed41b76L, 0x89d32be0L, 0x10da7a5aL, 0x67dd4accL,
0xf9b9df6fL, 0x8ebeeff9L, 0x17b7be43L, 0x60b08ed5L, 0xd6d6a3e8L, 0xa1d1937eL,
0x38d8c2c4L, 0x4fdff252L, 0xd1bb67f1L, 0xa6bc5767L, 0x3fb506ddL, 0x48b2364bL,
0xd80d2bdaL, 0xaf0a1b4cL, 0x36034af6L, 0x41047a60L, 0xdf60efc3L, 0xa867df55L,
0x316e8eefL, 0x4669be79L, 0xcb61b38cL, 0xbc66831aL, 0x256fd2a0L, 0x5268e236L,
0xcc0c7795L, 0xbb0b4703L, 0x220216b9L, 0x5505262fL, 0xc5ba3bbeL, 0xb2bd0b28L,
0x2bb45a92L, 0x5cb36a04L, 0xc2d7ffa7L, 0xb5d0cf31L, 0x2cd99e8bL, 0x5bdeae1dL,
0x9b64c2b0L, 0xec63f226L, 0x756aa39cL, 0x026d930aL, 0x9c0906a9L, 0xeb0e363fL,
0x72076785L, 0x05005713L, 0x95bf4a82L, 0xe2b87a14L, 0x7bb12baeL, 0x0cb61b38L,
0x92d28e9bL, 0xe5d5be0dL, 0x7cdcefb7L, 0x0bdbdf21L, 0x86d3d2d4L, 0xf1d4e242L,
0x68ddb3f8L, 0x1fda836eL, 0x81be16cdL, 0xf6b9265bL, 0x6fb077e1L, 0x18b74777L,
0x88085ae6L, 0xff0f6a70L, 0x66063bcaL, 0x11010b5cL, 0x8f659effL, 0xf862ae69L,
0x616bffd3L, 0x166ccf45L, 0xa00ae278L, 0xd70dd2eeL, 0x4e048354L, 0x3903b3c2L,
0xa7672661L, 0xd06016f7L, 0x4969474dL, 0x3e6e77dbL, 0xaed16a4aL, 0xd9d65adcL,
0x40df0b66L, 0x37d83bf0L, 0xa9bcae53L, 0xdebb9ec5L, 0x47b2cf7fL, 0x30b5ffe9L,
0xbdbdf21cL, 0xcabac28aL, 0x53b39330L, 0x24b4a3a6L, 0xbad03605L, 0xcdd70693L,
0x54de5729L, 0x23d967bfL, 0xb3667a2eL, 0xc4614ab8L, 0x5d681b02L, 0x2a6f2b94L,
0xb40bbe37L, 0xc30c8ea1L, 0x5a05df1bL, 0x2d02ef8dL
};

#define UPDC32(octet, crc) (crc_32_tab[((crc) ^ (octet)) & 0xff] ^ ((crc) >> 8))

unsigned long mcrc32(const char *buf, unsigned len, unsigned long crc)
{
    unsigned char    c;
    while(len >= 12)
    {
        c = *buf++; crc = UPDC32(c,crc);
        c = *buf++; crc = UPDC32(c,crc);
        c = *buf++; crc = UPDC32(c,crc);
        c = *buf++; crc = UPDC32(c,crc);
        len -= 4;
    }

    switch (len)
    {
    case 11: c = *buf++; crc = UPDC32(c,crc);
    case 10: c = *buf++; crc = UPDC32(c,crc);
    case 9: c = *buf++; crc = UPDC32(c,crc);
    case 8: c = *buf++; crc = UPDC32(c,crc);
    case 7: c = *buf++; crc = UPDC32(c,crc);
    case 6: c = *buf++; crc = UPDC32(c,crc);
    case 5: c = *buf++; crc = UPDC32(c,crc);
    case 4: c = *buf++; crc = UPDC32(c,crc);
    case 3: c = *buf++; crc = UPDC32(c,crc);
    case 2: c = *buf++; crc = UPDC32(c,crc);
    case 1: c = *buf++; crc = UPDC32(c,crc);
    }

    return(crc);
}

void noteError(offset_t offset, const char *format, ...) __attribute__((format(printf, 2, 3)));
void noteError(offset_t offset, const char *format, ...)
{
    va_list arg;
    va_start(arg, format);
    fprintf(stderr, "%s: ", curFileName);
    if (offset)
        fprintf(stderr, "%" I64F "x: ", offset);
    vfprintf(stderr, format, arg);
    va_end(arg);
    errors++;
    if (errors >= errorLimit)
        exit(2);
}

void checkNode(int f, KeyHdr &h, offset_t thisnode)
{
    _lseeki64(f, thisnode, SEEK_SET);
    char *nodeData = (char *) malloc(h.nodeSize);
    if (!nodeData || _read(f, nodeData, h.nodeSize) != h.nodeSize)
    {
        noteError(thisnode, "Could not read node (error %d)\n", errno);
    }
    else
    {
        NodeHdr &nodeHdr = *(NodeHdr *) nodeData;
        swap(nodeHdr);
        if ( nodeHdr.rightSib > h.phyrec || 
             nodeHdr.leftSib > h.phyrec || 
             nodeHdr.rightSib % h.nodeSize ||
             nodeHdr.leftSib % h.nodeSize ||
             nodeHdr.keyBytes == 0 ||
             nodeHdr.keyBytes > h.nodeSize )
        {
            noteError(thisnode, "Htree: Corrupt key node detected (keyBytes==%x)\n", nodeHdr.keyBytes );
        }
        else if (nodeHdr.crc32 && checkCRC)
        {
            unsigned crc = mcrc32(nodeData+sizeof(NodeHdr), nodeHdr.keyBytes, 0);
            if (nodeHdr.crc32 != crc)
            {
                noteError(thisnode, "CRC error on key node (keyBytes==%x)\n", nodeHdr.keyBytes );
            }
        }
    }
    free(nodeData);
}

unsigned countLevels(int f, KeyHdr &h, offset_t firstnode)
{
    _lseeki64(f, firstnode, SEEK_SET);
    char *nodeData = (char *) malloc(h.nodeSize);
    if (!nodeData || _read(f, nodeData, h.nodeSize) != h.nodeSize)
    {
        noteError(firstnode, "Could not read node (error %d)\n", errno);
        free(nodeData);
        return 0;
    }
    else
    {
        NodeHdr &nodeHdr = *(NodeHdr *) nodeData;
        swap(nodeHdr);
        if (!nodeHdr.leafFlag)
        {
            unsigned __int64 fpos = *(unsigned __int64 *) (nodeData + sizeof(nodeHdr));
            _WINREV(fpos);
            free(nodeData);
            return countLevels(f, h, fpos)+1;
        }
        else
        {
            free(nodeData);
            return 1;
        }
    }
}

void checkLevel(int f, KeyHdr &h, unsigned &level, offset_t firstnode)
{
    unsigned nodecount = 0;
    unsigned maxExpandSize = 0;
    unsigned __int64 totalExpandSize = 0;
    unsigned __int64 reccount = 0;
    unsigned __int64 maxcount = 0;
    offset_t thisnode = firstnode;
    while (thisnode)
    {
        _lseeki64(f, thisnode, SEEK_SET);
        char *nodeData = (char *) malloc(h.nodeSize);
        if (!nodeData || _read(f, nodeData, h.nodeSize) != h.nodeSize)
        {
            noteError(thisnode, "Could not read node (error %d)\n", errno);
            free(nodeData);
            return;
        }
        NodeHdr &nodeHdr = *(NodeHdr *) nodeData;
        swap(nodeHdr);
        if ( nodeHdr.rightSib > h.phyrec || 
             nodeHdr.leftSib > h.phyrec || 
             nodeHdr.rightSib % h.nodeSize ||
             nodeHdr.leftSib % h.nodeSize ||
             nodeHdr.keyBytes == 0 ||
             nodeHdr.keyBytes > h.nodeSize )
        {
            noteError(thisnode, "Htree: Corrupt key node detected (keyBytes==%x)\n", nodeHdr.keyBytes );
        }
        else if (nodeHdr.crc32 && checkCRC)
        {
            unsigned crc = mcrc32(nodeData+sizeof(NodeHdr), nodeHdr.keyBytes, 0);
            if (nodeHdr.crc32 != crc)
            {
                noteError(thisnode, "CRC error on key node (keyBytes==%x)\n", nodeHdr.keyBytes );
            }
        }
        if (nodeHdr.leafFlag)
        {
            if (skipLeafLevel)
            {
                level++;
                free(nodeData);
                return;
            }
            if ((h.ktype & (HTREE_COMPRESSED_KEY|HTREE_QUICK_COMPRESSED_KEY))==HTREE_COMPRESSED_KEY)
            {
                unsigned expandSize;
                memcpy(&expandSize, nodeData+sizeof(NodeHdr)+8, 4);
                _WINREV(expandSize);
                if (expandSize > maxExpandSize)
                    maxExpandSize = expandSize;
                totalExpandSize += expandSize;
            }
        }
        else 
        {
            unsigned nodeKeyLength = h.nodeKeyLength;
            if (nodeKeyLength==(unsigned)-1)
                nodeKeyLength = h.length;
            unsigned expandSize = nodeKeyLength * nodeHdr.numKeys;
            if (expandSize > maxExpandSize)
                maxExpandSize = expandSize;
            totalExpandSize += expandSize;
            if (!nodecount)
            {
                unsigned __int64 fpos = *(unsigned __int64 *) (nodeData + sizeof(nodeHdr));
                _WINREV(fpos);
                checkLevel(f, h, level, fpos);
            }
        }

        nodecount++;
        reccount+= nodeHdr.numKeys;
        if (nodeHdr.numKeys > maxcount)
            maxcount = nodeHdr.numKeys;
        thisnode = nodeHdr.rightSib;
        free(nodeData);
    }
    printf("%d nodes containing %" I64F "d records expanding to %" I64F "d bytes (average %.2f per node, maximum %" I64F "d, max expand %d) at level %d\n", nodecount, reccount, totalExpandSize, (float) reccount/nodecount, maxcount, maxExpandSize, level);
    level++;
}

void usage(int exitCode)
{
    printf("vkey [options] keyfiles\n");
    printf("  -crc              check node crc's match\n");
    printf("  -node <hexoffset> only check node at specified offset\n");
    printf("  -noleaf           skip leaf level\n");
    printf("  -quick            just check quick stats\n");
    
    exit(exitCode);
}

int main(int argc, const char *argv[])
{
    if (argc<2) 
        usage(0);

    int arg = 1;
    while (arg < argc)
    {
        if (stricmp(argv[arg], "-crc") == 0)
        {
            checkCRC = true;
        }
        else if (stricmp(argv[arg], "-errorLimit") == 0)
        {
            ++arg;
            if (arg>=argc)
                usage(1);           
            errorLimit = strtoul(argv[arg], NULL, 10);
            if (!errorLimit)
                errorLimit = (unsigned) -1;
        }
        else if (stricmp(argv[arg], "-node") == 0)
        {
            ++arg;
            if (arg>=argc)
                usage(1);           
            nodeAddress = strtoul(argv[arg], NULL, 16);
            checkCRC = true;
        }
        else if (stricmp(argv[arg], "-noleaf") == 0)
        {
            skipLeafLevel = true;
        }
        else if (stricmp(argv[arg], "-quick") == 0)
        {
            quick = true;
        }
        else if (*argv[arg]=='-')
            usage(1);
        ++arg;
    }
    arg = 1;
    while (arg < argc)
    {
        if (*argv[arg]!='-')
        {
            curFileName = argv[arg];
            printf("Processing key file %s\n", curFileName);
            int f = _open(argv[arg], _O_RDONLY|_O_BINARY);
            if (f==-1)
            {
                noteError(0, "Could not open file\n");
            }
            else
            {
                KeyHdr h;
                if (_read(f, &h, sizeof(h)) != sizeof(h))
                {
                    noteError(0, "Could not read key header\n");
                }
                else
                {
                    SwapBigEndian(h);
                    if (h.ktype & USE_TRAILING_HEADER)
                    {
                        printf("Reading trailing key header\n");
                        lseek(f, -h.nodeSize, SEEK_END);
                        if (_read(f, &h, sizeof(h)) != sizeof(h))
                        {
                            noteError(0, "Could not read trailing key header\n");
                        }
                        SwapBigEndian(h);
                    }
                    if (nodeAddress)
                    {
                        checkNode(f, h, nodeAddress);
                    }
                    else if (quick)
                    {
                        int levels = countLevels(f, h, h.root);
                        printf("%d levels found\n", levels);
                    }
                    else
                    {
                        unsigned level = 0;
                        checkLevel(f, h, level, h.root);
                    }
                }
                _close(f);
            }
        }
        arg++;
    }
    return errors;
}

