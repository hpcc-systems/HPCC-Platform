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

#ifndef CTFILE_HPP
#define CTFILE_HPP

#include "jiface.hpp"
#include "jhutil.hpp"
#include "hlzw.h"
#include "jcrc.hpp"
#include "jio.hpp"
#include "jfile.hpp"

#define NODESIZE 8192


#define HTREE_FPOS_OFFSET   0x01 // Obsolete, not supported
#define HTREE_TOPLEVEL_KEY  0x02
#define COL_PREFIX          0x04
#define COL_SUFFIX          0x08 // Obsolete, not supported
#define HTREE_VARSIZE       0x10
#define HTREE_FULLSORT_KEY  0x20
#define INDAR_TRAILING_SEG  0x80 // Obsolete, not supported
#define HTREE_COMPRESSED_KEY 0x40
#define HTREE_QUICK_COMPRESSED_KEY 0x48
#define KEYBUILD_VERSION 1 // unsigned short. NB: This should upped if a change would make existing keys incompatible with current build.
#define KEYBUILD_MAXLENGTH 0x7FFF

// structure to be read into - NO VIRTUALS.
// This header layout corresponds to FairCom cTree layout for compatibility with old systems ...

struct __declspec(novtable) jhtree_decl KeyHdr
{
    __int64 phyrec; /* last byte offset of file     00x */
    __int64 delstk; /* top of delete stack: fixed len data  08x */
    __int64 numrec; /* last byte offset written     10x */
    __int64 reshdr; /* resource header          18x */
    __int64 lstmbr; /* last super file member/position  20x */
    __int64 sernum; /* serial number            28x */
    __int64 nument; /* active entries           30x */
    __int64 root;   /* B-Tree root              38x */
    __int64 fileid; /* unique file id           40x */
    __int64 servid; /* unique server id         48x */
    short   verson; /* configuration options at create  50x */
    unsigned short  nodeSize;   /* node record size         52x */
    unsigned short  reclen; /* data record length           54x */
    unsigned short  extsiz; /* extend file (chunk) size     56x */
    unsigned short  flmode; /* file mode (virtual, etc)     58x */
    unsigned short  logtyp; /* permanent components of file mode    5ax */
    unsigned short  maxkbl; /* maximum key bytes leaf-var       5cx */
    unsigned short  maxkbn; /* maximum key bytes non leaf-var   5ex */
    char    updflg; /* update (corrupt) flag        60x */
    char    ktype;  /* file type flag           61x */
    char    autodup;/* duplicate flag           62x */
    char    deltyp; /* flag for type of idx delete      63x */
    unsigned char   keypad; /* padding byte             64x */
    unsigned char   flflvr; /* file flavor              65x */
    unsigned char   flalgn; /* file alignment           66x */
    unsigned char   flpntr; /* file pointer size            67x */
    unsigned short  clstyp; /* flag for file type           68x */
    unsigned short  length; /* key length               6ax */
    short   nmem;   /* number of members            6cx */
    short   kmem;   /* member number            6ex */
    __int64 lanchr; /* left most leaf anchor        70x */
    __int64 supid;  /* super file member #          78x */
    __int64 hdrpos; /* header position          80x */
    __int64 sihdr;  /* superfile header index hdr position  88x */
    __int64 timeid; /* time id#             90x */
    unsigned short  suptyp; /* super file type          98x */
    unsigned short  maxmrk; /* maximum exc mark entries per leaf    9ax */
    unsigned short  namlen; /* MAX_NAME at creation         9cx */
    unsigned short  xflmod; /* extended file mode info      9ex */
    __int64 defrel; /* file def release mask        a0x */
    __int64 hghtrn; /* tran# high water mark for idx    a8x */
    __int64 hdrseq; /* wrthdr sequence #            b0x */
    __int64 tstamp; /* update time stamp            b8x */
    __int64 rs3[3]; /* future use               c0x */
    __int64 fposOffset; /* amount by which file positions are biased        d8x */
    __int64 fileSize; /* fileSize - used in the bias calculation e0x */
    short nodeKeyLength; /* key length in intermediate level nodes e8x */
    unsigned short version; /* build version - to be updated if key format changes    eax*/
    short unused[2]; /* unused ecx */
    __int64 blobHead; /* fpos of first blob node f0x */
    __int64 metadataHead; /* fpos of first metadata node f8x */
};

//#pragma pack(1)
#pragma pack(push,1)
struct jhtree_decl NodeHdr
{
    __int64    rightSib;
    __int64    leftSib;
    unsigned short   numKeys;
    unsigned short   keyBytes;
    unsigned    crc32;
    char    unusedMemNumber;
    char    leafFlag;

    bool isValid(unsigned nodeSize)
    {
        return 
            (rightSib % nodeSize == 0) &&
            (leftSib % nodeSize == 0) &&
            (unusedMemNumber==0) &&
            (keyBytes < nodeSize);
    }
};
//#pragma pack(4)
#pragma pack(pop)

class jhtree_decl CKeyHdr : public CInterface, implements IInterface
{
private:
    KeyHdr hdr;
public:
    IMPLEMENT_IINTERFACE;
    CKeyHdr();

    void load(KeyHdr &_hdr);
    void write(IWriteSeq *, CRC32 *crc = NULL);
    void write(IFileIOStream *, CRC32 *crc = NULL);

    unsigned int getMaxKeyLength();
    bool isVariable();
    inline unsigned int getNodeKeyLength() 
    {
        return hdr.nodeKeyLength != -1 ? hdr.nodeKeyLength : getMaxKeyLength(); 
    }
    inline bool hasPayload()
    {
        return (hdr.nodeKeyLength != -1);
    }
    inline unsigned char getKeyPad() { return hdr.keypad; }
    inline char getKeyType() { return hdr.ktype; }
    inline offset_t getRootFPos() { return hdr.root; }
    inline unsigned short getMaxNodeBytes() { return hdr.maxkbl; }
    inline KeyHdr *getHdrStruct() { return &hdr; }
    inline static size32_t getSize() { return sizeof(KeyHdr); }
    inline unsigned getNodeSize() { return hdr.nodeSize; }
};

class jhtree_decl CNodeBase : public CInterface, implements IInterface
{
protected:
    NodeHdr hdr;
    byte keyType;
    size32_t keyLen;
    size32_t keyCompareLen;
    offset_t fpos;
    CKeyHdr *keyHdr;
    bool isVariable;

public:
    inline offset_t getFpos() const { return fpos; }
    inline size32_t getKeyLen() const { return keyLen; }
    inline size32_t getNumKeys() const { return hdr.numKeys; }
    inline bool isBlob() const { return hdr.leafFlag == 2; }
    inline bool isMetadata() const { return hdr.leafFlag == 3; }
    inline bool isLeaf() const { return hdr.leafFlag != 0; }

public:
    IMPLEMENT_IINTERFACE;

    CNodeBase();
    void load(CKeyHdr *keyHdr, offset_t fpos);
    ~CNodeBase();
};

class jhtree_decl CJHTreeNode : public CNodeBase
{
protected:
    size32_t keyRecLen;
    char *keyBuf;

    static SpinLock spin;
    static unsigned __int64 totalAllocatedCurrent;
    static unsigned __int64 totalAllocatedEver;
    static unsigned countAllocationsCurrent;
    static unsigned countAllocationsEver;

    void unpack(const void *node, bool needCopy);
    unsigned __int64 firstSequence;
    size32_t expandedSize;
    Owned<IRandRowExpander> rowexp;  // expander for rand rowdiff   

    static char *expandKeys(void *src,unsigned keylength,size32_t &retsize, bool rowcompression);
    static IRandRowExpander *expandQuickKeys(void *src, bool needCopy);

    static void releaseMem(void *togo, size32_t size);
    static void *allocMem(size32_t size);

public:
    CJHTreeNode();
    virtual void load(CKeyHdr *keyHdr, const void *rawData, offset_t pos, bool needCopy);
    ~CJHTreeNode();
    size32_t getMemSize() { return expandedSize; }

// reading methods
    offset_t prevNodeFpos() const;
    offset_t nextNodeFpos() const ;
    virtual bool getValueAt(unsigned int num, char *key) const;
    virtual size32_t getSizeAt(unsigned int num) const;
    virtual offset_t getFPosAt(unsigned int num) const;
    virtual int compareValueAt(const char *src, unsigned int index) const;
    bool contains(const char *src) const;
    inline offset_t getRightSib() const { return hdr.rightSib; }
    inline offset_t getLeftSib() const { return hdr.leftSib; }
    unsigned __int64 getSequence(unsigned int num) const;

    virtual void dump();
};

class CJHVarTreeNode : public CJHTreeNode 
{
    const char **recArray;

public:
    CJHVarTreeNode();
    ~CJHVarTreeNode();
    virtual void load(CKeyHdr *keyHdr, const void *rawData, offset_t pos, bool needCopy);
    virtual bool getValueAt(unsigned int num, char *key) const;
    virtual size32_t getSizeAt(unsigned int num) const;
    virtual offset_t getFPosAt(unsigned int num) const;
    virtual int compareValueAt(const char *src, unsigned int index) const;
    virtual void dump();
};

class CJHTreeBlobNode : public CJHTreeNode
{
public:
    CJHTreeBlobNode ();
    ~CJHTreeBlobNode ();
    virtual bool getValueAt(unsigned int num, char *key) const {throwUnexpected();}
    virtual offset_t getFPosAt(unsigned int num) const {throwUnexpected();}
    virtual size32_t getSizeAt(unsigned int num) const {throwUnexpected();}
    virtual int compareValueAt(const char *src, unsigned int index) const {throwUnexpected();}
    virtual void dump() {throwUnexpected();}

    size32_t getTotalBlobSize(unsigned offset);
    size32_t getBlobData(unsigned offset, void *dst);
};

class CJHTreeMetadataNode : public CJHTreeNode
{
public:
    virtual bool getValueAt(unsigned int num, char *key) const {throwUnexpected();}
    virtual offset_t getFPosAt(unsigned int num) const {throwUnexpected();}
    virtual size32_t getSizeAt(unsigned int num) const {throwUnexpected();}
    virtual int compareValueAt(const char *src, unsigned int index) const {throwUnexpected();}
    virtual void dump() {throwUnexpected();}
    void get(StringBuffer & out);
};

class jhtree_decl CNodeHeader : public CNodeBase
{
public:
    CNodeHeader();

    void load(NodeHdr &hdr);

    inline offset_t getRightSib() const { return hdr.rightSib; }
    inline offset_t getLeftSib() const { return hdr.leftSib; }
};

class jhtree_decl CWriteNodeBase : public CNodeBase
{
protected:
    char *nodeBuf;
    char *keyPtr;
    int maxBytes;
    KeyCompressor lzwcomp;
    void writeHdr();

public:
    CWriteNodeBase(offset_t fpos, CKeyHdr *keyHdr);
    ~CWriteNodeBase();

    void write(IFileIOStream *, CRC32 *crc = NULL);
    void setLeftSib(offset_t leftSib) { hdr.leftSib = leftSib; }
    void setRightSib(offset_t rightSib) { hdr.rightSib = rightSib; }
};


class jhtree_decl CWriteNode : public CWriteNodeBase
{
private:
    char *lastKeyValue;
    unsigned __int64 lastSequence;

public:
    CWriteNode(offset_t fpos, CKeyHdr *keyHdr, bool isLeaf);
    ~CWriteNode();

    size32_t compressValue(const char *keyData, size32_t size, char *result);
    bool add(offset_t pos, const void *data, size32_t size, unsigned __int64 sequence);
    const void *getLastKeyValue() const { return lastKeyValue; }
    unsigned __int64 getLastSequence() const { return lastSequence; }
};

class jhtree_decl CBlobWriteNode : public CWriteNodeBase
{
    static unsigned __int64 makeBlobId(offset_t nodepos, unsigned offset);
public:
    CBlobWriteNode(offset_t _fpos, CKeyHdr *keyHdr);
    ~CBlobWriteNode();

    unsigned __int64 add(const char * &data, size32_t &size);
};

class jhtree_decl CMetadataWriteNode : public CWriteNodeBase
{
public:
    CMetadataWriteNode(offset_t _fpos, CKeyHdr *keyHdr);
    size32_t set(const char * &data, size32_t &size);
};

enum KeyExceptionCodes
{
    KeyExcpt_IncompatVersion = 1,
};
interface IKeyException : extends IException { };
IKeyException *MakeKeyException(int code, const char *format, ...) __attribute__((format(printf, 2, 3)));


#endif
