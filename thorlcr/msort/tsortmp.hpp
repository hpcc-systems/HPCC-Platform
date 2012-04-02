#ifndef TSortMP_HPP
#define TSortMP_HPP

#include <jbuff.hpp>

#include <mpbase.hpp>
#include <mpcomm.hpp>


#define CMPFN_NORMAL 0
#define CMPFN_COLLATE 1
#define CMPFN_UPPER 2

interface ISortSlaveMP
{
    virtual bool Connect(unsigned _part, unsigned _numnodes)=0;
    virtual void StartGather()=0;
    virtual void GetGatherInfo(rowmap_t &numlocal, memsize_t &totalsize, unsigned &overflowscale, bool hasserializer)=0;
    virtual rowmap_t GetMinMax(size32_t &keybuffsize,void *&keybuff, size32_t &avrecsizesize)=0;
    virtual bool GetMidPoint     (size32_t lkeysize, const byte * lkey, size32_t hkeysize, const byte * hkey, size32_t &mkeysize, byte * &mkey)=0;
    virtual void GetMultiMidPoint(size32_t lkeybuffsize, const void * lkeybuff, size32_t hkeybuffsize, const void * hkeybuff, size32_t &mkeybuffsize, void * &mkeybuf)=0;
    virtual void GetMultiMidPointStart(size32_t lkeybuffsize, const void * lkeybuff, size32_t hkeybuffsize, const void * hkeybuff)=0; /* async */
    virtual void GetMultiMidPointStop(size32_t &mkeybuffsize, void * &mkeybuf)=0;
    virtual rowmap_t SingleBinChop(size32_t keysize,const byte *key,byte cmpfn)=0;
    virtual void MultiBinChop(size32_t keybuffsize,const byte *keybuff, unsigned num,rowmap_t *pos,byte cmpfn,bool useaux)=0;
    virtual void MultiBinChopStart(size32_t keybuffsize,const byte *keybuff, byte cmpfn)=0; /* async */
    virtual void MultiBinChopStop(unsigned num,rowmap_t *pos)=0;
    virtual void OverflowAdjustMapStart( unsigned mapsize,rowmap_t *map,size32_t keybuffsize,const byte *keybuff, byte cmpfn, bool useaux)=0; /* async */
    virtual rowmap_t OverflowAdjustMapStop( unsigned mapsize, rowmap_t *map)=0;
    virtual void MultiMerge(unsigned mapsize,rowmap_t *map,unsigned num,SocketEndpoint* endpoints)=0; /* async */
    virtual void MultiMergeBetween(unsigned mapsize,rowmap_t *map,rowmap_t *mapupper,unsigned num,SocketEndpoint* endpoints)=0; /* async */
    virtual void SingleMerge()=0; /* async */
    virtual bool FirstRowOfFile(const char *filename,size32_t &rowbuffsize, byte * &rowbuf)=0;
    virtual void GetMultiNthRow(unsigned numsplits,size32_t &mkeybuffsize, void * &mkeybuf)=0;              
    virtual void StartMiniSort(rowcount_t _totalrows)=0; /* async */
    virtual void Close()=0; /* async */
    virtual void CloseWait()=0;
    virtual void Disconnect()=0; /* async */

};


class SortSlaveMP: implements ISortSlaveMP
{
    Owned<ICommunicator> comm;
    rank_t rank;
    mptag_t tag;

    bool sendRecv(CMessageBuffer &mbuff, unsigned timeout=MP_WAIT_FOREVER);

public:
    void init(ICommunicator *_comm, rank_t _rank,mptag_t _tag);
    bool Connect(unsigned _part, unsigned _numnodes);
    void StartGather();
    void GetGatherInfo(rowmap_t &numlocal, memsize_t &totalsize, unsigned &overflowscale, bool hasserializer);
    rowmap_t GetMinMax(size32_t &keybuffsize,void *&keybuff, size32_t &avrecsizesize);
    bool GetMidPoint     (size32_t lkeysize, const byte * lkey, size32_t hkeysize, const byte * hkey, size32_t &mkeysize, byte * &mkey);
    void GetMultiMidPoint(size32_t lkeybuffsize, const void * lkeybuff, size32_t hkeybuffsize, const void * hkeybuff, size32_t &mkeybuffsize, void * &mkeybuf);
    void GetMultiMidPointStart(size32_t lkeybuffsize, const void * lkeybuff, size32_t hkeybuffsize, const void * hkeybuff); /* async */
    void GetMultiMidPointStop(size32_t &mkeybuffsize, void * &mkeybuf);
    rowmap_t SingleBinChop(size32_t keysize,const byte *key,byte cmpfn);
    void MultiBinChop(size32_t keybuffsize,const byte *keybuff, unsigned num,rowmap_t *pos,byte cmpfn,bool useaux);
    void MultiBinChopStart(size32_t keybuffsize,const byte *keybuff, byte cmpfn); /* async */
    void MultiBinChopStop(unsigned num,rowmap_t *pos);
    void OverflowAdjustMapStart( unsigned mapsize,rowmap_t *map,size32_t keybuffsize,const byte *keybuff, byte cmpfn,bool useaux); /* async */
    rowmap_t OverflowAdjustMapStop( unsigned mapsize, rowmap_t *map);
    void MultiMerge(unsigned mapsize,rowmap_t *map,unsigned num,SocketEndpoint* endpoints); /* async */
    void MultiMergeBetween(unsigned mapsize,rowmap_t *map,rowmap_t *mapupper,unsigned num,SocketEndpoint* endpoints); /* async */
    void SingleMerge(); /* async */
    bool FirstRowOfFile(const char *filename,size32_t &rowbuffsize, byte * &rowbuf);
    void GetMultiNthRow(unsigned numsplits,size32_t &mkeybuffsize, void * &mkeybuf);                
    virtual void StartMiniSort(rowcount_t totalrows); /* async */
    void Close(); /* async */
    void CloseWait();
    void Disconnect(); /* async */

    static bool marshall(ISortSlaveMP &slave, ICommunicator *comm, mptag_t tag);  // called slave side
    static void stopmarshall(ICommunicator *comm,mptag_t tag);                   // called slave side

};


#endif
