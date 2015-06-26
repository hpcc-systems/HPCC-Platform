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
    virtual void GetGatherInfo(rowcount_t &numlocal, offset_t &totalsize, unsigned &overflowscale, bool hasserializer)=0;
    virtual rowcount_t GetMinMax(size32_t &keybuffsize,void *&keybuff, size32_t &avrecsizesize)=0;
    virtual void GetMultiMidPointStart(size32_t lkeybuffsize, const void * lkeybuff, size32_t hkeybuffsize, const void * hkeybuff)=0; /* async */
    virtual void GetMultiMidPointStop(size32_t &mkeybuffsize, void * &mkeybuf)=0;
    virtual void MultiBinChop(size32_t keybuffsize,const byte *keybuff, unsigned num,rowcount_t *pos,byte cmpfn)=0;
    virtual void MultiBinChopStart(size32_t keybuffsize,const byte *keybuff, byte cmpfn)=0; /* async */
    virtual void MultiBinChopStop(unsigned num,rowcount_t *pos)=0;
    virtual void OverflowAdjustMapStart(unsigned mapsize,rowcount_t *map,size32_t keybuffsize,const byte *keybuff, byte cmpfn, bool useaux)=0; /* async */
    virtual rowcount_t OverflowAdjustMapStop(unsigned mapsize, rowcount_t *map)=0;
    virtual void MultiMerge(unsigned mapsize,rowcount_t *map,unsigned num,SocketEndpoint* endpoints)=0; /* async */
    virtual void MultiMergeBetween(unsigned mapsize,rowcount_t *map,rowcount_t *mapupper,unsigned num,SocketEndpoint* endpoints)=0; /* async */
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
    void GetGatherInfo(rowcount_t &numlocal, offset_t &totalsize, unsigned &overflowscale, bool hasserializer);
    rowcount_t GetMinMax(size32_t &keybuffsize,void *&keybuff, size32_t &avrecsizesize);
    void GetMultiMidPointStart(size32_t lkeybuffsize, const void * lkeybuff, size32_t hkeybuffsize, const void * hkeybuff); /* async */
    void GetMultiMidPointStop(size32_t &mkeybuffsize, void * &mkeybuf);
    void MultiBinChop(size32_t keybuffsize,const byte *keybuff, unsigned num,rowcount_t *pos,byte cmpfn);
    void MultiBinChopStart(size32_t keybuffsize,const byte *keybuff, byte cmpfn); /* async */
    void MultiBinChopStop(unsigned num,rowcount_t *pos);
    void OverflowAdjustMapStart(unsigned mapsize,rowcount_t *map,size32_t keybuffsize,const byte *keybuff, byte cmpfn,bool useaux); /* async */
    rowcount_t OverflowAdjustMapStop(unsigned mapsize, rowcount_t *map);
    void MultiMerge(unsigned mapsize,rowcount_t *map,unsigned num,SocketEndpoint* endpoints); /* async */
    void MultiMergeBetween(unsigned mapsize,rowcount_t *map,rowcount_t *mapupper,unsigned num,SocketEndpoint* endpoints); /* async */
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
