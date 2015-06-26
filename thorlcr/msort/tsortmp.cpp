#include "platform.h"
#include "jbuff.hpp"
#include "tsorts.hpp"


#include "tsortmp.hpp"

#ifdef _DEBUG
#define FULLTRACE
#endif

enum MPSlaveFunctions
{
    FN_Connect=100,
    FN_StartGather,
    FN_GetGatherInfo,
    FN_GetMinMax,
    FN_GetMultiMidPointStart,
    FN_GetMultiMidPointStop,
    FN_MultiBinChop,
    FN_MultiBinChopStart,
    FN_MultiBinChopStop,
    FN_OverflowAdjustMapStart,
    FN_OverflowAdjustMapStop,
    FN_MultiMerge,
    FN_MultiMergeBetween,
    FN_SingleMerge,
    FN_FirstRowOfFile,
    FN_GetMultiNthRow,
    FN_StartMiniSort,
    FN_Close,
    FN_CloseWait,
    FN_Disconnect
};

inline MemoryBuffer &serializeblk(MemoryBuffer &mb,size32_t bsize,const void *blk)
{
    return mb.append(bsize).append(bsize,blk);
}

inline MemoryBuffer &deserializeblk(MemoryBuffer &mb,size32_t &bsize,void *&blk)
{
    mb.read(bsize);
    assertex(bsize<0x80000000);
    blk = malloc(bsize);
    assertex(blk);
    return mb.read(bsize,blk);
}

inline MemoryBuffer &deserializeblk(MemoryBuffer &mb,size32_t &bsize,byte *&blk)
{
    void *blkout;
    MemoryBuffer &ret = deserializeblk(mb,bsize,blkout);
    blk = (byte *)blkout;
    return ret;
}

void SortSlaveMP::init(ICommunicator *_comm,rank_t _rank,mptag_t _tag)
{
    comm.set(_comm);
    rank = _rank;
    tag = _tag;
    comm->verifyConnection(rank);
}

bool SortSlaveMP::sendRecv(CMessageBuffer &mb, unsigned timeout)
{
    if (!comm->sendRecv(mb,rank,tag,timeout))
        return false;
    byte ok = 255;
    if (mb.length()) {
        mb.read(ok);
        if (ok==1)
            return true;
        if (ok==0) {
            int err;
            mb.read(err);
            StringAttr errstr;
            mb.read(errstr);
            throw MakeStringException(err, "%s", errstr.get());
        }
    }
    throw MakeStringException(-1,"SortSlaveMP::sendRecv() protocol error %d",(int)ok);
    return false;
}


bool SortSlaveMP::Connect(unsigned _part, unsigned _numnodes)
{
    CMessageBuffer mb;
    mb.append((byte)FN_Connect);
    mb.append(_part).append(_numnodes);
    sendRecv(mb);
    bool ret;
    mb.read(ret);
    return ret;
}

void SortSlaveMP::StartGather()
{
    CMessageBuffer mb;
    mb.append((byte)FN_StartGather);
    sendRecv(mb);
}

void SortSlaveMP::GetGatherInfo(rowcount_t &numlocal, offset_t &totalsize, unsigned &overflowscale, bool hasserializer)
{
    CMessageBuffer mb;
    mb.append((byte)FN_GetGatherInfo);
    mb.append(hasserializer);
    sendRecv(mb);
    mb.read(numlocal).read(totalsize).read(overflowscale);
}

rowcount_t SortSlaveMP::GetMinMax(size32_t &keybuffsize,void *&keybuff, size32_t &avrecsizesize)
{
    CMessageBuffer mb;
    mb.append((byte)FN_GetMinMax);
    sendRecv(mb);
    deserializeblk(mb,keybuffsize,keybuff);
    mb.read(avrecsizesize);
    rowcount_t ret;
    mb.read(ret);
    return ret;
}

void SortSlaveMP::GetMultiMidPointStart(size32_t lkeybuffsize, const void * lkeybuff, size32_t hkeybuffsize, const void * hkeybuff) /* async */
{
    CMessageBuffer mb;
    mb.append((byte)FN_GetMultiMidPointStart);
    serializeblk(mb,lkeybuffsize,lkeybuff);
    serializeblk(mb,hkeybuffsize,hkeybuff);
    sendRecv(mb);
}

void SortSlaveMP::GetMultiMidPointStop(size32_t &mkeybuffsize, void * &mkeybuf)
{
    CMessageBuffer mb;
    mb.append((byte)FN_GetMultiMidPointStop);
    sendRecv(mb);
    deserializeblk(mb,mkeybuffsize,mkeybuf);
}

void SortSlaveMP::MultiBinChop(size32_t keybuffsize,const byte *keybuff, unsigned num,rowcount_t *pos,byte cmpfn)
{
    CMessageBuffer mb;
    mb.append((byte)FN_MultiBinChop);
    serializeblk(mb,keybuffsize,keybuff).append(num).append(cmpfn);
    sendRecv(mb);
    mb.read(num*sizeof(rowcount_t),pos);
}

void SortSlaveMP::MultiBinChopStart(size32_t keybuffsize,const byte *keybuff, byte cmpfn) /* async */
{
    CMessageBuffer mb;
    mb.append((byte)FN_MultiBinChopStart);
    serializeblk(mb,keybuffsize,keybuff).append(cmpfn);
    sendRecv(mb);
}

void SortSlaveMP::MultiBinChopStop(unsigned num,rowcount_t *pos)
{
    CMessageBuffer mb;
    mb.append((byte)FN_MultiBinChopStop);
    mb.append(num);
    sendRecv(mb);
    mb.read(num*sizeof(rowcount_t),pos);
}

void SortSlaveMP::OverflowAdjustMapStart(unsigned mapsize,rowcount_t *map,size32_t keybuffsize,const byte *keybuff, byte cmpfn,bool useaux) /* async */
{
    CMessageBuffer mb;
    mb.append((byte)FN_OverflowAdjustMapStart);
    mb.append(mapsize).append(mapsize*sizeof(rowcount_t),map);
    serializeblk(mb,keybuffsize,keybuff).append(cmpfn).append(useaux);
    sendRecv(mb);

}

rowcount_t SortSlaveMP::OverflowAdjustMapStop( unsigned mapsize, rowcount_t *map)
{
    CMessageBuffer mb;
    mb.append((byte)FN_OverflowAdjustMapStop);
    mb.append(mapsize);
    sendRecv(mb);
    rowcount_t ret;
    mb.read(ret).read(mapsize*sizeof(rowcount_t),map);
    return ret;
}

void SortSlaveMP::MultiMerge(unsigned mapsize,rowcount_t *map,unsigned num,SocketEndpoint* endpoints) /* async */
{
    CMessageBuffer mb;
    mb.append((byte)FN_MultiMerge);
    mb.append(mapsize).append(mapsize*sizeof(rowcount_t),map);
    mb.append(num);
    while (num--) {
        endpoints->serialize(mb);
        endpoints++;
    }
    sendRecv(mb);
}


void SortSlaveMP::MultiMergeBetween(unsigned mapsize,rowcount_t *map,rowcount_t *mapupper,unsigned num,SocketEndpoint* endpoints) /* async */
{
    CMessageBuffer mb;
    mb.append((byte)FN_MultiMergeBetween);
    mb.append(mapsize).append(mapsize*sizeof(rowcount_t),map);
    mb.append(mapsize*sizeof(rowcount_t),mapupper);
    mb.append(num);
    while (num--) {
        endpoints->serialize(mb);
        endpoints++;
    }
    sendRecv(mb);
}


void SortSlaveMP::SingleMerge() /* async */
{
    CMessageBuffer mb;
    mb.append((byte)FN_SingleMerge);
    sendRecv(mb);
}

bool SortSlaveMP::FirstRowOfFile(const char *filename,size32_t &rowbuffsize, byte * &rowbuf)
{
    CMessageBuffer mb;
    mb.append((byte)FN_FirstRowOfFile);
    mb.append(filename);
    sendRecv(mb);
    deserializeblk(mb,rowbuffsize,rowbuf);
    bool ret;
    mb.read(ret);
    return ret;
}

void SortSlaveMP::GetMultiNthRow(unsigned numsplits,size32_t &mkeybuffsize, void * &mkeybuf)
{
    CMessageBuffer mb;
    mb.append((byte)FN_GetMultiNthRow);
    mb.append(numsplits);
    sendRecv(mb);
    deserializeblk(mb,mkeybuffsize,mkeybuf);

}

void SortSlaveMP::StartMiniSort(rowcount_t _totalrows) /* async */
{
    CMessageBuffer mb;
    mb.append((byte)FN_StartMiniSort);
    mb.append(_totalrows);
    sendRecv(mb);
}

void SortSlaveMP::Close() /* async */
{
    CMessageBuffer mb;
    mb.append((byte)FN_Close);
    sendRecv(mb);
}

void SortSlaveMP::CloseWait()
{
    CMessageBuffer mb;
    mb.append((byte)FN_CloseWait);
    sendRecv(mb);
}

void SortSlaveMP::Disconnect() /* async */
{
    CMessageBuffer mb;
    mb.append((byte)FN_Disconnect);
    sendRecv(mb);
}

bool SortSlaveMP::marshall(ISortSlaveMP &slave, ICommunicator* comm, mptag_t tag)
{
    CMessageBuffer mb;
    rank_t sender;
    comm->recv(mb,0,tag,&sender);       // NB only recv from master
    if (mb.length()==0) {
        PROGLOG("Stopping SortSlaveMP::marshall");
        return false;
    }
    byte fn;
    mb.read(fn);
    CMessageBuffer mbout;
    mbout.init(mb.getSender(),tag,mb.getReplyTag());
    byte okout=1;
    mbout.append(okout);
#ifdef FULLTRACE
    StringBuffer tmp1;
    PROGLOG(">SortSlaveMP::marshall(%d) got %d from %s tag %d replytag %d",(int)fn, mb.length(), mb.getSender().getUrlStr(tmp1).str(),tag,mb.getReplyTag());
#endif
    bool replydone = false;
    Owned<IException> err;
    try {
        switch ((MPSlaveFunctions)(int)fn) {
            case FN_Connect: {
                unsigned _part;
                unsigned _numnodes;
                mb.read(_part).read(_numnodes);
                bool ret = slave.Connect(_part,_numnodes);
                mbout.append(ret);
            }
            break;
            case FN_StartGather: {
                slave.StartGather();
            }
            break;
            case FN_GetGatherInfo: {
                bool hasserializer;
                mb.read(hasserializer);
                rowcount_t numlocal;
                unsigned overflowscale;
                offset_t totalsize;
                slave.GetGatherInfo(numlocal,totalsize,overflowscale,hasserializer);
                mbout.append(numlocal).append(totalsize).append(overflowscale);
            }
            break;
            case FN_GetMinMax: {
                size32_t keybuffsize;
                void *keybuff;
                size32_t avrecsize;
                rowcount_t ret = slave.GetMinMax(keybuffsize,keybuff,avrecsize);
                serializeblk(mbout,keybuffsize,keybuff).append(avrecsize).append(ret);
                free(keybuff);
            }
            break;
            case FN_GetMultiMidPointStart: {
                replydone = true;
                comm->reply(mbout);
                size32_t lkeybuffsize;
                void * lkeybuff;
                size32_t hkeybuffsize;
                void * hkeybuff;
                deserializeblk(mb,lkeybuffsize,lkeybuff);
                deserializeblk(mb,hkeybuffsize,hkeybuff);
                slave.GetMultiMidPointStart(lkeybuffsize,lkeybuff,hkeybuffsize,hkeybuff);
                free(lkeybuff);
                free(hkeybuff);
            }
            break;
            case FN_MultiBinChopStop: {
                unsigned num;
                mb.read(num);
                void *out = mbout.reserveTruncate(num*sizeof(rowcount_t));
                slave.MultiBinChopStop(num,(rowcount_t *)out);
            }
            break;
            case FN_GetMultiMidPointStop: {
                size32_t mkeybuffsize=0;
                void * mkeybuff = NULL;
                slave.GetMultiMidPointStop(mkeybuffsize,mkeybuff);
                serializeblk(mbout,mkeybuffsize,mkeybuff);
                free(mkeybuff);
            }
            break;
            case FN_MultiBinChopStart: {
                replydone = true;
                comm->reply(mbout);
                size32_t keybuffsize;
                void * keybuff;
                deserializeblk(mb,keybuffsize,keybuff);
                byte cmpfn;
                mb.read(cmpfn);
                slave.MultiBinChopStart(keybuffsize,(const byte *)keybuff,cmpfn);
                free(keybuff);
            }
            break;
            case FN_MultiBinChop: {
                size32_t keybuffsize;
                void * keybuff;
                deserializeblk(mb,keybuffsize,keybuff);
                unsigned num;
                byte cmpfn;
                mb.read(num).read(cmpfn);
                void *out = mbout.reserveTruncate(num*sizeof(rowcount_t));
                slave.MultiBinChop(keybuffsize,(const byte *)keybuff,num,(rowcount_t *)out,cmpfn);
                free(keybuff);
            }
            break;
            case FN_OverflowAdjustMapStart: {
                replydone = true;
                comm->reply(mbout);
                unsigned mapsize;
                mb.read(mapsize);
                const void * map = mb.readDirect(mapsize*sizeof(rowcount_t));
                size32_t keybuffsize;
                void * keybuff;
                deserializeblk(mb,keybuffsize,keybuff);
                byte cmpfn;
                mb.read(cmpfn);
                bool useaux;
                mb.read(useaux);
                slave.OverflowAdjustMapStart(mapsize,(rowcount_t *)map,keybuffsize,(const byte *)keybuff,cmpfn,useaux);
                free(keybuff);
            }
            break;
            case FN_OverflowAdjustMapStop: {
                unsigned mapsize;
                mb.read(mapsize);
                rowcount_t ret=0;
                size32_t retofs = mbout.length();
                mbout.append(ret);
                void *map=mbout.reserveTruncate(mapsize*sizeof(rowcount_t));
                ret = slave.OverflowAdjustMapStop(mapsize,(rowcount_t *)map);     // could avoid copy here if passed mb
                mbout.writeDirect(retofs,sizeof(ret),&ret);
            }
            break;
            case FN_MultiMerge: {
                replydone = true;
                comm->reply(mbout);
                unsigned mapsize;
                mb.read(mapsize);
                const void *map = mb.readDirect(mapsize*sizeof(rowcount_t));
                unsigned num;
                mb.read(num);
                SocketEndpointArray epa;
                for (unsigned i=0;i<num;i++) {
                    SocketEndpoint ep;
                    ep.deserialize(mb);
                    epa.append(ep);
                }
                slave.MultiMerge(mapsize,(rowcount_t *)map,num,epa.getArray());
            }
            break;
            case FN_MultiMergeBetween: {
                replydone = true;
                comm->reply(mbout);
                unsigned mapsize;
                mb.read(mapsize);
                const void *map = mb.readDirect(mapsize*sizeof(rowcount_t));
                const void *mapupper = mb.readDirect(mapsize*sizeof(rowcount_t));
                unsigned num;
                mb.read(num);
                SocketEndpointArray epa;
                for (unsigned i=0;i<num;i++) {
                    SocketEndpoint ep;
                    ep.deserialize(mb);
                    epa.append(ep);
                }
                slave.MultiMergeBetween(mapsize,(rowcount_t *)map,(rowcount_t *)mapupper,num,epa.getArray());
            }
            break;
            case FN_SingleMerge: {
                replydone = true;   
                comm->reply(mbout);     // async
                slave.SingleMerge();
            }
            break;
            case FN_FirstRowOfFile: {
                StringAttr filename;
                mb.read(filename);
                size32_t rowbufsize = 0;
                byte *rowbuf = NULL;
                bool ret = slave.FirstRowOfFile(filename,rowbufsize,rowbuf);
                serializeblk(mbout,rowbufsize,rowbuf);
                free(rowbuf);
                mbout.append(ret);
            }
            break;
            case FN_GetMultiNthRow: {
                unsigned numsplits;
                mb.read(numsplits);
                size32_t mkeybuffsize = 0;
                void * mkeybuf  = NULL;
                slave.GetMultiNthRow(numsplits,mkeybuffsize,mkeybuf);
                serializeblk(mbout,mkeybuffsize,mkeybuf);
                free(mkeybuf);
            }
            break;
            case FN_StartMiniSort: {
                replydone = true;   
                rowcount_t totalrows;
                mb.read(totalrows);
                comm->reply(mbout);     // async
                slave.StartMiniSort(totalrows);
            }
            break;
            case FN_Close: {
                replydone = true;   
                comm->reply(mbout);     // async
                slave.Close();
            }
            break;
            case FN_CloseWait: {
                slave.CloseWait();
            }
            break;
            case FN_Disconnect: {
                comm->reply(mbout);     // async
                replydone = true;   
                slave.Disconnect();
            }
            // fall through
            return false;
            default:
                throw MakeStringException(-1,"unknown function %d",(int)fn);
        }
    }
    catch (IException *e) {
        EXCLOG(e,"SortSlaveMP::marshall");
        if (!replydone)  {
            mbout.clear();
            okout = 0;
            mbout.append(okout);
            int err = e->errorCode();
            mbout.append(err);
            StringBuffer outs;
            e->errorMessage(outs);
            mbout.append(outs.str());
        }
        err.setown(e);
    }
    if (!replydone) {
#ifdef FULLTRACE
        StringBuffer tmp1;
        PROGLOG("<SortSlaveMP::marshall(%d) send %d to %s tag %d",(int)fn, mbout.length(), mbout.getSender().getUrlStr(tmp1).str(),mbout.getReplyTag());
#endif
        comm->reply(mbout);
    }
    if (err.get())
        throw err.getClear();
    return true;
}

void SortSlaveMP::stopmarshall(ICommunicator *comm,mptag_t tag)
{
    CMessageBuffer mbuff;
    comm->cancel(RANK_ALL,tag); 
}
