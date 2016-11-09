#ifndef PACKETSTORE_HPP
#define PACKETSTORE_HPP


#ifndef sa_decl
#define sa_decl DECL_IMPORT
#endif

#include "jlib.hpp"

typedef __int64 pkt_edition_t;


interface IPacketStoreEnumerator
{
    virtual bool first() = 0;
    virtual bool next() = 0;
    virtual bool isValid() = 0;
    virtual const char *queryId() =0;           // id
};


interface IPacketStoreValueEnumerator: extends IPacketStoreEnumerator
{
    virtual pkt_edition_t queryEdition()=0;                 // edition (at time of enumerator creation);
    virtual MemoryBuffer &getValue(MemoryBuffer &mb)=0;     // value (at time of enumerator creation);
};




interface IPacketStore: extends IInterface
{
    virtual bool put(const char *id, size32_t packetsize, const void *packetdata, unsigned timeout)=0;
    virtual bool multiPut(unsigned count, const char **id, const size32_t *packetsize, const void **packetdata, unsigned timeout)=0;
    virtual bool get(const char *id, MemoryBuffer &mb, unsigned timeout)=0;
    virtual bool multiGet(unsigned count, const char **id, MemoryBuffer &mb, size32_t *retsizes, size32_t *retoffsets, unsigned timeout)=0;
    virtual bool remove(const char *id, unsigned timeout)=0;
    virtual bool rename(const char *from, const char *to, unsigned timeout)=0;
    virtual bool copy(const char *from, const char *to, unsigned timeout)=0;

    virtual pkt_edition_t getEdition(const char *id)=0;
    virtual pkt_edition_t getChanged(const char *id, pkt_edition_t lastedition, MemoryBuffer &mb, unsigned timeout)=0;
    virtual IPacketStoreEnumerator * getEnumerator(const char *mask)=0;         // list of ids
    virtual IPacketStoreValueEnumerator * getValueEnumerator(const char *mask)=0;   // loads values as well
};


extern sa_decl IPacketStore * connectPacketStore(IGroup *psgroup);


extern sa_decl void runPacketStoreServer(IGroup *grp);      // server side

#endif






