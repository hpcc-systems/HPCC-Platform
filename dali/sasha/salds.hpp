#ifndef SALDS_HPP
#define SALDS_HPP

#include "jstring.hpp"

extern StringBuffer &getLdsPath(const char *relpath, StringBuffer & res);


// More to come....

interface ILdsConnection: extends IInterface
{
    virtual bool exists()=0;
    virtual bool remove()=0;
    virtual IFile &queryFile()=0;
    virtual offset_t size()=0;                          
    virtual bool get(MemoryBuffer &mb)=0;
    virtual bool get(size32_t sz,void *data,offset_t ofs=0)=0;
    virtual void put(const MemoryBuffer &mb)=0;
    virtual void put(size32_t sz,const void *data,offset_t ofs=0,bool truncate=true)=0;
    virtual bool rename(const char *name)=0;
    virtual const char *queryPath()=0;
};

typedef IIteratorOf<ILdsConnection> ILdsConnectionIterator;

interface ILargeDataStore
{
    virtual ILdsConnection *connect(const char *Ldspath)=0;
    virtual ILdsConnectionIterator *getIterator(const char *wildLdspath)=0;

};

ILargeDataStore &queryLargeDataStore();


#endif
