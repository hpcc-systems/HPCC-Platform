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

// MYSQL RFS Gateway

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#ifndef _CRT_SECURE_CPP_OVERLOAD_STANDARD_NAMES
#define _CRT_SECURE_CPP_OVERLOAD_STANDARD_NAMES 1
#undef _CRT_SECURE_NO_WARNINGS
#define _CRT_SECURE_NO_WARNINGS 1
#endif
#undef UNICODE
#include <windows.h>
#include <io.h>
#include <sys/utime.h>
#include <sys/types.h>
#include <ctype.h>

#else
#include <sys/types.h>
#include <unistd.h>
#define _strdup strdup
#define _O_RDONLY O_RDONLY
#define _O_WRONLY O_WRONLY
#define _O_RDWR O_RDWR
#define _O_CREAT O_CREAT
#define _O_TRUNC O_TRUNC
#define _O_BINARY (0)
#define _open ::open
#define _read ::read
#define _write ::write
#define _lseek ::lseek
#define _close ::close
#define _unlink unlink
#define _tempnam tempnam
#include <ctype.h>

static int _memicmp (const void *s1, const void *s2, size_t len)
{
    const unsigned char *b1 = (const unsigned char *)s1;
    const unsigned char *b2 = (const unsigned char *)s2;
    int ret = 0;
    while (len&&((ret = tolower(*b1)-tolower(*b2)) == 0)) {
        b1++;
        b2++;
        len--;
    }
    return ret;
}

#endif
#if defined(_M_X64) || defined ( __x86_64) || \
    defined(__aarch64__) || __WORDSIZE==64
#define __64BIT__
typedef unsigned long memsize_t;
#else
typedef unsigned memsize_t;
#endif


#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <errno.h>
#include <assert.h>
#include <time.h>
#include <fcntl.h>
#include <sys/stat.h>


typedef unsigned int    u_int;
#ifdef SOCKET
#undef SOCKET
#endif
typedef u_int  SOCKET;
#include <mysql.h>

#include "rfs.h"

static char *tempdir=NULL;

static int createTempFile(RFS_ServerBase &base,char *&name)
{
    free(name);
#ifdef _WIN32
    name = _tempnam(tempdir,"rfsmysqltmp");
    int ret = _open(name, _O_RDWR | _O_CREAT | _O_BINARY, _S_IREAD|_S_IWRITE);
#else
    size_t ds = tempdir?strlen(tempdir):0;
    name = (char *)malloc(ds+32);
    if (ds) {
        memcpy(name,tempdir,ds);
        if (tempdir[ds-1]!='/')
            tempdir[ds++] = '/';
    }
    else
        *name = 0;
    strcat(name,"rfsmysqltmp_XXXXXX");
    int ret = mkstemp(name);
#endif
    if (ret==-1) {
        printf("name = '%s'\n",name);
        free(name);
        name = NULL;
        base.throwError(errno,"Creating temp file");
    }
    return ret;
}

#define RFSMYSQLERR_BASE                            8200

static void mySqlError(RFS_ServerBase &base,MYSQL &mysql)
{
    static char errstr[8192];
    strcpy(errstr,"MYSQL Error: ");
    const char *err = mysql_error(&mysql);
    if (!err||!*err)
        err = "NONE";
    strcat(errstr,err);
    base.throwError(RFSMYSQLERR_BASE,errstr,false);
}

class CMySqlRFSconn: public RFS_ConnectionBase
{
    RFS_ServerBase &base;
    char * query;
    byte querymode;
    bool eos;
    MYSQL &mysql;
    MYSQL_RES *res;
    MYSQL_ROW row;
    MYSQL_ROW_OFFSET startrow;
    rfs_fpos_t lastpos;
    unsigned long *lengths;
    unsigned numfields;
    RFS_CSVwriter csvwriter;
    RFS_CSVreader csvreader;
    bool needlocalinput;
    bool multistatement;
    int tempfile;
    char * tempfilename;
    bool readok;
    bool writeok;
    rfs_fpos_t savesize;
    int localerr;


    int localread(char *buf, unsigned int buf_len)
    {
        if (lastpos>=savesize)
            return 0;
        long ret = (long)_lseek(tempfile,(long)lastpos,SEEK_SET);
        if (ret!=lastpos) {
            localerr = errno;
            return -1;
        }
        int rd = _read(tempfile,buf,buf_len);
        if (rd==-1) {
            localerr = errno;
            return -1;
        }
        lastpos += rd;
        return rd;
    }

    int localerror(char *error_msg, unsigned int error_msg_len)
    {
        if (!error_msg_len)
            return localerr;
        const char *err = NULL;
        if (localerr)
            err = strerror(localerr);
        if (!err)
            err = "Error";
        size_t l = strlen(err);
        if (l+1>error_msg_len)
            l = error_msg_len-1;
        memcpy(error_msg,err,l);
        error_msg[l] = 0;
        return localerr;
    }


    static int local_infile_init(void **ptr, const char *filename, void *userdata)
    {
        *ptr = userdata;                                // we are non reentrant so this should suffice
        return 0;
    }

    static int local_infile_read(void *ptr, char *buf, unsigned int buf_len)
    {
        return ((CMySqlRFSconn *)ptr)->localread(buf,buf_len);
    }

    static void local_infile_end(void *ptr)
    {
        // nothing to do here
    }

    static int local_infile_error(void *ptr, char *error_msg, unsigned int error_msg_len)
    {
        return ((CMySqlRFSconn *)ptr)->localerror(error_msg, error_msg_len);
    }



    void mySqlError()
    {
        ::mySqlError(base,mysql);
        free(query);
        query = NULL;
    }


    void transformQuery(const char *in, bool forwrite)
    {
        if (*in=='/')       // added by client
            in++;
        if (*in=='>')       // added by client
            in++;
        free(query);
        query = NULL;
        needlocalinput = false;
        multistatement = false;
        bool istable = true;
        if (!in)
            return;
        query = NULL;
        RFS_SimpleString out;
        // NB only the first INFILE is transformed (deliberately)
        bool quoting = false;
        while (isspace((unsigned char)*in))
            in++;
        while (*in) {
            char c = *(in++);
            if (c=='\\') {
                out.appendc(c);
                c = *(in++);
            }
            else if (c=='\'') {
                quoting = !quoting;
            }
            else if (!quoting) {
                if (c==';') {
                    unsigned i = 1;
                    while (isspace((unsigned char)in[i])||(in[i]==';'))
                        i++;
                    if (!in[i])
                        break;      // e.g. trailing ;
                    multistatement = true;
                }
                if (c==' ') {
                    if (!needlocalinput&&(_memicmp(in,"INFILE []",9)==0)) {
                        out.appends(" INFILE '#'");
                        in+=9;
                        needlocalinput = true;
                        continue;
                    }
                }
            }
            if (istable&&!isalnum((unsigned char)c)&&(c!='_')&&(c!='$'))
                istable = false;
            out.appendc(c);
        }
        out.trim();
        if (istable) {
            char *tablename = out.dup();
            out.clear();
            if (forwrite) {
                out.appends("LOAD DATA LOCAL INFILE '#' INTO TABLE ");
                out.appends(tablename);
                out.appends(" FIELDS TERMINATED BY ',' ENCLOSED BY '\\''");
            }
            else {
                out.appends("SELECT * FROM ");
                out.appends(tablename);
            }
        }
        if (multistatement&&needlocalinput)
            base.throwError(EACCES,"Multi-statement not allowed here");
        query = out.dup();
    }

    bool storeresults()
    {
        numfields = 0;
        while (1) { // this seems bit OTT
            res = mysql_store_result(&mysql);
            if (res) {
                startrow = mysql_row_tell(res);
                numfields = mysql_num_fields(res);
                if (numfields)
                    break;
                mysql_free_result(res);
                res = NULL;
            }
            int r = mysql_next_result(&mysql);
            if (r>0)
                mySqlError();
            if (r!=0)
                break;
        }
        return numfields!=0;
    }

    bool firstresult()
    {
        if (!query)
            return false;
        if (res)
            return numfields!=0;
        if (base.debugLevel())
            base.log("Query: '%s'",query);
        if (mysql_real_query(&mysql, query, strlen(query)))
            mySqlError();
        return storeresults();
    }

    bool nextresult()
    {
        if (!query||!multistatement)    // NB don't close in single statement
            return false;
        close(false);
        if (mysql_next_result(&mysql)!=0) { // see if more results
            free(query);
            query = NULL;
            return false;
        }
        return storeresults();
    }

    void writeQuery()
    {
        if (!query||!savesize||(tempfile==-1))
            return;
        lastpos = 0;
        localerr = 0;
        mysql_set_local_infile_handler(&mysql, local_infile_init, local_infile_read, local_infile_end, local_infile_error, this);
        if (mysql_real_query(&mysql, query, strlen(query))==0) {
            // I *think* this should execute all the multi statements
        }
        else {
            mysql_set_local_infile_default(&mysql);
            mySqlError();
            return;
        }
        mysql_set_local_infile_default(&mysql);
        free(query);
        query = NULL;
    }


    bool nextrow()
    {
        do {
            row = mysql_fetch_row(res);
            if (row) {
                lengths = mysql_fetch_lengths(res);
                if (lengths)
                    return true;
            }
        } while (nextresult());
        return false;
    }

public:
    CMySqlRFSconn(RFS_ServerBase &_base,MYSQL &_mysql)
        : base(_base), mysql(_mysql)
    {
        res = NULL;
        lastpos = 0;
        numfields = 0;
        query = NULL;
        readok = false;
        writeok = false;
        needlocalinput = false;
        multistatement = false;
        eos = false;
        tempfilename = NULL;
        tempfile = -1;
        savesize = (rfs_fpos_t)-1;
    }
    ~CMySqlRFSconn()
    {
        close(true);
    }



    bool openRead(const char *_query)
    {
        transformQuery(_query,false);
        close(true);
        eos = false;
        readok = true;
        writeok = false;
        savesize = (rfs_fpos_t)-1;
        return firstresult();
    }

    bool openWrite(const char *_query)
    {
        transformQuery(_query,true);
        close(true);
        eos = false;
        readok = false;
        writeok = true;
        savesize = (rfs_fpos_t)-1;
        return true;
    }



    void read(rfs_fpos_t pos, size_t len, size_t &outlen, void *out)
    {
        if (!readok)
            base.throwError(errno,"invalid mode for read");
        outlen = 0;
        if (tempfile!=-1) {
            if (out==NULL) {
                long length = (long)_lseek(tempfile,0,SEEK_END);
                if (length==-1)
                    base.throwError(errno,"read size.1");
                if ((long)pos>length)
                    outlen = 0;
                else if (length-(long)pos>(long)len)
                    outlen = len;
                else
                    outlen = (size_t)(length-(long)pos);
                return;
            }
            long ret = (long)_lseek(tempfile,(long)pos,SEEK_SET);
            if (ret!=pos)
                base.throwError(errno,"read.1");
            int rd = _read(tempfile,out,len);
            if (rd==-1)
                base.throwError(errno,"read.2");
            outlen = (size_t)rd;
            return;
        }
        if (pos==0) {
            row = NULL;
            csvwriter.rewrite();
            eos = false;
            lastpos = 0;
        }
        if (pos!=lastpos)
            base.throwError(EACCES,"Out of order read");
        if (eos||(csvwriter.length()>=len)) {
            if (len>csvwriter.length())
                len = csvwriter.length();
            if (out)
                memcpy(out,csvwriter.base(),len);
            csvwriter.consume(len);
            outlen = len;
            lastpos += len;
            return;
        }
        while (nextrow()) {
            for (unsigned f=0;f<numfields;f++)
                csvwriter.putField(lengths[f],row[f]);
            csvwriter.putRow();
            if (csvwriter.length()>=len) {
                if (out)
                    memcpy(out,csvwriter.base(),len);
                csvwriter.consume(len);
                outlen = len;
                lastpos += len;
                return;
            }
        }
        eos = true;
        outlen = csvwriter.length();
        if (outlen>len)
            outlen = len;
        if (out)
            memcpy(out,csvwriter.base(),outlen);
        csvwriter.consume(outlen);
        lastpos += outlen;
        savesize = lastpos+csvwriter.length();
    }

    rfs_fpos_t size()
    {
        if (savesize!=(rfs_fpos_t)-1)
            return savesize;
        if (lastpos!=0)
            base.throwError(EACCES,"Getting size mid-read");
        // multi (and not prev saved) then save to a temporary file
        // bit of a shame but ...
        if (tempfile==-1) {
            rfs_fpos_t pos = 0;
            if (multistatement) {
                int newtempfile = createTempFile(base,tempfilename);
                struct cBuff
                {
                    void *b;
                    cBuff() { b = malloc(0x10000); }
                    ~cBuff() { free(b); }
                } buf;
                while (1) {
                    size_t rd = 0;
                    read(pos,0x10000,rd,buf.b);
                    if (!rd)
                        break;
                    int wr = _write(newtempfile,buf.b,rd);
                    if (wr==-1)
                        base.throwError(errno,"size write.1");
                    if (wr!=rd)  // disk full
                        base.throwError(ENOSPC,"size write.2");
                    pos+=rd;
                    if (rd<0x10000)
                        break;
                }
                tempfile = newtempfile;
            }
            else {
                while (1) {
                    size_t rd = 0;
                    read(pos,0x10000000,rd,NULL);
                    if (!rd)
                        break;
                    pos+=rd;
                    if (rd<0x10000000)
                        break;
                }
                mysql_row_seek(res,startrow);       // seek back to 0
            }
            eos = false;
            // we could reopen tempfile for read here
            lastpos = 0;
            savesize = pos;
            return pos;
        }
        long savedPos = (long)_lseek(tempfile,0,SEEK_CUR);
        if (savedPos == -1)
            base.throwError(errno,"size.1");
        long length = (long)_lseek(tempfile,0,SEEK_END);
        if (length==-1)
            base.throwError(errno,"size.2");
        if ((long)_lseek(tempfile,savedPos,SEEK_SET)!=savedPos)
            base.throwError(errno,"size.3");
        savesize = (rfs_fpos_t)length;
        return savesize;
    }

    void close(bool closetmp)
    {
        if(res) {
            mysql_free_result(res);
            res = NULL;
            numfields = 0;
        }
        if (writeok)
            writeQuery();
        if (closetmp) {
            if (tempfile!=-1)
                _close(tempfile);
            if (tempfilename)
                _unlink(tempfilename);
            free(tempfilename);
            tempfilename = NULL;
            tempfile = -1;
        }
    }

    void close()
    {
        close(true);
    }



    void write(rfs_fpos_t pos, size_t len, const void *in)
    {
        if (!writeok)
            base.throwError(EACCES,"invalid mode for write");
        if (tempfile==-1)
            tempfile = createTempFile(base,tempfilename);
        long ret = (long)_lseek(tempfile,(long)pos,SEEK_SET);
        if (ret!=pos)
            base.throwError(errno,"write.1");
        int wr = _write(tempfile,in,len);
        if (wr==-1)
            base.throwError(errno,"write.2");
        if (wr!=len)  // disk full
            base.throwError(ENOSPC,"write.3");
        if (pos+wr>savesize)
            savesize = pos+wr;
    }

};


class CMySqlRFS: public RFS_ServerBase
{
    MYSQL mysql;
    bool isopen;
    unsigned lastping;

public:


    CMySqlRFS()
    {
        if (mysql_library_init(0, NULL, NULL)) {
            fprintf(stderr, "could not initialize MySQL library\n");
            exit(1);
        }
        isopen = false;
        tempdir = NULL;
        lastping = (unsigned)time(NULL);
    }

    ~CMySqlRFS()
    {
        if (isopen)
            mysql_close(&mysql);
        mysql_library_end();
        free(tempdir);
    }

    virtual RFS_ConnectionBase * open(const char *name, byte mode, byte share)
    {
        if ((unsigned)time(NULL)-lastping>60*30) {      // ping every 30m
            if (mysql_ping(&mysql)) {
                mySqlError(*this,mysql);
            }
            lastping = (unsigned)time(NULL);
        }

        CMySqlRFSconn *conn = new CMySqlRFSconn(*this,mysql);
        if ((*name=='/')||(*name=='\\'))
            *name++;
        if ((mode&RFS_OPEN_MODE_MASK)==RFS_OPEN_MODE_READ) {
            if (conn->openRead(name))
                return conn;
        }
        else if ((mode&RFS_OPEN_MODE_MASK)==RFS_OPEN_MODE_CREATE) {
            if (conn->openWrite(name))
                return conn;
        }
        else
            throwError(EACCES,"Open mode not supported");
        // error TBD?
        delete conn;
        return NULL;
    }

    virtual void existFile(const char *filename, bool &existsout)
    {
        existsout = true;  // assume exists (query may fail but that will be an error)
    }
    virtual void removeFile(const char *filename)
    {
        // error TBD
    }
    virtual void renameFile(const char *fromname,const char *toname)
    {
        // error TBD
    }
    virtual void getFileTime(const char *filename, time_t &outaccessedtime, time_t &outcreatedtime, time_t &outmodifiedtime)
    {
        time(&outaccessedtime);
        outcreatedtime = outaccessedtime;
        outmodifiedtime = outaccessedtime;
        // bit odd that changes...
        // Alternative would be to keep a past query cache (i.e. when done) but that isn't really ideal.
    }
    virtual void setFileTime(const char *filename, time_t *outaccessedtime, time_t *outcreatedtime, time_t *outmodifiedtime) // params NULL if not to be set
    {
        // ignore
    }
    virtual void isFile(const char *filename, bool &outisfile)
    {
        outisfile = true; // pretend we are a file
    }
    virtual void isDir(const char *filename, bool &outisdir)
    {
        outisdir = true; // we aren't a directory
    }
    virtual void isReadOnly(const char *filename, bool &outisreadonly)
    {
        outisreadonly = true; // no update supported currently
    }
    virtual void setReadOnly(const char *filename, bool readonly)
    {
        // ignore
    }
    virtual void createDir(const char *dirname,bool &createdout)
    {
        // ignore
    }
    virtual void openDir(const char *dirname, const char *mask, bool recursive, bool includedir, void * &outhandle)
    {
        // TBD use mysql_list_tables()
        outhandle = NULL;
    }
    virtual void nextDirEntry(void * handle, size_t maxfilenamesize, char *outfilename, bool &isdir, rfs_fpos_t &filesizeout, time_t &outmodifiedtime)  // empty return for filename marks end
    {
        // TBD return table
        outfilename[0] = 0;
    }
    virtual void closeDir(void * handle)
    {
        // TBD
    }

    virtual void getVersion(size_t programnamemax, char *programname, short &version)
    {
        assert(programnamemax>sizeof("rfsmysql")+1);
        strcpy(programname,"rfsmysql");
        version = 1;
    }

    int run(const char *server,const char *user,const char *password,const char *db, int mysqlport,const char *mygroup,const char *myconf,char *_tempdir)
    {
        if (_tempdir[0])
            tempdir = _strdup(_tempdir);
        mysql_init(&mysql);
        if (mygroup[0])
            mysql_options(&mysql,MYSQL_READ_DEFAULT_GROUP,mygroup);
        if (myconf[0]) {
            if (!mysql_options(&mysql,MYSQL_READ_DEFAULT_FILE ,myconf))
                fprintf(stderr, "Failed read option file: Error: %s\n", mysql_error(&mysql));
        }
        my_bool enabled = 1;
        mysql_options(&mysql, MYSQL_OPT_RECONNECT, &enabled);
        mysql_options(&mysql, MYSQL_OPT_LOCAL_INFILE, &enabled);
        if (!mysql_real_connect(&mysql, server, user, password, db, mysqlport, 0, CLIENT_MULTI_STATEMENTS | CLIENT_MULTI_RESULTS)) {
            fprintf(stderr, "Failed to connect to database: Error: %s\n", mysql_error(&mysql));
            mysql_close(&mysql);
            return 1;
        }
        isopen = false;
        return RFS_ServerBase::run();
    }

};



void usage()
{
    printf("rfsmysql --port=<port>\n");
    printf("          --mysqlserver=<mysqlep> --mysqlport=<port>\n");
    printf("          --user=<username>  --password=<password>\n");
    printf("          --db=<database>\n");
    printf("          --mygroup=<groupname>  -- for specifying server option group\n");
    printf("          --myconfig=<filename>  -- for specifying server option file\n");
    printf("          --tempdir=<dirname>    -- directory for temporary files\n");
}

bool checkparam(const char *param,const char *name,char *out,size_t size)
{
    if ((param[0]!='-')||(param[0]!='-'))
        return false;
    param+=2;
    if (strncmp(param,name,strlen(name))==0) {
        const char *v = param+strlen(name);
        if (*v=='=') {
            if (strlen(v+1)>size-1) {
                fprintf(stderr,"parameter %s to large (> %d chars)",param,size-1);
                exit(1);
            }
            strcpy(out,v+1);
            return true;
        }
    }
    return false;
}

int main(int argc, const char **argv)
{
#ifdef _WIN32
    // for windows service must be static (main returns)
    static
#endif
    CMySqlRFS rfsserver;
    if (!rfsserver.init(argc,argv)) {
        usage();
        return 1;
    }
    char server[256];
    char user[256];
    char password[256];
    char db[256];
    char mysqlport[32];
    char mygroup[128];
    char myconf[256];
    char tempdir[256];
    strcpy(server,"localhost");
    user[0] = 0;
    password[0] = 0;
    mysqlport[0] = 0;
    mygroup[0] = 0;
    myconf[0] = 0;
    db[0] = 0;
    tempdir[0] = 0;
    for (int i=1;i<argc;i++) {
        if (checkparam(argv[i],"mysqlserver",server,sizeof(server))) continue;
        if (checkparam(argv[i],"user",user,sizeof(user))) continue;
        if (checkparam(argv[i],"password",password,sizeof(password))) continue;
        if (checkparam(argv[i],"db",db,sizeof(db))) continue;
        if (checkparam(argv[i],"mysqlport",mysqlport,sizeof(mysqlport))) continue;
        if (checkparam(argv[i],"mygroup",mygroup,sizeof(mygroup))) continue;
        if (checkparam(argv[i],"myconf",myconf,sizeof(myconf))) continue;
        if (checkparam(argv[i],"tempdir",tempdir,sizeof(tempdir))) continue;
    }
    if (!db) {
        usage();
        return 0;
    }
    return rfsserver.run(server,user,password,db,atoi(mysqlport),mygroup,myconf,tempdir);
}
