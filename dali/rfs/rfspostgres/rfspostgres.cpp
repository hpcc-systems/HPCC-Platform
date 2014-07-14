// POSTGRES RFS Gateway

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

static char *_itoa(unsigned long n, char *str, int b)
{
    char *s = str;
    bool sign = false;
    if (n<0) {
        n = -n;
        sign = true;
    }
    do {
        char d = n % b;
        *(s++) = d+((d<10)?'0':('a'-10));
    }
    while ((n /= b) > 0);
    if (sign)
        *(s++) = '-';
    *s = '\0';
    // reverse
    char *s2 = str;
    s--;
    while (s2<s) {
        char tc = *s2;
        *(s2++) = *s;
        *(s--) = tc;
    }
    return str;
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

#include <libpq-fe.h>

#include "rfs.h"


static char *tempdir=NULL;

static int createTempFile(RFS_ServerBase &base,char *&name)
{
    free(name);
#ifdef _WIN32
    name = _tempnam(tempdir,"rfspgtmp");
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
    strcat(name,"rfspg_XXXXXX");
    int ret = mkstemp(name);
#endif
    if (ret==-1) {
        free(name);
        name = NULL;
        base.throwError(errno,"Creating temp file");
    }
    return ret;
}

#define RFSPOSTGRESERR_BASE                         8300

static void PostgresError(RFS_ServerBase &base,PGconn *pgconn,int errcode)
{
    static char errstr[8192];
    char errnum[16];
    strcpy(errstr,"Postgres Error: ");
    const char *err = PQerrorMessage(pgconn);
    if (!err||!*err) {
        if (errcode) {
            _itoa(errcode,errnum,10);
            err = errnum;
        }
        else
            err = "Unspecified";
    }
    strcat(errstr,err);
    base.throwError(RFSPOSTGRESERR_BASE,errstr,false);
}


class CPostgresRFSconn: public RFS_ConnectionBase
{
    RFS_ServerBase &base;
    char * query;
    byte querymode;
    bool eos;
    PGconn *pgconn;
    PGresult *res;
    rfs_fpos_t lastpos;
    unsigned long *lengths;
    unsigned numfields;
    unsigned numrows;
    unsigned currow;
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

    void postgresError(int err)
    {
        ::PostgresError(base,pgconn,err);
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
                out.appends("COPY ");
                out.appends(tablename);
                out.appends(" FROM STDIN WITH CSV QUOTE AS '\''");
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

    bool getresult()
    {
        if (!query)
            return false;
        if (res)
            return (numrows!=0);
        if (base.debugLevel())
            base.log("Query: '%s'",query);
        numfields = 0;
        numrows = 0;
        currow = 0;
        res = PQexec(pgconn, query);        // NB returns all results (i.e. only the last statement can return non-empty)
        ExecStatusType status = PQresultStatus(res);
        if (status == PGRES_TUPLES_OK) {
            if (res) {
                numfields = PQnfields(res);
                if (numfields)
                    numrows = PQntuples(res);
            }
            status = PGRES_COMMAND_OK;
        }
        if (res&&!numrows) {
            PQclear(res);
            res = NULL;
        }
        if (status !=  PGRES_COMMAND_OK)
            postgresError((int)status);
        return numrows!=0;
    }


    void writeQuery()
    {
        if (!query||!savesize||(tempfile==-1))
            return;
        _close(tempfile);
        freopen(tempfilename, "rb", stdin);
        if (base.debugLevel())
            base.log("Query(w): '%s'",query);
        res = PQexec(pgconn, query);        // NB returns all results (i.e. only the last statement can return non-empty)
        ExecStatusType status = PQresultStatus(res);
        if (status == PGRES_TUPLES_OK)
            status = PGRES_COMMAND_OK;      // no return
        PQclear(res);
        res = NULL;
        fclose(stdin);                      // should reopen?
        if (status !=  PGRES_COMMAND_OK)
            postgresError((int)status);
    }


public:
    CPostgresRFSconn(RFS_ServerBase &_base,PGconn *_pgconn)
        : base(_base)
    {
        pgconn = _pgconn;
        res = NULL;
        lastpos = 0;
        numfields = 0;
        numrows = 0;
        currow = 0;
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
    ~CPostgresRFSconn()
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
        return getresult();
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
        if (pos==0) {
            currow = 0;
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
        while (currow<numrows) {
            for (unsigned f=0;f<numfields;f++) {
                char * val = PQgetvalue(res, currow, f);
                if (val)
                    csvwriter.putField(strlen(val),val);
            }
            currow++;
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
        rfs_fpos_t pos = 0;
        while (1) {
            size_t rd = 0;
            read(pos,0x10000000,rd,NULL);
            if (!rd)
                break;
            pos+=rd;
            if (rd<0x10000000)
                break;
        }
        currow = 0;
        eos = false;
        // we could reopen tempfile for read here
        lastpos = 0;
        savesize = pos;
        return pos;
    }

    void close(bool closetmp)
    {
        if(res) {
            PQclear(res);
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


class CPostgresRFS: public RFS_ServerBase
{
    PGconn *pgconn;
    unsigned lastping;
    char *conninfo;

public:


    CPostgresRFS()
    {
        tempdir = NULL;
        lastping = (unsigned)time(NULL);
        pgconn = NULL;
        conninfo = NULL;
    }

    ~CPostgresRFS()
    {
        if (pgconn)
            PQfinish(pgconn);
        free(tempdir);
        free(conninfo);
    }

    virtual RFS_ConnectionBase * open(const char *name, byte mode, byte share)
    {
        if (lastping-(unsigned)time(NULL)>60*30) {      // ping every 30m
            // ping TBD
            lastping = (unsigned)time(NULL);
        }

        CPostgresRFSconn *conn = new CPostgresRFSconn(*this,pgconn);
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
        // TBD table list
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
        assert(programnamemax>sizeof("rfspg")+1);
        strcpy(programname,"rfspg");
        version = 1;
    }

    void param(RFS_SimpleString &out,const char *name,const char *val)
    {
        if (val&&*val) {
            out.appends(name);
            out.appends(" = '");
            out.appends(val);
            out.appends("' ");
        }
    }

    int run(const char *server,const char *user,const char *password,const char *db, const char *pgport, const char *pgopt, char *_tempdir)
    {
        if (_tempdir[0])
            tempdir = _strdup(_tempdir);

        RFS_SimpleString out;
        param(out,"hostaddr",server);
        param(out,"port",pgport);
        param(out,"dbname",db);
        param(out,"user",user);
        param(out,"password",password);
        out.appends(pgopt);
        out.trim();
        conninfo = out.dup();
        out.clear();
        pgconn = PQconnectdb(conninfo);
        if (PQstatus(pgconn) != CONNECTION_OK) {
            fprintf(stderr, "Connection to database failed: %s", PQerrorMessage(pgconn));
            return 1;
        }
        return RFS_ServerBase::run();
    }

};



void usage()
{
    printf("rfspg --port=<port>\n");
    printf("          --pgserver=<postgresep> --pgport=<port>\n");
    printf("          --user=<username>  --password=<password>\n");
    printf("          --db=<database>\n");
    printf("          --pgopt=<extra-params> -- for specifying other server options\n");
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
    CPostgresRFS rfsserver;
    if (!rfsserver.init(argc,argv)) {
        usage();
        return 1;
    }
    char server[256];
    char user[256];
    char password[256];
    char db[256];
    char pgport[32];
    char pgopt[1024];
    char tempdir[256];
    strcpy(server,"localhost");
    user[0] = 0;
    password[0] = 0;
    pgport[0] = 0;
    pgopt[0] = 0;
    db[0] = 0;
    tempdir[0] = 0;
    for (int i=1;i<argc;i++) {
        if (checkparam(argv[i],"pgserver",server,sizeof(server))) continue;
        if (checkparam(argv[i],"user",user,sizeof(user))) continue;
        if (checkparam(argv[i],"password",password,sizeof(password))) continue;
        if (checkparam(argv[i],"db",db,sizeof(db))) continue;
        if (checkparam(argv[i],"pgport",pgport,sizeof(pgport))) continue;
        if (checkparam(argv[i],"pgopt",pgport,sizeof(pgopt))) continue;
        if (checkparam(argv[i],"tempdir",tempdir,sizeof(tempdir))) continue;
    }
    if (!db) {
        usage();
        return 0;
    }
    return rfsserver.run(server,user,password,db,pgport,pgopt,tempdir);
}
