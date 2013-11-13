/*##############################################################################

HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems.

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

#include "ws_sqlService.hpp"
#include "exception_util.hpp"

void Cws_sqlEx::init(IPropertyTree *_cfg, const char *_process, const char *_service)
{
    cfg = _cfg;
    try
    {
        ECLFunctions::init();
    }
    catch (...)
    {
        throw MakeStringException(-1, "ws_sqlEx: Problem initiating ECLFunctions structure");
    }

    refreshValidClusters();
    m_envFactory.setown( getEnvironmentFactory() );
}

bool Cws_sqlEx::onEcho(IEspContext &context, IEspEchoRequest &req, IEspEchoResponse &resp)
{
    resp.setResponse(req.getRequest());
    return true;
}

bool Cws_sqlEx::onGetDBMetaData(IEspContext &context, IEspGetDBMetaDataRequest &req, IEspGetDBMetaDataResponse &resp)
{
    if (!context.validateFeatureAccess(WSSQLACCESS, SecAccess_Read, false))
        throw MakeStringException(-1, "Failed to fetch HPCC information. Permission denied.");

    bool success = false;
    StringBuffer username;
    context.getUserID(username);

    const char* passwd = context.queryPassword();

    bool includetables = req.getIncludeTables();
    if (includetables)
    {
        Owned<HPCCFileCache> tmpHPCCFileCache = HPCCFileCache::createFileCache(username.str(), passwd);
        tmpHPCCFileCache->populateTablesResponse(resp, req.getTableFilter());
        resp.setTableCount(resp.getTables().length());
    }

    bool includeStoredProcs = req.getIncludeStoredProcedures();
    if (includeStoredProcs)
    {
        const char * querysetfilter = req.getQuerySet();
        Owned<IStringIterator> targets = getTargetClusters(NULL, NULL);

        IArrayOf<IEspHPCCQuerySet> pquerysets;

        SCMStringBuffer target;
        ForEach(*targets)
        {
            const char *setname = targets->str(target).str();

            if ( querysetfilter && *querysetfilter && stricmp(setname, querysetfilter)!=0)
                continue;

            Owned<IEspHPCCQuerySet> pqset = createHPCCQuerySet();
            pqset->setName(setname);
            pquerysets.append(*pqset.getLink());

            Owned<IPropertyTree> settree = getQueryRegistry(setname, true);

            if (!settree)
               return false;

            IArrayOf<IEspPublishedQuery> queries;
            Owned<IPropertyTreeIterator> iter = settree->getElements("Query");
            ForEach(*iter)
            {

                const char * id = iter->query().queryProp("@id");
                const char * qname = iter->query().queryProp("@name");
                const char * wuid = iter->query().queryProp("@wuid");

                if (qname && *qname && wuid && *wuid)
                {
                    StringBuffer resp;

                    Owned<IEspPublishedQuery> pubQuery = createPublishedQuery();
                    pubQuery->setName(qname);
                    pubQuery->setId(id);
                    pubQuery->setWuid(wuid);
                    pubQuery->setSuspended(iter->query().getPropBool("@suspended"));

                    Owned<IEspQuerySignature> querysignature = createQuerySignature();
                    IArrayOf<IEspHPCCColumn> inparams;
                    IArrayOf<IEspOutputDataset> resultsets;

                    WsEclWuInfo wsinfo(wuid, setname, qname, username, passwd);
                    Owned<IResultSetFactory> resultSetFactory(getResultSetFactory(username, passwd));

                    //Each published query can have multiple results (datasets)
                    IConstWUResultIterator &results = wsinfo.wu->getResults();
                    ForEach(results)
                    {
                        Owned<IEspOutputDataset> outputdataset = createOutputDataset();
                        IArrayOf<IEspHPCCColumn> outparams;

                        IConstWUResult &result = results.query();

                        SCMStringBuffer resultName;
                        result.getResultName(resultName);
                        outputdataset->setName(resultName.s.str());

                        Owned<IResultSetMetaData> meta = resultSetFactory->createResultSetMeta(&result);

                        //Each result dataset can have multiple result columns
                        int columncount = meta->getColumnCount();
                        for (int i = 0; i < columncount; i++)
                        {
                            Owned<IEspHPCCColumn> col = createHPCCColumn();

                            SCMStringBuffer columnLabel;
                            meta->getColumnLabel(columnLabel,i);
                            col->setName(columnLabel.str());

                            SCMStringBuffer eclType;
                            meta->getColumnEclType(eclType, i);
                            col->setType(eclType.str());

                            outparams.append(*col.getLink());
                        }
                        outputdataset->setOutParams(outparams);
                        resultsets.append(*outputdataset.getLink());
                    }

                    //Each query can have multiple input parameters
                    IConstWUResultIterator &vars = wsinfo.wu->getVariables();
                    ForEach(vars)
                    {
                        Owned<IEspHPCCColumn> col = createHPCCColumn();

                        IConstWUResult &var = vars.query();

                        SCMStringBuffer varname;
                        var.getResultName(varname);
                        col->setName(varname.str());

                        Owned<IResultSetMetaData> meta = resultSetFactory->createResultSetMeta(&var);
                        SCMStringBuffer eclType;
                        meta->getColumnEclType(eclType, 0);
                        col->setType(eclType.str());

                        inparams.append(*col.getLink());
                    }

                    querysignature->setInParams(inparams);
                    querysignature->setResultSets(resultsets);
                    pubQuery->setSignature(*querysignature.getLink());
                    queries.append(*pubQuery.getLink());
                }
            }
            pqset->setQuerySetQueries(queries);

            IArrayOf<IEspQuerySetAliasMap> aliases;

            Owned<IPropertyTreeIterator> aliasiter = settree->getElements("Alias");
            ForEach(*aliasiter)
            {
                Owned<IEspQuerySetAliasMap> alias = createQuerySetAliasMap();
                const char * qname;
                const char * id;

                id = aliasiter->query().queryProp("@id");
                qname = aliasiter->query().queryProp("@name");

                alias->setId(id);
                alias->setName(qname);
                aliases.append(*alias.getLink());
            }

            pqset->setQuerySetAliases(aliases);
       }
        resp.setQuerySets(pquerysets);
    }
    bool includeTargetClusters = req.getIncludeTargetClusters();
    if (includeTargetClusters)
    {
        try
        {
            //another client (like configenv) may have updated the constant environment so reload it
            m_envFactory->validateCache();

            IArrayOf<IEspTpCluster> clusters;
            const char* type = req.getClusterType();
            if (!type || !*type || (strcmp(eqRootNode,type) == 0) || (strcmp(eqAllClusters,type) == 0))
            {
                m_TpWrapper.getClusterProcessList(eqHoleCluster, clusters);
                m_TpWrapper.getClusterProcessList(eqThorCluster, clusters);
                m_TpWrapper.getClusterProcessList(eqRoxieCluster,clusters);
            }
            else
            {
                m_TpWrapper.getClusterProcessList(type,clusters);
            }
            double version = context.getClientVersion();

            resp.setTpClusters(clusters);
        }
        catch(IException* e)
        {
            FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
        }
    }

    return success;
}

bool Cws_sqlEx::onGetDBSystemInfo(IEspContext &context, IEspGetDBSystemInfoRequest &req, IEspGetDBSystemInfoResponse &resp)
{
    bool success = false;
    resp.setName("HPCC Systems");

    if (!context.validateFeatureAccess(WSSQLACCESS, SecAccess_Access, false))
        throw MakeStringException(-1, "Failed to fetch HPCC information. Permission denied.");
    try
    {
        const char* build_ver = getBuildVersion();

        if (build_ver && *build_ver)
        {
            StringBuffer project;
            StringBuffer major;
            StringBuffer minor;
            StringBuffer point;
            StringBuffer maturity;

            //community_4.1.0-trunk1-Debug[heads/wssql-0-gb9e351-dirty
            const char * tail = build_ver;

            while (tail && *tail != '_')
                project.append(*tail++);

            tail++;
            while (tail && *tail != '.')
                major.append(*tail++);
            resp.setMajor(major.str());

            tail++;
            while (tail && *tail != '.')
                minor.append(*tail++);
            resp.setMinor(minor.str());

            tail++;
            while (tail && *tail != '-')
                point.append(*tail++);
            resp.setPoint(point.str());

            if (req.getIncludeAll())
            {
                resp.setFullVersion(build_ver);
                resp.setProject(project.str());

                tail++;
                while (tail && *tail != '-' && *tail != '[')
                    maturity.append(*tail++);
                resp.setMaturity(maturity.str());
            }

            success = true;
        }
    }
    catch (...)
    {
        ERRLOG("Error Parsing HPCC Version string.");
    }

    return success;
}

void  printTree(pANTLR3_BASE_TREE t, int indent)
{
    pANTLR3_BASE_TREE child = NULL;
    int     children = 0;
    char *  tokenText = NULL;
    string  ind = "";
    int     i = 0;

    if ( t != NULL )
    {
    children = t->getChildCount(t);
    for ( i = 0; i < indent; i++ )
      ind += "   ";

    for ( i = 0; i < children; i++ )
      {
        pANTLR3_BASE_TREE child = (pANTLR3_BASE_TREE)(t->getChild(t, i));
        ANTLR3_UINT32 tokenType = child->getType(child);

        tokenText = (char *)child->toString(child)->chars;
        fprintf(stderr, "%s%s\n", ind.c_str(), tokenText);
        if (tokenText == "<EOF>")
          break;
        printTree(child, indent+1);
      }
    }
}

void myDisplayRecognitionError (pANTLR3_BASE_RECOGNIZER recognizer,pANTLR3_UINT8 * tokenNames)
{
    StringBuffer errorMessage;

    pANTLR3_PARSER          parser = NULL;
    pANTLR3_TREE_PARSER     tparser = NULL;
    pANTLR3_INT_STREAM      is;
    pANTLR3_STRING          ttext;
    pANTLR3_EXCEPTION       ex;
    pANTLR3_COMMON_TOKEN    theToken;
    pANTLR3_BASE_TREE       theBaseTree;
    pANTLR3_COMMON_TREE     theCommonTree;

    ex      =       recognizer->state->exception;
    ttext   =       NULL;

    errorMessage.append("Error while parsing");
    if (ex)
    {
        errorMessage.appendf(": ANTLR Error %d : %s", ex->type, (pANTLR3_UINT8)(ex->message));

        switch  (recognizer->type)
        {
            case    ANTLR3_TYPE_PARSER:
            {
                parser      = (pANTLR3_PARSER) (recognizer->super);
                is          = parser->tstream->istream;
                theToken    = (pANTLR3_COMMON_TOKEN)(ex->token);
                ttext       = theToken->toString(theToken);

                if  (theToken != NULL)
                {
                    if (theToken->type == ANTLR3_TOKEN_EOF)
                        errorMessage.append(", at <EOF>");
                    else
                        errorMessage.appendf("\n    Near %s\n    ", ttext == NULL ? (pANTLR3_UINT8)"<no text for the token>" : ttext->chars);
                }
                break;
            }
            case    ANTLR3_TYPE_TREE_PARSER:
            {
                tparser     = (pANTLR3_TREE_PARSER) (recognizer->super);
                is          = tparser->ctnstream->tnstream->istream;
                theBaseTree = (pANTLR3_BASE_TREE)(ex->token);
                ttext       = theBaseTree->toStringTree(theBaseTree);

                if  (theBaseTree != NULL)
                {
                    theCommonTree   = (pANTLR3_COMMON_TREE)     theBaseTree->super;

                    if  (theCommonTree != NULL)
                        theToken   = (pANTLR3_COMMON_TOKEN)    theBaseTree->getToken(theBaseTree);

                    errorMessage.appendf( ", at offset %d", theBaseTree->getCharPositionInLine(theBaseTree));
                    errorMessage.appendf( ", near %s", ttext->chars);
                }
                break;
            }
            default:
                //errorMessage.appendf("Base recognizer function displayRecognitionError called by unknown parser type - provide override for this function\n");
                return;
                break;
        }

        switch  (ex->type)
        {
            case    ANTLR3_UNWANTED_TOKEN_EXCEPTION:
            {
                // Indicates that the recognizer was fed a token which seesm to be
                // spurious input. We can detect this when the token that follows
                // this unwanted token would normally be part of the syntactically
                // correct stream. Then we can see that the token we are looking at
                // is just something that should not be there and throw this exception.
                //
                if  (tokenNames == NULL)
                {
                    errorMessage.appendf( " : Extraneous input...");
                }
                else
                {
                    if  (ex->expecting == ANTLR3_TOKEN_EOF)
                        errorMessage.appendf(" : Extraneous input - expected <EOF>\n");
                    else
                        errorMessage.appendf(" : Extraneous input - expected %s ...\n", tokenNames[ex->expecting]);
                }
                break;
            }
            case    ANTLR3_MISSING_TOKEN_EXCEPTION:
            {
                // Indicates that the recognizer detected that the token we just
                // hit would be valid syntactically if preceeded by a particular
                // token. Perhaps a missing ';' at line end or a missing ',' in an
                // expression list, and such like.
                //
                if  (tokenNames == NULL)
                {
                    errorMessage.appendf( " : Missing token (%d)...\n", ex->expecting);
                }
                else
                {
                    if  (ex->expecting == ANTLR3_TOKEN_EOF)
                        errorMessage.appendf( " : Missing <EOF>\n");
                    else
                        errorMessage.appendf( " : Missing %s \n", tokenNames[ex->expecting]);
                }
                break;
            }
            case    ANTLR3_RECOGNITION_EXCEPTION:
            {
                // Indicates that the recognizer received a token
                // in the input that was not predicted. This is the basic exception type
                // from which all others are derived. So we assume it was a syntax error.
                // You may get this if there are not more tokens and more are needed
                // to complete a parse for instance.
                //
                errorMessage.appendf( " : syntax error...\n");
                break;
            }
            case    ANTLR3_MISMATCHED_TOKEN_EXCEPTION:
            {
                // We were expecting to see one thing and got another. This is the
                // most common error if we coudl not detect a missing or unwanted token.
                // Here you can spend your efforts to
                // derive more useful error messages based on the expected
                // token set and the last token and so on. The error following
                // bitmaps do a good job of reducing the set that we were looking
                // for down to something small. Knowing what you are parsing may be
                // able to allow you to be even more specific about an error.
                //
                if  (tokenNames == NULL)
                {
                    errorMessage.appendf(" : syntax error...\n");
                }
                else
                {
                    if  (ex->expecting == ANTLR3_TOKEN_EOF)
                        errorMessage.appendf(" : expected <EOF>\n");
                    else
                        errorMessage.appendf(" : expected %s ...\n", tokenNames[ex->expecting]);
                }
                break;
            }
            case    ANTLR3_NO_VIABLE_ALT_EXCEPTION:
            {
                // We could not pick any alt decision from the input given
                // so god knows what happened - however when you examine your grammar,
                // you should. It means that at the point where the current token occurred
                // that the DFA indicates nowhere to go from here.
                //
                errorMessage.appendf(" : cannot match to any predicted input...\n");
                break;
            }
            case    ANTLR3_MISMATCHED_SET_EXCEPTION:
            {
                ANTLR3_UINT32     count;
                ANTLR3_UINT32     bit;
                ANTLR3_UINT32     size;
                ANTLR3_UINT32     numbits;
                pANTLR3_BITSET    errBits;

                // This means we were able to deal with one of a set of
                // possible tokens at this point, but we did not see any
                // member of that set.
                errorMessage.appendf( " : unexpected input...\n  expected one of : ");

                // What tokens could we have accepted at this point in the parse?
                count   = 0;
                errBits = antlr3BitsetLoad      (ex->expectingSet);
                numbits = errBits->numBits      (errBits);
                size    = errBits->size         (errBits);

                if  (size > 0)
                {
                    // However many tokens we could have dealt with here, it is usually
                    // not useful to print ALL of the set here. I arbitrarily chose 8
                    // here, but you should do whatever makes sense for you of course.
                    // No token number 0, so look for bit 1 and on.
                    for (bit = 1; bit < numbits && count < 8 && count < size; bit++)
                    {
                        if  (tokenNames[bit])
                        {
                            errorMessage.appendf( "%s%s", count > 0 ? ", " : "", tokenNames[bit]);
                            count++;
                        }
                    }
                    errorMessage.appendf( "\n");
                }
                else
                {
                    errorMessage.appendf( "Unknown parsing error.\n");
                }
                break;
            }
            case    ANTLR3_EARLY_EXIT_EXCEPTION:
            {
                // We entered a loop requiring a number of token sequences
                // but found a token that ended that sequence earlier than
                // we should have done.
                errorMessage.appendf( " : missing elements...\n");
                break;
            }
            default:
            {
                // We don't handle any other exceptions here, but you can
                // if you wish. If we get an exception that hits this point
                // then we are just going to report what we know about the
                // token.
                //
                errorMessage.appendf( " : unrecognized syntax...\n");
                break;
            }
        }
    }
    throw MakeStringException(-1, "%s", errorMessage.str());
}

HPCCSQLTreeWalker * Cws_sqlEx::parseSQL(IEspContext &context, StringBuffer & sqltext)
{
    int limit = -1;
   pHPCCSQLLexer hpccSqlLexer = NULL;
   pANTLR3_COMMON_TOKEN_STREAM sqlTokens = NULL;
   pHPCCSQLParser hpccSqlParser = NULL;
   pANTLR3_BASE_TREE sqlAST  = NULL;
   pANTLR3_INPUT_STREAM sqlInputStream = NULL;
   Owned<HPCCSQLTreeWalker> hpccSqlTreeWalker;

   try
   {
       if (sqltext.length() <= 0)
           throw MakeStringException(-1, "Empty SQL String detected.");

       pANTLR3_UINT8 input_string = (pANTLR3_UINT8)sqltext.str();
       pANTLR3_INPUT_STREAM sqlinputstream = antlr3StringStreamNew(input_string,
                                                                ANTLR3_ENC_8BIT,
                                                                sqltext.length(),
                                                                (pANTLR3_UINT8)"SQL INPUT");

       pHPCCSQLLexer hpccsqllexer = HPCCSQLLexerNew(sqlinputstream);
       //hpccSqlLexer->pLexer->rec->displayRecognitionError = myDisplayRecognitionError;

       //ANTLR3_UINT32  lexerrors = hpccsqllexer->pLexer->rec->getNumberOfSyntaxErrors(hpccsqllexer->pLexer->rec);
       //if (lexerrors > 0)
       //     throw MakeStringException(-1, "HPCCSQL Lexer reported %d error(s), request aborted.", lexerrors);

       pANTLR3_COMMON_TOKEN_STREAM sqltokens = antlr3CommonTokenStreamSourceNew(ANTLR3_SIZE_HINT, TOKENSOURCE(hpccsqllexer));
       if (sqltokens == NULL)
       {
           throw MakeStringException(-1, "Out of memory trying to allocate ANTLR HPCCSQLParser token stream.");
       }

       pHPCCSQLParser hpccsqlparser = HPCCSQLParserNew(sqltokens);
//#if not defined(_DEBUG)
       hpccsqlparser->pParser->rec->displayRecognitionError = myDisplayRecognitionError;
//#endif
       pANTLR3_BASE_TREE sqlAST  = (hpccsqlparser->root_statement(hpccsqlparser)).tree;

       ANTLR3_UINT32 parserrors = hpccsqlparser->pParser->rec->getNumberOfSyntaxErrors(hpccsqlparser->pParser->rec);
       if (parserrors > 0)
           throw MakeStringException(-1, "HPCCSQL Parser reported %d error(s), request aborted.", parserrors);

#if defined(_DEBUG)
printTree(sqlAST, 0);
#endif

       hpccSqlTreeWalker.setown(new HPCCSQLTreeWalker(sqlAST, context));

       hpccsqlparser->free(hpccsqlparser);
       sqltokens->free(sqltokens);
       hpccsqllexer->free(hpccsqllexer);
       sqlinputstream->free(sqlinputstream);

   }
   catch(IException* e)
   {
       try
       {
           if (hpccSqlParser)
               hpccSqlParser->free(hpccSqlParser);
           if (sqlTokens)
               sqlTokens->free(sqlTokens);
           if (hpccSqlLexer)
               hpccSqlLexer->free(hpccSqlLexer);
           if (sqlInputStream)
               sqlInputStream->free(sqlInputStream);

           hpccSqlTreeWalker.clear();
       }
       catch (...)
       {
           ERRLOG("!!! Unable to free HPCCSQL parser/lexer objects.");
       }

       //All IExceptions get bubbled up
       throw e;
   }
   catch(...)
   {
       try
       {
           if (hpccSqlParser)
               hpccSqlParser->free(hpccSqlParser);
           if (sqlTokens)
               sqlTokens->free(sqlTokens);
           if (hpccSqlLexer)
               hpccSqlLexer->free(hpccSqlLexer);
           if (sqlInputStream)
               sqlInputStream->free(sqlInputStream);

           hpccSqlTreeWalker.clear();
       }
       catch (...)
       {
           ERRLOG("!!! Unable to free HPCCSQL parser/lexer objects.");
       }

       //All other unexpected exceptions are reported as generic ecl generation error.
       throw MakeStringException(-1, "Error generating ECL code.");
   }

   return hpccSqlTreeWalker.getLink();
}

bool Cws_sqlEx::getWUResult(IEspContext &context, const char * wuid, StringBuffer &result, unsigned start, unsigned count)
{
    if (wuid && *wuid)
    {
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
        Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid, false);

        if (!cw)
           throw MakeStringException(ECLWATCH_CANNOT_UPDATE_WORKUNIT,"Cannot open workunit %s.", wuid);

        SCMStringBuffer stateDesc;

        switch (cw->getState())
        {
           case WUStateCompleted:
           case WUStateFailed:
           case WUStateUnknown:
           {
               StringBufferAdaptor resultXML(result);
               Owned<IResultSetFactory> factory = getResultSetFactory(context.queryUserId(), context.queryPassword());
               Owned<INewResultSet> nr = factory->createNewResultSet(wuid, 0, "");
               if (nr.get())
                   getResultXml(resultXML, nr.get(), "WsSQLResult", start, count, NULL);
               else
                   return false;
               break;
           }
           default:
               break;
        }
        return true;
    }
    return false;
}

bool Cws_sqlEx::onExecuteSQL(IEspContext &context, IEspExecuteSQLRequest &req, IEspExecuteSQLResponse &resp)
{
    try
    {
        if (!context.validateFeatureAccess(WSSQLACCESS, SecAccess_Write, false))
            throw MakeStringException(-1, "Failed to execute SQL. Permission denied.");

        StringBuffer sqltext;
        StringBuffer ecltext;
        StringBuffer username;
        context.getUserID(username);
        const char* passwd = context.queryPassword();

        sqltext.set(req.getSqlText());

        if (sqltext.length() <= 0)
            throw MakeStringException(1,"Empty SQL request.");

        const char  *cluster = req.getTargetCluster();
        SCMStringBuffer compiledwuid;
        int resultLimit = req.getResultLimit();
        __int64 resultWindowStart = req.getResultWindowStart();
        __int64 resultWindowCount = req.getResultWindowCount();

        if (resultWindowStart < 0 || resultWindowCount <0 )
           throw MakeStringException(-1,"Invalid result window value");

        bool clonable = false;

        Owned<HPCCSQLTreeWalker> parsedSQL;
        parsedSQL.setown(parseSQL(context, sqltext));

        if (parsedSQL->getSqlType() == SQLTypeCall)
        {
            if (strlen(parsedSQL->getQuerySetName())==0)
            {
                if (strlen(req.getTargetQuerySet())==0)
                   throw MakeStringException(-1,"Missing Target QuerySet.");
               else
                   parsedSQL->setQuerySetName(req.getTargetQuerySet());
            }
        }

        StringBuffer xmlparams;
        StringBuffer normalizedSQL = parsedSQL->getNormalizedSQL();
        normalizedSQL.append("--HARDLIMIT").append(resultLimit);

        if(cachedSQLQueries.find(normalizedSQL.str()) != cachedSQLQueries.end())
        {
            compiledwuid.s  = cachedSQLQueries.find(normalizedSQL.str())->second.c_str();
            clonable = true;
        }
        else
        {
            if (parsedSQL->getSqlType() == SQLTypeCall)
            {
                WsEclWuInfo wsinfo("", parsedSQL->getQuerySetName(), parsedSQL->getStoredProcName(), username.str(), passwd);
                wsinfo.wu->getWuid(compiledwuid);

                clonable = true;
            }
            else
            {
                NewWsWorkunit wu(context);

                wu->getWuid(compiledwuid);

                if (notEmpty(cluster))
                {
                    if (isValidCluster(cluster))
                        wu->setClusterName(cluster);
                    else
                        throw MakeStringException(-1, "Invalid cluster name: %s", cluster);
                }
                else
                {
                    throw MakeStringException(-1,"Target cluster not set.");
                }

                if (clonable)
                    wu->setCloneable(true);
                wu->setAction(WUActionCompile);

                ECLEngine::generateECL(parsedSQL,ecltext.clear());
#if defined _DEBUG
                fprintf(stderr, "GENERATED ECL:\n%s\n", ecltext.toCharArray());
#endif
                if (notEmpty(ecltext))
                   wu.setQueryText(ecltext.str( ));
                else
                   throw MakeStringException(1,"Could not generate ECL from SQL.");

                if (resultLimit)
                    wu->setResultLimit(resultLimit);

                const char * wuusername = req.getUserName();
                if (wuusername && *wuusername)
                    wu->setUser(wuusername);

                wu->commit();
                wu.clear();

                WsWuProcess::submitWsWorkunit(context, compiledwuid.str(), cluster, NULL, 0, true, false, false, NULL, NULL, NULL);
                waitForWorkUnitToCompile(compiledwuid.str(), req.getWait());
            }
        }

        Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
        Owned<IConstWorkUnit> cw = factory->openWorkUnit(compiledwuid.str(), false);
        if (!cw)
            throw MakeStringException(ECLWATCH_CANNOT_UPDATE_WORKUNIT,"Cannot open workunit %s.", compiledwuid.str());

        WsWUExceptions errors(*cw);
        if (errors.ErrCount()>0)
        {
            WsWuInfo winfo(context, compiledwuid.str());
            winfo.getCommon(resp.updateWorkunit(), WUINFO_All);
            winfo.getExceptions(resp.updateWorkunit(), WUINFO_All);
        }
        else
        {
            //const char *cluster = req.getCluster();
            //if (notEmpty(cluster) && !isValidCluster(cluster))
            //   throw MakeStringException(ECLWATCH_INVALID_CLUSTER_NAME, "Invalid cluster name: %s", cluster);

            if (parsedSQL->getSqlType() == SQLTypeCall)
                createXMLParams(xmlparams, parsedSQL, NULL, cw);

            StringBuffer runningwuid;

            if (clonable)
            {
                cloneAndExecuteWU(context, compiledwuid.str(), runningwuid, xmlparams.str(), NULL, NULL, cluster);
                if (cachedSQLQueries.find(normalizedSQL.str()) != cachedSQLQueries.end() )
                    cachedSQLQueries.insert(std::pair<std::string,std::string>(normalizedSQL.str(), compiledwuid.str()));
            }
            else
            {
                WsWuProcess::submitWsWorkunit(context, compiledwuid.str(), cluster, NULL, 0, false, true, true, NULL, NULL, NULL);
                runningwuid.set(compiledwuid.str());
                cachedSQLQueries.insert(std::pair<std::string,std::string>(normalizedSQL.str(), runningwuid.str()));
            }

            int timeToWait = req.getWait();
            if (timeToWait != 0)
               waitForWorkUnitToComplete(runningwuid.str(), timeToWait);

            if (strcmp(runningwuid.str(), compiledwuid.str())!=0)
                resp.setParentWuId(compiledwuid.str());

            resp.setResultLimit(resultLimit);
            resp.setResultWindowCount( (unsigned)resultWindowCount);
            resp.setResultWindowStart( (unsigned)resultWindowStart);

            StringBuffer result;
            if (getWUResult(context, runningwuid.str(), result, (unsigned)resultWindowStart,  (unsigned)resultWindowCount))
                resp.setResult(result.str());

            WsWuInfo winfo(context, runningwuid);
            winfo.getCommon(resp.updateWorkunit(), WUINFO_All);
            winfo.getExceptions(resp.updateWorkunit(), WUINFO_All);
        }

        AuditSystemAccess(context.queryUserId(), true, "Updated %s", compiledwuid.str());
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, -1);
    }
    //catch (...)
    //{
    //    me->append(*MakeStringException(0,"Unknown exception submitting %s",wuid.str()));
    //}

    return true;
}

//Integrates all "variables" into "param" based xml
void Cws_sqlEx::createXMLParams(StringBuffer & xmlparams, HPCCSQLTreeWalker* parsedSQL, IArrayOf<IConstNamedValue> *variables, IConstWorkUnit * cw)
{
    IArrayOf<IConstWUResult> expectedparams;

    if (cw)
    {
        IConstWUResultIterator &vars = cw->getVariables();
        ForEach(vars)
        {
            IConstWUResult &cur = vars.query();
            expectedparams.append(cur);
        }
    }

    if (expectedparams.length() > 0)
    {
        int totalvars = 0;
        if (variables)
            totalvars = variables->length();

        if (parsedSQL && parsedSQL->getSqlType() == SQLTypeCall)
        {
            IArrayOf<ISQLExpression> * embeddedparams = NULL;
            if (parsedSQL)
                embeddedparams = parsedSQL->getStoredProcParamList();

            int parametersidx = 0;
            int varsidx = 0;


            SCMStringBuffer varname;

            if (embeddedparams && embeddedparams->length()>0)
            {
              xmlparams.append("<root>");

              for(int i=0; i < embeddedparams->length() && i < expectedparams.length(); i++)
              {
                  expectedparams.item(i).getResultName(varname);
                  xmlparams.append("<").append(varname.s.str()).append(">");

                  ISQLExpression* paramcol = &embeddedparams->item(i);
                  if (paramcol->getExpType() == ParameterPlaceHolder_ExpressionType)
                  {
                      if (varsidx < totalvars)
                      {
                          IConstNamedValue &item = variables->item(varsidx++);
                          const char *value = item.getValue();
                          if(value && *value)
                              encodeXML(value, xmlparams);
                          // else ??
                      }
                  }
                  else
                  {
                      paramcol->toString(xmlparams,false);
                  }
                  xmlparams.append("</").append(varname.s.str()).append(">");
              }
              xmlparams.append("</root>");
            }
        }
        else
        {
            int parametersidx = 0;
            int varsidx = 0;

            SCMStringBuffer varname;

            xmlparams.append("<root>");

            for(int i=0; i < expectedparams.length() && i < totalvars; i++)
            {
                expectedparams.item(i).getResultName(varname);
                xmlparams.append("<").append(varname.s.str()).append(">");

                IConstNamedValue &item = variables->item(i);
                const char *value = item.getValue();
                if(value && *value)
                  encodeXML(value, xmlparams);
                // else ??

              xmlparams.append("</").append(varname.s.str()).append(">");
            }
            xmlparams.append("</root>");
        }
    }
}

bool Cws_sqlEx::onExecutePreparedSQL(IEspContext &context, IEspExecutePreparedSQLRequest &req, IEspExecutePreparedSQLResponse &resp)
{
   try
   {
       if (!context.validateFeatureAccess(WSSQLACCESS, SecAccess_Write, false))
           throw MakeStringException(-1, "Failed to execute SQL. Permission denied.");

       const char *cluster = req.getTargetCluster();
       if (notEmpty(cluster) && !isValidCluster(cluster))
           throw MakeStringException(ECLWATCH_INVALID_CLUSTER_NAME, "Invalid cluster name: %s", cluster);

       Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());

       StringBuffer runningWuId;
       const char* parentWuId = req.getWuId();

       Owned<IConstWorkUnit> cw = factory->openWorkUnit(parentWuId, false);
       if (!cw)
           throw MakeStringException(-1,"Cannot open workunit %s.", parentWuId);

        WsWUExceptions errors(*cw);
        if (errors.ErrCount()>0)
        {
              WsWuInfo winfo(context, parentWuId);
              winfo.getCommon(resp.updateWorkunit(), WUINFO_All);
              winfo.getExceptions(resp.updateWorkunit(), WUINFO_All);
        }
        else
        {
           StringBuffer xmlparams;
           createXMLParams(xmlparams, NULL, &req.getVariables(),cw);

           if (parentWuId && *parentWuId)
           {
               cloneAndExecuteWU(context, parentWuId, runningWuId, xmlparams, NULL, NULL, cluster);
           }
           else
               throw MakeStringException(ECLWATCH_MISSING_PARAMS,"Missing WuId");

           int timeToWait = req.getWait();
           if (timeToWait != 0)
               waitForWorkUnitToComplete(runningWuId.str(), timeToWait);

           Owned<IConstWorkUnit> cw = factory->openWorkUnit(runningWuId.str(), false);
           if (!cw)
               throw MakeStringException(-1,"Cannot open workunit %s.", runningWuId.str());

           resp.setParentWuId(parentWuId);

           StringBuffer result;
           __int64 resultWindowStart = req.getResultWindowStart();
           __int64 resultWindowCount = req.getResultWindowCount();

           if (resultWindowStart < 0 || resultWindowCount <0 )
               throw MakeStringException(-1,"Invalid result window value");

           if (getWUResult(context, runningWuId.str(), result, (unsigned)resultWindowStart, (unsigned)resultWindowCount))
               resp.setResult(result.str());

            WsWuInfo winfo(context, runningWuId);
            winfo.getCommon(resp.updateWorkunit(), WUINFO_All);
            winfo.getExceptions(resp.updateWorkunit(), WUINFO_All);
            winfo.getVariables(resp.updateWorkunit(), WUINFO_All);
        }
   }
   catch(IException* e)
   {
       FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
   }
   return true;
}

bool Cws_sqlEx::onPrepareSQL(IEspContext &context, IEspPrepareSQLRequest &req, IEspPrepareSQLResponse &resp)
{
    bool success = false;
    StringBuffer sqltext;
    StringBuffer ecltext;
    bool clonable = false;
    try
    {
        if (!context.validateFeatureAccess(WSSQLACCESS, SecAccess_Write, false))
            throw MakeStringException(-1, "Failed to execute SQL. Permission denied.");

        sqltext.set(req.getSqlText());

        if (sqltext.length() <= 0)
            throw MakeStringException(1,"Empty SQL request.");

        Owned<HPCCSQLTreeWalker> parsedSQL;
        parsedSQL.setown(parseSQL(context, sqltext));

        if (parsedSQL->getSqlType() == SQLTypeCall)
        {
            if (strlen(parsedSQL->getQuerySetName())==0)
            {
                if (strlen(req.getTargetQuerySet())==0)
                   throw MakeStringException(-1,"Missing Target QuerySet.");
               else
                   parsedSQL->setQuerySetName(req.getTargetQuerySet());
            }
        }

        StringBuffer xmlparams;
        StringBuffer normalizedSQL = parsedSQL->getNormalizedSQL();
        SCMStringBuffer wuid;

        if(cachedSQLQueries.find(normalizedSQL.str()) != cachedSQLQueries.end())
        {
            wuid.s  = cachedSQLQueries.find(normalizedSQL.str())->second.c_str();
        }
        else
        {
            if (parsedSQL->getSqlType() == SQLTypeCall)
            {
                StringBuffer username;
                context.getUserID(username);
                const char* passwd = context.queryPassword();
                WsEclWuInfo wsinfo("", parsedSQL->getQuerySetName(), parsedSQL->getStoredProcName(), username.str(), passwd);
                wsinfo.wu->getWuid(wuid);

                //if call somePublishedQuery(1,2,3);
                //                   or
                //        someotherPublishedQuery(1,?)
                //must clone published query wuid and set params
                //else just return published query WUID
                if (parsedSQL->getStoredProcParamListCount() > 0 && !parsedSQL->isParameterizedCall() )
                    throw MakeStringException(-1, "Prepared Call query must be fully parameterized");
//KEEP THIS AROUND IF WE WANT TO SUPPORT CLONING WU with embedded params
//                if (parsedSQL->isParameterizedCall())
//                {
//                    createXMLParams(xmlparams, parsedSQL, NULL, wsinfo.wu);
//
//                    SCMStringBuffer newwuid;
//                    NewWsWorkunit wu(context);
//                    wu->getWuid(newwuid);
//                    if (xmlparams && *xmlparams)
//                        wu->setXmlParams(xmlparams);
//
//                    WsWuProcess::copyWsWorkunit(context, *wu, wuid.s.str());
//
//                    //StringBuffer params;
//                    //toXML(wu->getXmlParams(), params, true, true);
//                    wu->setCloneable(true);
//                    wu->setAction(WUActionCompile);
//
//                    wu->commit();
//                    wu.clear();
//
//                    WsWuProcess::submitWsWorkunit(context, newwuid.str(), req.getTargetCluster(), NULL, 0, true, false, false, xmlparams.str(), NULL, NULL);
//                    waitForWorkUnitToCompile(newwuid.str(), req.getWait());
//
//                    wuid.s.set(newwuid.str());
//                }
            }
            else
            {
                NewWsWorkunit wu(context);
                wu->getWuid(wuid);

                const char  *cluster = req.getTargetCluster();
                if (notEmpty(cluster))
                {
                    wu->setClusterName(req.getTargetCluster());
                    if (isValidCluster(cluster))
                       wu->setClusterName(cluster);
                    else
                       throw MakeStringException(-1/*ECLWATCH_INVALID_CLUSTER_NAME*/, "Invalid cluster name: %s", cluster);
                }
                else
                    throw MakeStringException(1,"Target cluster not set.");

                wu->setCloneable(true);
                wu->setAction(WUActionCompile);

                ECLEngine::generateECL(parsedSQL,ecltext.clear());
#if defined _DEBUG
                fprintf(stderr, "GENERATED ECL:\n%s\n", ecltext.toCharArray());
#endif

                if (notEmpty(ecltext))
                {
                   wu.setQueryText(ecltext.str());
                }
                else
                {
                   throw MakeStringException(1,"Could not generate ECL from SQL.");
                }

                StringBuffer xmlparams;
                createXMLParams(xmlparams, parsedSQL, NULL, NULL);

                wu->commit();
                wu.clear();

                WsWuProcess::submitWsWorkunit(context, wuid.str(), req.getTargetCluster(), NULL, 0, true, false, false, xmlparams.str(), NULL, NULL);
                waitForWorkUnitToCompile(wuid.str(), req.getWait());
            }

            cachedSQLQueries.insert(std::pair<std::string,std::string>(normalizedSQL.str(), wuid.s.str()));
        }
        WsWuInfo winfo(context, wuid.str());

        winfo.getCommon(resp.updateWorkunit(), WUINFO_All);
        winfo.getExceptions(resp.updateWorkunit(), WUINFO_All);

        //publishWorkunit(context, "myfirstpublished", wuid.str(), "thor");

        AuditSystemAccess(context.queryUserId(), true, "Updated %s", wuid.str());
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, -1);
    }

    return true;
}

bool Cws_sqlEx::executePublishedQueryByname(IEspContext &context, const char * queryset, const char * queryname, StringBuffer &clonedwuid, const char *paramXml, IArrayOf<IConstNamedValue> *variables, const char * targetcluster, int start, int count)
{
    bool success = true;

    Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
    if (factory.get())
    {
        StringBuffer wuid;
        StringBuffer username;
        context.getUserID(username);

        const char* passwd = context.queryPassword();

        WsEclWuInfo wsinfo(wuid, queryset, queryname, username.str(), passwd);

        success = executePublishedQueryByWuId(context, wsinfo.wuid.get(), clonedwuid, paramXml, variables, targetcluster, start, count);
    }
    else
        success = false;

    return success;
}

bool Cws_sqlEx::executePublishedQueryByWuId(IEspContext &context, const char * targetwuid, StringBuffer &clonedwuid, const char *paramXml, IArrayOf<IConstNamedValue> *variables, const char * targetcluster, int start, int count)
{
    bool success = true;

    if (targetwuid && *targetwuid)
    {
        success = cloneAndExecuteWU(context, targetwuid, clonedwuid, paramXml, variables, NULL, targetcluster);

/*
        if (waittime != 0)
            waitForWorkUnitToComplete(clonedwui.str(), waittime);

        Owned<IConstWorkUnit> cw = factory->openWorkUnit(clonedwui.str(), false);
        if (!cw)
            throw MakeStringException(ECLWATCH_CANNOT_UPDATE_WORKUNIT,"Cannot open workunit %s.", clonedwui.str());

        getWUResult(context, clonedwui.str(), resp, start, count);
        */

    }
    else
        success = false;

    return success;
}

bool Cws_sqlEx::executePublishedQuery(IEspContext &context, const char * queryset, const char * queryname, StringBuffer &resp, int start, int count, int waittime)
{
    bool success = true;

    Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
    if (factory.get())
    {
        StringBuffer wuid;
        StringBuffer username;
        context.getUserID(username);

        const char* passwd = context.queryPassword();

        WsEclWuInfo wsinfo(wuid, queryset, queryname, username.str(), passwd);

        StringBuffer clonedwui;
        cloneAndExecuteWU(context, wsinfo.wuid.get(), clonedwui, NULL, NULL, NULL, "");

        if (waittime != 0)
            waitForWorkUnitToComplete(clonedwui.str(), waittime);

        Owned<IConstWorkUnit> cw = factory->openWorkUnit(clonedwui.str(), false);
        if (!cw)
            throw MakeStringException(ECLWATCH_CANNOT_UPDATE_WORKUNIT,"Cannot open workunit %s.", clonedwui.str());

        getWUResult(context, clonedwui.str(), resp, start, count);
    }
    else
        success = false;

    return success;
}

bool Cws_sqlEx::executePublishedQuery(IEspContext &context, const char * wuid, StringBuffer &resp, int start, int count, int waittime)
{
    bool success = true;

    Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
    if (factory.get())
    {
        StringBuffer username;
        context.getUserID(username);

        const char* passwd = context.queryPassword();

        WsEclWuInfo wsinfo(wuid, "", "", username.str(), passwd);

        StringBuffer clonedwui;
        cloneAndExecuteWU(context, wsinfo.wuid.get(), clonedwui, NULL, NULL, NULL, "");

        if (waittime != 0)
            waitForWorkUnitToComplete(clonedwui.str(), waittime);

        Owned<IConstWorkUnit> cw = factory->openWorkUnit(clonedwui.str(), false);
        if (!cw)
            throw MakeStringException(ECLWATCH_CANNOT_UPDATE_WORKUNIT,"Cannot open workunit %s.", clonedwui.str());

        getWUResult(context, clonedwui.str(), resp, start, count);

    }
    else
        success = false;

    return success;
}

bool Cws_sqlEx::cloneAndExecuteWU(IEspContext &context, const char * originalwuid, StringBuffer &clonedwuid, const char *paramXml, IArrayOf<IConstNamedValue> *variables, IArrayOf<IConstNamedValue> *debugs, const char * targetcluster)
{
    bool success = true;
    try
    {
       Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());

       if (originalwuid && *originalwuid)
       {
           if (!looksLikeAWuid(originalwuid))
               throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invalid Workunit ID: %s", originalwuid);

           Owned<IConstWorkUnit> pwu = factory->openWorkUnit(originalwuid, false);

           if (!pwu)
               throw MakeStringException(-1,"Cannot open workunit %s.", originalwuid);

           if (pwu->getExceptionCount()>0)
           {
               WsWUExceptions errors(*pwu);
               if (errors.ErrCount()>0)
                   throw MakeStringException(-1,"Original query contains errors %s.", originalwuid);
           }

           StringBufferAdaptor isvWuid(clonedwuid);

           WsWuProcess::runWsWorkunit(
                   context,
                   clonedwuid,
                   originalwuid,
                   targetcluster,
                   paramXml,
                   variables,
                   debugs);
       }
       else
           throw MakeStringException(ECLWATCH_MISSING_PARAMS,"Missing WuId");
    }
    catch(IException* e)
    {
       FORWARDEXCEPTION(context, e, -1);
    }

    return success;
}

bool Cws_sqlEx::onGetResults(IEspContext &context, IEspGetResultsRequest &req, IEspGetResultsResponse &resp)
{
    if (!context.validateFeatureAccess(WSSQLACCESS, SecAccess_Read, false))
        throw MakeStringException(-1, "Failed to fetch results (open workunit). Permission denied.");

    bool success = true;
    const char* parentWuId = req.getWuId();
    if (parentWuId && *parentWuId)
    {
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());

        if (factory.get())
        {
            Owned<IConstWorkUnit> cw = factory->openWorkUnit(parentWuId, false);
            if (!cw)
                throw MakeStringException(-1,"Cannot open workunit %s.", parentWuId);

            __int64 resultWindowStart = req.getResultWindowStart();
            __int64 resultWindowCount = req.getResultWindowCount();

            if (resultWindowStart < 0 || resultWindowCount <0 )
               throw MakeStringException(-1,"Invalid result window value");

            //resp.setResultLimit(resultLimit);
            resp.setResultWindowCount((unsigned)resultWindowCount);
            resp.setResultWindowStart((unsigned)resultWindowStart);

            StringBuffer result;
            if (getWUResult(context, parentWuId, result, (unsigned)resultWindowStart, (unsigned)resultWindowCount))
                resp.setResult(result.str());

            WsWuInfo winfo(context, parentWuId);
            winfo.getCommon(resp.updateWorkunit(), WUINFO_All);
            winfo.getExceptions(resp.updateWorkunit(), WUINFO_All);
        }
        else
            throw MakeStringException(-1,"Could not create WU factory object");
    }
    else
        throw MakeStringException(-1,"Missing WuId");

    return success;
}

void Cws_sqlEx::refreshValidClusters()
{
    validClusters.kill();
    Owned<IStringIterator> it = getTargetClusters(NULL, NULL);
    ForEach(*it)
    {
        SCMStringBuffer s;
        IStringVal &val = it->str(s);
        if (!validClusters.getValue(val.str()))
            validClusters.setValue(val.str(), true);
    }
}

bool Cws_sqlEx::isValidCluster(const char *cluster)
{
    if (!cluster || !*cluster)
        return false;
    CriticalBlock block(crit);
    if (validClusters.getValue(cluster))
        return true;
    if (validateTargetClusterName(cluster))
    {
        refreshValidClusters();
        return true;
    }
    return false;
}

bool Cws_sqlEx::publishWorkunit(IEspContext &context, const char * queryname, const char * wuid, const char * targetcluster)
{
    Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
    Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid, false);
    if (!cw)
        throw MakeStringException(ECLWATCH_CANNOT_OPEN_WORKUNIT,"Cannot find the workunit %s", wuid);

    SCMStringBuffer queryName;
    if (notEmpty(queryname))
        queryName.set(queryname);
    else
        cw->getJobName(queryName).str();

    if (!queryName.length())
        throw MakeStringException(ECLWATCH_MISSING_PARAMS, "Query/Job name not defined for publishing workunit %s", wuid);

    SCMStringBuffer target;
    if (notEmpty(targetcluster))
        target.set(targetcluster);
    else
        cw->getClusterName(target);

    if (!target.length())
        throw MakeStringException(ECLWATCH_MISSING_PARAMS, "Cluster name not defined for publishing workunit %s", wuid);
    if (!isValidCluster(target.str()))
        throw MakeStringException(ECLWATCH_INVALID_CLUSTER_NAME, "Invalid cluster name: %s", target.str());
    //RODRIGO this is needed:
    //copyQueryFilesToCluster(context, cw, "", target.str(), queryName.str(), false);

    WorkunitUpdate wu(&cw->lock());
    wu->setJobName(queryName.str());

    StringBuffer queryId;

    addQueryToQuerySet(wu, target.str(), queryName.str(), NULL, MAKE_ACTIVATE, queryId, context.queryUserId());

    //Owned<IPropertyTree> queryTree = getQueryById(target.str(), queryId, false);
    //updateMemoryLimitSetting(queryTree, req.getMemoryLimit());
    //updateQuerySetting(req.getTimeLimit_isNull(), queryTree, "@timeLimit", req.getTimeLimit());
    // updateQuerySetting(req.getWarnTimeLimit_isNull(), queryTree, "@warnTimeLimit", req.getWarnTimeLimit());
    //updateQueryPriority(queryTree, req.getPriority());

    wu->commit();
    wu.clear();

    return true;
}

