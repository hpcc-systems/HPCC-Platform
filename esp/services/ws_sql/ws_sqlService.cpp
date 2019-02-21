/*##############################################################################

HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems.

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

void CwssqlEx::init(IPropertyTree *_cfg, const char *_process, const char *_service)
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

    setWsSqlBuildVersion(BUILD_TAG);
}

bool CwssqlEx::onEcho(IEspContext &context, IEspEchoRequest &req, IEspEchoResponse &resp)
{
    resp.setResponse(req.getRequest());
    return true;
}

bool CwssqlEx::onGetDBMetaData(IEspContext &context, IEspGetDBMetaDataRequest &req, IEspGetDBMetaDataResponse &resp)
{
    context.ensureFeatureAccess(WSSQLACCESS, SecAccess_Read, -1, "WsSQL::GetDBMetaData: Permission denied.");

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

            if (settree == NULL)
               continue;

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
                    IConstWUResultIterator &results = wsinfo.ensureWorkUnit()->getResults();
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
                    IConstWUResultIterator &vars = wsinfo.ensureWorkUnit()->getVariables();
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

            CTpWrapper topologyWrapper;
            IArrayOf<IEspTpLogicalCluster> clusters;
            topologyWrapper.getTargetClusterList(clusters, req.getClusterType(), NULL);

            StringArray dfuclusters;

            ForEachItemIn(k, clusters)
            {
                IEspTpLogicalCluster& cluster = clusters.item(k);
                dfuclusters.append(cluster.getName());
            }

            resp.setClusterNames(dfuclusters);
        }
        catch(IException* e)
        {
            FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
        }
    }

    return success;
}

bool CwssqlEx::onGetDBSystemInfo(IEspContext &context, IEspGetDBSystemInfoRequest &req, IEspGetDBSystemInfoResponse &resp)
{
    bool success = false;
    resp.setName("HPCC Systems");

    context.ensureFeatureAccess(WSSQLACCESS, SecAccess_Access, -1, "WsSQL::GetDBSystemInfo: Permission denied.");

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
        }

        const char* wssqlbuild_ver = getWsSqlBuildVersion();
        if (wssqlbuild_ver && *wssqlbuild_ver)
        {
            StringBuffer major;
            StringBuffer minor;
            StringBuffer point;
            StringBuffer maturity;

            //5.4.0-trunk1-Debug[heads/wssql-0-gb9e351-dirty
            const char * tail = wssqlbuild_ver;

            while (tail && *tail != '.')
                major.append(*tail++);
            resp.setWsSQLMajor(major.str());

            tail++;
            while (tail && *tail != '.')
                minor.append(*tail++);
            resp.setWsSQLMinor(minor.str());

            tail++;
            while (tail && *tail != '-')
                point.append(*tail++);
            resp.setWsSQLPoint(point.str());

            if (req.getIncludeAll())
            {
                resp.setWsSQLFullVersion(wssqlbuild_ver);

                tail++;
                while (tail && *tail != '-' && *tail != '[')
                    maturity.append(*tail++);
                resp.setWsSQLMaturity(maturity.str());
            }

            success = true;
        }
    }
    catch (...)
    {
        IERRLOG("Error Parsing HPCC and/or WsSQL Version string.");
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
            if (tokenType == ANTLR3_TOKEN_EOF)
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
    ttext   =       nullptr;

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

                if  (theToken)
                {
                    ttext = theToken->toString(theToken);
                    if (theToken->type == ANTLR3_TOKEN_EOF)
                        errorMessage.append(", at <EOF>");
                    else
                        errorMessage.appendf("\n    Near %s\n    ", ttext == nullptr ? (pANTLR3_UINT8)"<no text for the token>" : ttext->chars);
                }
                break;
            }
            case    ANTLR3_TYPE_TREE_PARSER:
            {
                tparser     = (pANTLR3_TREE_PARSER) (recognizer->super);
                is          = tparser->ctnstream->tnstream->istream;
                theBaseTree = (pANTLR3_BASE_TREE)(ex->token);

                if  (theBaseTree)
                {
                    ttext = theBaseTree->toStringTree(theBaseTree);
                    theCommonTree   = (pANTLR3_COMMON_TREE)     theBaseTree->super;

                    if  (theCommonTree != nullptr)
                        theToken = (pANTLR3_COMMON_TOKEN)    theBaseTree->getToken(theBaseTree);

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
                if  (tokenNames == nullptr)
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
                if  (tokenNames == nullptr)
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

HPCCSQLTreeWalker * CwssqlEx::parseSQL(IEspContext &context, StringBuffer & sqltext, bool attemptParameterization)
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

        hpccSqlTreeWalker.setown(new HPCCSQLTreeWalker(sqlAST, context, attemptParameterization));

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
            IERRLOG("!!! Unable to free HPCCSQL parser/lexer objects.");
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
            IERRLOG("!!! Unable to free HPCCSQL parser/lexer objects.");
        }

        //All other unexpected exceptions are reported as generic ecl generation error.
        throw MakeStringException(-1, "Error generating ECL code.");
    }
    return hpccSqlTreeWalker.getLink();
}

bool CwssqlEx::getWUResult(IEspContext &context, const char * wuid, StringBuffer &result, unsigned start, unsigned count, int sequence, const char * dsname, const char * schemaname)
{
    context.addTraceSummaryTimeStamp(LogMin, "StrtgetReslts");
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
           case WUStateCompiled:
           {
               StringBufferAdaptor resultXML(result);
               Owned<IResultSetFactory> factory = getResultSetFactory(context.queryUserId(), context.queryPassword());
               Owned<INewResultSet> nr = factory->createNewResultSet(wuid, sequence, NULL);
               if (nr.get())
               {
                   context.addTraceSummaryTimeStamp(LogMax, "strtgetXMLRslts");
                   getResultXml(resultXML, nr.get(), dsname, start, count, schemaname);
                   context.addTraceSummaryTimeStamp(LogMax, "endgetXMLRslts");
               }
               else
                   return false;
               break;
           }
           default:
               break;
        }
        context.addTraceSummaryTimeStamp(LogMin, "ExitgetRslts");
        return true;
    }
    context.addTraceSummaryTimeStamp(LogMin, "ExitgetRslts");
    return false;
}

bool CwssqlEx::onSetRelatedIndexes(IEspContext &context, IEspSetRelatedIndexesRequest &req, IEspSetRelatedIndexesResponse &resp)
{
    context.ensureFeatureAccess(WSSQLACCESS, SecAccess_Write, -1, "WsSQL::SetRelatedIndexes: Permission denied.");

    StringBuffer username;
    context.getUserID(username);

    const char* passwd = context.queryPassword();

    IArrayOf<IConstRelatedIndexSet>& relatedindexSets = req.getRelatedIndexSets();
    if (relatedindexSets.length() == 0)
        throw MakeStringException(-1, "WsSQL::SetRelatedIndexes empty request detected.");

    ForEachItemIn(relatedindexsetindex, relatedindexSets)
    {
        IConstRelatedIndexSet &relatedIndexSet = relatedindexSets.item(relatedindexsetindex);
        const char * fileName = relatedIndexSet.getFileName();

        if (!fileName || !*fileName)
            throw MakeStringException(-1, "WsSQL::SetRelatedIndexes error: Empty file name detected.");

        StringArray& indexHints = relatedIndexSet.getIndexes();

        int indexHintsCount = indexHints.length();
        if (indexHintsCount > 0)
        {
            Owned<HPCCFile> file = HPCCFileCache::fetchHpccFileByName(fileName,username.str(), passwd, false, false);

            if (!file)
                throw MakeStringException(-1, "WsSQL::SetRelatedIndexes error: could not find file: %s.", fileName);

            StringBuffer description;

            StringBuffer currentIndexes;
            description = file->getDescription();
            HPCCFile::parseOutRelatedIndexes(description, currentIndexes);

            description.append("\nXDBC:RelIndexes=[");
            for(int indexHintIndex = 0; indexHintIndex < indexHintsCount; indexHintIndex++)
            {
                description.appendf("%s%c", indexHints.item(indexHintIndex), (indexHintIndex < indexHintsCount-1 ? ';' : ' '));
            }
            description.append("]\n");
            HPCCFileCache::updateHpccFileDescription(fileName, username, passwd, description.str());
            file->setDescription(description.str());
        }
    }

    resp.setRelatedIndexSets(relatedindexSets);

    return true;
}

bool CwssqlEx::onGetRelatedIndexes(IEspContext &context, IEspGetRelatedIndexesRequest &req, IEspGetRelatedIndexesResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(WSSQLACCESS, SecAccess_Read, -1, "WsSQL::GetRelatedIndexes: Permission denied.");

        StringArray& filenames = req.getFileNames();
        if (filenames.length() == 0)
            throw MakeStringException(-1, "WsSQL::GetRelatedIndexes error: No filenames detected");

        StringBuffer username;
        context.getUserID(username);

        const char* passwd = context.queryPassword();

        IArrayOf<IEspRelatedIndexSet> relatedindexSets;

        ForEachItemIn(filenameindex, filenames)
        {
            const char * fileName = filenames.item(filenameindex);
            Owned<HPCCFile> file = HPCCFileCache::fetchHpccFileByName(fileName,username.str(), passwd, false, false);

            if (file)
            {
                StringArray indexHints;
                file->getRelatedIndexes(indexHints);

                Owned<IEspRelatedIndexSet> relatedIndexSet = createRelatedIndexSet("", "");
                relatedIndexSet->setFileName(fileName);
                relatedIndexSet->setIndexes(indexHints);
                relatedindexSets.append(*relatedIndexSet.getLink());
            }
        }

        resp.setRelatedIndexSets(relatedindexSets);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, -1);
    }
    return true;
}

void CwssqlEx::processMultipleClusterOption(StringArray & clusters, const char  * targetcluster, StringBuffer & hashoptions)
{
    int clusterscount = clusters.length();
    if (clusterscount > 0)
    {
        hashoptions.appendf("\n#OPTION('AllowedClusters', '%s", targetcluster);
        ForEachItemIn(i,clusters)
        {
            if (!isValidCluster(clusters.item(i)))
                throw MakeStringException(-1, "Invalid alternate cluster name: %s", clusters.item(i));

            hashoptions.appendf(",%s", clusters.item(i));
        }
        hashoptions.append("');\n#OPTION('AllowAutoQueueSwitch', TRUE);\n\n");
    }
}

bool CwssqlEx::onExecuteSQL(IEspContext &context, IEspExecuteSQLRequest &req, IEspExecuteSQLResponse &resp)
{
    try
    {
        context.addTraceSummaryTimeStamp(LogMin, "StrtOnExecuteSQL");
        context.ensureFeatureAccess(WSSQLACCESS, SecAccess_Write, -1, "WsSQL::ExecuteSQL: Permission denied.");

        double version = context.getClientVersion();

        StringBuffer sqltext;
        StringBuffer ecltext;
        StringBuffer username;
        context.getUserID(username);
        const char* passwd = context.queryPassword();

        sqltext.set(req.getSqlText());

        if (sqltext.length() <= 0)
            throw MakeStringException(1,"Empty SQL request.");

        const char * cluster = req.getTargetCluster();

        StringBuffer hashoptions;
        if (version > 3.03)
        {
            StringArray & alternates = req.getAlternateClusters();
            if (alternates.length() > 0)
                processMultipleClusterOption(alternates, cluster, hashoptions);
        }

        SCMStringBuffer compiledwuid;
        int resultLimit = req.getResultLimit();
        __int64 resultWindowStart = req.getResultWindowStart();
        __int64 resultWindowCount = req.getResultWindowCount();

        if (resultWindowStart < 0 || resultWindowCount <0 )
           throw MakeStringException(-1,"Invalid result window value");

        bool clonable = false;
        bool cacheeligible =  (version > 3.04 ) ? !req.getIgnoreCache() : true;

        Owned<HPCCSQLTreeWalker> parsedSQL;
        ESPLOG(LogNormal, "WsSQL: Parsing sql query...");
        parsedSQL.setown(parseSQL(context, sqltext));
        ESPLOG(LogNormal, "WsSQL: Finished parsing sql query...");

        SQLQueryType querytype = parsedSQL->getSqlType();
        if (querytype == SQLTypeCall)
        {
            if (strlen(parsedSQL->getQuerySetName())==0)
            {
                if (strlen(req.getTargetQuerySet())==0)
                    throw MakeStringException(-1,"Missing Target QuerySet.");
                else
                    parsedSQL->setQuerySetName(req.getTargetQuerySet());
            }
            ESPLOG(LogMax, "WsSQL: Processing call query...");

            WsEclWuInfo wsinfo("", parsedSQL->getQuerySetName(), parsedSQL->getStoredProcName(), username.str(), passwd);
            compiledwuid.set(wsinfo.ensureWuid());

            clonable = true;
        }
        else if (querytype == SQLTypeCreateAndLoad)
        {
           cacheeligible = false;
        }

        StringBuffer xmlparams;
        StringBuffer normalizedSQL = parsedSQL->getNormalizedSQL();

        normalizedSQL.append(" | --TC=").append(cluster);
        if (username.length() > 0)
           normalizedSQL.append("--USER=").append(username.str());
        if (resultLimit > 0)
           normalizedSQL.append("--HARDLIMIT=").append(resultLimit);
        const char * wuusername = req.getUserName();
        if (wuusername && *wuusername)
           normalizedSQL.append("--WUOWN=").append(wuusername);
        if (hashoptions.length()>0)
           normalizedSQL.append("--HO=").append(hashoptions.str());

        if (compiledwuid.length() != 0)
           normalizedSQL.append("--PWUID=").append(compiledwuid.str());

        ESPLOG(LogMax, "WsSQL: getWorkUnitFactory...");
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());

        ESPLOG(LogMax, "WsSQL: checking query cache...");
        if(cacheeligible && getCachedQuery(normalizedSQL.str(), compiledwuid.s))
        {
           ESPLOG(LogMax, "WsSQL: cache hit opening wuid %s...", compiledwuid.str());
           Owned<IConstWorkUnit> cw = factory->openWorkUnit(compiledwuid.str(), false);
           if (!cw)//cache hit but unavailable WU
           {
               ESPLOG(LogMax, "WsSQL: cache hit but unavailable WU...");
               removeQueryFromCache(normalizedSQL.str());
               compiledwuid.clear();
           }
           else
               clonable = true;
        }

        if (compiledwuid.length()==0)
        {
            {
                if (isEmpty(cluster))
                    throw MakeStringException(-1,"Target cluster not set.");

                if (!isValidCluster(cluster))
                    throw MakeStringException(-1, "Invalid cluster name: %s", cluster);

                if (querytype == SQLTypeCreateAndLoad)
                    clonable = false;

                context.addTraceSummaryTimeStamp(LogNormal, "StartECLGenerate");
                ECLEngine::generateECL(parsedSQL, ecltext);

                if (hashoptions.length() > 0)
                    ecltext.insert(0, hashoptions.str());

                context.addTraceSummaryTimeStamp(LogNormal, "EndECLGenerate");

                if (isEmpty(ecltext))
                   throw MakeStringException(1,"Could not generate ECL from SQL.");

                ecltext.appendf(EMBEDDEDSQLQUERYCOMMENT, sqltext.str(), normalizedSQL.str());

#if defined _DEBUG
                fprintf(stderr, "GENERATED ECL:\n%s\n", ecltext.str());
#endif

                ESPLOG(LogMax, "WsSQL: creating new WU...");
                NewWsWorkunit wu(context);
                compiledwuid.set(wu->queryWuid());

                wu->setJobName("WsSQL Job");

                wu.setQueryText(ecltext.str());
                wu->setClusterName(cluster);
                if (clonable)
                    wu->setCloneable(true);
                wu->setAction(WUActionCompile);
                if (resultLimit)
                    wu->setResultLimit(resultLimit);

                if (wuusername && *wuusername)
                    wu->setUser(wuusername);

                wu->commit();
                wu.clear();

                context.addTraceSummaryTimeStamp(LogNormal, "strtWUCompile");
                WsWuHelpers::submitWsWorkunit(context, compiledwuid.str(), cluster, NULL, 0, true, false, false, NULL, NULL, NULL);
                waitForWorkUnitToCompile(compiledwuid.str(), req.getWait());
                context.addTraceSummaryTimeStamp(LogNormal, "endWUCompile");
            }
        }

        ESPLOG(LogMax, "WsSQL: opening WU...");
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
            if (querytype == SQLTypeCall)
                createWUXMLParams(xmlparams, parsedSQL, NULL, cw);
            else if (querytype == SQLTypeSelect)
            {
                if (notEmpty(cluster) && !isValidCluster(cluster))
                    throw MakeStringException(ECLWATCH_INVALID_CLUSTER_NAME, "Invalid cluster name: %s", cluster);

                createWUXMLParams(xmlparams, parsedSQL->getParamList());
            }

            StringBuffer runningwuid;

            if (clonable)
            {
                context.addTraceSummaryTimeStamp(LogNormal, "StartWUCloneExe");
                cloneAndExecuteWU(context, compiledwuid.str(), runningwuid, xmlparams.str(), NULL, NULL, cluster);
                context.addTraceSummaryTimeStamp(LogNormal, "EndWUCloneExe");
                if(cacheeligible && !isQueryCached(normalizedSQL.str()))
                    addQueryToCache(normalizedSQL.str(), compiledwuid.str());
            }
            else
            {
                context.addTraceSummaryTimeStamp(LogNormal, "StartWUSubmit");
                WsWuHelpers::submitWsWorkunit(context, compiledwuid.str(), cluster, NULL, 0, false, true, true, NULL, NULL, NULL);
                context.addTraceSummaryTimeStamp(LogNormal, "EndWUSubmit");
                runningwuid.set(compiledwuid.str());
                if (cacheeligible)
                    addQueryToCache(normalizedSQL.str(), runningwuid.str());
            }

            int timeToWait = req.getWait();
            if (timeToWait != 0)
            {
                context.addTraceSummaryTimeStamp(LogNormal, "StartWUProcessWait");
                waitForWorkUnitToComplete(runningwuid.str(), timeToWait);
                context.addTraceSummaryTimeStamp(LogNormal, "EndWUProcessWait");
            }

            if (strcmp(runningwuid.str(), compiledwuid.str())!=0)
                resp.setParentWuId(compiledwuid.str());

            resp.setResultLimit(resultLimit);
            resp.setResultWindowCount( (unsigned)resultWindowCount);
            resp.setResultWindowStart( (unsigned)resultWindowStart);

            if (!req.getSuppressResults())
            {
                StringBuffer result;
                if (getWUResult(context, runningwuid.str(), result, (unsigned)resultWindowStart, (unsigned)resultWindowCount, 0, WSSQLRESULT, req.getSuppressXmlSchema() ? NULL : WSSQLRESULTSCHEMA))
                {
                    StringBuffer count;
                    if (getWUResult( context, runningwuid.str(), count , 0, 1, 1, WSSQLCOUNT, NULL))
                        result.append(count.str());
                    resp.setResult(result.str());
                }
            }

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
    context.addTraceSummaryTimeStamp(LogMin, "EndOnExecuteSQL");
    return true;
}

void CwssqlEx::createWUXMLParams(StringBuffer & xmlparams, const IArrayOf <ISQLExpression> * parameterlist)
{
    xmlparams.append("<root>");
    for (int expindex = 0; expindex < parameterlist->length(); expindex++)
    {
        ISQLExpression * exp = &parameterlist->item(expindex);
        if (exp->getExpType() == Value_ExpressionType)
        {
            SQLValueExpression * currentvalplaceholder = static_cast<SQLValueExpression *>(exp);
            currentvalplaceholder->trimTextQuotes();
            xmlparams.appendf("<%s>", currentvalplaceholder->getPlaceHolderName());
            encodeXML(currentvalplaceholder->getValue(), xmlparams);
            xmlparams.appendf("</%s>", currentvalplaceholder->getPlaceHolderName());
        }
        else
            ESPLOG(LogNormal, "WsSQL: attempted to create XML params from unexpected expression type.");
    }
    xmlparams.append("</root>");
    DBGLOG("XML PARAMS: %s", xmlparams.str());
}

//Integrates all "variables" into "param" based xml
void CwssqlEx::createWUXMLParams(StringBuffer & xmlparams, HPCCSQLTreeWalker* parsedSQL, IArrayOf<IConstNamedValue> *variables, IConstWorkUnit * cw)
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
                char * value = ((char *)item.getValue());

                if (value && *value)
                {
                    while(value && isspace(*value)) //fast trim left
                        value++;

                    int len = strlen(value);
                    while(len && isspace(value[len-1]))
                        len--;

                    value[len] = '\0';//fast trim right, even if len didn't change

                    if (len >= 2)
                    {
                        //WU cloning mechanism doesn't handle quoted strings very well...
                        //We're forced to blindly remove them here...
                        if (value[0] == '\'' && value[len-1] == '\'')
                        {
                            value[len-1] = '\0'; //clip rightmost quote
                            value++; //clip leftmost quote
                        }
                    }

                    if(len)
                      encodeXML(value, xmlparams);
                    // else ??
                }

                xmlparams.append("</").append(varname.s.str()).append(">");
            }
            xmlparams.append("</root>");
        }
    }
}

bool CwssqlEx::onExecutePreparedSQL(IEspContext &context, IEspExecutePreparedSQLRequest &req, IEspExecutePreparedSQLResponse &resp)
{
   try
   {
       context.ensureFeatureAccess(WSSQLACCESS, SecAccess_Write, -1, "WsSQL::ExecutePreparedSQL: Permission denied.");

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
           createWUXMLParams(xmlparams, NULL, &req.getVariables(),cw);

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

           __int64 resultWindowStart = req.getResultWindowStart();
           __int64 resultWindowCount = req.getResultWindowCount();

           if (resultWindowStart < 0 || resultWindowCount <0 )
               throw MakeStringException(-1,"Invalid result window value");

           if (!req.getSuppressResults())
           {
               StringBuffer result;
               if (getWUResult(context, runningWuId.str(), result, (unsigned)resultWindowStart, (unsigned)resultWindowCount, 0, WSSQLRESULT, req.getSuppressXmlSchema() ? NULL : WSSQLRESULTSCHEMA))
                   resp.setResult(result.str());
           }

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

bool CwssqlEx::isQueryCached(const char * sqlQuery)
{
    CriticalBlock block(critCache);
    return (sqlQuery && cachedSQLQueries.find(sqlQuery) != cachedSQLQueries.end());
}

bool CwssqlEx::getCachedQuery(const char * sqlQuery, StringBuffer & wuid)
{
    CriticalBlock block(critCache);
    if(sqlQuery && cachedSQLQueries.find(sqlQuery) != cachedSQLQueries.end())
    {
        wuid.set(cachedSQLQueries.find(sqlQuery)->second.c_str());
        return true;
    }
    return false;
}

void CwssqlEx::removeQueryFromCache(const char * sqlQuery)
{
    CriticalBlock block(critCache);
    cachedSQLQueries.erase(sqlQuery);
}

bool CwssqlEx::addQueryToCache(const char * sqlQuery, const char * wuid)
{
    if (sqlQuery && *sqlQuery && wuid && *wuid)
    {
        CriticalBlock block(critCache);
        if (isCacheExpired())
        {
            ESPLOG(LogNormal, "WsSQL: Query Cache has expired and is being flushed.");
            //Flushing cache logic could have been in dedicated function, but
            //putting it here makes this action more atomic, less synchronization concerns
            cachedSQLQueries.clear();
            setNewCacheFlushTime();
        }

        cachedSQLQueries.insert(std::pair<std::string,std::string>(sqlQuery, wuid));
    }
    return false;
}

bool CwssqlEx::onPrepareSQL(IEspContext &context, IEspPrepareSQLRequest &req, IEspPrepareSQLResponse &resp)
{
    bool success = false;
    StringBuffer sqltext;
    StringBuffer ecltext;
    bool clonable = false;
    try
    {
        context.ensureFeatureAccess(WSSQLACCESS, SecAccess_Write, -1, "WsSQL::PrepareSQL: Permission denied.");

        double version = context.getClientVersion();

        StringBuffer username;
        context.getUserID(username);
        const char* passwd = context.queryPassword();

        sqltext.set(req.getSqlText());

        if (sqltext.length() <= 0)
            throw MakeStringException(1,"Empty SQL request.");

        Owned<HPCCSQLTreeWalker> parsedSQL;
        parsedSQL.setown(parseSQL(context, sqltext, false));

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

        const char * cluster = req.getTargetCluster();

        StringBuffer hashoptions;
        if (version > 3.03)
        {
            StringArray & alternates = req.getAlternateClusters();
            if (alternates.length() > 0)
                processMultipleClusterOption(alternates, cluster, hashoptions);
        }

        StringBuffer xmlparams;
        StringBuffer normalizedSQL = parsedSQL->getNormalizedSQL();
        normalizedSQL.append(" | --TC=").append(cluster);
        if (username.length() > 0)
            normalizedSQL.append("--USER=").append(username.str());
        if (hashoptions.length()>0)
            normalizedSQL.append("--HO=").append(hashoptions.str());

        Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());

        SCMStringBuffer wuid;
        if(getCachedQuery(normalizedSQL.str(), wuid.s))
        {
            Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid.str(), false);
            if (!cw)//cache hit but unavailable WU
            {
                removeQueryFromCache(normalizedSQL.str());
                wuid.clear();
            }
        }

        if(wuid.length()==0)
        {
            if (parsedSQL->getSqlType() == SQLTypeCall)
            {
                WsEclWuInfo wsinfo("", parsedSQL->getQuerySetName(), parsedSQL->getStoredProcName(), username.str(), passwd);
                wuid.set(wsinfo.ensureWuid());

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
                if (isEmpty(cluster))
                    throw MakeStringException(1,"Target cluster not set.");

                if (!isValidCluster(cluster))
                    throw MakeStringException(-1/*ECLWATCH_INVALID_CLUSTER_NAME*/, "Invalid cluster name: %s", cluster);

                ECLEngine::generateECL(parsedSQL, ecltext);
                if (hashoptions.length() > 0)
                    ecltext.insert(0, hashoptions.str());

#if defined _DEBUG
                fprintf(stderr, "GENERATED ECL:\n%s\n", ecltext.str());
#endif

                if (isEmpty(ecltext))
                    throw MakeStringException(1,"Could not generate ECL from SQL.");

                //ecltext.appendf("\n\n/****************************************************\nOriginal SQL:     \"%s\"\nNormalized SQL: \"%s\"\n****************************************************/\n", sqltext.str(), normalizedSQL.str());
                ecltext.appendf(EMBEDDEDSQLQUERYCOMMENT, sqltext.str(), normalizedSQL.str());

                NewWsWorkunit wu(context);
                wuid.set(wu->queryWuid());
                wu->setClusterName(cluster);
                wu->setCloneable(true);
                wu->setAction(WUActionCompile);
                wu.setQueryText(ecltext.str());
                wu->setJobName("WsSQL PreparedQuery Job");

                StringBuffer xmlparams;
                createWUXMLParams(xmlparams, parsedSQL, NULL, NULL);

                wu->commit();
                wu.clear();

                WsWuHelpers::submitWsWorkunit(context, wuid.str(), cluster, NULL, 0, true, false, false, xmlparams.str(), NULL, NULL);
                success = waitForWorkUnitToCompile(wuid.str(), req.getWait());
            }

           if (success)
               addQueryToCache(normalizedSQL.str(), wuid.s.str());
        }

        WsWuInfo winfo(context, wuid.str());

        winfo.getCommon(resp.updateWorkunit(), WUINFO_All);
        winfo.getExceptions(resp.updateWorkunit(), WUINFO_All);

        StringBuffer result;
        getWUResult(context, wuid.str(), result, 0, 0, 0, WSSQLRESULT, WSSQLRESULTSCHEMA);
        resp.setResult(result);

        AuditSystemAccess(context.queryUserId(), true, "Updated %s", wuid.str());
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, -1);
    }

    return true;
}

bool CwssqlEx::executePublishedQueryByName(IEspContext &context, const char * queryset, const char * queryname, StringBuffer &clonedwuid, const char *paramXml, IArrayOf<IConstNamedValue> *variables, const char * targetcluster, int start, int count)
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

        success = executePublishedQueryByWuId(context, wsinfo.ensureWuid(), clonedwuid, paramXml, variables, targetcluster, start, count);
    }
    else
        success = false;

    return success;
}

bool CwssqlEx::executePublishedQueryByWuId(IEspContext &context, const char * targetwuid, StringBuffer &clonedwuid, const char *paramXml, IArrayOf<IConstNamedValue> *variables, const char * targetcluster, int start, int count)
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

bool CwssqlEx::executePublishedQuery(IEspContext &context, const char * queryset, const char * queryname, StringBuffer &resp, int start, int count, int waittime)
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
        cloneAndExecuteWU(context, wsinfo.ensureWuid(), clonedwui, NULL, NULL, NULL, "");

        if (waittime != 0)
            waitForWorkUnitToComplete(clonedwui.str(), waittime);

        Owned<IConstWorkUnit> cw = factory->openWorkUnit(clonedwui.str(), false);
        if (!cw)
            throw MakeStringException(ECLWATCH_CANNOT_UPDATE_WORKUNIT,"Cannot open workunit %s.", clonedwui.str());

        getWUResult(context, clonedwui.str(), resp, start, count, 0, WSSQLRESULT,  WSSQLRESULTSCHEMA);
    }
    else
        success = false;

    return success;
}

bool CwssqlEx::executePublishedQuery(IEspContext &context, const char * wuid, StringBuffer &resp, int start, int count, int waittime)
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
        cloneAndExecuteWU(context, wsinfo.ensureWuid(), clonedwui, NULL, NULL, NULL, "");

        if (waittime != 0)
            waitForWorkUnitToComplete(clonedwui.str(), waittime);

        Owned<IConstWorkUnit> cw = factory->openWorkUnit(clonedwui.str(), false);
        if (!cw)
            throw MakeStringException(ECLWATCH_CANNOT_UPDATE_WORKUNIT,"Cannot open workunit %s.", clonedwui.str());

        getWUResult(context, clonedwui.str(), resp, start, count, 0, WSSQLRESULT, WSSQLRESULTSCHEMA);

    }
    else
        success = false;

    return success;
}

bool CwssqlEx::cloneAndExecuteWU(IEspContext &context, const char * originalwuid, StringBuffer &clonedwuid, const char *paramXml, IArrayOf<IConstNamedValue> *variables, IArrayOf<IConstNamedValue> *debugs, const char * targetcluster)
{
    bool success = true;
    try
    {
       Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());

       if (originalwuid && *originalwuid)
       {
           if (!looksLikeAWuid(originalwuid, 'W'))
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

           WsWuHelpers::runWsWorkunit(
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

bool CwssqlEx::onCreateTableAndLoad(IEspContext &context, IEspCreateTableAndLoadRequest &req, IEspCreateTableAndLoadResponse &resp)
{
    context.ensureFeatureAccess(WSSQLACCESS, SecAccess_Write, -1, "WsSQL::CreateTableAndLoad: Permission denied.");

    bool success = true;

    const char * targetTableName = req.getTableName();
    if (!targetTableName || !*targetTableName)
        throw MakeStringException(-1, "WsSQL::CreateTableAndLoad: Error: TableName cannot be empty.");

    if (!HPCCFile::validateFileName(targetTableName))
        throw MakeStringException(-1, "WsSQL::CreateTableAndLoad: Error: Target TableName is invalid: %s.", targetTableName);

    const char * cluster = req.getTargetCluster();
    if (notEmpty(cluster) && !isValidCluster(cluster))
        throw MakeStringException(ECLWATCH_INVALID_CLUSTER_NAME, "WsSQL::CreateTableAndLoad: Invalid cluster name: %s", cluster);

    IConstDataSourceInfo & datasource = req.getDataSource();

    StringBuffer sourceDataFileName;
    sourceDataFileName.set(datasource.getSprayedFileName()).trim();

    if (sourceDataFileName.length() == 0)
    {
        sourceDataFileName.set(datasource.getLandingZoneFileName());
        if (sourceDataFileName.length() == 0)
            throw MakeStringException(-1, "WsSQL::CreateTableAndLoad: Error: Data Source File Name cannot be empty, provide either sprayed file name, or landing zone file name.");

        const char * lzIP = datasource.getLandingZoneIP();
        if (!lzIP || !*lzIP)
            throw MakeStringException(-1, "WsSQL::CreateTableAndLoad: Error: LandingZone IP cannot be empty if targeting a landing zone file.");

        StringBuffer lzPath = datasource.getLandingZonePath();

        if (!lzPath.length())
            throw MakeStringException(-1, "WsSQL::CreateTableAndLoad: Error: Landingzone path cannot be empty.");

        addPathSepChar(lzPath);

        RemoteFilename rfn;
        SocketEndpoint ep(lzIP);

        rfn.setPath(ep, lzPath.append(sourceDataFileName.str()).str());

        CDfsLogicalFileName dlfn;
        dlfn.setExternal(rfn);
        dlfn.get(sourceDataFileName.clear(), false, false);
    }

    IConstDataType & format = req.getDataSourceType();

    const char * formatname = "";
    CHPCCFileType formattype = format.getType();

    switch (formattype)
    {
        case CHPCCFileType_FLAT:
            formatname = "FLAT";
            break;
        case CHPCCFileType_CSV:
            formatname = "CSV";
            break;
        case CHPCCFileType_JSON:
            formatname = "JSON";
            break;
        case CHPCCFileType_XML:
            formatname = "XML";
            break;
        default:
            throw MakeStringException(-1, "WsSQL::CreateTableAndLoad: Error: Invalid file format detected.");
    }

    StringBuffer ecl;
    StringBuffer recDef;
    ecl.set("import std;\n");
    {
        IArrayOf<IConstEclFieldDeclaration>& eclFields = req.getEclFields();
        if (eclFields.length() == 0)
            throw MakeStringException(-1, "WsSQL::CreateTableAndLoad: Error: Empty record definition detected.");


        recDef.set("TABLERECORDDEF := RECORD\n");
        ForEachItemIn(fieldindex, eclFields)
        {
            IConstEclFieldDeclaration &eclfield = eclFields.item(fieldindex);
            IConstEclFieldType &ecltype = eclfield.getEclFieldType();

            const char * name = "";
            CHPCCFieldType format = ecltype.getType();
            switch (format)
            {
                case CHPCCFieldType_BOOLEAN:
                    name = "BOOLEAN";
                    break;
                case CHPCCFieldType_INTEGER:
                    name = "INTEGER";
                    break;
                case CHPCCFieldType_xUNSIGNED:
                    name = "UNSIGNED";
                    break;
                case CHPCCFieldType_REAL:
                    name = "REAL";
                    break;
                case CHPCCFieldType_DECIMAL:
                    name = "DECIMAL";
                    break;
                case CHPCCFieldType_xSTRING:
                    name = "STRING";
                    break;
                case CHPCCFieldType_QSTRING:
                    name = "QSTRING";
                    break;
                case CHPCCFieldType_UNICODE:
                    name = "UNICODE";
                    break;
                case CHPCCFieldType_DATA:
                    name = "DATA";
                    break;
                case CHPCCFieldType_VARSTRING:
                    name = "VARSTRING";
                    break;
                case CHPCCFieldType_VARUNICODE:
                    name = "VARUNICODE";
                    break;
                default:
                    throw MakeStringException(-1, "WsSQL::CreateTableAndLoad: Error: Unrecognized field type detected.");
            }

            int len                = ecltype.getLength();
            const char * locale    = ecltype.getLocale();
            int precision          = ecltype.getPrecision();

            recDef.appendf("\t%s", name);
            if (len > 0)
            {
                if(isdigit(recDef.charAt(recDef.length() - 1)))
                    recDef.append("_");
                recDef.append(len);
            }

            if (locale && *locale)
                recDef.append(locale);

            if (precision > 0)
                recDef.appendf("_%d", precision);

            recDef.appendf(" %s;\n", eclfield.getFieldName());
        }
        recDef.append("END;\n");
    }

    ecl.append(recDef.str());

    bool overwrite = req.getOverwrite();

    StringBuffer formatnamefull = formatname;
    IArrayOf<IConstDataTypeParam> & formatparams = format.getParams();
    int formatparamscount = formatparams.length();
    if (formatparamscount > 0 )
    {
        formatnamefull.append("(");
        for (int paramindex = 0; paramindex < formatparamscount; paramindex++)
        {
            IConstDataTypeParam &paramitem = formatparams.item(paramindex);
            const char * paramname = paramitem.getName();
            if (!paramname || !*paramname)
                throw MakeStringException(-1, "WsSQL::CreateTableAndLoad: Error: Format type '%s' appears to have unnamed parameter(s).", formatname);

            StringArray & paramvalues = paramitem.getValues();
            int paramvalueslen = paramvalues.length();
            formatnamefull.appendf("%s(", paramname);
            if (paramvalueslen > 1)
                formatnamefull.append("[");

            for (int paramvaluesindex = 0; paramvaluesindex < paramvalueslen; paramvaluesindex++)
            {
                formatnamefull.appendf("'%s'%s", paramvalues.item(paramvaluesindex), paramvaluesindex < paramvalueslen-1 ? "," : "");
            }
            if (paramvalueslen > 1)
                formatnamefull.append("]");
            formatnamefull.append(")");
            if (paramindex < formatparamscount-1)
                formatnamefull.append(",");
        }
        formatnamefull.append(")");
    }
    ecl.appendf("\nFILEDATASET := DATASET('~%s', TABLERECORDDEF, %s);\n",sourceDataFileName.str(), formatnamefull.str());
    ecl.appendf("OUTPUT(FILEDATASET, ,'~%s'%s);", targetTableName, overwrite ? ", OVERWRITE" : "");

    const char * description = req.getTableDescription();
    if (description && * description)
        ecl.appendf("\nStd.file.setfiledescription('~%s','%s')", targetTableName, description);

    ESPLOG(LogMax, "WsSQL: creating new WU...");

    NewWsWorkunit wu(context);
    SCMStringBuffer compiledwuid;
    compiledwuid.set(wu->queryWuid());

    wu->setJobName("WsSQL Create table");
    wu.setQueryText(ecl.str());

    wu->setClusterName(cluster);

    wu->setAction(WUActionCompile);

    const char * wuusername = req.getOwner();
    if (wuusername && *wuusername)
        wu->setUser(wuusername);

    wu->commit();
    wu.clear();

    ESPLOG(LogMax, "WsSQL: compiling WU...");
    WsWuHelpers::submitWsWorkunit(context, compiledwuid.str(), cluster, NULL, 0, true, false, false, NULL, NULL, NULL);
    waitForWorkUnitToCompile(compiledwuid.str(), req.getWait());

    ESPLOG(LogMax, "WsSQL: finish compiling WU...");

    ESPLOG(LogMax, "WsSQL: opening WU...");
    Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
    Owned<IConstWorkUnit> cw = factory->openWorkUnit(compiledwuid.str(), false);

    if (!cw)
        throw MakeStringException(ECLWATCH_CANNOT_UPDATE_WORKUNIT,"Cannot open WorkUnit %s.", compiledwuid.str());

    WsWUExceptions errors(*cw);
    if (errors.ErrCount()>0)
    {
        WsWuInfo winfo(context, compiledwuid.str());
        winfo.getExceptions(resp.updateWorkunit(), WUINFO_All);
        success = false;
    }
    else
    {
        ESPLOG(LogMax, "WsSQL: executing WU(%s)...", compiledwuid.str());
        WsWuHelpers::submitWsWorkunit(context, compiledwuid.str(), cluster, NULL, 0, false, true, true, NULL, NULL, NULL);

        ESPLOG(LogMax, "WsSQL: waiting on WU(%s)...", compiledwuid.str());
        waitForWorkUnitToComplete(compiledwuid.str(), req.getWait());
        ESPLOG(LogMax, "WsSQL: finished waiting on WU(%s)...", compiledwuid.str());

        Owned<IConstWorkUnit> rw = factory->openWorkUnit(compiledwuid.str(), false);

        if (!rw)
            throw MakeStringException(-1,"WsSQL: Cannot verify create and load request success.");

        WsWuInfo winfo(context, compiledwuid.str());
        WsWUExceptions errors(*rw);
        if (errors.ErrCount() > 0 )
        {
            winfo.getExceptions(resp.updateWorkunit(), WUINFO_All);
            success = false;
        }

        winfo.getCommon(resp.updateWorkunit(), WUINFO_All);
        resp.setSuccess(success);
        resp.setEclRecordDefinition(recDef.str());
        resp.setTableName(targetTableName);
    }

    return success;
}

bool CwssqlEx::onGetResults(IEspContext &context, IEspGetResultsRequest &req, IEspGetResultsResponse &resp)
{
    context.ensureFeatureAccess(WSSQLACCESS, SecAccess_Read, -1, "WsSQL::GetResults: Permission denied.");

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
            if (getWUResult(context, parentWuId, result, (unsigned)resultWindowStart, (unsigned)resultWindowCount, 0, WSSQLRESULT, req.getSuppressXmlSchema() ? NULL : WSSQLRESULTSCHEMA))
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

void CwssqlEx::refreshValidClusters()
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

bool CwssqlEx::isValidCluster(const char *cluster)
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

bool CwssqlEx::publishWorkunit(IEspContext &context, const char * queryname, const char * wuid, const char * targetcluster)
{
    Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
    Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid, false);
    if (!cw)
        throw MakeStringException(ECLWATCH_CANNOT_OPEN_WORKUNIT,"Cannot find the workunit %s", wuid);

    SCMStringBuffer queryName;
    if (notEmpty(queryname))
        queryName.set(queryname);
    else
        queryName.set(cw->queryJobName());

    if (!queryName.length())
        throw MakeStringException(ECLWATCH_MISSING_PARAMS, "Query/Job name not defined for publishing workunit %s", wuid);

    SCMStringBuffer target;
    if (notEmpty(targetcluster))
        target.set(targetcluster);
    else
        target.set(cw->queryClusterName());

    if (!target.length())
        throw MakeStringException(ECLWATCH_MISSING_PARAMS, "Cluster name not defined for publishing workunit %s", wuid);
    if (!isValidCluster(target.str()))
        throw MakeStringException(ECLWATCH_INVALID_CLUSTER_NAME, "Invalid cluster name: %s", target.str());
    //RODRIGO this is needed:
    //copyQueryFilesToCluster(context, cw, "", target.str(), queryName.str(), false);

    WorkunitUpdate wu(&cw->lock());
    wu->setJobName(queryName.str());

    StringBuffer queryId;

    addQueryToQuerySet(wu, target.str(), queryName.str(), MAKE_ACTIVATE, queryId, context.queryUserId());

    wu->commit();
    wu.clear();

    return true;
}

