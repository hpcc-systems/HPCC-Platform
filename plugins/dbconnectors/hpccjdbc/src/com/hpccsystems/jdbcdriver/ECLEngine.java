package com.hpccsystems.jdbcdriver;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class ECLEngine
{

    private String                  urlString;
    private HPCCQuery               hpccQuery;
    private final String            datasetName;
    private NodeList                resultSchema;
    private final Properties        props;
    private ArrayList<SQLWarning>   warnings;
    private SQLParser               parser;
    private HPCCDatabaseMetaData    dbMetadata;
    private HashMap<String, String> indexToUseMap;
    private HashMap<String, String> eclEntities;
    private HashMap<String, String> eclDSSourceMapping;

    public ECLEngine(SQLParser parser, HPCCDatabaseMetaData dbmetadata, Properties props, HPCCQuery query)
    {
        this.props = props;
        this.dbMetadata = dbmetadata;
        this.parser = parser;
        this.datasetName = null;
        this.resultSchema = null;
        this.hpccQuery = query;
        this.eclEntities = new HashMap<String, String>();
        this.indexToUseMap = new HashMap<String, String>();
        this.eclDSSourceMapping = new HashMap<String, String>();
    }

    public ECLEngine(SQLParser parser, HPCCDatabaseMetaData dbmetadata, Properties props,
            HashMap<String, String> indextouse)
    {
        this.props = props;
        this.dbMetadata = dbmetadata;
        this.parser = parser;
        this.indexToUseMap = indextouse;
        this.datasetName = null;
        this.resultSchema = null;
        this.hpccQuery = null;
        this.eclEntities = new HashMap<String, String>();
        this.eclDSSourceMapping = new HashMap<String, String>();
    }

    public ArrayList executeSelectConstant(String eclstring)
    {
        try
        {
            urlString = "http://" + props.getProperty("WsECLDirectAddress") + ":"
                    + props.getProperty("WsECLDirectPort") + "/EclDirect/RunEcl?Submit";

            if (props.containsKey("Cluster"))
            {
                urlString += "&cluster=";
                urlString += props.getProperty("Cluster");
            }
            else
                System.out.println("No cluster property found, executing query on EclDirect default cluster");

            urlString += "&eclText=";
            urlString += URLEncoder.encode(eclstring, "UTF-8");

            System.out.println("WSECL:executeSelect: " + urlString);

            long startTime = System.currentTimeMillis();

            URL url = new URL(urlString);
            HttpURLConnection conn = dbMetadata.createHPCCESPConnection(url);

            return parse(conn.getInputStream(), startTime);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public ArrayList execute() throws Exception
    {
        int sqlReqType = parser.getSqlType();

        List<HPCCColumnMetaData> expectedretcolumns = new ArrayList();
        String queryFileName = "";
        String indexPosField = null;
        StringBuilder keyedAndWild = new StringBuilder();
        DFUFile indexFileToUse = null;
        StringBuilder eclCode = new StringBuilder("");
        int totalParamCount = 0;
        boolean isPayloadIndex = false;

        switch (sqlReqType)
        {
            case SQLParser.SQL_TYPE_SELECT:
            {
                //Currently, query table is always 0th index.
                queryFileName = HPCCJDBCUtils.handleQuotedString(parser.getTableName(0));
                if (!dbMetadata.tableExists("", queryFileName))
                    throw new Exception("Invalid table found: " + queryFileName);

                DFUFile hpccQueryFile = dbMetadata.getDFUFile(queryFileName);

                expectedretcolumns = parser.getSelectColumns();

                totalParamCount = parser.getWhereClauseExpressionsCount();

                eclEntities.put("PARAMCOUNT", Integer.toString(totalParamCount));

                String tempindexname = indexToUseMap.get(queryFileName);
                if (tempindexname != null)
                {
                    System.out.print("Generating ECL using index file: " + tempindexname);
                    indexFileToUse = dbMetadata.getDFUFile(tempindexname);
                    indexPosField = indexFileToUse.getIdxFilePosField();

                    isPayloadIndex = processIndex(indexFileToUse, keyedAndWild);

                    eclEntities.put("KEYEDWILD", keyedAndWild.toString());
                    if (isPayloadIndex)
                        eclEntities.put("PAYLOADINDEX", "true");

                    eclDSSourceMapping.put(queryFileName, "IdxDS");

                    StringBuffer idxsetupstr = new StringBuffer();
                    idxsetupstr.append("Idx := INDEX(Tbl1DS, {")
                            .append(indexFileToUse.getKeyedFieldsAsDelmitedString(',', null)).append("}");

                    if (indexFileToUse.getNonKeyedColumnsCount() > 0)
                        idxsetupstr.append(",{ ").append(indexFileToUse.getNonKeyedFieldsAsDelmitedString(',', null))
                                .append(" }");

                    idxsetupstr.append(",\'~").append(indexFileToUse.getFullyQualifiedName()).append("\');\n");

                    eclEntities.put("IndexDef", idxsetupstr.toString());

                    idxsetupstr.setLength(0);

                    if (isPayloadIndex)
                    {
                        System.out.println(" as PAYLOAD");
                        idxsetupstr.append("IdxDS := Idx(").append(keyedAndWild.toString()).append(");\n");
                    }
                    else
                    {
                        System.out.println(" Not as PAYLOAD");
                        idxsetupstr.append("IdxDS := FETCH(Tbl1DS, Idx( ").append(keyedAndWild.toString())
                                .append("), RIGHT.").append(indexFileToUse.getIdxFilePosField()).append(");\n");
                    }
                    eclEntities.put("IndexRead", idxsetupstr.toString());

                }
                else
                    System.out.println("NOT USING INDEX!");

                if (hpccQueryFile.hasFileRecDef())
                {
                    if (indexFileToUse != null && indexPosField != null)
                        eclCode.append(hpccQueryFile.getFileRecDefwithIndexpos(
                                indexFileToUse.getFieldMetaData(indexPosField), "Tbl1RecDef"));
                    else
                        eclCode.append(hpccQueryFile.getFileRecDef("Tbl1RecDef"));
                    eclCode.append("\n");
                }
                else
                    throw new Exception("Target HPCC file (" + queryFileName
                            + ") does not contain ECL record definition");

                if (!eclDSSourceMapping.containsKey(queryFileName))
                    eclDSSourceMapping.put(queryFileName, "Tbl1DS");

                if (!hpccQueryFile.isKeyFile())
                    eclCode.append("Tbl1DS := DATASET(\'~").append(hpccQueryFile.getFullyQualifiedName())
                            .append("\', Tbl1RecDef,").append(hpccQueryFile.getFormat()).append("); ");
                else
                {
                    eclCode.append("Tbl1DS := INDEX( ");
                    eclCode.append('{');
                    eclCode.append(hpccQueryFile.getKeyedFieldsAsDelmitedString(',', "Tbl1RecDef"));
                    eclCode.append("},{");
                    eclCode.append(hpccQueryFile.getNonKeyedFieldsAsDelmitedString(',', "Tbl1RecDef"));
                    eclCode.append("},");
                    eclCode.append("\'~").append(hpccQueryFile.getFullyQualifiedName()).append("\');");
                }

                if (parser.hasJoinClause())
                {
                    String hpccJoinFileName = HPCCJDBCUtils.handleQuotedString(parser.getJoinClause()
                            .getJoinTableName());

                    if (!dbMetadata.tableExists("", hpccJoinFileName))
                        throw new Exception("Invalid Join table found: " + hpccJoinFileName);

                    DFUFile hpccJoinFile = dbMetadata.getDFUFile(hpccJoinFileName);

                    if (hpccJoinFile.hasFileRecDef())
                    {
                        // if (indexfiletouse != null && indexposfield != null)
                        // eclcode.append(hpccQueryFile.getFileRecDefwithIndexpos(indexfiletouse.getFieldMetaData(indexposfield),
                        // "Tbl1RecDef"));
                        // else
                        eclCode.append(hpccJoinFile.getFileRecDef("\nTbl2RecDef"));
                        eclCode.append("\n");
                    }
                    else
                        throw new Exception("Target HPCC file (" + hpccJoinFile
                                + ") does not contain ECL record definition");

                    eclDSSourceMapping.put(hpccJoinFile.getFullyQualifiedName(), "Tbl2DS");

                    if (!hpccJoinFile.isKeyFile())
                        eclCode.append("Tbl2DS := DATASET(\'~").append(hpccJoinFile.getFullyQualifiedName())
                                .append("\', Tbl2RecDef,").append(hpccJoinFile.getFormat()).append("); ");
                    else
                    {
                        eclCode.append("Tbl2DS := INDEX( ");
                        eclCode.append('{');
                        eclCode.append(hpccJoinFile.getKeyedFieldsAsDelmitedString(',', "Tbl2RecDef"));
                        eclCode.append("},{");
                        eclCode.append(hpccJoinFile.getNonKeyedFieldsAsDelmitedString(',', "Tbl2RecDef"));
                        eclCode.append("},");
                        eclCode.append("\'~").append(hpccJoinFile.getFullyQualifiedName()).append("\');");
                    }

                    HashMap<String, String> translator = new HashMap<String, String>(2);

                    translator.put(queryFileName, "LEFT");
                    translator.put(hpccJoinFileName, "RIGHT");

                    eclCode.append("\n").append("JndDS := JOIN(").append(" Tbl1DS").append(", Tbl2DS").append(", ")
                            .append(parser.getJoinClause().getOnClause().toStringTranslateSource(translator));
                    if (parser.getWhereClauseExpressionsCount() > 0)
                        eclCode.append(" AND ").append(parser.getWhereClauseStringTranslateSource(translator));
                    eclCode.append(", ").append(parser.getJoinClause().getECLTypeStr()).append(" );\n");

                    eclDSSourceMapping.put(queryFileName, "JndDS");
                    eclDSSourceMapping.put(hpccJoinFileName, "JndDS");

                    eclEntities.put("JoinQuery", "1");
                }

                eclEntities.put("SourceDS", eclDSSourceMapping.get(queryFileName));

                StringBuilder selectStructSB = new StringBuilder("SelectStruct := RECORD\n ");

                for (int i = 0; i < expectedretcolumns.size(); i++)
                {
                    HPCCColumnMetaData col = expectedretcolumns.get(i);

                    String datasource = eclDSSourceMapping.get(col.getTableName());

                    if (col.getColumnType() == HPCCColumnMetaData.COLUMN_TYPE_CONSTANT)
                    {
                        selectStructSB.append(col.getEclType()).append(" ").append(col.getColumnName()).append(" := ")
                                .append(col.getConstantValue()).append("; ");
                        if (i == 0 && expectedretcolumns.size() == 1)
                            eclEntities.put("SCALAROUTNAME", col.getColumnName());
                    }

                    else if (col.getColumnType() == HPCCColumnMetaData.COLUMN_TYPE_FNCTION)
                    {
                        if (col.getColumnName().equalsIgnoreCase("COUNT"))
                        {
                            eclEntities.put("COUNTFN", "TRUE");
                            selectStructSB.append(col.getAlias() + " := ");
                            if (parser.hasGroupByColumns())
                            {
                                selectStructSB.append(col.getColumnName().toUpperCase()).append("( GROUP");
                                List<HPCCColumnMetaData> funccols = col.getFunccols();

                                if (funccols.size() > 0)
                                {
                                    String paramname = funccols.get(0).getColumnName();
                                    if (!paramname.equals("*")
                                            && funccols.get(0).getColumnType() != HPCCColumnMetaData.COLUMN_TYPE_CONSTANT)
                                    {
                                        selectStructSB.append(", ");
                                        selectStructSB.append(datasource);
                                        selectStructSB.append(".");
                                        selectStructSB.append(paramname);
                                        selectStructSB.append("<> \'\'");
                                    }
                                }
                                selectStructSB.append(" );");
                            }
                            else
                            {
                                selectStructSB.append(" ScalarOut;");
                                if (expectedretcolumns.size() == 1)
                                    eclEntities.put("SCALAROUTNAME", col.getColumnName());
                            }

                            col.setSqlType(java.sql.Types.NUMERIC);
                        }
                        else if (col.getColumnName().equalsIgnoreCase("MAX"))
                        {
                            eclEntities.put("MAXFN", "TRUE");
                            selectStructSB.append(col.getAlias() + " := ");

                            if (parser.hasGroupByColumns())
                            {
                                selectStructSB.append("MAX( GROUP ");
                            }
                            else
                            {
                                selectStructSB.append("MAX( ").append(datasource);
                                if (eclEntities.size() > 0)
                                    addFilterClause(selectStructSB, eclEntities);
                            }

                            List<HPCCColumnMetaData> funccols = col.getFunccols();
                            if (funccols.size() > 0)
                            {
                                String paramname = funccols.get(0).getColumnName();
                                eclEntities.put("FNCOLS", paramname);
                                if (!paramname.equals("*")
                                        && funccols.get(0).getColumnType() != HPCCColumnMetaData.COLUMN_TYPE_CONSTANT)
                                {
                                    selectStructSB.append(", ");
                                    selectStructSB.append(datasource);
                                    selectStructSB.append(".");
                                    selectStructSB.append(paramname);
                                }
                            }
                            selectStructSB.append(" );");
                        }
                        else if (col.getColumnName().equalsIgnoreCase("MIN"))
                        {
                            eclEntities.put("MINFN", "TRUE");
                            selectStructSB.append(col.getAlias() + " := ");

                            if (parser.hasGroupByColumns())
                            {
                                selectStructSB.append("MIN( GROUP ");
                            }
                            else
                            {
                                selectStructSB.append("MIN( ").append(datasource);
                                if (eclEntities.size() > 0)
                                    addFilterClause(selectStructSB, eclEntities);
                            }

                            List<HPCCColumnMetaData> funccols = col.getFunccols();
                            if (funccols.size() > 0)
                            {
                                String paramname = funccols.get(0).getColumnName();
                                eclEntities.put("FNCOLS", paramname);
                                if (!paramname.equals("*")
                                        && funccols.get(0).getColumnType() != HPCCColumnMetaData.COLUMN_TYPE_CONSTANT)
                                {
                                    selectStructSB.append(", ");
                                    selectStructSB.append(datasource);
                                    selectStructSB.append(".");
                                    selectStructSB.append(paramname);
                                }
                            }
                            selectStructSB.append(" );");
                        }
                        else if (col.getColumnName().equalsIgnoreCase("SUM"))
                        {
                            eclEntities.put("SUMFN", "TRUE");
                            selectStructSB.append(col.getAlias() + " := ");

                            selectStructSB.append("SUM( ");
                            if (parser.hasGroupByColumns())
                            {
                                selectStructSB.append(" GROUP ");
                            }
                            else
                            {
                                selectStructSB.append(datasource);
                                if (eclEntities.size() > 0)
                                    addFilterClause(selectStructSB, eclEntities);
                            }

                            List<HPCCColumnMetaData> funccols = col.getFunccols();
                            if (funccols.size() > 0)
                            {
                                String paramname = funccols.get(0).getColumnName();
                                eclEntities.put("FNCOLS", paramname);
                                if (!paramname.equals("*")
                                        && funccols.get(0).getColumnType() != HPCCColumnMetaData.COLUMN_TYPE_CONSTANT)
                                {
                                    selectStructSB.append(", ");
                                    selectStructSB.append(datasource);
                                    selectStructSB.append(".");
                                    selectStructSB.append(paramname);
                                }
                            }
                            selectStructSB.append(" );");
                        }
                    }
                    else
                        selectStructSB.append(col.getEclType()).append(" ").append(col.getColumnName()).append(" := ")
                                .append(datasource).append(".").append(col.getColumnName()).append("; ");

                }
                selectStructSB.append("\nEND;\n");

                eclEntities.put("SELECTSTRUCT", selectStructSB.toString());

                if (parser.hasOrderByColumns())
                    eclEntities.put("ORDERBY", parser.getOrderByString());
                if (parser.hasGroupByColumns())
                    eclEntities.put("GROUPBY", parser.getGroupByString());
                if (parser.hasLimitBy())
                    eclEntities.put("LIMIT", Integer.toString(parser.getLimit()));

                return executeSelect(eclCode.toString(), eclEntities);
            }
            case SQLParser.SQL_TYPE_SELECTCONST:
            {
                System.out.println("Processing test_query...");
                eclCode.append("SelectStruct:=RECORD ");
                expectedretcolumns = parser.getSelectColumns();
                StringBuilder ecloutput = new StringBuilder("OUTPUT(DATASET([{ ");
                for (int i = 1; i <= expectedretcolumns.size(); i++)
                {
                    HPCCColumnMetaData col = expectedretcolumns.get(i - 1);
                    eclCode.append(col.getEclType()).append(" ").append(col.getColumnName()).append("; ");
                    ecloutput.append(col.getConstantValue());
                    if (i < expectedretcolumns.size())
                        ecloutput.append(", ");
                }
                ecloutput.append("}],SelectStruct), NAMED(\'");
                ecloutput.append("ConstECLQueryResult");
                ecloutput.append("\'));");

                eclCode.append(" END; ");
                eclCode.append(ecloutput.toString());

                return executeSelectConstant(eclCode.toString());
            }
            case SQLParser.SQL_TYPE_CALL:
            {
                if (hpccQuery == null)
                    throw new Exception("Invalid store procedure found");

                return executeCall(null);
            }
            default:

                break;
        }

        return null;
    }

    public boolean processIndex(DFUFile indexfiletouse, StringBuilder keyedandwild)
    {
        boolean isPayloadIndex = containsPayload(indexfiletouse.getAllFieldsProps(), parser.getSelectColumns()
                .iterator());

        Vector<String> keyed = new Vector<String>();
        Vector<String> wild = new Vector<String>();

        // Create keyed and wild string
        Properties keyedcols = indexfiletouse.getKeyedColumns();
        for (int i = 1; i <= keyedcols.size(); i++)
        {
            String keyedcolname = (String) keyedcols.get(i);
            if (parser.whereClauseContainsKey(keyedcolname))
                keyed.add(" " + parser.getExpressionFromColumnName(keyedcolname).toString() + " ");
            else if (keyed.isEmpty())
                wild.add(" " + keyedcolname + " ");
        }

        if (isPayloadIndex)
        {
            if (keyed.size() > 0)
            {
                keyedandwild.append("KEYED( ");
                for (int i = 0; i < keyed.size(); i++)
                {
                    keyedandwild.append(keyed.get(i));
                    if (i < keyed.size() - 1)
                        keyedandwild.append(" AND ");
                }
                keyedandwild.append(" )");
            }
            if (wild.size() > 0)
            {
                // TODO should I bother making sure there's a KEYED entry ?
                for (int i = 0; i < wild.size(); i++)
                {
                    keyedandwild.append(" and WILD( ");
                    keyedandwild.append(wild.get(i));
                    keyedandwild.append(" )");
                }
            }

            keyedandwild.append(" and (").append(parser.getWhereClauseString()).append(" )");
        }
        else
        // non-payload just AND the keyed expressions
        {
            keyedandwild.append("( ");
            keyedandwild.append(parser.getWhereClauseString());
            keyedandwild.append(" )");
        }

        return isPayloadIndex;
    }

    private boolean containsPayload(Properties indexfields, Iterator<HPCCColumnMetaData> selectcolsit)
    {
        while (selectcolsit.hasNext())
        {
            HPCCColumnMetaData currentselectcol = selectcolsit.next();
            String colname = currentselectcol.getColumnName();
            int type = currentselectcol.getColumnType();
            if (type == HPCCColumnMetaData.COLUMN_TYPE_DATA && !indexfields.containsKey(colname.toUpperCase()))
                return false;
            else if (type == HPCCColumnMetaData.COLUMN_TYPE_FNCTION
                    && !containsPayload(indexfields, currentselectcol.getFunccols().iterator()))
                return false;
        }
        return true;
    }

    public ArrayList executeSelect(String eclcode, HashMap parameters)
    {
        int responseCode = -1;
        try
        {
            urlString = "http://" + props.getProperty("WsECLDirectAddress") + ":"
                    + props.getProperty("WsECLDirectPort") + "/EclDirect/RunEcl?Submit";

            StringBuilder sb = new StringBuilder();

            if (props.containsKey("Cluster"))
                sb.append("&cluster=").append(props.getProperty("Cluster"));
            else
                System.out.println("No cluster property found, executing query on EclDirect default cluster");

            sb.append("&eclText=\n");
            sb.append(eclcode);
            sb.append("\n");

            if (eclEntities.get("IndexDef") == null)
            {
                if (!parameters.containsKey("GROUPBY"))
                {
                    if (parameters.containsKey("COUNTFN"))
                    {
                        sb.append("ScalarOut := COUNT( ").append(parameters.get("SourceDS"));

                        if (parameters.size() > 0)
                            addFilterClause(sb, parameters);
                        sb.append(");");
                        sb.append("\n");
                    }
                    else if (parameters.containsKey("SUMFN"))
                    {
                        sb.append("ScalarOut := SUM( ").append(eclEntities.get("SourceDS"));
                        if (parameters.size() > 0)
                            addFilterClause(sb, parameters);

                        sb.append(" , ");
                        sb.append(parameters.get("SourceDS"));
                        sb.append(".");
                        sb.append(parameters.get("FNCOLS"));
                        sb.append(");");
                        sb.append("\n");
                    }
                    else if (parameters.containsKey("MAXFN"))
                    {
                        sb.append("ScalarOut := MAX( ").append(parameters.get("SourceDS"));
                        if (parameters.size() > 0)
                            addFilterClause(sb, parameters);

                        sb.append(" , ");
                        sb.append(parameters.get("SourceDS"));
                        sb.append(".");
                        sb.append(parameters.get("FNCOLS"));
                        sb.append(");");
                        sb.append("\n");
                    }
                    else if (parameters.containsKey("MINFN"))
                    {
                        sb.append("ScalarOut := MIN( ").append(parameters.get("SourceDS"));
                        if (parameters.size() > 0)
                            addFilterClause(sb, parameters);

                        sb.append(" , ");
                        sb.append(parameters.get("SourceDS"));
                        sb.append(".");
                        sb.append(parameters.get("FNCOLS"));
                        sb.append(");");
                        sb.append("\n");
                    }
                }

                if (parameters.containsKey("SCALAROUTNAME"))
                {
                    sb.append("OUTPUT(ScalarOut ,NAMED(\'");
                    sb.append(parameters.get("SCALAROUTNAME"));
                    sb.append("\'));");
                }
                else
                {
                    sb.append(parameters.get("SELECTSTRUCT"));
                    sb.append("OUTPUT(CHOOSEN(");

                    if (parameters.containsKey("ORDERBY"))
                        sb.append("SORT( ");

                    sb.append("TABLE( ");
                    sb.append(eclEntities.get("SourceDS"));

                    if (parameters.size() > 0 && !parameters.containsKey("JoinQuery"))
                        addFilterClause(sb, parameters);

                    sb.append(", SelectStruct");
                    if (parameters.containsKey("GROUPBY"))
                    {
                        sb.append(",");
                        sb.append(parameters.get("GROUPBY"));
                    }

                    sb.append(")");

                    if (parameters.containsKey("ORDERBY"))
                    {
                        sb.append(",");
                        sb.append(parameters.get("ORDERBY"));
                        sb.append(")");
                    }
                }
            }
            else
            // use index
            {
                sb.append(parameters.get("IndexDef"));
                sb.append(parameters.get("IndexRead"));

                if (!parameters.containsKey("GROUPBY"))
                {
                    if (parameters.containsKey("COUNTFN"))
                    {
                        sb.append("ScalarOut := COUNT(IdxDS");
                        sb.append(parameters.containsKey("PAYLOADINDEX") == false ? ");" : ", KEYED);\n");
                    }
                    if (parameters.containsKey("SUMFN"))
                    {
                        sb.append("ScalarOut := SUM(IdxDS, ");
                        sb.append(eclEntities.get("SourceDS"));
                        sb.append(".");
                        sb.append(parameters.get("FNCOLS"));
                        sb.append(parameters.containsKey("PAYLOADINDEX") == false ? ");" : ", KEYED);\n");
                    }
                    if (parameters.containsKey("MAXFN"))
                    {
                        sb.append("ScalarOut := MAX(IdxDS, ");
                        sb.append(eclEntities.get("SourceDS"));
                        sb.append(".");
                        sb.append(parameters.get("FNCOLS"));
                        sb.append(parameters.containsKey("PAYLOADINDEX") == false ? ");" : ", KEYED);\n");
                    }
                    if (parameters.containsKey("MINFN"))
                    {
                        sb.append("ScalarOut := MIN(IdxDS, ");
                        sb.append(eclEntities.get("SourceDS"));
                        sb.append(".");
                        sb.append(parameters.get("FNCOLS"));
                        sb.append(parameters.containsKey("PAYLOADINDEX") == false ? ");" : ", KEYED);\n");
                    }
                }

                if (parameters.containsKey("SCALAROUTNAME"))
                {
                    sb.append("OUTPUT(ScalarOut ,NAMED(\'");
                    sb.append(parameters.get("SCALAROUTNAME"));
                    sb.append("\'));");
                }
                else
                {
                    sb.append(parameters.get("SELECTSTRUCT"));

                    sb.append("IdxDSTable := TABLE(IdxDS, SelectStruct ");

                    if (parameters.containsKey("GROUPBY"))
                    {
                        sb.append(", ");
                        sb.append(parameters.get("GROUPBY"));
                    }
                    sb.append(");\n");

                    if (parameters.containsKey("ORDERBY"))
                    {
                        sb.append("SortedIdxTable := SORT( IdxDSTable, ");
                        sb.append(parameters.get("ORDERBY"));
                        sb.append(");\n");
                        sb.append("ResultSet := SortedIdxTable;\n");
                    }
                    else
                        sb.append("ResultSet := IdxDSTable;\n");

                    sb.append("OUTPUT(CHOOSEN(");
                    sb.append(" ResultSet ");
                }
            }

            if (!parameters.containsKey("SCALAROUTNAME"))
            {
                sb.append(",");
                if (parameters.containsKey("LIMIT"))
                    sb.append(parameters.get("LIMIT"));
                else
                    sb.append(props.getProperty("EclResultLimit"));
                ;
                sb.append("),NAMED(\'HPCCJDBCSelectOutput\'));");
            }

            System.out.println("WSECL:executeSelect: " + urlString + sb.toString());

            // Send data
            long startTime = System.currentTimeMillis();

            URL url = new URL(urlString);
            HttpURLConnection conn = dbMetadata.createHPCCESPConnection(url);

            OutputStreamWriter wr = new OutputStreamWriter(conn.getOutputStream());
            wr.write(sb.toString());
            wr.flush();

            responseCode = conn.getResponseCode();

            return parse(conn.getInputStream(), startTime);
        }
        catch (Exception e)
        {
            if (responseCode != 200)
            {
                throw new RuntimeException("HTTP Connection Response code: " + responseCode
                        + " verify access to WsECLDirect", e);
            }
            else
                throw new RuntimeException(e);
        }
    }

    private void addFilterClause(StringBuilder sb, HashMap parameters)
    {
        String whereclause = parser.getWhereClauseString();
        if (whereclause != null && whereclause.length() > 0)
        {
            sb.append("( ");
            sb.append(whereclause);
            sb.append(" )");
        }
    }

    public ArrayList executeCall(Map parameters)
    {
        try
        {
            urlString = "http://" + props.getProperty("WsECLAddress")
                    + ":" + props.getProperty("WsECLPort")
                    + "/WsEcl/submit/query/" + hpccQuery.getQuerySet()
                    + "/" + hpccQuery.getName() + "/expanded";

            System.out.println("WSECL:executeCall: " + urlString);

            // Construct data
            StringBuilder sb = new StringBuilder();
            sb.append(URLEncoder.encode("submit_type_=xml", "UTF-8"));
            sb.append("&").append(URLEncoder.encode("S1=Submit", "UTF-8"));

            ArrayList<HPCCColumnMetaData> storeProcInParams = hpccQuery.getAllInFields();
            String[] procInParamValues = parser.getStoredProcInParamVals();

            if (procInParamValues.length != storeProcInParams.size())
                throw new Exception("Invalid number of parameter passed in.");

            for (int i = 0; i < procInParamValues.length; i++)
            {
                String key = storeProcInParams.get(i).getColumnName();
                sb.append("&").append(key).append("=").append(procInParamValues[i]);
            }

            long startTime = System.currentTimeMillis();
            // Send data
            URL url = new URL(urlString);
            HttpURLConnection conn = dbMetadata.createHPCCESPConnection(url);

            OutputStreamWriter wr = new OutputStreamWriter(conn.getOutputStream());
            wr.write(sb.toString());
            wr.flush();

            System.out.println("WsEcl Execute: " + urlString + " : " + sb.toString());

            // Get the response
            return parse(conn.getInputStream(), startTime);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public ArrayList parse(InputStream xml, long startTime) throws Exception
    {
        ArrayList results = null;

        try
        {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();

            Document dom = db.parse(xml);

            long elapsedTime = System.currentTimeMillis() - startTime;
            System.out.println("Total elapsed http request/response time in milliseconds: " + elapsedTime);

            Element docElement = dom.getDocumentElement();

            NodeList dsList = docElement.getElementsByTagName("Dataset");

            if (dsList != null && dsList.getLength() > 0)
            {
                // The dataset element is encapsulated within a Result element
                // need to fetch appropriate resulst dataset ie map eclqueryname
                // to dataset name

                ArrayList dsArray = new ArrayList();

                results = dsArray;

                for (int i = 0; i < dsList.getLength(); i++)
                {
                    Element ds = (Element) dsList.item(i);
                    String currentdatsetname = ds.getAttribute("name");
                    if (datasetName == null || datasetName.length() == 0
                            || currentdatsetname.equalsIgnoreCase(datasetName))
                    {
                        NodeList rowList = ds.getElementsByTagName("Row");

                        if (rowList != null && rowList.getLength() > 0)
                        {

                            ArrayList rowArray = new ArrayList();

                            dsArray.add(rowArray);

                            for (int j = 0; j < rowList.getLength(); j++)
                            {
                                Element row = (Element) rowList.item(j);

                                NodeList columnList = row.getChildNodes();

                                ArrayList columnsArray = new ArrayList();
                                rowArray.add(columnsArray);


                                for (int k = 0; k < columnList.getLength(); k++)
                                {
                                    columnsArray.add(new HPCCColumn(columnList.item(k).getNodeName(), columnList.item(k).getTextContent()));
                                }
                            }
                        }
                        break;
                    }

                }
            }
            else if (docElement.getElementsByTagName("Exception").getLength() > 0)
            {
                Exception xmlexception = new Exception();
                NodeList exceptionlist = docElement.getElementsByTagName("Exception");

                if (exceptionlist.getLength() > 0)
                {
                    Exception resexception = null;
                    NodeList currexceptionelements = exceptionlist.item(0).getChildNodes();

                    for (int j = 0; j < currexceptionelements.getLength(); j++)
                    {
                        Node exceptionelement = currexceptionelements.item(j);
                        if (exceptionelement.getNodeName().equals("Message"))
                        {
                            resexception = new Exception("HPCCJDBC error in response: \'"
                                    + exceptionelement.getTextContent() + "\'");
                        }
                    }
                    if (dsList == null || dsList.getLength() <= 0)
                        throw resexception;
                }
            }
            else
            {
                // The root element is itself the Dataset element
                if (dsList.getLength() == 0)
                {
                    ArrayList dsArray = new ArrayList();
                    results = dsArray;
                    NodeList rowList = docElement.getElementsByTagName("Row");

                    if (rowList != null && rowList.getLength() > 0)
                    {
                        ArrayList rowArray = new ArrayList();
                        dsArray.add(rowArray);

                        for (int j = 0; j < rowList.getLength(); j++)
                        {
                            Element row = (Element) rowList.item(j);
                            NodeList columnList = row.getChildNodes();

                            ArrayList columnsArray = new ArrayList();
                            rowArray.add(columnsArray);

                            for (int k = 0; k < columnList.getLength(); k++)
                            {
                                columnsArray.add(new HPCCColumn(columnList.item(k).getNodeName(), columnList.item(k)
                                        .getTextContent()));
                            }
                        }
                    }
                }
            }

            /*
             * not being used right not
             * setResultschema( docElement.getElementsByTagName("XmlSchema"));
             */

            System.out.println("Parsing results...");
            Iterator itr = results.iterator();
            System.out.println("Results datsets found: " + results.size());
            while (itr.hasNext())
            {
                ArrayList rows = (ArrayList) itr.next();
                System.out.println("Results rows found: " + rows.size());
                Iterator itr2 = rows.iterator();
                while (itr2.hasNext())
                {
                    ArrayList cols = (ArrayList) itr2.next();
                    Iterator itr3 = cols.iterator();
                    while (itr3.hasNext())
                    {
                        HPCCColumn element = (HPCCColumn) itr3.next();
                    }
                }
            }
            System.out.println("Finished Parsing results.");
        }
        catch (Exception e)
        {
            throw new Exception(
                    "Invalid response received, verify serveraddress and cluster name and HPCC query/file name:\n"
                            + e.getMessage());
        }

        return results;
    }

    public String convertInputStreamToString(InputStream ists) throws IOException
    {
        if (ists != null)
        {
            StringBuilder sb = new StringBuilder();
            String line;

            try
            {
                BufferedReader r1 = new BufferedReader(new InputStreamReader(ists, "UTF-8"));
                while ((line = r1.readLine()) != null)
                {
                    sb.append(line).append("\n");
                }
            }
            finally
            {
                ists.close();
            }
            return sb.toString();
        }
        else
        {
            return "";
        }
    }

    public boolean hasResultSchema()
    {
        return (this.resultSchema != null && this.resultSchema.getLength() > 0);
    }

    public void setResultschema(NodeList resultschema)
    {
        this.resultSchema = resultschema;

        if (this.resultSchema != null && this.resultSchema.getLength() > 0)
        {
            System.out.println("contains resultschema");
        }
    }

    public NodeList getResultschema()
    {
        return resultSchema;
    }
}
