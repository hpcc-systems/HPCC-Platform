package com.hpccsystems.ecljdbc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.URL;
import java.net.URLEncoder;
import java.net.UnknownHostException;
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

public class EclEngine
{

	private  		String		urlString;
	private final 	String 		basicAuth;
	private 	 	String 		eclqueryname;
	private final 	String 		datasetname;
	private 		NodeList 	resultschema;
	private final 	Properties 	props;
	private 		ArrayList<SQLWarning> warnings;
	private 		SqlParser 	parser;
	private			EclDatabaseMetaData dbMetadata;
	private 		String 		indexToUseName;
	private 		HashMap<String, String> 	eclEnteties;

	public EclEngine(String eclqueryname, String datasetname, Properties props)
	{
		this.props = props;
		this.datasetname = datasetname;
		this.eclqueryname = eclqueryname;
		basicAuth = props.getProperty("BasicAuth");
		resultschema = null;
		eclEnteties = new HashMap<String, String>();
	}

	public EclEngine(SqlParser parser, EclDatabaseMetaData dbmetadata, Properties props, String indextouse)
	{
		this.props = props;
		this.dbMetadata = dbmetadata;
		this.parser = parser;
		basicAuth = props.getProperty("BasicAuth");
		this.indexToUseName = indextouse;
		datasetname = null;
		resultschema = null;
		eclqueryname = null;
		eclEnteties = new HashMap<String, String>();
	}

	public ArrayList executeSelectConstant(String eclstring)
	{
		try
		{
			urlString = "http://" + props.getProperty("ServerAddress") + ":" + props.getProperty("WsECLDirectPort") + "/EclDirect/RunEcl?Submit&eclText=";
			urlString +=  URLEncoder.encode(eclstring, "UTF-8");

			System.out.println("WSECL:executeSelect: " + urlString);

			// Send data
			long startTime = System.currentTimeMillis();

			URL url = new URL(urlString);
			HttpURLConnection conn = (HttpURLConnection)url.openConnection();
			conn.setInstanceFollowRedirects(false);
			conn.setRequestProperty("Authorization", basicAuth);
			conn.setRequestMethod("GET");
			conn.setDoOutput(true);
			conn.setDoInput(true);

			return parse(conn.getInputStream(),startTime);
		}
		catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	public  ArrayList execute() throws Exception
	{
		int sqlreqtype = parser.getSqlType();

		List<EclColumnMetaData> expectedretcolumns = new ArrayList();
		ArrayList<EclColumnMetaData> storeProcInParams = new ArrayList();
		String hpccfilename = "";
		String indexposfield = null;
		StringBuilder keyedandwild = new StringBuilder();
		DFUFile indexfiletouse = null;
		StringBuilder eclcode = new StringBuilder("");
		int totalparamcount = 0;
		boolean isPayloadIndex = false;

		switch (sqlreqtype)
		{
			case SqlParser.SQL_TYPE_SELECT:
			{
				hpccfilename = Utils.handleQuotedString(parser.getTableName());
				if(!dbMetadata.tableExists("", hpccfilename))
					throw new Exception("Invalid table found: " + hpccfilename);

				DFUFile dfufile = dbMetadata.getDFUFile(hpccfilename);

				//in future this might need to be a container of dfufile(s)
				parser.verifySelectColumns(dfufile);

				expectedretcolumns = parser.getSelectColumns();
				totalparamcount = parser.getWhereClauseExpressionsCount();

				if (indexToUseName != null)
				{
					indexfiletouse = dbMetadata.getDFUFile(indexToUseName);
					indexposfield = indexfiletouse.getIdxFilePosField();

					isPayloadIndex = processIndex(indexfiletouse, keyedandwild);

					eclEnteties.put("KEYEDWILD", keyedandwild.toString());
					if (isPayloadIndex)
						eclEnteties.put("PAYLOADINDEX", "true");
				}

				eclEnteties.put("PARAMCOUNT", Integer.toString(totalparamcount));

				if(dfufile.hasFileRecDef())
				{
					if (indexfiletouse != null && indexposfield != null)
						eclcode.append(dfufile.getFileRecDefwithIndexpos(indexfiletouse.getFieldMetaData(indexposfield), "filerecstruct"));
					else
						eclcode.append(dfufile.getFileRecDef("filerecstruct"));
				}
				else
					throw new Exception("Target HPCC file ("+hpccfilename+") does not contain ECL record definition");

				if(!dfufile.isKeyFile())
					eclcode.append("fileds := DATASET(\'~").append(dfufile.getFullyQualifiedName()).append("\', filerecstruct,").append(dfufile.getFormat()).append("); ");
				else
				{
					eclcode.append("fileds := INDEX( ");
					eclcode.append('{');
					eclcode.append(dfufile.getKeyedFieldsAsDelmitedString(',', "filerecstruct"));
					eclcode.append("},{");
					eclcode.append(dfufile.getNonKeyedFieldsAsDelmitedString(',', "filerecstruct"));
					eclcode.append("},");
					eclcode.append("\'~").append(dfufile.getFullyQualifiedName()).append("\');");
				}

				StringBuilder selectstruct = new StringBuilder(" selectstruct:=RECORD ");
				String datasource = indexToUseName == null || isPayloadIndex ? "fileds" : "fetchedds";

				//boolean usescalar = expectedretcolumns.size() == 1;
				for (int i = 0; i < expectedretcolumns.size(); i++)
				{
					EclColumnMetaData col = expectedretcolumns.get(i);
					if (col.getColumnType() == EclColumnMetaData.COLUMN_TYPE_CONSTANT)
					{
						selectstruct.append(col.getEclType()).append(" ").append(col.getColumnName()).append(" := ").append(col.getConstantValue()).append("; ");
						if (i == 0 && expectedretcolumns.size() == 1)
							eclEnteties.put("SCALAROUTNAME", col.getColumnName());
					}

					else if (col.getColumnType() == EclColumnMetaData.COLUMN_TYPE_FNCTION)
					{
						if (col.getColumnName().equalsIgnoreCase("COUNT"))
						{
							eclEnteties.put("COUNTFN", "TRUE");
							selectstruct.append("countout := ");
							if (parser.hasGroupByColumns())
							{
								selectstruct.append(col.getColumnName().toUpperCase()).append("( GROUP");
								List<EclColumnMetaData> funccols = col.getFunccols();

								if (funccols.size() > 0)
								{
									String paramname = funccols.get(0).getColumnName();
									if (!paramname.equals("*") && funccols.get(0).getColumnType() != EclColumnMetaData.COLUMN_TYPE_CONSTANT)
									{
										selectstruct.append(", ");
										selectstruct.append(datasource);
										selectstruct.append(".");
										selectstruct.append(paramname);
										selectstruct.append("<> \'\'");
										//if (eclEnteties.size() > 0)
										//	addFilterClause(selectstruct, eclEnteties);
									}
								}
								selectstruct.append(" );");
							}
							else
							{
								selectstruct.append(" scalarout;");
								if (expectedretcolumns.size() == 1)
									eclEnteties.put("SCALAROUTNAME", col.getColumnName());
							}

							col.setSQLType(java.sql.Types.NUMERIC);
						}
						else if (col.getColumnName().equalsIgnoreCase("MAX"))
						{
							eclEnteties.put("MAXFN", "TRUE");
							selectstruct.append("maxout :=  ");

							if (parser.hasGroupByColumns())
							{
								selectstruct.append("MAX( GROUP ");
							}
							else
							{
								selectstruct.append("MAX( ").append(datasource);
								if (eclEnteties.size() > 0)
									addFilterClause(selectstruct, eclEnteties);
							}

							List<EclColumnMetaData> funccols = col.getFunccols();
							if (funccols.size() > 0)
							{
								String paramname = funccols.get(0).getColumnName();
								eclEnteties.put("FNCOLS", paramname);
								if (!paramname.equals("*") && funccols.get(0).getColumnType() != EclColumnMetaData.COLUMN_TYPE_CONSTANT)
								{
									selectstruct.append(", ");
									selectstruct.append(datasource);
									selectstruct.append(".");
									selectstruct.append(paramname);
								}
							}
							selectstruct.append(" );");

							/*
							if (parser.hasGroupByColumns())
							{
								selectstruct.append(col.getColumnName().toUpperCase()).append("( GROUP");
								List<EclColumnMetaData> funccols = col.getFunccols();

								if (funccols.size() > 0)
								{
									String paramname = funccols.get(0).getColumnName();
									eclEnteties.put("FNCOLS", paramname);
									if (!paramname.equals("*") && funccols.get(0).getColumnType() != EclColumnMetaData.COLUMN_TYPE_CONSTANT)
									{
										selectstruct.append(", ");
										selectstruct.append(datasource);
										selectstruct.append(".");
										selectstruct.append(paramname);
									}
								}
								selectstruct.append(" );");
							}
							else
							{
								selectstruct.append(" totalmax;");
								//if (expectedretcolumns.size() == 1)
								//	eclEnteties.put("SCALAROUTNAME", col.getColumnName());
							}
							*/
							/*

							selectstruct.append(col.getColumnName().toUpperCase()).append(" ( ");
							if (parser.hasGroupByColumns())
								selectstruct.append("GROUP");
							else
								selectstruct.append(datasource);

							List<EclColumnMetaData> funccols = col.getFunccols();

							if (funccols.size() > 0)
							{
								String paramname = funccols.get(0).getColumnName();
								if (!paramname.equals("*") && funccols.get(0).getColumnType() != EclColumnMetaData.COLUMN_TYPE_CONSTANT)
								{
									selectstruct.append(", ");
									selectstruct.append(datasource);
									selectstruct.append(".");
									selectstruct.append(paramname);
								}
							}
							selectstruct.append(" );");*/
						}
					}
					else
						selectstruct.append(col.getEclType()).append(" ").append(col.getColumnName()).append(" := ").append(datasource).append(".").append(col.getColumnName()).append("; ");

					//if (i == 0 && expectedretcolumns.size() == 1 &&  col.getColumnType() != EclColumnMetaData.COLUMN_TYPE_DATA )
					//	eclEnteties.put("SCALAROUTNAME", col.getColumnName());
				}
				selectstruct.append("END; ");

				eclEnteties.put("SELECTSTRUCT", selectstruct.toString());

				if(parser.hasOrderByColumns())
					eclEnteties.put("ORDERBY",parser.getOrderByString());
				if (parser.hasGroupByColumns())
					eclEnteties.put("GROUPBY",parser.getGroupByString());
				if (parser.hasLimitBy())
					eclEnteties.put("LIMIT",Integer.toString(parser.getLimit()));

				return executeSelect(eclcode.toString(), eclEnteties, indexfiletouse);
			}
			case SqlParser.SQL_TYPE_SELECTCONST:
			{
				System.out.println("Processing test_query...");
				//ArrayList<EclColumnMetaData> columns = new ArrayList();
				eclcode.append("selectstruct:=RECORD ");
				expectedretcolumns = parser.getSelectColumns();
				//defaultEclQueryReturnDatasetName = "ConstECLQueryResult";
				StringBuilder ecloutput = new StringBuilder(" OUTPUT(DATASET([{ ");
				for (int i = 1;  i <= expectedretcolumns.size(); i++)
				{
					EclColumnMetaData col = expectedretcolumns.get(i-1);
					eclcode.append(col.getEclType()).append(" ").append(col.getColumnName()).append("; ");
					ecloutput.append(col.getConstantValue());
					if (i < expectedretcolumns.size())
						ecloutput.append(", ");
				}
				ecloutput.append("}],selectstruct), NAMED(\'");
				//ecloutput.append(defaultEclQueryReturnDatasetName);
				ecloutput.append("ConstECLQueryResult");
				ecloutput.append("\'));");

				eclcode.append(" END; ");
				eclcode.append(ecloutput.toString());

				return executeSelectConstant(eclcode.toString());
			}
			case SqlParser.SQL_TYPE_CALL:
			{
				eclqueryname = Utils.handleQuotedString(parser.getStoredProcName());
				if(!dbMetadata.eclQueryExists("", eclqueryname))
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
		boolean isPayloadIndex = containsPayload(indexfiletouse.getAllFieldsProps(), parser.getSelectColumns().iterator());

		Vector<String> keyed = new Vector<String>();
		Vector<String> wild = new Vector<String>();

		//Create keyed and wild string
		Properties keyedcols = indexfiletouse.getKeyedColumns();
		for (int i = 1; i <= keyedcols.size(); i++)
		{
			String keyedcolname = (String)keyedcols.get(i);
			if(parser.whereClauseContainsKey(keyedcolname))
				keyed.add(" " + parser.getExpressionFromName(keyedcolname).toString() + " ");
			else if (keyed.isEmpty())
				wild.add(" " + keyedcolname + " ");
		}

		if(isPayloadIndex)
		{
			if (keyed.size()>0)
			{
				keyedandwild.append("KEYED( ");
				for (int i = 0 ; i < keyed.size(); i++)
				{
					keyedandwild.append(keyed.get(i));
					if (i < keyed.size()-1)
						keyedandwild.append(" AND ");
				}
				keyedandwild.append(" )");
			}
			if (wild.size()>0)
			{
				//TODO should I bother making sure there's a KEYED entry ?
				for (int i = 0 ; i < wild.size(); i++)
				{
					keyedandwild.append(" and WILD( ");
					keyedandwild.append(wild.get(i));
					keyedandwild.append(" )");
				}
			}

			keyedandwild.append(" and (").append(parser.getWhereClauseString()).append(" )");
		}
		else //non-payload just AND the keyed expressions
		{
			keyedandwild.append("( ");
			keyedandwild.append(parser.getWhereClauseString());
			keyedandwild.append(" )");
		}

		return isPayloadIndex;
	}

	private boolean containsPayload(Properties indexfields,	Iterator<EclColumnMetaData> selectcolsit)
	{
		while(selectcolsit.hasNext())
		{
			EclColumnMetaData currentselectcol = selectcolsit.next();
			String colname = currentselectcol.getColumnName();
			int type = currentselectcol.getColumnType();
			if (type == EclColumnMetaData.COLUMN_TYPE_DATA && !indexfields.containsKey(colname.toUpperCase()))
				return false;
			else if (type == EclColumnMetaData.COLUMN_TYPE_FNCTION && !containsPayload(indexfields, currentselectcol.getFunccols().iterator()))
				return false;
		}
		return true;
	}

	public ArrayList executeSelect(String eclcode, HashMap parameters, DFUFile indexfile)
	{
		int responseCode = -1;
		try
		{
			urlString = "http://" + props.getProperty("ServerAddress") + ":" + props.getProperty("WsECLDirectPort") + "/EclDirect/RunEcl?Submit"; 			//&eclText=

			StringBuilder sb = new StringBuilder("&eclText=");
			sb.append(eclcode);

			if (indexfile == null) //no indexfile read...
			{
				if (!parameters.containsKey("GROUPBY"))
				{
					if (parameters.containsKey("COUNTFN"))
					{
						sb.append("scalarout := COUNT(fileds");
						if (parameters.size() > 0)
							addFilterClause(sb, parameters);
						sb.append(");");
					}

					if (parameters.containsKey("MAXFN"))
					{
						sb.append("scalarout := MAX(fileds");
						if (parameters.size() > 0)
							addFilterClause(sb, parameters);

						sb.append(" , fileds.");
						sb.append(parameters.get("FNCOLS"));
						sb.append(");");
					}
				}

				if (parameters.containsKey("SCALAROUTNAME"))
				{
					sb.append("OUTPUT(scalarout ,NAMED(\'");
					sb.append(parameters.get("SCALAROUTNAME"));
					sb.append("\'));");
				}
				else
				{
					sb.append(parameters.get("SELECTSTRUCT"));
					sb.append("OUTPUT(CHOOSEN(");

					if (parameters.containsKey("ORDERBY"))
						sb.append("SORT( ");

					sb.append("TABLE(fileds");

					if (parameters.size() > 0)
						addFilterClause(sb, parameters);

					sb.append(", selectstruct");
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
			else // use index
			{
				sb.append("IDX := INDEX(fileds, {")
				.append(indexfile.getKeyedFieldsAsDelmitedString(',', null))
				.append("}");

				if(indexfile.getNonKeyedColumnsCount()>0)
					sb.append(",{ ").append(indexfile.getNonKeyedFieldsAsDelmitedString(',', null)).append(" }");

				sb.append(",\'~").append(indexfile.getFullyQualifiedName()).append("\');");

				if( parameters.containsKey("PAYLOADINDEX"))
				{
					sb.append(" OUTPUT(CHOOSEN(");
					if (parameters.containsKey("ORDERBY"))
						sb.append("SORT( ");

					sb.append("IDX(")
					.append(parameters.get("KEYEDWILD"))
					.append(")");
					if (parameters.containsKey("ORDERBY"))
					{
						sb.append(",");
						sb.append(parameters.get("ORDERBY"));
						sb.append(")");
					}
				}
				else
				{
					sb.append("fetchedds := FETCH(fileds, IDX( ");

					sb.append(parameters.get("KEYEDWILD"));
					sb.append("), RIGHT.");
					sb.append(indexfile.getIdxFilePosField()).append(");");


					if (!parameters.containsKey("GROUPBY"))
					{
						if (parameters.containsKey("COUNTFN"))
							sb.append("scalarout := COUNT(fetchedds);");
						if (parameters.containsKey("MAXFN"))
							sb.append("scalarout := MAX(fetchedds, fileds.zip);");
					}

					if (parameters.containsKey("SCALAROUTNAME"))
					{
						sb.append("OUTPUT(scalarout ,NAMED(\'");
						sb.append(parameters.get("SCALAROUTNAME"));
						sb.append("\'));");
					}
					else
					{

					sb.append(parameters.get("SELECTSTRUCT"));

					sb.append(" IDXTABLE := TABLE(fetchedds , selectstruct ");

					if (parameters.containsKey("GROUPBY"))
					{
						sb.append(", ");
						sb.append(parameters.get("GROUPBY"));
					}

					sb.append("); ");

					sb.append(" OUTPUT(CHOOSEN(");
					sb.append(" IDXTABLE ");
					}

					/*{
						sb.append(" OUTPUT(CHOOSEN(");
						if (parameters.containsKey("ORDERBY"))
							sb.append("SORT( ");

						sb.append("fetchedds");
						if (parameters.size() > 0)
							addFilterClause(sb, parameters);

						if (parameters.containsKey("ORDERBY"))
						{
							sb.append(",");
							sb.append(parameters.get("ORDERBY"));
							sb.append(")");
						}
					}*/
				}
			}

			if (!parameters.containsKey("SCALAROUTNAME"))
			{
				sb.append(",");
				if (parameters.containsKey("LIMIT"))
					sb.append(parameters.get("LIMIT"));
				else
					sb.append( props.getProperty("EclLimit"));
				sb.append("),NAMED(\'ECLJDBCSelectOutput\'));");
			}

			System.out.println("WSECL:executeSelect: " + urlString + sb.toString());

			// Send data
			long startTime = System.currentTimeMillis();

			URL url = new URL(urlString);
			HttpURLConnection conn = (HttpURLConnection)url.openConnection();
			conn.setInstanceFollowRedirects(false);
			conn.setRequestProperty("Authorization", basicAuth);
			conn.setRequestMethod("GET");
			conn.setDoOutput(true);
			conn.setDoInput(true);

			OutputStreamWriter wr = new OutputStreamWriter(	conn.getOutputStream());
			wr.write(sb.toString());
			wr.flush();

			responseCode = conn.getResponseCode();

			return parse(conn.getInputStream(),startTime);
		}
		catch (Exception e)
		{
			if (responseCode != 200)
			{
				throw new RuntimeException("HTTP Connection Response code: " + responseCode + " verify access to WsECLDirect", e);
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
			urlString = "http://" + props.getProperty("ServerAddress") + ":" + props.getProperty("WsECLPort") + "/WsEcl/submit/query/" + props.getProperty("Cluster") + "/" + eclqueryname + "/expanded";
			System.out.println("WSECL:executeCall: " + urlString);

			// Construct data
			StringBuilder sb = new StringBuilder();
			sb.append(URLEncoder.encode("submit_type_=xml", "UTF-8"));
			sb.append("&").append(URLEncoder.encode("S1=Submit", "UTF-8"));

			ArrayList<EclColumnMetaData> storeProcInParams = dbMetadata.getStoredProcInColumns("",eclqueryname);
			String[] procInParamValues = parser.getStoredProcInParamVals();

			for (int i = 0; i < procInParamValues.length; i++)
			{
				String key = storeProcInParams.get(i).getColumnName();
				{
					sb.append("&").append(key).append("=").append(procInParamValues[i]);
				}
			}

			//if (parameters.size() > 0)
			//{
			//	for (Object key : parameters.keySet())
			//	{
			//		Object value = parameters.get(key);
			//		sb.append("&").append(key).append("=").append(value);
			//	}
			//}

			long startTime = System.currentTimeMillis();
			// Send data
			URL url = new URL(urlString);
			HttpURLConnection conn = (HttpURLConnection)url.openConnection();
			conn.setInstanceFollowRedirects(false);
			conn.setRequestProperty("Authorization", basicAuth);
			conn.setRequestMethod("GET");
			conn.setDoOutput(true);
			conn.setDoInput(true);

			OutputStreamWriter wr = new OutputStreamWriter(	conn.getOutputStream());
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
			{ // The dataset element is encapsulated within a Result element

				//need to fetch appropriate resulst dataset ie map eclqueryname to dataset name
				ArrayList dsArray = new ArrayList();

				results = dsArray;

				for (int i = 0; i < dsList.getLength(); i++)
				{
					Element ds = (Element) dsList.item(i);
					String currentdatsetname = ds.getAttribute("name");
					if (datasetname == null || datasetname.length() == 0 || currentdatsetname.equalsIgnoreCase(datasetname))
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
									columnsArray.add(new EclColumn(columnList.item(k).getNodeName(), columnList.item(k).getTextContent()));
								}
							}
						}
						break;
					}

				}
			}
			else if (docElement.getElementsByTagName("Exception").getLength()>0)
			{
				Exception xmlexception = new Exception();
				NodeList exceptionlist = docElement.getElementsByTagName("Exception");
				//for (int i = 0; i < exceptionlist.getLength(); i++)
				//{
				if (exceptionlist.getLength()>0)
				{
					Exception resexception = null;
					NodeList currexceptionelements = exceptionlist.item(0).getChildNodes();

					for (int j = 0; j < currexceptionelements.getLength(); j++)
					{
						Node exceptionelement = currexceptionelements.item(j);
						if (exceptionelement.getNodeName().equals("Message"))
						{
							resexception = new Exception("ECLJDBC error in response: \'" + exceptionelement.getTextContent()+"\'");
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
								columnsArray.add(new EclColumn(columnList.item(k).getNodeName(), columnList.item(k).getTextContent()));
							}
						}
					}
				}
			}

			/*not being used right not
			 *
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
						EclColumn element = (EclColumn) itr3.next();
					}
				}
			}
			System.out.println("Finished Parsing results.");
		}
		catch (Exception e)
		{
			throw new Exception("Invalid response received, verify serveraddress and cluster name and HPCC query/file name:\n" +e.getMessage() );
		}

		return results;
	}

	public String convertInputStreamToString(InputStream ists)	throws IOException
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
		return (this.resultschema != null && this.resultschema.getLength() > 0);
	}

	public void setResultschema(NodeList resultschema)
	{
		this.resultschema = resultschema;

		if (this.resultschema != null && this.resultschema.getLength() > 0)
		{
			System.out.println("contains resultschema");
		}
	}

	public NodeList getResultschema()
	{
		return resultschema;
	}


	public static void main(String[] args)
	{
		// WsEcl wsEcl = new WsEcl("localhost", "myroxie", "person_query.1");
		//WsEcl wsEcl = new WsEcl("192.168.92.130", "myroxie",

		Properties info = new Properties();
		// info.put("ServerAddress",
		// "192.168.92.130:8002/WsEcl/definitions/query");
		//info.put("ServerAddress", "192.168.124.128");
		info.put("ServerAddress", "10.239.20.80");

		info.put("Cluster", "myroxie");
		info.put("WsECLWatchPort", "8010");
		info.put("WsECLPort", "8002");
		info.put("username", "_rpastrana");
		info.put("password", "ch@ng3m3");

		EclEngine wsEcl = new EclEngine("fetchpeoplebyzipservice.2","" ,info);
		// http://192.168.56.102:8002/
		HashMap params = new HashMap();
		params.put("zipvalue", "30005");
		ArrayList dataset = wsEcl.executeCall(params);

		Iterator itr = dataset.iterator();
		while (itr.hasNext())
		{
			ArrayList rows = (ArrayList) itr.next();
			Iterator itr2 = rows.iterator();
			while (itr2.hasNext())
			{
				ArrayList cols = (ArrayList) itr2.next();
				Iterator itr3 = cols.iterator();
				while (itr3.hasNext())
				{
					EclColumn element = (EclColumn) itr3.next();
					System.out.print(element.getName() + " : "	+ element.getValue());
				}
			}
		}
		System.out.println();
	}
}
