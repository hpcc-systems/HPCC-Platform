package com.hpccsystems.jdbcdriver;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;


/**
 *
 * @author  rpastrana
 */

public class HPCCDatabaseMetaData implements DatabaseMetaData {

	private HPCCQueries eclqueries;
	private Properties dfufiles;
	private static Map<Integer, String> SQLFieldMapping;

	public static final short JDBCVerMajor = 3;
	public static final short JDBCVerMinor = 0;

	private static String HPCCBuildVersionFull = "";
	@SuppressWarnings("unused")
	private static String HPCCBuildType = "";
	private static short HPCCBuildMajor = 0;
	private static float HPCCBuildMinor = 0;

	private boolean isMetaDataCached;
	private String serverAddress;
	private String cluster;
	private String wseclwatchaddress;
	private String wsecladdress;
	private String wseclwatchport;
	//private String wseclport;
	private String basicAuth;
	private String UserName;

	public HPCCDatabaseMetaData(Properties props)
	{
		super();
		this.serverAddress = props.getProperty("ServerAddress","localhost");
		this.cluster = props.getProperty("Cluster","myroxie");
		this.wseclwatchport  = props.getProperty("WsECLWatchPort");
		this.wseclwatchaddress  = props.getProperty("WsECLWatchAddress");
		this.UserName = props.getProperty("username");
		this.basicAuth = props.getProperty("BasicAuth");

		System.out.println("EclDatabaseMetaData ServerAddress: " + serverAddress + " Cluster: " + cluster + " eclwatch: " + wseclwatchaddress +":"+  wseclwatchport);

		dfufiles = new Properties();
		eclqueries = new HPCCQueries(this.cluster);
		SQLFieldMapping = new HashMap<Integer, String>();

		System.out.println("EclDatabaseMetaData initialized");

		if (!isMetaDataCached())
			setMetaDataCached(CacheMetaData());

		setSQLTypeNames();
	}

	private static void setSQLTypeNames()
	{
		//TODO
		//might want to just hardcode this list rather than creating at runtime...
		if (SQLFieldMapping.isEmpty())
		{
			Field[] fields = java.sql.Types.class.getFields();

			for (int i = 0; i < fields.length; i++) {
				try
				{
					String name = fields[i].getName();
					Integer value = (Integer) fields[i].get(null);
					SQLFieldMapping.put(value, name);
				}
				catch (IllegalAccessException e) {}
			}
		}
	}

	public static String getFieldName(Integer type)
	{
		if (SQLFieldMapping.isEmpty())
			setSQLTypeNames();

		return SQLFieldMapping.get(type);
	}

	public int getProcFieldType(String eclqueryname, String eclresultset, String fieldName)
	{
		try
		{
			HPCCQuery query = (HPCCQuery)eclqueries.getQuery(eclqueryname);
			if (query != null && query.containsField(eclresultset, fieldName))
				return query.getFieldMetaData(fieldName).getSqlType();
			return java.sql.Types.NULL;
		}
		catch (NumberFormatException e)
		{
			return java.sql.Types.NULL;
		}
	}
	public int getTableFieldType(String hpccfilename, String fieldName)
	{
		try
		{
			DFUFile file = (DFUFile)dfufiles.get(hpccfilename);
			if (file != null && file.containsField(fieldName))
				return file.getFieldMetaData(fieldName).getSqlType();
			return java.sql.Types.NULL;
		}
		catch (NumberFormatException e)
		{
			return java.sql.Types.NULL;
		}
	}

	public boolean isMetaDataCached() {
		return isMetaDataCached;
	}

	public void setMetaDataCached(boolean isMetaDataCached) {
		this.isMetaDataCached = isMetaDataCached;
	}

	private boolean CacheMetaData()
	{
		boolean isSuccess = true;
		if (serverAddress == null || cluster == null)
			return false;

		getHPCCInfo();

		try
		{
			String urlString = "http://" + wseclwatchaddress + ":" + wseclwatchport + "/WsDfu/DFUQuery?rawxml_";

			URL dfuLogicalFilesURL;
			dfuLogicalFilesURL = new URL(urlString);

			HttpURLConnection dfulogfilesConn = (HttpURLConnection)dfuLogicalFilesURL.openConnection();
			dfulogfilesConn.setInstanceFollowRedirects(false);
			System.out.println("Setting auth: " + basicAuth);
			dfulogfilesConn.setRequestProperty("Authorization", basicAuth);
			dfulogfilesConn.setRequestMethod("GET");
			dfulogfilesConn.setDoOutput(true);
			dfulogfilesConn.setDoInput(true);

			InputStream xml = dfulogfilesConn.getInputStream();

			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			Document dom = db.parse(xml);

			NodeList querySetList = dom.getElementsByTagName("DFULogicalFiles");

			if (querySetList.getLength() > 0)
			{
				NumberFormat format = NumberFormat.getInstance(Locale.US);

				NodeList queryList = querySetList.item(0).getChildNodes();
				for (int i = 0; i < queryList.getLength(); i++)
				{
					Node currentNode = queryList.item(i);
					if (currentNode.getNodeName().equals("DFULogicalFile"))
					{
						NodeList querysetquerychildren = currentNode.getChildNodes();

						DFUFile file = new DFUFile();
						for (int j = 0; j < querysetquerychildren.getLength(); j++)
						{
							if (!querysetquerychildren.item(j).getTextContent().equals(""))
							{
								String NodeName = querysetquerychildren.item(j).getNodeName();
								if (NodeName.equals("Prefix"))
								{
									file.setPrefix(querysetquerychildren.item(j).getTextContent());
								}
								else if (NodeName.equals("ClusterName"))
								{
									file.setClusterName(querysetquerychildren.item(j).getTextContent());
								}
								else if (NodeName.equals("Directory"))
								{
									file.setDirectory(querysetquerychildren.item(j).getTextContent());
								}
								//else if (NodeName.equals("Description"))
								//{
								//	file.setDescription((querysetquerychildren.item(j).getTextContent()));
								//}
								else if (NodeName.equals("Parts"))
								{
									file.setParts((Integer.parseInt(querysetquerychildren.item(j).getTextContent())));
								}
								else if (NodeName.equals("Name"))
								{
									file.setFullyQualifiedName(querysetquerychildren.item(j).getTextContent());
								}
								else if (NodeName.equals("Owner"))
								{
									file.setOwner(querysetquerychildren.item(j).getTextContent());
								}
								else if (NodeName.equals("Totalsize"))
								{
									file.setTotalSize(format.parse(querysetquerychildren.item(j).getTextContent()).longValue());
								}
								else if (NodeName.equals("RecordCount"))
								{
									file.setRecordCount(format.parse(querysetquerychildren.item(j).getTextContent()).longValue());
								}
								else if (NodeName.equals("Modified"))
								{
									file.setModified(querysetquerychildren.item(j).getTextContent());
								}
								else if (NodeName.equals("LongSize"))
								{
									file.setLongSize(format.parse(querysetquerychildren.item(j).getTextContent()).longValue());
								}
								else if (NodeName.equals("LongRecordCount"))
								{
									file.setLongRecordCount(Long.parseLong(querysetquerychildren.item(j).getTextContent()));
								}
								else if (NodeName.equals("isSuperfile"))
								{
									file.setSuperFile(querysetquerychildren.item(j).getTextContent().equals("1"));
								}
								else if (NodeName.equals("isZipfile"))
								{
									file.setZipFile(querysetquerychildren.item(j).getTextContent().equals("1"));
								}
								else if (NodeName.equals("isDirectory"))
								{
									file.setDirectory(querysetquerychildren.item(j).getTextContent());
								}
								else if (NodeName.equals("Replicate"))
								{
									file.setReplicate(Integer.parseInt(querysetquerychildren.item(j).getTextContent()));
								}
								else if (NodeName.equals("IntSize"))
								{
									file.setIntSize(Integer.parseInt(querysetquerychildren.item(j).getTextContent()));
								}
								else if (NodeName.equals("IntRecordCount"))
								{
									file.setIntRecordCount(Integer.parseInt(querysetquerychildren.item(j).getTextContent()));
								}
								else if (NodeName.equals("FromRoxieCluster"))
								{
									file.setFromRoxieCluster(querysetquerychildren.item(j).getTextContent().equals("1"));
								}
								else if (NodeName.equals("BrowseData"))
								{
									file.setBrowseData(querysetquerychildren.item(j).getTextContent().equals("1"));
								}
								else if (NodeName.equals("IsKeyFile"))
								{
									file.setIsKeyFile(querysetquerychildren.item(j).getTextContent().equals("1"));
								}
								else
									System.out.println("Unknow QuerySetElement found: " + NodeName);
							}

						}

						if (file.getFullyQualifiedName().length() > 0 && file.getClusterName()!=null)
						{
							String filedetailUrl = "http://" + wseclwatchaddress + ":" + wseclwatchport +
									"/WsDfu/DFUInfo?Name=" +
									URLEncoder.encode(file.getFullyQualifiedName(), "UTF-8") +
									"&Cluster=" +
									file.getClusterName() +
									"&rawxml_";

							//now request the schema for each roxy query
							URL queryschema = new URL(filedetailUrl);
							HttpURLConnection queryschemaconnection = (HttpURLConnection)queryschema.openConnection();
							queryschemaconnection.setInstanceFollowRedirects(false);
							queryschemaconnection.setRequestProperty("Authorization", basicAuth);
							queryschemaconnection.setRequestMethod("GET");
							queryschemaconnection.setDoOutput(true);
							queryschemaconnection.setDoInput(true);

							InputStream schema = queryschemaconnection.getInputStream();
							Document dom2 = db.parse(schema);

							Element docElement = dom2.getDocumentElement();

							// Get all pertinent detail info regarding this file (files are being treated as DB tables)
							registerFileDetails(docElement, file);

							//we might need more info if this file is actually an index:
							if(file.isKeyFile())
							{
								String openfiledetailUrl = "http://" + wseclwatchaddress + ":" + wseclwatchport +
										"/WsDfu/DFUSearchData?OpenLogicalName=" +
										URLEncoder.encode(file.getFullyQualifiedName(), "UTF-8") +
										"&Cluster=" +
										file.getClusterName() +
										"&RoxieSelections=0"+
										"&rawxml_";

								//now request the schema for each roxy query
								URL queryfiledata = new URL(openfiledetailUrl);
								HttpURLConnection queryfiledataconnection = (HttpURLConnection)queryfiledata.openConnection();
								queryfiledataconnection.setInstanceFollowRedirects(false);
								queryfiledataconnection.setRequestProperty("Authorization", basicAuth);
								queryfiledataconnection.setRequestMethod("GET");
								queryfiledataconnection.setDoOutput(true);
								queryfiledataconnection.setDoInput(true);

								InputStream filesearchinfo = queryfiledataconnection.getInputStream();
								Document dom3 = db.parse(filesearchinfo);

								Element docElement2 = dom3.getDocumentElement();

								NodeList keyfiledetail = docElement2.getChildNodes();
								if (keyfiledetail.getLength()>0)
								{
									for (int k = 0; k< keyfiledetail.getLength(); k++)
									{
										Node currentnode = keyfiledetail.item(k);
										if(currentnode.getNodeName().startsWith("DFUDataKeyedColumns"))
										{
											NodeList keyedColumns = currentnode.getChildNodes();
											for (int fieldindex = 0 ; fieldindex < keyedColumns.getLength(); fieldindex++)
											{
												Node KeyedColumn = keyedColumns.item(fieldindex);
												NodeList KeyedColumnFields = KeyedColumn.getChildNodes();
												for (int q = 0; q < KeyedColumnFields.getLength(); q++)
												{
													Node keyedcolumnfield = KeyedColumnFields.item(q);
													if (keyedcolumnfield.getNodeName().equals("ColumnLabel"))
													{
														file.addKeyedColumnInOrder(keyedcolumnfield.getTextContent());
														break;
													}
													/*currently not interested in any other field, if we need
													 * other field values, remove above break;
													 */
												}

											}
										}
										else if(currentnode.getNodeName().startsWith("DFUDataNonKeyedColumns"))
										{
											NodeList nonKeyedColumns = currentnode.getChildNodes();

											for (int fieldindex = 0 ; fieldindex < nonKeyedColumns.getLength(); fieldindex++)
											{
												Node nonKeyedColumn = nonKeyedColumns.item(fieldindex);
												NodeList nonKeyedColumnFields = nonKeyedColumn.getChildNodes();
												for (int q = 0; q < nonKeyedColumnFields.getLength(); q++)
												{
													Node nonkeyedcolumnfield = nonKeyedColumnFields.item(q);
													if (nonkeyedcolumnfield.getNodeName().equals("ColumnLabel"))
													{
														file.addNonKeyedColumnInOrder(nonkeyedcolumnfield.getTextContent());
														break;
													}
													//currently not interested in any other field, if we need
													 // other field values, remove above break;
												}
											}
										}
									}
								}
							}

							// Add this file name to files structure
							dfufiles.put(file.getFullyQualifiedName(), file);
						}
						else
							System.out.println("Found DFU file but could not determine name and/or cluster");
					}
				}
			}
			else
				System.out.println("No DFU files found, check server address and ECL watch port");
		}
		catch (MalformedURLException e)
		{
			isSuccess = false;
			e.printStackTrace();
		}
		catch (IOException e)
		{
			isSuccess = false;
			e.printStackTrace();
		}
		catch (ParserConfigurationException e)
		{
			isSuccess = false;
			e.printStackTrace();
		}
		catch (SAXException e)
		{
			isSuccess = false;
			e.printStackTrace();
		}
		catch (ParseException e)
		{
			isSuccess = false;
			e.printStackTrace();
		}

		try
		{

			String urlString = "http://" + wseclwatchaddress + ":" + wseclwatchport + "/WsWorkunits/WUQuerysetDetails?QuerySetName=" + cluster + "&rawxml_";

			URL querysetURL;
			querysetURL = new URL(urlString);

			HttpURLConnection querysetconnection = (HttpURLConnection)querysetURL.openConnection();
			querysetconnection.setInstanceFollowRedirects(false);
			querysetconnection.setRequestProperty("Authorization", basicAuth);
			querysetconnection.setRequestMethod("GET");
			querysetconnection.setDoOutput(true);
			querysetconnection.setDoInput(true);

			InputStream xml = querysetconnection.getInputStream();

			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			Document dom = db.parse(xml);

			NodeList querySetList = dom.getElementsByTagName("QuerysetQueries");

			if (querySetList.getLength() > 0)
			{
				NodeList queryList = querySetList.item(0).getChildNodes();
				for (int i = 0; i < queryList.getLength(); i++)
				{
					Node currentNode = queryList.item(i);
					if (currentNode.getNodeName().equals("QuerySetQuery"))
					{
						NodeList querysetquerychildren = currentNode.getChildNodes();

						HPCCQuery query = new HPCCQuery();
						for (int j = 0; j < querysetquerychildren.getLength(); j++)
						{
							String NodeName = querysetquerychildren.item(j).getNodeName();
							if (NodeName.equals("Id"))
							{
								query.setID(querysetquerychildren.item(j).getTextContent());
							}
							else if (NodeName.equals("Name"))
							{
								query.setName(querysetquerychildren.item(j).getTextContent());
							}
							else if (NodeName.equals("Wuid"))
							{
								query.setWUID(querysetquerychildren.item(j).getTextContent());
							}
							else if (NodeName.equals("Dll"))
							{
								query.setDLL(querysetquerychildren.item(j).getTextContent());
							}
							else if (NodeName.equals("Suspended"))
							{
								query.setSuspended(Boolean.parseBoolean(querysetquerychildren.item(j).getTextContent()));
							}
							else
								System.out.println("Unknow QuerySetElement found: " + NodeName);

						}
						//for each QuerySetQuery found above, get all schema related info:
						String queryinfourl = "http://" + wseclwatchaddress + ":" + wseclwatchport +
								"/WsWorkunits/WUInfo/WUInfoRequest?Wuid=" +
								query.getWUID() +
								"&IncludeExceptions=0" +
								"&IncludeGraphs=0" +
								"&IncludeSourceFiles=0" +
								"&IncludeTimers=0" +
								"&IncludeDebugValues=0" +
								"&IncludeApplicationValues=0" +
								"&IncludeWorkflows=0" +
								"&IncludeHelpers=0" +
								"&rawxml_";

						//now request the schema for each roxy query
						URL queryschema = new URL(queryinfourl);
						HttpURLConnection queryschemaconnection = (HttpURLConnection)queryschema.openConnection();
						queryschemaconnection.setInstanceFollowRedirects(false);
						queryschemaconnection.setRequestProperty("Authorization", basicAuth);
						queryschemaconnection.setRequestMethod("GET");
						queryschemaconnection.setDoOutput(true);
						queryschemaconnection.setDoInput(true);

						InputStream schema = queryschemaconnection.getInputStream();
						try
						{
							Document dom2 = db.parse(schema);

							Element docElement = dom2.getDocumentElement();

							// 	Get all pertinent info regarding this query (analogous to SQL DB)
							registerSchemaElements(docElement, query);

							// Add this query name to queries structure
							eclqueries.put(query.getName(),query);
						}
						catch (SAXException e)
						{
							System.out.println("Could not retreive Query info for: " + query.getName() + "(" + query.getWUID() +")");
						}
						catch (Exception e)
						{
							System.out.println("Could not retreive Query info for: " + query.getName() + "(" + query.getWUID() +")");
						}
					}
				}
			}
			else
				System.out.println("No ECL queries found, check server address, ECL cluster, and ECL watch port");
		}
		catch (IOException e)
		{
			isSuccess = false;
			e.printStackTrace();
		}
		catch (SAXException e)
		{
			isSuccess = false;
			e.printStackTrace();
		}
		catch (ParserConfigurationException e)
		{
			isSuccess = false;
			e.printStackTrace();
		}

		System.out.println("Tables found: ");
		Enumeration<Object> em = dfufiles.elements();
		while (em.hasMoreElements())
		{
			 DFUFile file = (DFUFile)em.nextElement();
			System.out.println("\t" + file.getClusterName() + "." +file.getFileName() +"("+ file.getFullyQualifiedName()+")");
		}

		System.out.println("Stored Procedures found: ");
		Enumeration<Object> em1 = eclqueries.getQueries();
		while (em1.hasMoreElements()) {
			HPCCQuery query = (HPCCQuery) em1.nextElement();
			System.out.println("\t" + query.getName() + "#" + query.getDefaultTableName());
		}

		if (!isSuccess)
			System.out.println("Could not query DB metadata check server address, cluster name, wsecl, and wseclwatch ports");
		return isSuccess;
	}

	private int registerFileDetails(Element node, DFUFile file) {

		NodeList fileDetail = node.getElementsByTagName("FileDetail");
		if (fileDetail.getLength()>0)
		{
			NodeList resultslist = fileDetail.item(0).getChildNodes(); //ECLResult nodes
			for (int i = 0; i < resultslist.getLength(); i++)
			{
				Node currentfiledetail = resultslist.item(i);

				if (currentfiledetail.getNodeName().equals("Ecl"))
				{
					//file.setFileFields(currentfiledetail.getTextContent());
					file.setFileRecDef(currentfiledetail.getTextContent());
				}
				else if (currentfiledetail.getNodeName().equals("Filename"))
				{
					file.setFileName(currentfiledetail.getTextContent());
				}
				else if (currentfiledetail.getNodeName().equals("Format"))
				{
					file.setFormat(currentfiledetail.getTextContent());
				}
				else if (currentfiledetail.getNodeName().equals("CsvSeparate"))
				{
					file.setCsvSeparate(currentfiledetail.getTextContent());
				}
				else if (currentfiledetail.getNodeName().equals("CsvQuote"))
				{
					file.setCsvQuote(currentfiledetail.getTextContent());
				}
				else if (currentfiledetail.getNodeName().equals("CsvTerminate"))
				{
					file.setCsvTerminate(currentfiledetail.getTextContent());
				}
				else if (currentfiledetail.getNodeName().equals("Description"))
				{
					file.setDescription(currentfiledetail.getTextContent());
				}
			}
		}
		return 0;
	}

	/**
	 * Translates a data type from an integer (java.sql.Types value) to a string
	 * that represents the corresponding class.
	 *
	 * @param type
	 *            The java.sql.Types value to convert to a string
	 *            representation.
	 * @return The class name that corresponds to the given java.sql.Types
	 *         value, or "java.lang.Object" if the type has no known mapping.
	 */
	public static String convertSQLtype2JavaClassName(int type)
	{
		String result = "java.lang.Object";

		switch (type)
		{
			case java.sql.Types.CHAR:
			case java.sql.Types.VARCHAR:
			case java.sql.Types.LONGVARCHAR:
				result = "java.lang.String";
				break;
			case java.sql.Types.NUMERIC:
			case java.sql.Types.DECIMAL:
				result = "java.math.BigDecimal";
				break;
			case java.sql.Types.BIT:
				result = "java.lang.Boolean";
				break;
			case java.sql.Types.TINYINT:
				result = "java.lang.Byte";
				break;
			case java.sql.Types.SMALLINT:
				result = "java.lang.Short";
				break;
			case java.sql.Types.INTEGER:
				result = "java.lang.Integer";
				break;
			case java.sql.Types.BIGINT:
				result = "java.lang.Long";
				break;
			case java.sql.Types.REAL:
				result = "java.lang.Real";
				break;
			case java.sql.Types.FLOAT:
			case java.sql.Types.DOUBLE:
				result = "java.lang.Double";
				break;
			case java.sql.Types.BINARY:
			case java.sql.Types.VARBINARY:
			case java.sql.Types.LONGVARBINARY:
				result = "java.lang.Byte[]";
				break;
			case java.sql.Types.DATE:
				result = "java.sql.Date";
				break;
			case java.sql.Types.TIME:
				result = "java.sql.Time";
				break;
			case java.sql.Types.TIMESTAMP:
				result = "java.sql.Timestamp";
				break;
		}
		return result;
	}

	private static int convertECLtypeCode2SQLtype(int ecltypecode)
	{
		int type = java.sql.Types.OTHER;

		if (ecltypecode == EclTypes.ECLTypeboolean.ordinal())
			type = java.sql.Types.BOOLEAN;
		else if (ecltypecode == EclTypes.ECLTypearray.ordinal())
			type = java.sql.Types.ARRAY;
		else if (ecltypecode == EclTypes.ECLTypeblob.ordinal())
			type = java.sql.Types.BLOB;
		else if (ecltypecode == EclTypes.ECLTypechar.ordinal())
			type = java.sql.Types.CHAR;
		else if (ecltypecode == EclTypes.ECLTypedate.ordinal())
			type = java.sql.Types.DATE;
		else if (ecltypecode == EclTypes.ECLTypedecimal.ordinal())
			type = java.sql.Types.DECIMAL;
		else if (ecltypecode ==  EclTypes.ECLTypeint.ordinal())
			type = java.sql.Types.INTEGER;
		else if (ecltypecode == EclTypes.ECLTypenull.ordinal())
			type = java.sql.Types.NULL;
		else if (ecltypecode == EclTypes.ECLTypenumeric.ordinal())
			type = java.sql.Types.NUMERIC;
		else if (ecltypecode == EclTypes.ECLTypepackedint.ordinal())
			type = java.sql.Types.INTEGER;
		else if (ecltypecode == EclTypes.ECLTypepointer.ordinal())
			type = java.sql.Types.REF;
		else if (ecltypecode == EclTypes.ECLTypeqstring.ordinal())
			type = java.sql.Types.VARCHAR;
		else if (ecltypecode == EclTypes.ECLTypereal.ordinal())
			type = java.sql.Types.REAL;
		else if (ecltypecode == EclTypes.ECLTypestring.ordinal())
			type = java.sql.Types.VARCHAR;
		else if (ecltypecode == EclTypes.ECLTypeunsigned.ordinal())
			type = java.sql.Types.NUMERIC;
		else if (ecltypecode == EclTypes.ECLTypevarstring.ordinal())
			type = java.sql.Types.VARCHAR;
		/*else if (ecltypecode == EclTypes.ECLTyperecord.ordinal():
		case EclTypes.ECLTyperow.ordinal():
		case EclTypes.ECLTyperule.ordinal():
		case EclTypes.ECLTypescalar.ordinal():
		case EclTypes.ECLTypescope.ordinal():
		case EclTypes.ECLTypeset.ordinal():
		case EclTypes.ECLTypesortlist.ordinal():
		case EclTypes.ECLTypestringorunicode.ordinal():
		case EclTypes.ECLTypeswapint.ordinal():
		case EclTypes.ECLTypetable.ordinal():
		case EclTypes.ECLTypetoken.ordinal():
		case EclTypes.ECLTypetransform.ordinal():
		case EclTypes.ECLTypeunicode.ordinal():
		case EclTypes.ECLTypeunused1.ordinal():
		case EclTypes.ECLTypeunused2.ordinal():
		case EclTypes.ECLTypeunused3.ordinal():
		case EclTypes.ECLTypeunused4.ordinal():
		case EclTypes.ECLTypeunused5.ordinal():
		case EclTypes.ECLTypeutf8.ordinal():
		case EclTypes.ECLTypevarunicode.ordinal():
		case EclTypes.ECLTypevoid.ordinal():
		case EclTypes.ECLTypeebcdic.ordinal():
		case EclTypes.ECLTypeenumerated.ordinal():
		case EclTypes.ECLTypeevent.ordinal():
		case EclTypes.ECLTypefeature.ordinal():
		case EclTypes.ECLTypefunction.ordinal():
		case EclTypes.ECLTypegroupedtable.ordinal():
		case EclTypes.ECLTypeifblock.ordinal():
		case EclTypes.ECLTypemodifier.ordinal():
		case EclTypes.ECLTypepattern.ordinal():
		case EclTypes.ECLTypealien.ordinal():
		case EclTypes.ECLTypeany.ordinal():
		case EclTypes.ECLTypebitfield.ordinal():
		case EclTypes.ECLTypeclass.ordinal():
		case EclTypes.ECLTypedata.ordinal():
		default:
			break;*/
		return type;
	}

	public static int convertECLtype2SQLtype(String ecltype)
	{
		int type = java.sql.Types.OTHER;
		String postfix = ecltype.substring(ecltype.lastIndexOf(':')+1);
		/*Simple types*/
		//if (postfix.startsWith("STRING"))
		if (postfix.contains("STRING"))
			type = java.sql.Types.VARCHAR;
		else if (postfix.startsWith("FLOAT"))
			type = java.sql.Types.FLOAT;
		else if (postfix.startsWith("DOUBLE"))
			type = java.sql.Types.DOUBLE;
		//else if (postfix.endsWith("DECIMAL"))
		else if (postfix.startsWith("DECIMAL"))
			type = java.sql.Types.DECIMAL;
		//else if (postfix.endsWith("INTEGER"))
		else if (postfix.startsWith("INTEGER"))
			type = java.sql.Types.INTEGER;
		//nonNegativeInteger
		//else if (ecltype.endsWith("NONNEGATIVEINTEGER"))
		//	type = java.sql.Types.??
		//positiveInteger
		//nonPositiveInteger
		//negativeInteger
		else if (postfix.startsWith("LONG"))
			type = java.sql.Types.NUMERIC;
		//unsignedLong
		else if (postfix.startsWith("INT"))
			type = java.sql.Types.INTEGER;
		//unsignedInt
		else if (postfix.startsWith("SHORT"))
			type = java.sql.Types.SMALLINT;
		//unsignedShort
		//else if (ecltype.endsWith("BYTE"))
		//	type = java.sql.Types.;
		else if (postfix.startsWith("UNSIGNED"))
			type = java.sql.Types.NUMERIC;
		/*XML primitive types*/
		else if (postfix.startsWith("DATETIME"))
			type = java.sql.Types.TIMESTAMP;
		else if (postfix.startsWith("TIME"))
			type = java.sql.Types.TIME;
		else if (postfix.startsWith("DATE"))
			type = java.sql.Types.DATE;
		else if (postfix.startsWith("GDAY"))
			type = java.sql.Types.DATE;
		else if (postfix.startsWith("GMONTH"))
			type = java.sql.Types.DATE;
		else if (postfix.startsWith("GYEAR"))
			type = java.sql.Types.DATE;
		else if (postfix.startsWith("GYEARMONTH"))
			type = java.sql.Types.DATE;
		else if (postfix.startsWith("GMONTHDAY"))
			type = java.sql.Types.DATE;
		else if (postfix.startsWith("DURATION"))
			type = java.sql.Types.VARCHAR;

		return type;
	}

	public static int registerSchemaElements(Element node, HPCCQuery query)
	{
		NodeList results = node.getElementsByTagName("Results");
		if (results.getLength()>0)
		{
			NodeList resultslist = results.item(0).getChildNodes(); //ECLResult nodes
			for (int i = 0; i < resultslist.getLength(); i++)
			{
				Node currentresult = resultslist.item(i);
				NodeList resultprops = currentresult.getChildNodes();
				String tablename = "";
				for (int j = 0; j < resultprops.getLength(); j++)
				{
					Node currentresultprop = resultprops.item(j);
					if (currentresultprop.getNodeName().equals("Name"))
					{
						tablename = currentresultprop.getTextContent();
						query.addResultDataset(tablename);
					}
					else if (currentresultprop.getNodeName().equals("ECLSchemas"))
					{
						NodeList schemaitems = currentresultprop.getChildNodes(); //ECLSchemaItem
						String columname = "";
						String columntype = "";
						for (int x = 0; x < schemaitems.getLength(); x++)
						{
							NodeList schemaitemfields = schemaitems.item(x).getChildNodes();

							for (int y = 0; y < schemaitemfields.getLength(); y++)
							{
								Node elem = schemaitemfields.item(y);
								if (elem.getNodeName().equals("ColumnName"))
									columname = elem.getTextContent();
								else if (elem.getNodeName().equals("ColumnType"))
									columntype = elem.getTextContent();
								//System.out.println(schemaitemfields.item(y).getNodeName() + ": " + schemaitemfields.item(y).getTextContent());
							}
							HPCCColumnMetaData elemmeta = new HPCCColumnMetaData(columname.toUpperCase(), 0, convertECLtype2SQLtype(columntype.toUpperCase()));
							elemmeta.setTableName(tablename);
							elemmeta.setParamType(procedureColumnOut);
							try {
								query.addResultElement(elemmeta);
							} catch (Exception e) {
								System.out.println("Could not add dataset element: " + tablename + ":"+ elemmeta.getColumnName());
							}
						}

					}
				}
			}
		}

		NodeList variables = node.getElementsByTagName("Variables");
		if (variables.getLength()>0)
		{
			NodeList resultslist = variables.item(0).getChildNodes(); //ECLResult nodes
			for (int i = 0; i < resultslist.getLength(); i++)
			{
				Node currentresult = resultslist.item(i);
				NodeList resultprops = currentresult.getChildNodes();
				String inParam = "";
				for (int j = 0; j < resultprops.getLength(); j++)
				{
					Node currentresultprop = resultprops.item(j);
					/*if (currentresultprop.getNodeName().equals("Name"))
					{
						inParam = currentresultprop.getTextContent();
						query.addResultDataset(inParam);
					}
					else*/
					if (currentresultprop.getNodeName().equals("ECLSchemas"))
					{
						NodeList schemaitems = currentresultprop.getChildNodes(); //ECLSchemaItem
						String columname = "";
						String columntype = "";
						for (int x = 0; x < schemaitems.getLength(); x++)
						{
							NodeList schemaitemfields = schemaitems.item(x).getChildNodes();

							for (int y = 0; y < schemaitemfields.getLength(); y++)
							{
								Node elem = schemaitemfields.item(y);
								if (elem.getNodeName().equals("ColumnName"))
									columname = elem.getTextContent();
								else if (elem.getNodeName().equals("ColumnType"))
									columntype = elem.getTextContent();
								//System.out.println(schemaitemfields.item(y).getNodeName() + ": " + schemaitemfields.item(y).getTextContent());
							}
							HPCCColumnMetaData elemmeta = new HPCCColumnMetaData(columname, i+1, convertECLtype2SQLtype(columntype.toUpperCase()));
							elemmeta.setTableName(query.getName());
							elemmeta.setParamType(procedureColumnIn);
							try {
								query.addResultElement(elemmeta);
							} catch (Exception e) {
								System.out.println("Could not add dataset element: " + inParam + ":"+ elemmeta.getColumnName());
							}
						}

					}
				}
			}
		}

		return 0;
	}

	@Override
	public boolean allProceduresAreCallable() throws SQLException {
		return false;
	}

	@Override
	public boolean allTablesAreSelectable() throws SQLException {
		return true;
	}

	@Override
	public String getURL() throws SQLException
	{
		return "http://www.hpccsystems.com";
	}

	@Override
	public String getUserName() throws SQLException {
		return UserName;
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		return true;
	}

	@Override
	public boolean nullsAreSortedHigh() throws SQLException {
		return false;
	}

	@Override
	public boolean nullsAreSortedLow() throws SQLException {
		return false;
	}

	@Override
	public boolean nullsAreSortedAtStart() throws SQLException {
		return false;
	}

	@Override
	public boolean nullsAreSortedAtEnd() throws SQLException {
		return false;
	}

	@Override
	public String getDatabaseProductName() throws SQLException {
		return "HPCC - Version " +  HPCCBuildVersionFull;
	}

	@Override
	public String getDatabaseProductVersion() throws SQLException {
		return HPCCBuildVersionFull;
	}

	@Override
	public String getDriverName() throws SQLException {
		return "HPCC JDBC Driver";
	}

	@Override
	public String getDriverVersion() throws SQLException
	{
		return HPCCVersionTracker.HPCCMajor + "." + HPCCVersionTracker.HPCCMinor + "." + HPCCVersionTracker.HPCCPoint;
	}

	@Override
	public int getDriverMajorVersion()
	{
		return HPCCVersionTracker.HPCCMajor;
	}

	@Override
	public int getDriverMinorVersion()
	{
		return HPCCVersionTracker.HPCCMinor;
	}

	@Override
	public boolean usesLocalFiles() throws SQLException
	{
		return false;
	}

	@Override
	public boolean usesLocalFilePerTable() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsMixedCaseIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesUpperCaseIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesLowerCaseIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesMixedCaseIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public String getIdentifierQuoteString() throws SQLException {
		return "";
	}

	@Override
	public String getSQLKeywords() throws SQLException {
		return "select from where AND call";
	}

	@Override
	public String getNumericFunctions() throws SQLException {
		return "";
	}

	@Override
	public String getStringFunctions() throws SQLException {
		return "";
	}

	@Override
	public String getSystemFunctions() throws SQLException {
		return "";
	}

	@Override
	public String getTimeDateFunctions() throws SQLException {
		return "";
	}

	@Override
	public String getSearchStringEscape() throws SQLException {
		return "";
	}

	@Override
	public String getExtraNameCharacters() throws SQLException {
		return "";
	}

	@Override
	public boolean supportsAlterTableWithAddColumn() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsAlterTableWithDropColumn() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsColumnAliasing() throws SQLException {
		return false;
	}

	@Override
	public boolean nullPlusNonNullIsNull() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsConvert() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsConvert(int fromType, int toType)
			throws SQLException {
		return false;
	}

	@Override
	public boolean supportsTableCorrelationNames() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsDifferentTableCorrelationNames() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsExpressionsInOrderBy() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsOrderByUnrelated() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsGroupBy() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsGroupByUnrelated() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsGroupByBeyondSelect() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsLikeEscapeClause() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsMultipleResultSets() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsMultipleTransactions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsNonNullableColumns() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsMinimumSQLGrammar() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsCoreSQLGrammar() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsExtendedSQLGrammar() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsANSI92EntryLevelSQL() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsANSI92IntermediateSQL() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsANSI92FullSQL() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsIntegrityEnhancementFacility() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsOuterJoins() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsFullOuterJoins() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsLimitedOuterJoins() throws SQLException {
		return false;
	}

	@Override
	public String getSchemaTerm() throws SQLException
	{
		//Arjuna: this will be the cluster name for now
		//throw new UnsupportedOperationException("yNot supported yet.");
		return "schema";
	}

	@Override
	public String getProcedureTerm() throws SQLException
	{
		return "NULL";
	}

	@Override
	public String getCatalogTerm() throws SQLException
	{
		//this will be the cluster name for now
		return "NULL";
	}

	@Override
	public boolean isCatalogAtStart() throws SQLException {
		return false;
	}

	@Override
	public String getCatalogSeparator() throws SQLException {
		return "NULL";
	}

	@Override
	public boolean supportsSchemasInDataManipulation() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSchemasInProcedureCalls() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSchemasInTableDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSchemasInIndexDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInDataManipulation() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInProcedureCalls() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInTableDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsPositionedDelete() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsPositionedUpdate() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSelectForUpdate() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsStoredProcedures() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsSubqueriesInComparisons() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSubqueriesInExists() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSubqueriesInIns() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSubqueriesInQuantifieds() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCorrelatedSubqueries() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsUnion() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsUnionAll() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
		return false;
	}

	@Override
	public int getMaxBinaryLiteralLength() throws SQLException {
		return 1024;
	}

	@Override
	public int getMaxCharLiteralLength() throws SQLException {
		return 1024;
	}

	@Override
	public int getMaxColumnNameLength() throws SQLException {
		return 256;
	}

	@Override
	public int getMaxColumnsInGroupBy() throws SQLException {
		return 4;
	}

	@Override
	public int getMaxColumnsInIndex() throws SQLException {
		return 4;
	}

	@Override
	public int getMaxColumnsInOrderBy() throws SQLException {
		return 4;
	}

	@Override
	public int getMaxColumnsInSelect() throws SQLException {
		return 16;
	}

	@Override
	public int getMaxColumnsInTable() throws SQLException {
		return 128;
	}

	@Override
	public int getMaxConnections() throws SQLException {
		return 1024;
	}

	@Override
	public int getMaxCursorNameLength() throws SQLException {
		return 50000;
	}

	@Override
	public int getMaxIndexLength() throws SQLException {
		return 256;
	}

	@Override
	public int getMaxSchemaNameLength() throws SQLException {
		return 256;
	}

	@Override
	public int getMaxProcedureNameLength() throws SQLException {
		return 256;
	}

	@Override
	public int getMaxCatalogNameLength() throws SQLException {
		return 256;
	}

	@Override
	public int getMaxRowSize() throws SQLException {
		return 10000;
	}

	@Override
	public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
		return true;
	}

	@Override
	public int getMaxStatementLength() throws SQLException {
		return 200;
	}

	@Override
	public int getMaxStatements() throws SQLException {
		return 100;
	}

	@Override
	public int getMaxTableNameLength() throws SQLException {
		return 100;
	}

	@Override
	public int getMaxTablesInSelect() throws SQLException {
		return 1;
	}

	@Override
	public int getMaxUserNameLength() throws SQLException {
		return 20;
		//This is an LDAP limitation, in the future the ESP will expose a query for this value
	}

	@Override
	public int getDefaultTransactionIsolation() throws SQLException {
		return java.sql.Connection.TRANSACTION_NONE;
	}

	@Override
	public boolean supportsTransactions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
		return false;
	}

	@Override
	public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
		return false;
	}

	@Override
	public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
		return false;
	}

	@Override
	public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
		return true;
	}

	@Override
	public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException
	{
		List<List> procedures = new ArrayList<List>();
		ArrayList<HPCCColumnMetaData> metacols = new ArrayList<HPCCColumnMetaData>();

		if (!isMetaDataCached())
			CacheMetaData();

		System.out.println("ECLDATABASEMETADATA GETPROCS catalog: " + catalog +", schemaPattern: " + schemaPattern + ", procedureNamePattern: " + procedureNamePattern);

		metacols.add(new HPCCColumnMetaData("PROCEDURE_CAT", 1, java.sql.Types.VARCHAR));
		metacols.add(new HPCCColumnMetaData("PROCEDURE_SCHEM", 2, java.sql.Types.VARCHAR));
		metacols.add(new HPCCColumnMetaData("PROCEDURE_NAME", 3, java.sql.Types.VARCHAR));
		metacols.add(new HPCCColumnMetaData("R1", 4, java.sql.Types.VARCHAR));
		metacols.add(new HPCCColumnMetaData("R2", 5, java.sql.Types.VARCHAR));
		metacols.add(new HPCCColumnMetaData("R6", 6, java.sql.Types.VARCHAR));
		metacols.add(new HPCCColumnMetaData("REMARKS", 7, java.sql.Types.VARCHAR));
		metacols.add(new HPCCColumnMetaData("PROCEDURE_TYPE", 8, java.sql.Types.SMALLINT));

		Enumeration<Object> queries = eclqueries.getQueries();
		while (queries.hasMoreElements())
		{
			HPCCQuery query = (HPCCQuery) queries.nextElement();
			String queryname = query.getName();

				//if(!wildproceduresearch && !procedureNamePattern.equalsIgnoreCase(queryname))
				//	continue;

				ArrayList rowValues = new ArrayList();
				procedures.add(rowValues);
				rowValues.add("");
				rowValues.add("");
				rowValues.add(queryname);
				rowValues.add("");
				rowValues.add("");
				rowValues.add("");
				rowValues.add("");
				rowValues.add(procedureResultUnknown);

				System.out.println("founc proc: " + queryname);
		}

		return new HPCCResultSet(procedures, metacols, "Procedures");
	}

	@Override
	public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException
	{
		System.out.println("ECLDATABASEMETADATA getProcedureColumns catalog: " + catalog +", schemaPattern: " + schemaPattern + ", procedureNamePattern: " + procedureNamePattern + " columnanmepat: " +columnNamePattern);

		List<List> procedurecols = new ArrayList<List>();
		ArrayList<HPCCColumnMetaData> metacols = new ArrayList<HPCCColumnMetaData>();

		if (!isMetaDataCached())
			CacheMetaData();

		System.out.println("ECLDATABASEMETADATA GETPROCS catalog: " + catalog +", schemaPattern: " + schemaPattern + ", procedureNamePattern: " + procedureNamePattern);

		boolean wildprocsearch = procedureNamePattern == null || procedureNamePattern.length()==0 || procedureNamePattern.contains("*") || procedureNamePattern.contains("%");
		boolean wildcolumnsearch = columnNamePattern == null || columnNamePattern.length()==0 || columnNamePattern.contains("*") || columnNamePattern.contains("%");

		metacols.add(new HPCCColumnMetaData("PROCEDURE_CAT", 1, java.sql.Types.VARCHAR));
		metacols.add(new HPCCColumnMetaData("PROCEDURE_SCHEM",2, java.sql.Types.VARCHAR));
		metacols.add(new HPCCColumnMetaData("PROCEDURE_NAME",3, java.sql.Types.VARCHAR));
		metacols.add(new HPCCColumnMetaData("COLUMN_NAME", 	4, java.sql.Types.VARCHAR));
		metacols.add(new HPCCColumnMetaData("COLUMN_TYPE", 	5, java.sql.Types.SMALLINT));
		metacols.add(new HPCCColumnMetaData("DATA_TYPE", 	6, java.sql.Types.INTEGER));
		metacols.add(new HPCCColumnMetaData("TYPE_NAME", 	7, java.sql.Types.VARCHAR));
		metacols.add(new HPCCColumnMetaData("PRECISION", 	8, java.sql.Types.INTEGER));
		metacols.add(new HPCCColumnMetaData("LENGTH", 		9, java.sql.Types.INTEGER));
		metacols.add(new HPCCColumnMetaData("SCALE", 		10, java.sql.Types.SMALLINT));
		metacols.add(new HPCCColumnMetaData("RADIX",			11, java.sql.Types.SMALLINT));
		metacols.add(new HPCCColumnMetaData("NULLABLE", 		12, java.sql.Types.SMALLINT));
		metacols.add(new HPCCColumnMetaData("REMARKS", 		13, java.sql.Types.VARCHAR));

		int coltype = java.sql.Types.NULL;

		Enumeration<Object> queries = eclqueries.getQueries();
		while(queries.hasMoreElements())
		{
			HPCCQuery query = (HPCCQuery)queries.nextElement();
			String eclqueryname = query.getName();
			if(!wildprocsearch && !procedureNamePattern.equalsIgnoreCase(eclqueryname))
				continue;

			String tablename = query.getDefaultTableName();

			Iterator<HPCCColumnMetaData> e = query.getAllFields().iterator();
			int index = 1;
			while (e.hasNext())
			{
				HPCCColumnMetaData col = (HPCCColumnMetaData)e.next();
				String fieldname = col.getColumnName();
				if(!wildcolumnsearch && !columnNamePattern.equalsIgnoreCase(fieldname))
					continue;

				coltype = getProcFieldType(eclqueryname,tablename, fieldname);

				System.out.println("Proc col Found: "  + eclqueryname+"#"+tablename+"."+fieldname + " of type: " + coltype + "("+convertSQLtype2JavaClassName(coltype)+")");

				ArrayList rowValues = new ArrayList();
				procedurecols.add(rowValues);

				/* 1*/rowValues.add(catalog);
				/* 2*/rowValues.add(schemaPattern);
				/* 3*/rowValues.add(query.getName());
				/* 4*/rowValues.add(fieldname);
				/* 5*/rowValues.add(col.getParamType());
				/* 6*/rowValues.add(coltype);
				/* 7*/rowValues.add(convertSQLtype2JavaClassName(coltype));
				/* 8*/rowValues.add(0);
				/* 9*/rowValues.add(0);
				/*10*/rowValues.add(0);
				/*11*/rowValues.add(1);
				/*12*/rowValues.add(procedureNoNulls);
				/*13*/rowValues.add("");

				if(!wildcolumnsearch)
					break;

				index++;
			}
			if(!wildprocsearch)
				break;
		}
		return new HPCCResultSet(procedurecols, metacols, "ProcedureColumns");
	}

	@Override
	public ResultSet getTables(String catalog, String schemaPattern,
			String tableNamePattern, String[] types) throws SQLException {
		/*
		 * TABLE_CAT String => table catalog (may be null) TABLE_SCHEM String =>
		 * table schema (may be null) TABLE_NAME String => table name TABLE_TYPE
		 * String => table type. Typical types are "TABLE", "VIEW",
		 * "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS",
		 * "SYNONYM". REMARKS String => explanatory comment on the table
		 * TYPE_CAT String => the types catalog (may be null) TYPE_SCHEM String
		 * => the types schema (may be null) TYPE_NAME String => type name (may
		 * be null) SELF_REFERENCING_COL_NAME String => name of the designated
		 * "identifier" column of a typed table (may be null) REF_GENERATION
		 * String => specifies how values in SELF_REFERENCING_COL_NAME are
		 * created. Values are "SYSTEM", "USER", "DERIVED". (may be null)
		 */

		if (!isMetaDataCached())
			CacheMetaData();

		System.out.println("ECLDATABASEMETADATA GETTABLES catalog: " + catalog +", schemaPattern: " + schemaPattern + ", tableNamePattern: " + tableNamePattern);

		//boolean wilddbsearch = schemaPattern == null || schemaPattern.length()==0 || schemaPattern.contains("*");
		boolean wildtablesearch = tableNamePattern == null || tableNamePattern.length()==0 || tableNamePattern.contains("*") || tableNamePattern.contains("%");

		List<List<String>> tables = new ArrayList<List<String>>();
		ArrayList<HPCCColumnMetaData> metacols = new ArrayList<HPCCColumnMetaData>();

		metacols.add(new HPCCColumnMetaData("TABLE_CAT", 1, java.sql.Types.VARCHAR));
		metacols.add(new HPCCColumnMetaData("TABLE_SCHEM", 2, java.sql.Types.VARCHAR));
		metacols.add(new HPCCColumnMetaData("TABLE_NAME", 3, java.sql.Types.VARCHAR));
		metacols.add(new HPCCColumnMetaData("TABLE_TYPE", 4, java.sql.Types.VARCHAR));
		metacols.add(new HPCCColumnMetaData("REMARKS", 5, java.sql.Types.VARCHAR));
		metacols.add(new HPCCColumnMetaData("TYPE_CAT", 6, java.sql.Types.VARCHAR));
		metacols.add(new HPCCColumnMetaData("TABLE_CAT", 7, java.sql.Types.VARCHAR));
		metacols.add(new HPCCColumnMetaData("TYPE_SCHEM", 8, java.sql.Types.VARCHAR));
		metacols.add(new HPCCColumnMetaData("TYPE_NAME", 9, java.sql.Types.VARCHAR));
		metacols.add(new HPCCColumnMetaData("SELF_REFERENCING_COL_NAME", 10, java.sql.Types.VARCHAR));
		metacols.add(new HPCCColumnMetaData("REF_GENERATION", 11, java.sql.Types.VARCHAR));

		Enumeration<Object> files = dfufiles.elements();
		while (files.hasMoreElements())
		{
			DFUFile file = (DFUFile) files.nextElement();
			//String filename = file.getFileName();
			String filename = file.getFullyQualifiedName();

			if(!wildtablesearch && !tableNamePattern.equalsIgnoreCase(filename))
				continue;
			System.out.println("Found table: " + filename);
			ArrayList<String> rowValues = new ArrayList<String>();
			tables.add(rowValues);
			rowValues.add(file.getClusterName());
			rowValues.add(filename);
			rowValues.add(filename);
			rowValues.add("TABLE");
			rowValues.add(file.getDescription() + (!file.hasFileRecDef() ? "**HPCC FILE DOESNOT CONTAIN ECL RECORD LAYOUT**" : "") + (file.hasRelatedIndexes() ? "Has " + file.getRelatedIndexesCount() + " related Indexes": "") +(file.isKeyFile() ? "-Keyed File " : "") + (file.isSuperFile() ? "-SuperFile " : "") + (file.isFromRoxieCluster() ? "-FromRoxieCluster" : ""));
			rowValues.add("null");
			rowValues.add("null");
			rowValues.add("null");
			rowValues.add("null");
			rowValues.add("null");

			if(!wildtablesearch)
				break;
		}
		return new HPCCResultSet(tables, metacols, "Tables");
	}

	@Override
	public ResultSet getSchemas() throws SQLException
	{
		if (!isMetaDataCached())
			CacheMetaData();

		System.out.println("ECLDATABASEMETADATA GETSCHEMAS");

		List<List<String>> tables = new ArrayList<List<String>>();
		ArrayList<HPCCColumnMetaData> metacols = new ArrayList<HPCCColumnMetaData>();

		metacols.add(new HPCCColumnMetaData("TABLE_SCHEM", 1, java.sql.Types.VARCHAR));
		metacols.add(new HPCCColumnMetaData("TABLE_CATALOG", 2, java.sql.Types.VARCHAR));

		Enumeration<Object> files = dfufiles.elements();
		while (files.hasMoreElements())
		{
			DFUFile file = (DFUFile)files.nextElement();
			String tablename = file.getFileName();

			ArrayList<String> rowValues = new ArrayList<String>();
			tables.add(rowValues);
			rowValues.add(tablename);
			rowValues.add("Catalog");

		}
		return new HPCCResultSet(tables, metacols, "Schemas");
	}

	@Override
	public ResultSet getCatalogs() throws SQLException {
		System.out.println("ECLDATABASEMETADATA getCatalogs");

		List<List<String>> catalogs = new ArrayList<List<String>>();
		ArrayList<HPCCColumnMetaData> metacols = new ArrayList<HPCCColumnMetaData>();

		metacols.add(new HPCCColumnMetaData("TABLE_CAT", 1, java.sql.Types.VARCHAR));

		ArrayList<String> rowValues = new ArrayList<String>();
		catalogs.add(rowValues);
		rowValues.add("");

		return new HPCCResultSet(catalogs, metacols, "Catalogs");
	}

	@Override
	public ResultSet getTableTypes() throws SQLException
	{
		System.out.println("ECLDATABASEMETADATA getTableTypes");

		List<List<String>> tabletypes = new ArrayList<List<String>>();
		ArrayList<HPCCColumnMetaData> metacols = new ArrayList<HPCCColumnMetaData>();

		metacols.add(new HPCCColumnMetaData("TABLE_TYPE", 1, java.sql.Types.VARCHAR));

		ArrayList<String> rowValues = new ArrayList<String>();
		tabletypes.add(rowValues);
		rowValues.add("TABLE");

		return new HPCCResultSet(tabletypes, metacols, "TableTypes");
	}

	public String [] getAllTableFields(String dbname, String tablename)
	{
		DFUFile file = (DFUFile)dfufiles.get(tablename);
		if (file != null)
			return file.getAllTableFieldsStringArray();

		return null;
	}

	@Override
	public ResultSet getColumns(String catalog, String schemaPattern,
			String tableNamePattern, String columnNamePattern)	throws SQLException
	{
		System.out.println("ECLDATABASEMETADATA GETCOLUMNS catalog: " + catalog +", schemaPattern: " + schemaPattern + ", tableNamePattern: " + tableNamePattern + ", columnNamePattern: " + columnNamePattern);

		boolean wildtablesearch = tableNamePattern == null || tableNamePattern.length()==0 || tableNamePattern.contains("*") || tableNamePattern.contains("%");
		boolean wildfieldsearch = columnNamePattern == null || columnNamePattern.length()==0 || columnNamePattern.contains("*") || columnNamePattern.contains("%");

		List<List<String>> columns = new ArrayList<List<String>>();
		ArrayList<HPCCColumnMetaData> metacols = new ArrayList<HPCCColumnMetaData>();

		metacols.add(new HPCCColumnMetaData("TABLE_CAT", 		1, java.sql.Types.VARCHAR));
		metacols.add(new HPCCColumnMetaData("TABLE_SCHEM", 		2, java.sql.Types.VARCHAR));
		metacols.add(new HPCCColumnMetaData("TABLE_NAME", 		3, java.sql.Types.VARCHAR));
		metacols.add(new HPCCColumnMetaData("COLUMN_NAME", 		4, java.sql.Types.VARCHAR));
		metacols.add(new HPCCColumnMetaData("DATA_TYPE", 		5, java.sql.Types.INTEGER));
		metacols.add(new HPCCColumnMetaData("TYPE_NAME", 		6, java.sql.Types.VARCHAR));
		metacols.add(new HPCCColumnMetaData("COLUMN_SIZE", 		7, java.sql.Types.INTEGER));
		metacols.add(new HPCCColumnMetaData("BUFFER_LENGTH", 	8, java.sql.Types.VARCHAR));
		metacols.add(new HPCCColumnMetaData("DECIMAL_DIGITS", 	9, java.sql.Types.INTEGER));
		metacols.add(new HPCCColumnMetaData("NUM_PREC_RADIX", 	10, java.sql.Types.INTEGER));
		metacols.add(new HPCCColumnMetaData("NULLABLE", 			11, java.sql.Types.INTEGER));
		metacols.add(new HPCCColumnMetaData("REMARKS", 			12, java.sql.Types.VARCHAR));
		metacols.add(new HPCCColumnMetaData("COLUMN_DEF", 		13, java.sql.Types.VARCHAR));
		metacols.add(new HPCCColumnMetaData("SQL_DATA_TYPE", 	14, java.sql.Types.INTEGER));
		metacols.add(new HPCCColumnMetaData("SQL_DATETIME_SUB", 	15, java.sql.Types.INTEGER));
		metacols.add(new HPCCColumnMetaData("CHAR_OCTET_LENGTH", 16, java.sql.Types.INTEGER));
		metacols.add(new HPCCColumnMetaData("ORDINAL_POSITION", 	17, java.sql.Types.INTEGER));
		metacols.add(new HPCCColumnMetaData("IS_NULLABLE", 		18, java.sql.Types.VARCHAR));
		metacols.add(new HPCCColumnMetaData("SCOPE_CATLOG", 		19, java.sql.Types.VARCHAR));
		metacols.add(new HPCCColumnMetaData("SCOPE_SCHEMA", 		20, java.sql.Types.VARCHAR));
		metacols.add(new HPCCColumnMetaData("SCOPE_TABLE", 		21, java.sql.Types.VARCHAR));
		metacols.add(new HPCCColumnMetaData("SOURCE_DATA_TYPE", 	22, java.sql.Types.SMALLINT));

		int coltype = java.sql.Types.NULL;

		Enumeration<Object> files = dfufiles.elements();
		while(files.hasMoreElements())
		{
			DFUFile file = (DFUFile)files.nextElement();
			//String filename = file.getFileName();
			String filename = file.getFullyQualifiedName();
			if(!wildtablesearch && !tableNamePattern.equalsIgnoreCase(filename))
				continue;

			Enumeration<Object> e = file.getAllFields();
			while (e.hasMoreElements())
			{
				HPCCColumnMetaData field = (HPCCColumnMetaData)e.nextElement();
				String fieldname = field.getColumnName();
				if(!wildfieldsearch && !columnNamePattern.equalsIgnoreCase(fieldname))
					continue;

				coltype = field.getSqlType();
				System.out.println("Table col found: "  + filename+"."+fieldname + " of type: " + coltype + "("+convertSQLtype2JavaClassName(coltype)+")");

				ArrayList rowValues = new ArrayList();
				columns.add(rowValues);

				/* 1*/rowValues.add(catalog);
				/* 2*/rowValues.add(schemaPattern);
				/* 3*/rowValues.add(file.getFullyQualifiedName());
				/* 4*/rowValues.add(fieldname);
				/* 5*/rowValues.add(coltype);
				/* 6*/rowValues.add(convertSQLtype2JavaClassName(coltype));
				/* 7*/rowValues.add(0);
				/* 8*/rowValues.add("null");
				/* 9*/rowValues.add(0);
				/*10*/rowValues.add(0);
				/*11*/rowValues.add(1);
				/*12*/rowValues.add(file.isKeyFile() && file.getIdxFilePosField() != null && file.getIdxFilePosField().equals(fieldname) ? "File Position Field" : "");
				/*13*/rowValues.add("");
				/*14*/rowValues.add(0);//unused
				/*15*/rowValues.add(0);
				/*16*/rowValues.add(0);
				/*17*/rowValues.add(field.getIndex()); //need to get index
				/*18*/rowValues.add("YES");
				/*19*/rowValues.add(coltype == java.sql.Types.REF ? null : "");
				/*20*/rowValues.add(coltype == java.sql.Types.REF ? null : "");
				/*21*/rowValues.add(coltype == java.sql.Types.REF ? null : "");
				/*22*/rowValues.add(coltype == java.sql.Types.REF ? null : "");

				if(!wildfieldsearch)
					break;
			}
			if(!wildtablesearch)
				break;
		}
		return new HPCCResultSet(columns, metacols, tableNamePattern);
	}

	@Override
	public ResultSet getColumnPrivileges(String catalog, String schema,	String table, String columnNamePattern) throws SQLException {
		throw new UnsupportedOperationException("EclDBMetaData:  getColumnPrivileges Not  supported yet.");
	}

	@Override
	public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
		throw new UnsupportedOperationException("EclDBMetaData: getTablePrivileges Not  supported yet.");
	}

	@Override
	public ResultSet getBestRowIdentifier(String catalog, String schema,
			String table, int scope, boolean nullable) throws SQLException {
		throw new UnsupportedOperationException("EclDBMetaData: getBestRowIdentifier Not  supported yet.");
	}

	@Override
	public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
		throw new UnsupportedOperationException("EclDBMetaData: getVersionColumns Not  supported yet.");
	}

	@Override
	public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
		throw new UnsupportedOperationException("EclDBMetaData: getPrimaryKeys Not  supported yet.");
	}

	@Override
	public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
		throw new UnsupportedOperationException("EclDBMetaData: getImportedKeys Not  supported yet.");
	}

	@Override
	public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException
	{
		System.out.println("ECLDATABASEMETADATA getExportedKeys catalog: " + catalog +", schema: " + schema + ", table: " + table);

		List<List<String>> exportedkeys = new ArrayList<List<String>>();
		ArrayList<HPCCColumnMetaData> metacols = new ArrayList<HPCCColumnMetaData>();

		metacols.add(new HPCCColumnMetaData("PKTABLE_CAT", 		1, java.sql.Types.VARCHAR));
		metacols.add(new HPCCColumnMetaData("PKTABLE_SCHEM", 	2, java.sql.Types.VARCHAR));
		metacols.add(new HPCCColumnMetaData("PKTABLE_NAME", 		3, java.sql.Types.VARCHAR));
		metacols.add(new HPCCColumnMetaData("PKCOLUMN_NAME", 	4, java.sql.Types.VARCHAR));
		metacols.add(new HPCCColumnMetaData("FKTABLE_CAT", 		5, java.sql.Types.VARCHAR));
		metacols.add(new HPCCColumnMetaData("FKTABLE_SCHEM", 	6, java.sql.Types.VARCHAR));
		metacols.add(new HPCCColumnMetaData("FKTABLE_NAME", 		7, java.sql.Types.VARCHAR));
		metacols.add(new HPCCColumnMetaData("FKCOLUMN_NAME", 	8, java.sql.Types.VARCHAR));
		metacols.add(new HPCCColumnMetaData("KEY_SEQ", 			9, java.sql.Types.SMALLINT));
		metacols.add(new HPCCColumnMetaData("UPDATE_RULE", 		10, java.sql.Types.SMALLINT));
		metacols.add(new HPCCColumnMetaData("DELETE_RULE", 		11, java.sql.Types.SMALLINT));
		metacols.add(new HPCCColumnMetaData("FK_NAME", 			12, java.sql.Types.VARCHAR));
		metacols.add(new HPCCColumnMetaData("PK_NAME", 			13, java.sql.Types.VARCHAR));
		metacols.add(new HPCCColumnMetaData("DEFERRABILITY", 	14, java.sql.Types.SMALLINT));

		DFUFile file = getDFUFile(table);
//		if(file != null && file.getFileName().length()>0 && file.isKeyFile())
//		{
//			Enumeration<Object> e = file.getKeyedColumns().elements();
//			while (e.hasMoreElements())
//			{
//				String fieldname = (String)e.nextElement();
//
//				ArrayList rowValues = new ArrayList();
//				exportedkeys.add(rowValues);
//
//				/* 1*/rowValues.add(catalog);
//				/* 2*/rowValues.add(schema);
//				/* 3*/rowValues.add(file.getFullyQualifiedName());
//				/* 4*/rowValues.add(fieldname);
//				/* 5*/rowValues.add("null");
//				/* 6*/rowValues.add("null");
//				/* 7*/rowValues.add("");
//				/* 8*/rowValues.add(fieldname);
//				/* 9*/rowValues.add(file.getKeyColumnIndex(fieldname));
//				/*10*/rowValues.add(importedKeyRestrict);
//				/*11*/rowValues.add(importedKeyRestrict);
//				/*12*/rowValues.add(0);
//				/*13*/rowValues.add(0);
//				/*14*/rowValues.add(importedKeyNotDeferrable);
//			}
//		}
		return new HPCCResultSet(exportedkeys, metacols, "ExportedKeys");
	}

	@Override
	public ResultSet getCrossReference(String parentCatalog,
			String parentSchema, String parentTable, String foreignCatalog,
			String foreignSchema, String foreignTable) throws SQLException {
		throw new UnsupportedOperationException("EclDBMetaData: getCrossReference Not  supported yet.");
	}

	@Override
	public ResultSet getTypeInfo() throws SQLException
	{

		System.out.println("ECLDATABASEMETADATA GETTYPEINFO");

		List<List<String>> types = new ArrayList<List<String>>();
		ArrayList<HPCCColumnMetaData> metacols = new ArrayList<HPCCColumnMetaData>();

		metacols.add(new HPCCColumnMetaData("TYPE_NAME", 		1, java.sql.Types.VARCHAR));
		metacols.add(new HPCCColumnMetaData("DATA_TYPE", 		2, java.sql.Types.INTEGER));//SQL data type from java.sql.Types
		metacols.add(new HPCCColumnMetaData("PRECISION", 		3, java.sql.Types.INTEGER));//maximum precision
		metacols.add(new HPCCColumnMetaData("LITERAL_PREFIX", 	4, java.sql.Types.VARCHAR));//prefix used to quote a literal (may be null)
		metacols.add(new HPCCColumnMetaData("LITERAL_SUFFIX", 	5, java.sql.Types.VARCHAR));//suffix used to quote a literal (may be null)
		metacols.add(new HPCCColumnMetaData("CREATE_PARAMS", 	6, java.sql.Types.VARCHAR));//parameters used in creating the type (may be null)
		metacols.add(new HPCCColumnMetaData("NULLABLE", 			7, java.sql.Types.SMALLINT));//can you use NULL for this type.	    typeNoNulls - does not allow NULL values	    typeNullable - allows NULL values	    typeNullableUnknown - nullability unknown
		metacols.add(new HPCCColumnMetaData("CASE_SENSITIVE", 	8, java.sql.Types.BOOLEAN));
		metacols.add(new HPCCColumnMetaData("SEARCHABLE", 		9, java.sql.Types.SMALLINT));
		metacols.add(new HPCCColumnMetaData("UNSIGNED_ATTRIBUTE",10, java.sql.Types.BOOLEAN));
		metacols.add(new HPCCColumnMetaData("FIXED_PREC_SCALE", 	11, java.sql.Types.BOOLEAN));//can it be a money value
		metacols.add(new HPCCColumnMetaData("AUTO_INCREMENT", 	12, java.sql.Types.BOOLEAN));//can it be used for an auto-increment value
		metacols.add(new HPCCColumnMetaData("LOCAL_TYPE_NAME", 	13, java.sql.Types.VARCHAR));//localized version of type name (may be null)
		metacols.add(new HPCCColumnMetaData("MINIMUM_SCALE", 	14, java.sql.Types.SMALLINT));
		metacols.add(new HPCCColumnMetaData("MAXIMUM_SCALE", 	15, java.sql.Types.SMALLINT));
		metacols.add(new HPCCColumnMetaData("SQL_DATA_TYPE", 	16, java.sql.Types.INTEGER));//unused
		metacols.add(new HPCCColumnMetaData("SQL_DATETIME_SUB", 	17, java.sql.Types.INTEGER));//unused
		metacols.add(new HPCCColumnMetaData("NUM_PREC_RADIX", 	18, java.sql.Types.INTEGER));//unused

		for(EclTypes ecltype : EclTypes.values())
		{
			ArrayList rowValues = new ArrayList();
			types.add(rowValues);

			/* 1*/rowValues.add(String.valueOf(ecltype));
			/* 2*/rowValues.add(convertECLtypeCode2SQLtype(ecltype.ordinal()));
			/* 3*/rowValues.add(0);
			/* 4*/rowValues.add(null);
			/* 5*/rowValues.add(null);
			/* 6*/rowValues.add(null);
			/* 7*/rowValues.add(typeNullableUnknown);
			/* 8*/rowValues.add(false);
			/* 9*/rowValues.add(typePredNone);
			/*10*/rowValues.add(false);
			/*11*/rowValues.add(false);
			/*12*/rowValues.add(false);
			/*13*/rowValues.add(convertSQLtype2JavaClassName(convertECLtypeCode2SQLtype(ecltype.ordinal())));
			/*14*/rowValues.add(0);
			/*15*/rowValues.add(0);
			/*16*/rowValues.add(0);
			/*17*/rowValues.add(0);
			/*18*/rowValues.add(0);
		}

		return new HPCCResultSet(types, metacols, "Types");
	}

	@Override
	public ResultSet getIndexInfo(String catalog, String schema, String table,
			boolean unique, boolean approximate) throws SQLException {
		throw new UnsupportedOperationException("EclDBMetaData: Not  supported yet.");
	}

	@Override
	public boolean supportsResultSetType(int type) throws SQLException {
		return true;
	}

	@Override
	public boolean supportsResultSetConcurrency(int type, int concurrency)	throws SQLException {
		return false;
	}

	@Override
	public boolean ownUpdatesAreVisible(int type) throws SQLException {
		return false;
	}

	@Override
	public boolean ownDeletesAreVisible(int type) throws SQLException {
		return false;
	}

	@Override
	public boolean ownInsertsAreVisible(int type) throws SQLException {
		return false;
	}

	@Override
	public boolean othersUpdatesAreVisible(int type) throws SQLException {
		return false;
	}

	@Override
	public boolean othersDeletesAreVisible(int type) throws SQLException {
		return false;
	}

	@Override
	public boolean othersInsertsAreVisible(int type) throws SQLException {
		return false;
	}

	@Override
	public boolean updatesAreDetected(int type) throws SQLException {
		return false;
	}

	@Override
	public boolean deletesAreDetected(int type) throws SQLException {
		return false;
	}

	@Override
	public boolean insertsAreDetected(int type) throws SQLException {
		return false;
	}

	@Override
	public boolean supportsBatchUpdates() throws SQLException {
		return false;
	}

	@Override
	public ResultSet getUDTs(String catalog, String schemaPattern,
			String typeNamePattern, int[] types) throws SQLException {
		/*
				TYPE_CAT String => the type's catalog (may be null)
	    	    TYPE_SCHEM String => type's schema (may be null)
	    	    TYPE_NAME String => type name
	    	    CLASS_NAME String => Java class name
	    	    DATA_TYPE int => type value defined in java.sql.Types. One of JAVA_OBJECT, STRUCT, or DISTINCT
	    	    REMARKS String => explanatory comment on the type
	    	    BASE_TYPE short => type code of the source type of a DISTINCT type or the type that implements the user-generated reference type of the SELF_REFERENCING_COLUMN of a structured type as defined in java.sql.Types (null if DATA_TYPE is not DISTINCT or not STRUCT with REFERENCE_GENERATION = USER_DEFINED)

	    	Note: If the driver does not support UDTs, an empty result set is returned.*/
	    	System.out.println("ECLDATABASEMETADATA GETTYPEINFO");

	    		List<List<String>> udts = new ArrayList<List<String>>();
	    		ArrayList<HPCCColumnMetaData> metacols = new ArrayList<HPCCColumnMetaData>();

	    		metacols.add(new HPCCColumnMetaData("TYPE_CAT", 		1, java.sql.Types.VARCHAR)); //the type's catalog (may be null)
	    		metacols.add(new HPCCColumnMetaData("TYPE_SCHEM", 	2, java.sql.Types.VARCHAR)); //type's schema (may be null)
	    		metacols.add(new HPCCColumnMetaData("TYPE_NAME", 	3, java.sql.Types.VARCHAR)); //type name
	    		metacols.add(new HPCCColumnMetaData("CLASS_NAME", 	4, java.sql.Types.VARCHAR)); //Java class name
	    		metacols.add(new HPCCColumnMetaData("DATA_TYPE", 	5, java.sql.Types.INTEGER)); //type value defined in java.sql.Types. One of JAVA_OBJECT, STRUCT, or DISTINCT
	    		metacols.add(new HPCCColumnMetaData("REMARKS", 		6, java.sql.Types.VARCHAR)); //explanatory comment on the type
	    		metacols.add(new HPCCColumnMetaData("BASE_TYPE", 	7, java.sql.Types.SMALLINT)); //

	    		/*for(EclTypes ecltype : EclTypes.values())
	    		{
	    			ArrayList rowValues = new ArrayList();
	    			udts.add(rowValues);

	    			rowValues.add(String.valueOf(ecltype));
	    			rowValues.add(convertECLtypeCode2SQLtype(ecltype.ordinal()));
	    			rowValues.add(0);
	    			rowValues.add(null);
	    			rowValues.add(null);
	    			rowValues.add(null);
	    			rowValues.add(typeNullableUnknown);
	    		}*/

	    		return new HPCCResultSet(udts, metacols, "UDTs");

	}

	@Override
	public Connection getConnection() throws SQLException {
		throw new UnsupportedOperationException("EclDBMetaData: getConnection Not  supported yet.");
	}

	@Override
	public boolean supportsSavepoints() throws SQLException {
		throw new UnsupportedOperationException("EclDBMetaData: supportsSavepoints Not  supported yet.");
	}

	@Override
	public boolean supportsNamedParameters() throws SQLException {
		throw new UnsupportedOperationException("EclDBMetaData: supportsNamedParameters Not  supported yet.");
	}

	@Override
	public boolean supportsMultipleOpenResults() throws SQLException {
		throw new UnsupportedOperationException("EclDBMetaData: supportsMultipleOpenResults Not  supported yet.");
	}

	@Override
	public boolean supportsGetGeneratedKeys() throws SQLException {
		throw new UnsupportedOperationException("EclDBMetaData: supportsGetGeneratedKeysNot  supported yet.");
	}

	@Override
	public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
		throw new UnsupportedOperationException("EclDBMetaData: getSuperTypes Not  supported yet.");
	}

	@Override
	public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
		throw new UnsupportedOperationException("EclDBMetaData: getSuperTables Not  supported yet.");
	}

	@Override
	public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
		throw new UnsupportedOperationException("EclDBMetaData: getAttributes Not  supported yet.");
	}

	@Override
	public boolean supportsResultSetHoldability(int holdability) throws SQLException {
		throw new UnsupportedOperationException("EclDBMetaData: supportsResultSetHoldability(int holdability) Not  supported yet.");
	}

	@Override
	public int getResultSetHoldability() throws SQLException {
		throw new UnsupportedOperationException("EclDBMetaData: getResultSetHoldability Not  supported yet.");
	}

	@Override
	public int getDatabaseMajorVersion() throws SQLException
	{
		return HPCCBuildMajor;
	}

	@Override
	public int getDatabaseMinorVersion() throws SQLException {
		return (int)HPCCBuildMinor;
	}

	private boolean getHPCCInfo()
	{
		if (serverAddress == null || cluster == null)
			return false;

		//String urlString = "http://" + serverAddress + ":"+wseclwatchport+"/WsSMC/Activity?rawxml_";
		String urlString = "http://" + wseclwatchaddress + ":"+wseclwatchport+"/WsSMC/Activity?rawxml_";

		URL querysetURL;

		try
		{
			querysetURL = new URL(urlString);

			HttpURLConnection querysetconnection = (HttpURLConnection)querysetURL.openConnection();
			querysetconnection.setInstanceFollowRedirects(false);
			querysetconnection.setRequestProperty("Authorization", basicAuth);
			querysetconnection.setRequestMethod("GET");
			querysetconnection.setDoOutput(true);
			querysetconnection.setDoInput(true);

			InputStream xml = querysetconnection.getInputStream();

			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			Document dom = db.parse(xml);


			NodeList activityList = dom.getElementsByTagName("ActivityResponse");
			if (activityList.getLength() > 0)
			{
				NodeList queryList = activityList.item(0).getChildNodes();
				for (int i = 0; i < queryList.getLength(); i++)
				{
					Node currentNode = queryList.item(i);

					/*Get build block*/
					if (currentNode.getNodeName().equals("Build"))
					{
						Node buildNode = currentNode.getFirstChild();

						if (buildNode != null && buildNode.getNodeType() == Node.TEXT_NODE)
							HPCCBuildVersionFull = buildNode.getTextContent();

						parseVersionString();

						break;
					}
					/*Get whatever other element block
					 *
					 * else if (currentNode.getNodeName().equals("RoxieClusters"))
					 * REMOVE THE ABOVE BREAK!!!!!
					 *
					 */
				}
			}
		}
		catch (Exception e)
		{
			return false;
		}

		return true;
	}

	private void parseVersionString()
	{
		try
		{
			HPCCBuildType = HPCCBuildVersionFull.substring(0, HPCCBuildVersionFull.lastIndexOf('_'));

			//String numeric = HPCCBuildVersionFull.substring(HPCCBuildVersionFull.lastIndexOf('_')+1, HPCCBuildVersionFull.indexOf('-'));
			String numeric = HPCCBuildVersionFull.substring(HPCCBuildVersionFull.indexOf('_')+1, HPCCBuildVersionFull.indexOf('-'));

			HPCCBuildMajor = Short.parseShort(numeric.substring(0, numeric.indexOf('.')));
			HPCCBuildMinor = Float.parseFloat(numeric.substring(numeric.indexOf('.')+1));
		}
		catch (Exception e)
		{
			System.out.println("Error parsing HPCC version: " + e.getMessage());
		}
	}

	@Override
	public int getJDBCMajorVersion() throws SQLException {
		return JDBCVerMajor;
	}

	@Override
	public int getJDBCMinorVersion() throws SQLException {
		return JDBCVerMinor;
	}

	@Override
	public int getSQLStateType() throws SQLException {
		throw new UnsupportedOperationException("EclDBMetaData: getSQLStateType Not  supported yet.");
	}

	@Override
	public boolean locatorsUpdateCopy() throws SQLException {
		throw new UnsupportedOperationException("EclDBMetaData: locatorsUpdateCopy Not  supported yet.");
	}

	@Override
	public boolean supportsStatementPooling() throws SQLException {
		throw new UnsupportedOperationException("EclDBMetaData: supportsStatementPooling Not  supported yet.");
	}

	@Override
	public RowIdLifetime getRowIdLifetime() throws SQLException {
		throw new UnsupportedOperationException("EclDBMetaData: getRowIdLifetime Not  supported yet.");
	}

	@Override
	public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
		throw new UnsupportedOperationException("EclDBMetaData: getSchemas Not supported yet.");
	}

	@Override
	public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
		throw new UnsupportedOperationException("EclDBMetaData:  supportsStoredFunctionsUsingCallSyntax Not supported yet.");
	}

	@Override
	public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
		throw new UnsupportedOperationException("EclDBMetaData: autoCommitFailureClosesAllResultSets Not  supported yet.");
	}

	@Override
	public ResultSet getClientInfoProperties() throws SQLException
	{
		//System.out.println("EclDBMetaData: getClientInfoProperties");
		throw new UnsupportedOperationException("EclDBMetaData: getClientInfoProperties Not  supported yet.");
	}

	@Override
	public ResultSet getFunctions(String catalog, String schemaPattern,	String functionNamePattern) throws SQLException {
		throw new UnsupportedOperationException("EclDBMetaData: getFunctions(String catalog, String schemaPattern, String functionNamePattern) Not  supported yet.");
	}

	@Override
	public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern)	throws SQLException {
		throw new UnsupportedOperationException("EclDBMetaData: getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) Not  supported yet.");
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new UnsupportedOperationException("EclDBMetaData: unwrap(Class<T> iface) Not  supported yet.");
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new UnsupportedOperationException("EclDBMetaData: isWrapperFor(Class<?> iface) Not  supported yet.");
	}

	public String getBasicAuth()
	{
		return basicAuth;
	}
	public enum EclTypes
	{
		ECLTypeboolean(0),
		ECLTypeint(1),
		ECLTypereal(2),
		ECLTypedecimal(3),
		ECLTypestring(4),
		ECLTypeunused1(5),
		ECLTypedate(6),
		ECLTypeunused2(7),
		ECLTypeunused3(8),
		ECLTypebitfield(9),
		ECLTypeunused4(10),
		ECLTypechar(11),
		ECLTypeenumerated(12),
		ECLTyperecord(13),
		ECLTypevarstring(14),
		ECLTypeblob(15),
		ECLTypedata(16),
		ECLTypepointer(17),
		ECLTypeclass(18),
		ECLTypearray(19),
		ECLTypetable(20),
		ECLTypeset(21),
		ECLTyperow(22),
		ECLTypegroupedtable(23),
		ECLTypevoid(24),
		ECLTypealien(25),
		ECLTypeswapint(26),
		ECLTypepackedint(28),
		ECLTypeunused5(29),
		ECLTypeqstring(30),
		ECLTypeunicode(31),
		ECLTypeany(32),
		ECLTypevarunicode(33),
		ECLTypepattern(34),
		ECLTyperule(35),
		ECLTypetoken(36),
		ECLTypefeature(37),
		ECLTypeevent(38),
		ECLTypenull(39),
		ECLTypescope(40),
		ECLTypeutf8(41),
		ECLTypetransform(42),
		ECLTypeifblock(43), // not a real type -but used for the rtlfield serialization
		ECLTypefunction(44),
		ECLTypesortlist(45),
		ECLTypemodifier(0xff), // used by getKind()
		ECLTypeunsigned(0x100),// combined with some of the above, when returning summary type information. Not returned by getTypeCode()
		ECLTypeebcdic(0x200),// combined with some of the above, when returning summary type information. Not returned by getTypeCode()
		//Some pseudo types - never actually created
		ECLTypestringorunicode(0xfc),// any string/unicode variant
		ECLTypenumeric(0xfd),
		ECLTypescalar(0xfe);

	    EclTypes(int eclcode)  {}

	 /*   public String getTypeName()
	    {
	    	switch(this.ordinal())
			{
				case EclTypes.ECLTypeboolean:
				case java.sql.Types.VARCHAR:
				case java.sql.Types.LONGVARCHAR:
					result = "java.lang.String";
					break;
				case java.sql.Types.NUMERIC:
				case java.sql.Types.DECIMAL:
					result = "java.math.BigDecimal";
					break;
				case java.sql.Types.BIT:
					result = "java.lang.Boolean";
					break;
				case java.sql.Types.TINYINT:
					result = "java.lang.Byte";
					break;
				case java.sql.Types.SMALLINT:
					result = "java.lang.Short";
					break;
				case java.sql.Types.INTEGER:
					result = "java.lang.Integer";
					break;
				case java.sql.Types.BIGINT:
					result = "java.lang.Long";
					break;
				case java.sql.Types.REAL:
					result = "java.lang.Real";
					break;
				case java.sql.Types.FLOAT:
				case java.sql.Types.DOUBLE:
					result = "java.lang.Double";
					break;
				case java.sql.Types.BINARY:
				case java.sql.Types.VARBINARY:
				case java.sql.Types.LONGVARBINARY:
					result = "java.lang.Byte[]";
					break;
				case java.sql.Types.DATE:
					result = "java.sql.Date";
					break;
				case java.sql.Types.TIME:
					result = "java.sql.Time";
					break;
				case java.sql.Types.TIMESTAMP:
					result = "java.sql.Timestamp";
					break;
	    	}
	    	return "";
	    }*/

	}

	/*public String findAlias(String name)
	{
		return eclQueryAliases.getProperty(name, null);
	}*/

	public String getdefaultECLQueryResultDatasetName(String clustername,	String eclqueryname)
	{
		HPCCQuery query = eclqueries.getQuery(eclqueryname);
		return query == null ? "" : query.getDefaultTableName();
	}

	public boolean tableExists(String clustername, String eclqueryname)
	{
		return dfufiles.containsKey(eclqueryname);
	}

	public boolean eclQueryExists(String clustername, String eclqueryname)
	{
		return eclqueries.getQuery(eclqueryname) == null ? false : true;
	}

	public boolean allFieldsExist(String clustername, String eclqueryname, String[] columnNames)
	{
		HPCCQuery query = eclqueries.getQuery(eclqueryname);
		if(query != null  && columnNames != null)
		{
			for (int i = 0; i < columnNames.length; i++)
			{
				if(!query.containsField(columnNames[i]))
					return false;
			}
			return true;
		}

		return false;
	}

	public boolean allDFUFileFieldsExist(String clustername, String dfufilename, String[] columnNames)
	{
		DFUFile file = (DFUFile)dfufiles.get(dfufilename);

		if(file != null  && columnNames != null)
		{
			for (int i = 0; i < columnNames.length; i++)
			{
				if(!file.containsField(columnNames[i]))
					return false;
			}
			return true;
		}

		return false;
	}

	public ArrayList<HPCCColumnMetaData> getStoredProcOutColumns(String cluster, String eclqueryname)
	{
		HPCCQuery query = eclqueries.getQuery(eclqueryname);
		if(query != null)
		{
			return query.getAllNonInFields();
		}
		return null;
	}

	public ArrayList<HPCCColumnMetaData> getStoredProcInColumns(String cluster, String eclqueryname)
	{
		HPCCQuery query = eclqueries.getQuery(eclqueryname);
		if(query != null)
		{
			return query.getAllInFields();
		}
		return null;

	}

	public HPCCColumnMetaData getTableColumn(String hpccfilename, String fieldName)
	{
		try
		{
			DFUFile file = (DFUFile)dfufiles.get(hpccfilename);
			if (file != null && file.containsField(fieldName))
				return file.getFieldMetaData(fieldName);
			return null;
		}
		catch (NumberFormatException e)
		{
			return null;
		}
	}

	public DFUFile getDFUFile(String hpccfilename)
	{
		return (DFUFile)dfufiles.get(hpccfilename);
	}
}
