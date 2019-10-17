/**
 * 
 */
package org.dragonli.service.db.service.metadata;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.dragonli.tools.general.StringUtil;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.core.PreparedStatementCreator;

import org.dragonli.service.db.service.DBConfig;

/**
 * @author freeangel
 *
 */
public class TableStructure {
	
	public static final ConcurrentMap<Integer,Object> defaultVales = new ConcurrentHashMap<>();
	static
	{
		defaultVales.put(Types.ARRAY,new byte[0]);//存疑，不过基本不用这个类型。这种字段最好默认允许为null
		defaultVales.put(Types.BIGINT,0);
		defaultVales.put(Types.BINARY,new byte[0]);
		defaultVales.put(Types.BIT,(byte)0);
		defaultVales.put(Types.BLOB,new byte[0]);//存疑，也许应该是String?这种字段最好默认允许为null
		defaultVales.put(Types.BOOLEAN,false);
		defaultVales.put(Types.CHAR,(char)0);
		defaultVales.put(Types.CLOB,new byte[0]);//存疑，也许应该是String?这种字段最好默认允许为null
		defaultVales.put(Types.DATALINK,new byte[0]);//存疑，不过基本不用这个类型。这种字段最好默认允许为null
		defaultVales.put(Types.DATE,new Date(0));
		defaultVales.put(Types.DECIMAL,0);//存疑，不过基本不用这个类型。这种字段最好默认允许为null
		defaultVales.put(Types.DISTINCT,new byte[0]);//存疑，不过基本不用这个类型。这种字段最好默认允许为null
		defaultVales.put(Types.DOUBLE,(double)0);
		defaultVales.put(Types.FLOAT,0.0f);
		defaultVales.put(Types.INTEGER,0);
		defaultVales.put(Types.JAVA_OBJECT,new byte[0]);//存疑，不过基本不用这个类型。这种字段最好默认允许为null
		defaultVales.put(Types.LONGNVARCHAR,"");
		defaultVales.put(Types.LONGVARBINARY,new byte[0]);
		defaultVales.put(Types.NCHAR,"");
		defaultVales.put(Types.NCLOB,new byte[0]);
		defaultVales.put(Types.NUMERIC,0);
		defaultVales.put(Types.NVARCHAR,"");
		defaultVales.put(Types.OTHER,new byte[0]);//存疑，不过基本不用这个类型。这种字段最好默认允许为null
		defaultVales.put(Types.REAL,new byte[0]);//存疑，不过基本不用这个类型。这种字段最好默认允许为null
		defaultVales.put(Types.REF,new byte[0]);//存疑，不过基本不用这个类型。这种字段最好默认允许为null
		defaultVales.put(Types.REF_CURSOR,new byte[0]);//存疑，不过基本不用这个类型。这种字段最好默认允许为null
		defaultVales.put(Types.ROWID,0);//存疑，不过基本不用这个类型。这种字段最好默认允许为null
		defaultVales.put(Types.SMALLINT,(short)0);
		defaultVales.put(Types.SQLXML,"");//存疑，不过基本不用这个类型。这种字段最好默认允许为null
		defaultVales.put(Types.STRUCT,new byte[0]);//存疑，不过基本不用这个类型。这种字段最好默认允许为null
		defaultVales.put(Types.TIME,new Time(0));
		defaultVales.put(Types.TIME_WITH_TIMEZONE,new Time(0));
		defaultVales.put(Types.TIMESTAMP,new Timestamp(0));
		defaultVales.put(Types.TIMESTAMP_WITH_TIMEZONE,new Timestamp(0));
		defaultVales.put(Types.TINYINT,(byte)0);
		defaultVales.put(Types.VARBINARY,new byte[0]);
		defaultVales.put(Types.VARCHAR,"");
		
//		defaultVales.put(Types.NULL,(char)0);
		
	}
	
	protected final ConcurrentMap<Object,AtomicInteger> optimisticLock = new ConcurrentHashMap<>();
	
	public ConcurrentMap<Object, AtomicInteger> getOptimisticLock() {
		return optimisticLock;
	}

	public AtomicInteger findOptimisticLock(Object pk)
	{
		AtomicInteger lock = optimisticLock.get(pk);
		if( lock == null )
		{
			optimisticLock.putIfAbsent(pk, new AtomicInteger(0));
			lock = optimisticLock.get(pk);
		}
		return lock;
	}
	
	private DBConfig config;
	
	public DBConfig getConfig() {
		return config;
	}

	private String primaryKey;
	private String tableName;
	protected Map<String,Integer> cols = new ConcurrentHashMap<String,Integer>();
	protected String[] colList ;
	protected int[] colsType;
	protected boolean[] colsNullAble;
	public String getSelectManySql() {
		return selectManySql;
	}

	protected String selectAllCols;
	protected String insertAllCols;
	public String getSelectMoreSql() {
		return selectMoreSql;
	}

	public String getSelectCountSql() {
		return selectCountSql;
	}

	protected String updateAllCols;
	
	protected String insertSql;
	protected String delSql;
	protected String delManySql;
	protected String delMoreSql;
	
	protected String logicDelSql;
	protected String logicDelManySql;
	protected String logicDelMoreSql;
	protected String selectCountDisactiveSql;
	protected String selectDisactiveSql;
	
	public String getSelectDisactiveSql() {
		return selectDisactiveSql;
	}

	public String getLogicDelSql() {
		return logicDelSql;
	}

	public String getLogicDelManySql() {
		return logicDelManySql;
	}

	public String getLogicDelMoreSql() {
		return logicDelMoreSql;
	}

	private String selectMoreSql;
	private String selectCountSql;
	public String getDelManySql() {
		return delManySql;
	}

	public String getDelMoreSql() {
		return delMoreSql;
	}

	protected String selectSql;
	protected String selectManySql;
	protected String updateSql;

	protected boolean useActive;
	
	public boolean isUseActive() {
		return useActive;
	}

	public String getPrimaryKey() {
		return primaryKey;
	}

	public String getTableName() {
		return tableName;
	}

	public Map<String, Integer> getCols() {
		return cols;
	}

	public String[] getColList() {
		return colList;
	}

	public int[] getColsType() {
		return colsType;
	}

	public boolean[] getColsNullAble() {
		return colsNullAble;
	}

	public String getSelectCountDisactiveSql() {
		return selectCountDisactiveSql;
	}

	public String getSelectAllCols() {
		return selectAllCols;
	}

	public String getInsertAllCols() {
		return insertAllCols;
	}

	public String getUpdateAllCols() {
		return updateAllCols;
	}

	public String getInsertSql() {
		return insertSql;
	}

	public String getDelSql() {
		return delSql;
	}

	public String getSelectSql() {
		return selectSql;
	}

	public String getUpdateSql() {
		return updateSql;
	}
	
	public TableStructure(String tableName,DBConfig config)
	{
		this.tableName = tableName;
		this.config = config;
		primaryKey = config.getPrimaryKey();
	}
	
	public void init(JdbcTemplate jdbcTemplate)
	{
		jdbcTemplate.execute(
			new PreparedStatementCreator()
		    {  
		        public PreparedStatement createPreparedStatement(Connection connection) throws SQLException
		        {  
		              
					String sql = " select * from "+tableName+" where "+config.getPrimaryKey()+"=0 ";
					PreparedStatement ps = connection.prepareStatement(sql,Statement.NO_GENERATED_KEYS);  
		               return ps;  
		        }  
		    },
			new PreparedStatementCallback<Boolean>() 
			{  
				public Boolean doInPreparedStatement(PreparedStatement pstmt) throws SQLException, DataAccessException
				{  
					pstmt.execute();
					ResultSet rs = pstmt.getResultSet();
					ResultSetMetaData data = rs.getMetaData();
					
					colList = new String[data.getColumnCount()];
					colsType = new int[data.getColumnCount()];
					colsNullAble = new boolean[data.getColumnCount()];
					
					useActive = false;
					for( int i = 1 ; i <= data.getColumnCount() ; i++ )
					{
						colsType[i-1] = data.getColumnType(i);
						colList[i-1] = data.getColumnName(i);
						colsNullAble[i-1] = data.isNullable(i) == ResultSetMetaData.columnNullable;
						if( config.getActiveTag() != null && config.getActiveTag().equals(data.getColumnName(i)) )
							useActive = true;
//						cols.put(name, i-1);
					}
//					
					if( !colList[ 0 ].equals(config.getPrimaryKey()) )
					{
						//让主键处于第一位
						int index = 0;
						int type = 0;
						for( int i = 0 ; i < colList.length ; i++ )
						{
							if( colList[ i ].equals(config.getPrimaryKey()) )
							{
								index = i;
								type = colsType[i];
								break;
							}
						}
						for(int i = index ;i>0;i--)
						{
							colList[i]=colList[i-1];
							colsType[i]=colsType[i-1];
							colsNullAble[i]=colsNullAble[i-1];
						}
						colList[0] = config.getPrimaryKey();
						colsType[0] = type;
						colsNullAble[0] = false;
					}
						
					
					
//					String.join(delimiter, elements)//不适合
					selectAllCols = config.getPrimaryKey();
					updateAllCols = "";
					insertAllCols = "";
					for( int i = 1 ; i < colList.length ; i++ )
					{
//						if( colList[ i ].equals(config.getPrimaryKey()) )
//							continue;
						if( !selectAllCols.equals("") )
							selectAllCols += ",";
						if( !updateAllCols.equals("") )
							updateAllCols += ",";
						if( !insertAllCols.equals("") )
							insertAllCols += ",";
						
						selectAllCols += colList[ i ];
						updateAllCols += colList[ i ]+"=?";
						insertAllCols += colList[ i ];
					}
					insertSql = "insert into {0}({1}) values ({2})";
					
					String[] temp = new String[colList.length-1];
					Arrays.fill(temp, "?");
					insertSql = StringUtil.substitute( insertSql ,
							new Object[]{ tableName,insertAllCols,String.join(",", Arrays.asList(temp)) } );
					
					updateSql = "update {0} set {1} where {2}=?";
					updateSql = StringUtil.substitute( updateSql ,
							new Object[]{ tableName,updateAllCols,config.getPrimaryKey() } );
					
					selectSql = "select {0} from {1} where {2}=?";
					selectSql = StringUtil.substitute( selectSql ,
							new Object[]{ selectAllCols,tableName,config.getPrimaryKey() } );
					
//					selectSql = "select {0} from {1} where {2}=?";
//					selectSql = StringUtil.substitute( selectSql ,
//							new Object[]{ selectAllCols,tableName,config.getPrimaryKey() } );
					
					selectMoreSql = "select "+selectAllCols+" from "+tableName+" where 1=1 "
									+ ( useActive ? "and "+config.getActiveTag()+"=1 {0}" : "{0}" );
					selectManySql = "select "+selectAllCols+" from "+tableName+" where "+config.getPrimaryKey()+" in ({0}) "
									+ ( useActive ? "and "+config.getActiveTag()+"=1 " : "" );
					selectCountSql = "select count(*) from "+tableName+" where 1=1 "
									+ ( useActive ? "and "+config.getActiveTag()+"=1 {0}" : "{0}" );
					
					delSql = "delete from {0} where {1}=?";
					delSql = StringUtil.substitute( delSql ,
							new Object[]{ tableName,config.getPrimaryKey() } );
					delMoreSql = "delete from " + tableName + " where 1=1 {0}";
					delManySql = "delete from " + tableName +" where "+config.getPrimaryKey()+" in ({0})";
					
					if(useActive)
					{
						logicDelSql = "update {0} set "+config.getActiveTag()+"=0 where {1}=?";
						logicDelSql = StringUtil.substitute( logicDelSql ,
								new Object[]{ tableName,config.getPrimaryKey() } );
						logicDelManySql = "update " + tableName +" set "+config.getActiveTag()+"=0 where "+config.getPrimaryKey()+" in ({0})";
						logicDelMoreSql = "update "+ tableName+" set "+config.getActiveTag()+"=0 where 1=1 {0}";
						selectDisactiveSql = "select "+selectAllCols+" from "+tableName+" where "+config.getActiveTag()+"=0 {0}" ;
						selectCountDisactiveSql = "select count(*) from "+tableName+" where "+config.getActiveTag()+"=0" ;
					}
					
					return true;//new LinkedHashMap<String,Object>();
				}
			}				
		);

	}
}
