/**
 * 
 */
package org.dragonli.service.db.service.metadata;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.dragonli.service.db.service.DBConfig;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.core.PreparedStatementCreator;

/**
 * @author freeangel
 *
 */
public class DBhandler {
	public static Logger logger = Logger.getLogger(DBhandler.class);
	protected final static ConcurrentMap<String,DBhandler> handlerPools = new ConcurrentHashMap<String,DBhandler>();
	
	protected volatile ConcurrentMap<String,TableStructure> tableStructures = new ConcurrentHashMap<String,TableStructure>();
	private AtomicBoolean initFlag = new AtomicBoolean(false);
	private volatile int updateVersion = -1;
	
	public JdbcTemplate getJdbcTemplate() {
		return jdbcTemplate;
	}

	private JdbcTemplate jdbcTemplate;

	private String dbName;

	private DBConfig config;
	
	public static DBhandler getInstanceof( String dbName,DBConfig config )
	{
		dbName = dbName == null ? config.getDefaultDbName() : dbName;
		DBhandler handler = handlerPools.get(dbName);
		if( handler == null )
		{
			try
			{
				
				handlerPools.putIfAbsent(dbName, new DBhandler()) ;
				handler = handlerPools.get(dbName);
				handler.init(dbName,config);
			}
			catch(Exception e)
			{
				handlerPools.remove(dbName);
				return null;
			}
		}
		handler.init(dbName, config);
		return handler;
	}
	
	public static String[] allDb()
	{
		return handlerPools.keySet().toArray(new String[0]);
	}
	
	public static DBhandler[] allHandlers()
	{
		return handlerPools.values().toArray(new DBhandler[0]);
	}

	public String getDbName() {
		return dbName;
	}

	public DBConfig getConfig() {
		return config;
	}

	public void init(String dbName, DBConfig config) {
		// TODO Auto-generated method stub
		if(initFlag.get())
			return;
		this.dbName = dbName == null ? config.getDefaultDbName() : dbName;
		this.config = config;
		synchronized(this) 
		{
			if(initFlag.get())
				return;
			//数据库链接池优化
//			if( initFlag.compareAndSet(false, true) )
			try{
//				DriverManagerDataSource springDSN = new DriverManagerDataSource();
//				springDSN.setDriverClassName(config.getDriverClassName(dbName));
//				springDSN.setUrl( StringUtil.substitute( config.getDriverUrl(dbName),new Object[]{dbName,config.getHost(dbName)} ));
//				springDSN.setUsername(config.getUsername(dbName));
//				springDSN.setPassword(config.getPassword(dbName));
//				logger.info("db handler init info1:"+config.getDriverClassName(dbName));
//				logger.info("db handler init info2:"+config.getDriverUrl(dbName));
//				logger.info("db handler init info3:"+config.getHost(dbName));
//				logger.info("db handler init info4:"+config.getUsername(dbName));
//				logger.info("db handler init info5:"+config.getPassword(dbName));
//				DriverManagerDataSource springDSN = new DriverManagerDataSource();
//				springDSN.setDriverClassName(config.getDriverClassName(dbName));
//				springDSN.setUrl( StringUtil.substitute( config.getDriverUrl(dbName),new Object[]{dbName,config.getHost(dbName)} ));
//				springDSN.setUsername(config.getUsername(dbName));
//				springDSN.setPassword(config.getPassword(dbName));
				
				jdbcTemplate = new JdbcTemplate();
//				jdbcTemplate.setDataSource(springDSN);
				jdbcTemplate.setDataSource(config.getDbinfos().get(dbName));
				this.initTables();
				this.checkUpdate(true);
			}catch (Exception e) {
				logger.error("dbhandler init error:",e);
			}
			initFlag.set(true);
		}
	}
	
	public TableStructure findTableStructure(String name)
	{
		return tableStructures.get(name);
	}
	
	public void checkUpdate()
	{
		this.checkUpdate(false);
	}
	
	public void checkUpdate(boolean first)
	{
		if(config.getAutoUpdateTableName()==null)
			return ;
		if( !first && !initFlag.get() )
			return;
//		logger.info("checkUpdate test:"+first+","+(jdbcTemplate==null)+","+config.getAutoUpdateTableName());
		int[] values = jdbcTemplate.execute(
				new PreparedStatementCreator()
			    {  
			        public PreparedStatement createPreparedStatement(Connection connection) throws SQLException
			        {  
						String sql = "select * from "+config.getAutoUpdateTableName();
						PreparedStatement ps = connection.prepareStatement(sql,Statement.NO_GENERATED_KEYS);  
						return ps;  
			        }  
			    },
				new PreparedStatementCallback<int[]>() 
				{  
					public int[] doInPreparedStatement(PreparedStatement pstmt) throws SQLException, DataAccessException
					{  
						pstmt.execute();
						ResultSet rs = pstmt.getResultSet();
						
						while( rs.next() )
							return new int[]{ rs.getInt(1) };
						
						return new int[0];//new LinkedHashMap<String,Object>();
					}
				}				
			);
		
		for(int v:values)
		{
			if(first)
			{
				updateVersion = v;
				return ;
			}
			if( updateVersion == v )
				return ;
			logger.info(this.dbName+" 数据库表结构更新: oldVersion-"+updateVersion+" newVersion:"+v);
			updateVersion = v;
			this.initTables();
			return;
		}

	}
	
	public void initTables()
	{
		Queue<String> names = jdbcTemplate.execute(
				new PreparedStatementCreator()
			    {  
			        public PreparedStatement createPreparedStatement(Connection connection) throws SQLException
			        {  
						String sql = "show tables ";
						PreparedStatement ps = connection.prepareStatement(sql,Statement.NO_GENERATED_KEYS);  
						return ps;  
			        }  
			    },
				new PreparedStatementCallback<Queue<String>>() 
				{  
					public Queue<String> doInPreparedStatement(PreparedStatement pstmt) throws SQLException, DataAccessException
					{  
						pstmt.execute();
						ResultSet rs = pstmt.getResultSet();
						
						Queue<String> queue = new LinkedList<>();
						while( rs.next() )
							queue.add(rs.getString(1));
						
						return queue;//new LinkedHashMap<String,Object>();
					}
				}				
			);
		ConcurrentMap<String,TableStructure> ts = new ConcurrentHashMap<String,TableStructure>();
		for( String name : names )
		{
			TableStructure t = new TableStructure(name,config);
			t.init(jdbcTemplate);
			ts.put(name,t);
			logger.info(this.dbName+" 数据库表结构更新-tableNmae:"+name);
			
		}
		tableStructures = ts;
	}
}
