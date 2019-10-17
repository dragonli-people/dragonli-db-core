/**
 * 
 */
package org.dragonli.service.db.service;

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

/**
 * @author freeangel
 *
 */
public class DBConfig {

	
	protected String redisKey;
	public void setRedisKey(String redisKey) {
		this.redisKey = redisKey;
	}


	protected String defaultDbName;

	protected Map<String,DataSource> dbinfos;
	


	public Map<String, DataSource> getDbinfos() {
		return dbinfos;
	}

	public void setDbinfos(Map<String, DataSource> dbinfos) {
		this.dbinfos = dbinfos;
	}


	protected String activeTag = "active";
	protected String autoUpdateTableName = "db_service_update";

//	private String DriverClassName;
//	public String getDriverClassName() {
//		return DriverClassName;
//	}

//	public String getUrl() {
//		return url;
//	}
//
//	public String getUsername() {
//		return username;
//	}
//
//	public String getPassword() {
//		return password;
//	}


//	private String url;
//	private String username;
//	private String password;
	

	private final Map<String,Map<String,String>> dbname2info = new HashMap<>();
	public static Logger logger = Logger.getLogger(DBConfig.class);
	public DBConfig(String defaultDbName,String redisKey,Map<String,DataSource> dbInfos)//,String url,String driver,String username,String password)
	{
		this.defaultDbName = defaultDbName;//config.achieveConfig("db.mysql.database.default");
		this.redisKey = redisKey;//config.achieveConfig("db.model.redis.key.prefix");
		this.dbinfos = dbInfos;//new HashMap<>();
//		for( Object o:dbInfos )
//		{
//			@SuppressWarnings("unchecked")
//			Map<String,String> one = (Map<String, String>) o;
//			String name = one.remove("dbName");
//			for( Entry<String,String> entry:one.entrySet() )
//				dbinfos.put("db.mysql."+name+"."+entry.getKey(),entry.getValue());
//		}
//		this.DriverClassName = driver;
//		this.url = url;
//		this.username = username;
//		this.password = password;

//		this.DriverClassName = config.achieveConfig("db.jdbc.driver");
//		this.url = config.achieveConfig("db.jdbc.url");
//		
//		
//		this.DriverClassName = "com.mysql.jdbc.Driver";
//		this.url = "jdbc:mysql://{1}/{0}?autoReconnect=true&useUnicode=true&characterEncoding=UTF-8&rewriteBatchedStatements=true";
		
	}

	public String getDefaultDbName() {
		return defaultDbName;
	}

	
//	private String dbInfo(String dbName,String key) throws Exception{
//		Map<String,String> dbinfo = dbname2info.containsKey(dbName) ? dbname2info.get(dbName) : new HashMap<String,String>();
//		dbname2info.put(dbName, dbinfo);
//		String value = dbinfo.containsKey(key) ? dbinfo.get(key) : configsService.achieveConfig("db.mysql."+dbName+"."+key);
//		if(value == null){
//			throw new Exception("unkown db config in zk: dbName"+dbName+" key:"+key);
//		}
//		logger.info("get dbInfo: dbName "+dbName+" key:"+key+" - "+value);
//		return value;
//	}
	
	private String dbInfo(String dbName,String key) throws Exception{
		return null;
//		Map<String,String> dbinfo = dbname2info.containsKey(dbName) ? dbname2info.get(dbName) : new HashMap<String,String>();
//		dbname2info.put(dbName, dbinfo);
//		String value = dbinfo.containsKey(key) ? dbinfo.get(key) : dbinfos.get("db.mysql."+dbName+"."+key);
//		if(value == null){
//			throw new Exception("unkown db config in zk: dbName"+dbName+" key:"+key);
//		}
//		logger.info("get dbInfo: dbName "+dbName+" key:"+key+" - "+value);
//		return value;
	}
	
	public String getDriverUrl(String dbName)  throws Exception {
		return dbInfo(dbName,"driverUrl");
	}
	
	
	public String getDriverClassName(String dbName)  throws Exception {
		return dbInfo(dbName,"driverClassName");
	}
	
	public String getHost(String dbName)  throws Exception {
		return dbInfo(dbName,"host");
	}
	public String getUsername(String dbName) throws Exception {
		return dbInfo(dbName,"username");
	}

	public String getPassword(String dbName) throws Exception {
		return dbInfo(dbName,"password");
	}


	public String getAutoUpdateTableName() {
		return autoUpdateTableName;
	}

	public void setAutoUpdateTableName(String autoUpdateTableName) {
		this.autoUpdateTableName = autoUpdateTableName;
	}

	public String getActiveTag() {
		return activeTag;
	}

	public void setActiveTag(String activeTag) {
		this.activeTag = activeTag;
	}


	private int batchWriteIn = 1024;//8000
	public int getBatchWriteIn() {
		return batchWriteIn;
	}

	public void setBatchWriteIn(int batchWriteIn) {
		this.batchWriteIn = batchWriteIn;
	}


	private long sleep = 64L;
	public long getSleep() {
		return sleep;
	}

	public void setSleep(long sleep) {
		this.sleep = sleep;
	}


	private String primaryKey = "id";
	private String versionKey = "version";
	public String getAsynIoPriorityTag() {
		return asynIoPriorityTag;
	}

	public void setAsynIoPriorityTag(String asynIoPriorityTag) {
		this.asynIoPriorityTag = asynIoPriorityTag;
	}

	private String tableNameTag = "__TABLE_NAME";
	private String asynIoPriorityTag = "__ASYN_IO_PRIORITY_TAG";

	public String getTableNameTag() {
		return tableNameTag;
	}

	public void setTableNameTag(String tableNameTag) {
		this.tableNameTag = tableNameTag;
	}

	public String getPrimaryKey() {
		return primaryKey;
	}

	public void setPrimaryKey(String primaryKey) {
		this.primaryKey = primaryKey;
	}

	public String getVersionKey() {
		return versionKey;
	}

	public void setVersionKey(String versionKey) {
		this.versionKey = versionKey;
	}

//	public String getUrl() {
//		return url;
//	}
//
//	public void setUrl(String url) {
//		this.url = url;
//	}
//
//
//
//	public String getDriverClassName() {
//		return DriverClassName;
//	}
//
//	public void setDriverClassName(String driverClassName) {
//		DriverClassName = driverClassName;
//	}

	public String getRedisKey() {
		return redisKey;
	}

	
	
}
