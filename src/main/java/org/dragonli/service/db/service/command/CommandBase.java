/**
 * 
 */
package org.dragonli.service.db.service.command;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import org.dragonli.service.db.service.DBConfig;
import org.dragonli.service.db.service.metadata.DBhandler;
import org.dragonli.service.db.service.metadata.TableStructure;
import org.dragonli.tools.general.DataCachePool;
import org.dragonli.tools.general.IDataCachePool;

/**
 * @author freeangel
 *
 */
public abstract class CommandBase implements ICommand, IDataCachePool {
	protected DBhandler handler;
	protected TableStructure ts;
	protected String table;
	protected String dbName;
	protected DBConfig config;
	protected CommandResult r;
	protected final boolean autoBack = true;
	protected final ExecutorService cachedThreadPool = Executors.newCachedThreadPool();

	protected long startTime = 0L;
	protected long endTime = 0L;
	protected String ak = null;
	protected int insertCount = 0;
	protected int updateCount = 0;
	protected int readCount = 0;
	protected int writeCount = 0;
	protected int totalnum = 0;
//totalnum
//readnum
//writenum
//insertnum
//updatenum
//fintotalnum
//fintotaltime
//finreadnum
//finreadtime
//finwritenum
//finwritetime
//fininsertnum
//fininserttime
//finupdatenum
//finupdatetime
//overreadnum
//overwritenum
//overinsertnum
//overupdatenum
//overtotalnum
//overreadnum
//overwritenum
//overinsertnum
//overupdatenum
//starttime
//endtime
//concurrentnum
	// ...

	public static Logger logger = Logger.getLogger(CommandBase.class);

//	private String redisKey;
//	protected DbInvokeLogger dbInvokeLogger;

	public void init(String dbName, String table, DBConfig config) throws Exception {
		dbName = dbName == null ? config.getDefaultDbName() : dbName;
		this.dbName = dbName;
		this.table = table;
		this.config = config;
//		dbInvokeLogger = config.getDbInvokeLogger();

		r = DataCachePool.get(CommandResult.class);
		if (!checkDbAndTable(r, dbName, config, table)) {
			backToPool(autoBack, true);
			throw new Exception("db or table is err");
		}
	}

	@Override
	public CommandResult execute(String dbName, DBConfig config, Map<String, Object> para) throws Exception {
		return null;
//		return execute(dbName,config,para,true);
	}

	@Override
	public CommandResult execute(String dbName, DBConfig config, Map<String, Object> para, boolean autoBack)
			throws Exception {
		return null;
	}

	protected boolean checkDb(CommandResult r, String dbName, DBConfig config) {
		return checkDb(r, dbName, config, null);
	}

	protected boolean checkDb(CommandResult r, String dbName, DBConfig config, Map<String, Object> para) {
		handler = DBhandler.getInstanceof(dbName, config);
		if (handler == null) {
			r.addNotExsitDb(dbName);
			return false;
		}
		return true;
	}

	protected boolean checkTable(CommandResult r, String dbName, DBConfig config, Map<String, Object> para) {
		String table = (String) para.get(config.getTableNameTag());
		return checkTable(r, dbName, config, table);
	}

	protected boolean checkTable(CommandResult r, String dbName, DBConfig config, String table) {
		if (!autoCheckTable())
			return true;

		if (table == null || (table = table.trim()).equals("")) {
			r.addOtherErrMsg("table cant be null or empty!");
			return false;
		}
		ts = handler.findTableStructure(table);
		if (ts == null) {
			r.addNotExsitTable(table);
			return false;
		}
		return true;
	}

	protected boolean checkDbAndTable(CommandResult r, String dbName, DBConfig config, String table) {
		if (!checkDb(r, dbName, config))
			return false;

		if (!checkTable(r, dbName, config, table))
			return false;

		return true;
	}

	protected boolean autoCheckTable() {
		return true;
	}

	protected boolean checkDbAndTable(CommandResult r, String dbName, DBConfig config, Map<String, Object> para) {
		if (!checkDb(r, dbName, config, para))
			return false;

		if (!checkTable(r, dbName, config, para))
			return false;

		return true;
	}

	public void backToPool(boolean autoBack) {
		backToPool(autoBack, false);
	}

	public void backToPool(boolean autoBack, boolean autoClearResult) {
		this.clear();
		if (autoBack)
			DataCachePool.back(this);
		if (autoClearResult && r != null) {
			r.clear();
			DataCachePool.back(r);
		}
	}

	public void writeToDbInvokeLogger(long startTime, long endTime, String ak, int readCount, int updateCount,
			int insertCount) {
//		dbInvokeLogger.performanceLog(startTime, endTime, ak, readCount, updateCount, insertCount);// ...
//		this.clear();
		return;
	}

	@Override
	public void clear() {
//		dbInvokeLogger = null;
		handler = null;
		ts = null;
		dbName = null;
		table = null;
		config = null;
		r = null;

//		startTime = 0L;
//		endTime = 0L;
//		ak = null;
//		insertCount = 0;
//		updateCount = 0;
//		readCount = 0;
//		writeCount = 0;
//		totalnum = 0;
		// ...
	}

}
