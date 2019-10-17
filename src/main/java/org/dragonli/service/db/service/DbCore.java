/**
 * 
 */
package org.dragonli.service.db.service;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.dragonli.tools.general.DataCachePool;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;

import com.alibaba.fastjson.JSON;
import org.dragonli.service.db.service.command.CommandBatchSave;
import org.dragonli.service.db.service.command.CommandCount;
import org.dragonli.service.db.service.command.CommandDelete;
import org.dragonli.service.db.service.command.CommandDeleteMulti;
import org.dragonli.service.db.service.command.CommandDeleteQuery;
import org.dragonli.service.db.service.command.CommandExec;
import org.dragonli.service.db.service.command.CommandExecBatchUpate;
import org.dragonli.service.db.service.command.CommandGet;
import org.dragonli.service.db.service.command.CommandGetMulti;
import org.dragonli.service.db.service.command.CommandList;
import org.dragonli.service.db.service.command.CommandSave;
import org.dragonli.service.db.service.command.ICommand;
import org.dragonli.service.db.service.metadata.DBhandler;
import org.dragonli.service.db.util.DbUtil;
import org.dragonli.service.db.util.IMultiGetAndSimpleListInAble;
import org.dragonli.service.db.util.RedisUtil;

/**
 * @author dev
 *这个继承自 ServiceBase 其实是不科学的  但是🈶有好多东西是在ServiceBase里面的 暂时先这样 以后在想办法让这件事合理吧
 */
public class DbCore implements IMultiGetAndSimpleListInAble {

	protected final static Logger logger = Logger.getLogger(DbCore.class);
	
	protected DBConfig config;
//	private String redisKey;
	protected String updateTableName;

	
	public String getUpdateTableName() {
		return updateTableName;
	}

	public void setUpdateTableName(String updateTableName) {
		this.updateTableName = updateTableName;
	}
	
	@SuppressWarnings("unused")
	protected RedissonClient redissonClient;
//	public RedissonClient getRedissonClient() {
//		return redissonClient;
//	}
//
//	public void setRedissonClient(RedissonClient redissonClient) {
//		this.redissonClient = redissonClient;
//	}
	
	public DbCore()
	{
		super();
		logger.info("=====new db===");
	}
	
	public DbCore(RedissonClient redissonClient,DBConfig config) 
	{
		super();
		//need config
		this.config = config;
		this.redissonClient = redissonClient;
		this.dbstart();
	}

	public String getRedisKey() {
		//临时写法，容错
		return null;//to redisson;jedisPool == null ? null :(this.config == null ? null : this.config.getRedisKey());
	}
//
//	public void setRedisKey(String redisKey) {
//		this.redisKey = redisKey;
//	}

	public void start(){
		//need start command
//		logger.info("dbcore start do nothing!");
//		this.startTelnetCommand();
	}
	
	public void dbstart()
	{
		logger.info("do dbcore start!");
		try{
			//need to redisson
//			this.startRedis();	
		}catch (Exception e) {
			logger.error("dbcore start error", e);
		}
		new Thread(
				
				new Runnable() {
					
					@Override
					public void run() {
						while(true)
						{
							DBhandler[] allHandler = DBhandler.allHandlers();
							for( DBhandler db : allHandler )
								db.checkUpdate();
							
							try {
								Thread.sleep(100);
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
						}
					}
				}
				
		).start();;
	}
	
	public DBConfig getConfig() {
		return config;
	}

	public void setConfig(DBConfig config) {
		this.config = config;
	}

	
	@SuppressWarnings("unchecked")
	protected <T extends ICommand> T createCommand(Class<? extends T> cls,String dbName,String table,DBConfig config) throws Exception
	{
		ICommand command = DataCachePool.get(cls);
		command.init(dbName, table, config);
		return (T)command;
		
	}
	
	protected Exception logException(Exception exception)
	{
		logger.error(exception);
		return exception;
	}

//	<entry key="/get"><value>com.ijizhe.publics.dbservice.actions.GetAction</value></entry>
	public Map<String,Object> get(String dbName,String table,Object pk) throws Exception
	{
		return get(dbName,table,pk,null);
	}
	
//	<entry key="/get"><value>com.ijizhe.publics.dbservice.actions.GetAction</value></entry>
	public Map<String,Object> get(String dbName,String table,Object pk,String ak) throws Exception
	{
		long t1 = System.currentTimeMillis();
		dbName = dbName == null ? config.getDefaultDbName() : dbName;
		Map<String,Object> r = null;
		if(this.getRedisKey() != null)
			r = this.getFromCache(dbName, table, pk);
		if( r != null )
			return r;
		CommandGet command = createCommand( CommandGet.class , dbName , table , config );
		r = command.exec(pk,ak);
		
		this.saveToCache(dbName, table, pk, r);
//		RMap<String, String> map = redissonClient.getMap(redisKey(dbName,table));
//		map.put(pk.toString(), JSON.toJSONString(r));
		long t2 = System.currentTimeMillis();
		logger.info("db get cost|"+(t2-t1)+"|"+t2+"|"+t1);
		return r; 
	}
	
	public void saveToCache(String dbName,String table,Object pk,Map<String,Object> model) throws Exception
	{
		RedisUtil.saveToCache(dbName, table, pk, model, redissonClient, getRedisKey());
		//this.saveToCache(dbName, table, pk, model,null);
	}
	
////	public void saveToCache(String dbName,String table,Object pk,Map<String,Object> model,RedissonClient redissonClient)
////	{
//		if(getRedisKey()==null||model==null)
//		{
////			logger.info("===save to return:"+(jedisPool==null)+";"+(model==null)+":"+(jedis==null));
//			return ;
//		}
//		
////		boolean back = jedisOrPipe == null;
////		if( jedisOrPipe == null)
////			jedisOrPipe = jedisPoolProxy.getResource(5000L, "dbService.saveToCache");//jedisPool.getResource();
//		// rr 改成下面
////		
////		   //map
////        Map<String,String> user = new HashMap<String,String>();
////        user.put("name", "cd");
////        user.put("password", "123456");
////        //map存入redis
////        jedis.hmset("user", user);
////        List<String> rsmap = jedis.hmget("user", "name","password");
////        System.out.println(rsmap);
////        //删除map中的某一个键值 password
////        jedis.hdel("user", "password");
////        System.out.println(jedis.hmget("user", "name", "password"));
//		
//		try
//		{
////			if(jedisOrPipe instanceof Jedis)
////				((Jedis)jedisOrPipe).hset(redisKey(dbName,table),pk.toString(), JSON.toJSONString(model));
////			else
////				((Pipeline)jedisOrPipe).hset(redisKey(dbName,table),pk.toString(), JSON.toJSONString(model));
//			// rr 改成下面
//			//need to redisson
//			RMap<String, String> map = redissonClient.getMap(redisKey(dbName,table));
//			map.put(pk.toString(), JSON.toJSONString(model));
//		}catch(Exception e){throw e;}
//		finally {
////			if(back && jedisOrPipe != null)
////				jedisPoolProxy.returnResource(((Jedis)jedisOrPipe));//.close();
//			// rr 改成下面
////			jedisPoolProxy.returnResource(jedis);
//		}
//	}
	
//	public void saveToCache(String dbName,String table,Object pk,Map<String,Object> model
//			,Pipeline pipe,Map<String, Response<String>> response) throws Exception
//	{
//		pipe.hset(redisKey(dbName,table),pk.toString(), JSON.toJSONString(model));
//	}
	
//	protected String redisKey(String dbName,String table)
//	{
//		return getRedisKey()+modelKey(dbName,table);
//	}
	
	public Map<String,Object> getFromCache(String dbName,String table,Object pk) throws Exception
	{
		return this.getFromCache(dbName, table, pk,null);
	}
	
	public Map<String,Object> getFromCache(String dbName,String table,Object pk,RedissonClient redissonClient) throws Exception
	{
		return RedisUtil.getFromCache(dbName, table, pk, redissonClient, getRedisKey());
//		if(getRedisKey()==null)
//			return null;
//		String json = null;
//		RMap<String, String> map = redissonClient.getMap(getRedisKey()+modelKey(dbName,table));
//		json = map.get(pk.toString());
//		return json == null ? null : JSON.parseObject(json);
	}
	
//	public Map<String,Object> getFromCache(String dbName,String table,Object pk
//			,Pipeline pipe,Map<String, Response<String>> response) throws Exception
//	{
//		response.put(modelKey_pk(dbName, table, pk) , pipe.hget(getRedisKey()+modelKey(dbName,table),pk.toString()));
//		
//		
//		return null;
//	}
	
	
	
	public void deleteFromCache(String dbName,String table,Object pk)
	{
		RedisUtil.deleteFromCache(dbName, table, pk, redissonClient, getRedisKey());
//		if(getRedisKey()==null)
//			return ;
//		RMap<String, String> map = redissonClient.getMap(getRedisKey()+modelKey(dbName,table));
//		map.remove(pk.toString());
	}
	
//	<entry key="/save"><value>com.ijizhe.publics.dbservice.actions.SaveAction</value></entry>
	public Map<String,Object> save(String dbName,String table,Map<String,Object> model) throws Exception
	{
		return save( dbName, table, model, null);
	}
	
//	<entry key="/save"><value>com.ijizhe.publics.dbservice.actions.SaveAction</value></entry>
	public Map<String,Object> save(String dbName,String table,Map<String,Object> model,String ak) throws Exception
	{
		dbName = dbName == null ? config.getDefaultDbName() : dbName;
		model.put(config.getTableNameTag(), table);
		CommandSave command = createCommand( CommandSave.class , dbName , table , config );
		try
		{
			Map<String,Object> r = command.exec(model,ak);
			this.saveToCache(dbName, table, r.get(config.getPrimaryKey()), r);
//			RMap<String, String> map = redissonClient.getMap(redisKey(dbName,table));
//			map.put(r.get(config.getPrimaryKey()).toString(), JSON.toJSONString(model));
			return r;
		}catch(Exception exception){
			logger.error("dbService.err:dbService.save:"+dbName+":"+table+":"+JSON.toJSONString(model));
			throw logException(exception);
		}
		finally {
//			System.gc();
		}
	}
	
//	<entry key="/list"><value>com.ijizhe.publics.dbservice.actions.ListAction</value></entry>
	public Map<String,Object>[] list(String dbName,String table,String where,List<Object> paraList) throws Exception
	{
		return list(dbName,table,where,paraList,null);
	}
	public Map<String,Object>[] list(String dbName,String table,String where,List<Object> paraList,String ak) throws Exception
	{
		Object[] paras = paraList.toArray(new Object[0]);
//		System.out.println("dbName:"+dbName+"|table:"+table+"|where:"+where+"|paras"+JSON.toJSONString(paras));
		return list(dbName,table,where,paras,ak);
	}
	
	public Map<String,Object>[] list(String dbName,String table,String where,Object[] paras) throws Exception
	{
		return list(dbName,table,where,paras,null);
	}
	//警告：没有存入redis，此处没想好要不要存。如果数据量巨大怎么办？或是添加一个参数，指定存则存，否则不存？待实现。可能造成list和get数据不一致的情况
	public Map<String,Object>[] list(String dbName,String table,String where,Object[] paras,String ak) throws Exception
	{
		dbName = dbName == null ? config.getDefaultDbName() : dbName;
		CommandList command = createCommand( CommandList.class , dbName , table , config );
		try
		{
			return command.exec(where,paras,ak);
		}catch(Exception exception){
			logger.error("=====bad sql in list:table:"+table+"====sql:"+where);
			logger.error("=====dbService.err:dbService.list:"+dbName+table+":"+where+":"+JSON.toJSONString(paras));
			throw logException(exception);
		}
	}
	
//	<entry key="/batch/get"><value>com.ijizhe.publics.dbservice.actions.BatchGetAction</value></entry>
	//==================
//	
//	public Map<String,Object>[] batchGet(String dbName,String table,List<Object> pks) throws Exception{
//		return batchGet(dbName,table,pks.toArray(new Object[0])); 
//	}
//	
//	@SuppressWarnings("unchecked")
//	public Map<String,Object>[] batchGet(String dbName,String table,Object[] pks) throws Exception{
//		Map<String,Object>[] models = new Map[pks.length];
//		Map<String,Object> one = null;
//		for(int i = 0 ; i<models.length;i++)
//		{
//			models[i] = one = new HashMap<>();
//			one.put(config.getTableNameTag(), table);
//			one.put(config.getPrimaryKey(), pks[i]);
//		}
//		return batchGet(dbName,models);
//	}
//	public Map<String,Object>[] batchGet(String dbName,List<Map<String,Object>> paraList) throws Exception
//	{
//		return batchGet(dbName,paraList, null);
//	}
//	public Map<String,Object>[] batchGet(String dbName,List<Map<String,Object>> paraList,String table) throws Exception
//	{
//		@SuppressWarnings("unchecked")
//		Map<String,Object>[] models = paraList.toArray(new Map[0]);
//		return batchGet(dbName,models, table);
//	}
//	public Map<String,Object>[] batchGet(String dbName,Map<String,Object>[] models) throws Exception
//	{
//		return batchGet(dbName,models, null);
//	}
//	@SuppressWarnings("unchecked")
//	public Map<String,Object>[] batchGet(String dbName,Map<String,Object>[] models,String table) throws Exception
//	{
//		dbName = dbName == null ? config.getDefaultDbName() : dbName;
//		if( table != null )
//		{
//			for( Map<String,Object> o : models )
//				o.put(config.getTableNameTag(), table);
//		}
//		Queue<Map<String,Object>> queue = null;
//		final Map<String, Response<String>> response = new HashMap<>();
//		if( getRedisKey() != null )
//		{
//			queue = new LinkedList<>();
//			Queue<Map<String,Object>> queue2 = new LinkedList<>();//组织新的数组
//			Map<String,Object> oo = null;
//			String ooStr = null;
//			
//			try {
//				for( Map<String,Object> o : models )
//					this.getFromCache(dbName, o.get(config.getTableNameTag()).toString(), o.get(config.getPrimaryKey()),pipeline,response);
//				for( Map<String,Object> o : models )
//				{
//					ooStr = response.get(
//							modelKey_pk( dbName , o.get(config.getTableNameTag()).toString() , o.get(config.getPrimaryKey() ) ) ).get();
//					
//					if( ooStr != null )
//						queue.add(JSON.parseObject(ooStr));
//					else
//						queue2.add(o);
//				}
//			} catch (Exception e) {
//				throw e;
//			}
//			
//			if( queue2.size() == 0 )
//				return queue.toArray(new Map[0]);//优化
//			
//			models = queue2.toArray(new Map[0]);
//		}
//		response.clear();
//		CommandBatchGet command = createCommand( CommandBatchGet.class , dbName , table , config );
//			Map<String,Object>[] rr = command.exec(models);
//			if(rr.length==0)
//				return queue == null ? rr : queue.toArray(new Map[0]);//优化
//			if( queue != null && queue.size() > 0 && getRedisKey() != null )
//			{
//				//need to redisson
//				Map<String, String> collect = Arrays.stream(rr).parallel().collect(Collectors.toMap(map -> map.get(config.getTableNameTag()).toString(), map -> JSON.toJSONString(map)));
//				redissonClient.getMap(redisKey(dbName,table)).putAllAsync(collect);
//				return queue.toArray(new Map[0]);//合并缓存
//			}
//			return rr;
//	}	
	
//	<entry key="/multi/get"><value>com.ijizhe.publics.dbservice.actions.GetMultiAction</value></entry>
//	public Map<String,Object>[] multiGet(String dbName,String table,List<Object> paraList,String ak) throws Exception
//	{//TODO
//		Object[] pks = paraList.toArray(new Object[0]);
//		return multiGet(dbName,table,pks,ak); 
//	}
//	
	public Map<String,Object>[] multiGet(String dbName,String table,List<Object> paraList) throws Exception
	{
		Object[] pks = paraList.toArray(new Object[0]);
		return multiGet(dbName,table,pks);
	}
//	public Map<String,Object>[] multiGet(String dbName,String table,Object[] pks) throws Exception
//	{
//		return multiGet(dbName,table,pks,null);	
//	}

	public Map<String,Object>[] multiGet(String dbName,String table,Object[] pks) throws Exception
	{
		return this.multiGet( dbName,table,pks,null);
	}
	
	public Map<String,Object>[] listByOneField(String dbName,String table,String field,Object[] values) throws Exception
	{
		return this.listIn(dbName, table, field, values);
	}
	
	@SuppressWarnings("unchecked")
	public Map<String,Object>[] multiGet(String dbName,String table,Object[] pks,String ak) throws Exception
	{
		dbName = dbName == null ? config.getDefaultDbName() : dbName;
		Queue<Object> queue = null;
		Queue<Map<String,Object>> queue2 = new LinkedList<>();
		if(redissonClient != null && getRedisKey()!=null)
		{
			queue = new LinkedList<>();			
			String rs = null;
			Set<String> set = new HashSet<>();
			for(Object pk : pks)
				set.add( pk.toString() );
			
			RMap<String,String> rmap = redissonClient.getMap(
					RedisUtil.redisMapKey(getRedisKey(),dbName,table));
			Map<String, String> response = rmap.getAllAsync(set).get();			
			
			for(Object pk : pks)
			{
				rs = response.get(pk.toString());
				if( rs != null )
					queue2.add(JSON.parseObject(rs));//缓存里有
				else
					queue.add(pk);//缓存里无，需要查询的主键
			}
			
			if( queue.size() == 0 )
				return queue2.toArray(new Map[0]);//优化
			pks = queue.toArray(new Object[0]);
		}
		
		CommandGetMulti command = createCommand( CommandGetMulti.class , dbName , table , config );

		Map<String,Object>[] all = command.exec(pks,ak);
		if( all.length == 0 )
			return queue2.toArray(new Map[0]);//优化
		if( queue != null )
		{
			//need to redisson
			Map<String, String> map = new HashMap<>();
			for( Map<String,Object> m : all )
				map.put(m.get(config.getPrimaryKey()).toString(), JSON.toJSONString(m));
			redissonClient.getMap(RedisUtil.redisMapKey(getRedisKey(),dbName,table)).putAllAsync(map);
		}			
		Map<String,Object>[] result  = new Map[all.length  + queue2.size()];  

		System.arraycopy(all, 0, result, 0, all.length);  
		System.arraycopy(queue2.toArray(new Map[0]), 0, result, all.length, queue2.size());	
		return result; 
	}
//	<entry key="/batch/save"><value>com.ijizhe.publics.dbservice.actions.BatchSaveAction</value></entry>
	
	public Map<String,Object> batchSave(String dbName,List<Map<String,Object>> paraList,String tempPkTag) throws Exception
	{
		return batchSave(dbName,paraList,tempPkTag, null);
	}
	
	public Map<String,Object> batchSave(String dbName,List<Map<String,Object>> paraList,String tempPkTag,String table) throws Exception
	{
		return batchSave(dbName,paraList,tempPkTag,table,null);
	}
	public Map<String,Object> batchSave(String dbName,List<Map<String,Object>> paraList,String tempPkTag,String table,String ak) throws Exception
	{
		@SuppressWarnings("unchecked")
		Map<String,Object>[] models = paraList.toArray(new Map[0]);
		return batchSave(dbName,models,tempPkTag,table,ak);
	}
	public Map<String,Object> batchSave(String dbName,Map<String,Object>[] models,String tempPkTag) throws Exception
	{
		return batchSave(dbName,models,tempPkTag,null);
	}
	public Map<String,Object> batchSave(String dbName,Map<String,Object>[] models,String tempPkTag,String table) throws Exception
	{
		return batchSave(dbName,models,tempPkTag,table,null);
	}
	@SuppressWarnings("unchecked")
	public Map<String,Object> batchSave(String dbName,Map<String,Object>[] models,String tempPkTag,String table,String ak) throws Exception
	{
		dbName = dbName == null ? config.getDefaultDbName() : dbName;
		if( table != null )
		{
			for( Map<String,Object> o : models )
				o.put(config.getTableNameTag(), table);
		}
		CommandBatchSave command = createCommand( CommandBatchSave.class , dbName , table , config );
		Map<String,Object> rr = command.exec(models,tempPkTag,ak);

		if(getRedisKey()!=null)
		{
				//need to redisson
				Map<String, String> map = new HashMap<>();
				for(Map<String, Object> r:(Map<String,Object>[])rr.get("list"))
					map.put(r.get(config.getPrimaryKey()).toString(), JSON.toJSONString(r));
				redissonClient.getMap(RedisUtil.redisMapKey(getRedisKey(),dbName,table)).putAllAsync(map);
		}
		return rr;
	}
	
//	<entry key="/delete"><value>com.ijizhe.publics.dbservice.actions.DeleteAction</value></entry>
	public Integer delete(String dbName,String table,Object pk) throws Exception
	{
		CommandDelete command = createCommand( CommandDelete.class , dbName , table , config );
		try
		{
			this.deleteFromCache(dbName, table, pk);
			return command.exec(pk);
		}catch(Exception exception){
			logger.error("dbService.err:dbService.delete:"+dbName+":"+table+":"+pk);
			throw logException(exception);
		}
	}
	
	
//	<entry key="/multi/delete"><value>com.ijizhe.publics.dbservice.actions.DeleteMultiAction</value></entry>
	public Integer multiDelete(String dbName,String table,List<Object> paras) throws Exception
	{
		Object[] pks = paras.toArray(new Object[0]);
		return multiDelete(dbName,table,pks);
	}
	public Integer multiDelete(String dbName,String table,Object[] pks) throws Exception
	{
		CommandDeleteMulti command = createCommand( CommandDeleteMulti.class , dbName , table , config );
			if(getRedisKey()!=null)
			{
//				jedis = jedisPoolProxy.getResource(1000L, "dbService.multiDelete");// jedisPool.getResource();
				for(Object pk:pks)
					this.deleteFromCache(dbName, table, pk);//有待优化为管道
			}
		return command.exec(pks);
	}
	
//	<entry key="/query/delete"><value>com.ijizhe.publics.dbservice.actions.DeleteQueryAction</value></entry>
	public Integer queryDelete(String dbName,String table,String where,List<Object> paraList) throws Exception
	{
		Object[] paras = paraList.toArray(new Object[0]);
		return queryDelete(dbName,table,where,paras);
	}
	//警告：目前还没有将对应记录其从redis中删除，如果有这样的需求，实现在此处补全
	public Integer queryDelete(String dbName,String table,String where,Object[] paras) throws Exception
	{
		CommandDeleteQuery command = createCommand( CommandDeleteQuery.class , dbName , table , config );
		try
		{
			if( getRedisKey() != null )
			{
				//先list，再delFromCache，待实现
				
			}
			return command.exec(where,paras);
		}catch(Exception exception){
			logger.error("dbService.err:dbService.queryDelete:"+dbName+":"+table+":"+where+":"+StringUtils.join(paras, ","));
			throw logException(exception);
		}
		finally {
//			System.gc();
		}
	}	
	
//	<entry key="/count"><value>com.ijizhe.publics.dbservice.actions.CountAction</value></entry>
	public Integer count(String dbName,String table,String where,List<Object> paraList) throws Exception
	{
		Object[] paras = paraList.toArray(new Object[0]);
		return count(dbName,table,where,paras);
	}
	public Integer count(String dbName,String table,String where,Object[] paras) throws Exception
	{
//		System.out.println("table:"+table+"     where: "+where+":"+JSON.toJSONString(paras));
		CommandCount command = createCommand( CommandCount.class , dbName , table , config );
		try
		{
			return command.exec(where,paras);
		}catch(Exception exception){
			logger.error("dbService.err:dbService.count:"+dbName+":"+table+":"+where+":"+JSON.toJSONString(paras));
			throw logException(exception);
		}
	}
	
	public Map<String,Object>[] exec(String dbName,String sql,List<Object> paras) throws Exception
	{
//		paras = paras == null ? new ArrayList<>() : paras;
		return exec(dbName,sql,paras != null ? paras.toArray(new Object[0] ) : null);

	}
	
//	public Map<String,Object>[] exec(String dbName,String[] sql) throws Exception
//	{
////		paras = paras == null ? new ArrayList<>() : paras;
//		return exec(dbName,sql,paras != null ? paras.toArray(new Object[0] ) : null);
//
//	}
	
	public Map<String,Object>[] exec(String dbName,String sql,Object[] paras) throws Exception
	{
		paras = paras == null ? new Object[0] : paras;
		CommandExec command = createCommand( CommandExec.class , dbName , null , config );
		try
		{
			return command.exec(sql,paras);
		}catch(Exception exception){
			logger.error("dbService.err:dbService.exec:"+dbName+":"+sql+":"+JSON.toJSONString(paras));
			throw logException(exception);
		}
	}
	
	@SuppressWarnings("unchecked")
	public Map<String,Object>[] batchUpdate(String dbName,String[] sqls) throws Exception
	{
		CommandExecBatchUpate command = createCommand( CommandExecBatchUpate.class , dbName , null , config );
		try
		{
			command.exec(sqls);
			return new Map[0];
		}catch(Exception exception){
//			logger.error("dbService.err:dbService.exec:"+dbName+":"+sql+":"+JSON.toJSONString(paras));
			throw logException(exception);
		}
	}
	
	@SuppressWarnings("unchecked")
	public Map<String,Object>[] batchUpdate(String dbName,String sql,List<Object[]> where) throws Exception
	{
		CommandExecBatchUpate command = createCommand( CommandExecBatchUpate.class , dbName , null , config );
		try
		{
			command.exec(sql,where);
			return new Map[0];
		}catch(Exception exception){
//			logger.error("dbService.err:dbService.exec:"+dbName+":"+sql+":"+JSON.toJSONString(paras));
			throw logException(exception);
		}
	}
	
	public Map<String,Object> toOne(String dbName,Map<String,Object> one,Map<String,String> mapping) throws Exception
	{
		return toOne(dbName,one,mapping,null);
	}
	
	public Map<String,Object> toOne(String dbName,Map<String,Object> one,Map<String,String> mapping,String ak) throws Exception
	{
		return this.toOne(dbName,new Map[]{ one },mapping,ak)[0];
	}
	
	
	public Map<String, Object>[] toOne(String dbName, List<Object> models, Map<String, String> mapping) throws Exception {
		return toOne(dbName,models,mapping,null);
	}
	
	
	public Map<String, Object>[] toOne(String dbName, List<Object> models, Map<String, String> mapping,String ak) throws Exception {
		@SuppressWarnings("unchecked")
		Map<String,Object>[] paras = models.toArray(new Map[0]);
		return toOne(dbName,paras,mapping,ak);
	}
	
	
	public Map<String,Object>[] toOne(String dbName,Map<String,Object>[] models,Map<String,String> mapping,String ak) throws Exception
	{
		return DbUtil.toOne(dbName, models, mapping, config.getPrimaryKey(), this);
	}
	//下面是以前的toOne，这些代码已经被提纯到DbUtil里
//	{
//		Map<String,Set<Object>> pks = new HashMap<>();
//		Object pk = null;
//		for( Entry<String,String> entry : mapping.entrySet() )
//		{
//			String[] tempArr = entry.getValue().split(":");
//			String key = tempArr[0] + ( tempArr.length == 3 ? ":" + tempArr[2] : "" );
//			if( !pks.containsKey(key) )
//				pks.put(key, new HashSet<>());
//			Set<Object> pkDic = pks.get(key);
//			for(Map<String,Object> m:models)
//			{
//				if( ( pk=m.get(entry.getKey())) != null )
//					pkDic.add(pk);
//			}
//		}
//		Map<String,Map<String,Map<Object,Map<String,Object>>>> query = new HashMap<>();
//		for(Entry<String,Set<Object>> entry:pks.entrySet())
//		{
//			String[] tempArr = entry.getKey().split(":");
//			String table = tempArr[0];
//			String field = tempArr.length == 2 ? tempArr[1] : config.getPrimaryKey();
//			Object[] allId = entry.getValue().toArray(new Object[0]);
//			if(!query.containsKey(table))
//				query.put(table, new HashMap<>());
//			if(!query.get(table).containsKey(config.getPrimaryKey()))
//				query.get(table).put(config.getPrimaryKey(), new HashMap<>());
//			if(!query.get(table).containsKey(field))
//				query.get(table).put(field, new HashMap<>());
//			Map<String,Map<Object,Map<String,Object>>> dic = query.get(table);
//			Map<String,Object>[] list = tempArr.length != 2 ? 
//					this.multiGet(dbName, table, allId ,ak) : this.listIn(dbName, table, field, allId);
//			for( Map<String,Object> m : list )
//			{
//				dic.get(field).put( m.get(field),m);
//				dic.get(config.getPrimaryKey()).put( m.get(config.getPrimaryKey()),m);
//			}
//		}
//		Map<String,Object> toOneModel = null;
//		for(Map<String,Object> m:models)
//		{
//			for( Entry<String,String> entry : mapping.entrySet() )
//			{
//				String[] tempArr = entry.getValue().split(":");
//				String table = tempArr[0];
//				String field = tempArr.length == 3 ? tempArr[2] : config.getPrimaryKey();
//				if( ( pk=m.get(entry.getKey())) != null && ( toOneModel = query.get(table).get(field).get(pk) ) != null )
//				{
//					Map<String,Object> copy = new HashMap<>();
//					copy.putAll(toOneModel);
//					m.put(tempArr[1], copy);
//				}
//			}
//		}
//		return models;
//	}
	
	@SuppressWarnings("unchecked")
	public Map<String,Object> toMany(String dbName,Map<String,Object> one,Map<String,String> mapping) throws Exception
	{
		return this.toMany(dbName,new Map[]{ one },mapping)[0];
	}
	
	@SuppressWarnings("unchecked")
	public Map<String,Object>[] toMany(String dbName,List<Object> models,Map<String,String> mapping,Map<String,Object> values) throws Exception
	{
	
			Map<String,Object>[] paras = models.toArray(new Map[0]);
			return toMany(dbName,paras,mapping,values);

		
	}
	public Map<String,Object>[] toMany(String dbName,Map<String,Object>[] models,Map<String,String> mapping,Map<String,Object> values) throws Exception
	{
		Map<String,Set<Object>> pks = new HashMap<>();
		Object pk = null;
		for( Entry<String,String> entry : mapping.entrySet() )
		{
			//必须传 表名字:放在什么key:按照什么字段查:其他条件
			String[] tempArr = entry.getValue().split(":");
			String key = tempArr[0] + ":" +  tempArr[2]  + ":" + tempArr[3] +":"+ entry.getKey() ;
			if( !pks.containsKey(key) )
				pks.put(key, new HashSet<>());
			Set<Object> pkDic = pks.get(key);
			for(Map<String,Object> m:models)
			{
				if( ( pk=m.get(entry.getKey())) != null )
					pkDic.add(pk);
			}
		}
		Map<String,Map<String,Map<Object,Queue<Map<String,Object>>>>> query = new HashMap<>();
		for(Entry<String,Set<Object>> entry:pks.entrySet())
		{
			String[] tempArr = entry.getKey().split(":");
			String table = tempArr[0];
			String field =  tempArr[1];
			String where = tempArr[2];
			String valueKey = tempArr[3];
			Object[] allId = entry.getValue().toArray(new Object[0]);
			if(!query.containsKey(table))
				query.put(table, new HashMap<>());
			if(!query.get(table).containsKey(field))
				query.get(table).put(field, new HashMap<>());
			Map<String,Map<Object,Queue<Map<String,Object>>>> dic = query.get(table);
			
			@SuppressWarnings("unchecked")
			Map<String,Object>[] list = this.listIn(dbName, table, field, allId,where,((List<Object>)values.get(valueKey)).toArray(new Object[0])); 
			for( Map<String,Object> m : list )
			{
				if( !dic.get(field).containsKey(m.get(field)) )
					dic.get(field).put(m.get(field), new LinkedList<>());
				dic.get(field).get(m.get(field)).add(m);
			}
		}
		Queue<Map<String,Object>> toOneModels = null;
		for(Map<String,Object> m:models)
		{
			for( Entry<String,String> entry : mapping.entrySet() )
			{
				String[] tempArr = entry.getValue().split(":");
				String table = tempArr[0];
				String field = tempArr.length == 3 ? tempArr[2] : config.getPrimaryKey();
				if( ( pk=m.get(entry.getKey())) != null && ( toOneModels = query.get(table).get(field).get(pk) ) != null )
				{
					Queue<Map<String,Object>> copy = new LinkedList<>();
					for( Map<String,Object> model : toOneModels )
					{
						Map<String,Object> copyModel = new HashMap<String,Object>();
						copyModel.putAll(model);
						copy.add(copyModel);
					}
					m.put(tempArr[1], copy.toArray(new Map[0]));
				}
			}
		}
		return models;
	}
	
	public Map<String,Object>[] toMany(String dbName,Map<String,Object>[] models,Map<String,String> mapping) throws Exception
	{
		return DbUtil.toMany(dbName, models, mapping, config.getPrimaryKey(), this);
	}
	//下面是以前的toMany，这些代码已经被提纯到DbUtil里
//	{
//		//mapping格式示例:  [propId => 'tbl_user:userList:id',...]
//		Map<String,Set<Object>> pks = new HashMap<>();
//		Object pk = null;
//		for( Entry<String,String> entry : mapping.entrySet() )
//		{
//			//按table:id的格式分组，取出models中propId，为listIn作准备.键值示例:tbl_user:id,用set去重
//			String[] tempArr = entry.getValue().split(":");
//			String key = tempArr[0] + ( tempArr.length >= 3 ? ":" + tempArr[2] : "" );
//			if( !pks.containsKey(key) )
//				pks.put(key, new HashSet<>());
//			Set<Object> pkDic = pks.get(key);
//			for(Map<String,Object> m:models)
//			{
//				if( ( pk=m.get(entry.getKey())) != null )
//					pkDic.add(pk);
//			}
//		}
//		Map<String,Map<String,Map<Object,Queue<Map<String,Object>>>>> query = new HashMap<>();
//		for(Entry<String,Set<Object>> entry:pks.entrySet())
//		{
//			String[] tempArr = entry.getKey().split(":");
//			String table = tempArr[0];
//			String field = tempArr.length == 2 ? tempArr[1] : config.getPrimaryKey();
//			Object[] allId = entry.getValue().toArray(new Object[0]);
//			if(!query.containsKey(table))
//				query.put(table, new HashMap<>());
//			if(!query.get(table).containsKey(field))
//				query.get(table).put(field, new HashMap<>());
//			Map<String,Map<Object,Queue<Map<String,Object>>>> dic = query.get(table);
//			Map<String,Object>[] list = this.listIn(dbName, table, field, allId); 
////			按table:id的格式分组,listIn查询出结果，并置入字典，例： { "tbl_user":{38:[model1,model2,model3...]} }
//			for( Map<String,Object> m : list )
//			{
//				if( !dic.get(field).containsKey(m.get(field)) ){
//					dic.get(field).put(m.get(field), new LinkedList<>());
//					
//				}
//				
//				
//					
//				dic.get(field).get(m.get(field)).add(m);
//			}
//		}
//		Queue<Map<String,Object>> toOneModels = null;
//		for(Map<String,Object> m:models)
//		{
//			for( Entry<String,String> entry : mapping.entrySet() )
//			{
//				String[] tempArr = entry.getValue().split(":");
//				String table = tempArr[0];
//				String field = tempArr.length == 3 ? tempArr[2] : config.getPrimaryKey();
//				
//		
//				if( ( pk=m.get(entry.getKey())) != null && ( toOneModels = query.get(table).get(field).get(pk) ) != null )
//				{
//					Queue<Map<String,Object>> copy = new LinkedList<>();
//					for( Map<String,Object> model : toOneModels )
//					{
//						Map<String,Object> copyModel = new HashMap<String,Object>();
//						copyModel.putAll(model);
//						copy.add(copyModel);
//					}
//					m.put(tempArr[1], copy.toArray(new Map[0]));
//				}
//			}
//		}
//		return models;
//	}
	
	public Map<String,Object>[] listBy(String dbName,String table,String field,Object v) throws Exception
	{
		return this.listBy(dbName, table, field, v,null,null,null);
	}
	
	public Map<String,Object>[] listBy(String dbName,String table,String field,Object v,String order) throws Exception
	{
		return this.listBy(dbName, table, field, v,order,null,null);
	}
	
	public Map<String,Object>[] listBy(String dbName,String table,String field,Object v,String order,Integer perPage,Integer page) throws Exception
	{
		return this.listBy(dbName, table, new String[]{field}, new Object[]{v},order,perPage,page);
	}
	
	public Map<String,Object>[] listBy(String dbName,String table,String[] fields,Object[] v) throws Exception
	{
		return this.listBy(dbName,table,fields,v,null,null,null); 
	}
	
	public Map<String,Object>[] listBy(String dbName,String table,String[] fields,Object[] v,String order) throws Exception
	{
		return this.listBy(dbName,table,fields,v,order,null,null); 
	}
	
	public Map<String,Object>[] listBy(String dbName,String table,String[] fields,Object[] v,String order,Integer perPage,Integer page) throws Exception
	{
		Queue<Object> paras = new LinkedList<>();
		for(Object vv:v)
			paras.add(vv);
		String where = "";
		for(String field:fields)
			where += " and " + field + "=?" ;
		where += ( order == null ? "" : " " + order );
		if( perPage != null && page != null )
		{
			where = " limit ?,?";
			if( page < 0 )
			{
				paras.add( perPage );
				paras.add(-1);
			}
			else
			{
				paras.add( perPage*(page-1));
				paras.add(perPage);
			}
		}
		return this.list(dbName, table, where, paras.toArray(new Object[0]));
	}
	
	//在满足指定字段中筛选满足条件的查询
	public Map<String,Object>[] listIn(String dbName,String table,String field,Object[] values,String where,Object[] whereValues) throws Exception{
		
		String[] arr = new String[values.length]; 
		Arrays.fill( arr, "?");
		where = " and " + field + " in("+ StringUtils.join(arr, ',') +") " + where;
		return this.list(dbName, table, where, ArrayUtils.addAll(values, whereValues));
		
		
	}
	
	public Map<String,Object>[] listIn(String dbName,String table,String field,List<Object> values) throws Exception
	{
		return this.listIn(dbName, table, field, values.toArray(new Object[0]));
	}
	
	@SuppressWarnings("unchecked")
	public Map<String,Object>[] listIn(String dbName,String table,String field,Object[] values) throws Exception
	{
		//这样合理吗?按理说穿过爱一个没有values 的数据 应该返回一个空数组吧
		if(values.length == 0 || field.equals("")) return new Map[0];
		String[] arr = new String[values.length]; 
		Arrays.fill( arr, "?");
		String where = " and " + field + " in("+ StringUtils.join(arr, ',') +")";
		return this.list(dbName, table, where, values);
	}


	public Map<String, Object>[] toMany(String dbName, List<Object> models, Map<String, String> mapping) throws Exception {
		@SuppressWarnings("unchecked")
		Map<String,Object>[] paras = models.toArray(new Map[0]);
		return toMany(dbName,paras,mapping);
	}
	
	private String serviceVersion;

	public String getServiceVersion() {
		long t1 = System.currentTimeMillis();
		Map<String,Object> map = null;
		try {
			map = this.getFromCache(config.getDefaultDbName(),"tbl_user", 2333);
		} catch (Exception e) {
			e.printStackTrace();
		}
		long t2 = System.currentTimeMillis();
		logger.info("===getServiceVersion===|"+(t2-t1)+"|"+t1+"|"+t2+"|"+(map==null));
		return map != null ? JSON.toJSONString(map) : serviceVersion;
	}

	public void setServiceVersion(String serviceVersion) {
		this.serviceVersion = serviceVersion;
	}

	
//	public Object test() {
//		if(100>12)
//		{
//			String dbName = config.getDefaultDbName() ;
//			String table = "tbl_user";
//			Object pk = 2333;
//			Map<String,Object> r = null;
//			try
//			{
//				return this.getFromCache(dbName, table, pk); 
//			}catch(Exception exception){
//				logger.error("dbService.err:dbService.get:"+dbName+":"+table+":"+pk);
//			}
//		}
//		Map<String,Object> map = null;
//		try {
//			map = this.getFromCache(config.getDefaultDbName(),"tbl_user", 2333);
//		} catch (Exception e) {
//			e.printStackTrace();
//		};
//		return map;
//	}

	

	public static void main(String[] args) {
		System.out.println("李文宇");
	}
}
