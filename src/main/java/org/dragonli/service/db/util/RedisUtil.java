package org.dragonli.service.db.util;

import java.util.Map;

import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;

import com.alibaba.fastjson.JSON;

public class RedisUtil {
	public static String getModelTextFromCache(String dbName,String table,Object pk,RedissonClient redissonClient,String redisKey) throws Exception
	{
		if(redissonClient == null || redisKey==null)
			return null;
		String json = null;
		RMap<String, String> map = redissonClient.getMap(redisMapKey(redisKey,dbName,table));
		json = map.get(pk.toString());
		return json == null ? null : json;
	}
	public static Map<String,Object> getFromCache(String dbName,String table,Object pk,RedissonClient redissonClient,String redisKey) throws Exception
	{
		String text = getModelTextFromCache(dbName,table,pk,redissonClient,redisKey);
		return text == null ? null : JSON.parseObject(text);
	}
	
	public static void saveToCache(String dbName,String table,Object pk,Map<String,Object> model,RedissonClient redissonClient,String redisKey) throws Exception
	{
		RedisUtil.doSaveToCache(dbName, table, pk, model, redissonClient, redisKey);
	}
	
	public static void saveModelTextToCache(String dbName,String table,Object pk,String model,RedissonClient redissonClient,String redisKey) throws Exception
	{
		RedisUtil.doSaveToCache(dbName, table, pk, model, redissonClient, redisKey);
	}
	
	public static void doSaveToCache(String dbName,String table,Object pk,Object theModel,RedissonClient redissonClient,String redisKey) throws Exception
	{
		if(redissonClient == null || redisKey==null||theModel==null)
			return;
		String model = theModel instanceof String ? (String)theModel : JSON.toJSONString(theModel);
		RMap<String, String> map = redissonClient.getMap(redisMapKey(redisKey,dbName,table));
		map.put(pk.toString(), model);
	}
	
	public static void deleteFromCache(String dbName,String table,Object pk,RedissonClient redissonClient,String redisKey)
	{
		if(redissonClient == null || redisKey==null)
			return ;
		RMap<String, String> map = redissonClient.getMap(redisMapKey(redisKey,dbName,table));
		map.remove(pk.toString());
	}
	
	public static String redisMapKey(String redisKey,String dbName,String table)
	{
		return redisKey+modelKey(dbName,table);
	}
	
	public static String modelKey_pk(String dbName,String table,Object pk)
	{
		return dbName+":"+table+":"+pk;
	}
	
	public static String modelKey(String dbName,String table)
	{
		return dbName+":"+table;
	}
	
}
