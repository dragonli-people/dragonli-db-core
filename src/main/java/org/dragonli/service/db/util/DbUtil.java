package org.dragonli.service.db.util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;

public class DbUtil {
	public static Map<String,Object>[] toOne(String dbName,Map<String,Object>[] models,Map<String,String> mapping,
			String defaultPrimaryKey,IMultiGetAndSimpleListInAble dbHandler) throws Exception
	{
		Map<String,Set<Object>> pks = new HashMap<>();
		Object pk = null;
		for( Entry<String,String> entry : mapping.entrySet() )
		{
			String[] tempArr = entry.getValue().split(":");
			String key = tempArr[0] + ( tempArr.length == 3 ? ":" + tempArr[2] : "" );
			if( !pks.containsKey(key) )
				pks.put(key, new HashSet<>());
			Set<Object> pkDic = pks.get(key);
			for(Map<String,Object> m:models)
			{
				if( ( pk=m.get(entry.getKey())) != null )
					pkDic.add(pk);
			}
		}
		Map<String,Map<String,Map<Object,Map<String,Object>>>> query = new HashMap<>();
		for(Entry<String,Set<Object>> entry:pks.entrySet())
		{
			String[] tempArr = entry.getKey().split(":");
			String table = tempArr[0];
			String field = tempArr.length == 2 ? tempArr[1] : defaultPrimaryKey;
			Object[] allId = entry.getValue().toArray(new Object[0]);
			if(!query.containsKey(table))
				query.put(table, new HashMap<>());
			if(!query.get(table).containsKey(defaultPrimaryKey))
				query.get(table).put(defaultPrimaryKey, new HashMap<>());
			if(!query.get(table).containsKey(field))
				query.get(table).put(field, new HashMap<>());
			Map<String,Map<Object,Map<String,Object>>> dic = query.get(table);
			Map<String,Object>[] list = tempArr.length != 2 ? 
					dbHandler.multiGet(dbName, table, allId ) : dbHandler.listByOneField(dbName, table, field, allId);
			for( Map<String,Object> m : list )
			{
				dic.get(field).put( m.get(field),m);
				dic.get(defaultPrimaryKey).put( m.get(defaultPrimaryKey),m);
			}
		}
		Map<String,Object> toOneModel = null;
		for(Map<String,Object> m:models)
		{
			for( Entry<String,String> entry : mapping.entrySet() )
			{
				String[] tempArr = entry.getValue().split(":");
				String table = tempArr[0];
				String field = tempArr.length == 3 ? tempArr[2] : defaultPrimaryKey;
				if( ( pk=m.get(entry.getKey())) != null && ( toOneModel = query.get(table).get(field).get(Long.valueOf(pk.toString())) ) != null )
				{
					Map<String,Object> copy = new HashMap<>();
					copy.putAll(toOneModel);
					m.put(tempArr[1], copy);
				}
			}
		}
		return models;
	}
	
	public static Map<String,Object>[] toMany(String dbName,Map<String,Object>[] models,Map<String,String> mapping,
			String defaultPrimaryKey,IMultiGetAndSimpleListInAble dbHandler) throws Exception
	{
		//mapping格式示例:  [propId => 'tbl_user:userList:id',...]
		Map<String,Set<Object>> pks = new HashMap<>();
		Object pk = null;
		for( Entry<String,String> entry : mapping.entrySet() )
		{
			//按table:id的格式分组，取出models中propId，为listIn作准备.键值示例:tbl_user:id,用set去重
			String[] tempArr = entry.getValue().split(":");
			String key = tempArr[0] + ( tempArr.length >= 3 ? ":" + tempArr[2] : "" );
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
			String field = tempArr.length == 2 ? tempArr[1] : defaultPrimaryKey;
			Object[] allId = entry.getValue().toArray(new Object[0]);
			if(!query.containsKey(table))
				query.put(table, new HashMap<>());
			if(!query.get(table).containsKey(field))
				query.get(table).put(field, new HashMap<>());
			Map<String,Map<Object,Queue<Map<String,Object>>>> dic = query.get(table);
			Map<String,Object>[] list = dbHandler.listByOneField(dbName, table, field, allId); 
//			按table:id的格式分组,listIn查询出结果，并置入字典，例： { "tbl_user":{38:[model1,model2,model3...]} }
			for( Map<String,Object> m : list )
			{
				if( !dic.get(field).containsKey(m.get(field)) ){
					dic.get(field).put(m.get(field), new LinkedList<>());
					
				}
				
				
					
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
				String field = tempArr.length == 3 ? tempArr[2] : defaultPrimaryKey;
				
		
				if( ( pk=m.get(entry.getKey())) != null && ( toOneModels = query.get(table).get(field).get(Long.valueOf(pk.toString())) ) != null )
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
}
