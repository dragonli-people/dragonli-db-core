/**
 * 
 */
package org.dragonli.service.db.service.command;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;

import org.dragonli.service.db.service.executor.BatchGetExcutor;
import org.dragonli.service.db.service.executor.BatchInsertExcutor;
import org.dragonli.service.db.service.executor.BatchInsertOnceExcutor;
import org.dragonli.service.db.service.executor.BatchUpdateExcutor;
import org.dragonli.tools.general.DataCachePool;

/**
 * @author freeangel
 *
 */
public class CommandBatchSave extends CommandBase {

	
	@SuppressWarnings("unchecked")
	public Map<String,Object> exec(Map<String,Object>[] models,String tempPkTag,String ak) throws Exception
	{
		// TODO Auto-generated method stub
		startTime = System.currentTimeMillis();
		Map<String,Object>[] list = models;
		for( int i = 0 ; i < list.length ; i++ )
		{
			Map<String,Object> current =list[i];
			String tName = (String) current.get(config.getTableNameTag());
			if( tName == null || (tName=tName.trim()).equals("") )
			{
				backToPool(autoBack,true);
				throw new Exception("some of data tableTag is empty#"+current.get(config.getPrimaryKey()));
			}
			if( handler.findTableStructure(tName) == null )
			{
				backToPool(autoBack,true);
				throw new Exception( "table " + tName + " not exsit!");
			}
		}
		
		Map<String,Queue<Map<String,Object>>> insertDic = new HashMap<>();
		Map<String,Queue<Map<String,Object>>> insertOnceDic = new HashMap<>();
		Map<String,Queue<Map<String,Object>>> updateDic = new HashMap<>();
		for( int i = 0 ; i < list.length ; i++ )
		{
			Map<String,Object> current =list[i];
			String tName = (String) current.get(config.getTableNameTag());
			Queue<Map<String,Object>> queue;
			Map<String,Queue<Map<String,Object>>> parentQueue = 
					current.get(config.getPrimaryKey()) == null ?
							( current.containsKey(tempPkTag) ? insertDic : insertOnceDic ) 
							: updateDic;
			if( ( queue = parentQueue.get(tName) ) == null )
				parentQueue.put( tName , queue = new LinkedList<>() );
			queue.add(current);
		}
		
//		
		
		Map<Object,Object> pkDic = new HashMap<>();
		Queue<Map<String,Object>> rr = new LinkedList<>();
		for( Entry<String,Queue<Map<String,Object>>> entry : insertOnceDic.entrySet() )
		{
			ts = handler.findTableStructure(entry.getKey());//....for...
			BatchInsertOnceExcutor e = DataCachePool.get(BatchInsertOnceExcutor.class);
			e.init(handler.getJdbcTemplate(), ts, entry.getValue().toArray(new Map[0]));
			rr.addAll( Arrays.asList( e.result() ) );
			DataCachePool.back(e);
			insertCount++;
		}
		for( Entry<String,Queue<Map<String,Object>>> entry : insertDic.entrySet() )
		{
			ts = handler.findTableStructure(entry.getKey());//....for...
			BatchInsertExcutor e = DataCachePool.get(BatchInsertExcutor.class);
			e.setTempPkTag(tempPkTag);
			e.init(handler.getJdbcTemplate(), ts, entry.getValue().toArray(new Map[0]));
			pkDic.put( entry.getKey() , e.result() );
			Object[] pks = e.result().keySet().toArray(new Object[0]);
			DataCachePool.back(e);
			
			BatchGetExcutor bge = DataCachePool.get(BatchGetExcutor.class);
			bge.init(handler.getJdbcTemplate(), ts, pks);
			rr.addAll( Arrays.asList( bge.result() ) );
			DataCachePool.back(bge);
			insertCount++;
		}
		for( Entry<String,Queue<Map<String,Object>>> entry : updateDic.entrySet() )
		{
			ts = handler.findTableStructure(entry.getKey());//....for...
			BatchUpdateExcutor e = DataCachePool.get(BatchUpdateExcutor.class);
			e.init(handler.getJdbcTemplate(), ts, entry.getValue().toArray(new Map[0]));
			rr.addAll( Arrays.asList( e.result() ) );
			DataCachePool.back(e);
			updateCount++;
		}
		
		Map<String,Object> rrr = new HashMap<>();
		rrr.put("list", rr.toArray(new Map[0]) );
		rrr.put("pkDic",pkDic);
		endTime = System.currentTimeMillis();
		//start end num type ak
		this.ak = ak;
		this.writeToDbInvokeLogger(startTime, endTime, ak,readCount,updateCount,insertCount);
		
		backToPool(autoBack,true);
		
		return rrr;
	}
	
	protected boolean autoCheckTable()
	{
		return false;
	}

}
