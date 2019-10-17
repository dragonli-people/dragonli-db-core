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
import org.dragonli.tools.general.DataCachePool;

/**
 * @author freeangel
 *
 */
public class CommandBatchGet extends CommandBase {

	public Map<String,Object>[] exec(Map<String,Object>[] models) throws Exception
	{
		// TODO Auto-generated method stub
		Map<String,Queue<Object>> dic = new HashMap<>();
		for( int i = 0 ; i < models.length ; i++ )
		{
			Map<String,Object> current = models[i];
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
			if( current.get(config.getPrimaryKey()) == null )
			{
				backToPool(autoBack,true);
				throw new Exception("some of data pk is empty#"+tName);
			}
			Queue<Object> queue;
			if( ( queue = dic.get(tName) ) == null )
				dic.put( tName , queue = new LinkedList<>() );
			queue.add(current.get(config.getPrimaryKey()));
		}
		
		Queue<Map<String,Object>> rr = new LinkedList<>();
		for( Entry<String,Queue<Object>> entry : dic.entrySet() )
		{
			ts = handler.findTableStructure(entry.getKey());//....for...
			BatchGetExcutor e = DataCachePool.get(BatchGetExcutor.class);
			e.init(handler.getJdbcTemplate(), ts, entry.getValue().toArray(new Object[0]));
			rr.addAll( Arrays.asList( e.result() ) );
			DataCachePool.back(e);
		}
		
		@SuppressWarnings("unchecked")
		Map<String,Object>[] rrr = rr.toArray(new Map[0] );
		
		backToPool(autoBack,true);
		
		return rrr;
	}
	
	protected boolean autoCheckTable()
	{
		return false;
	}

}
