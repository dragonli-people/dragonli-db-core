/**
 * 
 */
package org.dragonli.service.db.service.command;

import java.util.HashMap;
import java.util.Map;

import org.dragonli.service.db.service.executor.InsertOneExcutor;
import org.dragonli.service.db.service.executor.UpdateOneExcutor;
import org.dragonli.tools.general.DataCachePool;

/**
 * @author freeangel
 *
 */
public class CommandSave extends CommandBase {

	public Map<String,Object> exec(Map<String, Object> para,String ak) throws Exception
	{
		// TODO Auto-generated method stub
		startTime = System.currentTimeMillis();
		Map<String,Object> data = new HashMap<String, Object>();
		Map<String,Object> rr = null;
		
		if( para == null )
		{
			backToPool(autoBack,true);
			throw new Exception("data cant be null!");
		}
		
		Object pk = (Object) para.get(config.getPrimaryKey());
		if( pk != null )
		{
			UpdateOneExcutor ue = DataCachePool.get(UpdateOneExcutor.class);
			ue.init(handler.getJdbcTemplate(), ts, para);
			rr = ue.result();
			DataCachePool.back(ue);
			updateCount++;
		}
		else
		{
			InsertOneExcutor ie = DataCachePool.get(InsertOneExcutor.class);
			ie.init(handler.getJdbcTemplate(), ts, para);
			rr = ie.result();
			DataCachePool.back(ie);
			insertCount++;
		}
		
		
		endTime = System.currentTimeMillis();
		this.ak = ak;
		this.writeToDbInvokeLogger(startTime, endTime, ak,readCount,updateCount,insertCount);
		//start end num type ak
		backToPool(autoBack,true);
		return rr;
	}

}
