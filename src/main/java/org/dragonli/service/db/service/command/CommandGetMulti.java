/**
 * 
 */
package org.dragonli.service.db.service.command;

import java.util.Map;

import org.dragonli.service.db.service.executor.BatchGetExcutor;
import org.dragonli.tools.general.DataCachePool;

/**
 * @author freeangel
 *
 */
public class CommandGetMulti extends CommandBase {

	public Map<String,Object>[] exec(Object[] pks,String ak) throws Exception
	{
		// TODO Auto-generated method stub
		startTime = System.currentTimeMillis();
		BatchGetExcutor e = DataCachePool.get(BatchGetExcutor.class);
		e.init(handler.getJdbcTemplate(), ts, pks);
		Map<String,Object>[] rr = e.result() ;
		this.readCount = rr.length;
		DataCachePool.back(e);
		endTime = System.currentTimeMillis();
		//start end num type ak
		this.ak = ak;
		this.writeToDbInvokeLogger(startTime, endTime, ak,readCount,updateCount,insertCount);
		backToPool(autoBack,true);
		
		return rr;
	}

}
