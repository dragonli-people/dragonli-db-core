/**
 * 
 */
package org.dragonli.service.db.service.command;

import java.util.Map;

import org.dragonli.service.db.service.executor.ListQueryExcutorOld;
import org.dragonli.service.db.service.executor.QueryParameters;
import org.dragonli.tools.general.DataCachePool;

/**
 * @author freeangel
 *
 */
public class CommandList extends CommandBase 
{

	public Map<String,Object>[] exec(String where,Object[] paras,String ak) throws Exception
	{
		// TODO Auto-generated method stub
		startTime = System.currentTimeMillis();
		ListQueryExcutorOld e = DataCachePool.get(ListQueryExcutorOld.class);
		QueryParameters qp = DataCachePool.get(QueryParameters.class);
		qp.init(where, paras);
		e.init(handler.getJdbcTemplate(), ts, qp);
		Map<String,Object>[] rr = e.result() ;
		
		DataCachePool.back(e);
		
		endTime = System.currentTimeMillis();
		//start end num type ak
		this.readCount = rr.length;
		this.ak = ak;
		this.writeToDbInvokeLogger(startTime, endTime, ak,readCount,updateCount,insertCount);
		backToPool(autoBack,true);
		return rr;
	}

}
