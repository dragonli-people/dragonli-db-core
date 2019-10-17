/**
 * 
 */
package org.dragonli.service.db.service.command;

import org.dragonli.service.db.service.executor.CountExcutor;
import org.dragonli.service.db.service.executor.QueryParameters;
import org.dragonli.tools.general.DataCachePool;

/**
 * @author freeangel
 *
 */
public class CommandCount extends CommandBase 
{

	public Integer exec(String where,Object[] paras) throws Exception
	{
		// TODO Auto-generated method stub
		CountExcutor e = DataCachePool.get(CountExcutor.class);
		QueryParameters qp = DataCachePool.get(QueryParameters.class);
		qp.init(where,paras);// para);
		e.init(handler.getJdbcTemplate(), ts, qp);
		Integer rr = e.result() ;
		
		DataCachePool.back(e);
		DataCachePool.back(qp);
		backToPool(autoBack,true);
		
		return rr;
	}

}
