/**
 * 
 */
package org.dragonli.service.db.service.command;

import org.dragonli.service.db.service.executor.DeleteQueryExcutor;
import org.dragonli.service.db.service.executor.QueryParameters;
import org.dragonli.tools.general.DataCachePool;

/**
 * @author freeangel
 *
 */
public class CommandDeleteQuery extends CommandBase 
{

	public Integer exec(String where,Object[] paras) throws Exception
	{
		// TODO Auto-generated method stub
		DeleteQueryExcutor e = DataCachePool.get(DeleteQueryExcutor.class);
		QueryParameters qp = DataCachePool.get(QueryParameters.class);
		qp.init(where,paras);
		e.init(handler.getJdbcTemplate(), ts, qp);
		Integer rr = e.result() ;
		
		DataCachePool.back(e);
		
		backToPool(autoBack,true);
		
		return rr;
	}

}
