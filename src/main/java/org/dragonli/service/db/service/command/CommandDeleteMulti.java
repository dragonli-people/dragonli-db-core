/**
 * 
 */
package org.dragonli.service.db.service.command;

import org.dragonli.service.db.service.executor.BatchDeleteExcutor;
import org.dragonli.tools.general.DataCachePool;

/**
 * @author freeangel
 *
 */
public class CommandDeleteMulti extends CommandBase {

	public Integer exec(Object[] pks) throws Exception
	{
		// TODO Auto-generated method stub
		BatchDeleteExcutor e = DataCachePool.get(BatchDeleteExcutor.class);
		e.init(handler.getJdbcTemplate(), ts, pks);
		Integer rr = e.result() ;

		DataCachePool.back(e);
		
		backToPool(autoBack,true);
		
		return rr;
	}

}
