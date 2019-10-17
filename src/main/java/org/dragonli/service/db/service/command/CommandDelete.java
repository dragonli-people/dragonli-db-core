/**
 * 
 */
package org.dragonli.service.db.service.command;

import org.dragonli.service.db.service.executor.DeleteOneExcutor;
import org.dragonli.tools.general.DataCachePool;

/**
 * @author freeangel
 *
 */
public class CommandDelete extends CommandBase {

	public Integer exec(Object pk) throws Exception
	{
		// TODO Auto-generated method stub
		if( pk == null )
		{
			backToPool(autoBack,true);
			throw new Exception("cant delete bcz pk is null!");
		}

		DeleteOneExcutor e = DataCachePool.get(DeleteOneExcutor.class);
		e.init(handler.getJdbcTemplate(), ts, pk);
		Integer i = e.result();
		DataCachePool.back(e);
		backToPool(autoBack,true);
		
		return i;
	}

}
