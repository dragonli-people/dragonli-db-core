/**
 * 
 */
package org.dragonli.service.db.service.command;

import java.util.Map;

import org.dragonli.service.db.service.DBConfig;
import org.dragonli.service.db.service.executor.RestoreExcutor;
import org.dragonli.tools.general.DataCachePool;

/**
 * @author freeangel
 *
 */
public class CommandRestore extends CommandBase {

	@Override
	public CommandResult execute(String dbName, DBConfig config,Map<String, Object> para,boolean autoBack) throws Exception
	{
		// TODO Auto-generated method stub
		CommandResult r = DataCachePool.get(CommandResult.class);
		if( !checkDbAndTable(r,dbName,config,para) )
		{
			backToPool(autoBack);
			return r;
		}
		
		Object pk = (Object) para.get(config.getPrimaryKey());
		if( pk != null )
		{
			r.addOtherErrMsg("cant Restore "+ts.getTableName()+"#pk is null");
			backToPool(autoBack);
			return r;
		}

		RestoreExcutor e = DataCachePool.get(RestoreExcutor.class);
		e.init(handler.getJdbcTemplate(), ts, para);
		r.setResult( e.result() );
		DataCachePool.back(e);
		
		backToPool(autoBack);
		
		return r;
	}

}
