/**
 * 
 */
package org.dragonli.service.db.service.command;

import java.util.Map;

import org.dragonli.service.db.service.DBConfig;
import org.dragonli.service.db.service.executor.ListDisactiveExcutor;
import org.dragonli.service.db.service.executor.QueryParameters;
import org.dragonli.tools.general.DataCachePool;

/**
 * @author freeangel
 *
 */
public class CommandDisactiveQuery extends CommandBase 
{

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
		
		if( !ts.isUseActive() )
		{
			r.addOtherErrMsg("this table "+ts.getTableName()+" is not support active!");
			backToPool(autoBack);
			return r;
		}
		
		ListDisactiveExcutor e = DataCachePool.get(ListDisactiveExcutor.class);
		QueryParameters qp = DataCachePool.get(QueryParameters.class);
//		para.remove("where");
//		para.remove("paras");
		qp.init( para);
		e.init(handler.getJdbcTemplate(), ts, qp);
		r.setResult( e.result() );
		
		DataCachePool.back(e);
		
		backToPool(autoBack);
		
		return r;
	}

}
