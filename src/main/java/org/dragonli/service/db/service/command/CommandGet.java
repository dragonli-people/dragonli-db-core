/**
 * 
 */
package org.dragonli.service.db.service.command;

import java.util.Map;

import org.dragonli.service.db.service.executor.GetExcutor;
import org.dragonli.tools.general.DataCachePool;

/**
 * @author freeangel
 *
 */
public class CommandGet extends CommandBase {

	public Map<String,Object> exec(Object pk,String ak) throws Exception
	{
		// TODO Auto-generated method stub
		
		if( pk == null )
		{
			backToPool(autoBack,true);
			throw new Exception(table+"."+config.getPrimaryKey()+" cant be null");
		}
		startTime = System.currentTimeMillis();
		GetExcutor e = DataCachePool.get(GetExcutor.class);
		e.init(handler.getJdbcTemplate(), ts, pk);
		Map<String,Object> rusult = e.result();
		
		DataCachePool.back(e);
		
		endTime = System.currentTimeMillis();
		//start end num type ak
//		Map<String,Object> data = new HashMap<String, Object>();
//		data.put("start", start);data.put("end", end);data.put("num", rusult.size());data.put("type", "read");data.put("ak", ak);
		this.readCount = rusult!=null?rusult.size():0;
		this.ak = ak;
		this.writeToDbInvokeLogger(startTime, endTime, ak,readCount,updateCount,insertCount);
		backToPool(autoBack,true);
		return rusult;
	}
	

}
