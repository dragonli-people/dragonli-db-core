/**
 * 
 */
package org.dragonli.service.db.service.executor;

import java.util.Map;

/**
 * @author freeangel
 *
 */
public class GetExcutor extends ExcutorBase<Object,Map<String,Object>> 
{
	@Override 
	public void execute()
	{
		try{
			result = jdbcTemplate.queryForMap(table.getSelectSql(),new Object[]{para});
			formatObject(result);
		}catch (Exception e) {
			result = null;
		}
		
	}
	
}
