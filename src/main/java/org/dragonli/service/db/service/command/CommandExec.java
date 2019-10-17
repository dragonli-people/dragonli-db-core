/**
 * 
 */
package org.dragonli.service.db.service.command;

import java.util.List;
import java.util.Map;

/**
 * @author freeangel
 *
 */
public class CommandExec extends CommandBase 
{

	protected boolean autoCheckTable()
	{
		return false;
	}

	@SuppressWarnings("unchecked")
	public Map<String,Object>[] exec(String where,Object[] paras) throws Exception
	{
		// TODO Auto-generated method stub
		String cmd = where.trim().split(" ")[0].toLowerCase();
		if("select".equals(cmd))
			return this.execSelect(where,paras);
		else// if("update".equals(cmd))
			return this.execWrite(where,paras);
	
	}
	
	@SuppressWarnings("unchecked")
	public Map<String,Object>[] execSelect(String where,Object[] paras) throws Exception
	{
		List<Map<String,Object>> sources = this.handler.getJdbcTemplate().queryForList(where, paras);
		return sources.toArray(new Map[0]);
	}
	
	public Map<String,Object>[] execWrite(String where,Object[] paras) throws Exception
	{
		this.handler.getJdbcTemplate().execute(where);
		return null;
	}
	
	

}
