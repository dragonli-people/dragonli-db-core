/**
 * 
 */
package org.dragonli.service.db.service.command;

import java.util.List;

/**
 * @author freeangel
 *
 */
public class CommandExecBatchUpate extends CommandBase 
{

	protected boolean autoCheckTable()
	{
		return false;
	}

	public void exec(String[] where) throws Exception
	{
		// TODO Auto-generated method stub
		
		this.handler.getJdbcTemplate().batchUpdate(where);
	
	}
	
	public void exec(String where,List<Object[]> paras) throws Exception
	{
		// TODO Auto-generated method stub
		this.handler.getJdbcTemplate().batchUpdate(where,paras);
	}

}
