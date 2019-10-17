/**
 * 
 */
package org.dragonli.service.db.service.executor;

/**
 * @author freeangel
 *
 */
public class DeleteOneExcutor extends ExcutorBase<Object,Integer> 
{
	@Override 
	public void execute()
	{
		result = jdbcTemplate.update(
				table.isUseActive() ? table.getLogicDelSql() :table.getDelSql()
				, new Object[]{para}, new int[]{table.getColsType()[0]});
	}
	
}
