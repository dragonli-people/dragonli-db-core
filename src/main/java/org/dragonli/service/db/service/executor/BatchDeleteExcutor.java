/**
 * 
 */
package org.dragonli.service.db.service.executor;

import org.dragonli.tools.general.StringUtil;

import java.util.Arrays;

/**
 * @author freeangel
 *
 */
public class BatchDeleteExcutor extends ExcutorBase<Object[],Integer> 
{
	@Override 
	public void execute()
	{
		String[] qms = new String[para.length];
		Arrays.fill(qms, "?");
		String sql = StringUtil.substitute(
				table.isUseActive() ? table.getLogicDelManySql() : table.getDelManySql()
				, new Object[]{String.join(",", Arrays.asList(qms) )});
		int[] types = new int[para.length];
		Arrays.fill(types, table.getColsType()[0]);
		result = jdbcTemplate.update(sql, para, types);
	}
	
}
