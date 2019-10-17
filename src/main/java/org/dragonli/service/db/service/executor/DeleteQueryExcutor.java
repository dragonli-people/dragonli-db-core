/**
 * 
 */
package org.dragonli.service.db.service.executor;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.dragonli.tools.general.DataCachePool;
import org.dragonli.tools.general.StringUtil;
import org.springframework.jdbc.core.PreparedStatementSetter;

/**
 * @author freeangel
 *
 */
public class DeleteQueryExcutor extends ExcutorBase<QueryParameters,Integer> implements PreparedStatementSetter
{
	@Override 
	public void execute()
	{
		String sql = StringUtil.substitute(
				table.isUseActive() ? table.getLogicDelMoreSql() : table.getDelMoreSql() , new Object[]{ para.toQuery() } );
		result = jdbcTemplate.update(sql, this);
	}

	@Override
	public void setValues(PreparedStatement ps) throws SQLException {
		// TODO Auto-generated method stub
		for( int i = 0 ; i < para.paras.length ; i++ )
			ps.setObject(i+1, para.paras[i]);
		DataCachePool.back(para);
	}
	
}
