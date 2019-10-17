/**
 * 
 */
package org.dragonli.service.db.service.executor;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import org.springframework.jdbc.core.BatchPreparedStatementSetter;

/**
 * @author freeangel
 *
 */
public class BatchUpdateExcutor extends ExcutorBase<Map<String,Object>[],Map<String,Object>[]> implements BatchPreparedStatementSetter
{
	@Override 
	public void execute()
	{
		jdbcTemplate.batchUpdate(table.getUpdateSql(), this);
		result = para;
	}

	@Override
	public void setValues(PreparedStatement ps, int i) throws SQLException {
		// TODO Auto-generated method stub
		Map<String,Object> map = para[i];
		for( int ii = 1 ; ii < table.getColList().length ; ii++ )
		{
			if( map.containsKey(table.getColList()[ii]) )
				ps.setObject(ii,map.get( table.getColList()[ii] ));
			else
				ps.setNull(ii, table.getColsType()[ii]);
//				setDefaultValue(ps,ii,ii);
		}
		ps.setObject(table.getColList().length, map.get(table.getPrimaryKey()));
	}

	@Override
	public int getBatchSize() {
		// TODO Auto-generated method stub
		return para.length;
	}
	
}
