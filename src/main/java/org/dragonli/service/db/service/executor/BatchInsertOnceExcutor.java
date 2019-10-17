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
public class BatchInsertOnceExcutor extends ExcutorBase<Map<String,Object>[],Map<String,Object>[]> 
implements BatchPreparedStatementSetter
{
	private String tempPkTag;
	
	public String getTempPkTag() {
		return tempPkTag;
	}

	public void setTempPkTag(String tempPkTag) {
		this.tempPkTag = tempPkTag;
	}

	@Override 
	@SuppressWarnings("unchecked")
	public void execute()
	{
//		result = jdbcTemplate.execute(this,this);
		int[] ids = jdbcTemplate.batchUpdate(table.getInsertSql(), this);
		this.result = new Map[0];
//		Object[] pks = new Object[ids.length];
//		System.arraycopy(ids, 0, pks, 0, ids.length);
//		Map<String,Object>[] arr= this.listIn(pks);
//		for(Map<String,Object> one:arr)
//			formatObject(one);
//		this.result = arr;
	}
	
	@Override
	public void setValues(PreparedStatement ps, int i) throws SQLException {
		// TODO Auto-generated method stub
		Map<String,Object> one = para[i];
        for( int ii = 1 ; ii < table.getColList().length ; ii++ )
		{
			if( one.containsKey( table.getColList()[ii] ) )
				ps.setObject(ii, one.get(table.getColList()[ii]) );
			else
				ps.setNull(ii, table.getColsType()[ii]);
//					setDefaultValue(ps,ii,ii);
		}
	}

	@Override
	public int getBatchSize() {
		// TODO Auto-generated method stub
		return para.length;
	}



}
