/**
 * 
 */
package org.dragonli.service.db.service.executor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.core.PreparedStatementCreator;

/**
 * @author freeangel
 *
 */
public class BatchInsertExcutor extends ExcutorBase<Map<String,Object>[],Map<Object,Object>> 
implements PreparedStatementCreator,PreparedStatementCallback<Map<Object,Object>>
{
	private String tempPkTag;
	
	public String getTempPkTag() {
		return tempPkTag;
	}

	public void setTempPkTag(String tempPkTag) {
		this.tempPkTag = tempPkTag;
	}

	@Override 
	public void execute()
	{
		result = jdbcTemplate.execute(this,this);
	}
	
	@Override
	public PreparedStatement createPreparedStatement(Connection con) throws SQLException {
		// TODO Auto-generated method stub
		PreparedStatement ps = con.prepareStatement(table.getInsertSql(),Statement.RETURN_GENERATED_KEYS);  
		return ps;  
	}

	@Override
	public Map<Object,Object> doInPreparedStatement(PreparedStatement ps) throws SQLException, DataAccessException {
		// TODO Auto-generated method stub
		
//		ResultSet rs = pstmt.executeQuery();
		
		Map<Object,Object> pks = new HashMap<>();
		for(int i = 0 ;i<para.length;i++)
		{
			Map<String,Object> one = para[i];
			if( table.isUseActive() && one.get(table.getConfig().getActiveTag()) == null )
				one.put(table.getConfig().getActiveTag(), true);
			for( int ii = 1 ; ii < table.getColList().length ; ii++ )
			{
				if( one.containsKey( table.getColList()[ii] ) )
					ps.setObject(ii, one.get(table.getColList()[ii]) );
				else
					ps.setNull(ii, table.getColsType()[ii]);
//					setDefaultValue(ps,ii,ii);
			}
			ps.execute();
			ResultSet rs = ps.getGeneratedKeys();
			if( rs.next() )
			{
				Object pk = rs.getObject(1);
				Object pk2 = pk;
				if( tempPkTag != null && one.containsKey(tempPkTag) )
					pk2 = one.get(tempPkTag);
				pks.put(pk.toString(),pk2);
			}
//				one.put(table.getPrimaryKey(),  rs.getObject(1));
//			formatObject(one);
		}
//		ps.executeBatch();  
//		ResultSet rs = ps.getGeneratedKeys();  
//		while( rs.next() )
//		{
//			Map<String,Object> one = new HashMap<String,Object>();
//			for( int ii = 0 ; ii < table.getColList().length ; ii++ )
//				one.put(table.getColList()[ii], rs.getObject(ii+1));
//			queue.add(one);
//		}
		
		return pks;//.toArray(new Object[0]);
	}


}
