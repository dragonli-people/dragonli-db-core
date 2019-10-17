/**
 * 
 */
package org.dragonli.service.db.service.executor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.core.PreparedStatementCreator;

import org.dragonli.tools.general.DataCachePool;

/**
 * @author freeangel
 *
 */
public class CountDisactiveExcutor extends ExcutorBase<QueryParameters,Integer> 
implements PreparedStatementCreator,PreparedStatementCallback<Boolean>
{
	@Override 
	public void execute()
	{
		if( !table.isUseActive() )
		{
			//应该在Command层即过滤
			result = null;
			return ;
		}
		jdbcTemplate.execute(this,this);
	}
	
	@Override
	public PreparedStatement createPreparedStatement(Connection con) throws SQLException {
		// TODO Auto-generated method stub
		PreparedStatement ps = con.prepareStatement(table.getSelectCountDisactiveSql(),Statement.NO_GENERATED_KEYS);  
//		for( int i = 0 ; i < para.paras.length ; i++ )
//			ps.setObject(i+1,para.paras[i]);  
		DataCachePool.back(para);
//		for( int i = 0 ; i < para.paras.length ; i++ )
//			ps.setObject(i+1,para.paras[i]);  
		return ps;  
	}

	@Override
	public Boolean doInPreparedStatement(PreparedStatement ps) throws SQLException, DataAccessException {
		// TODO Auto-generated method stub
		
//		ResultSet rs = pstmt.executeQuery();
		ps.execute();
		ResultSet rs = ps.getResultSet();
		
		this.result = 0;
		if( rs.next() )
			this.result = rs.getInt(1);
		
		return true;
	}


}
