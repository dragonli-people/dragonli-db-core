/**
 * 
 */
package org.dragonli.service.db.service.executor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.dragonli.tools.general.StringUtil;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.core.PreparedStatementCreator;


/**
 * @author freeangel
 *
 */
public class CountExcutor extends ExcutorBase<QueryParameters,Integer> 
implements PreparedStatementCreator,PreparedStatementCallback<Boolean>
{
	@Override 
	public void execute()
	{
		jdbcTemplate.execute(this,this);
	}
	
	@Override
	public PreparedStatement createPreparedStatement(Connection con) throws SQLException {
		// TODO Auto-generated method stub
		String sql = StringUtil.substitute( table.getSelectCountSql() , new Object[]{ para.toQuery() } );
		PreparedStatement ps = con.prepareStatement(sql,Statement.NO_GENERATED_KEYS);  
		for( int i = 0 ; i < para.paras.length ; i++ )
			ps.setObject(i+1,para.paras[i]);  
//		DataCachePool.back(para);
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
