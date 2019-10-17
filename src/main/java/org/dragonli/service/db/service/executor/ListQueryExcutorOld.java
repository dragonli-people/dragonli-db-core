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
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import org.dragonli.tools.general.DataCachePool;
import org.dragonli.tools.general.StringUtil;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.core.PreparedStatementCreator;

/**
 * @author freeangel
 *
 */
public class ListQueryExcutorOld extends ExcutorBase<QueryParameters,Map<String,Object>[]> 
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
		String sql = StringUtil.substitute( table.getSelectMoreSql() , new Object[]{ para.toQuery() } );
		PreparedStatement ps = con.prepareStatement(sql,Statement.RETURN_GENERATED_KEYS);  
		for( int i = 0 ; i < para.paras.length ; i++ )
			ps.setObject(i+1,para.paras[i]); 
		if( para.getCount() != null )
		{
			ps.setInt(para.getParas().length+1, para.getStart()==null?0:para.getStart());
			ps.setInt(para.getParas().length+2, para.getCount());
		}
		DataCachePool.back(para);
		return ps;  
	}

	@Override
	@SuppressWarnings("unchecked")
	public Boolean doInPreparedStatement(PreparedStatement ps) throws SQLException, DataAccessException {
		// TODO Auto-generated method stub
		
//		ResultSet rs = pstmt.executeQuery();
		ps.execute();
		ResultSet rs = ps.getResultSet();
		
		Queue<Map<String,Object>> queue = new LinkedList<>();
		Map<String,Object> map;
		while( rs.next() )
		{
			map = new HashMap<String,Object>();
			for( int i = 0 ; i < table.getColList().length ; i++ )
				map.put( table.getColList()[i] , rs.getObject(i+1));
			formatObject(map);
			queue.add(map);
		}
		this.result = queue.toArray(new Map[0]);
		
		return true;
	}


}
