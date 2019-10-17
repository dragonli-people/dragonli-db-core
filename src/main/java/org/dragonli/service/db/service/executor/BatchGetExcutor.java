/**
 * 
 */
package org.dragonli.service.db.service.executor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.core.PreparedStatementCreator;

import org.dragonli.tools.general.StringUtil;

/**
 * @author freeangel
 *
 */
public class BatchGetExcutor extends ExcutorBase<Object[],Map<String,Object>[]> 
implements PreparedStatementCreator,PreparedStatementCallback<Boolean>
{
	@Override 
	public void execute()
	{
		//新代码
		Map<String,Object>[] sources = this.listIn(para);
		for(Map<String,Object> one : sources)
			formatObject(one);
		this.result = sources;
				
		//老代码
		//jdbcTemplate.execute(this,this);
	}
	
	@Override
	public PreparedStatement createPreparedStatement(Connection con) throws SQLException {
		// TODO Auto-generated method stub
		Object[] arr0 = para;
		String[] qms = new String[arr0.length];
		Arrays.fill(qms, "?");
		String sql = StringUtil.substitute(table.getSelectManySql(), new Object[]{String.join(",", Arrays.asList(qms) )});
		PreparedStatement ps = con.prepareStatement(sql,Statement.NO_GENERATED_KEYS);
		for( int i = 1 ; i <= arr0.length ; i++ )
			ps.setObject(i,arr0[i-1]);//存疑
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
