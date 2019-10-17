/**
 * 
 */
package org.dragonli.service.db.service.executor;

import java.util.List;
import java.util.Map;

import org.dragonli.tools.general.StringUtil;

/**
 * @author freeangel
 *
 */
public class ListQueryExcutor extends ExcutorBase<QueryParameters,Map<String,Object>[]> 
//implements PreparedStatementCreator,PreparedStatementCallback<Boolean>
{
	@SuppressWarnings("unchecked")
	@Override 
	public void execute()
	{
		try
		{
			String sql = StringUtil.substitute( table.getSelectMoreSql() , new Object[]{ para.toQuery() } );
			Object[] paras = new Object[ para.paras.length + ( para.getCount() != null ? 2 : 0 ) ];
			System.arraycopy(para.paras, 0, paras, 0, para.paras.length);
			if( para.getCount() != null )
			{
				paras[ para.getParas().length ] = para.getStart()==null?0:para.getStart();
				paras[para.getParas().length+1] =  para.getCount();
			}
			List<Map<String,Object>> sources = jdbcTemplate.queryForList(sql, paras);
			this.result = sources == null ? new Map[0] : sources.toArray(new Map[0]);
		}
		catch(Exception e)
		{
			throw e;
		}
		finally{
//			DataCachePool.back(para);
		}
//		jdbcTemplate.execute(this,this);
	}
	
	/*
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
	*/

}
