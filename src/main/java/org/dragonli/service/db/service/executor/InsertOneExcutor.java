/**
 * 
 */
package org.dragonli.service.db.service.executor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

/**
 * @author freeangel
 *
 */
public class InsertOneExcutor  extends ExcutorBase<Map<String,Object>,Map<String,Object>> implements PreparedStatementCreator
{

	@Override 
	public void execute()
	{
		//非自增长主键，待完成
		
		KeyHolder keyHolder = new GeneratedKeyHolder();
		jdbcTemplate.update(this,keyHolder);
		result = jdbcTemplate.queryForMap(table.getSelectSql(),new Object[]{keyHolder.getKey()});
		formatObject(result);
	}
	
	@Override
	public PreparedStatement createPreparedStatement(Connection con) throws SQLException
	{
		// TODO Auto-generated method stub
		
		//非自增长主键，待完成
		
		PreparedStatement ps = con.prepareStatement(table.getInsertSql(),Statement.RETURN_GENERATED_KEYS);  
		if( table.isUseActive() && para.get(table.getConfig().getActiveTag()) == null )
			para.put(table.getConfig().getActiveTag(), true);
		for( int i = 1 ; i < table.getColList().length ; i++ )
		{
			if( para.containsKey( table.getColList()[i])  )
				ps.setObject(i,para.get( table.getColList()[i] ));
			else
				ps.setNull(i, table.getColsType()[i]);
//				setDefaultValue(ps,i,i);
		}
		return ps;    
	}
	
	
//	KeyHolder keyHolder = new GeneratedKeyHolder();
//    jdbcTemplate.update(new PreparedStatementCreator()
//    {  
//        public PreparedStatement createPreparedStatement(Connection connection) throws SQLException
//        {  
////            if( pss != null )
////            	return pss;
//            String sql = "insert into test1(name,pass) values (?,?)";   
//            PreparedStatement pss = connection.prepareStatement(sql,Statement.RETURN_GENERATED_KEYS);  
//               pss.setString(1,"aaa");  
//               pss.setString(2, "bbbb2016");  
//               return pss;  
//        }  
//    }, keyHolder);  

}
