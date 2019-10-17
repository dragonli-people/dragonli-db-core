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

/**
 * @author freeangel
 *
 */
public class UpdateOneExcutor  extends ExcutorBase<Map<String,Object>,Map<String,Object>> implements PreparedStatementCreator
{

	@Override 
	public void execute()
	{
		jdbcTemplate.update(this);
		result = para;
	}
	
	@Override
	public PreparedStatement createPreparedStatement(Connection con) throws SQLException
	{
		// TODO Auto-generated method stub
		
		//非自增长主键，待完成
		
		PreparedStatement ps = con.prepareStatement(table.getUpdateSql(),Statement.NO_GENERATED_KEYS);  
		for( int i = 1 ; i < table.getColList().length ; i++ )
		{
			if( para.containsKey( table.getColList()[i] ) )
				ps.setObject(i,para.get( table.getColList()[i] ));
			else
				ps.setNull(i, table.getColsType()[i]);
//				setDefaultValue(ps,i,i);
		}
		ps.setObject(table.getColList().length, para.get(table.getPrimaryKey()));
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
