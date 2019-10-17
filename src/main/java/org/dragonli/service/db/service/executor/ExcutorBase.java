/**
 * 
 */
package org.dragonli.service.db.service.executor;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.dragonli.service.db.service.metadata.TableStructure;
import org.dragonli.tools.general.IDataCachePool;
import org.dragonli.tools.general.StringUtil;
import org.springframework.jdbc.core.JdbcTemplate;


/**
 * @author freeangel
 * @param <E>
 *
 */
public abstract class ExcutorBase<E,R> implements IExcutor<E,R>, IDataCachePool {

	protected JdbcTemplate jdbcTemplate;
	protected TableStructure table;
	protected R result;
	protected E para;
	
	protected void setDefaultValue(PreparedStatement ps,int indexKey,int indexValue) throws SQLException 
	{
		if( table.getColsNullAble()[indexValue] )
		{
			ps.setObject(indexKey, null);
			return ;
		}
		Object v = TableStructure.defaultVales.get(table.getColsType()[indexValue]);
		if( v instanceof Time )
			v = new Time(System.currentTimeMillis());
		else if( v instanceof Timestamp )
			v = new Timestamp(System.currentTimeMillis());
		else if( v instanceof Date )
			v = new Date(System.currentTimeMillis());
		ps.setObject(indexKey, v);
	}
	
	public void init(JdbcTemplate jdbcTemplate , TableStructure table,E para)
	{
		init(jdbcTemplate,table,para,true);
	}
	
	public void init(JdbcTemplate jdbcTemplate , TableStructure table,E para,boolean auto)
	{
		this.jdbcTemplate = jdbcTemplate;
		this.table = table;
		this.para = para;
		if( auto )
			execute();
	}
	
	public void execute()
	{
		
	}
	
	public R result()
	{
		return result;
	}
	
	protected void formatObject(Map<String,Object> map)
	{
		if( map.get(table.getConfig().getTableNameTag()) == null )
			map.put(table.getConfig().getTableNameTag(), table.getTableName());
		if( map.get(table.getConfig().getVersionKey()) == null )
			map.put(table.getConfig().getVersionKey()
					, table.findOptimisticLock(map.get(table.getConfig().getPrimaryKey())).get()
				);
		
	}
	
	@SuppressWarnings("unchecked")
	protected Map<String,Object>[] listIn(Object[] arr0)
	{
		String[] qms = new String[arr0.length];
		Arrays.fill(qms, "?");
		String sql = StringUtil.substitute(table.getSelectManySql(), new Object[]{String.join(",", Arrays.asList(qms) )});
		//jdbcTemplate.queryForList(sql, elementType, args)
		List<Map<String,Object>> sources = jdbcTemplate.queryForList(sql, arr0);
		return sources.toArray(new Map[0]);
	}
	
	public void clear()
	{
		this.jdbcTemplate = null;
		this.table = null;
		this.result = null;
		this.para = null;
	}
	
}
