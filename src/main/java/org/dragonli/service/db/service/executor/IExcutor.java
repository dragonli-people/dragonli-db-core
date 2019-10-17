/**
 * 
 */
package org.dragonli.service.db.service.executor;

import org.dragonli.service.db.service.metadata.TableStructure;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * @author freeangel
 *
 */
public interface IExcutor<E,R> {

	public void init(JdbcTemplate jdbcTemplate , TableStructure table,E para);
	public void init(JdbcTemplate jdbcTemplate , TableStructure table,E para,boolean auto);
	
	public R result();
	
	public void clear();
	
}
