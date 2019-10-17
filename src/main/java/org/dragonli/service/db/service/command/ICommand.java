/**
 * 
 */
package org.dragonli.service.db.service.command;

import java.util.Map;

import org.dragonli.service.db.service.DBConfig;

/**
 * @author freeangel
 *
 */
public interface ICommand
{
	public void init(String dbName,String table, DBConfig config) throws Exception;
	
	public CommandResult execute(String dbName,DBConfig config,Map<String, Object> para) throws Exception;
	
	public CommandResult execute(String dbName,DBConfig config,Map<String, Object> para,boolean autoBack) throws Exception;
	
}
