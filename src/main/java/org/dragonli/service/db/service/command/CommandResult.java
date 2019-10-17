/**
 * 
 */
package org.dragonli.service.db.service.command;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import com.alibaba.fastjson.JSON;
import org.dragonli.tools.general.IDataCachePool;

/**
 * this class is not thread safe,please use it in same thread!
 * @author freeangel
 *
 */
public class CommandResult implements IDataCachePool {
	
	public final static int SUCCESS = 0;
	public final static int ERR = 1;
	
	private int state;
	private final Queue<String> notExsitDb = new LinkedList<String>();
	private Queue<String> notExsitTable = new LinkedList<String>();
	private Queue<String> versionOldMsg = new LinkedList<String>();
	private Queue<String> otherErrMsg = new LinkedList<String>();
	private Object result;
	
	@Override
	public void clear() {
		// TODO Auto-generated method stub
		state = SUCCESS;
		result = null;
		notExsitDb.clear();
		notExsitTable.clear();
		versionOldMsg.clear();
		otherErrMsg.clear();
	}
	public int getState() {
		return state;
	}
	public void setState(int state) {
		this.state = state;
	}
	public Object getResult() {
		return result;
	}
	public void setResult(Object result) {
		this.result = result;
	}
	
	public void addNotExsitDb(String msg)
	{
		state = ERR;
		notExsitDb.add(msg);
	}
	
	public void addNotExsitTable(String msg)
	{
		state = ERR;
		notExsitTable.add(msg);
	}
	
	public void addNotversionOldMsg(String table,int id)
	{
		state = ERR;
		versionOldMsg.add(table+"#"+id);
	}
	
	public void addOtherErrMsg(String msg)
	{
		state = ERR;
		otherErrMsg.add(msg);
	}
	
	public String getErrMsg()
	{
		if( state == SUCCESS )
			return null;
		String msg = "";
		
		if( notExsitDb.size() != 0 )
			msg += "[some database not exsit:" + String.join(",", notExsitDb) + "];";
		if( notExsitTable.size() != 0 )
			msg += "[some table not exsit:" + String.join(",", notExsitTable) + "];";
		if( versionOldMsg.size() != 0 )
			msg += "[some record`s version is old:" + String.join(",", versionOldMsg) + "];";
		if( otherErrMsg.size() != 0 )
			msg += "[other err:" + String.join(",", otherErrMsg) + "];";
		
		return msg;
	}
	
	public String toJSON()
	{
		Map<String,Object> r = new HashMap<>();
		r.put("state", state);
		if( state == ERR )
			r.put("errMsg", getErrMsg() );
		if(result!=null)
			r.put("result", result);
		return JSON.toJSONString(r);
	}
	
}
