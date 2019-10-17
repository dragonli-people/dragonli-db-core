/**
 * 
 */
package org.dragonli.service.db.service.executor;

import java.util.Map;

import com.alibaba.fastjson.JSONArray;
import org.dragonli.tools.general.IDataCachePool;

/**
 * @author freeangel
 *
 */
public class QueryParameters implements IDataCachePool {
	protected String where;
	protected Object[] paras;
	protected Integer start;
	protected Integer count;
	public Integer getStart() {
		return start;
	}
	public Integer getCount() {
		return count;
	}
	public void init(Map<String,Object> para)
	{
		where = (String) para.get("where");
		paras = null;
		JSONArray jParas = ( (JSONArray) para.get("paras") );
		if( jParas == null )
			paras = new Object[]{};
		else
			paras = jParas.toArray(new Object[0]);
		start = (Integer) para.get("start");
		count = (Integer) para.get("count");
	}
	public void init(String where,Object[] paras)
	{
		this.init(where,paras,null,null);
	}
	public void init(String where,Object[] paras,Integer start,Integer count)
	{
		this.where = where;
		this.paras = paras;
		this.count = count;
		this.start = start;
	}
	public String getWhere() {
		return where;
	}
	public void setWhere(String where) {
		this.where = where;
	}
	public Object[] getParas() {
		return paras;
	}
	public void setParas(Object[] paras) {
		this.paras = paras;
	}
	
	public String toQuery()
	{
		return (where == null ? "" : " " + where + " ") 
				+ ( count == null ? "" : " limit ?,?");
	}
	
	public void clear()
	{
		this.where = null;
		this.paras = null;
		start = null;
		count = null;
	}
	
}
