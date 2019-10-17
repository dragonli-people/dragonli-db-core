/**
 * 
 */
package org.dragonli.service.db.service.command;

import java.util.Map;

import com.alibaba.fastjson.JSONArray;
import org.dragonli.service.db.service.DBConfig;
import org.dragonli.service.db.service.executor.BatchGetExcutor;
import org.dragonli.service.db.service.executor.BatchRestoreExcutor;
import org.dragonli.tools.general.DataCachePool;

/**
 * @author freeangel
 *
 */
public class CommandRestoreMulti extends CommandBase {

	@Override
	public CommandResult execute(String dbName, DBConfig config, Map<String, Object> para, boolean autoBack)
			throws Exception {
		CommandResult r = DataCachePool.get(CommandResult.class);
		if (!checkDbAndTable(r, dbName, config, para)) {
			backToPool(autoBack);
			return r;
		}

		JSONArray jList = (JSONArray) para.get("list");
		if (jList == null) {
			r.addOtherErrMsg("list cant be null");
			backToPool(autoBack);
			return r;
		}
		Object[] list = jList.toArray(new Object[0]);

		BatchRestoreExcutor e = DataCachePool.get(BatchRestoreExcutor.class);
		e.init(handler.getJdbcTemplate(), ts, list);
		DataCachePool.back(e);

		BatchGetExcutor bge = DataCachePool.get(BatchGetExcutor.class);
		bge.init(handler.getJdbcTemplate(), ts, list);
		r.setResult(bge.result());
		DataCachePool.back(bge);

		backToPool(autoBack);

		return r;
	}

}
