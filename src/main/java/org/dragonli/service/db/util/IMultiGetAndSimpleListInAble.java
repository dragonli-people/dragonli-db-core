package org.dragonli.service.db.util;

import java.util.Map;

public interface IMultiGetAndSimpleListInAble {
	Map<String,Object>[] multiGet(String dbName,String table,Object[] pks) throws Exception;
	Map<String,Object>[] listByOneField(String dbName,String table,String field,Object[] values) throws Exception;

}
