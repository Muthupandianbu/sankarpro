package com.iii.dao;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.dao.DataAccessException;

public interface IDao {
	
	void setDataSource(DataSource paramDataSource);
	
	
	public abstract List<Map<String, Object>> getQueryResult(String queryName, LinkedHashMap paramLinkedHashMap);
	
	public abstract List<Map<String, Object>> getQueryResult(String queryName);
	
	public abstract int[] batchUpdate(String queryName, List inparamValues);
	
	public abstract void executeUpdate(String queryName, LinkedHashMap paramLinkedHashMap);
		
	public abstract Map<String, Object> executeProcedure(String procName);
	
	public abstract Map<String, Object> executeProcedure(String packageName,String procName);
	
	public abstract Map<String, Object> executeProcedure(String SchemaName,String packageName,String procName);
	
	public abstract Map<String, Object> executeProcedure(String procName,Map inparamValues,Map inparamTypes);
	
	public abstract Map<String, Object> executeProcedure(String PackageName,String procName,Map inparamValues,Map inparamTypes);
	
	public abstract Map<String, Object> executeProcedure(String SchemaName,String PackageName,String procName,Map inparamValues,Map inparamTypes);
	
}
