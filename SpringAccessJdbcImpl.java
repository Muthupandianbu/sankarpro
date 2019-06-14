package com.iii.dao.impl;



import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.SqlParameter;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.core.simple.SimpleJdbcCall;

import com.iii.dao.IDao;

public class SpringAccessJdbcImpl implements IDao {

	protected final Log logger = LogFactory.getLog(getClass());
	
	private DataSource dataSource;	

	
	private JdbcTemplate jdbcTemplate;
	
	
	public void setDataSource(DataSource paramDataSource) {
		// TODO Auto-generated method stub
		this.dataSource = paramDataSource;		

	}
	
	public Connection getDataSourceConn() throws SQLException {
		// TODO Auto-generated method stub
		return this.dataSource.getConnection();		

	}


	@Override
	public void executeUpdate(String queryName, LinkedHashMap paramLinkedHashMap) {
		// TODO Auto-generated method stub
		Object[] params = paramLinkedHashMap.values().toArray();
		
		this.jdbcTemplate = new JdbcTemplate(this.dataSource);
	    
	    if (this.logger.isInfoEnabled()) {
		      this.logger.info("Query : "+queryName+" \n Input Parameter Values Count : "+paramLinkedHashMap.size());		      
		}
	    
	    this.jdbcTemplate.update(queryName, params);
	    
	}
	
	@Override
	public int[] batchUpdate(String queryName,final List inparamValues) {
		// TODO Auto-generated method stub
				
		this.jdbcTemplate = new JdbcTemplate(this.dataSource);
	    
	    if (this.logger.isInfoEnabled()) {
		      this.logger.info("Query : "+queryName+" \n Input Parameter Values Count : "+inparamValues.size());		      
		}

	    return this.jdbcTemplate.batchUpdate(queryName,new BatchPreparedStatementSetter() {
           	@Override
			public void setValues(PreparedStatement ps, int arg1)
					throws SQLException {
				// TODO Auto-generated method stub
				List inputparam = (List) inparamValues.get(arg1);
				for(int i = 1;i<=inputparam.size();i++){
					System.out.println(inputparam.get(i-1));
					ps.setString(i, (String) inputparam.get(i-1));
				}
			}

			@Override
			public int getBatchSize() {
				// TODO Auto-generated method stub
				return inparamValues.size();
			}
        });
	    
	}
	
	@Override
	public Map<String, Object> executeProcedure(String PackageName,String procName,Map inparamValues,Map inparamTypes) {
		
		
		this.jdbcTemplate = new JdbcTemplate(this.dataSource);
		
		//SimpleJdbcCall simpleJdbcCall = new SimpleJdbcCall(this.jdbcTemplate).withCatalogName(PackageName).withProcedureName(procName).withoutProcedureColumnMetaDataAccess();
		SimpleJdbcCall simpleJdbcCall = new SimpleJdbcCall(this.jdbcTemplate).withCatalogName(PackageName).withProcedureName(procName);
		
		Iterator it = inparamTypes.entrySet().iterator();		
		while (it.hasNext()) {
		    Map.Entry<String, Object> entry = (Map.Entry<String, Object>) it.next();		  		    
		    String name = (String) entry.getKey();
		    int sqlType = (int)((Object) entry.getValue());		  
		    if(!name.contains("|")){
		    	simpleJdbcCall.addDeclaredParameter(new SqlParameter(name, sqlType));
		    }else{
		    	int strIndex = name.indexOf("|");
		    	name = name.substring(0, strIndex);
		    	String typeName =name.substring(strIndex, name.length());
		    	System.out.print(typeName);
		    	simpleJdbcCall.addDeclaredParameter(new SqlParameter(name, sqlType, typeName));
		    }		    
		}
		
		
				
		if (this.logger.isInfoEnabled()) {
		      this.logger.info("PackageName : "+PackageName+" ProcedureName : "+procName+"\nInput Parameter Values Count : "+inparamValues.size()+"\tInput Parameter Types Count : "+inparamTypes.size());		      
		}
		
		SqlParameterSource in = new MapSqlParameterSource(inparamValues);
		
		Map<String, Object> OutParamResult = simpleJdbcCall.execute(inparamValues);
						
		return OutParamResult;
		
	}
	
	@Override
	public Map<String, Object> executeProcedure(String SchemaName,String PackageName,String procName,Map inparamValues,Map inparamTypes) {
		
		
		this.jdbcTemplate = new JdbcTemplate(this.dataSource);
		
		
		//SimpleJdbcCall simpleJdbcCall = new SimpleJdbcCall(this.jdbcTemplate).withSchemaName(SchemaName).withCatalogName(PackageName).withProcedureName(procName).withoutProcedureColumnMetaDataAccess();
		SimpleJdbcCall simpleJdbcCall = new SimpleJdbcCall(this.jdbcTemplate).withSchemaName(SchemaName).withCatalogName(PackageName).withProcedureName(procName);
		
		Iterator it = inparamTypes.entrySet().iterator();		
		while (it.hasNext()) {
		    Map.Entry<String, Object> entry = (Map.Entry<String, Object>) it.next();		  		    
		    String name = (String) entry.getKey();
		    int sqlType = (int)((Object) entry.getValue());		 
		    if(!name.contains("|")){
		    	simpleJdbcCall.addDeclaredParameter(new SqlParameter(name, sqlType));
		    }else{
		    	int strIndex = name.indexOf("|");
		    	name = name.substring(0, strIndex);
		    	String typeName =name.substring(strIndex, name.length());
		    	System.out.print(typeName);
		    	simpleJdbcCall.addDeclaredParameter(new SqlParameter(name, sqlType, typeName));
		    }		    
		}
		
		if (this.logger.isInfoEnabled()) {
		      this.logger.info("PackageName : "+PackageName+" ProcedureName : "+procName+"\nInput Parameter Values Count : "+inparamValues.size()+"\tInput Parameter Types Count : "+inparamTypes.size());		      
		}
		
		SqlParameterSource in = new MapSqlParameterSource(inparamValues);
		
		Map<String, Object> OutParamResult = simpleJdbcCall.execute(inparamValues);
						
		return OutParamResult;
		
	}

	@Override
	public Map<String, Object> executeProcedure(String procName,Map inparamValues,Map inparamTypes) {
		
		
		this.jdbcTemplate = new JdbcTemplate(this.dataSource);
		
		//SimpleJdbcCall simpleJdbcCall = new SimpleJdbcCall(this.jdbcTemplate).withProcedureName(procName).withoutProcedureColumnMetaDataAccess();
		SimpleJdbcCall simpleJdbcCall = new SimpleJdbcCall(this.jdbcTemplate).withProcedureName(procName);
		
		Iterator it = inparamTypes.entrySet().iterator();		
		while (it.hasNext()) {
		    Map.Entry<String, Object> entry = (Map.Entry<String, Object>) it.next();
		    String name = (String) entry.getKey();
		    String name1 = "";
		    int sqlType = (int)((Object) entry.getValue());		 
		    if(!name.contains("|")){
		    	simpleJdbcCall.addDeclaredParameter(new SqlParameter(name, sqlType));
		    }else{
		    	int strIndex = name.indexOf("|");
		    	name1 = name.substring(0, strIndex);
		    	String typeName =name.substring(strIndex+1, name.length());		    	
		    	simpleJdbcCall.addDeclaredParameter(new SqlParameter(name1, sqlType, typeName));
		    }
		    
		}
		
		if (this.logger.isInfoEnabled()) {
		      this.logger.info("ProcedureName : "+procName+" \n Input Parameter Values Count : "+inparamValues.size()+"\tInput Parameter Types Count : "+inparamTypes.size());		      
		}
		
		SqlParameterSource in = new MapSqlParameterSource(inparamValues);
		
		Map<String, Object> OutParamResult = simpleJdbcCall.execute(inparamValues);
						
		return OutParamResult;
		
	}

	@Override
	public Map<String, Object> executeProcedure(String procName) {
		// TODO Auto-generated method stub
		
		this.jdbcTemplate = new JdbcTemplate(this.dataSource);		
		
		SimpleJdbcCall simpleJdbcCall = new SimpleJdbcCall(this.jdbcTemplate).withProcedureName(procName);
		//SimpleJdbcCall simpleJdbcCall = new SimpleJdbcCall(this.jdbcTemplate).withProcedureName(procName).withoutProcedureColumnMetaDataAccess();
		
		if (this.logger.isInfoEnabled()) {
		      this.logger.info(" ProcedureName : "+procName);		      
		}
		
		Map<String, Object> OutParamResult = simpleJdbcCall.execute();
						
		return OutParamResult;
	}
	
	@Override
	public Map<String, Object> executeProcedure(String packageName,String procName) {
		// TODO Auto-generated method stub
		
		this.jdbcTemplate = new JdbcTemplate(this.dataSource);		
		
		SimpleJdbcCall simpleJdbcCall = new SimpleJdbcCall(this.jdbcTemplate).withCatalogName(packageName).withProcedureName(procName);
		//SimpleJdbcCall simpleJdbcCall = new SimpleJdbcCall(this.jdbcTemplate).withCatalogName(packageName).withProcedureName(procName).withoutProcedureColumnMetaDataAccess();
		if (this.logger.isInfoEnabled()) {
		      this.logger.info("PackageName : "+packageName+" ProcedureName : "+procName);		      
		}
		
		Map<String, Object> OutParamResult = simpleJdbcCall.execute();
						
		return OutParamResult;
	}
	
	@Override
	public Map<String, Object> executeProcedure(String SchemaName,String packageName,String procName) {
		// TODO Auto-generated method stub
		
		this.jdbcTemplate = new JdbcTemplate(this.dataSource);		
		
		SimpleJdbcCall simpleJdbcCall = new SimpleJdbcCall(this.jdbcTemplate).withSchemaName(SchemaName).withCatalogName(packageName).withProcedureName(procName);
		//SimpleJdbcCall simpleJdbcCall = new SimpleJdbcCall(this.jdbcTemplate).withSchemaName(SchemaName).withCatalogName(packageName).withProcedureName(procName).withoutProcedureColumnMetaDataAccess();
		
		if (this.logger.isInfoEnabled()) {
		      this.logger.info("PackageName : "+packageName+" ProcedureName : "+procName);		      
		}
		
		Map<String, Object> OutParamResult = simpleJdbcCall.execute();
						
		return OutParamResult;
	}

	@Override
	public List<Map<String, Object>> getQueryResult(String queryName,
			LinkedHashMap paramLinkedHashMap) {
		// TODO Auto-generated method stub
		Object[] params = paramLinkedHashMap.values().toArray();
		
		this.jdbcTemplate = new JdbcTemplate(this.dataSource);
		if (this.logger.isInfoEnabled()) {
		      this.logger.info("Query : "+queryName+" \n Input Parameter Values Count : "+paramLinkedHashMap.size());		      
		}
		return this.jdbcTemplate.queryForList(queryName, params);
		
	}

	@Override
	public List<Map<String, Object>> getQueryResult(String queryName) {
		// TODO Auto-generated method stub		
		
		this.jdbcTemplate = new JdbcTemplate(this.dataSource);
		if (this.logger.isInfoEnabled()) {
		      this.logger.info("Query : "+queryName);		      
		}
		return this.jdbcTemplate.queryForList(queryName);
	}
	
}
