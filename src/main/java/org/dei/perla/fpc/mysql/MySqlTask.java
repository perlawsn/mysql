package org.dei.perla.fpc.mysql;

import java.util.List;

import org.dei.perla.core.fpc.Attribute;
import org.dei.perla.core.fpc.Task;


public class MySqlTask implements Task {

	private String tableName;
	private Task task;
	
	public MySqlTask(Task task, String tableName){
		this.task = task;
		this.tableName = tableName;
	}
	
	

	public String getTableName() {
		return tableName;
	}

	public Task getTask() {
		return task;
	}



	public List<Attribute> getAttributes() {
		// TODO Auto-generated method stub
		return task.getAttributes();
	}



	public boolean isRunning() {
		// TODO Auto-generated method stub
		return task.isRunning();
	}



	public void stop() {
		// TODO Auto-generated method stub
	}


}
