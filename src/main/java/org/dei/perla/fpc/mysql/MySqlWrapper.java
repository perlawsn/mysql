package org.dei.perla.fpc.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.dei.perla.core.fpc.Attribute;
import org.dei.perla.core.fpc.DataType;
import org.dei.perla.core.fpc.Fpc;
import org.dei.perla.core.fpc.Sample;

import org.dei.perla.core.fpc.Task;
import org.dei.perla.core.fpc.TaskHandler;



public class MySqlWrapper {
	
	protected Fpc fpc;
	protected String schemaName;
	
	
	public MySqlWrapper(Fpc fpc) {
		this.fpc = fpc;
		
		schemaName = "FpcSchema" + fpc.getId();
	}
	
	public Task get(List<Attribute> attributes, TaskHandler handler,
			String tableName) 
			throws WrongCorrespondenceException {
		MySqlHandler dbHandler = initializeDbHandler(attributes, handler,
				tableName);
		Task task = fpc.get(attributes, dbHandler);
		MySqlTask dbTask = new MySqlTask(task, dbHandler.getTableName());
		return dbTask;
		
	}

	public Task get(List<Attribute> attributes, long periodMs,
			TaskHandler handler, String tableName)
			throws WrongCorrespondenceException {

		MySqlHandler dbHandler = initializeDbHandler(attributes, handler,tableName);
		Task task = fpc.get(attributes, periodMs, dbHandler);
		MySqlTask dbTask = new MySqlTask(task, dbHandler.getTableName());
		return dbTask;
	}
	
	public Task async(List<Attribute> attributes, TaskHandler handler,
			String tableName) throws WrongCorrespondenceException {
		MySqlHandler dbHandler = initializeDbHandler(attributes, handler,
				tableName);
		Task task = fpc.async(attributes, dbHandler);
		MySqlTask dbTask = new MySqlTask(task, dbHandler.getTableName());
		return dbTask;
	}
	
	
	
	private MySqlHandler initializeDbHandler(
			Collection<Attribute> attributes, TaskHandler handler,
			String tableName) throws WrongCorrespondenceException {
		MySqlHandler dbHandler = new MySqlHandler(handler, attributes);
		if (tableName != null) {
			boolean success = dbHandler.checkTable(tableName);
			if (!success)
				throw new WrongCorrespondenceException(
						"The table does not correspond to the attributes requested! Table: "
								+ tableName + " to the Attributes: "
								+ attributes);
		} else {
			dbHandler.setTableName(makeTableName(attributes));
			dbHandler.createTable();
		}
		return dbHandler;
	};
	
	private String makeTableName(Collection<Attribute> attributes) {
		String tableName = "";
		for (Attribute attribute : attributes) {
			tableName += attribute.getId() + "_";
		}
		tableName += generateRandString(8);
		return tableName;
	}
	
	private String generateRandString(int length) {
		char[] chars = "abcdefghijklmnopqrstuvwxyz123456789".toCharArray();
		StringBuilder sb = new StringBuilder();
		Random random = new Random();
		for (int i = 0; i < length; i++) {
			char c = chars[random.nextInt(chars.length)];
			sb.append(c);
		}
		return sb.toString();
	}


	private class MySqlHandler implements TaskHandler{

		private Collection<Attribute> attributes;
		private final Lock lock = new ReentrantLock();
		private String tableName;
		private TaskHandler handler;
		//inizialmente hostAddr è settato semplicemente sull'host
		private String hostAddr = "jdbc:mysql://localhost";
		private Connection con;
		private Statement cmd;
		
		public MySqlHandler(TaskHandler handler,Collection<Attribute> attributes) {
			this.setAttributes(attributes);
			this.setHandler(handler);
			try {
				connect(hostAddr);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			createSchema();
			
		}

		public void complete(Task task) {
			lock.lock();
			try {
								
				handler.complete(task);
				
			} finally {
				
				lock.unlock();
			}

		}

		public void data(Task task, Sample record) {
			lock.lock();
			try {
				saveRecord(record);				
				handler.data(task, record);			    
			} finally {
				lock.unlock();
			}

		}

		private void saveRecord(Sample record) {
			//Ricostruire bene la query
			Instant timeObj =  (Instant) record.getValue("timestamp");
			
			Timestamp out=timeStampOperation(timeObj);
			String names = "";
			String insertQuery = " ";
			for (Attribute attribute : attributes) {
				Object field = record.getValue(attribute.getId());
				boolean isString = attribute.getType() == DataType.STRING;
				boolean isTimeStamp=attribute.getType()==DataType.TIMESTAMP;
				if (isString){
					insertQuery += "'";
					insertQuery += field.toString();
				}else if (isTimeStamp){
					insertQuery += "'";
					insertQuery += timeStampOperation((Instant)field);
				}
				else {
					insertQuery += field.toString();
				}
				
				if (isString || isTimeStamp){
					insertQuery += "'";
					insertQuery += ", ";
				}else {
					insertQuery += ", ";
				}
				
				names += attribute.getId() + ",";
			}
			
			try {
				String insertingQuery="INSERT INTO  " + tableName
						+ "(" + names + " time) " + "VALUES ("+insertQuery +"'"+ out +"'"+ ");";
				cmd.executeUpdate(insertingQuery);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}

		public void error(Task task, Throwable cause) {
			lock.lock();
			try {
				closeConnection();
		
				handler.error(task, cause);
			} finally {
				lock.unlock();
			}
		}
		
		private void createSchema() {
			String schemaCreationQuery="CREATE DATABASE IF NOT EXISTS " + schemaName;
			//Connetto al database in oggetto, dopo averlo appena creato se non esiste
			try {
				cmd.executeUpdate(schemaCreationQuery);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			this.hostAddr= "jdbc:mysql://localhost/"+schemaName;
			try {
				connect(this.hostAddr);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		public void createTable() {
			
			String createTableQuery="CREATE TABLE IF NOT EXISTS "+tableName
					+" (id INTEGER not NULL  AUTO_INCREMENT, ";
			String type="";
			for (Attribute attribute : attributes) {
				
												
				switch (attribute.getType().getId()) {
				case "INTEGER":
					type="INTEGER";
					break;
				case "FLOAT":
					type="FLOAT";
					break;
				case "BOOLEAN":
					type="BOOLEAN";
					break;
				case "STRING":
					type = "VARCHAR(50)";
					break;
				case "TIMESTAMP":
					type="TIMESTAMP";
					break;
				}
				
				createTableQuery += attribute.getId() + " " + type + ",";
			}
			createTableQuery +="time TIMESTAMP, PRIMARY KEY ( id ))"; 		
			try {
				cmd.executeUpdate(createTableQuery);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		public boolean checkTable(String tableName) {
			// TODO Auto-generated method stub
			
				try {
					ResultSet rs=cmd.executeQuery("SELECT * FROM "+tableName);
					ResultSetMetaData rsMetaData=rs.getMetaData();
					//table has 2 columns more (id and timestamp)
					if ((rsMetaData.getColumnCount()-2)!=attributes.size()){
						return false;
					}
					
					for (Attribute attribute : attributes) {
						//Searching if each attribute exists into the table
						
						for (int i=0; i<rsMetaData.getColumnCount()-2; i++){
						 String columnName = rsMetaData.getColumnName(i);
						 if (attribute.getId()==columnName){
							 //The attribute exists but we have to control also the type
							if (rsMetaData.getColumnTypeName(i)!=
									attribute.getType().getId()){
								return false;
							}
							 continue;
						 	}else{
							 
							 return false;
						 	}
						}
						
					}
					this.setTableName(tableName);
					
					return true;
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			
				
			return true;
				
		}

		
		private void connect(String hostAddr2) throws SQLException {
			//Connection opening and declaration of the Statement
			//If the connection is already open, it closes it and reopen with 
			//new parameters
			if (con!=null){
			con.close();
			this.con = DriverManager.getConnection(hostAddr2, "root", "password");
			this.cmd=con.createStatement();
			}
			else{
				this.con = DriverManager.getConnection(hostAddr2, "root", "password");
				this.cmd=con.createStatement();
			}
		}
		
		private void closeConnection() {
			System.out.println("Cluster is shutting down...");
		      try {
				cmd.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		      try {
				con.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		public void setHandler(TaskHandler handler) {
			this.handler = handler;
		}

		public void setAttributes(Collection<Attribute> attributes) {
			this.attributes = attributes;
		}

		public String getTableName() {
			return tableName;
		}

		public void setTableName(String tableName) {
			this.tableName = tableName;
		}

		
	}
	
	public Timestamp timeStampOperation(Instant timeObj){
		return Timestamp.from(timeObj);
		
		
	}
	
	public class WrongCorrespondenceException extends Exception {
		private static final long serialVersionUID = -8450600048492031096L;

		public WrongCorrespondenceException() {
			super();
		}

		public WrongCorrespondenceException(String message) {
			super(message);
		}

		public WrongCorrespondenceException(String message, Throwable cause) {
			super(message, cause);
		}

		public WrongCorrespondenceException(Throwable cause) {
			super(cause);
		}
	}
	
}
