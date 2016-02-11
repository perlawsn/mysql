package org.dei.perla.fpc.mysql;

import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.*;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.dei.perla.core.channel.ChannelFactory;
import org.dei.perla.core.channel.IORequestBuilderFactory;
import org.dei.perla.core.channel.simulator.SimulatorChannelFactory;
import org.dei.perla.core.channel.simulator.SimulatorIORequestBuilderFactory;
import org.dei.perla.core.channel.simulator.SimulatorMapperFactory;
import org.dei.perla.core.fpc.DataType;
import org.dei.perla.core.descriptor.DeviceDescriptor;
import org.dei.perla.core.descriptor.JaxbDeviceDescriptorParser;
import org.dei.perla.core.fpc.Fpc;
import org.dei.perla.core.fpc.FpcFactory;
import org.dei.perla.core.fpc.Task;
import org.dei.perla.core.fpc.base.BaseFpcFactory;
import org.dei.perla.core.message.MapperFactory;
import org.dei.perla.core.fpc.Attribute;
import org.dei.perla.core.fpc.Sample;
import org.dei.perla.fpc.mysql.MySqlWrapper.WrongCorrespondenceException;
import org.junit.BeforeClass;
import org.junit.Test;

public class PeriodicTest {
	
	private static final String descriptorPath = "src/test/java/org/dei/perla/fpc/mysql/fpc_descriptor.xml";
	private static Fpc fpc;
	private static String schemaName;

	@BeforeClass
	public static void createFpc() throws Exception {
		List<String> packageList = new ArrayList<>();
		packageList.add("org.dei.perla.core.descriptor");
		packageList.add("org.dei.perla.core.descriptor.instructions");
		packageList.add("org.dei.perla.core.channel.simulator");
		JaxbDeviceDescriptorParser parser = new JaxbDeviceDescriptorParser(
				(Set<String>) packageList);

		List<MapperFactory> mapperFactoryList = new ArrayList<>();
		mapperFactoryList.add(new SimulatorMapperFactory());
		List<ChannelFactory> channelFactoryList = new ArrayList<>();
		channelFactoryList.add(new SimulatorChannelFactory());
		List<IORequestBuilderFactory> requestBuilderFactoryList = new ArrayList<>();
		requestBuilderFactoryList.add(new SimulatorIORequestBuilderFactory());
		FpcFactory fpcFactory = new BaseFpcFactory(mapperFactoryList,
				channelFactoryList, requestBuilderFactoryList);

		DeviceDescriptor desc = parser
				.parse(new FileInputStream(descriptorPath));
		fpc = fpcFactory.createFpc(desc, 1);
		schemaName = "FpcSchema" + fpc.getId();
	}

	@Test
	public void testPeriodicGetDatabaseOperation()
			throws InterruptedException, ExecutionException,
			WrongCorrespondenceException {

		List<Attribute> attributeList;
		Sample record;

		// Request string and integer
		attributeList = new ArrayList<>();
		System.out.println("post breakpoint");
		attributeList.add(Attribute.create("string", DataType.STRING));
		attributeList.add(Attribute.create("intero", DataType.INTEGER));

		LatchingTaskHandler handler1 = new LatchingTaskHandler(100);
		MySqlWrapper dbWrapper = new MySqlWrapper(fpc);
		Task task1 = dbWrapper.get(attributeList, 10, handler1, null);

		String tableName = ((MySqlTask) task1).getTableName();
		
		
		ArrayList<Sample> records = handler1.getAllRecords();
		
		System.out.println("------------------Display table-------------------------");
		displayTable(tableName);
		
		boolean check = checkInDatabase(attributeList, records, tableName);
		assertTrue(check);
		assertThat(task1, notNullValue());
		assertTrue(task1 instanceof MySqlTask);
		record = handler1.getLastRecord();
		assertThat(record, notNullValue());
		assertThat(record.getValue("string"), notNullValue());
		assertTrue(record.getValue("string") instanceof String);
		assertThat(record.getValue("intero"), notNullValue());
		assertTrue(record.getValue("intero") instanceof Integer);
		// Check if the Fpc is adding the timestamp
		assertThat(record.getValue("timestamp"), notNullValue());
		assertTrue(record.getValue("timestamp") instanceof Instant);
		
	}
	
	
	private void displayTable(String tableName) {
		 String url = "jdbc:mysql://localhost";
	     Connection con = null;
	     Statement cmd = null;
		 try {
			
			 con=DriverManager.getConnection(url+"/"+schemaName, "root", "francesco89");
			 cmd=con.createStatement();
		ResultSet results = cmd.executeQuery("SELECT * FROM " + tableName);
		System.out.println("------SCHEMA NAME------:   " + schemaName
				+ "            ------TABLE NAME------:   " + tableName);

		ArrayList<String> attrs = new ArrayList<String>();
		String trattini = " ";
		String formats = " ";

		ResultSetMetaData rsMetaData=results.getMetaData();
		
		int columnCount=rsMetaData.getColumnCount();
		for (int i=1; i<columnCount; i++) {
			String columnName=rsMetaData.getColumnName(i);
			attrs.add(columnName);
			trattini += "---------------------------------------+";
			formats += "%-35s\t";
		}
      attrs.add(trattini);
		System.out.println(String.format(formats + "\n%s",
				 attrs.toArray()));

		int totalRows=0;
		
			while (results.next()){

				ArrayList<String> attrsValues = new ArrayList<String>();
				for (int i=1; i<rsMetaData.getColumnCount(); i++) {
				
					
					String val = "";
					if (rsMetaData.getColumnTypeName(i).equalsIgnoreCase("float")) {
						val = results.getFloat(rsMetaData.getColumnTypeName(i)) + "";
					} else if (rsMetaData.getColumnTypeName(i).equalsIgnoreCase("integer")) {
						val = results.getInt(rsMetaData.getColumnTypeName(i)) + "";
					} else if (rsMetaData.getColumnTypeName(i).equalsIgnoreCase("string")) {
						val = results.getString(rsMetaData.getColumnTypeName(i));
					} else if (rsMetaData.getColumnTypeName(i).equalsIgnoreCase("boolean")) {
						val = results.getBoolean(rsMetaData.getColumnTypeName(i)) + "";
					} else if (rsMetaData.getColumnTypeName(i).equalsIgnoreCase("date")) {
					    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");			        
				        Date date = results.getDate(rsMetaData.getColumnTypeName(i));			      
						val = sdf.format(date);
					} else if (rsMetaData.getColumnTypeName(i).equalsIgnoreCase("uuid")) {
						val = results.getRowId(rsMetaData.getColumnTypeName(i)) + "";
					}
					attrsValues.add(val);
				}
				System.out.println(String.format(formats,
						attrsValues.toArray()));
				totalRows++;
				System.out.println("TOTAL ROWS: "+totalRows);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public boolean checkInDatabase(Collection<Attribute> attributes,
			Sample record, String tableName) {
			//connect(schemaName);
		 String url = "jdbc:mysql://localhost";
	     Connection con = null;
	     Statement cmd = null;
		Instant timeObj = (Instant) record.getValue("timestamp");

		Date date = Date.from(timeObj);

		HashMap<String, DataType> attributeNamesTypes = new HashMap<String, DataType>();
		for (Attribute attribute : attributes) {
			attributeNamesTypes.put(attribute.getId(), attribute.getType());
		}
	
		 try {
				con=DriverManager.getConnection(url+"/"+schemaName, "root", "francesco89");
				
				cmd=con.createStatement();
			 ResultSet results = cmd.executeQuery("SELECT * FROM " + tableName);
			 test: while (results.next()){
			Date rowDate = results.getDate("time");
			long time1 =rowDate.getTime();
			long time2 = results.getDate("time").getTime();
			if(time1!=time2) {
				continue;
			}
			for (Attribute a : record.fields()) {
				String name = a.getId();

				DataType type = attributeNamesTypes.get(name);
				if (DataType.FLOAT == type) {
					float val = results.getFloat(name);
					if (!record.getValue(name).equals(val))
						continue test;					
				} else if (DataType.INTEGER == type) {
					int val = results.getInt(name);		
					if (!record.getValue(name).equals(val))
						continue test;
				} else if (DataType.STRING == type) {
					String val = results.getString(name);
					if (!record.getValue(name).equals(val))
						continue test;
				} else if (DataType.BOOLEAN == type) {
					boolean val = results.getBoolean(name);
					if (!record.getValue(name).equals(val))
						continue test;
				}
				}
			return true;
		}
	}catch (SQLException e){
	      e.printStackTrace();
	    }

		return false;
	}

	public boolean checkInDatabase(Collection<Attribute> attributes,
			ArrayList<Sample> allRecords, String tableName)
			throws WrongCorrespondenceException {
		int count = 0;
		for (Sample record : allRecords) {			
			if (!checkInDatabase(attributes, record, tableName))		
				return false;			
			count++;			
		}
		System.out.println("TOTAL RECORS CHECKED: "+count);
		return true;

	}

	

}
