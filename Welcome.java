//package project1;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.sql.Statement;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.Month;
import java.time.temporal.IsoFields;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.sql.*; 

	
class Welcome{  
	
	public static  Boolean findinHashTable(String transaction_id,List<Map<String,String>> HashTable)
	{
		for(Map<String,String> h:HashTable)
		{
			if(h.get("TRANSACTION_ID").equals(transaction_id))
			{
				return true;
			}
			
		}
		return false;
	}
	public static  List<Map<String,String>> getStreamData(ResultSet rs) throws SQLException
	{
		List<Map<String,String>> st =new ArrayList<Map<String,String>>();
		int counter=0;
		while(rs.next())
		{
			
			Map<String,String> data = new HashMap<String,String>();
			data.put("TRANSACTION_ID", Integer.toString(rs.getInt("TRANSACTION_ID")));
			data.put("PRODUCT_ID", rs.getString("PRODUCT_ID"));
			
			data.put("CUSTOMER_ID", rs.getString("CUSTOMER_ID"));
			data.put("CUSTOMER_NAME", rs.getString("CUSTOMER_NAME"));
			data.put("QUANTITY", rs.getString("QUANTITY"));
			data.put("T_DATE", rs.getString("T_DATE"));
			data.put("STORE_ID", rs.getString("STORE_ID"));
			data.put("STORE_NAME", rs.getString("STORE_NAME"));
			st.add(data);
			counter++;
		}
		return st;
	}
	
	public static  List<Map<String,String>> getMasterData(ResultSet rs) throws SQLException
	{
		List<Map<String,String>> st =new ArrayList<Map<String,String>>();
		int counter=0;
		while(rs.next())
		{
			
			Map<String,String> data = new HashMap<String,String>();
			data.put("PRODUCT_ID", rs.getString("PRODUCT_ID"));
			data.put("PRODUCT_NAME",rs.getString("PRODUCT_NAME"));
			data.put("SUPPLIER_ID",rs.getString("SUPPLIER_ID"));
			data.put("SUPPLIER_NAME",rs.getString("SUPPLIER_NAME"));
			data.put("PRICE",rs.getString("PRICE"));
			
			st.add(data);
			counter++;
		}
		return st;
	}
	public static Map<String,String> TransformDate(String date)
	{
		

		
		LocalDate currentDate= LocalDate.parse(date);
		int day = currentDate.getDayOfMonth();
		DayOfWeek day1 = currentDate.getDayOfWeek();
		Month month = currentDate.getMonth();
		int quarter = currentDate.get(IsoFields.QUARTER_OF_YEAR);
		int year = currentDate.getYear();
		Map<String,String> data = new HashMap<String,String>();
		data.put("Day",Integer.toString(day));
		data.put("WEEKDAY",day1.toString());
		data.put("MONTH",month.toString());
		data.put("QUARTER",Integer.toString(quarter));
		data.put("YEAR",Integer.toString(year));
		
		return data;
	}
	
public static void main(String args[]) throws SQLException, ClassNotFoundException{  	

		Class.forName("com.mysql.cj.jdbc.Driver");  
		Connection con=DriverManager.getConnection(  
				"jdbc:mysql://localhost:3306/metrodb","root","metro");  
		// metrodb is database name, root is username and password metro
		Statement stmt=con.createStatement();
		Statement md=con.createStatement();
		
		Connection con2=DriverManager.getConnection(  
				"jdbc:mysql://localhost:3306/metrowh","root","metro"); 
		
		// metrodb is database name, root is username and password metro
		Statement stmt2=con2.createStatement();
					
		List<Map<String,String>> queue = new ArrayList<Map<String,String>>();
		int count=0;
		ArrayBlockingQueue<List<Map<String,String>>> queue1 = new ArrayBlockingQueue<List<Map<String,String>>>(10);
		List<Map<String,String>> hashtable = new ArrayList<Map<String,String>>();
		
		int MDCounter=0;
		while(true)
		{
			ResultSet rss=md.executeQuery("select * from masterdata limit "+count*10+","+(count+1)*10);
			if(count == 10)
				{count=0;
				break;
				}

			MDCounter++;
			queue1.add(getMasterData(rss));
			count++;
		}
		
		
		
		
		int i=0;
		int counter=0;
		int trannsactioncounter=0;
		//while(true)
		{
			ResultSet rs=stmt.executeQuery("select * from transactions");
			
			//System.out.println("ST  SIZE: "+st.size());
			queue=getStreamData(rs);
				
				int que1count=0;
				
				int matchcount=0;
				for(List<Map<String,String>> q:queue1)
				{int que1countv=0;
					for(Map<String,String> m: q)
					{
						que1countv++;
						
						int stcount=0;
						for(Map<String,String> s:queue)
						{
							stcount++;
							//System.out.println(s);
							if(Objects.equals(m.get("PRODUCT_ID"),(s.get("PRODUCT_ID"))))
							{
								
								if(!findinHashTable(m.get("TRANSACTION_ID"),hashtable))
								{
									matchcount++;
									hashtable.add(s);
									trannsactioncounter++;
									
									
									String Product1="Insert Ignore Into PRODUCT(Product_ID,Product_Name) values (?,?)";
									PreparedStatement p1 = con2.prepareStatement(Product1);
									p1.setString(1, (String) m.get("PRODUCT_ID"));
									p1.setString(2, (String) m.get("PRODUCT_NAME"));
									p1.executeUpdate();
									
									String Product2="Insert Ignore Into CUSTOMER(CUSTOMER_ID,CUSTOMER_Name) values (?,?)";
									PreparedStatement a = con2.prepareStatement(Product2);
									a.setString(1,  s.get("CUSTOMER_ID"));
									a.setString(2,  s.get("CUSTOMER_NAME"));
									a.executeUpdate();
									
									String Product3="Insert Ignore Into STORE(STORE_ID,STORE_Name) values (?,?)";
									PreparedStatement b = con2.prepareStatement(Product3);
									b.setString(1, s.get("STORE_ID"));
									b.setString(2, s.get("STORE_NAME"));
									b.executeUpdate();
									
									String Product4="Insert Ignore Into SUPPLIER(SUPPLIER_ID,SUPPLIER_Name) values (?,?)";
									PreparedStatement c = con2.prepareStatement(Product4);
									c.setString(1, (String) m.get("SUPPLIER_ID"));
									c.setString(2, (String) m.get("SUPPLIER_NAME"));
									c.executeUpdate();
									
									Map<String,String> DateList=TransformDate(s.get("T_DATE"));
									String Product5="Insert Ignore Into DATE_TIME(Time_Id,Day,Week,Month,Quater,Year) values (?,?,?,?,?,?)";
									PreparedStatement d = con2.prepareStatement(Product5);
									d.setString(1, (String) s.get("T_DATE"));
									d.setLong(2, Integer.parseInt( DateList.get("Day")));
									d.setString(3, (String) DateList.get("WEEKDAY"));
									d.setString(4, (String) DateList.get("MONTH"));
									d.setLong(5, (Integer.parseInt(DateList.get("QUARTER"))));
									d.setLong(6, (Integer.parseInt(DateList.get("YEAR"))));
									d.executeUpdate();
									
									String Product6="Insert Ignore Into fact_table (Transaction_ID,PRODUCT_ID,CUSTOMER_ID,STORE_ID,SUPPLIER_ID,Time_Id,Total_Sale) values (?,?,?,?,?,?,?)";
									PreparedStatement e = con2.prepareStatement(Product6);
									e.setLong(1, Integer.parseInt( s.get("TRANSACTION_ID")));
									e.setString(2, (String) m.get("PRODUCT_ID"));
									e.setString(3, (String) s.get("CUSTOMER_ID"));
									e.setString(4, (String) s.get("STORE_ID"));
									e.setString(5, (String) m.get("SUPPLIER_ID"));
									e.setString(6, (String) s.get("T_DATE"));
									e.setFloat(7, (Integer.parseInt(s.get("QUANTITY"))*(Float.parseFloat(m.get("PRICE")))));
									e.executeUpdate();
								}
								
							}
							
						}
					}
					
							
					}
				
			
			
			
			
			
			i++;
			
		}
		
		System.out.println("\n\n\n");
		/*for(List<Map<String,String>> q:queue1)
		{
			System.out.println(q.get(0).get("PRODUCT_ID"));
		}
		
		
		for(Map<String,String> h:hashtable)
		{
			System.out.println(h);
		}*/
		int c=0;
		for(Map<String,String> q:queue)
		{
			//for(Map<String,String> m: q)
			if(!findinHashTable(q.get("TRANSACTION_ID"),queue))
				c++;
		}
		System.out.println("MD COUNTER: "+MDCounter);
		System.out.println("QUEUE SIZE: "+c);
		System.out.println(queue.size());
		System.out.println(hashtable.size()+"  Hash Table:\n\n\n");
		System.out.println(counter+"   Counter:\n\n\n");
		System.out.println(trannsactioncounter+"   trannsactioncounter:\n\n\n");
		con.close();
		con2.close();
		
}
}		
			

			
			
	
