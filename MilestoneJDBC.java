package milestone1;
import java.util.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class MilestoneJDBC {

	public static void main(String[] args) throws Exception{
		List<MilestonePojo> inlist=new ArrayList<>();
		try {
	         File file = new File("C:\\Users\\RISHABH\\Downloads\\HRC ka saman\\1806331.csv");
	         FileReader fr = new FileReader(file);
	         BufferedReader br = new BufferedReader(fr);
	         String line = br.readLine();	   
	         line=br.readLine();
	         while(line!= null)
	         {
	        	String[] temparr= line.split(",");;
	        	MilestonePojo obj=new MilestonePojo();
	            obj.setBusiness_code(temparr[0]);
	            obj.setCust_number(temparr[1]);
	            obj.setName_customer(temparr[2]);
	            if(temparr[3]==null || temparr[3].equals(""))
	            {
	            	obj.setClear_date(null);
	            }
	            else
	            {
	            	obj.setClear_date(Timestamp.valueOf(temparr[3]));
	            }
	            Integer kt=(int)Math.round(Double.parseDouble(temparr[4]));
	            obj.setBusiness_year(kt);
	            try { 
	            	Long b=(long)Math.round(Double.parseDouble(temparr[5]));
	            	obj.setDoc_id(b);
	            	}
	            catch(Exception e)
	            {	            	
	            	e.printStackTrace();
	            }
	            if(temparr[6]==null || temparr[6].equals(""))
	            {
	            	obj.setPosting_date(null);
	            }
	            else
	            {
	            	obj.setPosting_date(Date.valueOf(temparr[6]));
	            }
	            obj.setDocument_create_date(temparr[7]);
	            obj.setDocument_create_date1(temparr[8]);
	            obj.setDue_in_date(temparr[9]);
	            obj.setInvoice_currency(temparr[10]);
	            obj.setDocument_type(temparr[11]);
	            Integer mt=(int)Math.round(Double.parseDouble(temparr[12]));
	            obj.setPosting_id(mt);
	            obj.setArea_business(temparr[13]);
	            try {
	            obj.setTotal_open_amount(Math.round(Double.parseDouble(temparr[14])));
	            }
	            catch(Exception e) {
	            	e.printStackTrace();
	            }
	            obj.setBaseline_create_date(temparr[15]);
	            obj.setCust_payment_terms(temparr[16]);
	            if(temparr[17]== null || temparr[17].equals(""))
	    			obj.setInvoice_id(null);
	    		else
	    		{
	    			Long inv=(Long)Math.round(Double.parseDouble(temparr[17]));
	    			obj.setInvoice_id(inv);
	    	     }
	            
	            
	            obj.setIsOpen(Integer.parseInt(temparr[18]));
	            inlist.add(obj);
	            line=br.readLine();
	         }
	         int counter=0;
	         for(MilestonePojo a:inlist)
	         {
	        	 counter++;
	         }
	         System.out.println(counter);
	         
	         br.close();
	         } catch(IOException ioe) {
	            ioe.printStackTrace();
	         }
		
		    DriverManager.registerDriver(new com.mysql.cj.jdbc.Driver());
		    String DB_URL="jdbc:mysql://localhost/h2h_internship";
		    String user="root";
		    String pwd="root";
		    int c=0;
		    try {
		    Connection conn=DriverManager.getConnection(DB_URL,user,pwd);
			 conn.setAutoCommit(false);
			System.out.println("connection stabilish");	
		    PreparedStatement stmt=conn.prepareStatement("INSERT INTO invoice_details (business_code,cust_number, name_customer,clear_date,business_year,doc_id,posting_date,document_create_date,due_in_date,invoice_currency,document_type,posting_id,area_business,total_open_amount,baseline_create_date,cust_payment_terms,invoice_id,isOpen,document_create_date1 ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
		    
		    for(MilestonePojo e : inlist)
            {
				stmt.setString(1, e.getBusiness_code());
				stmt.setString(2, e.getCust_number());
				stmt.setString(3, e.getName_customer());
			    
				if (e.getClear_date() != null) {
					Date date = new java.sql.Date(e.getClear_date().getTime());
					stmt.setDate(4, date);
				} else
					stmt.setDate(4, null);
				
				stmt.setInt(5, e.getBusiness_year());
				stmt.setLong(6, e.getDoc_id());
				
				
				stmt.setDate(7, (Date) e.getPosting_date());
				stmt.setString(8, e.getDocument_create_date());
				stmt.setString(9, e.getDue_in_date());
				stmt.setString(10, e.getInvoice_currency());
				stmt.setString(11, e.getDocument_type());
				stmt.setInt(12, e.getPosting_id());
				stmt.setString(13,e.getArea_business());
				stmt.setDouble(14, e.getTotal_open_amount());
				stmt.setString(15, e.getBaseline_create_date());
				stmt.setString(16, e.getCust_payment_terms());
				if(e.getInvoice_id()!=null)
				{
				stmt.setLong(17, e.getInvoice_id());
				}
				else
				{
				stmt.setNull(17, java.sql.Types.BIGINT);
				}
				stmt.setInt(18, e.getIsOpen());
				stmt.setString(19, e.getDocument_create_date1());
				stmt.addBatch();
				c=c+1;
                System.out.println("number of rows inserted in the database is : " +c);
				}
			stmt.executeBatch();
			 conn.commit();
		        conn.close();
            
		    }
		    catch(SQLException exc)
		    {
		    	exc.printStackTrace();
		    }
			finally {
		    System.out.println("number of rows inserted in the database is : " +c);}
		}
}