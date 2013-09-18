import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;

class InputRead
{	
	public static String usrname = null;
	public static String passwd =null;
	public static String support1 =null;
	public static String support2=null;
	public static String support3=null;
	public static String size3=null;
	public static String support4=null;
	public static String size4=null;
	public static String confidence4=null;
	
	public static void inputread()
			throws Exception
	{
		String templine = null;
		String[] temp = null;
		String[] temp1 = null;
			
		BufferedReader reader =new BufferedReader(new FileReader("system.in"));
		
		if((templine=reader.readLine())!=null)
			temp = templine.split(",");
		temp1 = temp[0].split("=");
		usrname = temp1[1].trim();
		temp1 = temp[1].split("=");
		passwd = temp1[1].trim();
		
		if((templine=reader.readLine())!=null)
			temp = templine.split("=");
		temp1 = temp[1].split("%");
		support1 = temp1[0].trim();
		
		if((templine=reader.readLine())!=null)
			temp = templine.split("=");
		temp1 = temp[1].split("%");
		support2 = temp1[0].trim();
		
		if((templine=reader.readLine())!=null)
			temp = templine.split("=");
		temp1 = temp[1].split("%");
		support3 = temp1[0].trim();
		size3 = temp[2].trim();
		
		if((templine=reader.readLine())!=null)
			temp = templine.split("=");
		temp1 = temp[1].split("%");
		support4 = temp1[0].trim();
		temp1 = temp[2].split("%");
		confidence4 = temp1[0].trim();
		size4 = temp[3].trim();
		
	}
}

class CreateTable
{
	public static Statement stmt =null;
	public static int   total=0;
	public static Connection conn=null;
	
	public static void createtable()
		throws Exception
	{
		 DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
		    conn =
		      DriverManager.getConnection ("jdbc:oracle:thin:hr/hr@oracle1.cise.ufl.edu:1521:orcl",
		    		  InputRead.usrname, InputRead.passwd);
		    stmt = conn.createStatement();
		    String tempString = null;
		    String[] result;
		    // build table trans and load data
		    stmt.execute("create table trans(transid char(20), itemid char(20))");
		    BufferedReader reader =new BufferedReader(new FileReader("trans.dat"));
		    while((tempString = reader.readLine()) != null)
		    {
			   result = tempString.split(",");
			   stmt.execute("insert into trans(transid,itemid) values("+result[0]+","+result[1]+")");
		    }
		    reader.close();
		  // build table items and load data 
		    stmt.execute("create table items(itemid char(20), itemname char(50))");
		    reader =new BufferedReader(new FileReader("items.dat"));
		    while ((tempString = reader.readLine()) != null)
		    {
			   result = tempString.split(",");
			   stmt.execute("insert into items(itemid,itemname) values("+result[0]+","+result[1]+")");
		    }
		    reader.close();
		     
		    ResultSet rset = stmt.executeQuery("select count(distinct transid) from trans");
		    rset.next (); 
		    total=rset.getInt(1);
		   
	}
	
	public static void task(int support, int size,int conf,int task4, String filename)
			throws Exception
	{  
	   float ratio=0;
	   ResultSet rset = null;
	   FileWriter fw = new FileWriter(filename);
		 
	   for(int i=1; i <= size; i++)
	   { 
		   if(i==1)
		   { 
			   stmt.execute(" create table FIset1(isetid char(20), itemid char(20), cnt char(20) )");
			   rset=stmt.executeQuery(" select itemid, count(*) from trans "+
			   						  " group by itemid having count(*)>=0.01*"+support+"*"+total);
					   							
			   ArrayList<String> set1 = new ArrayList<String>();
			   ArrayList<String> set5 = new ArrayList<String>();
			   while(rset.next())
			   {	   
				   set1.add(rset.getString(1));
				   set5.add(rset.getString(2));
			   }
			   for(int m=0; m<set1.size();m++)	
				   stmt.execute("insert into FIset1 values("+ m +","+ set1.get(m)+","+set5.get(m)+")");
			   
			   if(task4!=4)
			   {
				   rset = stmt.executeQuery( "select i.itemname, count(t.transid) " +
											 "from trans t, items i where t.itemid=i.itemid "+
											 "group by i.itemid, i.itemname "+
											 "having count(t.transid)>= 0.01*"+support+	 
											 " * (select count(distinct transid) from trans) "+
											 "order by count(t.transid) desc"); 
				   String s1=null;
				   while (rset.next ())
				   {
					    ratio=(float)rset.getInt(2)*100/(float)total;
				 	    s1 = "{"+rset.getString(1).trim()+"},s="+ String.valueOf(ratio)+"%\n";  
				 	    fw.write(s1,0,s1.length());  
				 	    fw.flush();
				   }
			   }
		   }
		   
		   if(i>1)
		   {
			   //choose two different  iset to combine
			   stmt.execute("create table FIset"+i+"(isetid char(20), itemid char(20), cnt char(20) )");
			   rset=stmt.executeQuery("select distinct IS1.isetid, IS2.isetid "+
					   			     " from FIset"+(i-1)+"  IS1, FIset"+(i-1)+"  IS2 "+
					   				 " where IS1.isetid<IS2.isetid and "+i+"="+
					   			     " (select count(distinct itemid) from FIset"+(i-1)+" IS3 "+
					   				 " where IS1.isetid=IS3.isetid or IS2.isetid=IS3.isetid)");
		   
			   int setid=0;
			   ArrayList<String> set2=new ArrayList<String>();
			   ArrayList<String> set3=new ArrayList<String>();
			   ArrayList<String> candidate=new ArrayList<String>();
			 			   
			   while(rset.next())
			   {
				   set2.add(rset.getString(1));
				   set3.add(rset.getString(2));		   
			   }
			   for(int t=0; t<set2.size();t++)
			   {			   
				   stmt.execute("create table temp(itemid char(20))");
				   stmt.executeQuery("insert into temp(itemid)"+
						   			 "select distinct f.itemid from  FIset"+(i-1)+" f "+
				   					 " where f.isetid="+set2.get(t)+" or f.isetid="+set3.get(t)+"  order by f.itemid");
				  
				   ResultSet temp = stmt.executeQuery("select * from temp");  // what these items are
				   String item=null;
				   while(temp.next())
					   item += temp.getString(1)+",";  //combine to a string
				   
				   int flag=0;
				   for(int j=0; j< candidate.size();j++)
				   {
					   if(item.equals(candidate.get(j)))
					   {
						   flag=1;	// if find the same combination, break and do nothing
						   break;					   
					   }  				   
				   }
				   if(flag==0)
					   candidate.add(item);  //no one is same with it, insert it
				   else 
				   {
					   stmt.execute("drop table temp");
					   continue;
				   } 
				   
				   temp= stmt.executeQuery( " select count(*) "+
						   			 		" from (select f.isetid "+
						   			        " from temp, FIset"+(i-1)+" f "+
						   			        " where temp.itemid=f.itemid "+
						   			        " group by f.isetid having count(*)="+(i-1)+")");
				   temp.next();	
				   
				   if(temp.getInt(1)!=i)  //if the return num is not equal to i, it is not FIs, do nothing
				   {
					   stmt.execute("drop table temp");
					   continue;
				   } 
				   else 
				   {	// the frequency of the combination
					   ArrayList<String> array=new ArrayList<String>();
					   temp = stmt.executeQuery("select * from temp");
					   while(temp.next())
						   array.add(temp.getString(1));
					   
					   String intersect="";
					   for(int k=0; k< array.size()-1; k++)
						   intersect+=" select TransID from Trans where ItemID="+array.get(k)+" intersect ";
					   intersect+=" select TransID from Trans where ItemID="+array.get(array.size()-1);
					   temp = stmt.executeQuery("select count(*) from ("+intersect+")");
					   temp.next();
					   
					   int fre=0;
					   fre=temp.getInt(1);
					   if(fre>= support*total*0.01)//if satisfy, insert it and print it  Integer.parseInt(InputRead.support3)
					   {
						   temp = stmt.executeQuery("select * from temp");
						   ArrayList<String> set4=new ArrayList<String>();
						   while(temp.next())
							   set4.add(temp.getString(1));
						   for(int p=0;p<set4.size();p++)
							   stmt.execute("insert into FIset"+i+"(isetid, itemid, cnt) "+
								   			"values("+ setid+","+set4.get(p)+","+fre+")");
						   setid++;
						   
						   if(task4!=4)
						   {   
							   String itemname="{";
							   for(int n=0;n<set4.size();n++)
							   {
								   temp=stmt.executeQuery("select itemname from items where itemid="+set4.get(n));
								   temp.next();
								   itemname+=temp.getString(1).trim()+",";
							   }
							   itemname+=",";
							   itemname= itemname.replaceFirst(",,", "},s=");
							   ratio= (float)fre*100/(float)total;
							   itemname+=ratio+"%\n";
							   fw.write(itemname,0,itemname.length());  
						 	   fw.flush();
						   }
					   } 	
					   stmt.execute("drop table temp");
					
				   }	  
			   }
		   }
	   }
	   
	   if(task4==4)
	   {	   
		   for(int i=1;i<size;i++)
		   {
			   rset=stmt.executeQuery(" select count(distinct isetid) from FIset"+i); // loop how many times, left-hand items
			   rset.next();
			   int count= rset.getInt(1);
			   for(int q=0; q< count; q++)
			   {	
				   rset=stmt.executeQuery(" select * from FIset"+i+" where isetid="+q);
				   ArrayList<String[]> set1 = new ArrayList<String[]>();
				   while(rset.next())
				   {
					   String[]  temp = new String[3];
					   temp[0]= rset.getString(1);
					   temp[1]= rset.getString(2);
					   temp[2]= rset.getString(3);
					   set1.add(temp);
				   }
				   
				   for(int j=i+1; j<=size; j++)
				   {	   
					   rset=stmt.executeQuery(" select count(distinct isetid) from FIset"+j);
					   rset.next();
					   int num=rset.getInt(1);
					   for(int k=0; k< num; k++)
					   {
						   rset=stmt.executeQuery(" select * from FIset"+j+ "  where isetid="+k);
						   ArrayList<String[]> set2 = new ArrayList<String[]>();
						   while(rset.next())
						   {
							   String[]  temp1 = new String[3];
							   temp1[0]= rset.getString(1);
							   temp1[1]= rset.getString(2);
							   temp1[2]= rset.getString(3);
							   set2.add(temp1);
						   }
						   int flag=0;
						   for(int m=0; m< set1.size(); m++)
						   {			
							   for(int n=0; n<set2.size(); n++)
							   {
								   if(set1.get(m)[1].equals(set2.get(n)[1]))
								  		flag++;
							   }
						   }
						  					   
						   if(flag==i)
						   {
							   int t1=Integer.parseInt(set2.get(0)[2].trim());
							   int t2=Integer.parseInt(set1.get(0)[2].trim());
							   ratio=(float)t1*100/(float)t2;
							   if(ratio>= (float)conf)
							   {
								   String output="{{";
								   for(int m=0; m< set1.size(); m++)
								   {				
									   rset=stmt.executeQuery("select itemname from items where itemid="+set1.get(m)[1]);
									   rset.next();
									   output+=rset.getString(1).trim()+",";
								   }
								   output+=",";
								   output=output.replaceFirst(",," , "}->{");
								   
								   int flag1=0;
								   for(int n=0; n< set2.size(); n++)
								   {
									   flag1=0;
									   for(int m=0; m<set1.size(); m++)
									   {
										   if(set2.get(n)[1].equals(set1.get(m)[1]))
										   {
											   flag1++;
										   }
									   }
									   if(flag1==0)
									   {
										   rset=stmt.executeQuery("select itemname from items where itemid="+set2.get(n)[1]);
										   rset.next();
										   output+=rset.getString(1).trim()+",";
									   }
								   }
								   output+=",";
								   output=output.replaceFirst(",," , "}},s=");
								   int t3=Integer.parseInt(set2.get(0)[2].trim());
								   output+=(float)t3*100/(float)total;
								   output+="%,c="+ratio+"%\n";
								   fw.write(output,0,output.length());  
							 	   fw.flush();
							   }
						   }
					   }
				   }
			   }
		   }
	   }
	   	   
	   for(int i=1;i<=size;i++) // drop all the FIset tables
		   stmt.execute("drop table Fiset"+i);
	   
	}	
	
	public static void endsql()
			throws Exception
	{
		 stmt.execute("drop table trans");
		 stmt.execute("drop table items");
		 conn.close();
	}
	
}


class arm
{  
  public static void main (String args [])
       throws Exception
  {
	  InputRead.inputread();
	 
	  CreateTable.createtable();
	  
	  CreateTable.task(Integer.parseInt(InputRead.support1), 1, 0, 0, "system.out.1");
	  
	  CreateTable.task(Integer.parseInt(InputRead.support2), 2, 0, 0, "system.out.2");
	  
	  CreateTable.task(Integer.parseInt(InputRead.support3), Integer.parseInt(InputRead.size3),  0, 0, "system.out.3");
	  
	  CreateTable.task(Integer.parseInt(InputRead.support4), Integer.parseInt(InputRead.size4), Integer.parseInt(InputRead.confidence4), 4, "system.out.4");
	 
	  CreateTable.endsql();
	  
  }
}
