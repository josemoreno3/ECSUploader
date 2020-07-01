package com.softcatalunya.TNT.ECS;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Properties;
import java.util.ResourceBundle;

import com.ibm.as400.access.AS400;
import com.ibm.as400.access.CharacterDataArea;
import com.ibm.as400.access.QSYSObjectPathName;
    
public class ECSUploaderDaemon {

	private HashMap config = new HashMap();

	private Connection jdbcCon = null;

	private AS400 system = null;

	private CharacterDataArea dataAra = null;

	private PreparedStatement stat = null;
	
	private Properties properties=null;
	
	final String version="v2.0";
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {

		ECSUploaderDaemon.redirectSystemOut();
		
		ECSUploaderDaemon ECSLoader = new ECSUploaderDaemon();

		ECSLoader.loadProperties();
		
		ECSLoader.inicializa();

		ECSLoader.doIt();

	}

	

	

	private void loadProperties() {
		 
	    properties = new Properties();
	    try {
	    	System.out.println("Voy a leer las properties");
	        properties.load(new FileInputStream("/home/scclasses/ECSUploader.properties"));
	        Enumeration keys=properties.keys();
	        
	        while (keys.hasMoreElements()) {
	        	  String key = (String)keys.nextElement();
	        	  String value = (String)properties.get(key);
	        	  System.out.println(key + ": " + value);
	        	}       
	        
	    } catch (IOException e) {
	    }
	}





	public PreparedStatement getStat() {
		return stat;
	}

	public void setStat(PreparedStatement stat) {
		this.stat = stat;
	}

	private void doIt() {

		try {
			System.out.println("Voy a leer del ·rea de datos.");
			boolean stop = getDataAra().read().equals("STOP");
			while (!stop) {
				Thread.sleep(new Integer("30000").intValue());

				this.JMSMsgs();

				stop = getDataAra().read().equals("STOP");
			}
			getJdbcCon().close();
		} catch (Exception e) {
			// TODO Bloque catch generado autom√°ticamente
			e.printStackTrace();
			System.exit(0);
		}

	}

public void JMSMsgs	() {
		// Obtenemos statement de la conexion y hacemos la consulta
		Statement stmt;
		PreparedStatement pstmt;
		String query;
		String update;
		ResultSet rs;
		BufferedWriter out;
		
		try {
			stmt = getJdbcCon().createStatement();
			query = "select * from SCEDI.ECSPEND where estado = '"
					+ "*PEND' order by conid";
			rs = stmt.executeQuery(query);

			while (rs.next()) {
				try {
						
					System.out.println("obtaining property " +rs.getString("depot"));
					
					System.out.println("obtaining property " +
							getProperties().getProperty(rs.getString("depot").trim()).trim());
					
					if (rs.getString("IDMSG").trim().equals("DAE"))
					{
						if (rs.getString("CONID").trim().length()==9) {
							System.out.println("/home/scclasses/ECS_out/"+getProperties().getProperty(rs.getString("depot").trim()).trim()+ "/" + rs.getString("conid").trim()
							+ ".ECS");
							out = new BufferedWriter(new FileWriter(
									"/home/scclasses/ECS_out/"+getProperties().getProperty(rs.getString("depot").trim()).trim()+ "/" + rs.getString("conid").trim()
									+ ".ECS", true));
							String str = rs.getString("depot") + rs.getString("comid")
								+ rs.getString("conid") + rs.getString("sequen")
								+ rs.getString("EMRN")+"\n";
							out.write(str);
							out.close();
						}
							//	si no lo han informado, lo logeamos pero no lo tratamos.
						else 
						{
							System.out.println("Detectado envÌo sin informar connote: "+rs.getString("depot").trim()+ "/"+ rs.getString("EMRN").trim()+". Connote: "+rs.getString("CONID").trim()); 
						}
					}
					
					if (rs.getString("IDMSG").trim().equals("EAL"))
					{
						// si han informado el connote, lo subimos
						if (rs.getString("CONID").trim().length()==9) {
							System.out.println("/home/scclasses/EAL_out/"+getProperties().getProperty(rs.getString("depot").trim()).trim()+ "/" + rs.getString("conid").trim()
										+ ".ECS");
							out = new BufferedWriter(new FileWriter(
										"/home/scclasses/EAL_out/"+getProperties().getProperty(rs.getString("depot").trim()).trim()+ "/" + rs.getString("conid").trim()
										+ ".ECS", true));
							String str = rs.getString("depot") + rs.getString("comid")
											+ rs.getString("conid") + rs.getString("sequen")
											+ rs.getString("EMRN")+ rs.getString("replay")+"\n";
							out.write(str);
							out.close();
						}
						// si no lo han informado, lo logeamos pero no lo tratamos.
						else 
						{
							System.out.println("Detectado envÌo sin informar connote: "+rs.getString("depot").trim()+ "/"+ rs.getString("EMRN").trim()+". Connote: "+rs.getString("CONID").trim()); 
						}
					}					
					
					update = "update SCEDI.ECSPEND set estado = '*OK', fecpro=? where conid=? and comid=? and depot=? and sequen=? and emrn=?";
					
					System.out.println("New record inserted: ");
					System.out.println("Conid: " + rs.getString("conid"));
					System.out.println("Comid: " + rs.getString("comid"));
					System.out.println("Depot: " + rs.getString("depot"));
					System.out.println("Sequen: " + rs.getString("sequen"));
					System.out.println("emrn: " + rs.getString("emrn"));
					System.out.println("tipo: " + rs.getString("idmsg"));
					
					pstmt = getJdbcCon().prepareStatement(update);
					pstmt.setString(1, getCurrentTimeStamp());
					pstmt.setString(2, rs.getString("conid")); 
				    pstmt.setString(3, rs.getString("comid")); 
				    pstmt.setString(4, rs.getString("depot")); 
				    pstmt.setString(5, rs.getString("sequen"));
				    pstmt.setString(6, rs.getString("emrn"));
				    
				    pstmt.execute();
				    pstmt.close();
					
				} catch (IOException e) {
					// TODO Bloque catch generado autom·ticamente
					e.printStackTrace();
				}

			}
			stmt.close();

		} catch (SQLException e) {
			// TODO Bloque catch generado autom·ticamente
			e.printStackTrace();
		}
	}

	public static void redirectSystemOut() {
		File f;

		try {
			Date d1 = new Date();
			java.text.DateFormat sdf = new java.text.SimpleDateFormat(
					"yyyyMMddHHmm");

			String fichero = "/home/scclasses/ECSlog" + sdf.format(d1);

			f = new File(fichero);
			if (!f.exists())
				f.createNewFile();

			System.setOut(new PrintStream(new FileOutputStream(fichero)));

		} catch (Exception ex) {
			ex.printStackTrace();
		}

	}

	private void inicializa() {
		// TODO Ap√©ndice MANUEL de m√©todo generado autom√°ticamente

		try {
						
			//Class.forName("com.ibm.as400.access.AS400JDBCDriver");
			Class.forName(properties.getProperty("driver"));
			
			//setJdbcCon(DriverManager.getConnection("jdbc:as400://164.39.54.43",
			//		"QEADMIN", "QEADMIN"));
			setJdbcCon(DriverManager.getConnection(properties.getProperty("jdbcurl"),properties.getProperty("user"), properties.getProperty("password")));
			
			
			//system = new AS400("164.39.54.43", "QEADMIN", "QEADMIN");
			system = new AS400(properties.getProperty("server"), properties.getProperty("user"), properties.getProperty("password"));
			
			//QSYSObjectPathName path = new QSYSObjectPathName("SCEDI",
			//		"ECSDMN", "DTAARA");
			QSYSObjectPathName path = new QSYSObjectPathName(properties.getProperty("DtaAraLib"),properties.getProperty("DtaAraName"), "DTAARA");
			
			setDataAra(new CharacterDataArea(system, path.getPath()));
			System.out.println("Sistema: " +system.getSystemName());
			System.out.println("Release: " +system.getVRM());
			System.out.println("Usuario: " +system.getUserId());
			System.out.println("ECSUploader version: " +this.version);
					
		} catch (Exception e) {
			// TODO Bloque catch generado autom·ticamente
			e.printStackTrace();
		}
	}

	public Connection getJdbcCon() {
		return jdbcCon;
	}

	public void setJdbcCon(Connection jdbcCon) {
		this.jdbcCon = jdbcCon;
	}

	public CharacterDataArea getDataAra() {
		return dataAra;
	}

	public void setDataAra(CharacterDataArea dataAra) {
		this.dataAra = dataAra;
	}

	public AS400 getSystem() {
		return system;
	}

	public void setSystem(AS400 system) {
		this.system = system;
	}

	public HashMap getConfig() {
		return config;
	}

	public void setConfig(HashMap newConfig) {
		config = newConfig;
	}

	public Properties getProperties() {
		return properties;
	}

	public void setProperties(Properties properties) {
		this.properties = properties;
	}
	
	public static String getCurrentTimeStamp() {
	    SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//dd/MM/yyyy
	    Date now = new Date();
	    String strDate = sdfDate.format(now);
	    return strDate;
	}
	
	

}
