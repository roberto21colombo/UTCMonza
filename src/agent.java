import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;

public class agent {

	
	public static void main(String[] args) throws Exception{
		
		//Mi restituisce il resultser contentnete le informazioni giornaliere orarie dei sensori interpellati.
		//Si connette al database di UTC
		ResultSet rsMonzaNew = getResultSetDBMonzaNew();
		
		//Funzione che carica sul db comunale MySql le informazioni contenute nel resultset appena ottenuto
		uploadRStoMySql(rsMonzaNew);
        
	}
	
	public static void uploadRStoMySql(ResultSet rsMonzaNew) throws Exception{
		//Esegue la connessione con il dbMySql
		Connection connect = getConnectionMySql();
        
		//Per ogni riga del resultset dbMonzaNew carica sul db MySql le informaizioni secondo una precisa formattazione
		while (rsMonzaNew.next()) {
			//fare connessione a Dd MySql
			String idSensore = rsMonzaNew.getString(1);
			String cnt = rsMonzaNew.getString(2);
			String ora = rsMonzaNew.getString(3);
			String giorno = rsMonzaNew.getString(4);
			String mese = rsMonzaNew.getString(5);
			String anno = rsMonzaNew.getString(6);
			String gsett = rsMonzaNew.getString(7);
			//System.out.println(idSensore + " - " + cnt + " - " + ora + " - " + giorno + " - " + mese + " - " + anno + " - " + gsett);
			String insertQuery = "INSERT INTO dati VALUES ('"+anno+"-"+mese+"-"+giorno+" "+ora+":00:00',"+gsett+","+cnt+","+idSensore+",NULL);";
			
			//System.out.println(insertQuery);
            // Statements allow to issue SQL queries to the database
            connect.createStatement().execute(insertQuery);
		}
	}
	
	public static Connection getConnectionMySql() throws Exception{
		try{
			String driver = "com.mysql.jdbc.Driver";
			String url = "jdbc:mysql://localhost:3306/mobilita_comune_monza";
			String username = "root";
			String password = "Monza.2017";
			Class.forName(driver); 
			Connection conn = DriverManager.getConnection(url,username,password);
			System.out.println("Connected to MySql");
			return conn;
		} catch(Exception e){
			System.out.println(e);
		}return null;
	}
	
	public static ResultSet getResultSetDBMonzaNew()  throws SQLException, ClassNotFoundException {
		//connessione al database
		Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");	
		Connection conn = DriverManager.getConnection("jdbc:sqlserver://213.82.16.77:1433;user=readdata;password=Dat1Traff!co;database=dbMonzaNEW");
		
		//Set calendario
	    Calendar cal = Calendar.getInstance();
	    cal.add(Calendar.DATE, -1); //-1 per prendere le informazioni della giornata di ieri
	    //cal.setMinimalDaysInFirstWeek(1);
	    
	    //get info calendatio
	    int year = cal.get(Calendar.YEAR);
	    String week_of_year = String.format("%02d", cal.get(Calendar.WEEK_OF_YEAR));
	    String month = String.format("%02d", cal.get(Calendar.MONTH) + 1); //i mesi li conta da 0
	    String day = String.format("%02d", cal.get(Calendar.DAY_OF_MONTH));
	    
	    //creo il nome della tabella corrispondete alla settimana di ieri
		String tableName = "dbo.H_DTP_DetectorPools"+year+week_of_year;
		
		//prendo la query composta con le informazioni prese dal calendatio
		String query = getQuery(tableName, year, month, day);
		Statement sta = conn.createStatement();
		ResultSet rs = sta.executeQuery(query);
		return rs;
	}
	
	public static String getQuery(String tableName, int year, String month, String day){
		return "SELECT "
				+ "ows_id id_sens, "
				+ "SUM(DTP_Growing_Counter) CNT, "
				+ "datepart(hour,TimeStamp) ora, "
				+ "datepart(dd, TimeStamp) giorno, "
				+ "datepart(MM, TimeStamp) mese, "
				+ "datepart(YY, TimeStamp) anno, "
				+ "datepart(WEEKDAY, TimeStamp) gsett "
			+ "FROM "+ tableName + " "
			+ "WHERE "
				+ "(OWS_ID = 5098 OR OWS_ID = 5099) "
				+ "AND timestamp >= '" + year + month + day + " 00:00:01:00' "
				+ "AND timestamp <= '" + year + month + day + " 23:59:59:00'"
			+ "GROUP BY "
				+ "ows_id, "
				+ "DATEPART(hour, Timestamp), "
				+ "datepart (dd, TimeStamp), "
				+ "datepart (MM,TimeStamp), "
				+ "datepart (YY, TimeStamp), "
				+ "datepart (WEEKDAY, TimeStamp) "
			+ "ORDER BY "
				+ "OWS_ID, "
				+ "datepart(MM, TimeStamp), "
				+ "datepart(dd, TimeStamp), "
				+ "datepart(hour,Timestamp) ";
	}
	
}
