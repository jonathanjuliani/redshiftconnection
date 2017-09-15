package br.com.fast.redshift.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(basePackages = { "br.com.fast.redshift" })
public class RSConnection {
	
	
	@Value("${aws.redshift.class.driver}")
	private String driver;
	
	@Value("${aws.redshift.user}")
	private String user;
	
	@Value("${aws.redshift.pass}")
	private String pass;
	
	@Value("${aws.redshift.full.link}")
	private String link;
	
	
	public Connection getRedShiftConnection() throws Exception {
		
		Connection conn = null;

		// Dynamically load driver at runtime.
		// Redshift JDBC 4.1 driver: com.amazon.redshift.jdbc41.Driver
		// Redshift JDBC 4 driver: com.amazon.redshift.jdbc4.Driver
		Class.forName(driver);

		// Open a connection and define properties.
		System.out.println("Connecting to database...");

		// Uncomment the following line if using a keystore and use properties class do define ssl.
		// props.setProperty("ssl", "true");
		conn = DriverManager.getConnection(link, user, pass);
		return conn;
			
	}

	public static void main(String[] args) {
		
		try {
			
			RSConnection redShift = new RSConnection();
			Statement stmt = null;
			Connection conn = redShift.getRedShiftConnection();
			
			// Try a simple query.
			System.out.println("Listing system tables...");
			stmt = conn.createStatement();
			
			String sql;
			sql = "select * from information_schema.tables;";
			ResultSet rs = stmt.executeQuery(sql);

			// Get the data from the result set.
			while (rs.next()) {
				// Retrieve two columns.
				String catalog = rs.getString("table_catalog");
				String name = rs.getString("table_name");

				// Display values.
				System.out.print("Catalog: " + catalog);
				System.out.println(", Name: " + name);
			}
			rs.close();
			stmt.close();
			conn.close();
		} catch (Exception e) {
			System.out.print("Error: " + e);
		}
			
	}

}
