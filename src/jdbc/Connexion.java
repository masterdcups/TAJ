package jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Connexion {
	
	/**
	 * Chargement du driver
	 */
	public static void load() {
		
		// Chargement du driver JDBC pour MySql
		try {
			//Class.forName("com.mysql.jdbc.Driver");
			Class.forName("com.mysql.cj.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();	// Gestion des erreurs de chargement
			System.out.println("Erreur lors du chargement du driver JDBC pour mySql");
		}
	}
	
	/**
	 * Ouverture de la connexion avec la base de données MySql
	 * @param url
	 * @param utilisateur
	 * @param mdp
	 * @return
	 */
	public static Connection connexionBase(String url, String utilisateur, String mdp) {
		
		Connection connexion = null;
		
		try {
			String urlComplet = url + "?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
			connexion = DriverManager.getConnection(urlComplet, utilisateur, mdp);
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("Erreur lors de l'ouverture de la connexion avec la base de données");
		}
		
		return connexion;
		
	}
	
	/**
	 * Fermeture de la connexion avec la base de données
	 * @param connexion
	 */
	public static void fermetureBase(Connection connexion) {
		try {
			connexion.close();
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("Erreur lors de la fermeture de la connexion avec la base de données");
		}
	}
}
