package utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;


public class Utilitaires {
	
	private Connection connexion;
	private HashMap<String, Integer> salles;
	private HashMap<String, Integer> typesMesure;
	private HashMap<String, Integer> batiments;
	private HashMap<String, Integer> ilots;
	
	/**
	 * Constructeur de la classe Utilitaire
	 * @param connexion
	 */
	public Utilitaires(Connection connexion) {
		this.connexion = connexion;
		salles = this.getSalles();
		typesMesure = this.getTypesMesure();
		ilots = this.getIlots();
		batiments = this.getBatiments();
	}

	/**
	 * Prend en entrée une ligne de mesure, la découpe, récupère les différents identifiants nécessaires,
	 * identifiant de l'ilot, identifiant du type de la mesure, dans la hashmap correspondante si cette dernière est présente
	 * envoi une requête à la base sinon.
	 * @param ligne
	 * @return
	 */
	public int insertMesure(String ligne) {
		
		// Découpage de la ligne lue
		String[] ligneDecoupee = ligne.split(";");	// Exemple de ligne : co2;u4/302/co2/ilot3;275;2017-09-22T16:27:39.511862;ppm;ilot3
		String[] infoIlot = ligneDecoupee[1].split("/");	// Exemple de ligneDecoupee[1] : u4/302/co2/ilot3
		
		String typeMesure = ligneDecoupee[0];
		String batiment = infoIlot[0];
		String salle = infoIlot[1];
		String ilot = infoIlot[3];
		float value = Float.parseFloat(ligneDecoupee[2]);
		String date = ligneDecoupee[3];
		String uniteMesure = ligneDecoupee[4];
		
		// Récupérer l'identifiant du type mesure
		int idTypeMesure = -1;
		// Si présent dans le HashMap
		if (typesMesure.containsKey(typeMesure + uniteMesure)) {
			idTypeMesure = typesMesure.get(typeMesure + uniteMesure);
		} else {	// Sinon faire une requête SQL qui va chercher la valeur dans la base de donnée et l'ajouter dans la hashmap pour éviter de futures requêtes
			idTypeMesure = getTypeMesure(typeMesure, uniteMesure);
		} // TODO Gérer le cas où le type n'est pas encore présent en base de données, on l'ajoute ? ou on passe cette mesure ?
		
		// Récupérer l'identifiant de l'ilot
		int idIlot = -1;
		if (ilot.contains(ilot + salle)) {		// Recherche dans la HashMap
			idIlot = ilots.get(ilot + salle);
		} else {
			idIlot = getIlot(ilot, salle, batiment);	// Recherche dans la base de données
			
			if (idIlot == -1) {			// Si absent de la base de données	
				idIlot = insertIlot(ilot, salle, batiment);	// On insère la donnée dans la base
			}	
		}
		
		// Formatage de la date et de l'heure
		date = date.replace("T", " ");
		
		Timestamp dateF = Timestamp.valueOf(date);
		
		PreparedStatement stmt = null;
		
		try {
			stmt = connexion.prepareStatement("INSERT INTO mesure (valeur, date, typemesure, ilot) VALUES (?, ?, ?, ?)");
			stmt.setFloat(1, value);
			stmt.setObject(2, dateF);
			stmt.setInt(3, idTypeMesure);
			stmt.setInt(4, idIlot);
			stmt.executeUpdate();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			
		// Requête
		
		System.out.println();
		// Tester si tout est ok avant d'insert sinon annuler l'insertion ?
		
		
		return 0;
	}
	
	private int insertIlot(String ilot, String salle, String batiment) {
		
		int result = -1;
		int id = -1;
		
		PreparedStatement stmt = null;
		
		// Récupérer l'identifiant de la salle
		int idSalle = -1;
		// Chercher dans le hashmap
		if (salles.containsKey(salle + batiment)) {
			idSalle = salles.get(salle + batiment);
		} else {	// Chercher en base de données
			idSalle = getSalle(salle, batiment);
		}
		// TODO Gérer le cas où la salle n'existe pas
		
		
		try {
			stmt = connexion.prepareStatement("INSERT INTO ilot (libelle, salle) VALUES (?, ?)");
			stmt.setString(1, ilot);
			stmt.setInt(2, idSalle);
			// TODO tester si l'identifiant de la salle n'est pas nul ou négatif
			result = stmt.executeUpdate();
			
			// Ajout dans la hashMap
			id = getIlot(ilot, salle, batiment);	// On récupère l'identifiant généré
			
			ilots.put(ilot + salle + batiment, result);
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return id;
		
	}

	private int getIlot(String ilot, String salle, String batiment) {
				
		int idResult = -1;
		
		ResultSet result = null;
		PreparedStatement stmt = null;
		
		try {	// Utilisation de requête préparée
			String requete = "SELECT ilot.id, ilot.libelle, salle.nomSalle, batiment.libelle FROM ilot JOIN salle ON ilot.salle = salle.id JOIN batiment ON batiment.id = salle.batiment WHERE ilot.libelle = ? AND salle.nomSalle = ? AND batiment.libelle = ?";
			stmt = connexion.prepareStatement(requete);
			stmt.setString(1, ilot);
			stmt.setString(2, salle);
			stmt.setString(3, batiment);
			result = stmt.executeQuery();
			
			
			while (result.next()) {
				idResult = result.getInt("ilot.id");	
			}
			
			// Si une valeur a été trouvée et n'est pas présente dans la hashmap
			// alors on l'ajoute
			if (idResult != -1 && !ilots.containsKey(ilot + salle + batiment)) {
				typesMesure.put(ilot + salle + batiment, idResult);
			}
			
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (result != null) {
				try {
					result.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
		
		return idResult;
		
	}

	/**
	 * Retourne l'ensemble des ilots présents en base de données dans une hashmap
	 * @return
	 */
	private HashMap<String, Integer> getIlots() {
		
		HashMap<String, Integer> resultats = new HashMap<String, Integer>();
		
		ResultSet results = null;
		try {
			PreparedStatement stmt = connexion.prepareStatement("SELECT ilot.id, ilot.libelle, salle.nomSalle, batiment.libelle "
																+ "FROM ilot JOIN salle ON salle.id = ilot.salle "
																+ "JOIN batiment ON salle.batiment = batiment.id");
			results = stmt.executeQuery();
			
			while (results.next()) {
				int id = results.getInt("ilot.id");
				String ilotLibelle = results.getString("ilot.libelle");
				String nomSalle = results.getString("salle.nomSalle");
				String batiment = results.getString("batiment.libelle");
				resultats.put(ilotLibelle + nomSalle + batiment, id);
			}
			
			stmt.close();
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println("Utilitaires.getIlots() - Erreur lors de la récupération des ilots.");
			e.printStackTrace();
		}
		
		return resultats;
	}

	/**
	 * Récupère l'ensemble des salles présentent dans la base de données
	 * et retourne le résultat sous forme de HashMap<Key, Value>, avec la 
	 * value correspondant à l'identifiant de la salle et la Key à son nom 
	 * combiné à son batiment pour obtenir une valeur unique
	 * @param connexion
	 * @return HashMap<Key, Value>
	 */
	public HashMap<String, Integer> getSalles() {
		
		HashMap<String, Integer> resultats = new HashMap<String, Integer>();
		
		ResultSet results = null;
		try {
			Statement stmt = connexion.createStatement();
			results = stmt.executeQuery("SELECT salle.id, salle.nomSalle, batiment.libelle FROM `salle` JOIN batiment ON salle.batiment = batiment.id;");
			
			while (results.next()) {
				int id = results.getInt("salle.id");
				String batimentLibelle = results.getString("batiment.libelle");
				String nomSalle = results.getString("salle.nomSalle");
				resultats.put(nomSalle + batimentLibelle, id);
			}
			
			stmt.close();
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println("Utilitaires.getSalles() - Erreur lors de la récupération des salles.");
			e.printStackTrace();
		}
		
		
		
		return resultats;
	}

	/**
	 * Récupère l'identifiant d'une salle en base de données à l'aide de son nom et de celui de son batiment
	 * @param nomBatiment
	 * @return
	 */
	public int getSalle(String nomSalle, String nomBatiment) {
		
		int idResult = -1;
		
		ResultSet result = null;
		PreparedStatement stmt = null;
		
		try {	// Utilisation de requête préparée
			stmt = connexion.prepareStatement("SELECT salle.id, salle.nomSalle, batiment.libelle"
					+ "FROM salle JOIN batiment ON batiment.id = salle.batiment "
					+ "WHERE salle.nomSalle = ? AND batiment.libelle = ?");
			stmt.setString(1, nomSalle);
			stmt.setString(2, nomBatiment);
			result = stmt.executeQuery();
			
			
			while (result.next()) {
				idResult = result.getInt("salle.id");	
			}
			
			// Si une valeur a été trouvée et n'est pas présente dans la hashmap
			// alors on l'ajoute
			if (idResult != -1 && !ilots.containsKey(nomSalle + nomBatiment)) {
				typesMesure.put(nomSalle + nomBatiment, idResult);
			}
			
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (result != null) {
				try {
					result.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
		
		return idResult;
		
	}
	
	public HashMap<String, Integer> getBatiments() {
		HashMap<String, Integer> resultats = new HashMap<String, Integer>();
		
		ResultSet results = null;
		
		try {
			Statement stmt = connexion.createStatement();
			results = stmt.executeQuery("SELECT * FROM batiment");
			
			while(results.next()) {
				resultats.put(results.getString("libelle"), results.getInt("id"));
			}
			
			stmt.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		
		return resultats;
	}

	/**
	 * 
	 * @return
	 */
	public HashMap<String, Integer> getTypesMesure() {
		
		HashMap<String, Integer> resultats = new HashMap<String, Integer>();
		
		
		ResultSet results = null;
		
		try {
			Statement stmt = connexion.createStatement();
			results = stmt.executeQuery("SELECT * FROM typemesure");
			
			while (results.next()) {
				int id = results.getInt("id");
				String libelle = results.getString("libelle");
				String unite = results.getString("unite");
				
				resultats.put(libelle + unite, id);	// Concaténation du nom et du type pour éviter les problèmes avec la luminosité.
			}
			
			stmt.close();
			
		} catch (SQLException e) {
			System.out.println("Utilitaires.getTypesMesure() - Erreur lors de la récupération des types de mesure.");
			e.printStackTrace();
		}
		
		return resultats;
	}
	
	/**
	 * Récupère une seule valeur dans la base de données, retourne l'identifiant de cette dernière mais ajoute également cette valeur
	 * dans la hashMap typeMesure pour éviter de futures requêtes sur cette même valeur
	 * @param libelle
	 * @param unite
	 * @return
	 */
	public int getTypeMesure(String libelle, String unite) {
		
		int idResult = -1;
		
		ResultSet result = null;
		PreparedStatement stmt = null;
		
		try {	// Utilisation de requête préparée
			stmt = connexion.prepareStatement("SELECT id FROM typemesure WHERE libelle = ? AND unite = ?");
			stmt.setString(1, libelle);
			stmt.setString(2, unite);
			result = stmt.executeQuery();
			
			
			while (result.next()) {
				idResult = result.getInt("id");	
			}
			
			// Si une valeur a été trouvée et n'est pas présente dans la hashmap
			// alors on l'ajoute
			if (idResult != -1 && !typesMesure.containsKey(libelle+unite)) {
				typesMesure.put(libelle + unite, idResult);
			}
			
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (result != null) {
				try {
					result.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
		
		return idResult;
	}
}
