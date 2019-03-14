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
	 * Prend en entr�e une ligne de mesure, la d�coupe, r�cup�re les diff�rents identifiants n�cessaires,
	 * identifiant de l'ilot, identifiant du type de la mesure, dans la hashmap correspondante si cette derni�re est pr�sente
	 * envoi une requ�te � la base sinon.
	 * @param ligne
	 * @return
	 */
	public int insertMesure(String ligne) {
		
		// D�coupage de la ligne lue
		String[] ligneDecoupee = ligne.split(";");	// Exemple de ligne : co2;u4/302/co2/ilot3;275;2017-09-22T16:27:39.511862;ppm;ilot3
		String[] infoIlot = ligneDecoupee[1].split("/");	// Exemple de ligneDecoupee[1] : u4/302/co2/ilot3
		
		String typeMesure = ligneDecoupee[0];
		String batiment = infoIlot[0];
		String salle = infoIlot[1];
		String ilot = infoIlot[3];
		float value = Float.parseFloat(ligneDecoupee[2]);
		String date = ligneDecoupee[3];
		String uniteMesure = ligneDecoupee[4];
		
		// R�cup�rer l'identifiant du type mesure
		int idTypeMesure = -1;
		// Si pr�sent dans le HashMap
		if (typesMesure.containsKey(typeMesure + uniteMesure)) {
			idTypeMesure = typesMesure.get(typeMesure + uniteMesure);
		} else {	// Sinon faire une requ�te SQL qui va chercher la valeur dans la base de donn�e et l'ajouter dans la hashmap pour �viter de futures requ�tes
			idTypeMesure = getTypeMesure(typeMesure, uniteMesure);
		} // TODO G�rer le cas o� le type n'est pas encore pr�sent en base de donn�es, on l'ajoute ? ou on passe cette mesure ?
		
		// R�cup�rer l'identifiant de l'ilot
		int idIlot = -1;
		if (ilot.contains(ilot + salle)) {		// Recherche dans la HashMap
			idIlot = ilots.get(ilot + salle);
		} else {
			idIlot = getIlot(ilot, salle, batiment);	// Recherche dans la base de donn�es
			
			if (idIlot == -1) {			// Si absent de la base de donn�es	
				idIlot = insertIlot(ilot, salle, batiment);	// On ins�re la donn�e dans la base
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
			
		// Requ�te
		
		System.out.println();
		// Tester si tout est ok avant d'insert sinon annuler l'insertion ?
		
		
		return 0;
	}
	
	private int insertIlot(String ilot, String salle, String batiment) {
		
		int result = -1;
		int id = -1;
		
		PreparedStatement stmt = null;
		
		// R�cup�rer l'identifiant de la salle
		int idSalle = -1;
		// Chercher dans le hashmap
		if (salles.containsKey(salle + batiment)) {
			idSalle = salles.get(salle + batiment);
		} else {	// Chercher en base de donn�es
			idSalle = getSalle(salle, batiment);
		}
		// TODO G�rer le cas o� la salle n'existe pas
		
		
		try {
			stmt = connexion.prepareStatement("INSERT INTO ilot (libelle, salle) VALUES (?, ?)");
			stmt.setString(1, ilot);
			stmt.setInt(2, idSalle);
			// TODO tester si l'identifiant de la salle n'est pas nul ou n�gatif
			result = stmt.executeUpdate();
			
			// Ajout dans la hashMap
			id = getIlot(ilot, salle, batiment);	// On r�cup�re l'identifiant g�n�r�
			
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
		
		try {	// Utilisation de requ�te pr�par�e
			String requete = "SELECT ilot.id, ilot.libelle, salle.nomSalle, batiment.libelle FROM ilot JOIN salle ON ilot.salle = salle.id JOIN batiment ON batiment.id = salle.batiment WHERE ilot.libelle = ? AND salle.nomSalle = ? AND batiment.libelle = ?";
			stmt = connexion.prepareStatement(requete);
			stmt.setString(1, ilot);
			stmt.setString(2, salle);
			stmt.setString(3, batiment);
			result = stmt.executeQuery();
			
			
			while (result.next()) {
				idResult = result.getInt("ilot.id");	
			}
			
			// Si une valeur a �t� trouv�e et n'est pas pr�sente dans la hashmap
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
	 * Retourne l'ensemble des ilots pr�sents en base de donn�es dans une hashmap
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
			System.out.println("Utilitaires.getIlots() - Erreur lors de la r�cup�ration des ilots.");
			e.printStackTrace();
		}
		
		return resultats;
	}

	/**
	 * R�cup�re l'ensemble des salles pr�sentent dans la base de donn�es
	 * et retourne le r�sultat sous forme de HashMap<Key, Value>, avec la 
	 * value correspondant � l'identifiant de la salle et la Key � son nom 
	 * combin� � son batiment pour obtenir une valeur unique
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
			System.out.println("Utilitaires.getSalles() - Erreur lors de la r�cup�ration des salles.");
			e.printStackTrace();
		}
		
		
		
		return resultats;
	}

	/**
	 * R�cup�re l'identifiant d'une salle en base de donn�es � l'aide de son nom et de celui de son batiment
	 * @param nomBatiment
	 * @return
	 */
	public int getSalle(String nomSalle, String nomBatiment) {
		
		int idResult = -1;
		
		ResultSet result = null;
		PreparedStatement stmt = null;
		
		try {	// Utilisation de requ�te pr�par�e
			stmt = connexion.prepareStatement("SELECT salle.id, salle.nomSalle, batiment.libelle"
					+ "FROM salle JOIN batiment ON batiment.id = salle.batiment "
					+ "WHERE salle.nomSalle = ? AND batiment.libelle = ?");
			stmt.setString(1, nomSalle);
			stmt.setString(2, nomBatiment);
			result = stmt.executeQuery();
			
			
			while (result.next()) {
				idResult = result.getInt("salle.id");	
			}
			
			// Si une valeur a �t� trouv�e et n'est pas pr�sente dans la hashmap
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
				
				resultats.put(libelle + unite, id);	// Concat�nation du nom et du type pour �viter les probl�mes avec la luminosit�.
			}
			
			stmt.close();
			
		} catch (SQLException e) {
			System.out.println("Utilitaires.getTypesMesure() - Erreur lors de la r�cup�ration des types de mesure.");
			e.printStackTrace();
		}
		
		return resultats;
	}
	
	/**
	 * R�cup�re une seule valeur dans la base de donn�es, retourne l'identifiant de cette derni�re mais ajoute �galement cette valeur
	 * dans la hashMap typeMesure pour �viter de futures requ�tes sur cette m�me valeur
	 * @param libelle
	 * @param unite
	 * @return
	 */
	public int getTypeMesure(String libelle, String unite) {
		
		int idResult = -1;
		
		ResultSet result = null;
		PreparedStatement stmt = null;
		
		try {	// Utilisation de requ�te pr�par�e
			stmt = connexion.prepareStatement("SELECT id FROM typemesure WHERE libelle = ? AND unite = ?");
			stmt.setString(1, libelle);
			stmt.setString(2, unite);
			result = stmt.executeQuery();
			
			
			while (result.next()) {
				idResult = result.getInt("id");	
			}
			
			// Si une valeur a �t� trouv�e et n'est pas pr�sente dans la hashmap
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
