package lecture;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LectureCSV {
	
	/**
	 * Lecture du fichier csv
	 * @param file
	 * @return
	 * @throws IOException
	 */
	public static List<String> readFile(File file) throws IOException {
		
		//int compteur = 0;
		List<String> uniqueValue = new ArrayList<String>();
		
        List<String> result = new ArrayList<String>();

        FileReader fr = new FileReader(file);
        BufferedReader br = new BufferedReader(fr);

        for (String line = br.readLine(); line != null; line = br.readLine()) {
            result.add(line);
            String[] ligneDecoupee = line.split(";");
            
            for (int i = 0 ; i < ligneDecoupee.length ; i++) {
            	//System.out.println(ligneDecoupee[i]);
            	if (!uniqueValue.contains(ligneDecoupee[0])){
            		uniqueValue.add(ligneDecoupee[0]);
            	}
            }
            
            System.out.println(line);
        }

        br.close();
        fr.close();

        System.out.println();
        
        return result;
    }

}
