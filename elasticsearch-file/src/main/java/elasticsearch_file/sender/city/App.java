package elasticsearch_file.sender.city;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/**
 * Hello world!
 *
 */
public class App {
	
	public static void main(String[] args) {

		City es = new City();
		
		String csvFile = "sort/nanpa-sorta-thousands-ile.csv";

		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = ",";

		try {

			int i = 0;
			br = new BufferedReader(new FileReader(csvFile));
			while ((line = br.readLine()) != null) {

				 //System.out.println(line);
				//System.out.println(i);

				// use comma as separator
				String[] splitedLine = line.split(cvsSplitBy);
				//System.out.println(splitedLine.length);

				
				if (splitedLine.length >= 5) {

					String npa = splitedLine[0].replace("\"", "");
					String nxx = splitedLine[1].replace("\"", "");
					String state = splitedLine[3].replace("\"", "");
					String city = isNumber(splitedLine[6].replace("\"", ""));
					String nickname = isNumber(splitedLine[10].replace("\"", ""));

					
					//System.out.println("Line: " + "|" + npa + "|" +  nxx + "|" +  state + "|" +  city + "|" + nickname);
					
					if(!city.equals("")) es.sendToElasticsearc(city, nickname, state, npa, nxx);
				}
				

				i++;
				//if(i==2) break;
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		System.out.println("Done");

	}

	public static String removeString(String original) {

		return original.replace("-", "");
	}

	public static String isNumber(String original) {
		try {
			double d = Double.parseDouble(original);
		} catch (NumberFormatException nfe) {
			return original;
		}
		return "";
	}
}
