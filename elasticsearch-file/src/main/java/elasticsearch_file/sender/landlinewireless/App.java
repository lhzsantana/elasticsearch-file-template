package elasticsearch_file.sender.landlinewireless;

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

		LandlineWireless es = new LandlineWireless();

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
					String phone = splitedLine[5].replace("\"", "");
					String state = splitedLine[3].replace("\"", "");
					String city = isNotNumber(splitedLine[6].replace("\"", ""));
					String type = splitedLine[10].replace("\"", "");
					
					if(type.equals("ILEC")||
							type.equals("ICO")||
							type.equals("RBOC")||
							type.equals("CLEC")||
							type.equals("LRSL")||
							type.equals("IXC")||
							type.equals("LRSL")||
							type.equals("PCS ")||
							type.equals("PAGING")||
							type.equals("CAP")){

						type="landline";
					}else if(type.equals("WIRE")||
							type.equals("WRSL")){
						type="wireless";
					}

					System.out.println("Line: " + "|" + type + "|" + phone  + "|" + npa + "|" +  nxx + "|" +  state + "|" +  city);
					
					if(!type.trim().equals("") && !phone.trim().equals("") && isNumber(phone)) es.sendToElasticsearc(type, phone, npa, nxx);
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

	public static boolean isNumber(String original) {
		try {
			double d = Double.parseDouble(original);
		} catch (NumberFormatException nfe) {
			return false;
		}
		return true;
	}
	
	public static String isNotNumber(String original) {
		try {
			double d = Double.parseDouble(original);
		} catch (NumberFormatException nfe) {
			return "";
		}
		return original;
	}
}
