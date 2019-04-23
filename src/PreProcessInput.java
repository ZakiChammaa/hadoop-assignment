import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class PreProcessInput {
	private static int threshold;
	private static int k = 1;
	
	public int getThreshold(){
		return PreProcessInput.threshold;
	}
	
	public int getNumberOfInputFiles(){
		return PreProcessInput.k;
	}
	
	/**
	 * Function that takes the raw input and splits it into k files
	 * with size (inputFileSize / r)
	 * 
	 * @param f:           Input file with all the baskets
	 * @param inputPath:   Path to the input file with all the baskets
	 * @param r:           Number of reducers         
	 * @throws IOException
	 */
	public static void splitFile(File f, String inputPath, int r) throws IOException {
		long inputFileSize = f.length();
        int partCounter = 1;
        long sizeOfFiles = inputFileSize / r;
        String[] everything = null;
		
        //Load all baskets into an array of Strings
		try(BufferedReader br = new BufferedReader(new FileReader(f))) {
		    StringBuilder sb = new StringBuilder();
		    String line = br.readLine();
		    threshold = Integer.parseInt(line);

		    line = br.readLine();
		    while (line != null) {
		        sb.append(line);
		        sb.append(System.lineSeparator());
		        line = br.readLine();
		    }
		    everything = sb.toString().split("\n");
		}
		
		int bytesAmount = 0;
		String filePartName = String.format("%s/file%02d", inputPath, partCounter++);
		File newFile = new File(filePartName);
		FileWriter fileWriter = new FileWriter(newFile);
		
		//Split the input into sub-files
		for(int i=0; i<everything.length; i++){
			if(i < everything.length-1){
				bytesAmount += (everything[i].getBytes("UTF-8").length+1);
			} else{
				bytesAmount += (everything[i].getBytes("UTF-8").length);
			}
			if(bytesAmount <= sizeOfFiles){
				fileWriter.write(everything[i] + "\n");
			} else{
				fileWriter.flush();
				fileWriter.close();
				bytesAmount = 0;
				filePartName = String.format("%s/file%02d", inputPath,partCounter++);
				newFile = new File(filePartName);
				fileWriter = new FileWriter(newFile);
				fileWriter.write(everything[i] + "\n");
				k++;
			}
		}
		fileWriter.flush();
		fileWriter.close();
    }
}