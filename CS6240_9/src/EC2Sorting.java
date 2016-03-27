import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.Buffer;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.zip.GZIPInputStream;

public class EC2Sorting {
	public static void main(String args[]) throws IOException {
		EC2Sorting ec2Sorting = new EC2Sorting();
		File folder = new File("/home/hduser/climate");
		File[] listofFiles = folder.listFiles();
		for (File file : listofFiles) {
			if (file.isFile()) {
				ec2Sorting.readFile(file.getAbsoluteFile().getPath());
				// System.out.println(file.getAbsoluteFile().getPath());
				break;
			}
		}

	}

	private void readFile(String string) throws IOException {
		InetAddress host = InetAddress.getLocalHost();
		Socket socket = null;
		ObjectOutputStream oos = null;
		ObjectInputStream ois = null;
		GZIPInputStream stream = null;
		System.out.println(string);
		stream = new GZIPInputStream(new FileInputStream(string));
		BufferedReader br = new BufferedReader(new InputStreamReader(stream));
		br.readLine();
		socket = new Socket(host.getHostName(), 9876 /*mr adib kindly change to your required port*/);
		while (true) {
			String line = br.readLine();
			// System.out.println(line);
			if (line == null)
				break;
			String token[] = line.split(",");
			// System.out.println(token.length);
			String dryBulbTemp = token[8];
			System.out.println(token[0] + " " + token[1] + " " + token[8]);
			int dryBTemp = Integer.parseInt(dryBulbTemp);
			if(dryBTemp < 1 && dryBTemp > 10){
				socket = new Socket(host.getHostName(), 1 /*mr adib kindly change to your required port*/);
				oos.writeObject(dryBTemp);
			} else if(dryBTemp < 11 && dryBTemp > 20){
				socket = new Socket(host.getHostName(), 2 /*mr adib kindly change to your required port*/);
				oos.writeObject(dryBTemp);
			} else if(dryBTemp < 21 && dryBTemp > 30){
				socket = new Socket(host.getHostName(), 3 /*mr adib kindly change to your required port*/);
				oos.writeObject(dryBTemp);
			} else if(dryBTemp < 31 && dryBTemp > 40){
				socket = new Socket(host.getHostName(), 4 /*mr adib kindly change to your required port*/);
				oos.writeObject(dryBTemp);
			} 		
		}
		br.close();
	}


}