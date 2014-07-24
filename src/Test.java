import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;


public class Test {

	public static void main(String[] args) throws IOException {
	    System.out.println(Files.probeContentType(Paths.get("/home/kyle/Desktop/Unforgettable.S02E01._.x264-DEMAND.mkv")));

	}

}
