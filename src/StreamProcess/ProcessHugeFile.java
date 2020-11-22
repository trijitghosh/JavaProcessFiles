package StreamProcess;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class ProcessHugeFile {

    public static void processFile(Path path){

        MyFileWriter mfw = new MyFileWriter("D:/TakeAway/Data/metadata.csv");
        DatabaseConnect dc = new DatabaseConnect();
        DataTransform dt = new DataTransform();

        try {

            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
            System.out.println(dtf.format(LocalDateTime.now()));

            Files.lines(path).map(x -> new MyObject(x).getJsonObject())
                            .filter(dt::cleanData)
                            .map(dt::jsonTransform)
                            .map(dt::getRequiredFields)
                            .map(mfw::write).count();


            dc.executeQuery("copy STG_PRODUCT from 'D:/TakeAway/Data/metadata.csv' delimiter ',' csv");
            dc.commitData();


            System.out.println(dtf.format(LocalDateTime.now()));


        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            mfw.close();
            dc.close();
        }

    }

    public static void main(String[] args){

        Path filePath = Paths.get("D:/TakeAway/Data/metadata.json");
        processFile(filePath);

    }

}
