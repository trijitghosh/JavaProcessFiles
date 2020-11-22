package StreamProcess;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.stream.Stream;

public class ProcessHugeFile {

    public static JSONObject jsonTransform(JSONObject obj){

        if (!obj.has("brand")){
            obj.put("brand","");
        }

        if (!obj.has("price")){
            obj.put("price",-1);
        }


        JSONArray categories = obj.getJSONArray("categories");
        categories = (JSONArray) categories.get(0);
        obj.remove("categories");
        obj.put("categories",categories);


        if (!obj.has("title")){
            obj.put("title","");
        }

        return obj;
    }

    public static boolean cleanData(JSONObject obj){

        try {
            return obj.has("asin") && obj.has("categories");
        }catch (JSONException e){
            System.out.println(obj.toString());
            return false;
        }
    }

    public static String getRequiredFields(JSONObject obj){

        return "\""+obj.getString("asin")
                + "\",\"" + obj.getString("title")
                + "\"," + obj.getDouble("price")
                + ",\"" + obj.getString("brand")
                + "\",\"" + obj.getJSONArray("categories").toString()
                                                    .replace("[", "{")
                                                    .replace("]", "}")
                                                    .replace("\"", "'") + "\"\n";
    }

    public static void processFile(Path path){

        MyFileWriter mfw = new MyFileWriter("D:/TakeAway/Data/metadata.csv");
        DatabaseConnect dc = new DatabaseConnect();

        try {

            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
            System.out.println(dtf.format(LocalDateTime.now()));

            Stream<JSONObject> obj = Files.lines(path).map(x -> new MyObject(x).getJsonObject())
                            .filter(x -> cleanData(x));

            Stream<String> myStr = obj.map(x -> jsonTransform(x))
                            .map(x -> getRequiredFields(x));


            myStr.map(mfw::write).count();



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
