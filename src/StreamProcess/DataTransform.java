package StreamProcess;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class DataTransform {

    public String getRequiredFields(JSONObject obj){

        return "\""+obj.getString("asin")
                + "\",\"" + obj.getString("title")
                + "\"," + obj.getDouble("price")
                + ",\"" + obj.getString("brand")
                + "\",\"" + obj.getJSONArray("categories").toString()
                .replace("[", "{")
                .replace("]", "}")
                .replace("\"", "'") + "\"\n";
    }

    public boolean cleanData(JSONObject obj){

        try {
            return obj.has("asin") && obj.has("categories");
        }catch (JSONException e){
            System.out.println(obj.toString());
            return false;
        }
    }

    public JSONObject jsonTransform(JSONObject obj){

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
}
