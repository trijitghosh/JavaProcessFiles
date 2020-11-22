package StreamProcess;

import org.json.JSONException;
import org.json.JSONObject;

public class MyObject {
    private JSONObject jsonObj;

    MyObject(String line){
        try {
            this.jsonObj = new JSONObject(line);
        }catch(JSONException e){
            System.out.println(line);
            this.jsonObj = new JSONObject("{}");
        }
    }

    public JSONObject getJsonObject(){
        return this.jsonObj;
    }
}
