package StreamProcess;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class MyFileWriter {

    private FileChannel fileChannel;
    private FileOutputStream fos;

    MyFileWriter(String path){
        try {
            fos = new FileOutputStream(path);
            fileChannel = fos.getChannel();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

    }

    public String write(String data){
        try {
            this.fileChannel.write(ByteBuffer.wrap(data.getBytes()));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return data;
    }

    public void close(){
        try {
            this.fileChannel.close();
            this.fos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
