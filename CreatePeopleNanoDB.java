import java.io.*;
import java.nio.*;
import java.util.*;
import java.nio.file.*;
import nanodb.NanoDB;
// @author Joe T. Schwarz (c)
public class CreatePeopleNanoDB {
  public CreatePeopleNanoDB( ) throws Exception {
    List<String> lines = Files.readAllLines((new File("people.txt")).toPath());
    int i, len = lines.size();
    //
    ByteArrayOutputStream bao = new ByteArrayOutputStream();
    NanoDB nano = new NanoDB("People");
    nano.open( );
    // set autoCommit
    nano.autoCommit(true);
    for (i = 0; i < len; i += 2) {
      char c = lines.get(i).charAt(0);
      if (c == '-') ++i;
      bao.reset();
      String key = lines.get(i).trim();
      System.out.println("Create:"+key);
      People obj = new People(key, lines.get(i+1).trim());      
      ObjectOutputStream oos = new ObjectOutputStream(bao);
      oos.writeObject(obj);
      oos.flush();
      nano.addObject("Joe", key, bao.toByteArray());
      oos.close();
      bao.close();
    }
    nano.close( );
    System.out.println("Done");
    System.exit(0);
  }
  private String dbName = "people";
  //
  public static void main(String... a) throws Exception {
    new CreatePeopleNanoDB( );
  }
}

