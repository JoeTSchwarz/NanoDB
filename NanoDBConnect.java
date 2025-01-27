package nanodb;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
//
import java.nio.*;
import java.nio.channels.*;
import java.nio.charset.Charset;
/**
NanoDBConnect. Interface for NanoDB's clients to NanoDBServer
@author Joe T. Schwarz (c)
*/
public class NanoDBConnect {
  /**
  contructor. API for Client app
  @param host  String, NanoDB Server hostname or IP
  @param port  int, NanoDB Server's port
  @exception Exception thrown by java
  */
  public NanoDBConnect(String host, int port) throws Exception {
    soc = SocketChannel.open(new InetSocketAddress(host, port));
    soc.socket().setReceiveBufferSize(65536); // 32KB
    soc.socket().setSendBufferSize(65536);
    dbLst.add("*");
    // start Shutdown listener
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        if (soc != null) try {
          disconnect();
        } catch (Exception ex) { }
      }
    });
  }  
  /**
  @param dbName String
  @param charsetName String
  @return String assigned userID to this connected dbName
  @exception Exception thrown by java
  */
  public String open(String dbName, String charsetName) throws Exception {
    if (!dbLst.contains(dbName)) {
      dbLst.add(dbName);
      return new String(send(dbName, 0, charsetName));
    } else throw new Exception(dbName+" is already opened.");
  }
  /**
  @param dbName String
  @exception Exception thrown by java
  */
  public void close(String dbName) throws Exception {
    send(dbName, 1);
    dbLst.remove(dbName);
  }
  /**
  @param dbName String
  @return List of key Strings
  @exception Exception thrown by java
  */
  public List<String> getKeys(String dbName) throws Exception {
    byte[] bb = send(dbName, 2);
    List<String> keys = new ArrayList<>();
    if (bb[1] != (byte) 0x00 || bb[2] != (byte) 0x00) {
      for (int l = 0, i = 1; i < bb.length; i += (2+l)) {
        l = ((int)(bb[i] & 0xFF) << 8) | (int)(bb[i+1] & 0xFF);
        keys.add(new String(bb, i+2, l));
      }
    }
    return keys;
  }
  /**
  @param dbName String
  @param boo boolean, true: set, false: reset
  @exception Exception thrown by java
  */
  public void autoCommit(String dbName, boolean boo) throws Exception {
    if (boo) send(dbName, 3, ""+(char)0x00);
    else     send(dbName, 3, ""+(char)0x01);
  }
  /**
  @param dbName String
  @return boolean true: set, false: not set
  @exception Exception thrown by java
  */
  public boolean isAutoCommit(String dbName) throws Exception {
    return send(dbName, 4)[1] == (byte)0x00;
  }
  /**
  @param dbName String
  @param key String
  @return boolean true: locked, false: no locked
  @exception Exception thrown by java
  */
  public boolean lock(String dbName, String key) throws Exception {
    return send(dbName, 5, key)[1] == (byte)0x00;
  }
  /**
  @param dbName String
  @param key String
  @return boolean true: locked, false: no locked
  @exception Exception thrown by java
  */
  public boolean unlock(String dbName, String key) throws Exception {
    return send(dbName, 6, key)[1] == (byte)0x00;
  }
  /**
  @param dbName String
  @param key String
  @return boolean true: locked, false: no locked
  @exception Exception thrown by java
  */
  public boolean isLocked(String dbName, String key) throws Exception {
    return send(dbName, 7, key)[1] == (byte)0x00;
  }
  /**
  @param dbName String
  @param key String
  @return boolean true: locked, false: no locked
  @exception Exception thrown by java
  */
  public boolean isExisted(String dbName, String key) throws Exception {
    return send(dbName, 8, key)[1] == (byte)0x00;
  }
  /**
  @param dbName String
  @param key String
  @return boolean true: locked, false: no locked
  @exception Exception thrown by java
  */
  public boolean isKeyDeleted(String dbName, String key) throws Exception {
    return send(dbName, 9, key)[1] == (byte)0x00;
  }
  /**
  @param dbName String
  @param key String
  @return Object either as serialized Object or as byte array
  @exception Exception thrown by java
  */
  public Object readObject(String dbName, String key) throws Exception {
    byte[] buf = send(dbName, 10, key); // ignore buf[0] as OK-byte 
    if (buf[1] != (byte)0xAC || buf[2] != (byte)0xED) {
      byte[] bb = new byte[buf.length-1];
      System.arraycopy(buf, 1, bb, 0, bb.length);
      return bb;
    }
    ObjectInputStream oi = new ObjectInputStream(new ByteArrayInputStream(buf, 1, buf.length-1));
    Object obj = oi.readObject();
    oi.close();
    return obj;
  }
  /**
  @param dbName String
  @param key String
  @param obj serializable Object
  @exception Exception thrown by java
  */
  public void addObject(String dbName, String key, Object obj) throws Exception {
    ByteArrayOutputStream bao = new ByteArrayOutputStream();
    ObjectOutputStream oo = new ObjectOutputStream(bao);
    oo.writeObject(obj);
    oo.flush();
    oo.close();
    send(dbName, 11, key, bao.toByteArray());
  }
  /**
  @param dbName String
  @param key String
  @param buf byte array
  @exception Exception thrown by java
  */
  public void addObject(String dbName, String key, byte[] buf) throws Exception {
    send(dbName, 11, key, buf);
  }
  /**
  @param dbName String
  @param key String
  @return Object either as serialized Object or as byte array
  @exception Exception thrown by java
  */
  public void deleteObject(String dbName, String key) throws Exception {
    send(dbName, 12, key);
  }
  /**
  @param dbName String
  @param key String
  @param obj serializable Object
  @exception Exception thrown by java
  */
  public void updateObject(String dbName, String key, Object obj) throws Exception {
    ByteArrayOutputStream bao = new ByteArrayOutputStream();
    ObjectOutputStream oo = new ObjectOutputStream(bao);
    oo.writeObject(obj);
    oo.flush();
    oo.close();
    send(dbName, 13, key, bao.toByteArray());
  }
  /**
  @param dbName String
  @param key String
  @param buf byte array
  @exception Exception thrown by java
  */
  public void updateObject(String dbName, String key, byte[] buf) throws Exception {
    send(dbName, 13, key, buf);
  }
  /**
  @param dbName String
  @param key String
  @return boolean true: committed, false: failed
  @exception Exception thrown by java
  */
  public boolean commit(String dbName, String key) throws Exception {
    return send(dbName, 14, key)[1] == (byte)0x00;
  }
  /**
  @param dbName String
  @param key String
  @exception Exception thrown by java
  */
  public void commitAll(String dbName) throws Exception {
    send(dbName, 15);
  }
  /**
  @param dbName String
  @param key String
  @return boolean true: rolled back, false: failed
  @exception Exception thrown by java
  */
  public boolean rollback(String dbName, String key) throws Exception {
    return send(dbName, 16, key)[1] == (byte)0x00;
  }
  /**
  @param dbName String
  @param key String
  @exception Exception thrown by java
  */
  public void rollbackAll(String dbName) throws Exception {
    send(dbName, 17);
  }
  /**
  @param dbName String
  @exception Exception thrown by java
  */
  public void disconnect() throws Exception {
    send("*", 18);
    soc.close();
    bao.close();
    soc = null;
  }
  //-------------------------------------------------------------------------------------
  //
  // buf format: 1st byte: cmd, 2 bytes: dbName length, 2 bytes: key length, 4 bytes: data Length, dbName, key, data
  //
  private byte[] send(String dbName, int cmd, String key, byte[] obj) throws Exception {
    if (!dbLst.contains(dbName)) throw new Exception("Unknown dbName "+dbName);
    int kl = key.length(), dl = dbName.length();
    byte[] buf = new byte[9+kl+dl+obj.length];
    buf[0] = (byte)cmd;
    buf[1] = (byte)(((int)dl & 0xFF00) >> 8); buf[2] = (byte)((int)dl & 0xFF);
    buf[3] = (byte)(((int)kl & 0xFF00) >> 8); buf[4] = (byte)((int)kl & 0xFF);
    buf[5] = (byte)(((int)obj.length & 0xFF000000) >> 24); buf[6] = (byte)(((int)obj.length & 0xFF0000) >> 16);
    buf[7] = (byte)(((int)obj.length & 0xFF00) >> 8); buf[8] = (byte)((int)obj.length & 0xFF);
    System.arraycopy(dbName.getBytes(), 0, buf, 9, dl);
    System.arraycopy(key.getBytes(), 0, buf, 9+dl, kl);
    System.arraycopy(obj, 0, buf, 9+dl+kl, obj.length);
    soc.write(ByteBuffer.wrap(buf, 0, buf.length));
    return readChannel();
  }
  //
  // buf format: 1st byte: cmd, 2 bytes: dbName length, 2 bytes: key/data length, dbName, keye/data
  //
  private byte[] send(String dbName, int cmd, String key) throws Exception {
    if (!dbLst.contains(dbName)) throw new Exception("Unknown dbName "+dbName);
    int kl = key.length(), dl = dbName.length();
    byte[] buf = new byte[5+kl+dl];
    buf[0] = (byte)cmd;
    buf[1] = (byte)(((int)dl & 0xFF00) >> 8); buf[2] = (byte)((int)dl & 0xFF);
    buf[3] = (byte)(((int)kl & 0xFF00) >> 8); buf[4] = (byte)((int)kl & 0xFF);
    System.arraycopy(dbName.getBytes(), 0, buf, 5, dl);
    System.arraycopy(key.getBytes(), 0, buf, 5+dl, kl);
    soc.write(ByteBuffer.wrap(buf, 0, buf.length));
    return readChannel();
  }
  //
  // buf format: 1st byte: cmd, 2 bytes: key length, key or name
  //
  private byte[] send(String dbName, int cmd) throws Exception {
    if (!dbLst.contains(dbName)) throw new Exception("Unknown dbName "+dbName);
    int dl = dbName.length();
    byte[] buf = new byte[3+dl];
    buf[0] = (byte)cmd;
    buf[1] = (byte)(((int)dl & 0xFF00) >> 8); buf[2] = (byte)((int)dl & 0xFF);
    System.arraycopy(dbName.getBytes(), 0, buf, 3, dl);
    soc.write(ByteBuffer.wrap(buf, 0, buf.length));
    return readChannel();
  }
  //-------------------------------------------------------------------------------
  private byte[] readChannel() throws Exception {
    bao.reset();
    int le = 0;
    do {
      bbuf.clear();
      le = soc.read(bbuf);
      bao.write(bbuf.flip().array(), 0, le);
    } while (le >= 65536);
    byte[] bb = bao.toByteArray();
    if (bb[0] == (byte)00) return bb;
    throw new Exception(new String(bb, 1, bb.length-1));
  }
  //------------------------------------------------------------------------------
  private SocketChannel soc;
  private ByteBuffer bbuf = ByteBuffer.allocate(65536);
  private ByteArrayOutputStream bao = new ByteArrayOutputStream(65536);
  private List<String> dbLst = Collections.synchronizedList(new ArrayList<>());
}
