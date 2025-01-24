package nanodb;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.nio.charset.Charset;
/**
NanoDBManager manages and synchronizes all NanoDBWorkers from accessing the underlying NanoDB files.
@author Joe T. Schwarz (c)
*/
public class NanoDBManager {
  // for NanoDBWorker loop
  public volatile boolean closed = false;
  /**
  contructor.
  @param path  String, directory path of NanoDB files
  @param limit int, limit NanoDB cache size (min. 1 MB, max. 1 GB)
  */
  public NanoDBManager(String path, int limit) {
    this.path = path+File.separator;
    if (limit < 0x100000) cacheLimit = 0x100000;
    else if (limit > 0x40000000) cacheLimit = 0x40000000;
    else cacheLimit = limit;
    
  }
  /**
  open - upper layer of NanoDB's open()
  @param userID String
  @param dbName String, NanoDB's name
  @param charsetName String
  @return byte array where the first byte element signifies the success (0) or failed (1), then a data in bytes
  */
  public byte[] open(String userID, String dbName, String charsetName) {
    try {
      if (!usersList.contains(userID)) {
        List<NanoDB> list = usersMap.get(userID);
        if (list == null) list = Collections.synchronizedList(new ArrayList<>());
        NanoDB nano = nanoMap.get(dbName); // already opened?
        if (nano == null) { // new NanoDB
          nano = new NanoDB(path+dbName, charsetName);
          nano.setCacheLimit(cacheLimit);
          nanoMap.put(dbName, nano);
          list.add(nano);
          nano.open();
        }
        usersList.add(userID);
        usersMap.put(userID, list);
      }
      return (""+(char)0x00+userID).getBytes();
    } catch (Exception ex) {
      return (""+(char)0x01+ex.toString()).getBytes();
    }
  }
  /**
  close - upper layer of NanoDB's close()
  <br>In case of error, the return byte array contains the error message.
  @param userID String
  @param dbName String, NanoDB's name
  @return byte array where the first byte element signifies the success (0) or failed (1)
  */
  public byte[] close(String userID, String dbName) {
    if (usersList.remove(userID)) try {
      NanoDB nano = nanoMap.get(dbName);
      nano.removeLockedKeys(userID); // remove all keyslocked by this user.
      if (usersList.size() == 0) { // last user?
        nanoMap.remove(dbName).close();
        usersMap.remove(userID);
      } else { // there're some users
        List<NanoDB> list = usersMap.get(userID);
        list.remove(nano);
        usersMap.put(userID, list);
      }
    } catch (Exception ex) { // exception
      return (""+(char)0x01+ex.toString()).getBytes();
    }
    return new byte[] { (byte)0x00 };
  }
  /**
  disconnect
  <br>In case of error, the return byte array contains the error message.
  @param userID String
  @return byte array where the first byte element signifies the success (0) or failed (1), then a data in bytes
  */
  public byte[] disconnect(String userID) {
    if (usersList.contains(userID)) try {
      List<NanoDB> list = usersMap.get(userID);
      for (NanoDB nano:list) {
        nano.removeLockedKeys(userID);
        nano.close();
      }
      usersList.remove(userID);
      usersMap.remove(userID);
    } catch (Exception ex) {
      return (""+(char)0x01+ex.toString()).getBytes();
    }
    return new byte[] { (byte)0x00 };
  }
  /**
  getKeys - upper layer of NanoDB's getKeys().
  <br>In case of error, the return byte array contains the error message.
  @param userID String
  @return byte array where the first byte element signifies the success (0) or failed (1), then a list of keys in bytes
  */
  public byte[] getKeys(String dbName) {
    try {
      List<String> keys = nanoMap.get(dbName).getKeys();
      if (keys.size() == 0) return new byte[] { (byte)0x00, (byte)0x00, (byte)0x00 };
      ByteArrayOutputStream bao = new ByteArrayOutputStream();
      bao.write(new byte[] { (byte)0x00 }); // successfull
      for (String k : keys) if (k.length() > 0) { // keyLength - keyContent as bytes      
        bao.write(new byte[] { (byte)((k.length() & 0xFF00) >> 8), (byte)(k.length() & 0xFF) });
        bao.write(k.getBytes());
      }
      bao.flush();
      bao.close();
      return bao.toByteArray();
    } catch (Exception ex) {
      return (""+(char)0x01+ex.toString()).getBytes();
    }
  }
  /**
  autoCommit - upper layer of NanoDB's autoCommit().
  @param dbName String
  @param boo boolean, true: set, false: reset
  @return byte array where the first byte element signifies the success (0) or failed (1)
  */
  public byte[] autoCommit(String dbName, boolean boo) {
    nanoMap.get(dbName).autoCommit(boo);
    return new byte[] { (byte)0x00 };
  }
  /**
  isAutoCommit - upper layer of NanoDB's autoCommit(). 
  <br>The returned string is either "true" or "false" in lower case.
  @param dbName String
  @return byte array where the first byte element signifies the success (0) or failed (1)
  */
  public byte[] isAutoCommit(String dbName) {
    if (nanoMap.get(dbName).isAutoCommit()) return new byte[] { (byte)0x00, (byte)0x00 };
    return new byte[] { (byte)0x00, (byte)0x01 };
  }
  /**
  lock - upper layer of NanoDB's lock(). The returned byte is x00 (OK) or x01 (failed).
  <br>The returned string is either "true" or "false" in lower case.
  @param userID String
  @param dbName String, NanoDB's name
  @param charsetName String
  @return byte array where the first byte element signifies the success (0) or failed (1)
  */
  public byte[] lock(String userID, String dbName, String key) {
    if (nanoMap.get(dbName).lock(userID, key)) return new byte[] { (byte)0x00, (byte)0x00 };
    return new byte[] { (byte)0x00, 0x01 };
  }
  /**
  unlock - upper layer of NanoDB's unlock(). The returned byte is x00 (OK) or x01 (failed).
  <br>The returned string is either "true" or "false" in lower case.
  @param userID String
  @param dbName String, NanoDB's name
  @param charsetName String
  @return byte array where the first byte element signifies the success (0) or failed (1)
  */
  public byte[] unlock(String userID, String dbName, String key) {
    if (nanoMap.get(dbName).unlock(userID, key))  return new byte[] { (byte)0x00, (byte)0x00 };
    return new byte[] { (byte)0x00, (byte)0x01 };
  }
  /**
  isLocked - upper layer of NanoDB's isLocked(). The returned byte is x00 (OK) or x01 (failed).
  <br>The returned string is either "true" or "false" in lower case.
  @param dbName String
  @param key String
  @return byte array where the first byte element signifies the success (0) or failed (1)
  */
  public byte[] isLocked(String dbName, String key) {
    if (nanoMap.get(dbName).isLocked(key)) return new byte[] { (byte)0x00, (byte)0x00 };
    return new byte[] { (byte)0x00, (byte)0x01 };
  }
  /**
  isExisted - upper layer of NanoDB's isExisted(). The returned byte is x00 (OK) or x01 (failed).
  <br>The returned string is either "true" or "false" in lower case.
  @param dbName String
  @param key String
  @return byte array where the first byte element signifies the success (0) or failed (1)
  */
  public byte[] isExisted(String dbName, String key) {
    if (nanoMap.get(dbName).isExisted(key)) return new byte[] { (byte)0x00, (byte)0x00 };
    return new byte[] { (byte)0x00, (byte)0x01 };
  }
  /**
  isKeyDeleted - upper layer of NanoDB's isKeyDeleted().
  <br>The returned string is either "true" or "false" in lower case.
  @param dbName String
  @param key String
  @return byte array where the first byte element signifies the success (0) or failed (1)
  */
  public byte[] isKeyDeleted(String dbName, String key) {
    if (nanoMap.get(dbName).isKeyDeleted(key)) return new byte[] { (byte)0x00, (byte)0x00 };
    return new byte[] { (byte)0x00, (byte)0x01 };
  }
  /**
  readObject - upper layer of NanoDB's getObject().
  <br>The returned is either the data or an error-message in byte array
  @param userID String
  @param dbName String
  @param key String
  @return byte array where the first byte element signifies the success (0) or failed (1)
  */
  public byte[] readObject(String userID, String dbName, String key) {
    try {
      byte[] buf = nanoMap.get(dbName).readObject(userID, key);
      byte[] bb = new byte[buf.length+1];
      bb[0] = (byte)0x00;
      System.arraycopy(buf, 0, bb, 1, buf.length);
      return bb;
    } catch (Exception ex) {
      return (""+(char)0x01+ex.toString()).getBytes();
    }
  }
  /**
  addObject - upper layer of NanoDB's addObject().
  <br>In case of error, the return byte array contains the error message.
  @param userID String
  @param dbName String
  @param key String
  @param buf byte array of (non)serialized object
  @return byte array where the first byte element signifies the success (0) or failed (1)
  */
  public byte[] addObject(String userID, String dbName, String key, byte[] buf) {
    try {
      nanoMap.get(dbName).addObject(userID, key, buf);
      return new byte[] { (byte)0x00 };
    } catch (Exception ex) {
      return (""+(char)0x01+ex.toString()).getBytes();
    }
  }
  /**
  deleteObject - upper layer of NanoDB's deleteObject().
  <br>In case of error, the return byte array contains the error message.
  @param userID String
  @param dbName String
  @param key String
  @return byte array where the first byte element signifies the success (0) or failed (1)
  */
  public byte[] deleteObject(String userID, String dbName, String key) {
    try {
      nanoMap.get(dbName).deleteObject(userID, key);
      return new byte[] { (byte)0x00 };
    } catch (Exception ex) {
      return (""+(char)0x01+ex.toString()).getBytes();
    }
  }
  /**
  updateObject - upper layer of NanoDB's updateObject().
  <br>In case of error, the return byte array contains the error message.
  @param userID String
  @param dbName String
  @param key String
  @param buf byte array of (non)serialized object
  @return byte array where the first byte element signifies the success (0) or failed (1)
  */
  public byte[] updateObject(String userID, String dbName, String key, byte[] buf) {
    try {
      nanoMap.get(dbName).updateObject(userID, key, buf);
      return new byte[] { (byte)0x00 };
    } catch (Exception ex) {
      return (""+(char)0x01+ex.toString()).getBytes();
    }
  }
  /**
  commit - upper layer of NanoDB's commit(). The returned byte is x00 (OK) or x01 (failed).
  <br>The returned string is either "true" or "false" in lower case.
  @param userID String
  @param dbName String
  @param key String
  @return byte array where the first byte element signifies the success (0) or failed (1)
  */
  public byte[] commit(String userID, String dbName, String key) {
    if (nanoMap.get(dbName).commit(userID, key)) return new byte[] { (byte)0x00, (byte)0x00 };
    return new byte[] { (byte)0x00, (byte)0x01 };
  }
  /**
  commitAll - upper layer of NanoDB's commitAll().
  @param userID String
  @param dbName String
  @return byte array where the first byte element signifies the success (0) or failed (1)
  */
  public byte[] commitAll(String userID, String dbName) {
    nanoMap.get(dbName).commitAll(userID);
    return new byte[] { (byte)0x00 };
  }
  /**
  rollback - upper layer of NanoDB's rollack(). The returned byte is x00 (OK) or x01 (failed).
  <br>The returned string is either "true" or "false" in lower case.
  @param userID String
  @param dbName String
  @param key String
  @return byte array where the first byte element signifies the success (0) or failed (1)
  */
  public byte[] rollback(String userID, String dbName, String key) {
    if (nanoMap.get(dbName).rollback(userID, key)) return new byte[] { (byte)0x00, (byte)0x00 };
    return new byte[] { (byte)0x00, (byte)0x01 };
  }
  /**
  rollbackAll - upper layer of NanoDB's rollbackAll().
  @param userID String
  @param dbName String
  @return byte array where the first byte element signifies the success (0) or failed (1)
  */
  public byte[] rollbackAll(String userID, String dbName) {
    nanoMap.get(dbName).rollbackAll(userID);
    return new byte[] { (byte)0x00 };
  }
  //
  private String path;
  private int cacheLimit;
  private ByteArrayOutputStream bao = new ByteArrayOutputStream(65536);
  private ConcurrentHashMap<String, NanoDB> nanoMap = new ConcurrentHashMap<>();
  private List<String> usersList = Collections.synchronizedList(new ArrayList<>());
  private ConcurrentHashMap<String, List<NanoDB>> usersMap = new ConcurrentHashMap<>();
}
