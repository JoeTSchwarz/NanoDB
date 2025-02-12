package nanodb;
//
import java.io.*;
import java.nio.*;
import java.util.*;
import java.nio.file.*;
import java.util.concurrent.*;
import java.nio.charset.Charset;
import java.nio.channels.FileLock;
/**
Nano Database in Nutshell.
<br>NanoDB consists of 2 areas: key area and data or record area
<br>- The key entry in the key area has 3 fields: key length, data/record size, key itself
<br>- The data area contains the records, which can be serializable objects or strings
<br>- The key of addObject is automatically locked and must be released (unlocked) by the owner for the others
<br>- The key of deleteObject remains locked until it is released (unlocked) by the owner.
<br>- Data must be committed before closed. Otherwise data will be lost.
@author Joe T. Schwarz (c)
*/
public class NanoDB {
  /**
  constructor
  @param fName  String, file name
  @exception Exception thrown by JAVA
  */
  public NanoDB(String fName) {
    this.fName = fName;
    cs = Charset.forName("UTF-8");
  }
  /**
  constructor
  @param fName  String, file name
  @param charsetName String, character set name (e.g. "UTF-8");
  @exception Exception thrown by JAVA
  */
  public NanoDB(String fName, String charsetName) {
    this.fName = fName;
    cs = Charset.forName(charsetName);
  }
  /**
  setCacheLimit (min. 1 MB)
  @param lim int, max. cache limit for NanoDB
  */
  public void setCacheLimit(int lim) {
    this.lim = lim;
    if (lim < 0x100000) lim = 0x100000;
    else if (lim > 0x40000000) lim = 0x40000000;
  }
  /**
  autoCommit (default: false)
  @param auto boolean, true: always aoto-commit after delete/update/add and no rollback, false: commit needed
  */
  public void autoCommit(boolean auto) {
    this.auto = auto;
  }
  /**
  getKeys() returns an ArrayList of all NanoDB keys
  @return ArrayList of strings (as keys) or an empty arraylist if NanoDB must be created
  */
  public ArrayList<String> getKeys() {
    return new ArrayList<>(keysList);
  }
  /**
  isAutoCommit.
  @return boolean true if autoCommit is set
  */
  public boolean isAutoCommit( ) {
    return auto;
  }
  /**
  lock key.
  @param userID String
  @param key String
  @return boolean true if key is locked, false if key is already locked by other user
  */
  public boolean lock(String userID, String key) {
    if (lockedList.contains(key)) return false;
    List<String> kList = keysLocked.get(userID);
    if (kList == null) kList = Collections.synchronizedList(new ArrayList<>());
    kList.add(key); // include key
    keysLocked.put(userID, kList);
    lockedList.add(key);
    return true;
  }
  /**
  unlock a locked key
  @param userID String
  @param key String
  @return boolean true if key is unlocked, false if key is already unlocked or unknown
  */
  public boolean unlock(String userID, String key) {
    if (!lockedList.contains(key)) return false;
    List<String> kList = keysLocked.get(userID);
    if (kList == null || kList.size() == 0) return false;
    kList.remove(key); // remove key
    keysLocked.put(userID, kList);
    lockedList.remove(key);
    return true;
  }
  /**
  isLocked.
  @param key String
  @return boolean true if key is locked
  */
  public boolean isLocked(String key) {
    return lockedList.contains(key);
  }
  /**
  isExisted. Excl. deleted key
  @param key String key
  @return boolean true if querried key exists (deleted keys won't count)
  */
  public boolean isExisted(String key) {
    return keysList.contains(key);
  }
  /**
  isKeyDeleted
  @param key String key
  @return boolean true if querried key is deleted
  */
  public boolean isKeyDeleted(String key) {
    return oCache.containsKey(key) && !keysList.contains(key);
  }
  /**
  readObject
  @param userID String
  @param key String
  @return byte array for (non)serializable object
  @exception Exception thrown by JAVA
  */
  public byte[] readObject(String userID, String key) throws Exception {
    if (!keysList.contains(key)) throw new Exception("Unknown "+key);
    // check for lock by other users
    List<String> kList = keysLocked.get(userID);
    if ((kList == null || !kList.contains(key)) &&
       lockedList.contains(key)) throw new Exception(key+" is locked by other User.");
    //
    if (cache.containsKey(key)) return cache.get(key);
    // read from file
    byte[] buf = new byte[sizes.get(key)];
    raf.seek(pointers.get(key));
    raf.read(buf);
    return buf; 
  }
  /**
  addObject with key by userID 
  @param userID string
  @param key String
  @param obj serializable object
  @exception Exception thrown by JAVA
  */
  public void addObject(String userID, String key, Object obj) throws Exception {
      ByteArrayOutputStream bao = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(bao);
      oos.writeObject(obj);
      oos.flush();
      oos.close();
      addObject(userID, key, bao.toByteArray());
  }
  /**
  addObject with key by userID 
  @param userID string
  @param key String
  @param buf byte array for (non)serializable object
  @exception Exception thrown by JAVA
  */
  public void addObject(String userID, String key, byte[] buf) throws Exception {
    if (keysList.contains(key)) throw new Exception(key+" exists.");
    if (!auto) {
      List<String> kList = keysLocked.get(userID);
      if (kList == null) kList = Collections.synchronizedList(new ArrayList<>());
      kList.add(key);
      lockedList.add(key);
      keysLocked.put(userID, kList);
      oCache.put(key, new byte[] {});
    } else committed = true;
    cache.put(key, buf);
    keysList.add(key);
  }
  /**
  deleteObject with key by userID
  @param userID String
  @param key String
  @exception Exception thrown by JAVA
  */
  public void deleteObject(String userID, String key) throws Exception {
    if (!keysList.contains(key)) throw new Exception(key+" is not existed.");
    if (oCache.containsKey(key)) throw new Exception("Object of "+key+" was modified but uncommitted.");
    List<String> kList = keysLocked.get(userID);
    if (kList == null || !kList.contains(key)) {
      if (lockedList.contains(key)) throw new Exception(key+" is locked by other.");
      throw new Exception(key+" is unlocked.");
    }
    if (!auto) {
      if (cache.containsKey(key)) oCache.put(key, cache.remove(key));
      else { // from file
        byte[] buf = new byte[sizes.get(key)];
        raf.seek(pointers.get(key));
        raf.read(buf);
        oCache.put(key, buf);
      }
    } else committed = true;
    keysList.remove(key);
  }
  /**
  updateObject
  @param userID String
  @param key String
  @param obj serializable object
  @exception Exception thrown by JAVA
  */
  public void updateObject(String userID, String key, Object obj) throws Exception {
      ByteArrayOutputStream bao = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(bao);
      oos.writeObject(obj);
      oos.flush();
      oos.close();
      updateObject(userID, key, bao.toByteArray());
  }
  /**
  updateObject
  @param userID String
  @param key String
  @param buf byte array for (non)serializable object
  @exception Exception thrown by JAVA
  */
  public void updateObject(String userID, String key, byte[] buf) throws Exception {
    if (!keysList.contains(key)) throw new Exception(key+" is not existed.");
    if (oCache.containsKey(key)) throw new Exception("Object of "+key+" was modified but uncommitted.");
    List<String> kList = keysLocked.get(userID);
    if (kList == null || !kList.contains(key)) {
      if (lockedList.contains(key)) throw new Exception(key+" is locked by other.");
      throw new Exception(key+" is unlocked.");
    }
    if (!auto) {
      if (cache.containsKey(key)) oCache.put(key, cache.get(key));
      else { // from file
        byte[] bb = new byte[sizes.get(key)];
        raf.seek(pointers.get(key));
        raf.read(bb);
        oCache.put(key, bb);
      }
    } else committed = true;
    cache.put(key, buf);
  }
  /**
  commit transaction of the given key.
  @param userID String
  @param key String
  @return boolean true if successful
  */
  public boolean commit(String userID, String key) {
    List<String> kList = keysLocked.get(userID);
    if (auto || kList == null || !kList.contains(key) || oCache.remove(key) == null) return false;
    committed = true;
    return true;     
  }
  /**
  commit all uncommitted transactions
  @param userID String
  */
  public void commitAll(String userID) {
    List<String> kList = keysLocked.get(userID);
    if (auto || kList == null || oCache.size() == 0) return;
    for (String key : kList) if (oCache.remove(key) != null) committed = true;
  }
  /** 
  rollback() rollbacks the LAST modified/added action
  @param userID String
  @param key String, key of object to be rollbacked
  @return boolean true if rollback is successful, false: unknown key, no rollback
  */
  public boolean rollback(String userID, String key) {
    List<String> kList = keysLocked.get(userID);
    if (auto || kList == null || !kList.contains(key) || oCache.size() == 0) return false;
    byte[] bb = oCache.remove(key);
    if (bb == null) return false;
    if (bb.length == 0) { // add ?
      keysList.remove(key);
      cache.remove(key);
      return true;
    }
    // update or delete. Restore key if it's delete
    if (!keysList.contains(key)) keysList.add(key);
    cache.put(key, bb);
    return true;
  }
  /** 
  rollback all uncommitted transactions
  @param userID String
  */
  public void rollbackAll(String userID) {
    List<String> kList = keysLocked.get(userID);
    if (auto || kList == null || oCache.size() == 0) return;
    for (String key : kList) {
      byte[] bb = oCache.remove(key);
      if (bb != null) {
        if (bb.length == 0) {
          keysList.remove(key);
          cache.remove(key);
        } else { // update or delete. Restore key if it's delete
          if (!keysList.contains(key)) keysList.add(key);
          cache.put(key, bb);
        }
      }
    }
  }
  /**
  open NanoDB.
  <br>If the specified fName from Constructor does not exist, it will be created with this fName.
  @exception Exception thrown by JAVA
  */
  public void open() throws Exception {
    cache.clear();
    oCache.clear();
    keysList = Collections.synchronizedList(new ArrayList<String>(512));
    pointers = new ConcurrentHashMap<>(512);
    sizes = new ConcurrentHashMap<>(512);
    // load keysList, pointers and sizes
    existed = (new File(fName)).exists();
    raf = new RandomAccessFile(fName, "rw");
    fLocked = raf.getChannel().lock();
    // start watchdog
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() { // watch for the unexpected
        if (raf != null) try {
           close( );
        } catch (Exception ex) { }
      }
    });
    if (!existed) return;
    cached = raf.length() < lim;
    long pt  = (long)raf.readInt();
    byte[] all = new byte[(int)(pt-4)];
    raf.read(all); // get the KeysList block
    // keys block-format: keyLength + dataLength + key = 2+4+n bytes
    for (int kl, dl, d, i = 0; i < all.length; i += (6+kl)) {
      // compute key Length and Data leng
      kl = (((int)(all[i])   & 0xFF) * 0x100) |(((int)all[i+1]) & 0xFF);
      dl = (((int)(all[i+2]) & 0xFF) * 0x1000000)|(((int)(all[i+3])& 0xFF) * 0x10000)|
           (((int)(all[i+4]) & 0xFF) * 0x100) |(((int)all[i+5]) & 0xFF);
      // cache keysList and pointers and sizes
      String key = new String(all, i+6, kl, cs);
      if (cached) { // cached
        byte[] bb = new byte[dl];
        raf.read(bb); // read
        cache.put(key, bb);
      }
      pointers.put(key, pt);
      sizes.put(key, dl);
      keysList.add(key);
      pt += dl;
    }
  }
  /**
  close and save NanoDB.
  <br>If not autoCommit, all changes must be committed before close.
  <br>Otherwise all changes will be lost (empty NanoDB file if it must be created)
  @exception Exception thrown by JAVA
  */
  public void close() throws Exception {
    if (!committed) {
      fLocked.release();
      raf.close();
    } else { // committed, rollback the Uncommitted
      if (committed && oCache.size() > 0) { // something in oCache
        List<String> keys = new ArrayList<>(oCache.keySet());
        for (String key:keys) { // recover the uncommitted
          byte[] bb = oCache.remove(key);
          if (bb.length > 0) { // It's update
            if (cache.containsKey(key)) cache.replace(key, bb);
            else { // It's delete key
              keysList.add(key);
              cache.put(key, bb);
            }
          } else { // It's add
            cache.remove(key);
            keysList.remove(key);
          }
        }
      }
      if (cache.size() > 0) {
        String tmp = String.format("%s_tmp", fName);
        if (!existed || cached) {
          fLocked.release();
          raf.close(); 
          tmp = fName; // this fName
          // data in cache: so delete
          if (cached) (new File(fName)).delete();
        }
        ByteArrayOutputStream bao = new ByteArrayOutputStream(65536);
        RandomAccessFile rTmp = new RandomAccessFile(tmp, "rw");
        FileLock fL = rTmp.getChannel().lock();
        // the key block
        for (String key : keysList) {
          int kl = key.length();
          int dl = cache.containsKey(key)? cache.get(key).length:sizes.get(key);
          bao.write(new byte[] { (byte)(kl / 0x100),
                                 (byte) kl,
                                 (byte)(dl / 0x1000000),
                                 (byte)(dl / 0x10000),
                                 (byte)(dl / 0x100),
                                 (byte) dl
                               }
                   );
          bao.write(key.getBytes(cs));
        }
        bao.flush();
        long pt = 4+bao.size();
        rTmp.write(new byte[] {(byte)((int)pt/ 0x1000000),
                               (byte)((int)pt / 0x10000),
                               (byte)((int)pt / 0x100),
                               (byte) (int)pt
                              }
                  );
        rTmp.write(bao.toByteArray());
        bao.close(); // release bao
        // now the data block
        for (String key : keysList) {
          byte[] bb = cache.get(key);
          if (bb == null) {
            bb = new byte[sizes.get(key)];           
            raf.seek(pointers.get(key));
            raf.read(bb);
          }
          // save data
          rTmp.write(bb, 0, bb.length);
        }
        fL.release();
        rTmp.close();
        if (existed && !cached) {
          fLocked.release();
          raf.close();
          File fi = new File(fName);
          fi.delete(); // delete the old
          TimeUnit.MICROSECONDS.sleep(20);
          (new File(tmp)).renameTo(fi);
        } 
      }
    }
    keysLocked.clear();
    lockedList.clear();
    keysList.clear();
    pointers.clear();
    oCache.clear();
    sizes.clear();
    cache.clear();
    raf = null;
  }
  /**
  removeLockedKeys of userID
  @param userID String
  */
  public void removeLockedKeys(String userID) {
    List<String> kList = keysLocked.remove(userID); // get locked keyList
    if (kList != null) for (String key:kList) lockedList.remove(key);
  }
  //---------------------------------------------------------------------------------------
  private ConcurrentHashMap<String, List<String>> keysLocked =  new ConcurrentHashMap<>(256);
  private volatile boolean existed = false, cached = false, committed = false, auto = false;
  private List<String> lockedList = Collections.synchronizedList(new ArrayList<>());
  private ConcurrentHashMap<String, byte[]> oCache =  new ConcurrentHashMap<>(256);
  private ConcurrentHashMap<String, byte[]> cache = new ConcurrentHashMap<>(256);
  private ConcurrentHashMap<String, Integer> sizes;
  private ConcurrentHashMap<String, Long> pointers;
  private List<String> keysList;
  private RandomAccessFile raf;
  private int lim = 0x200000;
  private FileLock fLocked;
  private String fName;
  private Charset cs;
}
