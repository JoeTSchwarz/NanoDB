import java.io.*;
import java.awt.*;
import java.net.URL;
import javax.swing.*;
import javax.swing.text.DefaultCaret;
//
import nanodb.NanoDB;
// Joe T. Schwarz(C)
public class NanoDBdirect extends JFrame {
  private NanoDB nano;
  private boolean isPeople;
  private java.util.List<String> keys;
  private String nanoDB = "People", userID = "NanoDB";
  //
  public NanoDBdirect() {
    setTitle("NanoDB");
    setDefaultCloseOperation(JFrame.DO_NOTHING_ON_CLOSE);
    //
    JTextArea jta = new JTextArea(35, 55);
    DefaultCaret caret = (DefaultCaret) jta.getCaret();
    caret.setUpdatePolicy(DefaultCaret.ALWAYS_UPDATE);  
     jta.setText("NOTE:\n"+
                "- When a NanoDB is created and filled with add, it must be committed or set to \n"+
                "  autoCommit before closing. Otherwise everything will be lost.\n"+
                "- Before each transaction, the key must be locked and then released for other users.\n"+
                "- Transaction like add, delete and update must be commit or rollback before close.\n"+
                "  Otherwise this transaction will be lost.\n"+
                "- If Object is added, its key is locked and must be unlocked for other users.\n"+
                "- If Object is deleted, its key is kept locked and must be unlocked for release.\n");
    JScrollPane sp =  new JScrollPane(jta,
                                      ScrollPaneConstants.VERTICAL_SCROLLBAR_AS_NEEDED,
                                      ScrollPaneConstants.HORIZONTAL_SCROLLBAR_AS_NEEDED
                                     );
    jta.setEditable(false);
    JButton open = new JButton("OPEN");
    open.setPreferredSize(new Dimension(300, 30));
    open.addActionListener(a -> { // open NanoDB
      nanoDB = JOptionPane.showInputDialog(this, "NanoDB ?", nanoDB);
      if (nanoDB == null) return;
      isPeople = nanoDB.equals("People");
      accMode = "NanoDB "+nanoDB+": ";
      open.setText("OPEN "+nanoDB);
      try {
        long t0 = System.nanoTime();
        nano = new NanoDB(nanoDB);
        nano.open();
        double d = ((double)System.nanoTime()-t0)/1000000;
        jta.append(String.format("%s open(%s) by %s. Elapsed time: %.03f milliSec.\n",
                   accMode,nanoDB,userID, d));
        open.setEnabled(false);
        enabled(true);
      } catch (Exception ex) {
        ex.printStackTrace();
        System.exit(0);
      }
    });
    lst = new JButton("GET KEYS");
    lst.setPreferredSize(new Dimension(300, 30));
    lst.addActionListener(a -> {
        long t0 = System.nanoTime();
        keys = nano.getKeys();
        double d = ((double)System.nanoTime()-t0)/1000000;
        jta.append(String.format("%s getKeys(). Elapsed time: %.03f milliSec.\n",accMode,d));
        for (String key:keys) jta.append("- "+key+"\n");
    });
    read = new JButton("READ");
    read.setPreferredSize(new Dimension(300, 30));
    read.addActionListener(a -> {
      String key = JOptionPane.showInputDialog(this, "Key Name:");
      if (key != null) {
        try {
          long t0 = System.nanoTime();
          byte[] bb = nano.readObject(userID, key);
          double d = ((double)System.nanoTime()-t0)/1000000;
          String time = String.format("%s read(%s, %s). Elapsed time: %.03f milliSec.\n",
                                      accMode,userID, key,d);
          Object obj = toObject(bb);
          jta.append(time+"Data from key:"+key+"\n"+
                          (isPeople?((People)obj).toString():(String)obj+"\n"));
          if (obj instanceof People) ((People)obj).picture(this);
        } catch (Exception ex) {
          JOptionPane.showMessageDialog(this, "Unable to read key:"+key+". Reason:"+ex.toString());
        }
      }
    });
    add = new JButton("ADD");
    add.setPreferredSize(new Dimension(300, 30));
    add.addActionListener(a -> {
      String key = JOptionPane.showInputDialog(this, "Key Name:");
      if (key != null) {
        String inp;
        if (isPeople) {
          inp = JOptionPane.showInputDialog(this, "Image URL:");
        } else {
          inp = JOptionPane.showInputDialog(this, "Any text:");
        }
        if (inp == null) return;
        try {
          long t0 = 0;
          double d = 0;
          if (isPeople) {
            t0 = System.nanoTime();
            nano.addObject(userID, key, new People(key, inp));
            d = ((double)System.nanoTime()-t0)/1000000;
          } else {
            t0 = System.nanoTime();
            nano.addObject(userID, key, inp.getBytes());
            d = ((double)System.nanoTime()-t0)/1000000;
          }
          jta.append(String.format("%s add(%s, %s). Elapsed time: %.03f milliSec.\n",
                                   accMode,userID, key,d));
        } catch (Exception ex) {
          JOptionPane.showMessageDialog(this, "Unable to add key:"+key+". Reason:"+ex.toString());
        }
      }
    });
    del = new JButton("DELETE");
    del.setPreferredSize(new Dimension(300, 30));
    del.addActionListener(a -> {
      String key = JOptionPane.showInputDialog(this, "Key Name:");
      if (key != null) {
        try {
          long t0 = System.nanoTime();
          nano.deleteObject(userID, key);
          double d = ((double)System.nanoTime()-t0)/1000000;
          jta.append(String.format("%s delete(%s, %s). Elapsed time: %.03f milliSec.\n",
                     accMode,userID, key,d));
        } catch (Exception ex) {
          JOptionPane.showMessageDialog(this, "Unable to delete key:"+key+". Reason:"+ex.toString());
        }
      }
    });
    upd = new JButton("UPDATE");
    upd.setPreferredSize(new Dimension(300, 30));
    upd.addActionListener(a -> {
      String key = JOptionPane.showInputDialog(this, "Key Name:");
      if (key != null) {
        try {
          String data = null;
          if (isPeople) {
            String inp[] = ((People)toObject(nano.readObject(userID, key))).getData();
            data = JOptionPane.showInputDialog(this, "Image URL:", inp[1]);
          } else {
            data = new String(nano.readObject(userID, key));
            data = JOptionPane.showInputDialog(this, "Any text:", data);
          }
          if (data == null) return;
          long t0 = 0;
          double d = 0;
          if (isPeople) { // People
            t0 = System.nanoTime();
            nano.updateObject(userID, key, new People(key, data));
            d = (double)(System.nanoTime()-t0)/1000000;
          } else { // String
            t0 = System.nanoTime();
            nano.updateObject(userID, key, data.getBytes());
            d = (double)(System.nanoTime()-t0)/1000000;
          }
          jta.append(String.format("%s update(%s, %s). Elapsed time: %.03f milliSec.\n",
                     accMode, userID, key,d));
        } catch (Exception ex) {
          JOptionPane.showMessageDialog(this, "Unable to update key:"+key+". Reason:"+ex.toString());
        }
      }
    });
    loop = new JButton("LOCK/READ/UPDATE/COMMIT/UNLOCK");
    loop.setPreferredSize(new Dimension(300, 30));
    loop.addActionListener(a -> {
      keys = nano.getKeys();
      if (keys != null && keys.size() > 0) {
        jta.append("Read & Update "+keys.size()+" Keys\n");
        long t0 = System.nanoTime();
        for (String key : keys) {
          try {
            nano.lock(userID, key);
            nano.updateObject(userID, key, nano.readObject(userID, key));
            nano.commit(userID, key);
            nano.unlock(userID, key);
          } catch (Exception ex) {
            JOptionPane.showMessageDialog(this, "Unable to eork with key:"+key+". Reason:"+ex.toString());
          }
        }
        double d = (double)(System.nanoTime()-t0)/1000000;
        jta.append(String.format("%s LOCK/READ/UPDATE/COMMIT/UNLOCK. Elapsed time: %.03f milliSec.\n"+
                                 "Average: %.03f milliSec.\n",accMode,d,(d/keys.size())));
      }
    });
    aut = new JButton("AUTOCOMMIT");
    aut.setPreferredSize(new Dimension(300, 30));
    aut.addActionListener(a -> {
      int au = JOptionPane.showConfirmDialog(this, "autoCommit", "NanoDB", JOptionPane.YES_NO_OPTION);
      boolean bool = au == JOptionPane.YES_OPTION;
      long t0 = System.nanoTime();
      nano.autoCommit(bool);
      double d = ((double)System.nanoTime()-t0)/1000000;
      jta.append(String.format("%s autoCommit(%b). Elapsed time: %.03f milliSec.\n",accMode,bool,d));
    });
    com = new JButton("COMMIT");
    com.setPreferredSize(new Dimension(300, 30));
    com.addActionListener(a -> {
      String key = JOptionPane.showInputDialog(this, "Key Name:");
      if (key != null) {
        long t0 = System.nanoTime();
        boolean b = nano.commit(userID, key);
        double d = ((double)System.nanoTime()-t0)/1000000;
        jta.append(String.format("%s commit(%s, %s): %b. Elapsed time: %.03f milliSec.\n",
                                 accMode,userID, key,b, d));
      }
    });
    call = new JButton("COMMIT ALL");
    call.setPreferredSize(new Dimension(250, 25));
    call.addActionListener(a -> {
      try {
        long t0 = System.nanoTime();
        nano.commitAll(userID);
        double d = ((double)System.nanoTime()-t0)/1000000;
        jta.append(String.format("%s commitAll(%s). Elapsed time: %.03f milliSec.\n",
                                 accMode,nanoDB, d));
      } catch (Exception ex) {
        jta.append(ex.toString()+"\n");
      }
    });
    roll = new JButton("ROLLBACK");
    roll.setPreferredSize(new Dimension(300, 30));
    roll.addActionListener(a -> {
      String key = JOptionPane.showInputDialog(this, "Key Name:");
      if (key != null) {
        long t0 = System.nanoTime();
        boolean b = nano.rollback(userID, key);
        double d = ((double)System.nanoTime()-t0)/1000000;
        jta.append(String.format("%s rollback(%s, %s): %b. Elapsed time: %.03f milliSec.\n",
                   accMode,userID, key,b,d));
      }
    });
    rall = new JButton("ROLLBACK ALL");
    rall.setPreferredSize(new Dimension(250, 25));
    rall.addActionListener(a -> {
      try {
        long t0 = System.nanoTime();
        nano.rollbackAll(userID);
        double d = ((double)System.nanoTime()-t0)/1000000;
        jta.append(String.format("%s rollbackAll(%s). Elapsed time: %.03f milliSec.\n",
                   accMode,nanoDB, d));
      } catch (Exception ex) {
        jta.append(ex.toString()+"\n");
      }
    });
    isAut = new JButton("isAutoCommit");
    isAut.setPreferredSize(new Dimension(300, 30));
    isAut.addActionListener(a -> {
      long t0 = System.nanoTime();
      boolean b = nano.isAutoCommit( );
      double d = ((double)System.nanoTime()-t0)/1000000;
      jta.append(String.format("%s isAutoCommit(): %b. Elapsed time: %.03f milliSec.\n",accMode,b,d));
    });
    isLck = new JButton("isLocked");
    isLck.setPreferredSize(new Dimension(300, 30));
    isLck.addActionListener(a -> {
      String key = JOptionPane.showInputDialog(this, "Key Name:");
      if (key == null) return;
      long t0 = System.nanoTime();
      boolean b = nano.isLocked(key);
      double d = ((double)System.nanoTime()-t0)/1000000;
      jta.append(String.format("%s isLocked(): %b. Elapsed time: %.03f milliSec.\n",accMode,b,d));
    });
    isExt = new JButton("isExisted");
    isExt.setPreferredSize(new Dimension(300, 30));
    isExt.addActionListener(a -> {
      String key = JOptionPane.showInputDialog(this, "Key Name:");
      if (key != null) {
        long t0 = System.nanoTime();
        boolean b = nano.isExisted(key);
        double d = ((double)System.nanoTime()-t0)/1000000;
        jta.append(String.format("%s isExisted(%s): %b. Elapsed time: %.03f milliSec.\n",
                   accMode,key,b,d));
      }
    });
    isDel = new JButton("isKeyDeleted");
    isDel.setPreferredSize(new Dimension(300, 30));
    isDel.addActionListener(a -> {
      String key = JOptionPane.showInputDialog(this, "Key Name:");
      if (key != null) {
        long t0 = System.nanoTime();
        boolean b = nano.isKeyDeleted(key);
        double d = ((double)System.nanoTime()-t0)/1000000;
        jta.append(String.format("%s isKeyDeleted(%s): %b. Elapsed time: %.03f milliSec.\n",
                   accMode,key,b,d));
      }
    });
    lck = new JButton("LOCK");
    lck.setPreferredSize(new Dimension(300, 30));
    lck.addActionListener(a -> {
      String key = JOptionPane.showInputDialog(this, "Key Name:");
      if (key == null) return;
      long t0 = System.nanoTime();
      boolean b = nano.lock(userID, key);
      double d = ((double)System.nanoTime()-t0)/1000000;
      jta.append(String.format("%s lock(%s, %s): %b. Elapsed time: %.03f milliSec.\n",
                 accMode,userID, key, b, d));
    });
    unlck = new JButton("UNLOCK");
    unlck.setPreferredSize(new Dimension(300, 30));
    unlck.addActionListener(a -> {
      String key = JOptionPane.showInputDialog(this, "Key Name:");
      if (key == null) return;
      long t0 = System.nanoTime();
      boolean b = nano.unlock(userID, key);
      double d = ((double)System.nanoTime()-t0)/1000000;
      jta.append(String.format("%s unlock(%s, %s): %b. Elapsed time: %.03f milliSec.\n",
                 accMode,userID, key, b, d));
    });
    close = new JButton("CLOSE");
    close.setPreferredSize(new Dimension(300, 30));
    close.addActionListener(a -> {
      if (nano != null) try {
        nano.removeLockedKeys(userID);
        nano.close( );
      } catch (Exception ex) { }
      open.setText("OPEN");
      open.setEnabled(true);
      enabled(false);
      nano = null;
    });   
    JButton exit = new JButton("EXIT");
    exit.setPreferredSize(new Dimension(300, 30));
    exit.addActionListener(a -> {
      if (nano != null) try {
        nano.close();
      } catch (Exception ex) { }
      System.exit(0);
    });
    JPanel bPanel = new JPanel();
    bPanel.setLayout(new GridLayout(21,1));
    bPanel.add(new JLabel("Access Mode"));
    bPanel.add(open); bPanel.add(lst); bPanel.add(read); bPanel.add(add); bPanel.add(del);
    bPanel.add(upd); bPanel.add(loop);bPanel.add(aut); bPanel.add(com); bPanel.add(call);
    bPanel.add(roll); bPanel.add(rall); bPanel.add(isAut); bPanel.add(isExt); bPanel.add(isDel);
    bPanel.add(isLck); bPanel.add(lck); bPanel.add(unlck); bPanel.add(close); bPanel.add(exit);
    
    Container contentPanel = getContentPane();  
    GroupLayout groupLayout = new GroupLayout(contentPanel);  
  
    contentPanel.setLayout(groupLayout);  
    groupLayout.setHorizontalGroup(  
                    groupLayout.createSequentialGroup()  
                               .addComponent(bPanel) 
                               .addGap(5)                                
                               .addComponent(sp));  
                                 
    groupLayout.setVerticalGroup(  
                    groupLayout.createParallelGroup(GroupLayout.Alignment.BASELINE)  
                               .addComponent(bPanel)  
                               .addComponent(sp));  
      
    setLocation(0, 0);
    pack();
    enabled(false);
    setVisible(true);
  }
  //
  private Object toObject(byte[] bb) throws Exception {
    if (bb[0] != (byte)0xAC || bb[1] != (byte)0xED) return new String(bb); 
    // is a serialized object
    ObjectInputStream oi = new ObjectInputStream(new ByteArrayInputStream(bb));
    Object obj = oi.readObject();
    oi.close();
    return obj;
  }
  //
  private void enabled(boolean boo) {
    read.setEnabled(boo); add.setEnabled(boo); del.setEnabled(boo);
    upd.setEnabled(boo); loop.setEnabled(boo); com.setEnabled(boo);
    roll.setEnabled(boo); isLck.setEnabled(boo); isExt.setEnabled(boo);
    unlck.setEnabled(boo); lst.setEnabled(boo); isDel.setEnabled(boo);
    lck.setEnabled(boo); close.setEnabled(boo); aut.setEnabled(boo);
    isAut.setEnabled(boo); call.setEnabled(boo); rall.setEnabled(boo); 
  }
  //
  private String accMode;
  private JButton lst, read, add, del, upd, loop, aut, com, call, roll, rall;
  private JButton isAut, isDel, isLck, isExt, lck, unlck, close;
  //
  public static void main(String... args) throws Exception {
    UIManager.setLookAndFeel("com.sun.java.swing.plaf.nimbus.NimbusLookAndFeel");
    new NanoDBdirect();
  }
}
