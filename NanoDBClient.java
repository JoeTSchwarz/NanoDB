import java.io.*;
import java.awt.*;
import java.net.URL;
import javax.swing.*;
import java.awt.event.*;
import javax.swing.text.DefaultCaret;
//
import nanodb.NanoDBConnect;
// Joe T. Schwarz(C)
public class NanoDBClient extends JFrame {
  private boolean isPeople;
  private NanoDBConnect con;
  private String nanoDB = "People";
  private java.util.List<String> keys;
  //
  public NanoDBClient( ) {
    setTitle("NanoDB: FileChannel VERSUS RandomAccessFile");
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
    JLabel lab = new JLabel("  No connection to NanoDBServer");
        
    JButton open = new JButton("OPEN");
    open.setEnabled(false);
    open.setPreferredSize(new Dimension(250, 25));
    open.addActionListener(a -> { // open NanoDB
      nanoDB = JOptionPane.showInputDialog(this, "NanoDB ?", nanoDB);
      if (nanoDB == null) return;
      isPeople = nanoDB.equals("People");
      accMode = "NanoDB "+nanoDB+": ";
      open.setText("OPEN "+nanoDB);
      try {
        long t0 = System.nanoTime();
        String uID = con.open(nanoDB, "UTF-8");
        double d = ((double)System.nanoTime()-t0)/1000000;
        jta.append(String.format("Assigned ID: %s\n%s open(%s) by %s. Elapsed time: %.03f milliSec.\n",
                   uID, accMode,nanoDB,nanoDB, d));
        open.setEnabled(false);
        enabled(true);
      } catch (Exception ex) {
        ex.printStackTrace();
        System.exit(0);
      }
    });
    lst = new JButton("GET KEYS");
    lst.setPreferredSize(new Dimension(250, 25));
    lst.addActionListener(a -> {
      try {
        long t0 = System.nanoTime();
        keys = con.getKeys(nanoDB);
        double d = ((double)System.nanoTime()-t0)/1000000;
        jta.append(String.format("%s getKeys(). Elapsed time: %.03f milliSec.\n",accMode,d));
        for (String key:keys) jta.append("- "+key+"\n");
      } catch (Exception ex) {
        jta.append(ex.toString()+"\n");
      }
    });
    read = new JButton("READ");
    read.setPreferredSize(new Dimension(250, 25));
    read.addActionListener(a -> {
      String key = JOptionPane.showInputDialog(this, "Key Name:");
      if (key != null) {
        try {
          long t0 = System.nanoTime();
          Object obj = con.readObject(nanoDB, key);
          double d = ((double)System.nanoTime()-t0)/1000000;
          String time = String.format("%s read(%s, %s). Elapsed time: %.03f milliSec.\n",
                                      accMode,nanoDB, key,d);
          jta.append(time+"Data from key:"+key+"\n"+
                          (isPeople?((People)obj).toString():new String((byte[])obj))+"\n");
          if (obj instanceof People) ((People)obj).picture(this);
        } catch (Exception ex) {
          jta.append(ex.toString()+"\n");
        }
      }
    });
    add = new JButton("ADD");
    add.setPreferredSize(new Dimension(250, 25));
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
            con.addObject(nanoDB, key, new People(key, inp));
            d = ((double)System.nanoTime()-t0)/1000000;
          } else {
            t0 = System.nanoTime();
            con.addObject(nanoDB, key, inp.getBytes());
            d = ((double)System.nanoTime()-t0)/1000000;
          }
          jta.append(String.format("%s add(%s, %s). Elapsed time: %.03f milliSec.\n",
                                   accMode,nanoDB, key,d));
        } catch (Exception ex) {
          jta.append(ex.toString()+"\n");
        }
      }
    });
    del = new JButton("DELETE");
    del.setPreferredSize(new Dimension(250, 25));
    del.addActionListener(a -> {
      String key = JOptionPane.showInputDialog(this, "Key Name:");
      if (key != null) {
        try {
          long t0 = System.nanoTime();
          con.deleteObject(nanoDB, key);
          double d = ((double)System.nanoTime()-t0)/1000000;
          jta.append(String.format("%s delete(%s, %s). Elapsed time: %.03f milliSec.\n",
                     accMode,nanoDB, key,d));
        } catch (Exception ex) {
          jta.append(ex.toString()+"\n");
        }
      }
    });
    upd = new JButton("UPDATE");
    upd.setPreferredSize(new Dimension(250, 25));
    upd.addActionListener(a -> {
      String key = JOptionPane.showInputDialog(this, "Key Name:");
      if (key != null) {
        try {
          String data = null;
          if (isPeople) {
            String inp[] = ((People)con.readObject(nanoDB, key)).getData();
            data = JOptionPane.showInputDialog(this, "Image URL:", inp[1]);
          } else {
            data = new String((byte[])con.readObject(nanoDB, key));
            data = JOptionPane.showInputDialog(this, "Any text:", data);
          }
          if (data == null) return;
          long t0 = 0;
          double d = 0;
          if (isPeople) { // People
            t0 = System.nanoTime();
            con.updateObject(nanoDB, key, new People(key, data));
            d = (double)(System.nanoTime()-t0)/1000000;
          } else { // String
            t0 = System.nanoTime();
            con.updateObject(nanoDB, key, data.getBytes());
            d = (double)(System.nanoTime()-t0)/1000000;
          }
          jta.append(String.format("%s update(%s, %s). Elapsed time: %.03f milliSec.\n",
                     accMode, nanoDB, key,d));
        } catch (Exception ex) {
          jta.append(ex.toString()+"\n");
        }
      }
    });
    loop = new JButton("LOCK/READ/UPDATE/COMMIT/UNLOCK");
    loop.setPreferredSize(new Dimension(250, 25));
    loop.addActionListener(a -> {
      try {
        keys = con.getKeys(nanoDB);
        if (keys != null && keys.size() > 0) {
          jta.append("Read & Update "+keys.size()+" Keys\n");
          long t0 = System.nanoTime();
          for (String key : keys) {
            con.lock(nanoDB, key);
            con.updateObject(nanoDB, key, con.readObject(nanoDB, key));
            con.commit(nanoDB, key);
            con.unlock(nanoDB, key);
          }
          double d = (double)(System.nanoTime()-t0)/1000000;
          jta.append(String.format("%s LOCK/READ/UPDATE/COMMIT/UNLOCK. Elapsed time: %.03f milliSec.\n"+
                                   "Average: %.03f milliSec.\n",accMode,d,(d/keys.size())));
        }
      } catch (Exception ex) {
        jta.append(ex.toString()+"\n");
      }
    });
    aut = new JButton("AUTOCOMMIT");
    aut.setPreferredSize(new Dimension(250, 25));
    aut.addActionListener(a -> {
      try {
        int au = JOptionPane.showConfirmDialog(this, "autoCommit", "NanoDB", JOptionPane.YES_NO_OPTION);
        boolean bool = au == JOptionPane.YES_OPTION;
        long t0 = System.nanoTime();
        con.autoCommit(nanoDB, bool);
        double d = ((double)System.nanoTime()-t0)/1000000;
        jta.append(String.format("%s autoCommit(%b). Elapsed time: %.03f milliSec.\n",accMode,bool,d));
      } catch (Exception ex) {
        jta.append(ex.toString()+"\n");
      }
    });
    com = new JButton("COMMIT");
    com.setPreferredSize(new Dimension(250, 25));
    com.addActionListener(a -> {
      String key = JOptionPane.showInputDialog(this, "Key Name:");
      if (key != null) try {
        long t0 = System.nanoTime();
        boolean b = con.commit(nanoDB, key);
        double d = ((double)System.nanoTime()-t0)/1000000;
        jta.append(String.format("%s commit(%s, %s): %b. Elapsed time: %.03f milliSec.\n",
                                 accMode,nanoDB, key,b, d));
      } catch (Exception ex) {
        jta.append(ex.toString()+"\n");
      }
    });
    call = new JButton("COMMIT ALL");
    call.setPreferredSize(new Dimension(250, 25));
    call.addActionListener(a -> {
      try {
        long t0 = System.nanoTime();
        con.commitAll(nanoDB);
        double d = ((double)System.nanoTime()-t0)/1000000;
        jta.append(String.format("%s commitAll(%s). Elapsed time: %.03f milliSec.\n",
                                 accMode,nanoDB, d));
      } catch (Exception ex) {
        jta.append(ex.toString()+"\n");
      }
    });
    roll = new JButton("ROLLBACK");
    roll.setPreferredSize(new Dimension(250, 25));
    roll.addActionListener(a -> {
      String key = JOptionPane.showInputDialog(this, "Key Name:");
      if (key != null) try {
        long t0 = System.nanoTime();
        boolean b = con.rollback(nanoDB, key);
        double d = ((double)System.nanoTime()-t0)/1000000;
        jta.append(String.format("%s rollback(%s, %s): %b. Elapsed time: %.03f milliSec.\n",
                   accMode,nanoDB, key,b,d));
      } catch (Exception ex) {
        jta.append(ex.toString()+"\n");
      }
    });
    rall = new JButton("ROLLBACK ALL");
    rall.setPreferredSize(new Dimension(250, 25));
    rall.addActionListener(a -> {
      try {
        long t0 = System.nanoTime();
        con.rollbackAll(nanoDB);
        double d = ((double)System.nanoTime()-t0)/1000000;
        jta.append(String.format("%s rollbackAll(%s). Elapsed time: %.03f milliSec.\n",
                   accMode,nanoDB, d));
      } catch (Exception ex) {
        jta.append(ex.toString()+"\n");
      }
    });
    isAut = new JButton("isAutoCommit");
    isAut.setPreferredSize(new Dimension(250, 25));
    isAut.addActionListener(a -> {
      try {
        long t0 = System.nanoTime();
        boolean b = con.isAutoCommit(nanoDB);
        double d = ((double)System.nanoTime()-t0)/1000000;
        jta.append(String.format("%s isAutoCommit(): %b. Elapsed time: %.03f milliSec.\n",accMode,b,d));
      } catch (Exception ex) {
        jta.append(ex.toString()+"\n");
      }
    });
    isLck = new JButton("isLocked");
    isLck.setPreferredSize(new Dimension(250, 25));
    isLck.addActionListener(a -> {
      String key = JOptionPane.showInputDialog(this, "Key Name:");
      if (key != null) try {
        long t0 = System.nanoTime();
        boolean b = con.isLocked(nanoDB, key);
        double d = ((double)System.nanoTime()-t0)/1000000;
        jta.append(String.format("%s isLocked(): %b. Elapsed time: %.03f milliSec.\n",accMode,b,d));
      } catch (Exception ex) {
        jta.append(ex.toString()+"\n");
      }
    });
    isExt = new JButton("isExisted");
    isExt.setPreferredSize(new Dimension(250, 25));
    isExt.addActionListener(a -> {
      String key = JOptionPane.showInputDialog(this, "Key Name:");
      if (key != null) try {
        long t0 = System.nanoTime();
        boolean b = con.isExisted(nanoDB, key);
        double d = ((double)System.nanoTime()-t0)/1000000;
        jta.append(String.format("%s isExisted(%s): %b. Elapsed time: %.03f milliSec.\n",
                   accMode,key,b,d));
      } catch (Exception ex) {
        jta.append(ex.toString()+"\n");
      }
    });
    isDel = new JButton("isKeyDeleted");
    isDel.setPreferredSize(new Dimension(250, 25));
    isDel.addActionListener(a -> {
      String key = JOptionPane.showInputDialog(this, "Key Name:");
      if (key != null) try {
        long t0 = System.nanoTime();
        boolean b = con.isKeyDeleted(nanoDB, key);
        double d = ((double)System.nanoTime()-t0)/1000000;
        jta.append(String.format("%s isKeyDeleted(%s): %b. Elapsed time: %.03f milliSec.\n",
                   accMode,key,b,d));
      } catch (Exception ex) {
        jta.append(ex.toString()+"\n");
      }
    });
    lck = new JButton("LOCK");
    lck.setPreferredSize(new Dimension(250, 25));
    lck.addActionListener(a -> {
      String key = JOptionPane.showInputDialog(this, "Key Name:");
      if (key != null) try {
        long t0 = System.nanoTime();
        boolean b = con.lock(nanoDB, key);
        double d = ((double)System.nanoTime()-t0)/1000000;
        jta.append(String.format("%s lock(%s, %s): %b. Elapsed time: %.03f milliSec.\n",
                   accMode,nanoDB, key, b, d));
      } catch (Exception ex) {
        jta.append(ex.toString()+"\n");
      }
    });
    unlck = new JButton("UNLOCK");
    unlck.setPreferredSize(new Dimension(250, 25));
    unlck.addActionListener(a -> {
      String key = JOptionPane.showInputDialog(this, "Key Name:");
      if (key != null) try {
        long t0 = System.nanoTime();
        boolean b = con.unlock(nanoDB, key);
        double d = ((double)System.nanoTime()-t0)/1000000;
        jta.append(String.format("%s unlock(%s, %s): %b. Elapsed time: %.03f milliSec.\n",
                   accMode,nanoDB, key, b, d));
      } catch (Exception ex) {
        jta.append(ex.toString()+"\n");
      }
    });
    close = new JButton("CLOSE");
    close.setPreferredSize(new Dimension(250, 25));
    close.addActionListener(a -> {
      try {
        con.close(nanoDB);
      } catch (Exception ex) { }
      open.setText("OPEN");
      open.setEnabled(true);
      enabled(false);
    });
    
    JButton exit = new JButton("EXIT");
    exit.setPreferredSize(new Dimension(250, 25));
    exit.addActionListener(a -> {
      if (con != null) try {
        con.close(nanoDB);
        con.disconnect();
      } catch (Exception ex) { }
      con = null;
      System.exit(0);
    });
    //
    JPanel bPanel = new JPanel();
    bPanel.setLayout(new GridLayout(21,1));
    
    bPanel.add(lab);
    bPanel.add(open); bPanel.add(lst); bPanel.add(read); bPanel.add(add); bPanel.add(del);
    bPanel.add(upd); bPanel.add(loop);bPanel.add(aut); bPanel.add(com); bPanel.add(call);
    bPanel.add(roll); bPanel.add(rall); bPanel.add(isAut); bPanel.add(isExt); bPanel.add(isDel);
    bPanel.add(isLck); bPanel.add(lck); bPanel.add(unlck); bPanel.add(close); bPanel.add(exit);
    
    Container container = getContentPane();
    //container.setPreferredSize(new Dimension(600, 600));
    GroupLayout groupLayout = new GroupLayout(container);  
  
    container.setLayout(groupLayout);  
    groupLayout.setHorizontalGroup(  
                    groupLayout.createSequentialGroup()  
                               .addComponent(bPanel) 
                               .addGap(5)                                
                               .addComponent(sp));  
                                 
    groupLayout.setVerticalGroup(  
                    groupLayout.createParallelGroup(GroupLayout.Alignment.BASELINE)  
                               .addComponent(bPanel)  
                               .addComponent(sp));  
      
    //setUndecorated(true);
    getRootPane().setBorder(BorderFactory.createMatteBorder(5, 5, 5, 5, Color.LIGHT_GRAY));
    setLocation(0, 0);
    pack();
    enabled(false);
    setVisible(true);
    // dialog for Host & port
    JDialog dia = new JDialog(this, "NanoDBConnect");
    JTextField jhost = new JTextField("localhost");
    jhost.setPreferredSize(new Dimension(100, 20));
    JTextField jport = new JTextField("9999");
    jport.setPreferredSize(new Dimension(100, 20));
    jport.addKeyListener(new KeyAdapter() {
      public void keyTyped(KeyEvent e) {
        char c = e.getKeyChar();
        if (c < '0' || c > '9') e.consume(); 
      }
    });
    JButton ok = new JButton("OK");
    ok.addActionListener(b -> {
      try {
        con = new NanoDBConnect(jhost.getText().trim(), Integer.parseInt(jport.getText().trim()));
        lab.setText("  NanoDBServer @"+jhost.getText().trim()+":"+jport.getText().trim());
        open.setEnabled(true);
      } catch (Exception ex) {
        JOptionPane.showMessageDialog(this, "Unable to connect to "+jhost.getText()+":"+jport.getText(),
                                      "ERROR", JOptionPane.ERROR_MESSAGE);
        System.exit(0);
      }
      dia.dispose();
    });
    JPanel pn = new JPanel();
    pn.setLayout(new GridLayout(2, 2));
    pn.add(new Label("HostName/IP")); pn.add(jhost);
    pn.add(new Label("PortNumber")); pn.add(jport);
    dia .addWindowListener(new WindowAdapter() {
      public void windowClosing(WindowEvent we) {
        dia.dispose();
        System.exit(0);
      }
    });
    JPanel ps = new JPanel();
    ps.add(ok);
    dia.add("North", pn);
    dia.add("South", ps);
    dia.getRootPane().setBorder(BorderFactory.createMatteBorder(5, 5, 5, 5, Color.RED));
    
    dia.pack();
    dia.setVisible(true);       
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
    new NanoDBClient( );
  }
}