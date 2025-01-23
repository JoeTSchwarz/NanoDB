import java.io.*;
import java.awt.*;
import java.net.URL;
import javax.swing.*;
import java.nio.file.*;
import java.awt.image.BufferedImage;
import javax.imageio.ImageIO;
// @author Joe T. Schwarz (c)
public class People implements java.io.Serializable {
  private static final long serialVersionUID = 1234L;
  public People(String name, String image) {
    this.name = name;
    this.image = image;
  }
  private String name, image;
  public String toString() {
    return "Name: "+name+", Image: "+image;
  }
  public String[] getData() {
    return new String[] {name, image };
  }
  public void print() {
    System.out.println("Name: "+name+System.lineSeparator()+", Link to Image:  "+image);
  }
  public void picture(JFrame jf) {
    (new Picture(jf)).setVisible(true);
  }
  class Picture extends JDialog {
    public Picture(JFrame jf) {
      setTitle(name);
      setDefaultCloseOperation(DISPOSE_ON_CLOSE);
      JButton but = getButton( );
      but.setContentAreaFilled(false);
      but.setBorderPainted(false);
      add(but, BorderLayout.CENTER);     
      setLocationRelativeTo(jf);
      pack();
    }
    //
    private JButton getButton( ) {
      try {
        int w, h;
        ByteArrayInputStream bis = null;
        if (image.indexOf("://") > 0) {
          byte[] buf = new byte[65536];
          ByteArrayOutputStream bao = new ByteArrayOutputStream();
          InputStream is = (new URL(image)).openStream();
          while ((w = is.read(buf)) != -1) bao.write(buf, 0, w);
          bis = new ByteArrayInputStream(bao.toByteArray());
        } else if ((new File(image)).exists()) {
          bis = new ByteArrayInputStream(Files.readAllBytes((new File(image)).toPath()));
        } else { // something wrong with image
          JButton but = new JButton("NO IMAGE");
          return but;
        }
        BufferedImage bImg = ImageIO.read(bis);
        w = bImg.getWidth(); h = bImg.getHeight();
        double ratio = 1d, rw = 400d/w, rh = 300d/h;
        if (rw < 1d || rh < 1d) {
          if (rw < rh) ratio = rh;
          else ratio = rw;
        }
        if (ratio < 1d) {
          // resizing the image
          w = (int)(w * ratio);
          h = (int)(h * ratio);
          Image img = bImg.getScaledInstance(w, h, Image.SCALE_SMOOTH);
          bImg = new BufferedImage(w, h, BufferedImage.TYPE_INT_RGB);
          bImg.getGraphics().drawImage(img, 0, 0 , null);
        }
        JButton but = new JButton(new ImageIcon(bImg));
        return but;
      } catch (Exception ex) { }
      JButton but = new JButton("NO IMAGE");
      return but;
    }
  }
}
