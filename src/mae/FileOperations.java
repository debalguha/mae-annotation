/*
 * MAE - Multi-purpose Annotation Environment
 *
 * Copyright Keigh Rim (krim@brandeis.edu)
 * Department of Computer Science, Brandeis University
 * Original program by Amber Stubbs (astubbs@cs.brandeis.edu)
 *
 * MAE is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, @see <a href="http://www.gnu.org/licenses">http://www.gnu.org/licenses</a>.
 *
 * For feedback, reporting bugs, use the project repo on github
 * @see <a href="https://github.com/keighrim/mae-annotation">https://github.com/keighrim/mae-annotation</a>
 */

package mae;

/**
 * FileOperations handles the input and output of files to and from MAE
 * @author Amber Stubbs, Keigh Rim
 * @version v0.11
 *
 */


import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import javax.swing.text.Document;
import javax.swing.text.Style;
import javax.swing.text.StyleContext;
import javax.swing.text.StyledDocument;
import javax.swing.text.rtf.RTFEditorKit;
import java.io.*;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Scanner;

class FileOperations {

    
  /**
   * 
   * @param f the plain text file being loaded
   * @param doc the styleDocument of the JTextPane that will hold the text
   * @return the StyleDocument with the text from the file
   * @throws Exception
   */
  public static StyledDocument setText(File f, StyledDocument doc)
      throws Exception{
     // Load the default style and add it as the "regular" text
    Style def = StyleContext.getDefaultStyleContext().getStyle( StyleContext.DEFAULT_STYLE );
    Style regular = doc.addStyle( "regular", def );
    Scanner scan = new Scanner(f,"UTF-8");
    //adding a newline to account for the newline that the XML formatting adds
    doc.insertString(doc.getLength(), "\n", regular);
    while (scan.hasNextLine()){
        String line = scan.nextLine();
        doc.insertString(doc.getLength(), line+"\n", regular);
    }
    scan.close();
    return doc;
  }
  
  public static boolean hasTags(File f) throws Exception{
    Scanner scan = new Scanner(f,"UTF-8");
    while (scan.hasNextLine()){
        String line = scan.nextLine();
        if(line.equals("<TAGS>")==true){
            scan.close();
            return true;
        }
     }
     scan.close();
   return false;
  }

 public static void saveRTF(File f, JTextPane pane){
     Document doc = pane.getDocument();
     RTFEditorKit kit = new RTFEditorKit();
      try {
        OutputStream os = new BufferedOutputStream(new FileOutputStream(f));
        kit.write(System.out, doc, 0, doc.getLength());
        kit.write(os, doc, 0, doc.getLength());
        os.close();
      } catch (Exception e) {
                System.out.println(e.toString());
            }
 }
 
  public static void saveXML(File f, JTextPane pane, 
    Hashtable<String,JTable> elementTables, ArrayList<Elem> elementNames, String dtdName){
    String paneText = pane.getText();            
       try{
          OutputStreamWriter fw = new OutputStreamWriter(new FileOutputStream(f),"UTF-8");
          String t = "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n";
          t = t + "<"+dtdName+">\n";
          t = t + "<TEXT><![CDATA[";
          fw.write(t,0,t.length());
          fw.write(paneText,0,paneText.length());
          t = "]]></TEXT>\n";
          fw.write(t,0,t.length());
          String s = "<TAGS>\n";
          fw.write(s,0,s.length());
          for(int i = 0;i<elementNames.size();i++){
              String name = elementNames.get(i).getName();
              JTable table = elementTables.get(name);
              DefaultTableModel tableModel = (DefaultTableModel)table.getModel();
              tableWrite(name,tableModel,fw);
          }
          s = "</TAGS>\n</"+dtdName+">";
          fw.write(s,0,s.length());
          fw.close();
       }catch(Exception ex){
           System.out.println(ex.toString());
       }
 }
 
 
 
 private static void tableWrite(String elem, DefaultTableModel tm, OutputStreamWriter fw)
            throws Exception{
     int rows = tm.getRowCount();
     int cols = tm.getColumnCount();
     for(int i=0;i<rows;i++){
         String tag = ("<"+elem + " ");
         for(int j=0;j<cols;j++){
             String colName = tm.getColumnName(j);
             String tagText = (String)tm.getValueAt(i,j);
             tagText=tagText.replace("\n"," ");
             tagText=tagText.replace("<","&lt;");
             tagText=tagText.replace(">","&gt;");
             tagText=tagText.replace("&","&amp;");
             tagText=tagText.replace("\"","'");
             tag = tag+colName+"=\""+tagText+"\" ";
         }
         tag = tag + "/>\n";
         fw.write(tag,0,tag.length());
     }
 }
 
    
    
}