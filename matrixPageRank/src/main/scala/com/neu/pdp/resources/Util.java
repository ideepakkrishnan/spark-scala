/**
 * 
 */
package com.neu.pdp.resources;

import java.io.StringReader;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.regex.Pattern;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.XMLReader;

/**
 * Utility class for the project which maintains all helper
 * functions
 * @author ideepakkrishnan
 */
public class Util {
	
	/**
	 * Extracts all out-links from the HTML source passed
	 * as argument to this method. This method considers
	 * only the content inside the div container tag named
	 * 'bodyContent'.
	 * @param strHtml : The HTML source of this page
	 * @return An array containing out-links from this page
	 */
	public static String[] fetchOutlinks(String line) {
		Pattern linkPattern;
		SAXParserFactory spf;
		SAXParser saxParser;
		XMLReader xmlReader;
		HashSet<String> linkPageNames = new HashSet<String>();
		String strPageName, strHtml;
		int iDelimLoc;
		
		// Each line is formatted as (Wiki-page-name:Wiki-page-html)
		iDelimLoc = line.indexOf(':');
		
		// Split and get the page name and corresponding html
		strPageName = line.substring(0, iDelimLoc);
		strHtml = line.substring(iDelimLoc + 1);
		
		// To keep only html filenames having relative paths and not
		// containing tilde (~)
		linkPattern = Pattern.compile("^\\..*/([^~]+)\\.html$");
		
		try {
			
			// Configure the XML parser
			spf = SAXParserFactory.newInstance();
			
			spf.setFeature(
					"http://apache.org/xml/features/nonvalidating/load-external-dtd", 
					false);
			
			spf.setFeature(
					"http://apache.org/xml/features/continue-after-fatal-error", 
					true);
			
			saxParser = spf.newSAXParser();
			xmlReader = saxParser.getXMLReader();
			
			// Initialize the List object to store out-links
			linkPageNames = new LinkedHashSet<String>();
			
			xmlReader.setContentHandler(
					new WikiParser(linkPageNames, linkPattern));
			
			// Fix system identifier in <!DOCTYPE .. > if the tag is
			// malformed.
			// Since this doesn't affect our computation, do a blind
			// rewrite of <!DOCTYPE .. >
			if (strHtml.substring(0, 9).equals("<!DOCTYPE")) {
				int index = strHtml.indexOf('>');
				strHtml = "<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Transitional//EN\" \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\">"
						+ strHtml.substring(index + 1);
			}
			
			// Encode '&' with '&amp;'
			strHtml = strHtml.replaceAll("&", "&amp;");
			
			xmlReader.parse(
					new InputSource(
							new StringReader(strHtml)));
		} catch (SAXNotRecognizedException e) {			
			e.printStackTrace();
		} catch (SAXNotSupportedException e) {
			e.printStackTrace();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (Exception e) {
			// Discard ill-formatted pages.
		}
		
		// Remove self links
		linkPageNames.remove(strPageName);
		
		return linkPageNames.toArray(new String[linkPageNames.size()]);
	}
	
}
