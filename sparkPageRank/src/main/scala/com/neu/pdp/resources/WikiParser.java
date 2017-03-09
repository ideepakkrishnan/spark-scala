/**
 * 
 */
package com.neu.pdp.resources;

import java.net.URLDecoder;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * Contains methods to parse the HTML content of a Wiki page
 * and return the out-links.
 * @author ideepakkrishnan
 */
public class WikiParser extends DefaultHandler {
	
	// Class level variables
	private HashSet<String> linkPageNames; // Stores out-links in this page
	private int count = 0; // Nesting depth inside bodyContent
	private Pattern linkPattern; // Valid link pattern

	/**
	 * Overloaded Constructor
	 * @param linkPageNames Object to store the out-links in this page
	 * @param linkPattern Expected valid string pattern for out-links
	 */
	public WikiParser(
			HashSet<String> linkPageNames, 
			Pattern linkPattern) {
		super();
		this.linkPageNames = linkPageNames;
		this.linkPattern = linkPattern;
	}
	
	/**
	 * Parses the contents of a tag and extracts the required 
	 * information.
	 * @param uri Namespace URI
	 * @param localName Local name of the resource
	 * @param qName Qualified name of the tag
	 * @param attributes Attributes of this tag
	 */
	@Override
	public void startElement(
			String uri, 
			String localName, 
			String qName, 
			Attributes attributes) throws SAXException {
		
		super.startElement(uri, localName, qName, attributes);
		
		if ("div".equalsIgnoreCase(qName) && 
				"bodyContent".equalsIgnoreCase(
						attributes.getValue("id")) && 
				count == 0) {
			// Inside actual body of the html
			count = 1;
		} else if (count > 0 && "a".equalsIgnoreCase(qName)) {
			// Anchor tag inside bodyContent div element.
			count++;
			
			String link = attributes.getValue("href");
			if (link == null) {
				return;
			}
			
			try {
				// Decode escaped characters in URL.
				link = URLDecoder.decode(link, "UTF-8");
			} catch (Exception e) {
				// Wiki-weirdness; use link as is.
			}
			
			// Keep only html filenames ending as relative paths
			Matcher matcher = linkPattern.matcher(link);
			if (matcher.find()) {
				linkPageNames.add(matcher.group(1));
			}
		} else if (count > 0) {
			// Other element inside bodyContent div.
			count++;
		}
	}

	/**
	 * Updates the current nesting depth of the parse while
	 * coming out of a tag
	 * @param uri Namespace URI
	 * @param localName Local name of the resource
	 * @param qName Qualified Name of the resource
	 */
	@Override
	public void endElement(
			String uri, 
			String localName, 
			String qName) throws SAXException {
		super.endElement(uri, localName, qName);
		
		if (count > 0) {
			count--;
		}
	}

}
