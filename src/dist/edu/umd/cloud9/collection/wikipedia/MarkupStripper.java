/*
 *    MarkupStripper.java
 *    Copyright (C) 2007 David Milne, d.n.milne@gmail.com
 *
 *    This program is free software; you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation; either version 2 of the License, or
 *    (at your option) any later version.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    You should have received a copy of the GNU General Public License
 *    along with this program; if not, write to the Free Software
 *    Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
 */

package edu.umd.cloud9.collection.wikipedia;

import java.util.*;
import java.util.regex.*;

/**
 * This provides tools to strip out markup from wikipedia articles, or anything else that has been written
 * in mediawiki's format. It's all pretty simple, so don't expect perfect parsing. It is particularly bad at 
 * dealing with templates (these are simply removed rather than resolved).  
 * 
 * @author David Milne
 */
public class MarkupStripper {
	
	/**
	 * Strips a string of all markup; tries to turn it into plain text	 
	 * 
	 * @param markup the text to be stripped
	 * @return the stripped text
	 */
	public static String stripEverything(String markup)  {
		
		String strippedMarkup = stripTemplates(markup) ;
		strippedMarkup = MarkupStripper.stripSection(strippedMarkup, "see also") ;
		strippedMarkup = MarkupStripper.stripSection(strippedMarkup, "references") ;
		strippedMarkup = MarkupStripper.stripSection(strippedMarkup, "further reading") ;
		strippedMarkup = MarkupStripper.stripSection(strippedMarkup, "external links") ;
		strippedMarkup = stripTables(strippedMarkup) ;
		strippedMarkup = stripIsolatedLinks(strippedMarkup) ;
		strippedMarkup = stripLinks(strippedMarkup) ;
		strippedMarkup = stripHTML(strippedMarkup) ;
		strippedMarkup = stripExternalLinks(strippedMarkup) ;
		strippedMarkup = stripFormatting(strippedMarkup) ;
		strippedMarkup = stripExcessNewlines(strippedMarkup) ;
		
		return strippedMarkup ;
	}
	
	/**
	 * Strips all links from the given markup; anything like [[this]] is replaced. If it is a link to a wikipedia article, 
	 * then it is replaced with its anchor text. Only links to images are treated differently: they are discarded entirely. 
	 * 
	 * You may want to first strip non-article links, isolated links, category links etc before calling this method. 	 
	 * 
	 * @param markup the text to be stripped
	 * @return the stripped text
	 */
	public static String stripLinks(String markup) {
		
		HashSet<String> discardPrefixes = new HashSet<String>() ;
		discardPrefixes.add("image") ;
		
		Vector<Integer> linkStack = new Vector<Integer>() ; 
		
		Pattern p = Pattern.compile("(\\[\\[|\\]\\])") ;
		Matcher m = p.matcher(markup) ;
		
		StringBuffer sb = new StringBuffer() ;
		int lastIndex = 0 ;
		
		while (m.find()) {
			String tag = markup.substring(m.start(), m.end()) ;
			
			if (tag.equals("[["))
				linkStack.add(m.start()) ;
			else {
				if (!linkStack.isEmpty()) {
					int linkStart = linkStack.lastElement() ;
					linkStack.remove(linkStack.size()-1) ;
					
					if (linkStack.isEmpty()) {
						sb.append(markup.substring(lastIndex, linkStart)) ;
						
						//we have the whole link, with other links nested inside if it's an image
						String linkMarkup = markup.substring(linkStart+2, m.start()) ;
						sb.append(stripLink(linkMarkup, discardPrefixes, false)) ;
						
						lastIndex = m.end() ;
					}
				}
			}
		}
		
		if (!linkStack.isEmpty()) {
			System.err.println("MarkupStripper | Warning: links were not well formed, so we cannot guarantee that they were stripped out correctly. ") ;
		}
		
		sb.append(markup.substring(lastIndex)) ;
		return sb.toString() ;
	}
	
	/**
	 * Removes all references to images in the given markup
	 * 
	 * @param markup the markup to be stripped
	 * @return the stripped markup
	 */
	public static String stripImages(String markup) {
			
		Vector<Integer> linkStack = new Vector<Integer>() ; 
		
		Pattern p = Pattern.compile("(\\[\\[|\\]\\])") ;
		Matcher m = p.matcher(markup) ;
		
		StringBuffer sb = new StringBuffer() ;
		int lastIndex = 0 ;
		
		while (m.find()) {
			String tag = markup.substring(m.start(), m.end()) ;
			
			if (tag.equals("[["))
				linkStack.add(m.start()) ;
			else {
				if (!linkStack.isEmpty()) {
					int linkStart = linkStack.lastElement() ;
					linkStack.remove(linkStack.size()-1) ;
					
					if (linkStack.isEmpty()) {
						sb.append(markup.substring(lastIndex, linkStart)) ;
						
						//we have the whole link, with other links nested inside if it's an image
						String linkMarkup = markup.substring(linkStart+2, m.start()) ;
						if (!linkMarkup.toLowerCase().startsWith("image:")){
							sb.append("[[") ;
							sb.append(linkMarkup) ;
							sb.append("]]") ;							
						}
						lastIndex = m.end() ;
					}
				}
			}
		}
		
		if (!linkStack.isEmpty()) {
			System.err.println("MarkupStripper | Warning: links were not well formed, so we cannot guarantee that they were stripped out correctly. ") ;
		}
		
		sb.append(markup.substring(lastIndex)) ;
		return sb.toString() ;
	}
	
	/**
	 * Strips all non-article links from the given markup; anything like [[this]] is removed unless it
	 * goes to a wikipedia article, redirect, or disambiguation page. 
	 * 
	 * @param markup the text to be stripped
	 * @return the stripped text
	 */
	public static String stripNonArticleLinks(String markup) {
				
		Vector<Integer> linkStack = new Vector<Integer>() ; 
		
		Pattern p = Pattern.compile("(\\[\\[|\\]\\])") ;
		Matcher m = p.matcher(markup) ;
		
		StringBuffer sb = new StringBuffer() ;
		int lastIndex = 0 ;
		
		while (m.find()) {
			String tag = markup.substring(m.start(), m.end()) ;
			
			if (tag.equals("[["))
				linkStack.add(m.start()) ;
			else {
				if (!linkStack.isEmpty()) {
					int linkStart = linkStack.lastElement() ;
					linkStack.remove(linkStack.size()-1) ;
					
					if (linkStack.isEmpty()) {
						sb.append(markup.substring(lastIndex, linkStart)) ;
						
						//we have the whole link, with other links nested inside if it's an image
						String linkMarkup = markup.substring(linkStart+2, m.start()) ;
						if (linkMarkup.indexOf(":") < 0)
							sb.append("[[" + linkMarkup + "]]") ;
						else						
							sb.append(stripLink(linkMarkup, null, true)) ;
						
						lastIndex = m.end() ;
					}
				}
			}
		}
		
		if (! linkStack.isEmpty()) 
			System.err.println("MarkupStripper | Warning: links were not well formed, so we cannot guarantee that they were stripped out correctly. ") ;
		
		sb.append(markup.substring(lastIndex)) ;
		return sb.toString() ; 
	}
	
	/**
	 * Strips all non-article links from the given markup; anything like [[this]] is removed unless it
	 * goes to a wikipedia article, redirect, or disambiguation page. 
	 * 
	 * @param markup the text to be stripped
	 * @return the stripped text
	 */
	public static String stripIsolatedLinks(String markup) {
				
		Vector<Integer> linkStack = new Vector<Integer>() ; 
		
		Pattern p = Pattern.compile("(\\[\\[|\\]\\])") ;
		Matcher m = p.matcher(markup) ;
		
		StringBuffer sb = new StringBuffer() ;
		int lastIndex = 0 ;
		
		while (m.find()) {
			String tag = markup.substring(m.start(), m.end()) ;
			
			if (tag.equals("[["))
				linkStack.add(m.start()) ;
			else {
				if (!linkStack.isEmpty()) {
					int linkStart = linkStack.lastElement() ;
					linkStack.remove(linkStack.size()-1) ;
					
					if (linkStack.isEmpty()) {
						sb.append(markup.substring(lastIndex, linkStart)) ;
						
						//we have the whole link, with other links nested inside if it's an image
						String linkMarkup = markup.substring(linkStart+2, m.start()) ;
						
						//System.out.println(" - " + linkStart + ", " + m.end() + ", " + markup.length()) ;
						
						if (markup.substring(Math.max(0, linkStart-10), linkStart).matches("(?s).*(\\W*)\n") && (m.end() >= markup.length()-1 || markup.substring(m.end(), Math.min(markup.length()-1, m.end()+10)).matches("(?s)(\\W*)(\n.*|$)"))) {
							//discarding link
						} else {
							sb.append("[[") ;
							sb.append(linkMarkup) ;
							sb.append("]]") ;
						}
						
						lastIndex = m.end() ;
					}
				}
			}
		}
		
		if (!linkStack.isEmpty())
			System.err.println("MarkupStripper | Warning: links were not well formed, so we cannot guarantee that they were stripped out correctly. ") ;
		
		sb.append(markup.substring(lastIndex)) ;
		return sb.toString() ;
	}
	
	
	private static String stripLink(String linkMarkup, HashSet<String> discardedPrefixes, boolean discardAllPrefixes) {
		
		int colonPos = linkMarkup.indexOf(":") ;
		if (colonPos>0) {
			//prefix is specified
			
			String prefix = linkMarkup.substring(0, colonPos) ;
			if (discardAllPrefixes || (discardedPrefixes != null && discardedPrefixes.contains(prefix.toLowerCase()))) {
				//prefix indicates a link we want cleared
				return "" ;
			} else {
				linkMarkup = linkMarkup.substring(colonPos+1) ;
			}
		}
		
		int pos = linkMarkup.lastIndexOf("|") ;
			
		if (pos>0) {
			//link is piped 
			return linkMarkup.substring(pos+1) ;
		} else {
			//link is not piped ;
			return linkMarkup ;
		}
	}
	
	
	/**
	 * Removes all sections (both header and content) with the given sectionName
	 * 
	 * @param sectionName the name of the section (case insensitive) to remove.
	 * @param markup the markup to be stripped
	 * @return the stripped markup
	 */
	public static String stripSection(String markup, String sectionName) {
		
		Pattern p = Pattern.compile("(={2,})\\s*" + sectionName + "\\s*\\1.*?([^=]\\1[^=])", Pattern.CASE_INSENSITIVE + Pattern.DOTALL) ;
		Matcher m = p.matcher(markup) ;
		
		StringBuffer sb = new StringBuffer() ;
		int lastIndex = 0 ;
		
		while (m.find()) {				
			sb.append(markup.substring(lastIndex, m.start())) ;
			sb.append(m.group(2)) ;
			lastIndex = m.end() ;	
		}
		
		sb.append(markup.substring(lastIndex)) ;
		markup = sb.toString() ;
		
		//if this was the last section in the doc, then it won't be discarded because we can't tell where it ends.
		//best we can do is delete the title and the paragraph below it.
		
		p = Pattern.compile("(={2,})\\s*" + sectionName + "\\s*\\1\\W*.*?\n\n", Pattern.CASE_INSENSITIVE + Pattern.DOTALL) ;
		m = p.matcher(markup) ;
		
		sb = new StringBuffer() ;
		lastIndex = 0 ;
		
		while (m.find()) {		
			sb.append(markup.substring(lastIndex, m.start())) ;
			lastIndex = m.end()-2 ;	
		}
		
		sb.append(markup.substring(lastIndex)) ;	
		return sb.toString() ;
	}

	/**
	 * Strips all templates from the given markup; anything like {{this}}. 
	 * 
	 * @param markup the text to be stripped
	 * @return the stripped text
	 */
	public static String stripTemplates(String markup) {
		
		Vector<Integer> templateStack = new Vector<Integer>() ; 
		
		Pattern p = Pattern.compile("(\\{\\{|\\}\\})") ;
		Matcher m = p.matcher(markup) ;
		
		StringBuffer sb = new StringBuffer() ;
		int lastIndex = 0 ;
		
		while (m.find()) {
			
			String tag = markup.substring(m.start(), m.end()) ;
			
			if (tag.equals("{{"))
				templateStack.add(m.start()) ;
			else {
				if (!templateStack.isEmpty()) {
					int templateStart = templateStack.lastElement() ;
					templateStack.remove(templateStack.size()-1) ;
					
					if (templateStack.isEmpty()) {
						sb.append(markup.substring(lastIndex, templateStart)) ;
						
						//TODO: here is where we would resolve a template, instead of just removing it.
						//sb.append(stripTemplate(markup.substring(templateStart+2, m.start()))) ;
						
						//we have the whole template, with other templates nested inside					
						lastIndex = m.end() ;
					}
				}
			}
		}
		
		if (!templateStack.isEmpty())
			System.err.println("MarkupStripper | Warning: templates were not well formed, so we cannot guarantee that they were stripped out correctly. ") ;
		
		sb.append(markup.substring(lastIndex)) ;
		return sb.toString() ;
	}
	
	/*
	private static String stripTemplate(String markup) {
		//TODO: ideally we would have all the templates summarized, so here we could looking up the template and resolve it to html. For now we just get rid of all templates.
		
		return "" ;
	}*/
	
	/**
	 * Strips all tables from the given markup; anything like {|this|}. 
	 * 
	 * @param markup the text to be stripped
	 * @return the stripped text
	 */
	public static String stripTables(String markup) {
		
		Vector<Integer> tableStack = new Vector<Integer>() ; 
		
		Pattern p = Pattern.compile("(\\{\\||\\|\\})") ;
		Matcher m = p.matcher(markup) ;
		
		StringBuffer sb = new StringBuffer() ;
		int lastIndex = 0 ;
		
		while (m.find()) {
			
			String tag = markup.substring(m.start(), m.end()) ;
			
			if (tag.equals("{|"))
				tableStack.add(m.start()) ;
			else {
				if (!tableStack.isEmpty()) {
					int templateStart = tableStack.lastElement() ;
					tableStack.remove(tableStack.size()-1) ;
					
					if (tableStack.isEmpty()) {
						sb.append(markup.substring(lastIndex, templateStart)) ;
						
						//we have the whole table, with other tables nested inside					
						lastIndex = m.end() ;
					}
				}
			}
		}
		
		if (!tableStack.isEmpty())
			System.err.println("MarkupStripper | Warning: tables were not well formed, so we cannot guarantee that they were stripped out correctly. ") ;
		
		sb.append(markup.substring(lastIndex)) ;		
		return sb.toString() ;
	}
	
	
	/**
	 * Strips all <ref> tags from the given markup; both those that provide links to footnotes, and the footnotes themselves.
	 * 
	 * @param markup the text to be stripped
	 * @return the stripped text
	 */
	public static String stripRefs(String markup) {
		
		String strippedMarkup = markup.replaceAll("<ref\\\\>", "") ;					//remove simple ref tags
		strippedMarkup = strippedMarkup.replaceAll("(?s)<ref>(.*?)</ref>", "") ;			//remove ref tags and all content between them. 
		strippedMarkup = strippedMarkup.replaceAll("(?s)<ref\\s(.*?)>(.*?)</ref>", "") ; 	//remove ref tags and all content between them (with attributes).
	    
		return strippedMarkup ;
	}
	
	/**
	 * Strips all html tags and comments from the given markup. Text found between tags is not removed.
	 * 
	 * @param markup the text to be stripped
	 * @return the stripped text
	 */
	public static String stripHTML(String markup) {
		
		String strippedMarkup = markup.replaceAll("(?s)\\<\\!\\-\\-(.*?)\\-\\-\\>","") ;	//strip comments
		
		strippedMarkup = stripRefs(strippedMarkup) ;
		strippedMarkup = strippedMarkup.replaceAll("<(.*?)>", "") ;	// remove remaining tags ;	
		
		return strippedMarkup ;
	}
	
	
	/**
	 * Strips all links to external web pages; anything like [this] that starts with "http" or "www". 
	 * 
	 * @param markup the text to be stripped
	 * @return the stripped text
	 */
	public static String stripExternalLinks(String markup) {
		
		String strippedMarkup = markup.replaceAll("\\[(http|www)(.*?)\\]", "") ;
		return strippedMarkup ;
	}
	
	/**
	 * Strips all wiki formatting, the stuff that makes text bold, italicised, intented, listed, or made into headers. 
	 * 
	 * @param markup the text to be stripped
	 * @return the stripped markup
	 */
	public static String stripFormatting(String markup) {
		
		String strippedMarkup = markup.replaceAll("'{2,}", "") ;       //remove all bold and italic markup ;
		strippedMarkup = strippedMarkup.replaceAll("={2,}","") ;	   //remove all header markup
		strippedMarkup = strippedMarkup.replaceAll("\n:+", "\n") ;	   //remove indents.
		strippedMarkup = strippedMarkup.replaceAll("\n(\\*+)\\W*", "\n") ; //remove list markers.
		
		
		
		return strippedMarkup ;
	}
	
	
	
	/**
	 * Removes anything at the start of the markup that is indented. Normally this indicates notes that the author
	 * should have used a template for, such as a "For other uses, see ****" note.
	 * 
	 * @param markup the text to be stripped
	 * @return the stripped markup
	 */
	public static String stripIndentedStart(String markup) {
		
		Pattern p = Pattern.compile("(.*?)\n", Pattern.DOTALL) ;
		Matcher m = p.matcher(markup) ;
		
		StringBuffer sb = new StringBuffer() ;
		int newStart = 0 ;
		
		while (m.find()) {
			//System.out.println(" - \"" + m.group() + "\"\n\n") ;
			
			if (m.group().matches("(?s)([\\s\\W]*)([\\:\\*]+)(.*)")||m.group().matches("\\W*"))
				newStart = m.end() ;
			else
				break ;
		}
		
		sb.append(markup.substring(newStart)) ;		
		return sb.toString() ;
	}
	
	
	/**
	 * Collapses consecutive newlines into at most two newlines. 
	 * This is provided because stripping out templates and tables often leaves large gaps in the text.  
	 * 
	 * @param markup the text to be stripped
	 * @return the stripped markup
	 */
	public static String stripExcessNewlines(String markup) {
		
		String strippedMarkup = markup.replaceAll("\n{3,}", "\n\n") ;		
		return strippedMarkup ;
	}	
	
	/**
	 * Removes all ordered and unordered list items.
	 * 
	 * @param markup the text to be stripped
	 * @return the stripped markup
	 */
	public static String stripListItems(String markup) {
		
		String strippedMarkup = markup.replaceAll("\n\\s*[\\#\\*]+\\s*(.*?)\n", "\n") ;		
		return strippedMarkup ;
	}	
	
	
	/**
	 * Removes all brackets that have nothing in them but space. This is a hack, a symptom of not dealing with templates very well.
	 * 
	 * @param markup the text to be stripped
	 * @return the stripped markup
	 */
	public static String stripOrphanedBrackets(String markup) {
		
		String strippedMarkup = markup.replaceAll("\\([\\W]*?\\)", "") ;		
		return strippedMarkup ;
	}
	
	/**
	 * Removes special "magic word" (???) syntax, such as __NOTOC__
	 * 
	 * @param markup the text to be stripped
	 * @return the stripped markup
	 */
	public static String stripMagicWords(String markup) {
		
		String strippedMarkup = markup.replaceAll("\\_\\_(\\p{Upper}+\\_\\_)", "") ;		
		return strippedMarkup ;
	}
	
	/**
	 * Removes all section headers. 
	 * 
	 * @param markup the text to be stripped
	 * @return the stripped markup
	 */
	public static String stripHeadings(String markup) {
		Pattern p = Pattern.compile("(={2,})([^=]+)(\\1)") ;
		Matcher m = p.matcher(markup) ;
		
		StringBuffer sb = new StringBuffer() ;
		int lastIndex = 0 ;
		
		while (m.find()) {
			sb.append(markup.substring(lastIndex, m.start())) ;
			lastIndex = m.end() ;		
		}
		
		sb.append(markup.substring(lastIndex)) ;
		return sb.toString() ;		
	}
}
