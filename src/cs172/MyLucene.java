package cs172;
import java.io.*;
import java.util.StringTokenizer;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;


class WebDocument {
	public String user;
	public String tweet;
	public String time;
	public String url;
	public String title;
	public String geo;
	
	public WebDocument(String usr, String twt, String time, String u, String title, String g) {
		this.user=usr;
		this.tweet=twt;
		this.time=time;
		this.url = u;
		this.title = title;
		this.geo = g;
	}
}

public class MyLucene {
	public static final String INDEX_DIR = "Index";
	public static void main(String[] args) throws CorruptIndexException, IOException {
		
		if (args.length == 0) {
		
		//READ FROM FILES
		BufferedReader reader = null;
        int count = 0;
		
        try {
        	File file = new File("file1.txt");
        	
        	while (file.exists()) {
        	
	        	System.out.println("Reading from file '" + file + "'...");
	            reader = new BufferedReader(new FileReader(file));
	            
	            // Read every line in the file, and parse each tweet.
	            for (String line; (line = reader.readLine()) != null; ) {
	            	count++; //Count number of tweets
	            	System.out.println("Tweets = " + count);
						
						String User = line.substring(line.indexOf("\"User\"")+8, line.indexOf("\"Time\"")-2);
						String Time = line.substring(line.indexOf("\"Time\"")+7, line.indexOf("\"Tweet\"")-1);
						String Tweet = line.substring(line.indexOf("\"Tweet\"")+9, line.indexOf("\"LinkUrl\"")-2);
						String LinkUrl = line.substring(line.indexOf("\"LinkUrl\"")+10, line.indexOf("\"LinkTitle\"")-1);
						String LinkTitle = line.substring(line.indexOf("\"LinkTitle\"")+12, line.indexOf("\"Geo\"")-1);
						String Geo = line.substring(line.indexOf("\"Geo\"")+6, line.length()-1);
						
						//Declare tweet, and index it in Lucene
						WebDocument page = new WebDocument(User,Tweet,Time,LinkUrl,LinkTitle,Geo);
						index(page);
	            }
	            
	            reader.close();
	            System.out.println("Current number of tweets = " + count);
	            file = new File("twitter_data.txt");
            
        	}
        } 
        catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            try {
                reader.close();
                System.out.println("Total number of tweets = " + count);
            }
            catch (IOException e) {
                e.printStackTrace();
            }

        } 
        
		}
		
		
		
		
	}
		
    
    public static void index (WebDocument tweet) {
    	File index = new File(INDEX_DIR);
    	IndexWriter writer = null;
    	
    	try {	
			IndexWriterConfig indexConfig = new IndexWriterConfig(Version.LUCENE_34, new StandardAnalyzer(Version.LUCENE_34));
			writer = new IndexWriter(FSDirectory.open(index), indexConfig);
			Document luceneDoc = new Document();
			luceneDoc.add(new Field("user", tweet.user, Field.Store.YES, Field.Index.NOT_ANALYZED,Field.TermVector.YES));
			luceneDoc.add(new Field("tweet", tweet.tweet, Field.Store.YES, Field.Index.ANALYZED,Field.TermVector.YES));
			luceneDoc.add(new Field("time", tweet.time, Field.Store.YES, Field.Index.ANALYZED,Field.TermVector.YES));
			luceneDoc.add(new Field("url", tweet.url, Field.Store.YES, Field.Index.NOT_ANALYZED,Field.TermVector.YES));
			luceneDoc.add(new Field("title", tweet.title, Field.Store.YES, Field.Index.ANALYZED,Field.TermVector.YES));
			luceneDoc.add(new Field("geo", tweet.geo, Field.Store.YES, Field.Index.ANALYZED,Field.TermVector.YES));
			//write into lucene
			luceneDoc.setBoost((float)2.0);
			writer.addDocument(luceneDoc);			
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			if (writer !=null)
				try {
					writer.close();
				} catch (CorruptIndexException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
		}
    
    }
    
    public static String[] search (String queryString, int topk) throws CorruptIndexException, IOException {
		
		IndexReader indexReader = IndexReader.open(FSDirectory.open(new File(INDEX_DIR)));
		IndexSearcher indexSearcher = new IndexSearcher(indexReader);
		QueryParser queryparser = new QueryParser(Version.LUCENE_34, "text", new StandardAnalyzer(Version.LUCENE_34));

		try {
			StringTokenizer strtok = new StringTokenizer(queryString, " ~`!@#$%^&*()_-+={[}]|:;'<>,./?\"\'\\/\n\t\b\f\r");
			String querytoparse = "";
			while(strtok.hasMoreElements()) {
				String token = strtok.nextToken();
				querytoparse += "text:" + token + "^1" + "hashtags:" + token+ "^1.5" + "ptitle:" + token+"^2.0";
				//querytoparse += "text:" + token;
			}		
			Query query = queryparser.parse(querytoparse);
			//System.out.println(query.toString());
			TopDocs results = indexSearcher.search(query, topk);
			int num_results = results.scoreDocs.length;
			System.out.println(num_results);
			String[] returnTweets = new String[num_results];
			for (int i = 0; i < num_results; i++) {
				String temp = "@" + indexSearcher.doc(results.scoreDocs[i].doc).getFieldable("user").stringValue();
				String date = indexSearcher.doc(results.scoreDocs[i].doc).getFieldable("date").stringValue();
				date = date.replace("+0000", "");
				temp += ": " + indexSearcher.doc(results.scoreDocs[i].doc).getFieldable("text").stringValue();
				temp += "<br/>" + date + "    Score: " +  results.scoreDocs[i].score;;
				System.out.println(indexSearcher.doc(results.scoreDocs[i].doc).getFieldable("text").stringValue());
				System.out.println("score: " + results.scoreDocs[i].score);
				returnTweets[i] = temp;
			}
			
			
			return returnTweets;			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			indexSearcher.close();
		}
		return null;
	}
    
    
}