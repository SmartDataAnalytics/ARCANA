package tech.sda.arcana.spark.profiling;

import java.io.IOException;
import java.net.URL;

import edu.mit.jwi.Dictionary;
import edu.mit.jwi.IDictionary;
import edu.mit.jwi.item.IIndexWord;
import edu.mit.jwi.item.IWord;
import edu.mit.jwi.item.IWordID;
import edu.mit.jwi.item.POS;
/*
 * A class that implements an API for the Wordnet files
 */
public class JWI {
	public void testDictionary() throws IOException {
		
		/* @Ali
		 * The base Wordnet directory is assumed to be stored in a
			system environment variable called WNHOME. Note that the WNHOME variable points to the root of the Wordnet
			installation directory and the dictionary data directory \dict" must be appended to this path. This may be
			different on your system depending on where your Wordnet files are located. The second block of code, two
			lines long (9-10), constructs an instance of the default Dictionary object, and opens it by calling the open()
			method. The final block of six lines (13-18) demonstrates searching the dictionary for the first sense of the
			noun \dog". Listing 2 shows the console output of the method.
		 * 
		 * When you get compatibility error with the Scala version, replace _ with - in the jar name.
		 * https://stackoverflow.com/questions/39943739/scala-library-incompatibility-with-jwi
		 */
		
		
		//construct the URL to the Wordnet dictionary directory
		//String wnhome = System . getenv (" WNHOME ");
		//String path = wnhome + File . separator + " dict ";
		String path = "C:/Users/ali-d/Documents/WordNet/2.1/dict";
		URL url = new URL("file", null , path );

		// construct the dictionary object and open it
		IDictionary dict = new Dictionary(url);
		 dict . open ();

		 // look up first sense of the word "dog "
		 IIndexWord idxWord = dict . getIndexWord ("cat", POS. NOUN );
		 IWordID wordID = idxWord . getWordIDs ().get (0) ;
		 IWord word = dict . getWord ( wordID );
		 System .out . println ("Id = " + wordID );
		 System .out . println ("Lemma = " + word . getLemma ());
		 System .out . println ("Gloss = " + word . getSynset (). getGloss ());
	}
	public void printz(){
		System.out.println("Hi Fun!");
	}
	public static void main(String[] args) {
		JWI obj = new JWI();
		try {
			obj.testDictionary();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}