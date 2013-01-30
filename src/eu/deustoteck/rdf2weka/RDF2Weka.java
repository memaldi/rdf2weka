package eu.deustoteck.rdf2weka;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;

import com.clarkparsia.stardog.StardogException;
import com.clarkparsia.stardog.api.Connection;
import com.clarkparsia.stardog.api.ConnectionConfiguration;
import com.clarkparsia.stardog.api.Query;

public class RDF2Weka {

	public static void main (String args[]) {

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd - HH:mm:ss");
		Properties configFile = new Properties();
		InputStream in;
		try {
			in = new FileInputStream("config.properties");
			configFile.load(in);
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		String server = configFile.getProperty("STARDOG_SERVER");
		String dbStr = configFile.getProperty("STARDOG_DB");
		String user = configFile.getProperty("STARDOG_USER");
		String password = configFile.getProperty("STARDOG_PASS");
		String outputFile = configFile.getProperty("GRAPH_FILE");
		
		String [] dbs = dbStr.split(",");
		
		try {
			
			Map<String, Map<String, Integer>> datasetMap = new HashMap<String, Map<String,Integer>>();
			
			for (String db : dbs) {
			
				Map<String, Integer> propertyDict = new HashMap<String, Integer>();
				System.out.println(String.format("Analyzing %s..", db));
				Connection aConn = ConnectionConfiguration.to(db).url(server).credentials(user, password).connect();
				Query aQuery = aConn.query("SELECT ?property WHERE { [] ?property ?o}");
				TupleQueryResult aResult = aQuery.executeSelect();
				while(aResult.hasNext()) {
					BindingSet bindingSet = aResult.next();
					String property = bindingSet.getBinding("property").getValue().toString();
					String namespace = getNamespace(property);
					if (propertyDict.containsKey(namespace)) {
						propertyDict.put(namespace, propertyDict.get(namespace) + 1);
					} else {
						propertyDict.put(namespace, 1);
					}
				}
				aResult.close();
				aConn.close();
				datasetMap.put(db, propertyDict);
				//System.out.println(propertyDict);
			}
			
			List<String> ontologySet = new ArrayList<String>();
			Map<String, Integer> datasetCount = new HashMap<String, Integer>();
			
			for (String dataset : datasetMap.keySet()) {
				int count = 0;
				Map<String, Integer> propertyMap = datasetMap.get(dataset);
				for (String ontology : propertyMap.keySet()) {
					if (!ontologySet.contains(ontology)) {
						ontologySet.add(ontology);
					}
					count += propertyMap.get(ontology);
				}
				datasetCount.put(dataset, count);
				//System.out.println(count);
			}
			
			System.out.println(ontologySet);
			
			FileWriter out = new FileWriter(outputFile);
			out.write("@relation dataset\n\n");
			
			for (String ontology : ontologySet) {
				out.write(String.format("@attribute %s real\n", ontology));
			}
			out.write("\n@data\n");
			int i = 1;
			for (String dataset : datasetMap.keySet()) {
				System.out.println(String.format("Instance %s: %s", i, dataset));
				Map<String, Integer> propertyMap = datasetMap.get(dataset);
				String data = "";
				for (String ontology : ontologySet) {
					if (propertyMap.containsKey(ontology)) {
						data += propertyMap.get(ontology) / (float)datasetCount.get(dataset) + ",";
					} else {
						data += 0 + ",";
					}
				}
				out.write(data.substring(0, data.length() - 1) + "\n");
				i++;
			}
			out.close();
			
		} catch (StardogException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (QueryEvaluationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static String getNamespace(String URI) {
		if (URI.contains("#")) {
			return URI.split("#")[0] + "#";
		} else {
			String[] splittedURI = URI.split("/");
			String namespace = "";
			for (int i = 0; i < splittedURI.length - 1; i++) {
				namespace += splittedURI[i] + "/";
			}
			return namespace;
		}
	}
	
}
