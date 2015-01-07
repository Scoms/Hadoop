

import java.io.IOException;
import java.util.*;
import java.io.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Projet {

	static 	HashSet<String> keys;

	static String logs;
	static String start = "undefined";
	static String processedFileName = "";

	static Map<String,Integer> arcWeights;

	/* PASSE 1 */
	public static class FirstPassMap extends Mapper<Object, Text, Text, Text> 
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] splited = value.toString().split(" ");
			String myKey = splited[0];

			String sval = myKey.equals(start) ? "0" : "-1";

			int val = Integer.parseInt(sval);
			String voisins =splited[1];
			String sendVal = sval + "," + voisins;

			//logs += myKey + "\n";	
			if(val != -1)
			{
				//logs += "VAL NOT NULL \n";
				String currentSommet = myKey;
				String sommets = voisins.substring(1, voisins.length() -1);
				for(String sommet : sommets.split(","))
				{
					myKey = sommet;
					String matriceKey = currentSommet + myKey; 
					sendVal = arcWeights.get(matriceKey) + ",{}" ;

					logs += "	" + myKey + " " + sendVal + "\n"; 
					context.write(new Text(myKey),new Text(sendVal));
				}
			}
			else
			{
				logs += "	" + myKey + " " + sendVal + "\n"; 
				context.write(new Text(myKey),new Text(sendVal));
			}

		}
	} 

	public static class FirstPassReduce extends Reducer<Text, Text, Text, Text> 
	{
		private ArrayList<String> sommetsAtteignables = new ArrayList<String>();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			String sval = "";
			int bestVal = -1;
			int oldint = -1;
			String voisins = "";
			boolean keepOld = false;
			//Pour tout les voisins de l'item 

			logs += "traitement de : " + key + "\n";

			//logs += key + "\n";	
			for(Text item : values)
			{
				String[] toSplit = item.toString().split(",");
				int val = Integer.parseInt(toSplit[0]);
				oldint = val;
				//context.write(key, new Text("" + val));
				//logs += "	" + val + "\n";

				if(val != -1)
				{
					if(bestVal == -1 || val < bestVal)
					{
						//logs += "replace" + bestVal + " with " + val + "\n";
						bestVal = val;
						keepOld = true;

					}	
				}
				
				//logs += "DEBUG " + toSplit.length + "\n";
				//logs += "toSplit " + toSplit[1] + " length " + toSplit[1].length() + "\n";
				
				if(toSplit.length >= 2)
					voisins += toSplit[1].substring(1,toSplit[1].length() -1) + ",";
			}


			boolean hasVoisins = false;
			// prepare val 
			String proccessedVoisins = "{";
			for(String sommet : voisins.split(","))
			{
				if(sommet != null && !sommet.isEmpty())
				{
					proccessedVoisins += sommet + ",";
					sommetsAtteignables.add(sommet);
					hasVoisins = true;
				}
			}
/*
			if(voisins.contains(","))
			{
				context.write(key, new Text("{}"));
			}*/


			if(proccessedVoisins.length() != 1)
				proccessedVoisins = proccessedVoisins.substring(0, proccessedVoisins.length()-1);

			proccessedVoisins += "}";
			// System.out.println(key+" "+values);
			String sendVal = bestVal + "," + proccessedVoisins;

			logs += key + " " + sendVal + "\n";
			//logs += "	sommets sommetsAtteignables : " + sommetsAtteignables.toString() + "key :" + key +"\n";
			context.write(key, new Text(sendVal));


			//keep old 
			if(keepOld)
			{
				oldint = bestVal;
				String oldVal =  oldint + "," + proccessedVoisins;
				//context.write(key, new Text(oldVal));
			}
		}
	}

	/* PASSE 2 */

	public static class SecondPassMap extends Mapper<Object, Text, Text, Text> 
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String sval = value.toString();

			String myKey = sval.substring(0,1);
			String myValue = sval.substring(1,sval.length());
			myValue = removeEmpty(myValue);

			//logs += "Key : " + myKey + "\n";
			//get weight
			int weight = getWeight(myValue);


			logs += "Mapping : " + myKey + "(" + getVoisins(myValue) + ")" + "\n";

			//logs +=  myKey + "/" + myValue + "/weigth ="+ weight + "\n";
			//si poid non infini on map les voisins 
			if(weight != -1)
			{
				//logs += myKey + "\n";
				String[] voisins = getVoisins(myValue);
				if(voisins != null)
				{
					for(String voisin : voisins)
					{
						if(voisin.length() > 0)
						{
							//logs += "	Voisin : " + voisin+"\n";
							int voisingWeight = arcWeights.get(myKey+voisin);
							String nVal = (voisingWeight + weight) + "," + "{}"; 
							logs += "	" + voisin + " " + nVal + "\n";
							context.write(new Text(voisin), new Text(nVal));
						}
						else
						{
							//String[] test = sval.split(",").split(" ");
							context.write(new Text(myKey), new Text("" + weight));
						}
					}
				}
				context.write(new Text(myKey), new Text("" + weight));
			}
			else
			{
				logs += "	" + myKey + ". " + myValue + "\n";
				context.write(new Text(myKey), new Text(myValue));
			}

			//logs += "******endmap*******\n";
		}
	}

	public static String removeEmpty(String p)
	{
		String res = "";
		for(char c : p.toCharArray())
		{
			if(c != ' ' && c != 9)
			{
				//logs += "	add : " + c + "(" + (int)c + ")"+ "\n";
				res += c;
			}
		}
		return res;
	}


	//Utils functions 
	public static int getWeight(String p)
	{
		String cut = "";
		int res = -1;
		for (int i = 0; i < p.length(); i++) 
		{
			char c = p.charAt(i); 

			boolean isDigit = (c >= '0' && c <= '9');
			if(isDigit || c == '-')
			{
				cut += c;
				try
				{
					//logs+= "tp " + cut;
					Integer.parseInt(cut);
				}
				catch(Exception e)
				{
					return res;
				}
				res = Integer.parseInt(cut);
			}
		}

		return res;
	}

	public static String[] getVoisins(String p)
	{
		int start = p.indexOf('{');
		int end = p.indexOf('}');
		String[] res = p.substring(start + 1 ,end).split(",");
		return res;		
	}

	public static boolean isProcessusOver(String outputFile) throws IOException
	{
		keys = new HashSet<String>();
		HashSet<Integer> values = new HashSet<Integer>();

		boolean res = true;
		BufferedReader br = new BufferedReader(new FileReader(outputFile));
		String line;
		
		logs += "\n********************** CHECK *********************\n";
		while ((line = br.readLine()) != null) {
		   int val = getWeight(line);
		   if(val > -1)
		   {
		   		String key = line.split("	")[0];
		   		//logs += "WEIGHT : " + val + " KEY : " + key + "\n";
		   		keys.add(key);
		   		values.add(val);
		   }

		   String xx = line.split(",")[1].trim();
		   logs += xx + "\n";
		   if(!xx.equals("{}"))
		   {
		   	res = false;
		   	logs+="VOISINS";
		   }	
		}
		
		logs += "KEYS : " + keys.size() + "\n";
		logs += "\n********************** CHECK *********************\n";
		br.close();

		if(keys.size() == 1)
		{
			for(Integer i : values)
			{
				/*if(res == -1 || res > i)
					res = i;*/
			}
		}

		return res;
	}

	public static void preprocessFile(String p) throws IOException
	{

		processedFileName = p.split("/")[p.split("/").length - 1];

		arcWeights = new HashMap<String, Integer>();
		Map<String,String> sommetsVoisins = new HashMap<String,String>();
		BufferedReader br = new BufferedReader(new FileReader(p));
		String line;

		int i = 0;
		while ((line = br.readLine()) != null) {

			String sommetDepart = "";
			String sommetArrive = "";
			if(i == 0)
			{
				start = line;				
			}
			else
			{
				String[] splited = line.split(" ");

				int j = 0;
				for(String s : splited)
				{
					switch(j)
					{
						case 0 : // sommet
							sommetDepart = s;
							String sommet = sommetsVoisins.get(sommetDepart);
							if(sommet == null)
							{
								sommetsVoisins.put(s,"");
							}
							break;
						case 1 : // voisin
							sommetArrive = s;
							String res = sommetsVoisins.get(sommetDepart);
							//String val = sommetsVoisins;
							//System.out.println("res AV : " + res);
							sommetsVoisins.put(sommetDepart, res + " " + s);
							 res = sommetsVoisins.get(sommetDepart);

							//System.out.println("res AP : " + res);
							break;
						case 2 : // val
							arcWeights.put(sommetDepart + sommetArrive, Integer.parseInt(s));
							break;
					}

					j++;
				}


			}
		   	i++;

		}
		br.close();

		//Cré fichier départ 
		PrintWriter writer = new PrintWriter(processedFileName, "UTF-8");
		for(Map.Entry<String, String> entry : sommetsVoisins.entrySet()) {
				    String key = entry.getKey();
				    String value = entry.getValue();
				    String toWrite  = "";

				    for(String sommet : value.split(" "))
				    {
				    	if(!sommet.equals(""))
				    	{
					    	toWrite += sommet + ",";
				    	}
				    }

				    toWrite = key + " {" + toWrite.substring(0,toWrite.length() -1) + "}";

				    //System.out.println(key + " " + value);
				    // do what you have to do here
				    // In your case, an other loop.
				    writer.println(toWrite);
				}

		writer.close();

		//Check les poids
		for(Map.Entry<String, Integer> entry : arcWeights.entrySet()) {
		    String key = entry.getKey();
		    Integer value = entry.getValue();
		    // do what you have to do here
		    // In your case, an other loop.
		}
	}

	public static void main(String[] args) throws Exception 
	{
		// Preprocessing the file

		preprocessFile(args[0]);
		System.out.println("Sommet de départ : " + start + "\n");

		// appmlyting the algorith
		
		if (args.length < 2) 
		{
			System.err.println("Usage : hadoop jar Projet.jar Projet matrices.in res.out"); 
			System.exit(0);
		}

		String outputBase = args[1];
		String outputDir = outputBase;
		String inputFile = processedFileName;
		String outFile = "/part-r-00000";

		logs = "";
	    Configuration conf = new Configuration();
	    Job job = new Job(conf, "Projet");
	    job.setJarByClass(Projet.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);

	    job.setMapperClass(FirstPassMap.class);
	    job.setReducerClass(FirstPassReduce.class);

	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);

	    FileInputFormat.addInputPath(job, new Path(inputFile));
	    FileOutputFormat.setOutputPath(job, new Path(outputDir));
	    
	    logs+= "******************* PASSE 1 ****************\n";
	    job.waitForCompletion(true);


	    //boucle 
	    Job boucle;
	    String inFile = "output/part-r-00000";

	    boolean isOver = false;
	    int i = 2;
	    while(!isOver)
	    {
	    	String in = outputDir + outFile;
		    outputDir = outputBase + i;

	    	logs += "******************* PASSE " + i + " ****************\n";
	    	logs += "In = " + in + "\n";
	    	logs += "Out = " + outputDir+ "\n";
	    	System.out.println(logs);
	    	logs = "";
	    	boucle = new Job(conf, "Projet");
	    	boucle.setJarByClass(Projet.class);
		    boucle.setOutputKeyClass(Text.class);
		    boucle.setOutputValueClass(Text.class);
		    boucle.setMapperClass(SecondPassMap.class);
		    boucle.setReducerClass(FirstPassReduce.class);
		    boucle.setInputFormatClass(TextInputFormat.class);
		    boucle.setOutputFormatClass(TextOutputFormat.class);

		    FileInputFormat.addInputPath(boucle, new Path(in));
		    FileOutputFormat.setOutputPath(boucle, new Path(outputDir));
	   		 boucle.waitForCompletion(true);

	   		isOver = isProcessusOver(outputDir + "/part-r-00000");
	   		i++;
	    
	    }
 
 		Iterator iter = keys.iterator();

 		logs += "Les résultats ceux trouvent dans le fichier : " + outputDir + "/part-r-00000" + "\n";
	    /*logs = "********************** RESULT ************************\n"
	    + "			Sommet : " + iter.next() + "\n"
	    //+ "			Poids : " + res + "\n"
	    + "			Passes : " + (i - 1) + "\n"  
	    + "********************** RESULT ************************\n";*/
	    System.out.println(logs);

	}
}
