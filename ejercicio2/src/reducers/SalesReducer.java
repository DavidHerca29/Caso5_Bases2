package reducers;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Objects;


public class SalesReducer extends MapReduceBase implements Reducer<Text,Text,Text,Text> {
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text,Text> output, Reporter reporter) throws IOException {
		long amount1=0;
		long amount2=0;
		long amount3=0;
		long montoTotoal=0;
		String resultado1 = "";
		String resultado2 = "";
		String resultado3 = "";
		boolean bandera = false;

		ArrayList<Divisiones> divisions = new ArrayList<Divisiones>();

		while (values.hasNext()) {
			String valueString = values.next().toString();
			String[] arraystring = valueString.split(":");
			long montoDivision = Long.parseLong(arraystring[1]);

			bandera = false;
			for (Divisiones div: divisions) {
				if (Objects.equals(div.getDivision(), arraystring[0])){
					div.setMonto(div.getMonto()+montoDivision);
					bandera = true;
					break;
				}
			}
			if (!bandera){
				divisions.add(new Divisiones(arraystring[0], montoDivision));
			}
		}
		for (Divisiones div: divisions) {
			if (div.getMonto()>amount1){
				amount3 = amount2;
				resultado3 = resultado2;
				amount2 = amount1;
				resultado2 = resultado1;

				amount1 = div.getMonto();
				resultado1 = div.getDivision();
			}
			else if (div.getMonto()>amount2){
				amount3 = amount2;
				resultado3 = resultado2;

				amount2 = div.getMonto();
				resultado2 = div.getDivision();
			}
			else if (div.getMonto()>amount3){
				amount3 = div.getMonto();
				resultado3 = div.getDivision();
			}
		}


		output.collect(key,new Text("\n\t1. "+resultado1+" Con un presupuesto de: "+ amount1 +".\n\t2. "+resultado2+" Con un presupuesto de: "+
				amount2+".\n\t3. "+resultado3+" Con un presupuesto de: "+amount3+"\n"));
	}    
}
