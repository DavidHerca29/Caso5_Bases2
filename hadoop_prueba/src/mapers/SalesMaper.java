package mapers;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Objects;

public class SalesMaper extends MapReduceBase implements Mapper<LongWritable,Text, Text,Text> {
    
    public void map(LongWritable key, Text value, OutputCollector<Text,Text> output, Reporter reporter) throws IOException{
        String line = value.toString();

        String values[] = line.split(","); // [2015,926,Dirección Administración y Otros Órganos de Apoyo,0,Remuneraciones,00101,Sueldos para cargos fijos ,15356701298,00]
        // Ocupo acceder al valor 2 del arreglo para determinar a cuál división del poder judicial pertenece.
        Text division = new Text(values[2]);
        // Para obtener el monto es mediante el indice 7 y para dar un mayor detalle pasamos el indice 6
        // hay lagunos valores que tienen "" en vez de 0 y para evitar errores asignamos un default de 0
        if (Objects.equals(values[7], "")){
            values[7]= "0";
        }
        String texto = values[6]+":"+values[7];

        Text amount = new Text(texto);

        output.collect(division, amount);
        /*
        String values[] = line.split(",");  
        String year = values[0].split("/")[2];
        IntWritable intYear = new IntWritable(Integer.valueOf(year));
        FloatWritable amount = new FloatWritable( Float.valueOf(values[1]));

        output.collect(intYear, amount);
         */

    }
}
