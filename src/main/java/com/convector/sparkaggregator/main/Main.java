package com.convector.sparkaggregator.main;

import com.cartodb.CartoDBClientIF;
import com.cartodb.impl.ApiKeyCartoDBClient;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * Created by pyro_ on 27/06/2016.
 */
public class Main {

    static final String HADOOP_COMMON_PATH = "C:\\Users\\pyro_\\OneDrive\\Documentos\\Big Data\\Practica 5 Spark Streaming\\lab5\\hadoop-common-2.2.0-bin-master\\";
    static final String VALIDATIONS_PATH = "C:\\Users\\pyro_\\Downloads\\validacion_metro_20160401 - copia.csv";
    static final long ONEMINUTE = 60000;

    public static void main(String[] args) throws Exception {

        System.setProperty("hadoop.home.dir", HADOOP_COMMON_PATH);
        SparkConf conf = new SparkConf().setAppName("UPCSchool-Spark").setMaster("local[*]");
        JavaSparkContext ctx = new JavaSparkContext(conf);
        ctx.setLogLevel("WARN");

        final SimpleDateFormat parser = new SimpleDateFormat("DD/MM/YY HH:mm:ss");
        final Date lowerThreshold = parser.parse("01/04/16 04:00:00");


        //84 per 17.30

        for(int i=0;i<55;i++) {
            //final Date upperThreshold = parser.parse("01/04/16 04:30:00");
            final Date upperThreshold = new Date(lowerThreshold.getTime()+i*10*ONEMINUTE);
            JavaRDD<String> validations = ctx.textFile(VALIDATIONS_PATH);

            JavaPairRDD<String, Date> pairs = validations.mapToPair(new PairFunction<String, String, Date>() {
                public Tuple2<String, Date> call(String line) {
                    String[] parts = line.split("\\|");
                    Date d = null;
                    try {
                        d = parser.parse(parts[2]);
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                    return new Tuple2<String, Date>(parts[0], d);
                }
            });

            JavaPairRDD<String, Date> filteredPairs = pairs.filter(new Function<Tuple2<String, Date>, Boolean>() {
                public Boolean call(Tuple2<String, Date> t) {
                    return t._2.before(upperThreshold) && t._2.after(lowerThreshold);
                }
            });
/*
        filteredPairs.foreach(new VoidFunction<Tuple2<String, Date>>() {
            public void call(Tuple2<String, Date> t) throws Exception {
                System.out.println(t._1() + "," + t._2.toString());
            }
        });
*/
            JavaPairRDD<String, Integer> ones = filteredPairs.mapToPair(new PairFunction<Tuple2<String, Date>, String, Integer>() {
                public Tuple2<String, Integer> call(Tuple2<String, Date> t) throws Exception {
                    return new Tuple2<String, Integer>(t._1, 1);
                }
            });

            JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
                public Integer call(Integer i1, Integer i2) {
                    return i1 + i2;
                }
            });

            PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("myfile.csv", true)));
            DateFormat df = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
            CartoDBClientIF cartoClient = new ApiKeyCartoDBClient("cteixidogalvez", "7977f5eaebeb6279ca4b9224a1f2988f14ee1824");

            List<Tuple2<String, Integer>> list = counts.collect();
            for (Tuple2<String, Integer> t : list) {
                if(Coordinater.getCoordinates(t._1())!= null){
                    String str = t._1() + "," + t._2 + "," + df.format(upperThreshold) + "," + Coordinater.getCoordinates(t._1());
                    String subQuery = "ST_SetSRID(ST_Point(" + Coordinater.getCoordinates(t._1()) + "),4326),"
                            + "'" + t._1() + "',"
                            + "TO_TIMESTAMP('" + df.format(upperThreshold) + "','DD/MM/YYYY HH24:MI:SS'),"
                            + "ST_SetSRID(ST_Point(" + Coordinater.getCoordinates(t._1()) + "),3857),"
                            + t._2() + ");";
                    System.out.println(str + "          INSERT INTO validacions_online_metro_animation (the_geom, codi_estacio, instant_pas, the_geom_webmercator, validacions) VALUES(" + subQuery);
                    cartoClient.executeQuery("INSERT INTO validacions_online_metro_animation (the_geom, codi_estacio, instant_pas, the_geom_webmercator, validacions) VALUES(" + subQuery);
                    out.println(str);
                }
            }
            out.close();

        /*SQLContext sqlContext = new SQLContext(ctx);

        //DataFrame df = sqlContext.read().format("com.databricks.spark.csv").option("header","true").option("inferSchema","true").load(VALIDATIONS_PATH);
        DataFrame df = sqlContext.read().format("csv").load(VALIDATIONS_PATH);
        System.out.println(df.first().toString());*/
        }
    }
}
