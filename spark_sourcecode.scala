// First declear alfa,eta and num_partiotions.

// package spark

import org.apache.spark.SparkContext

import scala.math._

import org.apache.spark.SparkContext._

import org.apache.spark.SparkConf

import java.io._

import scala.io._

import util.control.Breaks._

import org.apache.spark.HashPartitioner
 
  class RWRHNFF{
    
    val conf = new SparkConf().setAppName("spark").setMaster("local[2]")

    val sc = new SparkContext(conf)
    
    def calculate_rwrhn(path_heternet:String,path_ranknet: String ,path_result:String): Unit={
      
      //set number of partitions

      var num_partitions= NUMBER  of PARTITIONS
      
      //set alfa parameters

      var epcilon= EPCILONE
      
      //set alfa parameters

      var alfa= ALFA

      var eta= ETA
    
       //read first networks and parsed to interactions then groupbykey and partitioning base hashcode

      var interactions=sc.textFile(path_heternet).map { lines => 
           val parts_of_lines_of_interactions=lines.split(",")
           var id2 = parts_of_lines_of_interactions(0)
           var id1= parts_of_lines_of_interactions(1)
           var weight= parts_of_lines_of_interactions(2)
           (id2.toDouble,(id1.toDouble,weight.toDouble))}.groupByKey().partitionBy(new HashPartitioner(num_partitions))
         
       //read ranks of nodes file and parse it    
      var rank= sc.textFile(path_ranknet).map { lines =>
        val parts_of_line_ranks=lines.split(",")
        var node=parts_of_line_ranks(0)
        var rank=parts_of_line_ranks(1)
       (node.toDouble,rank.toDouble)}
          
       //join two up rdd for create ranks rdd and avoid shuffle in join inside while loop,(note:this join will does once)    
      val join_two_rdd = interactions.join(rank)
      var ranks=join_two_rdd.map{case(id,(iterable,rank))=>(id,rank)}
 
      //read ranks file again,(note: beacuse this file use for exit of loop)
      
      
      var ranks_again= sc.textFile(path_ranknet).map { lines =>
        val parts_of_line_ranks=lines.split(",")
        var node=parts_of_line_ranks(0)
        var rank=parts_of_line_ranks(1)
       (node.toDouble,rank.toDouble)}.groupByKey()
     
       
     //start loop for calculate ranks of nodes!
       
       while(true){
         var befor_setion_ranks=ranks
         //join 
         val contribs = interactions.join(ranks).values.flatMap{ case (iterable, rank) =>
     iterable.map{case(id,weight)=>(id,weight*rank)}}
         
         //update rank of each node
         ranks = contribs.reduceByKey(_ + _).mapValues(alfa * _)
         
         //sum ranks updated with initioal ranks
         var update_ranks=ranks_again.join(ranks).mapValues{
                        case(rank_ini,rank_updated)=>
                                  rank_ini.max*eta+rank_updated}
         //final ranks
        ranks=update_ranks.reduceByKey(_+_)

        //cheke foe exit() 

        var cheke=ranks.join(befor_setion_ranks).filter{case(id,(after_rank,befor_rank))=> (after_rank-befor_rank).abs > epcilon}
        
        var size=cheke.count
       
          //result

        if(size <1){
          
          val output = ranks.collect()

          var file=path_result
   
          var writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))
          for (resul <- output) {
          writer.write( resul + "   ")
          writer.write("\n")}
    
         writer.close 
         break
       }
             
       }
        
    
  }}
  

object RWRHN {
 
  
  def main(args: Array[String]): Unit = {
     // set paths of networks
    
     var path1="path_matrix"

     val path2="path_init_rankns"

     var result_out="path_out"
    
     var ob1=new RWRHNFF()

     ob1.calculate_rwrhn(path1,path2,result_out)
     
         
   }
  
}
