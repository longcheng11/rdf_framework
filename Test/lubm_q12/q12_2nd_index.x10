import x10.io.File;
import x10.util.ArrayList;
import x10.util.List;
import x10.array.Array;
import x10.util.HashMap;
import x10.io.ReaderIterator;
import x10.util.Map.Entry;
import x10.util.Random;
import x10.util.concurrent.AtomicBoolean;
import x10.util.concurrent.AtomicLong;
import x10.util.concurrent.AtomicInteger;
import x10.compiler.Native;
import x10.compiler.NativeCPPInclude;
import x10.compiler.NativeCPPCompilationUnit;
import x10.util.StringBuilder;
import x10.util.concurrent.AtomicInteger;
import x10.util.HashSet;

@NativeCPPInclude("gzRead.h")
@NativeCPPCompilationUnit("gzRead.cc")

public class q12_2nd_index {
	
	@Native("c++","gzRead(#1->c_str())")
	static native def gzRead(file:String):String;
	
	public static def Parsing(s:String):Array[Long]{ 
		var triple:Array[Long]=new Array[Long](3);
		
		var tmp:String;
		
		//parsing the subject
		tmp =s.substring(0,s.indexOf('\t'));
		triple(0)=Long.parse(tmp);
		
		//parsing the predicate
		var s1:String=s.substring(tmp.length()+1);
		tmp=s1.substring(0,s1.indexOf('\t'));
		triple(1)=Long.parse(tmp);
		
		//parsing the object
		var s2:String=s1.substring(tmp.length()+1);
		triple(2)=Long.parse(s2);
		
		return triple;
	} 
	
	public static def hash_3(key:Long,size:Int):Int {
		var s:Long=size as Long;
		var mod:long=key%s;	
		return mod as Int;
	} 
	
	public static def main(args: Array[String]) {
		// TODO auto-generated stub
		
		val N:Int=Place.MAX_PLACES;
		Console.OUT.println("the number of places is "+N);
		val r:Region=0..(N-1);
		val d:Dist=Dist.makeBlock(r);
		
		//the index root of P and PO and PS
		val pRoot=DistArray.make[pNode](d);	
		val poRoot=DistArray.make[poNode](d);	

		//remote receive in the hash joins
		val R1_keys_receive=DistArray.make[Array[RemoteArray[Long]]](d);	
		val R2_keys_receive=DistArray.make[Array[RemoteArray[Long]]](d);	
		val R3_keys_receive=DistArray.make[Array[RemoteArray[Long]]](d);	
		val R3_payload_receive=DistArray.make[Array[RemoteArray[Long]]](d);
		val R4_keys_receive=DistArray.make[Array[RemoteArray[Long]]](d);
		
		// <?a ?b>
		val T1_keys_receive=DistArray.make[Array[RemoteArray[Long]]](d);	
		val T1_payloads_receive=DistArray.make[Array[RemoteArray[Long]]](d);	
		
		//record the size of receive R and S
		val counters=DistArray.make[Array[AtomicInteger]](d);
		
		//initialize the object at each place		
		finish for (p in Place.places()){
			at (p) async {
				
				//indexRoot and filter tree
				pRoot(here.id)=new pNode(0);
				poRoot(here.id)=new poNode(0.toString());
				
				//receive -  the remote arrays
				R1_keys_receive(here.id)=new Array[RemoteArray[Long]](N);
				R2_keys_receive(here.id)=new Array[RemoteArray[Long]](N);				
				R3_keys_receive(here.id)=new Array[RemoteArray[Long]](N);
				R3_payload_receive(here.id)=new Array[RemoteArray[Long]](N);
				R4_keys_receive(here.id)=new Array[RemoteArray[Long]](N);
				
				T1_keys_receive(here.id)=new Array[RemoteArray[Long]](N);
				T1_payloads_receive(here.id)=new Array[RemoteArray[Long]](N);
				
				counters(here.id)=new Array[AtomicInteger](3);	
				for(i in 0..2){
					counters(here.id)(i)=new AtomicInteger(0);		
				}				
			}
		}		
		
		Console.OUT.println("///////////////// start to build index////////////////");
		var read_start:Long=System.currentTimeMillis();
		
		finish for( p in Place.places()){
			at (p) async {
				var pn:Int=here.id;
				
				var ns:String;
				val s1="/data/RDF_Processing/data_1b/"+pn.toString()+".long.gz";
				var lstring:String=gzRead(s1);
				var len:Int=lstring.length();
				var start:Int=0;
				var end:Int=0;
				var line:String;
				var triple:Array[Long]=new Array[Long](3);
				var tmpNode:pNode;
				var tmpoNode:poNode;
				var postring:String;
				while(start<len) {
					end=lstring.indexOf('\n',start);
					line=lstring.substring(start,end);
					start=end+1;
					triple=Parsing(line);   //get the S,P,O here
					
					//for the index of P
					if(pRoot(here.id).hasChild(triple(1))){
						tmpNode=pRoot(here.id).getChild(triple(1));
						tmpNode.insertData(triple(0),triple(2));
					}
					else{
						tmpNode=new pNode(triple(1));
						tmpNode.insertData(triple(0),triple(2));
						pRoot(here.id).addChild(tmpNode);
					}
					
					//for the index of PO
					postring= triple(1).toString()+triple(2).toString();
					if(poRoot(here.id).hasChild(postring)){
						tmpoNode=poRoot(here.id).getChild(postring);
						tmpoNode.insertData(triple(0));
					}
					else{
						tmpoNode=new poNode(postring);
						tmpoNode.insertData(triple(0));
						poRoot(here.id).addChild(tmpoNode);
					}
				}
			}
		}
		
		var read_end:Long=System.currentTimeMillis();
		Console.OUT.println("indexed time is "+(read_end-read_start)+" ms");
		System.gc();
		
		Console.OUT.println("///////////////// start to query////////////////");
		
		var dis_s1:Long=System.currentTimeMillis();	
		//process R1 R2 R3 R4
		finish for( p in Place.places()){
			at (p) async {
				val pn:Int=here.id;				
				
				//process R3 <?a 13426 ?b> 
				var R_Node:pNode;
				var R_tuple:ArrayList[Array[Long]];
				var pindex:Long=13426L;
				R_Node=pRoot(here.id).getChild(pindex);
				R_tuple=R_Node.getData();
				
				var R_key_collector:Array[ArrayList[Long]]=new Array[ArrayList[Long]](N);
				var R_payload_collector:Array[ArrayList[Long]]=new Array[ArrayList[Long]](N);
				for(j in (0..(N-1))){
					R_key_collector(j)=new ArrayList[Long]();
					R_payload_collector(j)=new ArrayList[Long]();
				}
				
				//hash partitioning
				var des:Int;
				for(tuple in R_tuple){
					des=hash_3(tuple(1),N);
					R_key_collector(des).add(tuple(1));
					R_payload_collector(des).add(tuple(0));
				}
				
				//push the R to remote places
				var keys_array:Array[long];
				var payload_array:Array[long];
				for( k in (0..(N-1))) {
					val kk=(k+pn)%N;
					keys_array=R_key_collector(kk).toArray();
					payload_array=R_payload_collector(kk).toArray();
					val pk=Place.place(kk);
					val s1=keys_array.size;	
					at(pk){
						R3_keys_receive(here.id)(pn)= new RemoteArray(new Array[Long](s1));
						R3_payload_receive(here.id)(pn)= new RemoteArray(new Array[Long](s1));
					}
					Array.asyncCopy( keys_array, at (pk) R3_keys_receive(here.id)(pn));
					Array.asyncCopy( payload_array, at (pk) R3_payload_receive(here.id)(pn));
				} //end pushing	
				
				if(pn==0){Console.OUT.println("R3 is done");}
				
				//clear the p Index
				pRoot(here.id)=null;
				
				//processing R1 <?a 20758 11757> 
				var keyString:String=20758.toString()+11757.toString();
				var R0_Node:poNode;
				var R0_tuple:ArrayList[Long];
				if(poRoot(here.id).hasChild(keyString)){
					R0_Node=poRoot(here.id).getChild(keyString);
					R0_tuple=R0_Node.getData();
				}
				else{
					R0_tuple=new ArrayList[Long]();
				}
				for(j in (0..(N-1))){
					R_key_collector(j)=new ArrayList[Long]();
				}
				
				//hash partitioning
				for(tuple in R0_tuple){				
					des=hash_3(tuple,N);
					R_key_collector(des).add(tuple);
				}
				
				for( k in (0..(N-1))) {
					val kk=(k+pn)%N;
					keys_array=R_key_collector(kk).toArray();				
					val pk=Place.place(kk);
					val s1=keys_array.size;	
					at(pk){
						R1_keys_receive(here.id)(pn)= new RemoteArray(new Array[Long](s1));
					}
					Array.asyncCopy( keys_array, at (pk) R1_keys_receive(here.id)(pn));
				} //end pushing	R1
				
				if(pn==0){Console.OUT.println("R1 is done");}
				
				//processing R2  <?c 20758 16635>
				keyString=20758.toString()+16635.toString();
				R0_Node=poRoot(here.id).getChild(keyString);
				R0_tuple=R0_Node.getData();
				for(j in (0..(N-1))){
					R_key_collector(j)=new ArrayList[Long]();
				}
				
				//hash partitioning
				for(tuple in R0_tuple){				
					des=hash_3(tuple,N);
					R_key_collector(des).add(tuple);
				}
				
				for( k in (0..(N-1))) {
					val kk=(k+pn)%N;
					keys_array=R_key_collector(kk).toArray();				
					val pk=Place.place(kk);
					val s1=keys_array.size;	
					at(pk){
						R2_keys_receive(here.id)(pn)= new RemoteArray(new Array[Long](s1));
					}
					Array.asyncCopy( keys_array, at (pk) R2_keys_receive(here.id)(pn));
				} //end pushing	R2
				
				//processing R4  <?b 4069 14193>
				keyString=4069.toString()+14193.toString();
				if(poRoot(here.id).hasChild(keyString)){
					R0_Node=poRoot(here.id).getChild(keyString);
					R0_tuple=R0_Node.getData();
				}
				else{
					R0_tuple=new ArrayList[Long]();
				}
				
				for(j in (0..(N-1))){
					R_key_collector(j)=new ArrayList[Long]();
				}
				
				//hash partitioning
				for(tuple in R0_tuple){				
					des=hash_3(tuple,N);
					R_key_collector(des).add(tuple);
				}
				
				for( k in (0..(N-1))) {
					val kk=(k+pn)%N;
					keys_array=R_key_collector(kk).toArray();				
					val pk=Place.place(kk);
					val s1=keys_array.size;	
					at(pk){
						R4_keys_receive(here.id)(pn)= new RemoteArray(new Array[Long](s1));
					}
					Array.asyncCopy( keys_array, at (pk) R4_keys_receive(here.id)(pn));
				} //end pushing	R4
				
				if(pn==0){Console.OUT.println("R4 is done");}
				poRoot(here.id)=null;			
			}
		}
		
		var dis_e1:Long=System.currentTimeMillis();	
		Console.OUT.println("1st Step Takes "+(dis_e1-dis_s1)+" ms");
		
		//just for debug R
		finish for( p in Place.places()){
			at (p) async {	
				val pn:Int=here.id;	
				if(pn==0){
					var rev:Array[Int]=new Array[Int](4);
					for( k in (0..(N-1))) {
						rev(0)+=R1_keys_receive(here.id)(k).size;
						rev(1)+=R2_keys_receive(here.id)(k).size;
						rev(2)+=R3_keys_receive(here.id)(k).size;
						rev(3)+=R4_keys_receive(here.id)(k).size;
					}
					Console.OUT.print("Debug R at place 0: ");
					for(i in 0..3){
						Console.OUT.print(rev(i)+" ");
					}
					Console.OUT.println();
				}
			}
		}
		
		//make 2nd-level index
		var index_s:Long=System.currentTimeMillis();	
		finish for( p in Place.places()){
			at (p) async {
				val pn:Int=here.id;
				
				//new indexRoot
				pRoot(here.id)=new pNode(0);
				poRoot(here.id)=new poNode(0.toString());
				var tmpNode:pNode;
				var tmpoNode:poNode;
				var postring:String;			
				
				//add R1 <?a 20758 11757> 
				var keyString:String=20758.toString()+11757.toString();
				tmpoNode=new poNode(keyString);
				for(i in 0..(N-1)){
					var s1:Int=R1_keys_receive(here.id)(i).size;
					for(var j:Int=0;j<s1;j++){
						tmpoNode.insertData(R1_keys_receive(here.id)(i)(j));							
					}
				}
				poRoot(here.id).addChild(tmpoNode);
				
				//add R2 <?a 20758 16635>
				keyString=20758.toString()+16635.toString();
				tmpoNode=new poNode(keyString);
				for(i in 0..(N-1)){
					var s2:Int=R2_keys_receive(here.id)(i).size;
					for(var j:Int=0;j<s2;j++){
						tmpoNode.insertData(R2_keys_receive(here.id)(i)(j));							
					}
				}
				poRoot(here.id).addChild(tmpoNode);
				
				//add R4 <?a 4069 14193>
				keyString=4069.toString()+ 14193.toString();
				tmpoNode=new poNode(keyString);
				for(i in 0..(N-1)){
					var s4:Int=R4_keys_receive(here.id)(i).size;
					for(var j:Int=0;j<s4;j++){
						tmpoNode.insertData(R4_keys_receive(here.id)(i)(j));							
					}
				}
				poRoot(here.id).addChild(tmpoNode);
				
				if(pn==0){Console.OUT.println("2nd-levl po-index is done");}
				
				//add R3 <?c 13426 ?b>
				var p_index:Long=13426L;
				tmpNode=new pNode(p_index);
				for(i in 0..(N-1)){
					var s3:Int=R3_keys_receive(here.id)(i).size;
					for(var j:Int=0;j<s3;j++){
						tmpNode.insertData(R3_keys_receive(here.id)(i)(j),R3_payload_receive(here.id)(i)(j));							
					}
				}
				pRoot(here.id).addChild(tmpNode);		
			}
		}	
		
		var index_e:Long=System.currentTimeMillis();	
		Console.OUT.println("2nd-level indexing Takes "+(index_e-index_s)+" ms");
		
		Console.OUT.println("//// Output 2-level Index ////");
		finish for( p in Place.places()){
			at (p) async {
				val pn:Int=here.id;
				
				//print R1
				var opath:String="/data/RDF_Processing/data_1b/index2/lubm_q12/"+pn.toString()+".R1";
				var OutFile:File=new File(opath);
				val pr1=OutFile.printer(true);
				for(i in 0..(N-1)){
					var s1:Int=R1_keys_receive(here.id)(i).size;
					for(var j:Int=0;j<s1;j++){
						pr1.println(R1_keys_receive(here.id)(i)(j).toString());		
					}
				}
				pr1.flush();
				pr1.close();
				
				//print R2
				opath="/data/RDF_Processing/data_1b/index2/lubm_q12/"+pn.toString()+".R2";
				OutFile=new File(opath);
				val pr2=OutFile.printer(true);
				for(i in 0..(N-1)){
					var s2:Int=R2_keys_receive(here.id)(i).size;
					for(var j:Int=0;j<s2;j++){
						pr2.println(R2_keys_receive(here.id)(i)(j).toString());		
					}
				}
				pr2.flush();
				pr2.close();
				
				//print R3
				opath="/data/RDF_Processing/data_1b/index2/lubm_q12/"+pn.toString()+".R3";
				OutFile=new File(opath);
				val pr3=OutFile.printer(true);
				for(i in 0..(N-1)){
					var s3:Int=R3_keys_receive(here.id)(i).size;
					for(var j:Int=0;j<s3;j++){
						pr3.println(R3_keys_receive(here.id)(i)(j).toString()+"\t"+R3_payload_receive(here.id)(i)(j).toString());		
					}
				}
				pr3.flush();
				pr3.close();
				
				//print R4
				opath="/data/RDF_Processing/data_1b/index2/lubm_q12/"+pn.toString()+".R4";
				OutFile=new File(opath);
				val pr4=OutFile.printer(true);
				for(i in 0..(N-1)){
					var s4:Int=R4_keys_receive(here.id)(i).size;
					for(var j:Int=0;j<s4;j++){
						pr4.println(R4_keys_receive(here.id)(i)(j).toString());		
					}
				}
				pr4.flush();
				pr4.close();	
			}
		}
		Console.OUT.println("Output is Done ");		
	}
}
