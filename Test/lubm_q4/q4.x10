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

public class q4 {
	
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
		
		//the index root of P and PO
		val pRoot=DistArray.make[pNode](d);	
		val poRoot=DistArray.make[poNode](d);	
		
		//remote receive in the hash joins
		val R1_keys_receive=DistArray.make[Array[RemoteArray[Long]]](d);	
		val R2_keys_receive=DistArray.make[Array[RemoteArray[Long]]](d);
		val R3_keys_receive=DistArray.make[Array[RemoteArray[Long]]](d);
		val R3_payloads_receive=DistArray.make[Array[RemoteArray[Long]]](d);
		val R4_keys_receive=DistArray.make[Array[RemoteArray[Long]]](d);
		val R4_payloads_receive=DistArray.make[Array[RemoteArray[Long]]](d);
		val R5_keys_receive=DistArray.make[Array[RemoteArray[Long]]](d);
		val R5_payloads_receive=DistArray.make[Array[RemoteArray[Long]]](d);
		
		//record the size of receive R and S
		val counters=DistArray.make[Array[AtomicInteger]](d);
		
		//initialize the object at each place		
		finish for (p in Place.places()){
			at (p) async {
				
				//indexRoot and filter tree
				pRoot(here.id)=new pNode(0L);
				poRoot(here.id)=new poNode(0.toString());
				
				//receive -  the remote arrays
				R1_keys_receive(here.id)=new Array[RemoteArray[Long]](N);
				R2_keys_receive(here.id)=new Array[RemoteArray[Long]](N);
				R3_keys_receive(here.id)=new Array[RemoteArray[Long]](N);
				R3_payloads_receive(here.id)=new Array[RemoteArray[Long]](N);
				R4_keys_receive(here.id)=new Array[RemoteArray[Long]](N);
				R4_payloads_receive(here.id)=new Array[RemoteArray[Long]](N);
				R5_keys_receive(here.id)=new Array[RemoteArray[Long]](N);
				R5_payloads_receive(here.id)=new Array[RemoteArray[Long]](N);
				
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
		
		//implement the query using common hash joins
		Console.OUT.println("///////////////// start to query////////////////");
		
		var dis_s1:Long=System.currentTimeMillis();	
		//process R1 R2
		finish for( p in Place.places()){
			at (p) async {
				val pn:Int=here.id;				
				
				//process R1 <?a 20758 11757>
				var R0_tuple:ArrayList[Long];
				var R0_Node:poNode;
				var keyString:String=20758.toString()+11757.toString();
				if(poRoot(here.id).hasChild(keyString)){
					R0_Node=poRoot(here.id).getChild(keyString);
					R0_tuple=R0_Node.getData();
				}
				else{
					R0_tuple=new ArrayList[Long]();
				}				
				
				var R_key_collector:Array[ArrayList[Long]]=new Array[ArrayList[Long]](N);
				for(j in (0..(N-1))){
					R_key_collector(j)=new ArrayList[Long]();
				}
				
				var des:Int;
				//hash partitioning
				for(tuple in R0_tuple){				
					des=hash_3(tuple,N);
					R_key_collector(des).add(tuple);
				}
				
				var keys_array:Array[Long];
				for( k in (0..(N-1))) {
					val kk=(k+pn)%N;
					keys_array=R_key_collector(kk).toArray();				
					val pk=Place.place(kk);
					val s1=keys_array.size;	
					at(pk){
						R1_keys_receive(here.id)(pn)= new RemoteArray(new Array[Long](s1));
						//counters(here.id)(1).addAndGet(s1);
					}
					Array.asyncCopy( keys_array, at (pk) R1_keys_receive(here.id)(pn));
				} //end pushing	R1
				
				if(pn==0){Console.OUT.println("R1 is done");}
				
				//processing R2  <?a 13426 20871>
				keyString=13426.toString()+20871.toString();				
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
						R2_keys_receive(here.id)(pn)= new RemoteArray(new Array[Long](s1));
						counters(here.id)(0).addAndGet(s1);
					}
					Array.asyncCopy( keys_array, at (pk) R2_keys_receive(here.id)(pn));
				} //end pushing	R2
				
				if(pn==0){Console.OUT.println("R2 is done");}
				poRoot(here.id)=null;	
				
				//process R3 <?a 8518 ?b> 
				var R_Node:pNode=pRoot(here.id).getChild(8518);
				var R_tuple:ArrayList[Array[Long]]=R_Node.getData();
				
				var R_payload_collector:Array[ArrayList[Long]]=new Array[ArrayList[Long]](N);
				for(j in (0..(N-1))){
					R_key_collector(j)=new ArrayList[Long]();
					R_payload_collector(j)=new ArrayList[Long]();
				}
				
				//hash partitioning
				for(tuple in R_tuple){
					des=hash_3(tuple(0),N);
					R_key_collector(des).add(tuple(0));
					R_payload_collector(des).add(tuple(1));
				}
				
				//push the R to remote places
				var payload_array:Array[long];
				for( k in (0..(N-1))) {
					val kk=(k+pn)%N;
					keys_array=R_key_collector(kk).toArray();
					payload_array=R_payload_collector(kk).toArray();
					val pk=Place.place(kk);
					val s1=keys_array.size;	
					at(pk){
						R3_keys_receive(here.id)(pn)= new RemoteArray(new Array[Long](s1));
						R3_payloads_receive(here.id)(pn)= new RemoteArray(new Array[Long](s1));
					}
					Array.asyncCopy( keys_array, at (pk) R3_keys_receive(here.id)(pn));
					Array.asyncCopy( payload_array, at (pk) R3_payloads_receive(here.id)(pn));
				} //end pushing	
				
				if(pn==0){Console.OUT.println("R3 is done");}
				
				//process R4 <?a 5513 ?b> 
				R_Node=pRoot(here.id).getChild(5513);
				R_tuple=R_Node.getData();
				
				for(j in (0..(N-1))){
					R_key_collector(j)=new ArrayList[Long]();
					R_payload_collector(j)=new ArrayList[Long]();
				}
				
				//hash partitioning
				for(tuple in R_tuple){
					des=hash_3(tuple(0),N);
					R_key_collector(des).add(tuple(0));
					R_payload_collector(des).add(tuple(1));
				}
				
				//push the R to remote places
				for( k in (0..(N-1))) {
					val kk=(k+pn)%N;
					keys_array=R_key_collector(kk).toArray();
					payload_array=R_payload_collector(kk).toArray();
					val pk=Place.place(kk);
					val s1=keys_array.size;	
					at(pk){
						R4_keys_receive(here.id)(pn)= new RemoteArray(new Array[Long](s1));
						R4_payloads_receive(here.id)(pn)= new RemoteArray(new Array[Long](s1));
					}
					Array.asyncCopy( keys_array, at (pk) R4_keys_receive(here.id)(pn));
					Array.asyncCopy( payload_array, at (pk) R4_payloads_receive(here.id)(pn));
				} //end pushing	
				
				if(pn==0){Console.OUT.println("R4 is done");}
				
				//process R5 <?a 21175 ?b> 
				R_Node=pRoot(here.id).getChild(21175);
				R_tuple=R_Node.getData();
				
				for(j in (0..(N-1))){
					R_key_collector(j)=new ArrayList[Long]();
					R_payload_collector(j)=new ArrayList[Long]();
				}
				
				//hash partitioning
				for(tuple in R_tuple){
					des=hash_3(tuple(0),N);
					R_key_collector(des).add(tuple(0));
					R_payload_collector(des).add(tuple(1));
				}
				
				//push the R to remote places
				for( k in (0..(N-1))) {
					val kk=(k+pn)%N;
					keys_array=R_key_collector(kk).toArray();
					payload_array=R_payload_collector(kk).toArray();
					val pk=Place.place(kk);
					val s1=keys_array.size;	
					at(pk){
						R5_keys_receive(here.id)(pn)= new RemoteArray(new Array[Long](s1));
						R5_payloads_receive(here.id)(pn)= new RemoteArray(new Array[Long](s1));
					}
					Array.asyncCopy( keys_array, at (pk) R5_keys_receive(here.id)(pn));
					Array.asyncCopy( payload_array, at (pk) R5_payloads_receive(here.id)(pn));
				} //end pushing	
				
				if(pn==0){Console.OUT.println("R5 is done");}
				pRoot(here.id)=null;									
			}
		}
		
		var dis_e1:Long=System.currentTimeMillis();	
		Console.OUT.println("1st Step Takes "+(dis_e1-dis_s1)+" ms");
		
		//just for debug R
		finish for( p in Place.places()){
			at (p) async {	
				val pn:Int=here.id;	
				if(pn==0){
					var rev:Array[Int]=new Array[Int](5);
					for( k in (0..(N-1))) {
						rev(0)+=R1_keys_receive(here.id)(k).size;
						rev(1)+=R2_keys_receive(here.id)(k).size;
						rev(2)+=R3_keys_receive(here.id)(k).size;
						rev(3)+=R4_keys_receive(here.id)(k).size;
						rev(4)+=R5_keys_receive(here.id)(k).size;
					}
					Console.OUT.print("Debug R at place 0: ");
					for(i in 0..4){
						Console.OUT.print(rev(i)+" ");
					}
					Console.OUT.println();
				}
			}
		}
		
		var join_s:Long=System.currentTimeMillis();	
		//final join
		finish for( p in Place.places()){
			at (p) async {			
				val pn:Int=here.id;	
				
				//build R2 hash set
				val s0:Int=counters(here.id)(0).get();
				var r2_hash_set:HashSet[Long]=new HashSet[Long](s0);
				for(i in 0..(N-1)){
					var s1:Int=R2_keys_receive(here.id)(i).size;
					for(var j:Int=0;j<s1;j++){
						r2_hash_set.add(R2_keys_receive(here.id)(i)(j));
					}
				}				
				
				//check R1 in R2_hashset and build hashset R1_hashset
				var r1_hash_set:HashSet[Long]=new HashSet[Long]();
				for(i in 0..(N-1)){
					var s2:Int=R1_keys_receive(here.id)(i).size;
					for(var j:Int=0;j<s2;j++){
						if(r2_hash_set.contains(R1_keys_receive(here.id)(i)(j))){
							r1_hash_set.add(R1_keys_receive(here.id)(i)(j));
						}
					}
				}
				r2_hash_set=null;
				
				//check R4 in R1_hashset and build hashset R4_hashset
				var r4_hash_set:HashSet[Long]=new HashSet[Long]();
				for(i in 0..(N-1)){
					var s2:Int=R4_keys_receive(here.id)(i).size;
					for(var j:Int=0;j<s2;j++){
						if(r1_hash_set.contains(R4_keys_receive(here.id)(i)(j))){
							r4_hash_set.add(R4_keys_receive(here.id)(i)(j));
						}
					}
				}
				r1_hash_set=null;
				
				//check R5 in R4_hashset and build hashset R5_hashset
				var r5_hash_set:HashSet[Long]=new HashSet[Long]();
				for(i in 0..(N-1)){
					var s2:Int=R5_keys_receive(here.id)(i).size;
					for(var j:Int=0;j<s2;j++){
						if(r4_hash_set.contains(R5_keys_receive(here.id)(i)(j))){
							r5_hash_set.add(R5_keys_receive(here.id)(i)(j));
						}
					}
				}
				r4_hash_set=null;
				
				//check R3 in R5_hashset
				for(i in 0..(N-1)){
					var s2:Int=R3_keys_receive(here.id)(i).size;
					for(var j:Int=0;j<s2;j++){
						if(r5_hash_set.contains(R3_keys_receive(here.id)(i)(j))){
							//out put the results
						}
					}
				}			
			}  
		}	
		
		var join_e:Long=System.currentTimeMillis();	
		Console.OUT.println("The final Step Takes "+(join_e-join_s)+" ms");
		
		val TIME=(join_e-join_s)+(dis_e1-dis_s1);
		Console.OUT.println("Whole Takes "+TIME+" ms");
		
	}
}
