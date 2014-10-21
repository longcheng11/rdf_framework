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

public class q8{
	
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
		val R5_keys_receive=DistArray.make[Array[RemoteArray[Long]]](d);	
		val R5_payload_receive=DistArray.make[Array[RemoteArray[Long]]](d);
		
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
				R5_keys_receive(here.id)=new Array[RemoteArray[Long]](N);
				R5_payload_receive(here.id)=new Array[RemoteArray[Long]](N);
				
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
				
				//process R3 <?a 17528 ?b> 
				var R_Node:pNode;
				var R_tuple:ArrayList[Array[Long]];
				var pindex:Long=17528L;
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
				
				//R5 <?a 5513 ?c>
				pindex=5513L;
				R_Node=pRoot(here.id).getChild(pindex);
				R_tuple=R_Node.getData();
				
				for(j in (0..(N-1))){
					R_key_collector(j)=new ArrayList[Long]();
					R_payload_collector(j)=new ArrayList[Long]();
				}
				
				for(tuple in R_tuple){
					des=hash_3(tuple(0),N);
					R_key_collector(des).add(tuple(0));
					R_payload_collector(des).add(tuple(1));
				}
				
				//push the R5 to remote places
				for( k in (0..(N-1))) {
					val kk=(k+pn)%N;
					keys_array=R_key_collector(kk).toArray();
					payload_array=R_payload_collector(kk).toArray();
					val pk=Place.place(kk);
					val s1=keys_array.size;	
					at(pk){
						R5_keys_receive(here.id)(pn)= new RemoteArray(new Array[Long](s1));
						R5_payload_receive(here.id)(pn)= new RemoteArray(new Array[Long](s1));
					}
					Array.asyncCopy( keys_array, at (pk) R5_keys_receive(here.id)(pn));
					Array.asyncCopy( payload_array, at (pk) R5_payload_receive(here.id)(pn));
				} //end pushing	
				
				if(pn==0){Console.OUT.println("R5 is done");}
				
				//clear the p Index
				pRoot(here.id)=null;
				
				//processing R1 <?a 20758 8799> 
				var keyString:String=20758.toString()+8799.toString();
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
					var rev:Array[Int]=new Array[Int](5);
					for( k in (0..(N-1))) {
						rev(0)+=R1_keys_receive(here.id)(k).size;
						rev(1)+=R2_keys_receive(here.id)(k).size;
						rev(2)+=R3_keys_receive(here.id)(k).size;
						rev(3)+=R4_keys_receive(here.id)(k).size;
						rev(5)+=R4_keys_receive(here.id)(k).size;
					}
					Console.OUT.print("Debug R at place 0: ");
					for(i in 0..4){
						Console.OUT.print(rev(i)+" ");
					}
					Console.OUT.println();
				}
			}
		}
		
		var dis_s2:Long=System.currentTimeMillis();	
		//Join on {?Y}- Redistribute {?Y}
		finish for( p in Place.places()){
			at (p) async {			
				val pn:Int=here.id;	
				
				//build R4 hash set
				var r4_hash_set:HashSet[Long]=new HashSet[Long]();
				for(i in 0..(N-1)){
					var s4:Int=R4_keys_receive(here.id)(i).size;
					for(var j:Int=0;j<s4;j++){
						r4_hash_set.add(R4_keys_receive(here.id)(i)(j));
					}
				}				
				
				//build R2 hashset based on checking R4_hashset
				var key:Long;
				var value:Long;
				var r2_hash_set:HashSet[Long]=new HashSet[Long]();
				for(i in 0..(N-1)){
					var s2:Int=R2_keys_receive(here.id)(i).size;
					for(var j:Int=0;j<s2;j++){
						key=R2_keys_receive(here.id)(i)(j);
						if(r4_hash_set.contains(key)){
							r2_hash_set.add(key);
						}
					}
				}
				r4_hash_set=null;
				
				var T1_key_collector:Array[ArrayList[Long]]=new Array[ArrayList[Long]](N);
				var T1_payload1_collector:Array[ArrayList[Long]]=new Array[ArrayList[Long]](N);	
				
				for(i in (0..(N-1))){
					T1_key_collector(i)=new ArrayList[Long]();
					T1_payload1_collector(i)=new ArrayList[Long]();
				}
				
				//checking R3 in R2_hashtable				
				var des:Int;
				for(i in 0..(N-1)){
					var s3:Int=R3_keys_receive(here.id)(i).size; 
					for(var j:Int=0;j<s3;j++){
						key=R3_keys_receive(here.id)(i)(j);
						value=R3_payload_receive(here.id)(i)(j);
						if(r2_hash_set.contains(key)){
							des=hash_3(value,N);
							T1_key_collector(des).add(value);
							T1_payload1_collector(des).add(key);
						}
					}					
				}
				
				//push to T1 at remote places
				var keys_array:Array[long];
				var payload1_array:Array[long];
				for( k in (0..(N-1))) {
					val kk=(k+pn)%N;
					keys_array=T1_key_collector(kk).toArray();
					payload1_array=T1_payload1_collector(kk).toArray();
					val pk=Place.place(kk);
					val s3=keys_array.size;	
					at(pk){
						T1_keys_receive(here.id)(pn)= new RemoteArray(new Array[Long](s3));
						T1_payloads_receive(here.id)(pn)= new RemoteArray(new Array[Long](s3));
					}
					Array.asyncCopy( keys_array, at (pk) T1_keys_receive(here.id)(pn));
					Array.asyncCopy( payload1_array, at (pk) T1_payloads_receive(here.id)(pn));
				} //end push to T1
				
				if(pn==0){Console.OUT.println("T1 is done");}
				
				//clear R2 R3 R4
				R2_keys_receive(here.id)=null;
				R3_keys_receive(here.id)=null;
				R3_payload_receive(here.id)=null;
				R4_keys_receive(here.id)=null;	
			}  
		}	
		var dis_e2:Long=System.currentTimeMillis();	
		Console.OUT.println("2nd Step Takes "+(dis_e2-dis_s2)+" ms");
		
		//just for debug T
		finish for( p in Place.places()){
			at (p) async {	
				val pn:Int=here.id;	
				if(pn==0){
					var rev:Array[Int]=new Array[Int](2);
					for( k in (0..(N-1))) {
						rev(0)+=T1_keys_receive(here.id)(k).size;
					}
					Console.OUT.print("Debug T at place 0: ");
					for(i in 0..1){
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
				
				//build T1 hashtable
				var key:Long;
				var value:Long;
				var T1_hash_table:HashMap[Long,Long]=new HashMap[Long,Long]();
				for(i in 0..(N-1)){
					var s7:Int=T1_keys_receive(here.id)(i).size;
					for(var j:Int=0;j<s7;j++){
						key=T1_keys_receive(here.id)(i)(j);
						T1_hash_table.put(key,T1_payloads_receive(here.id)(i)(j));
					}
				}
				
				//check R1 in T1_hashtable and build R1_hashtable	
				var R1_hash_table:HashMap[Long,Long]=new HashMap[Long,Long]();
				for(i in 0..(N-1)){
					var s8:Int=R1_keys_receive(here.id)(i).size; 
					for(var j:Int=0;j<s8;j++){
						key=R1_keys_receive(here.id)(i)(j);
						value=T1_hash_table.getOrElse(key,0L);
						if(value!=0L){
							R1_hash_table.put(key,value);
						}
					}					
				}	
				
				//check R5 in R1_hashtable
				for(i in 0..(N-1)){
					var s5:Int=R5_keys_receive(here.id)(i).size; 
					for(var j:Int=0;j<s5;j++){
						key=R5_keys_receive(here.id)(i)(j);
						if(R1_hash_table.containsKey(key)){
							//output results
						}
					}					
				}	
			}
		} //end async at place
		
		var join_e:Long=System.currentTimeMillis();	
		Console.OUT.println("The final Step Takes "+(join_e-join_s)+" ms");
		
		val TIME=(join_e-join_s)+(dis_e2-dis_s2)+(dis_e1-dis_s1);
		Console.OUT.println("Whole Takes "+TIME+" ms");
		
	}
}
