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

public class q12_cracking {
	
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
	
	public static def Parsing2(s:String):Array[Long]{ 
		var triple:Array[Long]=new Array[Long](2);		
		var tmp:String;
		
		//parsing the subject
		tmp =s.substring(0,s.indexOf('\t'));
		triple(0)=Long.parse(tmp);
		
		//parsing the predicate
		var s1:String=s.substring(tmp.length()+1);
		triple(1)=Long.parse(s1);
		
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
		val poRoot=DistArray.make[poNode](d);	
		val pRoot=DistArray.make[pNode](d);
		
		val T1_keys_receive=DistArray.make[Array[RemoteArray[Long]]](d);	
		val T1_payload_receive=DistArray.make[Array[RemoteArray[Long]]](d);
		
		//initialize the object at each place		
		finish for (p in Place.places()){
			at (p) async {
				
				//indexRoot
				poRoot(here.id)=new poNode(0.toString());
				pRoot(here.id)=new pNode(0);
				
				T1_keys_receive(here.id)=new Array[RemoteArray[Long]](N);
				T1_payload_receive(here.id)=new Array[RemoteArray[Long]](N);
			}
		}
		
		Console.OUT.println("///////////////// read 2nd-level index////////////////");
		var read_start:Long=System.currentTimeMillis();
		
		finish for( p in Place.places()){
			at (p) async {
				var pn:Int=here.id;
				
				//build for R1 <?a 20758 11757> 
				var ns:String;
				val s2="/data/RDF_Processing/data_1b/index2/lubm_q12/"+pn.toString()+".R1.gz";
				var lstring:String=gzRead(s2);
				var len:Int=lstring.length();
				var start:Int=0;
				var end:Int=0;
				var line:String;
				var tmpoNode:poNode;
				var tmpNode:pNode;
				var postring:String;
				var pindex:Long;
				var tuple:Array[Long];
				
				postring= 20758.toString()+11757.toString();
				tmpoNode=new poNode(postring);
				while(start<len) {
					end=lstring.indexOf('\n',start);
					line=lstring.substring(start,end);
					start=end+1;
					tmpoNode.insertData(Long.parse(line));						
				}
				poRoot(here.id).addChild(tmpoNode);
				
				//build for R2  <?c 20758 16635>
				val s3="/data/RDF_Processing/data_1b/index2/lubm_q12/"+pn.toString()+".R2.gz";
				lstring=gzRead(s3);
				len=lstring.length();
				start=0;
				end=0;
				postring= 20758.toString()+16635.toString();
				tmpoNode=new poNode(postring);
				while(start<len) {
					end=lstring.indexOf('\n',start);
					line=lstring.substring(start,end);
					start=end+1;
					tmpoNode.insertData(Long.parse(line));						
				}
				poRoot(here.id).addChild(tmpoNode);	
				
				//build for R3 <?a 13426 ?b>
				val s4="/data/RDF_Processing/data_1b/index2/lubm_q12/"+pn.toString()+".R3.gz";
				lstring=gzRead(s4);
				len=lstring.length();
				start=0;
				end=0;
				pindex=13426L;
				tmpNode=new pNode(pindex);
				while(start<len) {
					end=lstring.indexOf('\n',start);
					line=lstring.substring(start,end);
					start=end+1;
					tuple=Parsing2(line);
					tmpNode.insertData(tuple(0),tuple(1));						
				}
				pRoot(here.id).addChild(tmpNode);	
				
				//build for R4  <?b 4069 14193>
				val s5="/data/RDF_Processing/data_1b/index2/lubm_q12/"+pn.toString()+".R4.gz";
				lstring=gzRead(s5);
				len=lstring.length();
				start=0;
				end=0;
				postring= 4069.toString()+14193.toString();
				tmpoNode=new poNode(postring);
				while(start<len) {
					end=lstring.indexOf('\n',start);
					line=lstring.substring(start,end);
					start=end+1;
					tmpoNode.insertData(Long.parse(line));						
				}
				poRoot(here.id).addChild(tmpoNode);	
			}
		}
		
		var read_end:Long=System.currentTimeMillis();
		Console.OUT.println("read 2nd-level index time is "+(read_end-read_start)+" ms");
		System.gc();
		
		Console.OUT.println("///////////////// start to query////////////////");
		
		var join_s:Long=System.currentTimeMillis();	
		//final join
		finish for( p in Place.places()){
			at (p) async {
				val pn:Int=here.id;	
				
				var key:Long;
				var value:Long;
				
				//build R4 hash set <?b 4069 14193>
				var R0_Node:poNode;
				var R0_tuple:ArrayList[Long];
				var postring:String=4069.toString()+14193.toString();				
				if(poRoot(here.id).hasChild(postring)){
					R0_Node=poRoot(here.id).getChild(postring);
					R0_tuple=R0_Node.getData();
				}
				else{
					R0_tuple=new ArrayList[Long]();
				}
				
				var r4_hash_set:HashSet[Long]=new HashSet[Long](R0_tuple.size());
				for(key1 in R0_tuple){
					r4_hash_set.add(key1);
				}	
				
				//check R2 based on checking R4_hashset  <?c 20758 16635> and build R2_hashset
				postring=20758.toString()+16635.toString();
				var r2_hash_set:HashSet[Long]=new HashSet[Long]();
				if(poRoot(here.id).hasChild(postring)){
					R0_Node=poRoot(here.id).getChild(postring);
					R0_tuple=R0_Node.getData();
				}
				else{
					R0_tuple=new ArrayList[Long]();
				}					
				for(key1 in R0_tuple){
					if(r4_hash_set.contains(key1)){
						r2_hash_set.add(key1);
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
				var R_Node:pNode;
				var R_tuple:ArrayList[Array[Long]];
				var pindex:Long=13426L;
				R_Node=pRoot(here.id).getChild(pindex);
				R_tuple=R_Node.getData();
				
				for(tuple in R_tuple){	
					if(r2_hash_set.contains(tuple(0))){
						des=hash_3(tuple(1),N);
						T1_key_collector(des).add(tuple(1));
						T1_payload1_collector(des).add(tuple(0));
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
						T1_payload_receive(here.id)(pn)= new RemoteArray(new Array[Long](s3));
					}
					Array.asyncCopy( keys_array, at (pk) T1_keys_receive(here.id)(pn));
					Array.asyncCopy( payload1_array, at (pk) T1_payload_receive(here.id)(pn));
				} //end push to T1
				
				if(pn==0){Console.OUT.println("T1 is done");}				
			}
		} //end async at place
		
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
						T1_hash_table.put(key,T1_payload_receive(here.id)(i)(j));
					}
				}
				
				//processing R1 <?a 20758 11757>
				var keyString:String=20758.toString()+11757.toString();
				var R1_hash_table:HashMap[Long,Long]=new HashMap[Long,Long]();
				var R0_Node:poNode;
				var R0_tuple:ArrayList[Long];
				if(poRoot(here.id).hasChild(keyString)){
					R0_Node=poRoot(here.id).getChild(keyString);
					R0_tuple=R0_Node.getData();
				}
				else{
					R0_tuple=new ArrayList[Long]();
				}
				
				for(key1 in R0_tuple){
					if(T1_hash_table.containsKey(key1)){
						//output the results
					}
				}		
			}
		}
		
		var join_e:Long=System.currentTimeMillis();	
		Console.OUT.println("The final Step Takes "+(join_e-join_s)+" ms");
		
		val TIME=(join_e-join_s);
		Console.OUT.println("Whole Takes "+TIME+" ms");
		
	}
}