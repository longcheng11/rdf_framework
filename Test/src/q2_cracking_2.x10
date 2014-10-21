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

public class q2_cracking_2 {
	
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
		
		// <?a ?b ?c>
		val T1_keys_receive=DistArray.make[ArrayList[Long]](d);	
		val T1_payload1_receive=DistArray.make[ArrayList[Long]](d);
		val T1_payload2_receive=DistArray.make[ArrayList[Long]](d);
		
		// <?a ?b>
		val T2_keys_receive=DistArray.make[ArrayList[Long]](d);
		val T2_payload_receive=DistArray.make[ArrayList[Long]](d);
		
		//initialize the object at each place		
		finish for (p in Place.places()){
			at (p) async {
				
				//indexRoot
				poRoot(here.id)=new poNode(0.toString());
				
				//local list		
				T1_keys_receive(here.id)=new ArrayList[Long](N);
				T1_payload1_receive(here.id)=new ArrayList[Long](N);
				T1_payload2_receive(here.id)=new ArrayList[Long](N);
				T2_keys_receive(here.id)=new ArrayList[Long](N);
				T2_payload_receive(here.id)=new ArrayList[Long](N);	
			}
		}
		
		Console.OUT.println("///////////////// start to build 3rd-level index////////////////");
		var read_start:Long=System.currentTimeMillis();
		
		finish for( p in Place.places()){
			at (p) async {
				var pn:Int=here.id;
				
				for(e in 1..2){
					val ee=e;
					var ns:String;
					val s1="/data/RDF_Processing/data_1b/index3/lubm_q2/"+pn.toString()+".T"+ee.toString()+".gz";
					var lstring:String=gzRead(s1);
					var len:Int=lstring.length();
					var start:Int=0;
					var end:Int=0;
					var line:String;
					var triple1:Array[Long]=new Array[Long](3);
					var triple2:Array[Long]=new Array[Long](2);
					var tmpNode:pNode;
					var tmpoNode:poNode;
					var p_index:Long;
					var postring:String;
					
					switch(ee){
					
					// T1
					case 1: 
						while(start<len) {
							end=lstring.indexOf('\n',start);
							line=lstring.substring(start,end);
							start=end+1;
							triple1=Parsing(line);   //get the k,v1,v2 here
							T1_keys_receive(here.id).add(triple1(0));
							T1_payload1_receive(here.id).add(triple1(1));
							T1_payload2_receive(here.id).add(triple1(2));
						}
						break;
						
						//T2
					case 2: 
						while(start<len) {
							end=lstring.indexOf('\n',start);
							line=lstring.substring(start,end);
							start=end+1;
							triple2=Parsing2(line);   //get the k,v here
							T2_keys_receive(here.id).add(triple2(0));
							T2_payload_receive(here.id).add(triple2(1));					
						}
						break;						
					} //end switch ee
				} //end for e
				
				//build for R6
				var ns:String;
				val s2="/data/RDF_Processing/data_1b/index2/lubm_q2/"+pn.toString()+".R6.gz";
				var lstring:String=gzRead(s2);
				var len:Int=lstring.length();
				var start:Int=0;
				var end:Int=0;
				var line:String;
				var tmpoNode:poNode;
				var postring:String;
				
				postring= 20758.toString()+5811.toString();
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
		Console.OUT.println("read 3rd-level index time is "+(read_end-read_start)+" ms");
		System.gc();
		
		Console.OUT.println("///////////////// start to query////////////////");
		
		var join_s:Long=System.currentTimeMillis();	
		//final join
		finish for( p in Place.places()){
			at (p) async {
				val pn:Int=here.id;	
				
				var key:Long;
				var value:Long;
				
				//build R6 hash set <?b 20758 5811>
				var postring:String=20758.toString()+5811.toString();
				var R0_Node:poNode=poRoot(here.id).getChild(postring);
				var R0_tuple:ArrayList[Long]=R0_Node.getData();
				var r6_hash_set:HashSet[Long]=new HashSet[Long](R0_tuple.size());
				for(key1 in R0_tuple){
					r6_hash_set.add(key1);
				}	
				
				//build T2 hashtable based on checking R6_hashset
				var s8:Int=T2_keys_receive(here.id).size();
				var T2_hash_table:HashMap[Long,Long]=new HashMap[Long,Long](s8);							
				for(var j:Int=0;j<s8;j++){
					key=T2_keys_receive(here.id)(j);
					if(r6_hash_set.contains(key)){
						T2_hash_table.put(key,T2_payload_receive(here.id)(j));
					}
				}
				
				//check T1 in T2_hashtable	
				var s7:Int=T1_keys_receive(here.id).size();
				for(var j:Int=0;j<s7;j++){
					key=T1_keys_receive(here.id)(j);
					value=T2_hash_table.getOrElse(key,0L);
					if(value==T1_payload1_receive(here.id)(j)){
						//then here is a final result
					}				
				}				
			}
		} //end async at place
		
		var join_e:Long=System.currentTimeMillis();	
		Console.OUT.println("The final Step Takes "+(join_e-join_s)+" ms");
		
		val TIME=(join_e-join_s);
		Console.OUT.println("Whole Takes "+TIME+" ms");
		
	}
}