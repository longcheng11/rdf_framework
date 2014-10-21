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

public class q3_cracking {
	
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
		
		// <?a>
		val T1_keys_receive=DistArray.make[ArrayList[Long]](d);	
		
		// <?a>
		val T2_keys_receive=DistArray.make[ArrayList[Long]](d);
		
		//initialize the object at each place		
		finish for (p in Place.places()){
			at (p) async {
				
				//indexRoot
				poRoot(here.id)=new poNode(0.toString());
				
				//local list		
				T1_keys_receive(here.id)=new ArrayList[Long](N);
				T2_keys_receive(here.id)=new ArrayList[Long](N);
			}
		}
		
		Console.OUT.println("///////////////// start to build 3rd-level index////////////////");
		var read_start:Long=System.currentTimeMillis();
		
		finish for( p in Place.places()){
			at (p) async {
				var pn:Int=here.id;
				
				//build for R1 <?a 20758 267>
				var ns:String;
				val s2="/data/RDF_Processing/data_1b/index2/lubm_q3/"+pn.toString()+".R1.gz";
				var lstring:String=gzRead(s2);
				var len:Int=lstring.length();
				var start:Int=0;
				var end:Int=0;
				var line:String;
				var tmpoNode:poNode;
				var postring:String;
				
				postring= 20758.toString()+267.toString();
				tmpoNode=new poNode(postring);
				while(start<len) {
					end=lstring.indexOf('\n',start);
					line=lstring.substring(start,end);
					start=end+1;
					tmpoNode.insertData(Long.parse(line));						
				}
				poRoot(here.id).addChild(tmpoNode);
				
				
				//build for R2 <?a 11886 6909>
				val s3="/data/RDF_Processing/data_1b/index2/lubm_q3/"+pn.toString()+".R2.gz";
				lstring=gzRead(s3);
				len=lstring.length();
				start=0;
				end=0;
				postring= 11886.toString()+6909.toString();
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
				
				//build R2 hash set <?a 11886 6909>
				var R0_Node:poNode;
				var R0_tuple:ArrayList[Long];
				var postring:String=11886.toString()+6909.toString();				
				if(poRoot(here.id).hasChild(postring)){
					R0_Node=poRoot(here.id).getChild(postring);
					R0_tuple=R0_Node.getData();
				}
				else{
					R0_tuple=new ArrayList[Long]();
				}
				
				var r2_hash_set:HashSet[Long]=new HashSet[Long](R0_tuple.size());
				for(key1 in R0_tuple){
					r2_hash_set.add(key1);
				}	
				
				//check R1 based on checking R2_hashset <?a 20758 267>
				postring=20758.toString()+267.toString();
				if(poRoot(here.id).hasChild(postring)){
					R0_Node=poRoot(here.id).getChild(postring);
					R0_tuple=R0_Node.getData();
				}
				else{
					R0_tuple=new ArrayList[Long]();
				}					
				for(key1 in R0_tuple){
					if(r2_hash_set.contains(key1)){
						//output the results ?a
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