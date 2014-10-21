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

public class initial_index {
	
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
		val poRoot_e=DistArray.make[poNode](d);	
		
		//triple list
		val triple_list=DistArray.make[ArrayList[String]](d);			
		
		//initialize the object at each place		
		finish for (p in Place.places()){
			at (p) async {
				
				//indexRoot and filter tree
				pRoot(here.id)=new pNode(0);
				poRoot(here.id)=new poNode(0.toString());
				poRoot_e(here.id)=new poNode(0.toString());
				triple_list(here.id)=new ArrayList[String]();
				
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
				while(start<len) {
					end=lstring.indexOf('\n',start);
					line=lstring.substring(start,end);
					start=end+1;					
					triple_list(here.id).add(line);					
				}
			}
		}
		
		var read_end:Long=System.currentTimeMillis();
		Console.OUT.println("read time is "+(read_end-read_start)+" ms");
		System.gc();
		
		var index_s:Long=System.currentTimeMillis();
		finish for( p in Place.places()){
			at (p) async {
				var pn:Int=here.id;
				
				var triple:Array[Long]=new Array[Long](3);
				var tmpNode:pNode;
				var tmpoNode:poNode;
				var postring:String;
				for(line in triple_list(here.id)){
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
					
					//for the index of PS
					postring= triple(1).toString()+triple(0).toString();
					if(poRoot_e(here.id).hasChild(postring)){
						tmpoNode=poRoot_e(here.id).getChild(postring);
						tmpoNode.insertData(triple(2));
					}
					else{
						tmpoNode=new poNode(postring);
						tmpoNode.insertData(triple(2));
						poRoot_e(here.id).addChild(tmpoNode);
					}
					
				} //end for				
			}
		}
		
		var index_e:Long=System.currentTimeMillis();
		Console.OUT.println("in-memory indexing time is "+(index_e-index_s)+" ms");		
	}
}
