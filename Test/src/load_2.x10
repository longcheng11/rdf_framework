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

public class load_2 {
	
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
		val pRoot=DistArray.make[HashMap[Long,Array[ArrayList[Long]]]](d);	
		val poRoot=DistArray.make[HashMap[String,ArrayList[Long]]](d);
		val psRoot=DistArray.make[HashMap[String,ArrayList[Long]]](d);

		//initialize the object at each place		
		finish for (p in Place.places()){
			at (p) async {
				
				//indexRoot and filter tree
				pRoot(here.id)=new HashMap[Long,Array[ArrayList[Long]]]();
				poRoot(here.id)=new HashMap[String,ArrayList[Long]]();
				psRoot(here.id)=new HashMap[String,ArrayList[Long]]();
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
				var postring:String;
				var rl:Array[ArrayList[Long]];
				var rs:ArrayList[Long];
				while(start<len) {
					end=lstring.indexOf('\n',start);
					line=lstring.substring(start,end);
					start=end+1;
					triple=Parsing(line);   //get the S,P,O here
					
					//for the index of P
					rl=pRoot(here.id).getOrElse(triple(1),null);					
					if(rl!=null){
						rl(0).add(triple(0));
						rl(1).add(triple(2));
					}
					else{
						pRoot(here.id).put(triple(1), new Array[ArrayList[Long]](2));
						rl=pRoot(here.id).get(triple(1)).value;
						for(i in 0..1){
							rl(i)=new ArrayList[Long]();
						}
						rl(0).add(triple(0));
						rl(1).add(triple(2));
					}
					
					//for the index of PO
					postring= triple(1).toString()+triple(2).toString();
					rs=poRoot(here.id).getOrElse(postring,null);
					if(rs!=null){
						rs.add(triple(0));
					}
					else{
						poRoot(here.id).put(postring, new ArrayList[Long]());
						poRoot(here.id).get(postring).value.add(triple(0));
					}
					
					//for the index of PS
					postring= triple(1).toString()+triple(0).toString();
					rs=psRoot(here.id).getOrElse(postring,null);
					if(rs!=null){
						rs.add(triple(2));
					}
					else{
						psRoot(here.id).put(postring, new ArrayList[Long]());
						psRoot(here.id).get(postring).value.add(triple(2));
					}
					
				}
			}
		}
		
		var read_end:Long=System.currentTimeMillis();
		Console.OUT.println("indexed time is "+(read_end-read_start)+" ms");
		System.gc();
		
	}
}
