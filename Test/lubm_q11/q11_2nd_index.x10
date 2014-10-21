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

public class q11_2nd_index {
	
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
		// <?a type graduateStduent>  <?a 20758 267>
		val R1_keys_receive=DistArray.make[Array[RemoteArray[Long]]](d);	
		
		// <?a takeCourse GC0>  <?a 11886 6909>
		val R2_keys_receive=DistArray.make[Array[RemoteArray[Long]]](d);	
		
		//record the size of receive R and S
		val counters=DistArray.make[Array[AtomicInteger]](d);
		
		//initialize the object at each place		
		finish for (p in Place.places()){
			at (p) async {
				
				//indexRoot and filter tree
				poRoot(here.id)=new poNode(0.toString());
				
				//receive -  the remote arrays
				R1_keys_receive(here.id)=new Array[RemoteArray[Long]](N);
				R2_keys_receive(here.id)=new Array[RemoteArray[Long]](N);
				
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
				
				//process R1    <?a 20758 2949>			
				var R0_tuple:ArrayList[Long];
				var R0_Node:poNode;
				var keyString:String=20758.toString()+2949.toString();
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
				
				//processing R2 <?a 4069 14193>
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
						R2_keys_receive(here.id)(pn)= new RemoteArray(new Array[Long](s1));
						//counters(here.id)(0).addAndGet(s1);
					}
					Array.asyncCopy( keys_array, at (pk) R2_keys_receive(here.id)(pn));
				} //end pushing	R1
				
				if(pn==0){Console.OUT.println("R2 is done");}
				
				//clear po Index
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
					var rev:Array[Int]=new Array[Int](2);
					for( k in (0..(N-1))) {
						rev(0)+=R1_keys_receive(here.id)(k).size;
						rev(1)+=R2_keys_receive(here.id)(k).size;
					}
					Console.OUT.print("Debug R at place 0: ");
					for(i in 0..1){
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
				poRoot(here.id)=new poNode(0.toString());
				var tmpoNode:poNode;
				var postring:String;
				
				//add R1   <?a 20758 2949>	
				postring= 20758.toString()+2949.toString();
				tmpoNode=new poNode(postring);
				for(i in 0..(N-1)){
					var s1:Int=R1_keys_receive(here.id)(i).size;
					for(var j:Int=0;j<s1;j++){
						tmpoNode.insertData(R1_keys_receive(here.id)(i)(j));							
					}
				}
				poRoot(here.id).addChild(tmpoNode);
				
				//add R2 <?a 4069 14193>
				postring=4069.toString()+14193.toString();
				tmpoNode=new poNode(postring);
				for(i in 0..(N-1)){
					var s4:Int=R2_keys_receive(here.id)(i).size;
					for(var j:Int=0;j<s4;j++){
						tmpoNode.insertData(R2_keys_receive(here.id)(i)(j));							
					}
				}
				poRoot(here.id).addChild(tmpoNode);					
			}
		}	
		
		var index_e:Long=System.currentTimeMillis();	
		Console.OUT.println("2nd-level indexing Takes "+(index_e-index_s)+" ms");
		
		Console.OUT.println("//// Output 2-level Index ////");
		finish for( p in Place.places()){
			at (p) async {
				val pn:Int=here.id;
				
				//print R1
				var opath:String="/data/RDF_Processing/data_1b/index2/lubm_q11/"+pn.toString()+".R1";
				var OutFile:File=new File(opath);
				val pr2=OutFile.printer(true);
				for(i in 0..(N-1)){
					var s2:Int=R1_keys_receive(here.id)(i).size;
					for(var j:Int=0;j<s2;j++){
						pr2.println(R1_keys_receive(here.id)(i)(j).toString());		
					}
				}
				pr2.flush();
				pr2.close();
				
				//print R2
				opath="/data/RDF_Processing/data_1b/index2/lubm_q11/"+pn.toString()+".R2";
				OutFile=new File(opath);
				val pr3=OutFile.printer(true);
				for(i in 0..(N-1)){
					var s3:Int=R2_keys_receive(here.id)(i).size;
					for(var j:Int=0;j<s3;j++){
						pr3.println(R2_keys_receive(here.id)(i)(j).toString());		
					}
				}
				pr3.flush();
				pr3.close();	
			}
		}
		Console.OUT.println("Output is Done ");
		
	}
}
