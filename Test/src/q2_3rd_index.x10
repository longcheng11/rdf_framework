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

public class q2_3rd_index {
	
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
		val pRoot=DistArray.make[pNode](d);	
		val poRoot=DistArray.make[poNode](d);	
		
		// <?a ?b ?c>
		val T1_keys_receive=DistArray.make[Array[RemoteArray[Long]]](d);	
		val T1_payload1_receive=DistArray.make[Array[RemoteArray[Long]]](d);
		val T1_payload2_receive=DistArray.make[Array[RemoteArray[Long]]](d);
		
		// <?a ?b>
		val T2_keys_receive=DistArray.make[Array[RemoteArray[Long]]](d);	
		val T2_payload_receive=DistArray.make[Array[RemoteArray[Long]]](d);;
		
		//initialize the object at each place		
		finish for (p in Place.places()){
			at (p) async {
				
				//indexRoot
				pRoot(here.id)=new pNode(0);
				poRoot(here.id)=new poNode(0.toString());
				
				//receive -  the remote arrays			
				T1_keys_receive(here.id)=new Array[RemoteArray[Long]](N);
				T1_payload1_receive(here.id)=new Array[RemoteArray[Long]](N);
				T1_payload2_receive(here.id)=new Array[RemoteArray[Long]](N);
				T2_keys_receive(here.id)=new Array[RemoteArray[Long]](N);
				T2_payload_receive(here.id)=new Array[RemoteArray[Long]](N);		
			}
		}		
		
		Console.OUT.println("///////////////// start to build 2nd-level index////////////////");
		var read_start:Long=System.currentTimeMillis();
		
		finish for( p in Place.places()){
			at (p) async {
				var pn:Int=here.id;
				
				for(e in 1..6){
					val ee=e;
					var ns:String;
					val s1="/data/RDF_Processing/data_1b/index2/lubm_q2/"+pn.toString()+".R"+ee.toString()+".gz";
					var lstring:String=gzRead(s1);
					var len:Int=lstring.length();
					var start:Int=0;
					var end:Int=0;
					var line:String;
					var triple:Array[Long]=new Array[Long](2);
					var tmpNode:pNode;
					var tmpoNode:poNode;
					var p_index:Long;
					var postring:String;
					
					switch(ee){
					
					// R2 <?c 4069 ?b>
					case 2: 
						p_index=4069L;
						tmpNode=new pNode(p_index);
						while(start<len) {
							end=lstring.indexOf('\n',start);
							line=lstring.substring(start,end);
							start=end+1;
							triple=Parsing2(line);   //get the k,v here
							tmpNode.insertData(triple(0),triple(1));						
						}
						pRoot(here.id).addChild(tmpNode);
						break;
						
						// R3 {?a 17528 ?c}
					case 3: 
						p_index=17528L;
						tmpNode=new pNode(p_index);
						while(start<len) {
							end=lstring.indexOf('\n',start);
							line=lstring.substring(start,end);
							start=end+1;
							triple=Parsing2(line);   //get the k,v here
							tmpNode.insertData(triple(0),triple(1));						
						}
						pRoot(here.id).addChild(tmpNode);
						break;
						
						// R5 <?a 21508 ?b> 
					case 5: 
						p_index=21508L;
						tmpNode=new pNode(p_index);
						while(start<len) {
							end=lstring.indexOf('\n',start);
							line=lstring.substring(start,end);
							start=end+1;
							triple=Parsing2(line);   //get the k,v here
							tmpNode.insertData(triple(0),triple(1));						
						}
						pRoot(here.id).addChild(tmpNode);
						break;
						
						// R1 <?c 20758 16635>
					case 1: 
						postring= 20758.toString()+16635.toString();
						tmpoNode=new poNode(postring);
						while(start<len) {
							end=lstring.indexOf('\n',start);
							line=lstring.substring(start,end);
							start=end+1;
							tmpoNode.insertData(Long.parse(line));						
						}
						poRoot(here.id).addChild(tmpoNode);
						break;
						
						// R4 <?a 20758 8799>
					case 4: 
						postring= 20758.toString()+8799.toString();
						tmpoNode=new poNode(postring);
						while(start<len) {
							end=lstring.indexOf('\n',start);
							line=lstring.substring(start,end);
							start=end+1;
							tmpoNode.insertData(Long.parse(line));						
						}
						poRoot(here.id).addChild(tmpoNode);
						break;
						
						// R6 <?b 20758 5811>
					case 6: 
						postring= 20758.toString()+5811.toString();
						tmpoNode=new poNode(postring);
						while(start<len) {
							end=lstring.indexOf('\n',start);
							line=lstring.substring(start,end);
							start=end+1;
							tmpoNode.insertData(Long.parse(line));						
						}
						poRoot(here.id).addChild(tmpoNode);
						break;
						
					} //end switch ee
				} //end for e
			}
		}
		
		var read_end:Long=System.currentTimeMillis();
		Console.OUT.println("read 2nd-level index time is "+(read_end-read_start)+" ms");
		System.gc();
		
		Console.OUT.println("///////////////// start to query////////////////");
		
		var dis_s:Long=System.currentTimeMillis();			
		//Join on {?c} - Redistribute {?b}; Join on {?a} - Redistribute {?b}
		finish for( p in Place.places()){
			at (p) async {			
				val pn:Int=here.id;	
				
				var p_index:Long;
				var postring:String;
				var R0_Node:poNode;
				var R0_tuple:ArrayList[Long];
				var R_Node:pNode;
				var R_tuple:ArrayList[Array[Long]];
				var key:Long;
				var value:Long;
				
				//build R1 hashset  <?c 20758 16635>
				postring=20758.toString()+16635.toString();
				R0_Node=poRoot(here.id).getChild(postring);
				R0_tuple=R0_Node.getData();
				var r1_hash_set:HashSet[Long]=new HashSet[Long](R0_tuple.size());
				for(key1 in R0_tuple){
					r1_hash_set.add(key1);
				}				
				
				//build R2 hashtable based on checking R1_hashset <?c 4069 ?b>
				R_Node=pRoot(here.id).getChild(4069);
				R_tuple=R_Node.getData();
				var r2_hash_table:HashMap[Long,Long]=new HashMap[Long,Long]();
				for(tuple in R_tuple){
					if(r1_hash_set.contains(tuple(0))){
						r2_hash_table.put(tuple(0),tuple(1));
					}
				}
				
				var T1_key_collector:Array[ArrayList[Long]]=new Array[ArrayList[Long]](N);
				var T1_payload1_collector:Array[ArrayList[Long]]=new Array[ArrayList[Long]](N);	
				var T1_payload2_collector:Array[ArrayList[Long]]=new Array[ArrayList[Long]](N);	
				
				for(i in (0..(N-1))){
					T1_key_collector(i)=new ArrayList[Long]();
					T1_payload1_collector(i)=new ArrayList[Long]();
					T1_payload2_collector(i)=new ArrayList[Long]();
				}
				
				//checking R3 in R2_hashtable	<?a 17528 ?c>			
				var des:Int;
				R_Node=pRoot(here.id).getChild(17528);
				R_tuple=R_Node.getData();
				for(tuple in R_tuple){
					key=tuple(0);
					value=r2_hash_table.getOrElse(key,0L);
					if(value!=0L){
						des=hash_3(value,N);
						T1_key_collector(des).add(value);
						T1_payload1_collector(des).add(tuple(1));
						T1_payload2_collector(des).add(key);
					}					
				}
				
				//push to T1 at remote places
				var keys_array:Array[long];
				var payload1_array:Array[long];
				var payload2_array:Array[long];
				for( k in (0..(N-1))) {
					val kk=(k+pn)%192;
					keys_array=T1_key_collector(kk).toArray();
					payload1_array=T1_payload1_collector(kk).toArray();
					payload2_array=T1_payload2_collector(kk).toArray();;
					val pk=Place.place(kk);
					val s3=keys_array.size;	
					at(pk){
						T1_keys_receive(here.id)(pn)= new RemoteArray(new Array[Long](s3));
						T1_payload1_receive(here.id)(pn)= new RemoteArray(new Array[Long](s3));
						T1_payload2_receive(here.id)(pn)= new RemoteArray(new Array[Long](s3));
					}
					Array.asyncCopy( keys_array, at (pk) T1_keys_receive(here.id)(pn));
					Array.asyncCopy( payload1_array, at (pk) T1_payload1_receive(here.id)(pn));
					Array.asyncCopy( payload2_array, at (pk) T1_payload2_receive(here.id)(pn));
				} //end push to T1
				
				if(pn==0){Console.OUT.println("T1 is done");}
				
				//clear R1 R2 R3
				postring=20758.toString()+16635.toString();
				poRoot(here.id).deleteChild(postring);
				pRoot(here.id).deleteChild(4069);
				pRoot(here.id).deleteChild(17528);
				
				
				//build R4 hashset <?a 20758 8799>
				postring=20758.toString()+8799.toString();
				R0_Node=poRoot(here.id).getChild(postring);
				R0_tuple=R0_Node.getData();
				var r4_hash_set:HashSet[Long]=new HashSet[Long](R0_tuple.size());
				for(key1 in R0_tuple){
					r4_hash_set.add(key1);
				}	
				
				for(i in (0..(N-1))){
					T1_key_collector(i)=new ArrayList[Long]();
					T1_payload1_collector(i)=new ArrayList[Long]();
				}
				
				//check R5 based on R4_hashset <?a 21508 ?b> 
				R_Node=pRoot(here.id).getChild(21508);
				R_tuple=R_Node.getData();
				for(tuple in R_tuple){
					key=tuple(0);
					value=tuple(1);
					if(r4_hash_set.contains(key)){
						des=hash_3(value,N);
						T1_key_collector(des).add(value);
						T1_payload1_collector(des).add(key);
					}				
				}
				
				//push to T2 at remote places
				for( k in (0..(N-1))) {
					val kk=(k+pn)%192;
					keys_array=T1_key_collector(kk).toArray();
					payload1_array=T1_payload1_collector(kk).toArray();;
					val pk=Place.place(kk);
					val s3=keys_array.size;	
					at(pk){
						T2_keys_receive(here.id)(pn)= new RemoteArray(new Array[Long](s3));
						T2_payload_receive(here.id)(pn)= new RemoteArray(new Array[Long](s3));
					}
					Array.asyncCopy( keys_array, at (pk) T2_keys_receive(here.id)(pn));
					Array.asyncCopy( payload1_array, at (pk) T2_payload_receive(here.id)(pn));
				} //end push to T2
				
				if(pn==0){Console.OUT.println("T2 is done");}
				
				//clear R4 R5
				postring=20758.toString()+8799.toString();
				poRoot(here.id).deleteChild(postring);
				pRoot(here.id).deleteChild(21508);			
			}  
		}	
		var dis_e:Long=System.currentTimeMillis();	
		Console.OUT.println("Distribution Takes "+(dis_e-dis_s)+" ms");
		
		//just for debug T
		finish for( p in Place.places()){
			at (p) async {	
				val pn:Int=here.id;	
				if(pn==0){
					var rev:Array[Int]=new Array[Int](2);
					for( k in (0..(N-1))) {
						rev(0)+=T1_keys_receive(here.id)(k).size;
						rev(1)+=T2_keys_receive(here.id)(k).size;
					}
					Console.OUT.print("Debug T at place 0: ");
					for(i in 0..1){
						Console.OUT.print(rev(i)+" ");
					}
					Console.OUT.println();
				}
			}
		}
		
		//make 3rd-level index
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
				
				//add T1, it is the simulated operations
				var p_index:Long=4069L;
				tmpNode=new pNode(p_index);
				for(i in 0..(N-1)){
					var s2:Int=T1_keys_receive(here.id)(i).size;
					for(var j:Int=0;j<s2;j++){
						tmpNode.insertData(T1_keys_receive(here.id)(i)(j),T1_payload1_receive(here.id)(i)(j));							
					}
				}
				pRoot(here.id).addChild(tmpNode);
				
				//add T2
				p_index=17528L;
				tmpNode=new pNode(p_index);
				for(i in 0..(N-1)){
					var s3:Int=T2_keys_receive(here.id)(i).size;
					for(var j:Int=0;j<s3;j++){
						tmpNode.insertData(T2_keys_receive(here.id)(i)(j),T2_payload_receive(here.id)(i)(j));							
					}
				}
				pRoot(here.id).addChild(tmpNode);						
			}
		}	
		var index_e:Long=System.currentTimeMillis();	
		Console.OUT.println("3rd-level indexing Takes "+(index_e-index_s)+" ms");
		
		//print out the 3rd index
		finish for( p in Place.places()){
			at (p) async {
				val pn:Int=here.id;	
				
				var postring:String;
				var R0_Node:poNode;
				var R0_tuple:ArrayList[Long];
				var key:Long;
				var value:Long;
				
				//print T1
				var opath:String="/data/RDF_Processing/data_1b/index3/lubm_q2/"+pn.toString()+".T1";
				var OutFile:File=new File(opath);
				val pt1=OutFile.printer(true);
				for(i in 0..(N-1)){
					var s2:Int=T1_keys_receive(here.id)(i).size;
					for(var j:Int=0;j<s2;j++){
						pt1.println(T1_keys_receive(here.id)(i)(j).toString()+"\t"+T1_payload1_receive(here.id)(i)(j).toString()+"\t"+T1_payload2_receive(here.id)(i)(j).toString());		
					}
				}
				pt1.flush();
				pt1.close();
				
				//print T2
				opath="/data/RDF_Processing/data_1b/index3/lubm_q2/"+pn.toString()+".T2";
				OutFile=new File(opath);
				val pt2=OutFile.printer(true);
				for(i in 0..(N-1)){
					var s2:Int=T1_keys_receive(here.id)(i).size;
					for(var j:Int=0;j<s2;j++){
						pt2.println(T2_keys_receive(here.id)(i)(j).toString()+"\t"+T2_payload_receive(here.id)(i)(j).toString());		
					}
				}
				pt2.flush();
				pt2.close();				
			}
		} //end async at place
		
		Console.OUT.println("3rd-index Output is Done ");
		
	}
}
