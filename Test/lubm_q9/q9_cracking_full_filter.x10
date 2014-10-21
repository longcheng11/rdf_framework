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

public class q9_cracking_full_filter {
	
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
		val filter=DistArray.make[HashSet[Long]](d);
		
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
				filter(here.id)=new HashSet[Long]();
				
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
					val s1="/data/RDF_Processing/data_1b/index2/lubm_q91/"+pn.toString()+".R"+ee.toString()+".gz";
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
					
					// R2 <?y 12940 ?z>
					case 2: 
						p_index=12940L;
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
						
						// R5 <?x 25782 ?z> 
					case 5: 
						p_index=25782L;
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
						
						// R3 <?x 11907 ?y>
					case 3: 
						p_index=11907L;
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
						
						// R6 <?z 20758 25796>
					case 6: 
						postring= 20758.toString()+25796.toString();
						tmpoNode=new poNode(postring);
						while(start<len) {
							end=lstring.indexOf('\n',start);
							line=lstring.substring(start,end);
							start=end+1;
							tmpoNode.insertData(Long.parse(line));						
						}
						poRoot(here.id).addChild(tmpoNode);
						break;
						
						// R4 <?x 20758 8799>
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
						
						// R1 <?y 20758 11757>
					case 1: 
						postring= 20758.toString()+11757.toString();
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
				var valueList:ArrayList[Long];
				
				var f_counter_1:Int=0;
				var f_counter_2:Int=0;
				
				//build R1 hashset <?y 20758 11757>
				postring=20758.toString()+ 11757.toString();
				R0_Node=poRoot(here.id).getChild(postring);
				R0_tuple=R0_Node.getData();
				var r1_hash_set:HashSet[Long]=new HashSet[Long](R0_tuple.size());
				for(key1 in R0_tuple){
					r1_hash_set.add(key1);
				}
				
				//build R2 hashtable based on checking R1_hashset  <?y 12940 ?z>
				R_Node=pRoot(here.id).getChild(12940);
				R_tuple=R_Node.getData();
				var r2_hash_table:HashMap[Long,ArrayList[Long]]=new HashMap[Long,ArrayList[Long]]();
				for(tuple in R_tuple){
					if(filter(here.id).contains(tuple(1)) && r1_hash_set.contains(tuple(0))){
						valueList=r2_hash_table.getOrElse(tuple(0),null);
						if(valueList!=null){
							valueList.add(tuple(1));
						}
						else{
							r2_hash_table.put(tuple(0), new ArrayList[Long]());
							r2_hash_table.get(tuple(0)).value.add(tuple(1));
						}
					}
				}
				
				var T1_key_collector:Array[ArrayList[Long]]=new Array[ArrayList[Long]](N);
				var T1_payload1_collector:Array[ArrayList[Long]]=new Array[ArrayList[Long]](N);	
				var T1_payload2_collector:Array[ArrayList[Long]]=new Array[ArrayList[Long]](N);	
				var T2_key_collector:Array[ArrayList[Long]]=new Array[ArrayList[Long]](N);
				var T2_payload1_collector:Array[ArrayList[Long]]=new Array[ArrayList[Long]](N);	
				
				for(i in (0..(N-1))){
					T1_key_collector(i)=new ArrayList[Long]();
					T1_payload1_collector(i)=new ArrayList[Long]();
					T1_payload2_collector(i)=new ArrayList[Long]();
					T2_key_collector(i)=new ArrayList[Long]();
					T2_payload1_collector(i)=new ArrayList[Long]();
				}
				
				//checking R3 in R2_hashtable <?x 11907 ?y>		
				var des:Int;
				R_Node=pRoot(here.id).getChild(11907);
				R_tuple=R_Node.getData();
				for(tuple in R_tuple){
					key=tuple(0);
					valueList=r2_hash_table.getOrElse(key,null);
					if(valueList!=null){
						for(v in valueList){
							des=hash_3(v,N);
							T1_key_collector(des).add(v);
							T1_payload1_collector(des).add(tuple(1));
							T1_payload2_collector(des).add(key);
							f_counter_1++;
						}
					}					
				}
				
				postring=20758.toString()+8799.toString();
				R0_Node=poRoot(here.id).getChild(postring);
				R0_tuple=R0_Node.getData();
				var r4_hash_set:HashSet[Long]=new HashSet[Long](R0_tuple.size());
				for(key1 in R0_tuple){
					r4_hash_set.add(key1);
				}	
				
				for(i in (0..(N-1))){
					T2_key_collector(i)=new ArrayList[Long]();
					T2_payload1_collector(i)=new ArrayList[Long]();
				}
				
				//check R5 based on R4_hashset <?x 25782 ?z> 
				R_Node=pRoot(here.id).getChild(25782);
				R_tuple=R_Node.getData();
				for(tuple in R_tuple){
					key=tuple(0);
					value=tuple(1);
					if(filter(here.id).contains(tuple(1)) && r4_hash_set.contains(key)){
						des=hash_3(value,N);
						T2_key_collector(des).add(value);
						T2_payload1_collector(des).add(key);
						f_counter_2++;
					}				
				}
				
				//push to T1 T2 at remote places
				var keys_array:Array[long];
				var payload1_array:Array[long];
				var payload2_array:Array[long];
				var keys_array3:Array[long];
				var payload4_array:Array[long];
				for( k in (0..(N-1))) {
					val kk=(k+pn)%N;
					keys_array=T1_key_collector(kk).toArray();
					payload1_array=T1_payload1_collector(kk).toArray();
					payload2_array=T1_payload2_collector(kk).toArray();					
					keys_array3=T2_key_collector(kk).toArray();
					payload4_array=T2_payload1_collector(kk).toArray();
					val pk=Place.place(kk);
					val s3=keys_array.size;	
					val s4=keys_array3.size;
					
					at(pk){
						T1_keys_receive(here.id)(pn)= new RemoteArray(new Array[Long](s3));
						T1_payload1_receive(here.id)(pn)= new RemoteArray(new Array[Long](s3));
						T1_payload2_receive(here.id)(pn)= new RemoteArray(new Array[Long](s3));
						
						T2_keys_receive(here.id)(pn)= new RemoteArray(new Array[Long](s4));
						T2_payload_receive(here.id)(pn)= new RemoteArray(new Array[Long](s4));
					}
					Array.asyncCopy( keys_array, at (pk) T1_keys_receive(here.id)(pn));
					Array.asyncCopy( payload1_array, at (pk) T1_payload1_receive(here.id)(pn));
					Array.asyncCopy( payload2_array, at (pk) T1_payload2_receive(here.id)(pn));
					
					Array.asyncCopy( keys_array3, at (pk) T2_keys_receive(here.id)(pn));
					Array.asyncCopy( payload4_array, at (pk) T2_payload_receive(here.id)(pn));
					
				} //end push to T1
				
				if(pn==0){Console.OUT.println("T1 is done");}
				
				//clear R1 R2 R3   <?y 20758 11757>  <?y 12940 ?z> <?x 11907 ?y>		
				postring=20758.toString()+11757.toString();
				poRoot(here.id).deleteChild(postring);
				pRoot(here.id).deleteChild(12940);
				pRoot(here.id).deleteChild(1907);
				
				//clear R4 R5 <?x 20758 8799> <?x 25782 ?z> 
				postring=20758.toString()+8799.toString();
				poRoot(here.id).deleteChild(postring);
				pRoot(here.id).deleteChild(25782);	
				
				//debug on the filters
				Console.OUT.println("place: "+pn+" "+filter(here.id).size()+" "+f_counter_1+" "+f_counter_2);
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
					Console.OUT.print("Debug T at place: "+pn+" ");
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
				
				var postring:String;
				var R0_Node:poNode;
				var R0_tuple:ArrayList[Long];
				var key:Long;
				var value:Long;
				
				//build R6 hash set <?z 20758 25796>
				postring=20758.toString()+25796.toString();
				R0_Node=poRoot(here.id).getChild(postring);
				R0_tuple=R0_Node.getData();
				var r6_hash_set:HashSet[Long]=new HashSet[Long](R0_tuple.size());
				for(key1 in R0_tuple){
					r6_hash_set.add(key1);
				}	
				
				//build T1 hashtable based on checking R6_hashset
				var T1_hash_table:HashMap[Long,Long]=new HashMap[Long,Long]();
				for(i in 0..(N-1)){
					var s7:Int=T1_keys_receive(here.id)(i).size;
					for(var j:Int=0;j<s7;j++){
						key=T1_keys_receive(here.id)(i)(j);
						if(r6_hash_set.contains(key)){
							T1_hash_table.put(key,T1_payload1_receive(here.id)(i)(j));
						}
					}
				}
				
				//check T2 in T1_hashtable				
				for(i in 0..(N-1)){
					var s8:Int=T2_keys_receive(here.id)(i).size; 
					for(var j:Int=0;j<s8;j++){
						key=T2_keys_receive(here.id)(i)(j);
						value=T1_hash_table.getOrElse(key,0L);
						if(value==T2_payload_receive(here.id)(i)(j)){
							//then here is a final result
						}
					}					
				}				
			}
		} //end async at place
		
		var join_e:Long=System.currentTimeMillis();	
		Console.OUT.println("The final Step Takes "+(join_e-join_s)+" ms");
		
		val TIME=(join_e-join_s)+(dis_e-dis_s);
		Console.OUT.println("Whole Takes "+TIME+" ms");
		
	}
}

