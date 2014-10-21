import x10.io.File;
import x10.io.FileWriter;
import x10.util.ArrayList;
import x10.util.List;
import x10.array.Array;
import x10.util.HashMap;
import x10.io.ReaderIterator;
import x10.util.StringBuilder;
import x10.util.HashSet;
import x10.compiler.Native;
import x10.compiler.NativeCPPInclude;
import x10.compiler.NativeCPPCompilationUnit;

@NativeCPPInclude("gzRead.h")
@NativeCPPCompilationUnit("gzRead.cc")

//encode all the triples and ouput them to disk

public class encode {
	
	@Native("c++","gzRead(#1->c_str())")
	static native def gzRead(file:String):String;
	
	public static def Serialize(A:Array[String],B:Array[Char]){
		var size:Int=A.size;
		var num1:Int=0;
		for (i in (0..(size-1))){
			val b=A(i);
			val c=b.length();
			for(j in (0..(c-1))){
				B(num1)=b(j);
				num1++;
			}
			B(num1)='\n';
			num1++;
		}
	}
	
	public static def DeSerialize(A:RemoteArray[Char],B:Array[String]){
		var size:Int=A.size;
		var tmp:StringBuilder=new StringBuilder();
		var num1:Int=0;
		for(i in (0..(size-1))){
			if(A(i)!='\n') {
				tmp.add(A(i));
			}
			else {
				B(num1)=tmp.toString();
				num1++;
				tmp=new StringBuilder();
			}	  	
		}
		tmp=null;
	}
	
	public static def toByte(x:Long):Array[Byte]{
		var tb:Array[Byte]=new Array[Byte](8);
		tb(7) = (x >> 56) as Byte;
		tb(6) = (x >> 48) as Byte;
		tb(5) = (x >> 40) as Byte;
		tb(4) = (x >> 36) as Byte;
		tb(3) = (x >> 24) as Byte;
		tb(2) = (x >> 16) as Byte;
		tb(1) = (x >> 8) as Byte;
		tb(0) = (x >> 0) as Byte;
		return tb;
	}
	
	//Parsing N-Triples
	public static def Parsing(s:String):Array[String]{ 		
		var value:Array[String]=new Array[String](3);
		
		//parsing the subject
		if (s.startsWith("<")) {  //is URI
			value(0)=s.substring(0,s.indexOf('>')+1).toLowerCase(); 
		} else { //is blank node
			value(0) =s.substring(0,s.indexOf(' ')).toLowerCase();
		}
		
		//parsing the predicate, only URI
		var s1:String=s.substring(value(0).length()+1);
		value(1)=s1.substring(0,s1.indexOf('>')+1).toLowerCase();
		
		//parsing the object
		var s2:String=s1.substring(value(1).length()+1);
		if(s2.startsWith("<")) {  // is URI
			value(2)=s2.substring(0,s2.indexOf('>')+1).toLowerCase();
		}
		else if(s2.charAt(0) == '"') { //is literal 
			value(2) = s2.substring(0,s2.substring(1).indexOf('"') + 2);
			var s3:String =s2.substring(value(2).length(), s2.length());
			value(2) += s3.substring(0,s3.indexOf(' ')).toLowerCase();
		}
		else{ //is blank node
			value(2) =s2.substring(0,s2.indexOf(' ')).toLowerCase();
		}
		return value;
	} 
	
	//parsing N-QUAD, the context could be URI/Literal/NodeID
	public static def Parsing2(s:String):Array[String]{ 		
		var value:Array[String]=new Array[String](4);
		try{
			//parsing the subject
			if (s.startsWith("<")) {  //is URI
				value(0)=s.substring(0,s.indexOf('>')+1).toLowerCase(); 
			} else { //is blank node
				value(0) =s.substring(0,s.indexOf(' ')).toLowerCase();
			}
			
			//parsing the predicate, only URI
			var s1:String=s.substring(value(0).length()+1);
			value(1)=s1.substring(0,s1.indexOf('>')+1).toLowerCase();
			
			//parsing the object
			var s2:String=s1.substring(value(1).length()+1);
			if(s2.startsWith("<")) {  // is URI
				value(2)=s2.substring(0,s2.indexOf('>')+1).toLowerCase();
			}
			else if(s2.charAt(0) == '"') { //is literal 
				value(2) = s2.substring(0,s2.substring(1).indexOf('"') + 2);
				var s3:String =s2.substring(value(2).length(), s2.length());
				value(2) += s3.substring(0,s3.indexOf(' ')).toLowerCase();
			}
			else{ //is blank node
				value(2) =s2.substring(0,s2.indexOf(' ')).toLowerCase();
			}
			
			var s4:String=s2.substring(value(2).length()+1);
			if(s4.startsWith("<")) {  // is URI
				value(3)=s4.substring(0,s4.indexOf('>')+1).toLowerCase();
			}
			else if(s4.charAt(0) == '"') { //is literal 
				value(3) = s4.substring(0,s4.substring(1).indexOf('"') + 2);
				var s5:String =s4.substring(value(3).length(), s4.length());
				value(3) += s5.substring(0,s5.indexOf(' ')).toLowerCase();
			}
			else{ //is blank node
				value(3) =s4.substring(0,s4.indexOf(' ')).toLowerCase();
			}
			
		} catch (Exception){
			value(0)=null;
			value(1)=null;
			value(2)=null;
			value(3)=null;
		}
		return value;
	}
	
	public static def RSHash(str:String,pieces:Int):Int {
		
		//get the HashValue of the String
		var b:Int = 378551;
		var a:Int = 63689;
		var hash:Long = 0;
		
		for (var i:Int = 0; i < str.length(); i++) {
			hash = hash * a + str.charAt(i).ord();
			a = a * b;
		}
		
		//MOD (Place Location) of the HashValue
		var Mod_O:Long;
		var mod_o:Int;		
		Mod_O=hash%pieces;
		if(Mod_O>=0){
			mod_o=Mod_O as Int;
		}
		else{
			mod_o=(Mod_O as Int)+pieces; 	//the mod value would be less than 0, so plus the divisor
		}				
		return mod_o;
	}  		
	
	public static def main(args: Array[String]) {
		// TODO auto-generated stub
		
		/**define the partition number of the file */
		val N:Int=Place.MAX_PLACES;
		Console.OUT.println("the number of places is "+N);
		
		val loop:Int=Int.parseInt(args(0));
		//val InPath:String=args(1);
		// val chunks:Int=Int.parseInt(args(1));
		val r:Region=0..(N-1);
		val d:Dist=Dist.makeBlock(r);
		
		/**initialize the Dictionary Tables on each place*/
		val table=DistArray.make[HashMap[String,Long]](d);
		val key_collector=DistArray.make[ArrayList[Array[String]]](d);		
		val term_collector=DistArray.make[ArrayList[String]](d);		
		val table_0=DistArray.make[ArrayList[RemoteArray[Char]]](d);		
		val table_2_value=DistArray.make[ArrayList[RemoteArray[Long]]](d);
		val counters=DistArray.make[Array[Int]](d);
		val triple_list=DistArray.make[ArrayList[String]](d);
		
		val pfilter=DistArray.make[HashSet[Long]](d);
		
		finish for (p in table.dist.places()){
			at (p) async {
				table(here.id)= new HashMap[String,Long]();	
				key_collector(here.id)=new ArrayList[Array[String]](N);
				table_0(here.id)= new ArrayList[RemoteArray[Char]](N);
				table_2_value(here.id)= new ArrayList[RemoteArray[Long]](N);
				term_collector(here.id)=new ArrayList[String]();
				counters(here.id)=new Array[Int](1,0);
				triple_list(here.id)=new ArrayList[String]();
				
				pfilter(here.id)=new HashSet[Long]();
			}
		}
		
		//Console.OUT.println("///////////////// Start to Read Triples////////////////");
		var read_start:Long=System.currentTimeMillis();
		
		for (var IE:int=0;IE<loop;IE++){ 
			val I=IE;
			
			//read triples to memeory
			finish for( p in Place.places()){
				at (p) async {
					triple_list(here.id).clear();
					System.gc();					
					for(var f111:Int=here.id+N*I;f111<N*(I+1);f111=f111+N){
						val s1="/data/lubm1b_8k/"+f111.toString()+".nt.gz";
						var lstring:String=gzRead(s1);
						var len:Int=lstring.length();
						var start:Int=0;
						var end:Int=0;
						var line:String;
						var value:Array[String];
						while(start<len) {
							value=new Array[String](2);
							end=lstring.indexOf('\n',start);
							line=lstring.substring(start,end);
							start=end+1;
							triple_list(here.id).add(line);
						}
					}
					System.gc();
				}
			}
			
			var read_start_1:Long=System.currentTimeMillis();
			
			//initilize local object			
			finish for (p in table.dist.places()){
				at (p) async {key_collector(here.id)=new ArrayList[Array[String]](N);
				table_0(here.id)= new ArrayList[RemoteArray[Char]](N);
				table_2_value(here.id)= new ArrayList[RemoteArray[Long]](N);
				term_collector(here.id)=new ArrayList[String]();
				counters(here.id)=new Array[Int](1,0);
				}
			}
			
			finish for( p in Place.places()){
				at (p) async {
					var hash_collector:Array[HashSet[String]]=new Array[HashSet[String]](N); 
					for(n in (0..(N-1))){
						hash_collector(n)=new HashSet[String]();
					}
					
					var ns:String;
					var value:Array[String];
					for(line in triple_list(here.id)){						
						value=new Array[String](2);
						value=Parsing(line);   //parsing
						//if(value(0)!=null) {
						for( i in (0..2)){
							var loc:Int=RSHash(value(i),N);
							if(!hash_collector(loc).contains(value(i))){
								hash_collector(loc).add(value(i));
							}
							term_collector(here.id).add(value(i));
						}
						//} //end if  this part for N-Quad parsing 
					} //end for line
					
					for(n in (0..(N-1))){
						var size:Int=hash_collector(n).size();
						counters(here.id)(0)+=size;
						key_collector(here.id)(n)=new Array[String](size);
						val iter= hash_collector(n).iterator();
						var j:Int=0;
						while(iter.hasNext()){
							val entry=iter.next();
							key_collector(here.id)(n)(j)= entry;
							j++;
						}
					}
					//Console.OUT.println("Hash Collector Finished FROM "+here.id);
					var Ser_1:Array[Char];
					for( k in (0..(N-1))) {  //push keys
						val kk=k;
						val pk=Place.place(k);
						val size=key_collector(here.id)(k).size;
						var num:Int=0;
						var a:Int;
						for (k_1 in (0..(size-1))){
							a=key_collector(here.id)(k)(k_1).length()+1;
							num+=a;
						}
						Ser_1=new Array[Char](num);
						Serialize(key_collector(here.id)(k),Ser_1);
						val SIZE=Ser_1.size;
						val local=here.id;
						at(pk){
							table_0(here.id)(local)= new RemoteArray(new Array[Char](SIZE));
						}
						Array.asyncCopy( Ser_1, at (pk) table_0(here.id)(local));
					}
					//Console.OUT.println("items push finished from: "+here.id);
				}
			}
			
			//START TO SORT AND PUSH BACK
			finish for( p in Place.places()){
				at (p) async {
					val loc_2=here.id;	
					var id_1:Long=0;  //if hit use id_1
					var id_2:Long=loc_2+N*table(here.id).size(); //if miss use id_2
					
					//prepare for output dictionay	
					//var DictFile_k:ArrayList[String]=new ArrayList[String]();
					//var DictFile_v:ArrayList[Long]=new ArrayList[Long]();
					var Deser_1:Array[String];
					var value_2:Array[Long];
					for( k in (0..(N-1))) {
						val kk_2=k;
						val pk_2=Place.place(k);
						val size=table_0(here.id)(k).size;							
						var num:Int=0;
						for(k_2 in (0..(size-1))){
							if(table_0(here.id)(k)(k_2)=='\n') {
								num++;
							}
						}
						Deser_1=new Array[String](num);
						DeSerialize(table_0(here.id)(k),Deser_1);
						val SIZE_2=Deser_1.size;
						value_2=new Array[Long](SIZE_2);
						var e:Int=0;
						for (i in (0..(SIZE_2-1))){
							var s:String=Deser_1(i);
							if(table(here.id).containsKey(s)){
								id_1=table(here.id).get(s).value;
								value_2(e)=id_1	;
								e++;
							}
							else {
								id_2+=N;
								table(here.id).put(s,id_2);
								value_2(e)=id_2;
								e++;
								//DictFile_k.add(s);
								//DictFile_v.add(id_2);
							}
						}
						at (pk_2) {
							table_2_value(here.id)(loc_2)=new RemoteArray(new Array[Long](SIZE_2));   
						}                                  
						Array.asyncCopy(value_2,at(pk_2) table_2_value(here.id)(loc_2));    
					}
				}  //end async
			} //end finish
			
			//ENCODE and Output 
			finish for( p in Place.places()){
				at (p) async {    
					
					var opath:String="/data/RDF_Processing/data_1b/"+here.id.toString()+".long_2";
					var OutFile:File=new File(opath);
					val pr=OutFile.printer(true);
					var tmp_dict:HashMap[String,Long]=new HashMap[String,Long](counters(here.id)(0));
					var SIZE_3:Int;
					for( k in (0..(N-1))) {
						SIZE_3=table_2_value(here.id)(k).size; 					
						for (i in (0..(SIZE_3-1))){
							tmp_dict.put(key_collector(here.id)(k)(i),table_2_value(here.id)(k)(i));
						}
					}	
					
					//output the predicates and some objects
					var s1:String="<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#graduatestudent>";
					var s2:String="<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#university>";
					var s3:String="<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#department>";
					var s4:String="<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#fullprofessor>";
					var s5:String="<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduatestudent>";
					var s6:String="<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#course>";
					var s7:String="<http://www.department0.university0.edu/graduatecourse0>";
					var s8:String="<http://www.department0.university0.edu/assistantprofessor0>";
					var s9:String="<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publication>";
					var s10:String="<http://www.department0.university0.edu>";
					var s11:String="<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#associateprofessor>";
					var s12:String="<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#assistantprofessor>";
					var s13:String="<http://www.department0.university0.edu/associateprofessor0>";
					var s14:String="<http://www.university0.edu>";
					var s15:String="<http://www.department0.university0.edu/graduateCourse0>";
					var oFilter:HashSet[String]=new HashSet[String]();
					oFilter.add(s1); oFilter.add(s2); oFilter.add(s3); oFilter.add(s4); oFilter.add(s5); oFilter.add(s6);
					oFilter.add(s7); oFilter.add(s8); oFilter.add(s9); oFilter.add(s10); oFilter.add(s11); oFilter.add(s12);
					oFilter.add(s13); oFilter.add(s14); oFilter.add(s15);
					
					var pp1:String="/data/RDF_Processing/data_1b/predicate.map2."+here.id.toString();
					var pp1File:File=new File(pp1);
					val pp1r=pp1File.printer(true);
					var y:Int=0;
					
					var IdFile:ArrayList[Long]=new ArrayList[Long]();
					var id_final:Long;
					for(term in term_collector(here.id)){
						id_final=tmp_dict.get(term).value;
						IdFile.add(id_final);
						if(y%3==1){
							if(!pfilter(here.id).contains(id_final)){
								pfilter(here.id).add(id_final);
								pp1r.println(id_final.toString()+"\t"+term);
							}
						}
						if(y%3==2){
							if(oFilter.contains(term)){
								if(!pfilter(here.id).contains(id_final)){
									pfilter(here.id).add(id_final);
									pp1r.println(id_final.toString()+"\t"+term);
								}
							}
						}						
						y++;						
					}
					pp1r.flush();
					pp1r.close();
					
					//print out the encoded triples
					var x:Int=0;
					for(i in 0..(IdFile.size()/3-1)){
						pr.println(IdFile(x).toString()+"\t"+IdFile(x+1).toString()+"\t"+IdFile(x+2).toString());
						x+=3;
					}
					pr.flush();
					pr.close();					
				}
			}
			
			var encode_end_1:Long=System.currentTimeMillis();
			Console.OUT.println(IE+" ENCODE TAKES "+(encode_end_1-read_start_1)+" ms///////////////"); 
		}
		var encode_end:Long=System.currentTimeMillis();
		Console.OUT.println("///////////WHOLE ENCODE FINISHS "+(encode_end-read_start)+" ms///////////////");                       		
	}
}

