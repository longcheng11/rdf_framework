import x10.io.File;
import x10.util.ArrayList;
import x10.util.List;
import x10.util.HashSet;
import x10.util.Random;
import x10.array.Array;
import x10.util.HashMap;

public class pNode {
	private var selfId:Long;  // for predicate
	protected var data:ArrayList[Array[Long]]; // for S and O
	protected var childList:HashMap[Long,pNode]; // the ID mapped to node
	
	public def this (nodeId:Long){
		selfId=nodeId;
		data=new ArrayList[Array[Long]]();	
		if(nodeId==0L){
			childList=new HashMap[Long,pNode]();
		}		
	}
	
	public def hasChild(childId:Long):boolean{
		if(childList.containsKey(childId)){
			return true;
		}
		else{
			return false;
		}		
	}
	
	public def getId()=selfId;
	
	public def getChild(childId:Long):pNode{
		return  childList.get(childId).value;
	}
	
	//delete a childNode of current node
	public def deleteChild(childId:Long){
		childList.remove(childId);				
	}
	
	//add a new childNode of current node
	public def addChild(childNode:pNode){
		var childId:Long=childNode.selfId;
		childList.put(childId,childNode);				
	}
	
	//insert data to this node
	/*public def insertData(S:Long,O:Long){
		var i:Int=data.size();
		data(i)=new Array[Long](2);
		data(i)(0)=S;
		data(i)(1)=O;
	}*/
	
	public def insertData(S:Long,O:Long){
		var a:Array[Long]=new Array[Long](2);
		a(0)=S;
		a(1)=O;
		data.add(a);
	}
	
	//return the data
	public def getData():ArrayList[Array[Long]]=data;
	
	//return the data in the form of Array[S]
	public def getDataSubjects():Array[Long]{
		var s1:Int=data.size();
		var triple:Array[Long]=new Array[Long](s1);
		for(i in 0..(s1-1)){
			triple(i)=data(i)(0); //for s
		}
		return triple;		
	}
	
	//return the data in the form of Array[O]
	public def getDataObjects():Array[Long]{
		var s1:Int=data.size();
		var triple:Array[Long]=new Array[Long](s1);
		for(i in 0..(s1-1)){
			triple(i)=data(i)(1); //for o
		}
		return triple;		
	}
}