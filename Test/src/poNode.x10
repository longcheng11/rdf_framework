import x10.io.File;
import x10.util.ArrayList;
import x10.util.List;
import x10.util.HashSet;
import x10.util.Random;
import x10.array.Array;
import x10.util.HashMap;

public class poNode {
	private var selfId:String;  // for predicate
	protected var data:ArrayList[Long]; // for S
	protected var childList:HashMap[String,poNode]; // the ID mapped to node
	
	public def this (nodeId:String){
		selfId=nodeId;
		data=new ArrayList[Long]();	
		if(nodeId.equals("0")){
			childList=new HashMap[String,poNode]();	
		}
	}	
	
	public def hasChild(childId:String):boolean{
		if(childList.containsKey(childId)){
			return true;
		}
		else{
			return false;
		}		
	}
	
	public def getId()=selfId;
	
	public def getChild(childId:String):poNode{
		return  childList.get(childId).value;
	}
	
	//delete a childNode of current node
	public def deleteChild(childId:String){
		childList.remove(childId);				
	}
	
	//add a new childNode of current node
	public def addChild(childNode:poNode){
		var childId:String=childNode.selfId;
		childList.put(childId,childNode);				
	}
	
	//insert subject to this node
	public def insertData(S:Long){
		data.add(S);		
	}
	
	//insert subject to this node
	public def insertDataSet(S:RemoteArray[Long]){
		var s1:Int=S.size;
		data=new ArrayList[Long](s1);
		for(var i:Int=0;i<s1;i++){
			data(i)=S(i);			
		}				
	}
	
	//return the data
	public def getData():ArrayList[Long]=data;
	
	//return the data in the form of Array[S]
	public def getDataSubjects():Array[Long]=data.toArray();
	
}