import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

public class Apriori {
	
	public static ArrayList<String> findFirstLevelCandidateItemSets(String minSup, ArrayList<String> transactions,
			ArrayList<String> newCandidateSet) {

		Map<String, Integer> frequentMap = new HashMap<String, Integer>();
		for(String candidate : newCandidateSet) 
		{
			for(String trans : transactions) 
			{
				String[] candidateArr = candidate.split(",");
				String[] transArr = trans.split(",");
				transArr[0]=null;
								
				if(Arrays.asList(transArr).containsAll(Arrays.asList(candidateArr))) {
					Integer count = frequentMap.get(candidate);
				    if(count == null) {
				        count = 0;
				    }
				    frequentMap.put(candidate, (count.intValue()+1));					
				}
			}						
		}
		
		Iterator iterator = frequentMap.entrySet().iterator();		
		while(iterator.hasNext()){
			
			Map.Entry mentry = (Map.Entry)iterator.next();					
			int value = (Integer)mentry.getValue();
			if(value < Integer.valueOf(minSup))
				iterator.remove();
		}
		ArrayList<String> frequentItemSets = new ArrayList<String>(frequentMap.keySet());
		return frequentItemSets;
	}

	public static ArrayList<String> findKLevelCandidateItemSets(String minSup, ArrayList<String> firstFrequentSet) {
		
		ArrayList<String> newCandidates = new ArrayList<String>();
		for(int i=0; i<firstFrequentSet.size(); i++) 
		{			
			for(int j=i+1; j<firstFrequentSet.size(); j++) 
			{
				String[] firstItem = firstFrequentSet.get(i).split(",");
				String[] secondItem = firstFrequentSet.get(j).split(",");
				if(firstItem.length == secondItem.length) {
					
					if(firstItem.length>1) {
						
						int len = firstItem.length-1;
						Boolean equal = true;
						for(int k=0; k<len; k++) {
							if(Integer.valueOf(firstItem[k]) != Integer.valueOf(secondItem[k])) {								
								equal = false;
								break;
							}
						}
						if(equal) {
							if(Integer.valueOf(firstItem[len]) != Integer.valueOf(secondItem[len])) {								
								newCandidates.add(StringUtils.join(firstItem, ",") + "," + secondItem[secondItem.length-1]);
							}
						}
					}else {
						String item = firstItem[0] + "," + secondItem[0];
						newCandidates.add(item);						
					}
											
				}
					
			}
		}
		return newCandidates;	 
		
	}

	public static ArrayList<String> findFirstLevelCandidateItemSets(String minSup, ArrayList<String> transactions) {

		Map<String, Integer> map = new HashMap<String, Integer>();

		for (String itemSet : transactions) {
			String[] items = itemSet.split(","); 
			for(int i=1; i<items.length; i++) {				
				Integer count = map.get(items[i]);
			    if(count == null) {
			        count = 0;
			    }
			    map.put(items[i].toString(), (count.intValue()+1));
			}			
		}
		
		Iterator iterator = map.entrySet().iterator();		
		while(iterator.hasNext()){
			
			Map.Entry mentry = (Map.Entry)iterator.next();					
			int value = (Integer)mentry.getValue();
			if(value < Integer.valueOf(minSup))
				iterator.remove();
		}
		ArrayList<String> firstLevelCandidateItemSets = new ArrayList<String>(map.keySet());
		Collections.sort(firstLevelCandidateItemSets, new MyComparator());
		return firstLevelCandidateItemSets;
	}
}

class MyComparator implements Comparator<String> {

    public int compare(String o1, String o2){
        return new Integer(o1).compareTo(new Integer(o2));
    }

}
