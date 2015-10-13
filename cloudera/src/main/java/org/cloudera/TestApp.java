package org.cloudera;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;

import org.cloudera.utility.PairTuple;

public class TestApp {
	
	public static void main(String[] args) {
		String fromDate="2015-10-12";
		String toDate="2015-10-14";
		List<PairTuple<String,String>> dailyTime=extractDates(fromDate, toDate);
		for(PairTuple<String,String> eachPair:dailyTime){
			System.out.println(eachPair.getFirstElement()+" "+eachPair.getSecondElement());
		}
		
	}
	private static List<PairTuple<String,String>> extractDates(String fromDate, String toDate) {
		Calendar cal1 = new GregorianCalendar();
	    Calendar cal2 = new GregorianCalendar();
	    List<PairTuple<String,String>> dailyTimes=new ArrayList<PairTuple<String,String>>();
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		try {
			Date fromDateFormat = formatter.parse(fromDate);
			cal1.setTime(fromDateFormat);
			Date toDateFormat = formatter.parse(toDate);
			cal2.setTime(toDateFormat);
			int days = daysBetween(cal1.getTime(), cal2.getTime());
			while(days!=0){
				String initialFromDate=formatter.format(fromDateFormat);
				fromDateFormat=new Date(fromDateFormat.getTime() + (1000 * 60 * 60 * 24));
				String newFromDate=formatter.format(fromDateFormat);
				dailyTimes.add(new PairTuple<String, String>(initialFromDate,newFromDate));
				days--;
			}

		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return dailyTimes;
	}
	 public static int daysBetween(Date d1, Date d2){
         return (int)( (d2.getTime() - d1.getTime()) / (1000 * 60 * 60 * 24));
 }
}
