package org.cloudera.timeseries;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import javax.ws.rs.core.Response;

import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.cloudera.timeseries.utility.PairTuple;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.cloudera.api.ApiRootResource;
import com.cloudera.api.ClouderaManagerClientBuilder;
import com.cloudera.api.v10.RootResourceV10;
import com.cloudera.api.v6.TimeSeriesResourceV6;

/**
 * @author vijith.reddy
 *
 */
public class TimeSeriesMetrics {
	// final static ByteArrayOutputStream out=new ByteArrayOutputStream();
	public static void main(String[] args) {

		if (args.length != 7) {
			System.err
					.println("Please use: Hostname Cluster-username Cluster-password tsQuery fromDate(yyyy-MM-dd) toDate(yyyy-MM-dd) output-file-location");
		}
		String hostname = args[0];
		String username = args[1]; // admin
		String password = args[2]; // admin
		String tsQuery = args[3]; // select total_cpu_user where
									// serviceType=IMPALA
		String fromDate = args[4];
		String toDate = args[5];
		String location = args[6];
		ApiRootResource root = new ClouderaManagerClientBuilder()
				.withHost(hostname).withUsernamePassword(username, password)
				.build();
		RootResourceV10 v10 = root.getRootV10();

		TimeSeriesResourceV6 tv6 = v10.getTimeSeriesResource();
		List<PairTuple<String, String>> dates = extractDates(fromDate, toDate);
		calculateMetrics(tsQuery, tv6, dates, location);

		// ------------------Older Version of Time
		// series-------------------------
		/*
		 * ApiTimeSeriesResponseList asr=tv6.queryTimeSeries(tsQuery, fromDate,
		 * toDate); System.out.println(asr.getResponses()); List<ApiTimeSeries>
		 * listApis=asr.getResponses().get(0).getTimeSeries(); for
		 * (ApiTimeSeries res:listApis){
		 * System.out.println("Entity name: "+res.getMetadata
		 * ().getEntityName());
		 * System.out.println("Expression: "+res.getMetadata().getExpression());
		 * System
		 * .out.println("Metric Name: "+res.getMetadata().getMetricName());
		 * System.out.println("Rollup: "+res.getMetadata().getRollupUsed());
		 * System.out.println("Metric freq: "+res.getMetadata().
		 * getMetricCollectionFrequencyMs());
		 * System.out.println("Start Date: "+res.getMetadata().getStartTime());
		 * System.out.println("End Date: "+res.getMetadata().getEndTime());
		 * System.out.println("------------Data---------"); int count=0;
		 * for(ApiTimeSeriesData res1:res.getData()){ count+=1;
		 * System.out.println("Type: "+res1.getType());
		 * System.out.println("Value: "+res1.getValue());
		 * if(res1.getAggregateStatistics()!=null){
		 * System.out.println("Max stat"
		 * +res1.getAggregateStatistics().getMax()); } // }
		 * System.out.println("Number of metrics: "+count);
		 * System.out.println(); }
		 */

		// -----Cluster information API

		/*
		 * System.out.println("--------Cluster Information--------");
		 * ClustersResourceV10 cr10 = v10.getClustersResource(); ApiClusterList
		 * clusters = cr10.readClusters(DataView.FULL); for (ApiCluster cluster
		 * : clusters) { ServicesResourceV10 serviceR =
		 * cr10.getServicesResource(cluster .getName());
		 * 
		 * for (ApiService service : serviceR.readServices(DataView.FULL)) {
		 * System.out.println("\t" + service.getName()); RolesResourceV2
		 * rolesResource = serviceR .getRolesResource(service.getName()); for
		 * (ApiRole role : rolesResource.readRoles()) {
		 * System.out.println("\t\t" + role.getName()); } } }
		 */

	}

	/**
	 * Calculates the metrics (peakvalue) with the given TSQuery
	 * @param tsQuery  Time series query (refer timeseries query language: {@link http://www.cloudera.com/content/www/en-us/documentation/enterprise/latest/topics/cm_dg_tsquery.html}  )
	 * @param tv6 TimeSeriesResourceV6 object
	 * @param dates A list of pair objects, Obj[String,String] with fromDate, toDate
	 * @param location File location to save the excel file
	 */
	private static void calculateMetrics(String tsQuery,
			TimeSeriesResourceV6 tv6, List<PairTuple<String, String>> dates,
			String location) {
		int index = 2;
		HSSFWorkbook workbook = new HSSFWorkbook();
		HSSFSheet sheet = workbook.createSheet("timeseries metrics");
		Map<Integer, Object[]> sheetData = new TreeMap<Integer, Object[]>();
		sheetData.put(1, new Object[] { "From Date", "To Date", "Metric Name",
				"Entity Name", "Start Time", "End Time", "attributes",
				"timeseries Expression", "Rollup used", "Peak TimeStamp",
				"Peak Value", "Aggregate Stats" });
		for (PairTuple<String, String> eachPair : dates) {
			String fromDate = "";
			String toDate = "";
			String metricName = "";
			String entityName = "";
			String startTime = "";
			String endTime = "";
			String attributes = "";
			String timeSeriesExpr = "";
			String rollup = "";
			String peakTimeStamp = "";
			Double peakValue = null;
			String aggregateStats = "";

			// Roll ups can be set to TEN_MINUTELY, HOURLY, SIX_HOURLY, DAILY,
			// or
			// WEEKLY.
			System.out.println("-----------Results for :"
					+ eachPair.getFirstElement() + " "
					+ eachPair.getSecondElement() + " ----------");
			Response response = tv6.queryTimeSeries(tsQuery,
					eachPair.getFirstElement(), eachPair.getSecondElement(),
					"application/json", "HOURLY", true);
			fromDate = eachPair.getFirstElement();
			toDate = eachPair.getSecondElement();
			String jsonResponse = response.readEntity(String.class);
			// To Print the whole response uncomment the following
			// System.out.println(jsonResponse);
			JSONParser parser = new JSONParser();
			Object obj;
			try {
				obj = parser.parse(jsonResponse);
				JSONObject jsonObj = (JSONObject) obj;
				JSONArray items = (JSONArray) jsonObj.get("items");
				JSONObject timeSeriesObj = (JSONObject) items.get(0);
				JSONArray timeSeriesArray = (JSONArray) timeSeriesObj
						.get("timeSeries");

				for (Object eachMetadata : timeSeriesArray) {
					JSONObject metaObj = (JSONObject) eachMetadata;
					JSONObject metadata = (JSONObject) metaObj.get("metadata");
					System.out.println("Metric Name: "
							+ metadata.get("metricName"));
					metricName = metadata.get("metricName").toString();
					System.out.println("EntityName: "
							+ metadata.get("entityName"));
					entityName = metadata.get("entityName").toString();
					System.out.println("Start Time: "
							+ metadata.get("startTime"));
					startTime = metadata.get("startTime").toString();
					System.out.println("End Time: " + metadata.get("endTime"));
					endTime = metadata.get("endTime").toString();
					System.out.println("attributes: "
							+ metadata.get("attributes"));
					attributes = metadata.get("attributes").toString();
					System.out.println("timeseries Expression: "
							+ metadata.get("expression"));
					timeSeriesExpr = metadata.get("expression").toString();
					System.out.println("Rollup Used: "
							+ metadata.get("rollupUsed"));
					rollup = metadata.get("rollupUsed").toString();
					JSONArray data = (JSONArray) metaObj.get("data");
					JSONObject peakUsage = null;
					for (Object eachData : data) {
						peakUsage = calculatePeakUsage(peakUsage, eachData);
					}
					if (peakUsage != null) {
						System.out
								.println("-----------Your Peak usage-----------");
						System.out.println("TimeStamp: "
								+ peakUsage.get("timestamp"));
						peakTimeStamp = peakUsage.get("timestamp").toString();
						System.out.println("Peak Value: "
								+ peakUsage.get("value"));
						peakValue = (Double) peakUsage.get("value");
						if (peakUsage.get("aggregateStatistics") != null) {
							System.out.println("Aggregate Stats: "
									+ peakUsage.get("aggregateStatistics"));
							aggregateStats = peakUsage.get(
									"aggregateStatistics").toString();
						}
					} else {
						System.out.println("No Values to measure peak usage");
					}

					sheetData.put(index, new Object[] { fromDate, toDate,
							metricName, entityName, startTime, endTime,
							attributes, timeSeriesExpr, rollup, peakTimeStamp,
							peakValue, aggregateStats });

					index++;

					System.out.println();
				}

			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		saveExcel(location, workbook, sheet, sheetData);
	}

	/**
	 * This method is to save the output metrics into an Excel sheet
	 * @param location Location in the file system where the excel file has to be saved
	 * @param workbook workbook object
	 * @param sheet Sheet object
	 * @param sheetData The data to be saved in the excel
	 */
	private static void saveExcel(String location, HSSFWorkbook workbook,
			HSSFSheet sheet, Map<Integer, Object[]> sheetData) {
		Set<Integer> keyset = sheetData.keySet();
		int rownum = 0;
		for (Integer key : keyset) {
			Row row = sheet.createRow(rownum++);
			Object[] objArr = sheetData.get(key);
			int cellnum = 0;
			for (Object obj : objArr) {
				Cell cell = row.createCell(cellnum++);
				if (obj instanceof Date)
					cell.setCellValue((Date) obj);
				else if (obj instanceof Boolean)
					cell.setCellValue((Boolean) obj);
				else if (obj instanceof String)
					cell.setCellValue((String) obj);
				else if (obj instanceof Double)
					cell.setCellValue((Double) obj);
			}
		}
		try {
			FileOutputStream out = new FileOutputStream(new File(location));
			workbook.write(out);
			out.close();
			System.out.println("Excel written successfully..");

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * This method compares two json objects and returns the highest Json object with the highest value 
	 * @param peakUsage Json object with a double value
	 * @param eachData Object object with a double value
	 * @return peakUsage Json object with peak value
	 */
	private static JSONObject calculatePeakUsage(JSONObject peakUsage,
			Object eachData) {
		JSONObject eachDataObj = (JSONObject) eachData;
		if (eachDataObj.get("value") instanceof Double) {
			if (peakUsage != null) {
				Double peakValue = (Double) peakUsage.get("value");
				Double eachDataValue = (Double) eachDataObj.get("value");
				if (eachDataValue > peakValue) {
					peakUsage = eachDataObj;
				}
			} else {
				peakUsage = (JSONObject) eachData;
			}
		}
		return peakUsage;
	}

	/**
	 * This method will return a list of fromDate and toDate with 1 day intervals. That is if the parameters specified to this method are 2015-10-12 2015-10-14, the returned tuples will be t1[2015-10-12,2015-10-13], t2[2015-10-13,2015-10-14]
	 * @param fromDate start date in String with (yyyy-MM-dd) fromat
	 * @param toDate end date in String with (yyyy-MM-dd) fromat
	 * @return list of Pair Tuples (PairTuple{String,String})
	 */
	private static List<PairTuple<String, String>> extractDates(
			String fromDate, String toDate) {
		Calendar cal1 = new GregorianCalendar();
		Calendar cal2 = new GregorianCalendar();
		List<PairTuple<String, String>> dailyTimes = new ArrayList<PairTuple<String, String>>();
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		try {
			Date fromDateFormat = formatter.parse(fromDate);
			cal1.setTime(fromDateFormat);
			Date toDateFormat = formatter.parse(toDate);
			cal2.setTime(toDateFormat);
			int days = daysBetween(cal1.getTime(), cal2.getTime());
			while (days != 0) {
				String initialFromDate = formatter.format(fromDateFormat);
				fromDateFormat = new Date(fromDateFormat.getTime()
						+ (1000 * 60 * 60 * 24));
				String newFromDate = formatter.format(fromDateFormat);
				dailyTimes.add(new PairTuple<String, String>(initialFromDate,
						newFromDate));
				days--;
			}

		} catch (java.text.ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return dailyTimes;
	}

	/**
	 * This method calculates the number of days between two dates
	 * @param day1 from date 
	 * @param day2 to date
	 * @return The number of days
	 */
	public static int daysBetween(Date day1, Date day2) {
		return (int) ((day2.getTime() - day1.getTime()) / (1000 * 60 * 60 * 24));
	}
}
