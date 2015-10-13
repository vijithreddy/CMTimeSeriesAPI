package org.cloudera;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;

import javax.ws.rs.core.Response;

import org.cloudera.utility.PairTuple;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.cloudera.api.ApiRootResource;
import com.cloudera.api.ClouderaManagerClientBuilder;
import com.cloudera.api.v10.RootResourceV10;
import com.cloudera.api.v6.TimeSeriesResourceV6;

/**
 * 
 *
 */
public class TimeSeriesMetrics {
	// final static ByteArrayOutputStream out=new ByteArrayOutputStream();
	public static void main(String[] args) {

		if (args.length != 6) {
			System.err
					.println("Please use: Hostname Cluster-username Cluster-password tsQuery fromDate(yyyy-MM-dd) toDate(yyyy-MM-dd)");
		}
		String hostname = args[0]; // ec2-54-189-132-139.us-west-2.compute.amazonaws.com
		String username = args[1]; // admin
		String password = args[2]; // admin
		String tsQuery = args[3]; // select total_cpu_user where
									// serviceType=IMPALA
		String fromDate = args[4];
		String toDate = args[5];

		ApiRootResource root = new ClouderaManagerClientBuilder()
				.withHost(hostname).withUsernamePassword(username, password)
				.build();
		RootResourceV10 v10 = root.getRootV10();

		TimeSeriesResourceV6 tv6 = v10.getTimeSeriesResource();
		List<PairTuple<String, String>> dates = extractDates(fromDate, toDate);
		for (PairTuple<String, String> eachPair : dates) {
			// Roll ups can be set to TEN_MINUTELY, HOURLY, SIX_HOURLY, DAILY,
			// or
			// WEEKLY.
			System.out.println("-----------Results for :"
					+ eachPair.getFirstElement() + " "
					+ eachPair.getSecondElement() + " ----------");
			Response response = tv6.queryTimeSeries(tsQuery,
					eachPair.getFirstElement(), eachPair.getSecondElement(),
					"application/json", "HOURLY", true);
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
					System.out.println("EntityName: "
							+ metadata.get("entityName"));
					System.out.println("Start Time: "
							+ metadata.get("startTime"));
					System.out
							.println("EntityName: " + metadata.get("endTime"));
					System.out.println("attributes: "
							+ metadata.get("attributes"));
					System.out.println("timeseries Expression: "
							+ metadata.get("expression"));
					System.out.println("Rollup Used: "
							+ metadata.get("rollupUsed"));
					JSONArray data = (JSONArray) metaObj.get("data");
					JSONObject peakUsage = null;
					for (Object eachData : data) {

						JSONObject eachDataObj = (JSONObject) eachData;
						if (eachDataObj.get("value") instanceof Double) {
							if (peakUsage != null) {
								Double peakValue = (Double) peakUsage
										.get("value");
								Double eachDataValue = (Double) eachDataObj
										.get("value");
								if (eachDataValue > peakValue) {
									peakUsage = eachDataObj;
								}
							} else {
								peakUsage = (JSONObject) eachData;
							}
						}

					}
					if (peakUsage != null) {
						System.out
								.println("-----------Your Peak usage-----------");
						System.out.println("TimeStamp: "
								+ peakUsage.get("timestamp"));
						System.out.println("Peak Value: "
								+ peakUsage.get("value"));
						if (peakUsage.get("aggregateStatistics") != null) {
							System.out.println("Aggregate Stats: "
									+ peakUsage.get("aggregateStatistics"));
						}
					} else {
						System.out.println("No Values to measure peak usage");
					}

					System.out.println();
				}

			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

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

	public static int daysBetween(Date d1, Date d2) {
		return (int) ((d2.getTime() - d1.getTime()) / (1000 * 60 * 60 * 24));
	}
}
