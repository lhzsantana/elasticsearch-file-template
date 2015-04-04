package elasticsearch_file.sender.city;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;

public class City {

	private static Client client;
	private static TransportClient transportClient;

	private static String server="198.23.188.82";
	private static String port="9301";
	private static String cluster = "elasticsearch";
	private static String type = "city";
	

	private static BulkRequestBuilder bulkRequest;

	private static int total;


	public static synchronized Client getClient() throws Exception {

		if (client == null) {
			Settings settings = ImmutableSettings.settingsBuilder()
					.put("cluster.name", cluster).build();
			transportClient = new TransportClient(settings);
			client = transportClient
					.addTransportAddress(new InetSocketTransportAddress(server,
							Integer.parseInt(port)));
		}

		return client;
	}

	public void write(String city, String nickname, String state, String npa, String nxx)
			throws ElasticsearchException, Exception {
		

		if(bulkRequest==null){
			bulkRequest = getClient().prepareBulk();
		}

		XContentBuilder doc = jsonBuilder().startObject();
		doc.field("city").value(city);
		doc.field("nickname").value(nickname);
		doc.field("state").value(state);
		doc.field("npa").value(npa);
		doc.field("nxx").value(nxx);
		doc.endObject();

		System.out.println(total+"--"+doc.string());
		
		bulkRequest.add(getClient().prepareIndex("randomnumbers", type).setSource(doc));
		
		total++;
		
		if(total%1000==0){
			bulkRequest.execute().actionGet();
			bulkRequest = null;
			System.out.println("Sent requests "+total);
;		}

		//this.getClient().prepareIndex("randomnumbers", type).setSource(doc).get();
	}

	public void sendToElasticsearc(String city, String nickname, String state, String npa,
			String nxx) {

		try {
			this.write(city, nickname, state, npa, nxx);
		} catch (ElasticsearchException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
