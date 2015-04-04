package elasticsearch_file.sender.businessfax;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;

public class BusinessFax {

	private static Client client;
	private static TransportClient transportClient;

	private static String server = "198.23.188.82";
	private static String port = "9301";
	private static String cluster = "elasticsearch";

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

	public void write(String type, String phone, String code)
			throws ElasticsearchException, Exception {

		if (bulkRequest == null) {
			bulkRequest = getClient().prepareBulk();
		}

		XContentBuilder doc = jsonBuilder().startObject();
		doc.field("phone").value(phone);
		doc.field("code").value(code);
		doc.field("complete").value(code + phone);
		doc.endObject();

		bulkRequest.add(getClient().prepareIndex("randomnumbers", type)
				.setSource(doc));

		total++;

		if (total % 1000 == 0) {
			bulkRequest.execute().actionGet();
			bulkRequest = null;
			System.out.println("Sent requests " + total);
			;
		}
	}

	public void sendToElasticsearch(String type, String phone) {

		if (!phone.equals("") && phone.length() == 10) {

			// System.out.println(phone);

			String id = phone.substring(0, 3);
			phone = phone.substring(3, phone.length());

			System.out.println(type);
			System.out.println(id);
			System.out.println(phone);

			try {
				this.write(type, phone, id);
			} catch (ElasticsearchException e) {
				// TODO Auto-generated catch block e.printStackTrace();
			} catch (Exception e) { // TODO Auto-generated catch block
									// e.printStackTrace(); }
			}
		}
	}

}
