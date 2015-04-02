package elasticsearch_file.sender.city;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;

public class City {

	private static Client client;
	private static TransportClient transportClient;

	private static String server = "52.4.162.5";
	private static String port = "9300";
	private static String cluster = "elasticsearch";
	private static String type = "city";

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

	public void write(String city, String state, String npa, String nxx)
			throws ElasticsearchException, Exception {

		XContentBuilder doc = jsonBuilder().startObject();
		doc.field("city").value(city);
		doc.field("state").value(state);
		doc.field("npa").value(npa);
		doc.field("nxx").value(nxx);
		doc.endObject();

		this.getClient().prepareIndex("kubal", type).setSource(doc).get();
	}

	public void sendToElasticsearc(String city, String state, String npa,
			String nxx) {

		try {
			this.write(city, state, npa, nxx);
		} catch (ElasticsearchException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
