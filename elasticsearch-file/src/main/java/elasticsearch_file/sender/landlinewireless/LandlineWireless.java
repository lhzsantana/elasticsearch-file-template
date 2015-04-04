package elasticsearch_file.sender.landlinewireless;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;

public class LandlineWireless {

	private static Client client;
	private static TransportClient transportClient;

	private static String server="198.23.188.82";
	private static String port="9301";
	private static String cluster = "elasticsearch";

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

	public void write(String type, String phone, String npa, String nxx)
			throws ElasticsearchException, Exception {

		XContentBuilder doc = jsonBuilder().startObject();
		doc.field("phone").value(phone);
		doc.field("npa").value(npa);
		doc.field("nxx").value(nxx);
		doc.field("complete").value(npa+nxx+phone);
		doc.endObject();

		this.getClient().prepareIndex("randomnumbers", type).setSource(doc).get();
	}

	public void sendToElasticsearc(String type, String phone, String npa,
			String nxx) {

		try {
			this.write(type, phone, npa, nxx);
		} catch (ElasticsearchException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
