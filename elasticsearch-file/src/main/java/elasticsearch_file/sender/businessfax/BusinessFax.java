package elasticsearch_file.sender.businessfax;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;

public class BusinessFax {

	private static Client client;
	private static TransportClient transportClient;

	private static String server="52.4.162.5";
	private static String port="9300";
	private static String cluster="elasticsearch";

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

	public void write(String type, String phone,
			String code) throws ElasticsearchException, Exception {
	

		XContentBuilder doc = jsonBuilder().startObject();
		doc.field("phone").value(phone);
		doc.field("code").value(code);
		doc.field("complete").value(code+phone);
		doc.endObject();
		
		this.getClient()
			.prepareIndex("kubal", type)
			.setSource(doc)
			.get();
	}

	public void sendToElasticsearch(String type, String phone) {

		if (!phone.equals("") && phone.length()==10) {
			
			System.out.println(phone);
			
			String id=phone.substring(0, 3);
			phone=phone.substring(3, phone.length());

			/*
			System.out.println(type);
			System.out.println(id);
			System.out.println(phone);
			 */
			
			try {
				this.write(type, phone, id);
			} catch (ElasticsearchException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
