import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class Main {
    private static final Logger log = LogManager.getLogger(Main.class);
    public static void main(String[] args)
            throws InterruptedException, ExecutionException {
        log.info("Sleeping for 1 minute");
        Thread.sleep(1000*60);
        while(true) {
            queryPrometheus();
            log.info("Sleeping for 1 second");
            Thread.sleep(10*1000);
        }
    }

    static void queryPrometheus()
            throws ExecutionException, InterruptedException {
        HttpClient client = HttpClient.newHttpClient();
        String testtopic1 = "http://prometheus-operated:9090/api/v1/query?" +
                "query=testtopic1i";
        String testtopic2 = "http://prometheus-operated:9090/api/v1/query?" +
                "query=testtopic2";
        String testtopic2i = "http://prometheus-operated:9090/api/v1/query?" +
                "query=testtopic2i";
        String testtopic3 = "http://prometheus-operated:9090/api/v1/query?" +
                "query=testtopic3";

        List<URI> queries = new ArrayList<>();
        try {
            queries = Arrays.asList(
                    new URI(testtopic1),
                    new URI(testtopic2),
                    new URI(testtopic2i),
                    new URI(testtopic3)
            );
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

        List<CompletableFuture<String>> results = queries.stream()
                .map(target -> client
                        .sendAsync(
                                HttpRequest.newBuilder(target).GET().build(),
                                HttpResponse.BodyHandlers.ofString())
                        .thenApply(HttpResponse::body))
                .collect(Collectors.toList());

        for (CompletableFuture<String> cf :  results) {
            System.out.println(parseJson(cf.get()));
            }
    }

    static Double parseJson(String json ) {
        //json string from prometheus
        //{"status":"success","data":{"resultType":"vector","result":
        // [{"metric":{"topic":"testtopic1"},"value":[1659006264.066,"144.05454545454546"]}]}}
        //{"status":"success","data":{"resultType":"vector","result":
        // [{"metric":{"__name__":"testtopic2","container":"cons1persec","endpoint":"brom",
        // "instance":"10.124.2.54:8080","job":"default/demoobservabilitypodmonitor","namespace":"default",
        // "pod":"cons1persec-6765c9946c-pl9f4","topic_to":"testtopic2"},"value":[1680516659.037,"37612"]}]}}
        try {
            JSONObject jsonObject = JSONObject.parseObject(json);
            JSONObject j2 = (JSONObject) jsonObject.get("data");
            JSONArray inter = j2.getJSONArray("result");
            JSONObject jobj = (JSONObject) inter.get(0);
            JSONArray jreq = jobj.getJSONArray("value");
            return Double.parseDouble(jreq.getString(1));
        } catch (IndexOutOfBoundsException e) {
           // e.printStackTrace();
            log.info("looks like the service is still not discovered");
            return 0.0;
        }
    }

}



