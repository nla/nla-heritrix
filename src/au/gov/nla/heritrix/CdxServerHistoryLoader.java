package au.gov.nla.heritrix;

import org.apache.commons.httpclient.util.URIUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.archive.modules.CrawlURI;
import org.archive.modules.Processor;
import org.archive.util.DateUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.text.ParseException;
import java.util.HashMap;

import static org.archive.modules.CoreAttributeConstants.A_FETCH_BEGAN_TIME;
import static org.archive.modules.recrawl.RecrawlAttributeConstants.A_CONTENT_DIGEST;

/**
 * Populates the fetch history by querying a CDX server (such as OutbackCDX or pywb) enabling identical digest
 * deduplication against an existing web archive. Intended to be used together with FetchHistoryProcessor.
 *
 * <pre>
 * {@code
 * <bean id="cdxHistoryLoader" class="au.gov.nla.heritrix.CdxServerHistoryLoader">
 *   <property name="cdxServerUrl" value="http://localhost:8080/mycollection" />
 * </bean>
 * <bean id="fetchHistoryProcessor" class="org.archive.modules.recrawl.FetchHistoryProcessor">
 *   <property name="historyLength" value="2" />
 * </bean>
 * <bean id="fetchProcessors" class="org.archive.modules.FetchChain">
 *  <property name="processors">
 *   <list>
 *    <ref bean="cdxHistoryLoader"/>
 *    ...
 *    <ref bean="fetchHttp"/>
 *    <ref bean="fetchHistoryProcessor"/>
 *     ...
 *   </list>
 *  </property>
 * </bean>
 * }
 * </pre>
 */
public class CdxServerHistoryLoader extends Processor {
    private static final Log log = LogFactory.getLog(CdxServerHistoryLoader.class);

    private String cdxServerUrl;
    private int queryLimit = 10;

    @Override
    public void start() {
        if (cdxServerUrl == null) {
            throw new RuntimeException("No cdxServerUrl configured");
        }
        super.start();
    }

    @Override
    protected boolean shouldProcess(CrawlURI uri) {
        String scheme = uri.getUURI().getScheme();
        return scheme.equals("http") || scheme.equals("https");
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void innerProcess(CrawlURI uri) {
        if (uri.getFetchHistory() != null) return;
        try {
            String exactUrl = uri.toString();
            String queryUrl = cdxServerUrl + "?url=" + URIUtil.encodeWithinQuery(exactUrl) + "&sort=reverse&limit=" + queryLimit;
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(new URL(queryUrl).openStream()))) {
                while (true) {
                    String line = reader.readLine();
                    if (line == null) return;
                    String[] fields = line.split(" ");
                    if (fields.length < 6) {
                        log.warn("Invalid CDX line in " + queryUrl);
                        return;
                    }
                    if (!fields[2].equals(exactUrl)) continue;
                    HashMap<String, Object> map = new HashMap<>();
                    map.put(A_FETCH_BEGAN_TIME, DateUtils.parse14DigitDate(fields[1]).getTime());
                    map.put(A_CONTENT_DIGEST, "sha1:" + fields[5]);
                    uri.setFetchHistory(new HashMap[]{map});
                }
            }
        } catch (IOException | ParseException e) {
            log.warn("Exception fetching history for " + uri, e);
        }
    }

    public void setCdxServerUrl(String cdxServerUrl) {
        this.cdxServerUrl = cdxServerUrl;
    }

    public int getQueryLimit() {
        return queryLimit;
    }

    public void setQueryLimit(int queryLimit) {
        this.queryLimit = queryLimit;
    }
}
